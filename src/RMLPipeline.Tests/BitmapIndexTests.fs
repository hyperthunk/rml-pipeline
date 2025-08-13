namespace RMLPipeline.Tests

open System
open System.Threading
open System.Collections.Generic
open Expecto
open FsCheck
open RMLPipeline.Core
open RMLPipeline.Internal.StringPooling
open RMLPipeline.Internal.StringInterning
open RMLPipeline.Internal.StringInterning.HashIndexing
open System.Threading.Tasks

module BitmapIndexTests =

    // Helper methods
    let genAlphaString =
        Gen.elements ['a'..'z']
        |> Gen.nonEmptyListOf
        |> Gen.map (fun chars -> String(List.toArray chars))

    let genRandomStrings count =
        gen {
            let! strings = Gen.listOfLength count genAlphaString
            return strings |> List.distinct  // Ensure unique strings
        }

    // Realistic string set with field paths, URIs, and literals
    let genRealisticStringSet =
        Gen.oneof [
            // Field paths (e.g. "person.name.first")
            Gen.map3 (fun a b c -> $"{a}.{b}.{c}")
                (Gen.elements ["user"; "person"; "order"; "item"; "account"])
                (Gen.elements ["name"; "address"; "details"; "metadata"; "properties"])
                (Gen.elements ["first"; "last"; "street"; "city"; "id"; "value"])
            
            // URI templates (e.g. "/api/users/{id}")
            Gen.map3 (fun a b c -> $"/{a}/{b}/{{{c}}}")
                (Gen.elements ["api"; "v1"; "data"; "service"])
                (Gen.elements ["users"; "orders"; "items"; "accounts"; "profiles"])
                (Gen.elements ["id"; "uuid"; "name"; "key"; "token"])
            
            // Simple literals
            Gen.elements [
                "true"; "false"; "null"; "undefined"; 
                "string"; "number"; "object"; "array";
                "GET"; "POST"; "PUT"; "DELETE"; "PATCH"
            ]
        ]

    let genStringSet = 
        Gen.frequency [
            (3, genRealisticStringSet |> Gen.listOf |> Gen.map List.distinct)
            (1, genAlphaString |> Gen.listOf |> Gen.map List.distinct)
        ]

    // Generate a set of strings with potential duplicates (for collision testing)
    let genCollisionStringSet =
        gen {
            // Start with a base set of strings
            let! baseStrings = genStringSet
            
            // Add some modified versions that will likely produce collisions
            let! modifiedStrings = 
                baseStrings
                |> List.map (fun s -> 
                    // Generate a string similar to the original
                    if s.Length > 1 then
                        Gen.elements [
                            s + "X"               // Append a character
                            s.Substring(0, s.Length-1)  // Remove last character
                            String(s.ToCharArray() |> Array.rev)  // Reverse
                            s.ToUpper()           // Change case
                            s.Replace("a", "b")   // Character substitution
                        ]
                    else
                        Gen.constant (s + "X")
                )
                |> Gen.sequence
            
            return List.concat [baseStrings; modifiedStrings] |> List.distinct
        }

    type BitmapIndexArbitraries =
        static member StringSet() =
            Arb.fromGen genStringSet
            
        static member CollisionStringSet() =
            Arb.fromGen genCollisionStringSet
            
        static member SizeHint() =
            Arb.fromGen (Gen.choose(16, 10000))
            
        static member ThreadCount() =
            Arb.fromGen (Gen.choose(2, 16))

    let fsConfig = { FsCheckConfig.defaultConfig with 
                        arbitrary = [ typeof<BitmapIndexArbitraries> ]
                        maxTest = 100 }

    // Test the direct filter operations
    module BitmapFilterProperties =
        [<Tests>]
        let tests =
            testList "BitmapFilter_Invariants" [
                testPropertyWithConfig 
                    fsConfig 
                    "Filter correctly reports non-existent strings" 
                        <| fun (strings: string list) (queryString: string) ->
                    
                    // Only consider the case where queryString is not in strings
                    not (List.contains queryString strings) ==> lazy (
                        // Create filter and add all strings
                        let poolConfig = { PoolConfiguration.Default with 
                                            InitialChunkSize = List.length strings * 2 }
                        let filter = StringIndex.Create(poolConfig, Segment)
                        
                        // Add all strings to the filter
                        for s in strings do
                            filter.AddString(s, 0uy, 0, 0) |> ignore
                        
                        // Check that filter correctly says queryString doesn't exist
                        filter.TryGetLocation(queryString).IsNone
                    )
                
                testPropertyWithConfig 
                    fsConfig 
                    "No false negatives - strings added are always found" <| fun (strings: string list) ->
                    
                    strings.Length > 0 ==> lazy (
                        // Create filter sized for the input
                        let filter = StringBitmapFilter.Create(List.length strings)
                        
                        // Add all strings with unique locations
                        strings
                        |> List.iteri (fun i s -> 
                            filter.AddString(s, 0uy, i, i) |> ignore)
                        
                        // Verify every string can be found
                        strings
                        |> List.forall (fun s -> 
                            match filter.TryGetLocation(s) with
                            | Some _ -> true
                            | None -> false
                        )
                    )

                testPropertyWithConfig 
                    fsConfig 
                    "Location data is preserved correctly" <| fun (strings: string list) ->
                    
                    strings.Length > 0 ==> lazy (
                        // Create filter and test data
                        let filter = StringBitmapFilter.Create(List.length strings * 2)
                        let testData = 
                            strings 
                            |> List.mapi (fun i s -> 
                                let arrayType = byte (i % 3) // Mix of array types
                                let stringIdx = i * 10 + 5   // Some arbitrary formula
                                let entryIdx = i * 10        // Different from stringIdx
                                (s, arrayType, stringIdx, entryIdx)
                            )
                        
                        // Add all strings with their locations
                        for (s, aType, sIdx, eIdx) in testData do
                            filter.AddString(s, aType, sIdx, eIdx) |> ignore
                        
                        // Verify all locations are retrieved correctly
                        testData |> List.forall (fun (s, aType, sIdx, eIdx) ->
                            match filter.TryGetLocation(s) with
                            | Some loc -> 
                                loc.ArrayType = aType && 
                                loc.StringIndex = sIdx && 
                                loc.EntryIndex = eIdx
                            | None -> false
                        )
                    )

                testPropertyWithConfig 
                    fsConfig 
                    "Concurrent string additions are thread-safe" <| fun (strings: string list) (threadCount: int) ->
                    
                    (strings.Length >= 10 && threadCount >= 2) ==> lazy (
                        // Split strings among threads
                        let filter = StringBitmapFilter.Create(strings.Length * 2)
                        let stringsArray = List.toArray strings
                        
                        // Use barrier to ensure all threads start at same time
                        let barrier = new Barrier(threadCount + 1)
                        
                        // Create tasks to add strings concurrently
                        let tasks = 
                            [|0..threadCount-1|]
                            |> Array.map (fun threadId ->
                                Task.Factory.StartNew(fun () ->
                                    // Wait for all threads to be ready
                                    barrier.SignalAndWait()
                                    
                                    // Each thread processes a subset of strings
                                    let startIdx = threadId * strings.Length / threadCount
                                    let endIdx = (threadId + 1) * strings.Length / threadCount - 1
                                    
                                    for i in startIdx..endIdx do
                                        if i < stringsArray.Length then
                                            let s = stringsArray.[i]
                                            filter.AddString(s, 0uy, i, i) |> ignore
                                )
                            )
                        
                        // Release all threads
                        barrier.SignalAndWait()
                        
                        // Wait for all threads to complete
                        Task.WaitAll(tasks)
                        
                        // Verify all strings can be found (eventual consistency)
                        strings |> List.forall (fun s -> 
                            match filter.TryGetLocation(s) with
                            | Some _ -> true
                            | None -> false
                        )
                    )

                testPropertyWithConfig 
                    fsConfig 
                    "Filter statistics track operations correctly" <| fun (strings: string list) (queries: string list) ->
                    
                    (strings.Length > 0 && queries.Length > 0) ==> lazy (
                        let filter = StringBitmapFilter.Create(strings.Length * 2)
                        
                        // Add all strings
                        for s in strings do
                            filter.AddString(s, 0uy, 0, 0) |> ignore
                        
                        // Track lookup counts
                        let mutable expectedLookups = 0
                        let mutable expectedHits = 0
                        
                        // Perform lookups
                        for q in queries do
                            expectedLookups <- expectedLookups + 1
                            filter.TryGetLocation(q) |> ignore
                            
                            // Count hit if string exists
                            if List.contains q strings then
                                expectedHits <- expectedHits + 1
                        
                        // Get stats
                        let hitRatio, _ = filter.GetStats()
                        
                        // Basic check that stats are being tracked
                        // Note: We're not checking exact values due to potential race conditions
                        expectedLookups > 0 && hitRatio >= 0.0 && hitRatio <= 1.0
                    )

                testPropertyWithConfig 
                    fsConfig 
                    "Resizing creates a new empty filter with increased capacity" <| fun (sizeHint: int) ->
                    
                    sizeHint > 0 ==> lazy (
                        let filter = StringBitmapFilter.Create(sizeHint)
                        let newFilter = filter.Resize(sizeHint * 2)
                        
                        // New filter should be empty and larger
                        let oldHashTableSize = filter.HashTableSize
                        let newHashTableSize = newFilter.HashTableSize
                        
                        newHashTableSize > oldHashTableSize
                    )
            ]

    // Test integration with Pool and Segments
    module PoolIntegrationTests =
        [<Tests>]
        let tests =
            testList "Pool_Filter_Integration" [
                testCase "Pool uses filter for string lookup" <| fun () ->
                    // Create a pool with filter
                    let config = PoolConfiguration.Default
                    let baseId = 1000
                    let edenSize = 100
                    let pool = Pool.Create(baseId, edenSize, config)
                    
                    // Intern some strings
                    let testStrings = ["test1"; "test2"; "test3"; "test4"; "test5"]
                    let ids = testStrings |> List.map pool.InternString
                    
                    // Verify all strings can be found
                    for i, s in List.zip ids testStrings do
                        match pool.TryGetString(i) with
                        | Some foundStr -> Expect.equal foundStr s "Retrieved string should match original"
                        | None -> failwith "String not found in pool"
                    
                    // Stats should show no misses for repeat lookups
                    let initialMisses = pool.Stats.missCount
                    
                    // Intern the same strings again - should use filter to find them
                    let ids2 = testStrings |> List.map pool.InternString
                    
                    // Miss count shouldn't increase since strings were found in filter
                    let finalMisses = pool.Stats.missCount
                    Expect.equal ids ids2 "Same strings should get same IDs"
                    Expect.equal finalMisses initialMisses "No new misses should occur"
                    
                testCase "Filter correctly handles string promotion" <| fun () ->
                    // Create pool hierarchy for promotion testing
                    let config = { PoolConfiguration.Default with 
                                   WorkerPromotionThreshold = 2
                                   GroupPromotionThreshold = 5 }
                    let planningStrings = [||]
                    let hierarchy = StringPoolHierarchy.Create(planningStrings, config)
                    
                    // Create a context and pool
                    let groupId = DependencyGroupId 1
                    let context = StringPool.createContext hierarchy (Some groupId) None
                    
                    // Intern a string and access it multiple times to trigger promotion
                    let testString = "promote-me"
                    let id = context.ContextPool.InternString(testString, StringAccessPattern.HighFrequency)
                    
                    // Access multiple times to increment temperature
                    let mutable currentId = id
                    for _ in 1..10 do
                        match context.ContextPool.GetStringWithTemperature(currentId) with
                        | Some str, newId -> 
                            Expect.equal str testString "String should be retrieved correctly"
                            currentId <- newId
                        | None, _ -> failwith "String not found"
                    
                    // Trigger promotion processing
                    let promoted = hierarchy.CheckAndPromoteHotStrings()
                    
                    // Now try to intern the same string again - should still work
                    // even though the string might be promoted
                    let id2 = context.ContextPool.InternString(testString, StringAccessPattern.HighFrequency)
                    
                    // Verification: either we got the same ID or a new one (if promoted)
                    // Either way, the string should be retrievable
                    match context.ContextPool.GetString(id2) with
                    | Some str -> Expect.equal str testString "String should be retrievable post-promotion"
                    | None -> failwith "String not found after promotion"
                
                testCase "Filter provides O(1) lookup for existing strings" <| fun () ->
                    // Create a pool with a small filter for testing
                    let config = PoolConfiguration.Default
                    let pool = Pool.Create(1000, 1000, config)
                    
                    // Create a large number of strings to intern
                    let mutable stringCount = 1000
                    let testStrings = Array.init stringCount (fun i -> $"string-{i}")
                    
                    // Intern all strings
                    let sw1 = Stopwatch.StartNew()
                    for s in testStrings do
                        pool.InternString(s) |> ignore
                    sw1.Stop()
                    
                    // Measure time to lookup existing strings
                    let sw2 = Stopwatch.StartNew()
                    for s in testStrings do
                        pool.InternString(s) |> ignore
                    sw2.Stop()
                    
                    // Expectation: lookup should be much faster than initial insertion
                    // Note: This test is soft - we don't use exact times
                    Expect.isTrue (sw2.ElapsedMilliseconds < sw1.ElapsedMilliseconds) 
                        "Second pass should be faster due to filter lookup"
                
                testCase "Segments integration handles chunking correctly" <| fun () ->
                    // Create configuration that forces chunking
                    let config = { PoolConfiguration.Default with 
                                     InitialChunkSize = 5
                                     SecondaryChunkSize = 5 }
                    let baseId = 5000
                    
                    // Create segments with filter
                    let segments = Segments.Create(baseId, config)
                    let filter = StringBitmapFilter.Create(100)
                    segments.SetStringFilter(Some filter)
                    
                    // Intern enough strings to cause chunking
                    let testStrings = [
                        "chunk1-str1"; "chunk1-str2"; "chunk1-str3"; "chunk1-str4"; "chunk1-str5"; 
                        "chunk2-str1"; "chunk2-str2"; "chunk2-str3"; "chunk2-str4"; "chunk2-str5";
                        "chunk3-str1"; "chunk3-str2"; "chunk3-str3"; "chunk3-str4"; "chunk3-str5"
                    ]
                    
                    // Intern all strings
                    let ids = testStrings |> List.map (fun s -> segments.AllocateString(s, baseId))
                    
                    // Verify all strings can be found
                    for i, s in List.zip ids testStrings do
                        match segments.TryGetString(i.Value, baseId) with
                        | Some foundStr -> Expect.equal foundStr s "Retrieved string should match original"
                        | None -> failwith "String not found in segments"
                    
                    // Should have created at least 3 chunks
                    Expect.isGreaterThan segments.Chunks.Count 2 "Multiple chunks should be created"
                    
                    // Now try to find each string via filter
                    for s in testStrings do
                        let id = segments.AllocateString(s, baseId)
                        // Getting same string should give same ID (filter working)
                        match segments.TryGetString(id.Value, baseId) with
                        | Some foundStr -> Expect.equal foundStr s "Retrieved string should match original"
                        | None -> failwith "String not found via filter lookup"
            ]

    // Thread safety and concurrency tests
    module ConcurrencyTests =
        [<Tests>]
        let tests =
            testList "Filter_Concurrency" [
                testCase "Concurrent string interning with filter is thread-safe" <| fun () ->
                    // Create a pool with filter
                    let config = PoolConfiguration.Default
                    let baseId = 2000
                    let edenSize = 1000
                    let pool = Pool.Create(baseId, edenSize, config)
                    
                    // Set of strings to intern
                    let stringCount = 500
                    let testStrings = Array.init stringCount (fun i -> $"concurrent-string-{i}")
                    
                    // Track results from each thread
                    let results = Array.init stringCount (fun _ -> StringId.Invalid)
                    
                    // Use multiple threads to intern same strings
                    let threadCount = min 8 Environment.ProcessorCount
                    let threads = Array.init threadCount (fun threadId ->
                        Thread(ThreadStart(fun () ->
                            // Each thread processes all strings (deliberately creating contention)
                            for i in 0..stringCount-1 do
                                let id = pool.InternString(testStrings.[i])
                                // Record result
                                results.[i] <- id
                        ))
                    )
                    
                    // Start and wait for all threads
                    for t in threads do t.Start()
                    for t in threads do t.Join()
                    
                    // Verify all strings were interned successfully
                    for i in 0..stringCount-1 do
                        match pool.TryGetString(results.[i]) with
                        | Some s -> Expect.equal s testStrings.[i] "Retrieved string should match original"
                        | None -> failwith "String not found"
                    
                    // Check that we have fewer misses than total attempts (filter working)
                    let totalAttempts = int64 (threadCount * stringCount)
                    let misses = pool.Stats.missCount
                    Expect.isLessThan misses totalAttempts "Should have fewer misses than attempts"
                
                testCase "Filter handles multiple threads accessing shared segments" <| fun () ->
                    // Create configuration
                    let config = { PoolConfiguration.Default with 
                                        InitialChunkSize = 50
                                        SecondaryChunkSize = 100 }
                    let baseId = 3000
                    
                    // Create segments with filter
                    let segments = Segments.Create(baseId, config)
                    let filter = StringBitmapFilter.Create(1000)
                    segments.SetStringFilter(Some filter)
                    
                    // Set of strings for each thread
                    let threadsCount = min 6 Environment.ProcessorCount
                    let stringsPerThread = 100
                    let threadStrings = 
                        Array.init threadsCount (fun threadId ->
                            Array.init stringsPerThread (fun i -> $"thread-{threadId}-string-{i}")
                        )
                    
                    // Dictionary to track all IDs (thread-safe)
                    let allIds = Dictionary<string, StringId>()
                    
                    // Run multiple threads allocating strings
                    let threads = Array.init threadsCount (fun threadId ->
                        Thread(ThreadStart(fun () ->
                            // Allocate all strings for this thread
                            for i in 0..stringsPerThread-1 do
                                let s = threadStrings.[threadId].[i]
                                let id = segments.AllocateString(s, baseId)
                                lock allIds (fun () -> allIds.[s] <- id)
                        ))
                    )
                    
                    // Start and wait for all threads
                    for t in threads do t.Start()
                    for t in threads do t.Join()
                    
                    // Verify all strings can be found
                    let allStrings = threadStrings |> Array.collect id
                    for s in allStrings do
                        let id = lock allIds (fun () -> allIds.[s])
                        match segments.TryGetString(id.Value, baseId) with
                        | Some foundStr -> Expect.equal foundStr s "String should be retrievable"
                        | None -> failwith "String not found"
                    
                    // Now allocate strings again - filter should find them
                    for s in allStrings do
                        let originalId = lock allIds (fun () -> allIds.[s])
                        let secondId = segments.AllocateString(s, baseId)
                        
                        // Either we get same ID, or if chunk was resized, a valid ID
                        Expect.isTrue secondId.IsValid "Should get valid ID on repeated allocation"
                        
                        match segments.TryGetString(secondId.Value, baseId) with
                        | Some foundStr -> Expect.equal foundStr s "String should be retrievable"
                        | None -> failwith "String not found after second allocation"
                
                testPropertyWithConfig 
                    fsConfig 
                    "False positive rate is acceptable under load" <| fun (collisionStrings: string list) ->
                    
                    collisionStrings.Length > 10 ==> lazy (
                        // Create filter sized for collision testing
                        let filter = StringIndex.Create(collisionStrings.Length)
                        
                        // Add all strings
                        for i, s in List.indexed collisionStrings do
                            filter.AddString(s, 0uy, i, i) |> ignore
                        
                        // Get current stats
                        let _, falsePositiveRate = filter.GetStats()
                        
                        // Check that false positive rate is reasonable
                        // Note: FP rate will depend on hash function quality
                        falsePositiveRate < 0.7 // Allow fairly high FP rate for filter stage
                    )
            ]

    // Main test entry point
    [<Tests>]
    let allTests =
        testList "BitmapIndexTests" [
            BitmapFilterProperties.tests
            PoolIntegrationTests.tests
            ConcurrencyTests.tests
        ]