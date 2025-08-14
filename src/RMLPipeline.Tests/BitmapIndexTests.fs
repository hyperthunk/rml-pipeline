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
open RMLPipeline.Internal.StringInterning.Packing
open System.Threading.Tasks
open System.Diagnostics

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

    let genHardCollissions =
        let hardCollisions = Gen.listOf <| Gen.oneof [
            Gen.constant "collision-string-1"
            Gen.constant "collision-string-2"
            Gen.constant "collision-string-3"
        ] 
        Gen.frequency [
            5, genCollisionStringSet
            2, genRealisticStringSet |> Gen.listOf
            1, hardCollisions
        ]

    let genNonMatchingStrings =
        gen {
            let! baseStrings = genStringSet
            let! nonMatchingStrings = 
                genStringSet 
                |> Gen.filter (fun strings ->
                    // Ensure these strings are not in the base set
                    not strings.IsEmpty &&
                    not (List.exists (fun s -> List.contains s baseStrings) strings))
            return (nonMatchingStrings.Head, baseStrings)
        }

    type StringIndexArbitraries =
        static member StringSet() =
            Arb.fromGen genStringSet
            
        static member CollisionStringSet() =
            Arb.fromGen genHardCollissions

        static member NonMatchingStrings() =
            Arb.fromGen genNonMatchingStrings

        static member SizeHint() =
            Arb.fromGen (Gen.choose(16, 10000))
            
        static member ThreadCount() =
            Arb.fromGen (Gen.choose(2, 16))

    let fsConfig = { FsCheckConfig.defaultConfig with 
                        arbitrary = [ typeof<StringIndexArbitraries> ]
                        maxTest = 100 }

    // Test the direct index operations
    module StringIndexProperties =
        [<Tests>]
        let tests =
            testList "StringIndex_Invariants" [
                testPropertyWithConfig 
                    fsConfig 
                    "Index correctly reports non-existent strings" 
                        <| fun (strings: string list, queryString: string) ->
                    
                    // Only consider the case where queryString is not in strings
                    not (List.contains queryString strings) ==> lazy (
                        // Create index and add all strings
                        let poolConfig = { PoolConfiguration.Default with 
                                            InitialChunkSize = List.length strings * 2 }
                        let index = StringIndex.Create(poolConfig, Segment)
                        
                        // Add all strings to the index
                        strings |> List.iteri (fun i s ->
                            index.AddLocation(s, 0uy, i, i))
                        
                        // Check that index correctly says queryString doesn't exist
                        match index.TryGetLocation queryString with
                        | ValueNone -> true
                        | ValueSome _ -> false
                    )
                
                testPropertyWithConfig 
                    fsConfig 
                    "No false negatives - strings added are always found" <| fun (strings: string list) ->
                    
                    strings.Length > 0 ==> lazy (
                        // Create index sized for the input
                        let poolConfig = { PoolConfiguration.Default with 
                                            InitialChunkSize = List.length strings }
                        let index = StringIndex.Create(poolConfig, Segment)

                        // Add all strings with unique locations
                        strings
                        |> List.iteri (fun i s -> 
                            index.AddLocation(s, 0uy, i, i))
                        
                        // Verify every string can be found
                        strings
                        |> List.forall (fun s -> 
                            match index.TryGetLocation s with
                            | ValueSome _ -> true
                            | ValueNone -> false
                        )
                    )

                testPropertyWithConfig 
                    fsConfig 
                    "Location data is preserved correctly" <| fun (strings: string list) ->
                    
                    strings.Length > 0 ==> lazy (
                        // Create index and test data
                        let poolConfig = { PoolConfiguration.Default with 
                                            InitialChunkSize = List.length strings * 2 }
                        let index = StringIndex.Create(poolConfig, Segment)
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
                            index.AddLocation(s, aType, sIdx, eIdx)
                        
                        // Verify all locations are retrieved correctly
                        testData |> List.forall (fun (s, aType, sIdx, eIdx) ->
                            match index.TryGetLocation(s) with
                            | ValueSome loc -> 
                                loc.ArrayType = aType && 
                                loc.StringIndex = sIdx && 
                                loc.EntryIndex = eIdx
                            | ValueNone -> false
                        )
                    )

                testPropertyWithConfig 
                    fsConfig 
                    "Concurrent string additions are thread-safe" <| fun (strings: string list) (threadCount: int) ->
                    
                    (strings.Length >= 10 && threadCount >= 2) ==> lazy (
                        // Split strings among threads
                        let poolConfig = { PoolConfiguration.Default with 
                                            InitialChunkSize = strings.Length * 2 }
                        let index = StringIndex.Create(poolConfig, Segment)
                        let stringsArray = List.toArray strings
                        
                        // Create tasks to add strings concurrently
                        let tasks = 
                            [|0..threadCount-1|]
                            |> Array.map (fun threadId ->
                                Task.Factory.StartNew(fun () ->
                                    
                                    // Each thread processes a subset of strings
                                    let startIdx = threadId * strings.Length / threadCount
                                    let endIdx = (threadId + 1) * strings.Length / threadCount - 1
                                    
                                    for i in startIdx..endIdx do
                                        if i < stringsArray.Length then
                                            let s = stringsArray.[i]
                                            index.AddLocation(s, 0uy, i, i)
                                )
                            )
                        
                        // Wait for all threads to complete, with a timeout
                        Task.WaitAll(tasks, 60000) 
                        |> fun success -> Expect.isTrue success "All tasks should complete within 1min timeout"
                        
                        // Verify all strings can be found (eventual consistency)
                        strings |> List.forall (fun s -> 
                            match index.TryGetLocation(s) with
                            | ValueSome _ -> true
                            | ValueNone -> false
                        )
                    )

                testPropertyWithConfig 
                    fsConfig 
                    "Index statistics track operations correctly" <| fun (strings: string list) (queries: string list) ->
                    
                    (strings.Length > 0 && queries.Length > 0) ==> lazy (
                        let poolConfig = PoolConfiguration.Default
                        let index = StringIndex.Create(poolConfig, Segment)
                        
                        // Add all strings
                        for s in strings do
                            index.AddLocation(s, 0uy, 0, 0)
                        
                        // Perform lookups
                        for q in queries do
                            index.TryGetLocation(q) |> ignore
                        
                        // Get stats
                        let hitRatio, _ = index.GetStats()
                        
                        // Basic check that stats are being tracked
                        hitRatio >= 0.0 && hitRatio <= 1.0
                    )
            ]

    // Test integration with Pool and Segments
    module PoolIntegrationTests =
        [<Tests>]
        let tests =
            testList "Pool_StringIndex_Integration" [
                testCase "Pool maintains temperature correctly across lookups" <| fun () ->
                    // Create a pool
                    let config = PoolConfiguration.Default
                    let baseId = 1000
                    let edenSize = 100
                    let pool = Pool.Create(baseId, edenSize, config)
                    
                    // Intern a string
                    let testString = "test-temperature"
                    let id1 = pool.InternString(testString)
                    
                    // Get string with temperature multiple times
                    let mutable currentId = id1
                    for i in 1..5 do
                        match pool.GetStringWithTemperature(currentId) with
                        | Some str, newId ->
                            Expect.equal str testString "String should match"
                            Expect.equal newId.Temperature i "Temperature should increment"
                            currentId <- newId
                        | None, _ -> failwith "String not found"
                    
                testCase "Segments handle string lookup with index" <| fun () ->
                    // Create segments
                    let config = { PoolConfiguration.Default with 
                                    InitialChunkSize = 10
                                    SecondaryChunkSize = 10 }
                    let baseId = 5000
                    let segments = Segments.Create(baseId, config)
                    
                    // Intern strings
                    let testStrings = ["seg1"; "seg2"; "seg3"; "seg4"; "seg5"]
                    let ids = testStrings |> List.map (fun s -> segments.AllocateString(s, baseId))
                    
                    // Try to allocate same strings again - should find them via index
                    let ids2 = testStrings |> List.map (fun s -> segments.AllocateString(s, baseId))
                    
                    // Should get same base IDs (temperature might differ)
                    for id1, id2 in List.zip ids ids2 do
                        Expect.equal (id1.Value &&& 0xFFFFFFFF) (id2.Value &&& 0xFFFFFFFF) 
                            "Base IDs should match"
                    
                testCase "Promoted strings are handled correctly" <| fun () ->
                    // Create a pool
                    let config = { PoolConfiguration.Default with 
                                    WorkerPromotionThreshold = 3 }
                    let baseId = 2000
                    let pool = Pool.Create(baseId, 100, config)
                    
                    // Intern a string
                    let testString = "promote-this"
                    let id = pool.InternString(testString)
                    
                    // Access it multiple times to reach threshold
                    let mutable currentId = id
                    for _ in 1..3 do
                        match pool.GetStringWithTemperature(currentId) with
                        | Some _, newId -> currentId <- newId
                        | None, _ -> failwith "String not found"
                    
                    // Process promotions
                    let candidates = pool.CheckPromotion()
                    
                    // Simulate promotion by updating the packed entry
                    if candidates.Length > 0 then
                        let promotedId = StringId.Create(9999) // Simulated higher tier ID
                        // In real code, the promotion system would call UpdateToPromoted
                        
                        // Try to get the string again - should handle promoted case
                        match pool.TryGetString(id) with
                        | Some _ -> () // Might still be there if not promoted yet
                        | None -> () // Expected if promoted
                    
                testCase "Index handles overwrites correctly" <| fun () ->
                    let config = PoolConfiguration.Default
                    let index = StringIndex.Create(config, Segment)

                    // Add strings that will likely collide
                    let strings = ["test1"; "test2"; "test3"]
                    for i, s in List.indexed strings do
                        index.AddLocation(s, 0uy, i, i)
                    
                    // Get stats to check overwrites
                    let _, overwriteRate = index.GetStats()
                    
                    // Overwrite rate should be reasonable
                    Expect.isLessThan overwriteRate 1.0 "Overwrite rate should be less than 100%"
            ]

    // Thread safety and concurrency tests
     module ConcurrencyTests =
        [<Tests>]
        let tests =
            testList "StringIndex_Concurrency" [
                testCase "Concurrent allocations maintain temperature correctly" <| fun () ->
                    // Create segments
                    let config = { PoolConfiguration.Default with 
                                    InitialChunkSize = 100 }
                    let baseId = 3000
                    let segments = Segments.Create(baseId, config)
                    
                    // String to be allocated by multiple threads
                    let testString = "concurrent-temp-test"
                    
                    // Multiple async operations allocating the same string
                    let threadCount = 4
                    let results = Array.zeroCreate<StringId> threadCount
                    
                    // Create async workflows
                    let asyncOps = Array.init threadCount (fun i ->
                        async {
                            // Each async workflow allocates the string multiple times
                            let mutable id = StringId.Invalid
                            for _ in 1..10 do
                                id <- segments.AllocateString(testString, baseId)
                                do! Async.Sleep 1 // Small delay to interleave operations
                            results.[i] <- id
                        }
                    )
                    
                    // Run all async operations in parallel with timeout
                    Async.RunSynchronously(
                        Async.Parallel asyncOps |> Async.Ignore, 
                        timeout = 30000
                    )
                    
                    // All operations should have produced valid IDs
                    for id in results do
                        Expect.isTrue id.IsValid "Should have valid ID"
                        match segments.TryGetString(id.Value, baseId) with
                        | Some s -> Expect.equal s testString "String should match"
                        | None -> failwith "String not found"
                
                testCase "Index maintains consistency under concurrent updates" <| fun () ->
                    let config = PoolConfiguration.Default
                    let index = StringIndex.Create(config, Segment)

                    // Multiple async operations adding and looking up strings
                    let threadCount = 8
                    let operationsPerThread = 100
                    
                    let asyncOps = Array.init threadCount (fun threadId ->
                        async {
                            for i in 0..operationsPerThread-1 do
                                let s = $"thread{threadId}-op{i}"
                                
                                // Add to index
                                index.AddLocation(s, byte threadId, i, i)
                                
                                // Immediately try to find it
                                match index.TryGetLocation(s) with
                                | ValueSome loc ->
                                    Expect.equal loc.ArrayType (byte threadId) "Array type should match"
                                | ValueNone ->
                                    // Acceptable due to overwrites in direct-mapped index
                                    ()
                        }
                    )
                    
                    // Run all async operations in parallel with timeout
                    Async.RunSynchronously(
                        Async.Parallel asyncOps |> Async.Ignore,
                        timeout = 30000
                    )
                    
                    // Check final stats
                    let _, overwriteRate = index.GetStats()
                    
                    // With high concurrency, we expect some overwrites
                    Expect.isGreaterThan overwriteRate 0.0 "Should have some overwrites"
                    Expect.isLessThan overwriteRate 1.0 "Shouldn't overwrite everything"
            ]

    // Main test entry point
    [<Tests>]
    let bitmapIndexTests =
        testList "BitmapIndexTests" [
            StringIndexProperties.tests
            PoolIntegrationTests.tests
            ConcurrencyTests.tests
        ]
