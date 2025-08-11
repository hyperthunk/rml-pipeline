namespace RMLPipeline.Tests

open System
open System.Threading
open System.Threading.Tasks
open System.Collections.Generic
open System.Collections.Concurrent
open System.Diagnostics
open Expecto
open Expecto.ExpectoFsCheck
open FsCheck
open RMLPipeline
open RMLPipeline.Core
open RMLPipeline.FastMap.Types
open RMLPipeline.Internal.StringPooling
open FSharp.HashCollections
open StringInterningGenerators

/// Low-level tests for the string interning components
module StringPoolTests =

    // Helper functions for testing
    module TestHelpers =
        // Run operations concurrently and wait for all to complete
        let runConcurrent (operations: (unit -> unit)[]) =
            let tasks = operations |> Array.map (fun op -> Task.Run(op))
            Task.WaitAll(tasks)
        
        // Execute a function with timeout and return result with success flag
        let executeWithTimeout<'T> (timeoutMs: int) (f: unit -> 'T) : bool * 'T option =
            let result = ref None
            let task = Task.Run(fun () -> result := Some(f()))
            
            if task.Wait(timeoutMs) then
                (true, !result)
            else
                (false, None)
        
        // Create segments for testing
        let createTestSegments (baseId: int) (config: PoolConfiguration) =
            Segments.Create(baseId, config)
        
        // Create pool for testing
        let createTestPool (baseId: int) (edenSize: int) (config: PoolConfiguration) =
            Pool.Create(baseId, edenSize, config)
        
        // Verifies a key invariant of the string pool: the same string should always get the same ID
        let verifyStringIdentity (internFn: string -> StringId) (strings: string[]) =
            // First intern all strings
            let initialIds = strings |> Array.map internFn
            
            // Then intern again and verify IDs match
            let secondIds = strings |> Array.map internFn
            
            // Verify identity property
            Array.zip initialIds secondIds
            |> Array.forall (fun (id1, id2) -> id1 = id2)
            
        // Verifies that round-trip string interning works
        let verifyRoundTrip (internFn: string -> StringId) (retrieveFn: StringId -> string option) (strings: string[]) =
            strings |> Array.forall (fun str ->
                let id = internFn str
                match retrieveFn id with
                | Some retrieved -> retrieved = str
                | None -> false
            )
        
        // Verify that temperature tracking increases with access
        let verifyTemperatureIncreases (segments: Segments) =
            // Helper to read temperature
            let getTemperature (str: string) =
                match segments.StringTemperatures.TryGetValue(str) with
                | true, temp -> temp
                | false, _ -> 0
            
            // Create test string
            let testString = "temperature_test_" + Guid.NewGuid().ToString()
            let baseId = 1000
            
            // Initial temperature should be 0
            let initialTemp = getTemperature testString
            
            // Intern string multiple times
            for _ in 1..10 do
                segments.AllocateString(testString, baseId) |> ignore
            
            // Get updated temperature
            let updatedTemp = getTemperature testString
            
            updatedTemp > initialTemp
            
        // Verify that temperature decay works
        let verifyTemperatureDecay (segments: Segments) =
            // Create test string
            let testString = "decay_test_" + Guid.NewGuid().ToString()
            let baseId = 1000
            
            // Intern string multiple times to increase temperature
            for _ in 1..20 do
                segments.AllocateString(testString, baseId) |> ignore
            
            // Get temperature before decay
            let beforeDecay = 
                match segments.StringTemperatures.TryGetValue(testString) with
                | true, temp -> temp
                | false, _ -> 0
            
            // Apply decay
            segments.DecayTemperatures 0.5
            
            // Get temperature after decay
            let afterDecay = 
                match segments.StringTemperatures.TryGetValue(testString) with
                | true, temp -> temp
                | false, _ -> 0
            
            afterDecay < beforeDecay && afterDecay > 0

    let poolConfig = { FsCheckConfig.defaultConfig with 
                        arbitrary = [ typeof<StringInterningArbitraries> ] }

    module SegmentsTests =
        module SingleThreaded =
            [<Tests>]
            let tests =
                testList "SingleThreadedSegmentsTests" [
                    testPropertyWithConfig 
                            poolConfig 
                            "AllocateString assigns unique IDs" <| fun (config: PoolConfiguration) ->
                        // Create segments
                        let baseId = 1000
                        use segments = TestHelpers.createTestSegments baseId config
                        
                        // Allocate multiple strings
                        let uniqueStrings = 
                            [| for i in 1..50 -> "test_string_" + i.ToString() |]
                        
                        let ids = uniqueStrings |> Array.map (fun s -> segments.AllocateString(s, baseId))
                        
                        // Verify all IDs are unique
                        ids.Length = (ids |> Array.distinct |> Array.length)
                    
                    testProperty "TryGetString retrieves correct string" <| fun (testString: string) ->
                        if String.IsNullOrEmpty(testString) then true else
                        
                        // Create segments
                        let baseId = 1000
                        use segments = TestHelpers.createTestSegments baseId PoolConfiguration.Default
                        
                        // Allocate string
                        let id = segments.AllocateString(testString, baseId)
                        
                        // Retrieve and verify
                        match segments.TryGetString(id.Value, baseId) with
                        | Some retrieved -> retrieved = testString
                        | None -> false
                    
                    testCase "New chunk allocation works correctly" <| fun () ->
                        // Create segments with small chunk size
                        let config = { 
                            PoolConfiguration.Default with 
                                InitialChunkSize = 10
                                SecondaryChunkSize = 20 
                        }
                        let baseId = 1000
                        use segments = TestHelpers.createTestSegments baseId config
                        
                        // Allocate more strings than fit in first chunk
                        let uniqueStrings = 
                            [| for i in 1..30 -> "chunk_test_" + i.ToString() |]
                        
                        let ids = uniqueStrings |> Array.map (fun s -> segments.AllocateString(s, baseId))
                        
                        // Verify all strings can be retrieved
                        let allRetrieved = 
                            Array.zip uniqueStrings ids
                            |> Array.forall (fun (str, id) ->
                                match segments.TryGetString(id.Value, baseId) with
                                | Some retrieved -> retrieved = str
                                | None -> false
                            )
                        
                        Expect.isTrue allRetrieved "All strings should be retrievable after chunk allocation"
                        Expect.isGreaterThan segments.CurrentChunkIndex 0 "Should have allocated new chunks"
                    
                    testCase "Temperature tracking increases with access" <| fun () ->
                        use segments = TestHelpers.createTestSegments 1000 PoolConfiguration.Default
                        Expect.isTrue (TestHelpers.verifyTemperatureIncreases segments) "Temperature should increase with access"
                    
                    testCase "Temperature decay works correctly" <| fun () ->
                        use segments = TestHelpers.createTestSegments 1000 PoolConfiguration.Default
                        Expect.isTrue (TestHelpers.verifyTemperatureDecay segments) "Temperature should decay correctly"
                    
                    testCase "CheckPromotion returns candidates when ready" <| fun () ->
                        // Create segments with short promotion interval
                        let config = { 
                            PoolConfiguration.Default with 
                                MinPromotionInterval = TimeSpan.FromMilliseconds(1.0)
                        }
                        let baseId = 1000
                        use segments = TestHelpers.createTestSegments baseId config
                        
                        // Set promotion threshold
                        segments.PromotionThreshold <- 5
                        
                        // Create hot string
                        let testString = "promotion_test"
                        segments.StringTemperatures.TryAdd(testString, 10) |> ignore
                        
                        // Signal promotion
                        Interlocked.Exchange(&segments.PromotionSignalCount, 1L) |> ignore
                        
                        // Check for promotion
                        let candidates = segments.CheckPromotion()
                        
                        Expect.isTrue (candidates.Length > 0) "Should find promotion candidates"
                        Expect.isTrue (candidates |> Array.exists (fun c -> c.Value = testString)) 
                            "Should include hot string in candidates"
                ]

        module MultiThreaded =
            [<Tests>]
            let tests =
                testList "MultiThreadedSegmentsTests" [
                    testCase "Concurrent allocation maintains consistency" <| fun () ->
                        // Create segments
                        let baseId = 1000
                        use segments = TestHelpers.createTestSegments baseId PoolConfiguration.Default
                        
                        // Track allocated strings and IDs
                        let results = ConcurrentDictionary<string, StringId>()
                        
                        // Define concurrent operations
                        let threadCount = 5
                        let stringsPerThread = 20
                        let operations = 
                            [| for threadId in 0..threadCount-1 ->
                                fun () ->
                                    for i in 0..stringsPerThread-1 do
                                        let str = $"thread_{threadId}_string_{i}"
                                        let id = segments.AllocateString(str, baseId)
                                        results.TryAdd(str, id) |> ignore
                            |]
                        
                        // Run concurrently
                        TestHelpers.runConcurrent operations
                        
                        // Verify all strings can be retrieved
                        let allRetrieved = 
                            results |> Seq.forall (fun kvp ->
                                match segments.TryGetString(kvp.Value.Value, baseId) with
                                | Some retrieved -> retrieved = kvp.Key
                                | None -> false
                            )
                        
                        Expect.isTrue allRetrieved "All strings should be retrievable after concurrent allocation"
                    
                    testCase "Concurrent promotion checking is thread-safe" <| fun () ->
                        // Create segments with short promotion interval
                        let config = { 
                            PoolConfiguration.Default with 
                                MinPromotionInterval = TimeSpan.FromMilliseconds(5.0)
                        }
                        let baseId = 1000
                        use segments = TestHelpers.createTestSegments baseId config
                        
                        // Set up for promotion
                        segments.PromotionThreshold <- 5
                        let testString = "concurrent_promotion_test"
                        segments.StringTemperatures.TryAdd(testString, 10) |> ignore
                        Interlocked.Exchange(&segments.PromotionSignalCount, 1L) |> ignore
                        
                        // Track how many threads get candidates
                        let successCount = ref 0
                        
                        // Define concurrent operations
                        let threadCount = 5
                        let operations = 
                            [| for _ in 1..threadCount ->
                                fun () ->
                                    let candidates = segments.CheckPromotion()
                                    if candidates.Length > 0 then
                                        Interlocked.Increment(successCount) |> ignore
                            |]
                        
                        // Run concurrently
                        TestHelpers.runConcurrent operations
                        
                        // Only one thread should get candidates (atomic timestamp)
                        Expect.equal !successCount 1 "Only one thread should get promotion candidates"
                ]

    // ==================== Pool Tests ====================
    module PoolTests =
        module SingleThreaded =
            
            [<Tests>]
            let tests =
                testList "SingleThreadedPoolTests" [
                    testPropertyWithConfig 
                            poolConfig
                            "InternString assigns sequential IDs in eden space" <| fun (config: PoolConfiguration) ->
                        // Create pool with reasonable eden size
                        let baseId = 1000
                        printfn "Test Configuration: %A" config
                        let edenSize = min config.GlobalEdenSize 100 // Keep test manageable
                        let pool = TestHelpers.createTestPool baseId edenSize config
                        
                        // Intern strings less than eden size
                        let strings = [| for i in 1..edenSize/2 -> $"eden_string_{i}" |]
                        let ids = strings |> Array.map pool.InternString
                        
                        // Verify IDs are sequential and in expected range
                        let expectedIds = [| for i in 0..strings.Length-1 -> StringId.Create(baseId + i) |]
                        ids = expectedIds
                    
                    testProperty "Round-trip string interning works" <| fun (strings: string[]) ->
                        if strings.Length = 0 then true else
                        
                        // Create pool
                        let baseId = 1000
                        let edenSize = 100
                        let pool = TestHelpers.createTestPool baseId edenSize PoolConfiguration.Default
                        
                        // Filter out null/empty strings
                        let validStrings = 
                            strings |> Array.filter (not << String.IsNullOrEmpty)
                                   |> Array.truncate 50  // Keep test manageable
                        
                        if validStrings.Length = 0 then true else
                        
                        // Verify round trip
                        TestHelpers.verifyRoundTrip pool.InternString pool.TryGetString validStrings
                    
                    testCase "Post-eden allocation works when eden is full" <| fun () ->
                        // Create pool with small eden size
                        let baseId = 1000
                        let edenSize = 10
                        let pool = TestHelpers.createTestPool baseId edenSize PoolConfiguration.Default
                        
                        // Fill eden space
                        for i in 1..edenSize do
                            pool.InternString($"eden_string_{i}") |> ignore
                        
                        // Intern string that should go to post-eden
                        let postEdenString = "post_eden_string"
                        let id = pool.InternString(postEdenString)
                        
                        // Verify string can be retrieved
                        match pool.TryGetString(id) with
                        | Some retrieved -> Expect.equal retrieved postEdenString "Should retrieve post-eden string"
                        | None -> Expect.isTrue false "Should retrieve post-eden string"
                        
                        // Verify ID is in post-eden range
                        Expect.isGreaterThanOrEqual id.Value (baseId + edenSize) "ID should be in post-eden range"
                    
                    testCase "CheckPromotion returns hot candidates" <| fun () ->
                        // Create pool with short promotion interval
                        let config = { 
                            PoolConfiguration.Default with 
                                MinPromotionInterval = TimeSpan.FromMilliseconds 1.0
                        }
                        let baseId = 1000
                        let edenSize = 10
                        let pool = TestHelpers.createTestPool baseId edenSize config
                        
                        // Create hot string in eden
                        let edenString = "hot_eden_string"
                        let _ = pool.InternString edenString
                        pool.EdenTemperatures.AddOrUpdate(edenString, 10, fun _ _ -> 10) |> ignore
                        Interlocked.Exchange(&pool.PromotionSignalCount, 1L) |> ignore
                        
                        // Create hot string in post-eden
                        let postEdenString = "hot_post_eden_string"
                        // First fill eden
                        for i in 1..edenSize do
                            pool.InternString($"eden_filler_{i}") |> ignore
                        // Then add post-eden string
                        let _ = pool.InternString(postEdenString)
                        pool.PostEden.StringTemperatures.AddOrUpdate(postEdenString, 10, fun _ _ -> 10) |> ignore
                        Interlocked.Exchange(&pool.PostEden.PromotionSignalCount, 1L) |> ignore
                        
                        // Check for promotion
                        let candidates = pool.CheckPromotion()
                        
                        Expect.isTrue (candidates.Length > 0) "Should find promotion candidates"
                        let candidateStrings = candidates |> Array.map (fun c -> c.Value)
                        Expect.isTrue (Array.contains edenString candidateStrings || 
                                       Array.contains postEdenString candidateStrings)
                            "Candidates should include hot strings from eden or post-eden"
                ]

        module MultiThreaded =
            [<Tests>]
            let tests =
                testList "MultiThreadedPoolTests" [
                    testCase "Concurrent interning maintains consistency" <| fun () ->
                        // Create pool
                        let baseId = 1000
                        let edenSize = 50
                        let pool = TestHelpers.createTestPool baseId edenSize PoolConfiguration.Default
                        
                        // Track results
                        let results = ConcurrentDictionary<string, StringId>()
                        
                        // Define concurrent operations
                        let threadCount = 5
                        let stringsPerThread = 20
                        let operations = 
                            [| for threadId in 0..threadCount-1 ->
                                fun () ->
                                    for i in 0..stringsPerThread-1 do
                                        let str = $"thread_{threadId}_string_{i}"
                                        let id = pool.InternString(str)
                                        results.TryAdd(str, id) |> ignore
                            |]
                        
                        // Run concurrently
                        TestHelpers.runConcurrent operations
                        
                        // Verify all strings can be retrieved and have consistent IDs
                        let allConsistent = ref true
                        
                        results |> Seq.iter (fun kvp ->
                            // Verify retrieval
                            match pool.TryGetString(kvp.Value) with
                            | Some retrieved when retrieved = kvp.Key -> ()
                            | _ -> allConsistent := false
                            
                            // Verify consistent ID on reintern
                            let reId = pool.InternString(kvp.Key)
                            if reId <> kvp.Value then
                                allConsistent := false
                        )
                        
                        Expect.isTrue !allConsistent "All strings should be retrievable and have consistent IDs"
                    
                    testCase "Concurrent promotion checking is thread-safe" <| fun () ->
                        // Create pool with short promotion interval
                        let config = { 
                            PoolConfiguration.Default with 
                                MinPromotionInterval = TimeSpan.FromMilliseconds(5.0)
                        }
                        let baseId = 1000
                        let edenSize = 10
                        let pool = TestHelpers.createTestPool baseId edenSize config
                        
                        // Create hot string
                        let testString = "concurrent_promotion_test"
                        let _ = pool.InternString(testString)
                        pool.EdenTemperatures.AddOrUpdate(testString, 10, fun _ _ -> 10) |> ignore
                        Interlocked.Exchange(&pool.PromotionSignalCount, 1L) |> ignore
                        
                        // Track successful promotion checks
                        let successCount = ref 0
                        
                        // Define concurrent operations
                        let threadCount = 5
                        let operations = 
                            [| for _ in 1..threadCount ->
                                fun () ->
                                    let candidates = pool.CheckPromotion()
                                    if candidates.Length > 0 then
                                        Interlocked.Increment(successCount) |> ignore
                            |]
                        
                        // Run concurrently
                        TestHelpers.runConcurrent operations
                        
                        // Only one thread should get candidates
                        Expect.equal !successCount 1 "Only one thread should get promotion candidates"
                ]

    // ==================== LocalPool Tests ====================
    module LocalPoolTests =
        // Create test local pool
        let createTestLocalPool (globalPool: GlobalPool) (groupPool: GroupPool option) =
            let workerId = WorkerId.Create()
            let lpBaseId = IdAllocation.getLocalPoolBaseId 
                              (groupPool |> Option.map (fun p -> p.GroupId)) 
                              workerId
            {
                WorkerId = workerId
                GroupPool = groupPool
                GlobalPool = globalPool
                LocalStrings = HashMap.empty
                LocalArray = Array.create 100 Unchecked.defaultof<string>
                NextLocalId = 0
                LocalPoolBaseId = lpBaseId
                MaxSize = 100
                AccessOrder = LinkedList<string>()
                StringToNode = FastMap.empty
                StringTemperatures = FastMap.empty
                HotStrings = HashSet.empty
                LastDecayTime = DateTime.UtcNow
                LastPromotionTime = Stopwatch.GetTimestamp()
                PromotionSignalCount = 0L
                Configuration = PoolConfiguration.Default
                Stats = { accessCount = 0L; missCount = 0L }
            }
        
        module SingleThreaded =
            [<Tests>]
            let tests =
                testList "SingleThreadedLocalPoolTests" [
                    testCase "Access pattern routing works correctly" <| fun () ->
                        // Create global pool with planning strings
                        let planningStrings = [| "planning1"; "planning2" |]
                        let globalPool = 
                            {
                                PlanningStrings = 
                                    planningStrings 
                                    |> Array.mapi (fun i s -> s, StringId i)
                                    |> Array.fold (fun acc (s, id) -> FastMap.add s id acc) FastMap.empty
                                PlanningArray = planningStrings
                                PlanningCount = planningStrings.Length
                                RuntimePool = TestHelpers.createTestPool planningStrings.Length 50 PoolConfiguration.Default
                                Stats = { accessCount = 0L; missCount = 0L }
                            }
                        
                        // Create group pool
                        let groupId = DependencyGroupId 1
                        let groupBaseId = IdAllocation.getGroupPoolBaseId groupId
                        let groupPool = {
                            GroupId = groupId
                            GlobalPool = globalPool
                            GroupPoolBaseId = groupBaseId
                            GroupPool = TestHelpers.createTestPool groupBaseId 50 PoolConfiguration.Default
                            CrossTierCache = ConcurrentDictionary<StringId, string>()
                            Stats = { accessCount = 0L; missCount = 0L }
                        }
                        
                        // Create local pool
                        let localPool = createTestLocalPool globalPool (Some groupPool)
                        
                        // Test routing for different access patterns
                        
                        // Planning should go to global pool
                        let planningStr = "planning1"
                        let planningId = localPool.InternString(planningStr, StringAccessPattern.Planning)
                        Expect.equal planningId.Value 0 "Planning string should get global planning ID"
                        
                        // High frequency should stay local
                        let highFreqStr = "high_freq_string"
                        let highFreqId = localPool.InternString(highFreqStr, StringAccessPattern.HighFrequency)
                        Expect.isGreaterThanOrEqual highFreqId.Value localPool.LocalPoolBaseId 
                            "High frequency string should get local ID"
                        
                        // Medium/Low frequency should go to group pool
                        let mediumFreqStr = "medium_freq_string"
                        let mediumFreqId = localPool.InternString(mediumFreqStr, StringAccessPattern.MediumFrequency)
                        Expect.isGreaterThanOrEqual mediumFreqId.Value groupBaseId 
                            "Medium frequency string should get group ID"
                        Expect.isLessThan mediumFreqId.Value localPool.LocalPoolBaseId 
                            "Medium frequency string ID should be less than local base ID"
                    
                    testCase "LRU eviction works when capacity is reached" <| fun () ->
                        // Create local pool with small capacity
                        let globalPool = {
                            PlanningStrings = FastMap.empty
                            PlanningArray = [||]
                            PlanningCount = 0
                            RuntimePool = TestHelpers.createTestPool 0 50 PoolConfiguration.Default
                            Stats = { accessCount = 0L; missCount = 0L }
                        }
                        
                        let localPool = createTestLocalPool globalPool None
                        localPool.MaxSize <- 5 // Set small capacity
                        
                        // Fill local pool
                        for i in 1..5 do
                            localPool.InternString($"local_string_{i}", StringAccessPattern.HighFrequency) |> ignore
                        
                        // Verify all strings are present
                        for i in 1..5 do
                            let str = $"local_string_{i}"
                            match FastMap.tryFind str localPool.LocalStrings with
                            | ValueSome _ -> ()
                            | ValueNone -> failwith $"String {str} should be in local pool"
                        
                        // Update LRU order by accessing some strings
                        localPool.InternString("local_string_1", StringAccessPattern.HighFrequency) |> ignore
                        localPool.InternString("local_string_3", StringAccessPattern.HighFrequency) |> ignore
                        
                        // Add new string that should trigger eviction
                        localPool.InternString("new_string", StringAccessPattern.HighFrequency) |> ignore
                        
                        // Verify LRU eviction (string_2 or string_4 should be evicted)
                        let evicted = 
                            [2; 4; 5] |> List.exists (fun i ->
                                let str = $"local_string_{i}"
                                match FastMap.tryFind str localPool.LocalStrings with
                                | ValueSome _ -> false
                                | ValueNone -> true
                            )
                        
                        Expect.isTrue evicted "An LRU string should be evicted"
                        
                        // Verify new string is present
                        match FastMap.tryFind "new_string" localPool.LocalStrings with
                        | ValueSome _ -> ()
                        | ValueNone -> failwith "New string should be in local pool"
                    
                    testCase "Temperature tracking identifies hot strings" <| fun () ->
                        // Create local pool
                        let globalPool = {
                            PlanningStrings = FastMap.empty
                            PlanningArray = [||]
                            PlanningCount = 0
                            RuntimePool = TestHelpers.createTestPool 0 50 PoolConfiguration.Default
                            Stats = { accessCount = 0L; missCount = 0L }
                        }
                        
                        let localPool = createTestLocalPool globalPool None
                        
                        // Set lower threshold for testing
                        localPool.Configuration <- 
                            { localPool.Configuration with WorkerPromotionThreshold = 5 }
                        
                        // Create test string
                        let testString = "temperature_test"
                        
                        // Access multiple times to increase temperature
                        for _ in 1..10 do
                            localPool.InternString(testString, StringAccessPattern.HighFrequency) |> ignore
                        
                        // Verify temperature is tracked
                        match FastMap.tryFind testString localPool.StringTemperatures with
                        | ValueSome temp -> 
                            Expect.isGreaterThan temp 0 "Temperature should be greater than 0"
                        | ValueNone -> 
                            failwith "String temperature should be tracked"
                        
                        // Verify string is marked as hot
                        Expect.isTrue (HashSet.contains testString localPool.HotStrings)
                            "String should be marked as hot"
                        
                        // Verify promotion signal is set
                        Expect.isGreaterThan localPool.PromotionSignalCount 0L
                            "Promotion signal should be set"
                    
                    testCase "CheckPromotion identifies and promotes hot strings" <| fun () ->
                        // Create pools
                        let globalPool = {
                            PlanningStrings = FastMap.empty
                            PlanningArray = [||]
                            PlanningCount = 0
                            RuntimePool = TestHelpers.createTestPool 0 50 PoolConfiguration.Default
                            Stats = { accessCount = 0L; missCount = 0L }
                        }
                        
                        let groupId = DependencyGroupId 1
                        let groupBaseId = IdAllocation.getGroupPoolBaseId groupId
                        let groupPool = {
                            GroupId = groupId
                            GlobalPool = globalPool
                            GroupPoolBaseId = groupBaseId
                            GroupPool = TestHelpers.createTestPool groupBaseId 50 PoolConfiguration.Default
                            CrossTierCache = ConcurrentDictionary<StringId, string>()
                            Stats = { accessCount = 0L; missCount = 0L }
                        }
                        
                        let localPool = createTestLocalPool globalPool (Some groupPool)
                        
                        // Set lower threshold for testing
                        localPool.Configuration <- 
                            { localPool.Configuration with 
                                WorkerPromotionThreshold = 3
                                MinPromotionInterval = TimeSpan.FromMilliseconds(1.0) }
                        
                        // Create hot string
                        let hotString = "hot_promotion_test"
                        localPool.InternString(hotString, StringAccessPattern.HighFrequency) |> ignore
                        
                        // Make it hot enough for promotion
                        localPool.StringTemperatures <- 
                            FastMap.add hotString 5 localPool.StringTemperatures
                        localPool.HotStrings <- HashSet.add hotString localPool.HotStrings
                        localPool.PromotionSignalCount <- 1L
                        
                        // Check promotion
                        localPool.CheckPromotion()
                        
                        // Verify string is promoted to group pool
                        let groupId = groupPool.InternString(hotString)
                        Expect.isGreaterThanOrEqual groupId.Value groupBaseId 
                            "String should be promoted to group pool"
                        Expect.isLessThan groupId.Value localPool.LocalPoolBaseId 
                            "Promoted string ID should be less than local base ID"
                ]

        module MultiThreaded =
            // This section is intentionally left empty as LocalPool is designed
            // to be used by a single thread (worker) at a time
            [<Tests>]
            let tests =
                testList "MultiThreadedLocalPoolTests" []

    // ==================== Promotion Tests ====================
    module PromotionTests =
        [<Tests>]
        let tests =
            testList "PromotionTests" [
                testCase "Worker to Group promotion works correctly" <| fun () ->
                    // Create pools
                    let globalPool = {
                        PlanningStrings = FastMap.empty
                        PlanningArray = [||]
                        PlanningCount = 0
                        RuntimePool = TestHelpers.createTestPool 0 50 PoolConfiguration.Default
                        Stats = { accessCount = 0L; missCount = 0L }
                    }
                    
                    let groupId = DependencyGroupId 1
                    let groupBaseId = IdAllocation.getGroupPoolBaseId groupId
                    let groupPool = {
                        GroupId = groupId
                        GlobalPool = globalPool
                        GroupPoolBaseId = groupBaseId
                        GroupPool = TestHelpers.createTestPool groupBaseId 50 PoolConfiguration.Default
                        CrossTierCache = ConcurrentDictionary<StringId, string>()
                        Stats = { accessCount = 0L; missCount = 0L }
                    }
                    
                    let localPool = LocalPoolTests.createTestLocalPool globalPool (Some groupPool)
                    
                    // Configure for quick promotion
                    let config = {
                        PoolConfiguration.Default with
                            WorkerPromotionThreshold = 3
                            MinPromotionInterval = TimeSpan.FromMilliseconds(1.0)
                    }
                    localPool.Configuration <- config
                    
                    // Create string and make it hot
                    let testString = "promotion_test_worker_to_group"
                    let localId = localPool.InternString(testString, StringAccessPattern.HighFrequency)
                    
                    // Verify it's initially in local pool
                    Expect.isGreaterThanOrEqual localId.Value localPool.LocalPoolBaseId 
                        "String should initially be in local pool"
                    
                    // Make it hot by accessing repeatedly
                    for _ in 1..10 do
                        localPool.InternString(testString, StringAccessPattern.HighFrequency) |> ignore
                    
                    // Manually trigger promotion check
                    localPool.CheckPromotion()
                    
                    // Verify string is now in group pool
                    let groupId = groupPool.InternString(testString)
                    Expect.isGreaterThanOrEqual groupId.Value groupBaseId 
                        "String should be promoted to group pool"
                    Expect.isLessThan groupId.Value localPool.LocalPoolBaseId 
                        "Promoted string ID should be less than local base ID"
                    
                    // Verify temperature is reset after promotion
                    match FastMap.tryFind testString localPool.StringTemperatures with
                    | ValueSome temp -> Expect.equal temp 0 "Temperature should be reset after promotion"
                    | ValueNone -> () // Also acceptable if fully removed
                
                testCase "Group to Global promotion works correctly" <| fun () ->
                    // Create pools with short promotion interval
                    let config = {
                        PoolConfiguration.Default with
                            GroupPromotionThreshold = 3
                            MinPromotionInterval = TimeSpan.FromMilliseconds(1.0)
                    }
                    
                    let globalPool = {
                        PlanningStrings = FastMap.empty
                        PlanningArray = [||]
                        PlanningCount = 0
                        RuntimePool = TestHelpers.createTestPool 0 50 config
                        Stats = { accessCount = 0L; missCount = 0L }
                    }
                    
                    let groupId = DependencyGroupId 1
                    let groupBaseId = IdAllocation.getGroupPoolBaseId groupId
                    let groupPool = {
                        GroupId = groupId
                        GlobalPool = globalPool
                        GroupPoolBaseId = groupBaseId
                        GroupPool = TestHelpers.createTestPool groupBaseId 50 config
                        CrossTierCache = ConcurrentDictionary<StringId, string>()
                        Stats = { accessCount = 0L; missCount = 0L }
                    }
                    
                    // Create test string in group pool
                    let testString = "promotion_test_group_to_global"
                    let initialId = groupPool.InternString(testString)
                    
                    // Verify it's initially in group pool
                    Expect.isGreaterThanOrEqual initialId.Value groupBaseId 
                        "String should initially be in group pool"
                    
                    // Make it hot enough for promotion
                    groupPool.GroupPool.EdenTemperatures.AddOrUpdate(
                        testString, 10, fun _ _ -> 10) |> ignore
                    Interlocked.Exchange(&groupPool.GroupPool.PromotionSignalCount, 1L) |> ignore
                    
                    // Check promotion
                    groupPool.CheckPromotion()
                    
                    // Verify string is now in global pool
                    let globalId = globalPool.InternString(testString)
                    Expect.isLessThan globalId.Value IdAllocation.GroupPoolBase 
                        "String should be promoted to global pool"
                
                testCase "StringPoolHierarchy CheckAndPromoteHotStrings works" <| fun () ->
                    // Create hierarchy with test configuration
                    let config = {
                        PoolConfiguration.Default with
                            GroupPromotionThreshold = 3
                            WorkerPromotionThreshold = 3
                            MinPromotionInterval = TimeSpan.FromMilliseconds(1.0)
                    }
                    let hierarchy = StringPoolHierarchy.Create([||], config)
                    
                    // Create group pool
                    let groupId = DependencyGroupId 1
                    let groupPool = hierarchy.GetOrCreateGroupPool(groupId)
                    
                    // Make a string hot in group pool
                    let testString = "hierarchy_promotion_test"
                    let _ = groupPool.InternString(testString)
                    
                    // Make it hot enough for promotion
                    groupPool.GroupPool.EdenTemperatures.AddOrUpdate(
                        testString, 10, fun _ _ -> 10) |> ignore
                    Interlocked.Exchange(&groupPool.GroupPool.PromotionSignalCount, 1L) |> ignore
                    
                    // Check and promote hot strings
                    let promotedCount = StringPool.checkAndPromoteHotStrings hierarchy
                    
                    // Verify some strings were promoted
                    Expect.isGreaterThan promotedCount 0 "Some strings should be promoted"
                    
                    // Verify string is now in global pool
                    let globalId = hierarchy.GlobalPool.InternString(testString)
                    Expect.isLessThan globalId.Value IdAllocation.GroupPoolBase 
                        "String should be promoted to global pool"
                
                testCase "Incremental promotion is integrated with normal operations" <| fun () ->
                    // Create hierarchy with test configuration
                    let config = {
                        PoolConfiguration.Default with
                            GroupPromotionThreshold = 5
                            WorkerPromotionThreshold = 5
                            MinPromotionInterval = TimeSpan.FromMilliseconds(1.0)
                    }
                    let hierarchy = StringPoolHierarchy.Create([||], config)
                    
                    // Create execution context
                    use scope = StringPool.createScope hierarchy (Some (DependencyGroupId 1)) None
                    
                    // Create test string and access it repeatedly
                    let testString = "integrated_promotion_test"
                    
                    // Intern with high frequency to keep in local pool
                    for _ in 1..10 do
                        scope.InternString(testString, StringAccessPattern.HighFrequency) |> ignore
                    
                    // Check string is in local pool initially
                    let localId = scope.InternString(testString, StringAccessPattern.HighFrequency)
                    let localPoolBaseId = scope.Context.ContextPool.LocalPoolBaseId
                    Expect.isGreaterThanOrEqual localId.Value localPoolBaseId 
                        "String should initially be in local pool"
                    
                    // Intern with medium frequency to promote to group
                    for _ in 1..20 do
                        scope.InternString(testString, StringAccessPattern.MediumFrequency) |> ignore
                    
                    // Check string is promoted to group or global
                    let finalId = scope.InternString(testString)
                    Expect.isLessThan finalId.Value localPoolBaseId 
                        "String should be promoted to higher tier"
            ]

    // ==================== Hierarchy Integration Tests ====================
    module HierarchyTests =
        [<Tests>]
        let tests =
            testList "HierarchyTests" [
                testCase "StringPoolHierarchy creates all required components" <| fun () ->
                    // Create hierarchy
                    let planningStrings = [| "planning1"; "planning2" |]
                    let hierarchy = StringPoolHierarchy.Create(planningStrings)
                    
                    // Verify global pool is initialized
                    Expect.equal hierarchy.GlobalPool.PlanningCount planningStrings.Length 
                        "Planning strings count should match"
                    
                    // Create group pool and verify
                    let groupId = DependencyGroupId 1
                    let groupPool = hierarchy.GetOrCreateGroupPool(groupId)
                    Expect.equal groupPool.GroupId groupId "Group ID should match"
                    
                    // Create execution context and verify
                    let context = StringPool.createContext hierarchy (Some groupId) None
                    Expect.equal context.GroupId (Some groupId) "Group ID should match"
                
                testCase "PoolContextScope provides RAII pattern" <| fun () ->
                    // Create hierarchy
                    let hierarchy = StringPoolHierarchy.Create([||])
                    
                    // Use scope to intern strings
                    let testString = "scope_test"
                    let testId = 
                        use scope = StringPool.createScope hierarchy None None
                        scope.InternString(testString)
                    
                    // Verify string can still be retrieved after scope disposed
                    use scope2 = StringPool.createScope hierarchy None None
                    match scope2.GetString(testId) with
                    | ValueSome str -> Expect.equal str testString "String should be retrievable after scope disposed"
                    | ValueNone -> failwith "String should be retrievable after scope disposed"
                
                testCase "Cross-tier string access works correctly" <| fun () ->
                    // Create hierarchy
                    let planningStrings = [| "planning_string" |]
                    let hierarchy = StringPoolHierarchy.Create(planningStrings)
                    
                    // Create multiple execution contexts
                    use globalScope = StringPool.createScope hierarchy None None
                    use groupScope = StringPool.createScope hierarchy (Some (DependencyGroupId 1)) None
                    use workerScope = StringPool.createScope hierarchy (Some (DependencyGroupId 1)) None
                    
                    // Intern strings at different tiers
                    let planningId = globalScope.InternPlanningString("planning_string")
                    let globalId = globalScope.InternString("global_string")
                    let groupId = groupScope.InternString("group_string", StringAccessPattern.MediumFrequency)
                    let workerId = workerScope.InternString("worker_string", StringAccessPattern.HighFrequency)
                    
                    // Verify all strings can be accessed from any context
                    let contexts = [globalScope; groupScope; workerScope]
                    
                    for scope in contexts do
                        // Planning string
                        match scope.GetString(planningId) with
                        | ValueSome str -> Expect.equal str "planning_string" "Planning string should be accessible"
                        | ValueNone -> failwith "Planning string should be accessible"
                        
                        // Global string
                        match scope.GetString(globalId) with
                        | ValueSome str -> Expect.equal str "global_string" "Global string should be accessible"
                        | ValueNone -> failwith "Global string should be accessible"
                        
                        // Group string
                        match scope.GetString(groupId) with
                        | ValueSome str -> Expect.equal str "group_string" "Group string should be accessible"
                        | ValueNone -> failwith "Group string should be accessible"
                        
                        // Worker string - only accessible from worker context
                        match scope.GetString(workerId) with
                        | ValueSome str -> 
                            if scope = workerScope then
                                Expect.equal str "worker_string" "Worker string should be accessible from worker"
                            else
                                failwith "Worker string should not be accessible from other contexts"
                        | ValueNone -> 
                            if scope = workerScope then
                                failwith "Worker string should be accessible from worker"
                
                testCase "Temperature decay works across hierarchy" <| fun () ->
                    // Create hierarchy with short decay interval
                    let config = {
                        PoolConfiguration.Default with
                            TemperatureDecayFactor = 0.5
                            DecayInterval = TimeSpan.FromMilliseconds(1.0)
                    }
                    let hierarchy = StringPoolHierarchy.Create([||], config)
                    
                    // Create execution context
                    use scope = StringPool.createScope hierarchy None None
                    
                    // Create test string and access it repeatedly
                    let testString = "decay_test"
                    for _ in 1..10 do
                        scope.InternString(testString, StringAccessPattern.HighFrequency) |> ignore
                    
                    // Get stats before decay
                    let statsBefore = scope.GetStats()
                    
                    // Force decay
                    StringPool.decayTemperatures hierarchy 0.1
                    
                    // Get stats after decay
                    let statsAfter = scope.GetStats()
                    
                    // Verify temperature decreased
                    Expect.isLessThan statsAfter.AverageTemperature statsBefore.AverageTemperature
                        "Temperature should decrease after decay"
            ]

    // Main test entry point
    [<Tests>]
    let allTests =
        testList "StringPoolTests" [
            SegmentsTests.SingleThreaded.tests
            SegmentsTests.MultiThreaded.tests
            PoolTests.SingleThreaded.tests
            PoolTests.MultiThreaded.tests
            LocalPoolTests.SingleThreaded.tests
            LocalPoolTests.MultiThreaded.tests
            PromotionTests.tests
            HierarchyTests.tests
        ]
    