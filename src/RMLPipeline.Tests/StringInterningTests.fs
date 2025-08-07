namespace RMLPipeline.Tests

open Expecto
open FsCheck
open System
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open System.Diagnostics
open RMLPipeline
open RMLPipeline.Core
open RMLPipeline.Model
open RMLPipeline.DSL
open RMLPipeline.Internal

(*
AIMS

1. Concurrency Testing
Multi-threaded Workers: Simulates real async/await patterns with configurable worker counts
Dependency Groups: Tests bounded contention within groups vs independent groups
Performance Metrics: Latency, throughput, memory usage, hit ratios
Thread Safety Validation: Detects race conditions and data corruption

2. Property-Based Behaviour Tests
String Identity: Same strings return same IDs across contexts
Round-trip Consistency: Intern → retrieve cycle preserves strings
Access Pattern Routing: Different patterns for same string work correctly
Group Isolation: Dependency groups maintain proper separation
Statistics Consistency: Counters work correctly under concurrent load

3. Performance, Stress, and Limit Testing
Global Pool Performance: Pre-populated planning strings are lock-free
Local Pool Limits: Capacity limits and overflow behavior
Memory Tracking: Usage increases appropriately
Complex RML Mappings: Real-world mapping configurations with joins
Unicode/Special Characters: Proper handling of diverse character sets
High-Frequency Access: Sustained concurrent operations
Memory Efficiency: Resource usage under load
Contention Analysis: High vs low contention scenarios
Scalability: Performance across different worker/group configurations

6. Error Handling and Edge Cases
Invalid IDs: Proper handling of corrupted StringIds
Large Strings: Memory and performance with large data
Context Disposal: Proper cleanup and resource management

7. Model-Based Testing
State Consistency: Verifies pool behavior matches expected semantics
Operation Sequences: Tests complex operation patterns
Invariant Checking: Ensures system properties are maintained

*)

module StringPoolTests =

    module Generators =
        
        // Helper to ensure strings are never null or empty
        let ensureNonEmpty (str: string) =
            if String.IsNullOrEmpty(str) then "default_string" else str
            
        let genShortString = 
            Gen.choose(1, 10) 
            |> Gen.map (fun len -> String.replicate len "a")
            |> Gen.map ensureNonEmpty
            
        let genMediumString = 
            Gen.choose(50, 200) 
            |> Gen.map (fun len -> String.replicate len "b")
            |> Gen.map ensureNonEmpty
            
        let genLongString = 
            Gen.choose(500, 2000) 
            |> Gen.map (fun len -> String.replicate len "c")
            |> Gen.map ensureNonEmpty
        
        let genCommonRMLStrings = Gen.elements [
            "http://example.org/person/"
            "http://xmlns.com/foaf/0.1/name"
            "http://xmlns.com/foaf/0.1/Person"
            "http://schema.org/name"
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
            "$.people[*]"
            "$.employees[*]"
            "{id}"
            "{name}"
            "{email}"
            "{age}"
            "JSONPath"
            "XPath"
            "CSV"
        ]
        
        let genJSONPaths = Gen.elements [
            "$.root"
            "$.people[*]"
            "$.employees[*].department"
            "$.data.items[*]"
            "$.nested.deep.value"
            "$.array[0].property"
            "$.complex[*].relationships[*]"
        ]
        
        let genTemplateStrings = Gen.elements [
            "http://example.org/person/{id}"
            "http://example.org/employee/{employeeId}"
            "{firstName} {lastName}"
            "Person_{id}_Profile"
            "Department_{deptId}"
            "http://company.org/{type}/{id}"
        ]
        
        let genRandomAsciiString = 
            Gen.choose(1, 100)
            >>= fun length ->
                Gen.listOfLength length (Gen.choose(33, 126)) // Start from 33 to avoid space as first char
                |> Gen.map (List.map char >> List.toArray >> String)
                |> Gen.map ensureNonEmpty
        
        let genStringVariants = 
            Gen.oneof [
                genShortString
                genMediumString
                genLongString
                genCommonRMLStrings
                genJSONPaths
                genTemplateStrings
                genRandomAsciiString
            ] |> Gen.map ensureNonEmpty

        let genReferenceFormulation = Gen.elements [JSONPath; XPath; CSV]
        
        let genTermType = Gen.elements [BlankNodeTerm; IRITerm; LiteralTerm; URITerm]
        
        let genRDFNode = Gen.oneof [
            genStringVariants |> Gen.map URI
            genStringVariants |> Gen.map Literal
            genStringVariants |> Gen.map BlankNode
        ]
        
        let genLogicalSource = gen {
            let! iterator = 
                Gen.oneof [Gen.constant None; genJSONPaths |> Gen.map Some]
            let! refForm = 
                Gen.oneof [Gen.constant None; genReferenceFormulation |> Gen.map Some]
            return { SourceIterator = iterator; SourceReferenceFormulation = refForm }
        }
        
        let genPredicateObjectMap = gen {
            let! predicateCount = Gen.choose(1, 3)
            let! objectCount = Gen.choose(1, 3)
            let! predicates = Gen.listOfLength predicateCount genStringVariants
            let! objects = Gen.listOfLength objectCount genRDFNode
            
            return buildPredicateObjectMap (predicateObjectMap {
                for pred in predicates do
                    do! addPredicate pred
                for obj in objects do
                    do! addObject obj
            })
        }
        
        let genTriplesMap = gen {
            let! logicalSrc = genLogicalSource
            let! subjectTemplate = genTemplateStrings
            let! pomCount = Gen.choose(1, 5)
            let! poms = Gen.listOfLength pomCount genPredicateObjectMap
            
            return buildTriplesMap (triplesMap {
                do! setLogicalSource (logicalSource {
                    match logicalSrc.SourceIterator with
                    | Some iter -> do! iterator iter
                    | None -> ()
                    match logicalSrc.SourceReferenceFormulation with
                    | Some JSONPath -> do! asJSONPath
                    | Some XPath -> do! asXPath
                    | Some CSV -> do! asCSV
                    | _ -> ()
                })
                do! setSubjectMap (subjectMap {
                    do! subjectTermMap (templateTermAsIRI subjectTemplate)
                })
                for pom in poms do
                    do! modify (fun tm -> { tm with PredicateObjectMap = pom :: tm.PredicateObjectMap })
            })
        }
        
        let genRMLMappingSet = gen {
            let! mapCount = Gen.choose(2, 10)
            let! maps = Gen.listOfLength mapCount genTriplesMap
            return maps |> List.toArray
        }
        
        let genStringAccessPattern = Gen.elements [
            StringAccessPattern.HighFrequency
            StringAccessPattern.MediumFrequency
            StringAccessPattern.LowFrequency
            StringAccessPattern.Planning
            StringAccessPattern.Runtime
            StringAccessPattern.HighFrequency ||| StringAccessPattern.Planning
            StringAccessPattern.MediumFrequency ||| StringAccessPattern.Runtime
        ]
        
        let genDependencyGroupId = Gen.choose(0, 5) |> Gen.map DependencyGroupId
        
        let genConcurrentAccessScenario = gen {
            let! workerCount = Gen.choose(2, 8)
            let! groupCount = Gen.choose(1, 3)
            let! operationsPerWorker = Gen.choose(10, 100)
            let! accessPattern = genStringAccessPattern
            return {| 
                WorkerCount = workerCount
                GroupCount = groupCount
                OperationsPerWorker = operationsPerWorker
                AccessPattern = accessPattern
            |}
        }
    
    // Add this to your StringPoolArbitraries type:
    type StringPoolArbitraries =
        static member StringVariants() = Arb.fromGen Generators.genStringVariants
        static member StringAccessPattern() = Arb.fromGen Generators.genStringAccessPattern        
        static member TriplesMap() = Arb.fromGen RMLCoreGenerators.genComplexTriplesMap
        static member TriplesMapArray() = Arb.fromGen RMLCoreGenerators.genTriplesMapArray
        static member TriplesMapCollection() = Arb.fromGen RMLCoreGenerators.genTriplesMapCollection 
        static member ConcurrentAccessScenario() = Arb.fromGen Generators.genConcurrentAccessScenario
        static member LogicalSource() = Arb.fromGen RMLCoreGenerators.genLogicalSource
        static member PredicateObjectMap() = Arb.fromGen RMLCoreGenerators.genPredicateObjectMap
        static member SubjectMap() = Arb.fromGen RMLCoreGenerators.genSubjectMap
        
        // Add these to prevent FsCheck from using reflection on core RML types:
        static member AbstractLogicalSource() = Arb.fromGen RMLCoreGenerators.genLogicalSource
        static member ExpressionMap() = Arb.fromGen RMLCoreGenerators.genExpressionMap
        static member TermMap() = Arb.fromGen RMLCoreGenerators.genTermMap
        static member ObjectMap() = Arb.fromGen RMLCoreGenerators.genObjectMap
        static member Join() = Arb.fromGen RMLCoreGenerators.genJoin
        
        // Override RDFNode to prevent recursive generation
        static member RDFNode() = Arb.fromGen (Gen.oneof [
            Generators.genStringVariants |> Gen.map URI
            Generators.genStringVariants |> Gen.map Literal  
            Generators.genStringVariants |> Gen.map BlankNode
        ])
        
        // Override enum types that might cause issues
        static member ReferenceFormulation() = Arb.fromGen (Gen.elements [JSONPath; XPath; CSV])
        static member TermType() = Arb.fromGen (Gen.elements [BlankNodeTerm; IRITerm; LiteralTerm; URITerm])

    // some types for maintaining test-run state
    type StringPoolOperation =
        | InternString of string * StringAccessPattern
        | GetString of StringId
        | GetStats
        | CreateContext of DependencyGroupId option
        | DisposeContext

    type StringPoolState = {
        Hierarchy: StringPoolHierarchy
        KnownStrings: Map<string, StringId>
        ActiveContexts: ExecutionContext list
        TotalOperations: int64
    }

    type ConcurrentTestResult = {
        TotalOperations: int64
        TotalErrors: int64
        AverageLatencyMs: float
        MaxLatencyMs: float
        MemoryUsageMB: float
        HitRatio: float
        ThreadSafetyViolations: int64
    }

    let extractStringsFromTriplesMap (triplesMap: TriplesMap) : string list =
        let mutable strings = []
        
        triplesMap.LogicalSource.SourceIterator 
        |> Option.iter (fun s -> strings <- s :: strings)
        
        triplesMap.SubjectMap |> Option.iter (fun sm ->
            sm.SubjectTermMap.ExpressionMap.Template 
            |> Option.iter (fun s -> strings <- s :: strings)
            sm.SubjectTermMap.ExpressionMap.Reference 
            |> Option.iter (fun s -> strings <- s :: strings)
            match sm.SubjectTermMap.ExpressionMap.Constant with
            | Some (URI u) -> strings <- u :: strings
            | Some (Literal l) -> strings <- l :: strings
            | Some (BlankNode b) -> strings <- b :: strings
            | None -> ()
            sm.Class |> List.iter (fun c -> strings <- c :: strings)
        )
        
        triplesMap.Subject |> Option.iter (fun s -> strings <- s :: strings)
        
        for pom in triplesMap.PredicateObjectMap do
            pom.Predicate |> List.iter (fun p -> strings <- p :: strings)
            pom.Object |> List.iter (fun obj ->
                match obj with
                | URI u -> strings <- u :: strings
                | Literal l -> strings <- l :: strings
                | BlankNode b -> strings <- b :: strings)
            
            for om in pom.ObjectMap do
                om.ObjectTermMap.ExpressionMap.Template 
                |> Option.iter (fun s -> strings <- s :: strings)
                om.ObjectTermMap.ExpressionMap.Reference 
                |> Option.iter (fun s -> strings <- s :: strings)
                om.Datatype 
                |> Option.iter (fun d -> strings <- d :: strings)
                om.Language 
                |> Option.iter (fun l -> strings <- l :: strings)
        
        triplesMap.BaseIRI 
        |> Option.iter (fun b -> strings <- b :: strings)
        
        strings 
        |> List.distinct
        |> List.filter (fun s -> not (String.IsNullOrEmpty(s))) // Additional safety filter

    let createPlanningStringsFromMappings (mappings: TriplesMap[]) : string[] =
        mappings
        |> Array.collect (extractStringsFromTriplesMap >> List.toArray)
        |> Array.distinct
        |> Array.filter (fun s -> not (String.IsNullOrEmpty(s))) // Additional safety filter

    let runConcurrentTest (hierarchy: StringPoolHierarchy) (scenario: {| WorkerCount: int; GroupCount: int; OperationsPerWorker: int; AccessPattern: StringAccessPattern |}) (testStrings: string[]) : Async<ConcurrentTestResult> =
        async {
            if testStrings.Length = 0 then
                failwith "testStrings cannot be empty"
                
            let errors = ref 0L
            let operations = ref 0L
            let latencies = ConcurrentBag<float>()
            let threadSafetyViolations = ref 0L
            
            let workers = Array.init scenario.WorkerCount (fun workerId -> async {
                try
                    let groupId = DependencyGroupId (workerId % scenario.GroupCount)
                    use scope = StringPool.createScope hierarchy (Some groupId) (Some 1000)
                    let context = scope.Context
                    
                    // Avoid getting a divide-by-zero error if we have fewer strings than workers
                    let stringsPerWorker = max 1 (testStrings.Length / scenario.WorkerCount)
                    let startIndex = (workerId * stringsPerWorker) % testStrings.Length
                    let stringSubset = 
                        if testStrings.Length <= scenario.WorkerCount then
                            // If we have fewer strings than workers, each worker uses all strings
                            testStrings
                        else
                            // Otherwise divide strings among workers
                            testStrings 
                            |> Array.skip startIndex
                            |> Array.take (min stringsPerWorker (testStrings.Length - startIndex))
                    
                    if stringSubset.Length = 0 then
                        failwith $"Worker {workerId} has no strings to work with"
                    
                    for i in 1..scenario.OperationsPerWorker do
                        try
                            let sw = Stopwatch.StartNew()

                            // Let's try and detect some potential races...
                            // 1. double allocation: Two threads simultaneously decide a string isn't in the pool
                            // 2. cache coherence: One thread's write to the internal map not being visible to another thread
                            // 3. lost updates: Updates to concurrent data structures being overwritten
                            // 4. inconsistent reads: Reading a string that was just interned by another thread

                            let stringToUse = stringSubset.[i % stringSubset.Length]
                            let id1 = scope.InternString(stringToUse, scenario.AccessPattern)
                            let id2 = scope.InternString(stringToUse, scenario.AccessPattern)
                            
                            // Verify consistency
                            if id1 <> id2 then
                                Interlocked.Increment threadSafetyViolations |> ignore

                            // Round Trip Races
                            // Torn reads/writes: Partial updates to string storage arrays
                            // Index corruption: StringId pointing to wrong array location due to concurrent modifications
                            // Memory reordering: CPU/compiler reordering causing reads to see inconsistent state
                            match scope.GetString(id1) with
                            | ValueSome retrievedString 
                                when retrievedString = stringToUse -> ()
                            | ValueSome _ -> 
                                Interlocked.Increment threadSafetyViolations |> ignore
                            | ValueNone -> 
                                Interlocked.Increment threadSafetyViolations |> ignore

                            sw.Stop()
                            latencies.Add sw.Elapsed.TotalMilliseconds
                            Interlocked.Increment operations |> ignore
                            
                            // Add some variability in timing
                            if i % 10 = 0 then
                                do! Async.Sleep(1)
                        with
                        | ex -> 
                            Interlocked.Increment errors |> ignore
                            printfn "Worker %d operation error: %s" workerId ex.Message
                with
                | ex ->
                    Interlocked.Increment errors |> ignore
                    printfn "Worker %d setup error: %s" workerId ex.Message
            })
            
            // Run all workers concurrently
            do! workers |> Async.Parallel |> Async.Ignore
            
            let stats = StringPool.getAggregateStats hierarchy
            let latencyArray = latencies.ToArray()

            printfn "stats: %A" stats
            
            return {
                TotalOperations = operations.Value
                TotalErrors = errors.Value
                AverageLatencyMs = 
                    if latencyArray.Length > 0 then 
                        Array.average latencyArray 
                    else 0.0
                MaxLatencyMs = 
                    if latencyArray.Length > 0 then 
                        Array.max latencyArray 
                    else 0.0
                MemoryUsageMB = float stats.MemoryUsageBytes / (1024.0 * 1024.0)
                HitRatio = 
                    if stats.AccessCount > 0L then 
                        float (stats.AccessCount - stats.MissCount) / float stats.AccessCount
                    else 0.0
                ThreadSafetyViolations = threadSafetyViolations.Value
            }
        }

    let propertyTests = [
        testProperty "String identity consistency across contexts" <| fun (strings: string[]) ->
            // The pool has a double-check in 'TryIntern', which we want to validate here
            // by looking for races in hierarchical pool routing logic:            
            // - Tier inconsistency: String ending up in different tiers for different contexts
            // - Routing races: Concurrent access to tier selection causing inconsistent decisions
            // - Context isolation violations: Changes in one context affecting another

            let validStrings = strings |> Array.filter (fun s -> not (String.IsNullOrEmpty(s)))
            (validStrings.Length > 0 && validStrings.Length <= 20) ==> lazy (
                let hierarchy = StringPool.create validStrings
                use scope1 = StringPool.createScope hierarchy None None
                use scope2 = StringPool.createScope hierarchy None None
                
                validStrings |> Array.forall (fun str ->
                    let id1 = scope1.InternString str 
                    let id2 = scope2.InternString str
                    id1 = id2
                )
            )

        testProperty "Round-trip string interning" <| fun (testStrings: string[]) ->
            let validStrings = testStrings |> Array.filter (fun s -> not (String.IsNullOrEmpty(s)))
            (validStrings.Length > 0 && validStrings.Length <= 50) ==> lazy (
                let planningStrings = 
                    validStrings |> Array.take (min 10 validStrings.Length)
                let hierarchy = StringPool.create planningStrings
                use scope = StringPool.createScope hierarchy None None

                validStrings |> Array.forall (fun originalStr ->
                    try
                        let id = scope.InternString(originalStr)
                        match scope.GetString(id) with
                        | ValueSome retrievedStr -> retrievedStr = originalStr
                        | ValueNone -> false
                    with
                    | _ -> false
                )
            )

        testProperty "Same access pattern returns consistent IDs" <| fun (str: string) (pattern: StringAccessPattern) ->
            (not (String.IsNullOrEmpty(str))) ==> lazy (
                let hierarchy = StringPool.create [||]
                use scope = StringPool.createScope hierarchy None None
                
                let id1 = scope.InternString(str, pattern)
                let id2 = scope.InternString(str, pattern)
                
                // Same string with same pattern should return same ID
                id1 = id2
            )

        testProperty "Access pattern routing optimizes for performance" <| fun (str: string) ->
            (not (String.IsNullOrEmpty(str))) ==> lazy (
                let hierarchy = StringPool.create [||]
                use scope = StringPool.createScope hierarchy None (Some 100) // Allow local pool
                
                // High-frequency should prefer local pools (when space available)
                let highFreqId = scope.InternString(str, StringAccessPattern.HighFrequency)
                
                // Planning should go to global pool
                let planningId = scope.InternString(str, StringAccessPattern.Planning)
                
                // Both should be retrievable and return the same string
                match scope.GetString(highFreqId), scope.GetString(planningId) with
                | ValueSome s1, ValueSome s2 -> s1 = str && s2 = str
                | _ -> false
            )

        testProperty "Dependency groups maintain isolation" <| fun (strings: string[]) ->
            let validStrings = strings |> Array.filter (fun s -> not (String.IsNullOrEmpty(s)))
            (validStrings.Length > 0 && validStrings.Length <= 30) ==> lazy (
                let hierarchy = StringPool.create [||]
                let group1 = DependencyGroupId 1
                let group2 = DependencyGroupId 2
                
                use scope1 = StringPool.createScope hierarchy (Some group1) None
                use scope2 = StringPool.createScope hierarchy (Some group2) None

                // Both groups should be able to intern the same strings
                validStrings |> Array.forall (fun str ->
                    try
                        let id1 = scope1.InternString(str, StringAccessPattern.MediumFrequency)
                        let id2 = scope2.InternString(str, StringAccessPattern.MediumFrequency)
                        
                        match scope1.GetString(id1), scope2.GetString(id2) with
                        | ValueSome s1, ValueSome s2 -> s1 = s2 && s1 = str
                        | _ -> false
                    with
                    | _ -> false
                )
            )

        testProperty "Statistics consistency under load" <| fun (strings: string[]) ->
            // A few more potential races here...
            // Lost increments: Counter updates being lost due to non-atomic operations
            // ABA problems: Counters appearing correct but having been corrupted and reset
            // Memory visibility: Updates to statistics not being visible across threads

            let validStrings = strings |> Array.filter (fun s -> not (String.IsNullOrEmpty(s)))
            (validStrings.Length > 0 && validStrings.Length <= 20) ==> lazy (
                let hierarchy = StringPool.create validStrings
                use scope = StringPool.createScope hierarchy None None

                let initialStats = scope.GetStats()
                
                validStrings |> Array.iter (fun str -> 
                    scope.InternString(str) |> ignore
                    scope.InternString(str) |> ignore  // Duplicate to test hit counting
                )
                
                let finalStats = scope.GetStats()
                
                finalStats.AccessCount >= initialStats.AccessCount
            )

        testCase "Concurrent access preserves string identity - fixed data" <| fun _ ->
            async {
                // Use more comprehensive simple mappings
                let simpleMappings = [|
                    buildTriplesMap (triplesMap {
                        do! setLogicalSource (logicalSource {
                            do! iterator "$.people[*]"
                            do! asJSONPath
                        })
                        do! setSubjectMap (subjectMap {
                            do! subjectTermMap (templateTermAsIRI "http://example.org/person/{id}")
                            do! addClass "http://xmlns.com/foaf/0.1/Person"
                        })
                        do! addPredicateObjectMap (predicateObjectMap {
                            do! addPredicate "http://xmlns.com/foaf/0.1/name"
                            do! addObject (Literal "John")
                        })
                    })
                    buildTriplesMap (triplesMap {
                        do! setLogicalSource (logicalSource {
                            do! iterator "$.employees[*]"
                            do! asJSONPath
                        })
                        do! setSubjectMap (subjectMap {
                            do! subjectTermMap (templateTermAsIRI "http://example.org/employee/{empId}")
                        })
                    })
                |]
                
                let planningStrings = createPlanningStringsFromMappings simpleMappings
                
                // Ensure we have enough strings for testing
                let testStrings = 
                    if planningStrings.Length < 10 then
                        // Add more test strings if planning strings are insufficient
                        Array.concat [
                            planningStrings
                            [| "test_string_1"; "test_string_2"; "test_string_3"; "test_string_4"; "test_string_5" |]
                        ]
                    else
                        planningStrings
                        
                let hierarchy = StringPool.create planningStrings
                
                let scenario = {| 
                    WorkerCount = 4
                    GroupCount = 2 
                    OperationsPerWorker = 20
                    AccessPattern = StringAccessPattern.HighFrequency 
                |}
                
                let! result = runConcurrentTest hierarchy scenario testStrings
                
                printfn "Test completed: Errors=%d, ThreadSafetyViolations=%d, TestStrings=%d" 
                    result.TotalErrors result.ThreadSafetyViolations testStrings.Length
                    
                Expect.equal result.ThreadSafetyViolations 0L "Should have no thread safety violations"
                Expect.equal result.TotalErrors 0L "Should have no errors"
            } |> Async.RunSynchronously

        testCase "Concurrent access preserves string identity with complex mappings" <| fun _ ->
            async {
                // Generate mappings manually using your non-recursive generators
                let sampleMappings = [|
                    Gen.sample 0 1 RMLCoreGenerators.genBasicTriplesMap |> List.head
                    Gen.sample 0 1 RMLCoreGenerators.genTriplesMapWithJoin |> List.head
                    Gen.sample 0 1 RMLCoreGenerators.genStringPoolStressTriplesMap |> List.head
                |]
                
                let planningStrings = createPlanningStringsFromMappings sampleMappings
                let hierarchy = StringPool.create planningStrings
                
                let scenario = {| 
                    WorkerCount = 4
                    GroupCount = 2 
                    OperationsPerWorker = 20
                    AccessPattern = StringAccessPattern.HighFrequency 
                |}
                
                let! result = runConcurrentTest hierarchy scenario planningStrings
                
                printfn "Complex mapping test: Errors=%d, ThreadSafetyViolations=%d, Strings=%d" 
                    result.TotalErrors result.ThreadSafetyViolations planningStrings.Length
                    
                Expect.equal result.ThreadSafetyViolations 0L "Should have no thread safety violations"
                Expect.equal result.TotalErrors 0L "Should have no errors"
            } |> Async.RunSynchronously

        // TODO: FsCheck just will NOT stop trying to generate a reflection based input for the
        // following property, which causes it to overflow the stack, since the TriplesMaps hierarchy
        // contains mutually recursive discriminated unions. Even an attempt to wrap the generators in
        // an out object seems doomed to fail... 

        (* ftestProperty "Concurrent access preserves string identity" <| 
            fun (collection: RMLCoreGenerators.TriplesMapCollection) ->
            let mappings = collection.Maps
            // In the hunt for races, let's try and overwhelm the system with concurrent operations
            // This should be skipped most of the time
            (mappings.Length > 0 && mappings.Length <= 5) ==> lazy (
                async {
                    let planningStrings = createPlanningStringsFromMappings mappings
                    let hierarchy = StringPool.create planningStrings
                    
                    let scenario = {| 
                        WorkerCount = 4
                        GroupCount = 2 
                        OperationsPerWorker = 20
                        AccessPattern = StringAccessPattern.HighFrequency 
                    |}
                    
                    let! result = runConcurrentTest hierarchy scenario planningStrings
                    
                    return result.ThreadSafetyViolations = 0L && result.TotalErrors = 0L
                } |> Async.RunSynchronously
            ) *)
    ]

    let bugfixTests = [
        testCase "Global pool pre-populated strings use fast path" <| fun _ ->
            let planningStrings = [|
                "http://xmlns.com/foaf/0.1/Person"
                "http://xmlns.com/foaf/0.1/name" 
                "$.people[*]"
                "{id}"
            |]
            
            let hierarchy = StringPool.create planningStrings
            use scope = StringPool.createScope hierarchy None None
            
            // Test 1: Planning strings should be found immediately
            planningStrings |> Array.iter (fun str ->
                let id = scope.InternString(str, StringAccessPattern.Planning)
                match scope.GetString(id) with
                | ValueSome retrievedStr -> Expect.equal retrievedStr str "Should retrieve planning string"
                | ValueNone -> failtest "Should find planning string"
            )
            
            // Test 2: Planning strings should have low StringIds (indicating global pool)
            planningStrings |> Array.iteri (fun i str ->
                let id = scope.InternString(str, StringAccessPattern.Planning)
                Expect.equal id.Value i $"Planning string '{str}' should have global pool ID {i}"
            )
            
            // Test 3: Statistics should show hits, not misses for planning strings
            let initialStats = scope.GetStats()
            planningStrings |> Array.iter (fun str ->
                scope.InternString(str, StringAccessPattern.Planning) |> ignore
            )
            let finalStats = scope.GetStats()
            
            Expect.isGreaterThan finalStats.AccessCount initialStats.AccessCount "Should register hits"
            Expect.equal finalStats.MissCount initialStats.MissCount "Should not register misses for planning strings"

        testCase "Worker local pool capacity limits" <| fun _ ->
            let hierarchy = StringPool.create [||]
            use scope = StringPool.createScope hierarchy None (Some 5) // Limit to 5 local strings
            
            // Fill local pool
            for i in 1..5 do
                let str = sprintf "local_string_%d" i
                let id = scope.InternString(str, StringAccessPattern.HighFrequency)
                Expect.isTrue id.IsValid "Should successfully intern local string"
            
            // Exceed local pool - should delegate to parent
            let overflowStr = "overflow_string"
            let overflowId = scope.InternString(overflowStr, StringAccessPattern.HighFrequency)
            
            match scope.GetString overflowId with
            | ValueSome retrievedStr -> Expect.equal retrievedStr overflowStr "Should handle overflow correctly"
            | ValueNone -> failtest "Should handle overflow string"

        testCase "Memory usage tracking" <| fun _ ->
            let hierarchy = StringPool.create [||]
            let initialStats = StringPool.getGlobalStats hierarchy
            
            use scope = StringPool.createScope hierarchy None None
            
            // Add some strings
            let testStrings = [|
                "http://example.org/test"
                "$.data.items[*]"
                "{firstName} {lastName}"
                "http://schema.org/Person"
            |]
            
            testStrings |> Array.iter (fun str ->
                scope.InternString(str) |> ignore
            )
            
            let finalStats = StringPool.getGlobalStats hierarchy
            Expect.isGreaterThan finalStats.MemoryUsageBytes initialStats.MemoryUsageBytes "Memory usage should increase"

        testCase "Complex RML mapping string extraction" <| fun _ ->
            let complexMapping = buildTriplesMap (triplesMap {
                do! setLogicalSource (logicalSource {
                    do! iterator "$.employees[*]"
                    do! asJSONPath
                })
                
                do! setSubjectMap (subjectMap {
                    do! subjectTermMap (templateTermAsIRI "http://company.org/employee/{employeeId}")
                    do! addClass "http://company.org/Employee"
                    do! addClass "http://xmlns.com/foaf/0.1/Person"
                })
                
                do! addPredicateObjectMap (predicateObjectMap {
                    do! addPredicate "http://xmlns.com/foaf/0.1/name"
                    do! addObjectMap (objectMap {
                        do! objectTermMap (refTermAsLiteral "fullName")
                    })
                })
                
                do! addPredicateObjectMap (predicateObjectMap {
                    do! addPredicate "http://company.org/startDate"
                    do! addObjectMap (objectMap {
                        do! objectTermMap (refTermAsLiteral "startDate")
                        do! datatype "http://www.w3.org/2001/XMLSchema#dateTime"
                    })
                })
            })
            
            let extractedStrings = extractStringsFromTriplesMap complexMapping
            let hierarchy = StringPool.create (extractedStrings |> List.toArray)
            
            use scope = StringPool.createScope hierarchy None None
            
            // Verify all extracted strings can be found
            extractedStrings |> List.iter (fun str ->
                let id = scope.InternString(str, StringAccessPattern.Planning)
                match scope.GetString(id) with
                | ValueSome retrievedStr -> Expect.equal retrievedStr str "Should find extracted string"
                | ValueNone -> failtest $"Should find extracted string: {str}"
            )

        testCase "Join condition string handling" <| fun _ ->
            let parentMapping = buildTriplesMap (triplesMap {
                do! setLogicalSource (logicalSource {
                    do! iterator "$.departments[*]"
                    do! asJSONPath
                })
                do! setSubjectMap (subjectMap {
                    do! subjectTermMap (templateTermAsIRI "http://company.org/department/{id}")
                })
                do! addPredicateObjectMap (predicateObjectMap {
                    do! addPredicate "http://company.org/departmentName"
                    do! addObjectMap (objectMap {
                        do! objectTermMap (refTermAsLiteral "name")
                    })
                })
            })
            
            let childMapping = buildTriplesMap (triplesMap {
                do! setLogicalSource (logicalSource {
                    do! iterator "$.employees[*]"
                    do! asJSONPath
                })
                do! setSubjectMap (subjectMap {
                    do! subjectTermMap (templateTermAsIRI "http://company.org/employee/{id}")
                })
                do! addPredicateObjectMap (predicateObjectMap {
                    do! addPredicate "http://company.org/worksInDepartment"
                    do! addRefObjectMap parentMapping (refObjectMap {
                        do! addJoinCondition (join {
                            do! child "departmentId"
                            do! parent "id"
                        })
                    })
                })
            })
            
            let allStrings = 
                [
                    extractStringsFromTriplesMap parentMapping
                    extractStringsFromTriplesMap childMapping
                    ["departmentId"; "id"] // Join condition strings
                ] |> List.concat |> List.distinct
            
            let hierarchy = StringPool.create (allStrings |> List.toArray)
            use scope = StringPool.createScope hierarchy None None
            
            allStrings |> List.iter (fun str ->
                let id = scope.InternString(str)
                Expect.isTrue id.IsValid $"Should successfully intern join-related string: {str}"
            )
    ]

    let performanceTests = [
        testCase "High-frequency access performance" <| fun _ ->
            // Test performance with new strings (low hit ratio expected)
            async {
                let hierarchy = StringPool.create [||]
                let testStrings = Array.init 100 (sprintf "high_freq_string_%d")
                
                let scenario = {| 
                    WorkerCount = 8
                    GroupCount = 1
                    OperationsPerWorker = 1000
                    AccessPattern = StringAccessPattern.Planning 
                |}
                
                let! result = runConcurrentTest hierarchy scenario testStrings
                
                Expect.equal result.TotalErrors 0L "Should have no errors"
                Expect.equal result.ThreadSafetyViolations 0L "Should have no thread safety violations"
                Expect.isLessThan result.AverageLatencyMs 1.0 "Average latency should be under 1ms"
                
                printfn "Performance Results - Ops: %d, Avg Latency: %.2fms, Hit Ratio: %.2f"
                    result.TotalOperations result.AverageLatencyMs result.HitRatio
            } |> Async.RunSynchronously

        testCase "Hit ratio with pre-populated strings" <| fun _ ->
            // Test hit ratio with known strings (high hit ratio expected)
            async {
                let testStrings = Array.init 50 (sprintf "known_string_%d")
                let hierarchy = StringPool.create testStrings
                
                let scenario = {| 
                    WorkerCount = 4
                    GroupCount = 1
                    OperationsPerWorker = 500
                    AccessPattern = StringAccessPattern.Planning 
                |}
                
                let! result = runConcurrentTest hierarchy scenario testStrings
                
                Expect.equal result.TotalErrors 0L "Should have no errors"
                Expect.equal result.ThreadSafetyViolations 0L "Should have no thread safety violations"
                Expect.isGreaterThan result.HitRatio 0.8 "Hit ratio should be high for known strings"
                
                printfn "Hit Ratio Results - Ops: %d, Hit Ratio: %.2f"
                    result.TotalOperations result.HitRatio
            } |> Async.RunSynchronously

        testCase "Memory efficiency under load" <| fun _ ->
            async {
                let planningStrings = Array.init 50 (sprintf "planning_string_%d")
                let hierarchy = StringPool.create planningStrings
                
                let testStrings = Array.init 200 (sprintf "runtime_string_%d")
                
                let scenario = {| 
                    WorkerCount = 4
                    GroupCount = 2
                    OperationsPerWorker = 500
                    AccessPattern = StringAccessPattern.MediumFrequency 
                |}
                
                let! result = runConcurrentTest hierarchy scenario testStrings
                
                Expect.equal result.TotalErrors 0L "Should have no errors"
                Expect.isLessThan result.MemoryUsageMB 50.0 "Memory usage should be reasonable"
                
                printfn "Memory Usage: %.2f MB for %d operations" result.MemoryUsageMB result.TotalOperations
            } |> Async.RunSynchronously

        testCase "Dependency group contention" <| fun _ ->
            async {
                let hierarchy = StringPool.create [||]
                let sharedStrings = Array.init 50 (sprintf "shared_string_%d")
                
                // Test with many workers in few groups (high contention)
                let highContentionScenario = {| 
                    WorkerCount = 12
                    GroupCount = 2  // Force contention
                    OperationsPerWorker = 200
                    AccessPattern = StringAccessPattern.MediumFrequency 
                |}
                
                let! highContentionResult = runConcurrentTest hierarchy highContentionScenario sharedStrings
                
                // Test with many groups (low contention)
                let lowContentionScenario = {| 
                    WorkerCount = 12
                    GroupCount = 6  // Spread across groups
                    OperationsPerWorker = 200
                    AccessPattern = StringAccessPattern.MediumFrequency 
                |}
                
                let! lowContentionResult = runConcurrentTest hierarchy lowContentionScenario sharedStrings
                
                Expect.equal highContentionResult.TotalErrors 0L "High contention should have no errors"
                Expect.equal lowContentionResult.TotalErrors 0L "Low contention should have no errors"
                Expect.equal highContentionResult.ThreadSafetyViolations 0L "High contention should be thread-safe"
                Expect.equal lowContentionResult.ThreadSafetyViolations 0L "Low contention should be thread-safe"
                
                printfn "High Contention - Avg Latency: %.2fms, Low Contention - Avg Latency: %.2fms"
                    highContentionResult.AverageLatencyMs lowContentionResult.AverageLatencyMs
            } |> Async.RunSynchronously
    ]

    let edgeCaseTests = [
        testCase "Invalid StringId handling" <| fun _ ->
            let hierarchy = StringPool.create [||]
            use scope = StringPool.createScope hierarchy None None
            
            let invalidId = StringId -1
            match scope.GetString(invalidId) with
            | ValueSome _ -> failtest "Should not find string for invalid ID"
            | ValueNone -> () // Expected

        testCase "Single character string handling" <| fun _ ->
            let hierarchy = StringPool.create [||]
            use scope = StringPool.createScope hierarchy None None

            // Single character string
            let singleCharId = scope.InternString("x")
            match scope.GetString(singleCharId) with
            | ValueSome str -> Expect.equal str "x" "Should handle single character string"
            | ValueNone -> failtest "Should find single character string"
            
            // Whitespace string
            let whitespaceId = scope.InternString("   ")
            match scope.GetString(whitespaceId) with
            | ValueSome str -> Expect.equal str "   " "Should handle whitespace string"
            | ValueNone -> failtest "Should find whitespace string"

        testCase "Large string handling" <| fun _ ->
            let hierarchy = StringPool.create [||]
            use scope = StringPool.createScope hierarchy None None
            
            let largeString = String.replicate 10000 "large"
            let id = scope.InternString(largeString, StringAccessPattern.LowFrequency)
            
            match scope.GetString(id) with
            | ValueSome retrievedStr -> Expect.equal retrievedStr largeString "Should handle large strings"
            | ValueNone -> failtest "Should find large string"

        testCase "Unicode and special character handling" <| fun _ ->
            let hierarchy = StringPool.create [||]
            use scope = StringPool.createScope hierarchy None None
            
            let unicodeStrings = [|
                "café"
                "北京"
                "🌍🌎🌏"
                "Ñandú"
                "αβγ"
                "हिन्दी"
            |]
            
            unicodeStrings |> Array.iter (fun str ->
                let id = scope.InternString(str)
                match scope.GetString(id) with
                | ValueSome retrievedStr -> Expect.equal retrievedStr str $"Should handle unicode string: {str}"
                | ValueNone -> failtest $"Should find unicode string: {str}"
            )

        testCase "Context disposal cleanup" <| fun _ ->
            let hierarchy = StringPool.create [||]
            let mutable contextCount = 0
            
            // Create and dispose multiple contexts
            for i in 1..10 do
                use scope = StringPool.createScope hierarchy None None
                contextCount <- contextCount + 1
                let str = sprintf "context_string_%d" i
                scope.InternString(str) |> ignore
            
            // Verify system still works after many context disposals
            use finalScope = StringPool.createScope hierarchy None None
            let finalStr = "final_test_string"
            let finalId = finalScope.InternString(finalStr)
            
            match finalScope.GetString(finalId) with
            | ValueSome retrievedStr -> Expect.equal retrievedStr finalStr "Should work after context disposals"
            | ValueNone -> failtest "Should find string after context disposals"
    ]

    let modelBasedTests = [
        (* testCase "Debug String pool routing" <| fun _ ->
            let hierarchy = StringPool.create [||]
            use scope = StringPool.createScope hierarchy None (Some 100)
            
            printfn "\n=== Starting Debug ==="
            
            // Test the exact failing sequence
            let testOperations = [
                ("string1", StringAccessPattern.HighFrequency)
                ("string2", StringAccessPattern.MediumFrequency) 
                ("string1", StringAccessPattern.HighFrequency)
                ("string3", StringAccessPattern.Planning)
            ]
            
            for (str, pattern) in testOperations do
                let id = scope.InternString(str, pattern)
                printfn "Interned '%s' with %A -> StringId %d" str pattern id.Value
                
                match scope.GetString(id) with
                | ValueSome retrievedStr -> 
                    printfn "  Retrieved StringId %d -> '%s' (Match: %b)" id.Value retrievedStr (retrievedStr = str)
                    if retrievedStr <> str then
                        printfn "  *** MISMATCH DETECTED ***"
                | ValueNone -> 
                    printfn "  Retrieved StringId %d -> NONE" id.Value
            
            printfn "=== End Debug ===\n" *)
        
        testCase "String pool routing respects access patterns" <| fun _ ->
            let hierarchy = StringPool.create [||]
            use scope = StringPool.createScope hierarchy None (Some 100)
            
            let testOperations = [
                ("string1", StringAccessPattern.HighFrequency)   // → Local pool
                ("string2", StringAccessPattern.MediumFrequency) // → Global pool  
                ("string1", StringAccessPattern.HighFrequency)   // → Should reuse local
                ("string3", StringAccessPattern.Planning)        // → Global pool
            ]
            
            let mutable results = []
            
            for (str, pattern) in testOperations do
                let id = scope.InternString(str, pattern)
                results <- (str, pattern, id) :: results
                
                // Verify round-trip always works
                match scope.GetString(id) with
                | ValueSome retrievedStr -> Expect.equal retrievedStr str "Round-trip should work"
                | ValueNone -> failtest "Should retrieve interned string"
            
            // Verify same string + same pattern = same ID
            let highFreqIds = results |> List.filter (fun (s, p, _) -> s = "string1" && p = StringAccessPattern.HighFrequency)
            Expect.equal (List.length highFreqIds) 2 "Should have two HighFrequency entries for string1"
            
            let (_, _, id1) = highFreqIds.[0]
            let (_, _, id2) = highFreqIds.[1]
            Expect.equal id1 id2 "Same string + same pattern should give same ID"
    ]

    [<Tests>]
    let allStringPoolTests = 
        testList "StringPool" [
            testList "FsCheck" propertyTests
            testList "Bugfix" bugfixTests
            testList "Robustness" performanceTests
            testList "EdgeCases" edgeCaseTests
            testList "ModelTests" modelBasedTests
        ]

// In StringInterningTests.fs, update the registration at the bottom:
    do Arb.register<StringPoolArbitraries>() |> ignore
    do Arb.register<RMLCoreGenerators.RMLCoreArbitraries>() |> ignore