namespace RMLPipeline.Tests

open Expecto
open FsCheck
open RMLPipeline
open RMLPipeline.Model
open RMLPipeline.DSL
open RMLPipeline.Internal
open RMLPipeline.Internal.Planner

module PlannerModelTests =

    type PlanId = PlanId of int

    type ModelState = {
        Plans: Map<PlanId, unit> // Model only tracks existence, not actual plans
        NextPlanId: int
        ExecutionCount: int
        DependencyAnalysisCount: int
        LastError: string option
        TotalPlansCreated: int
    } with
        static member Initial = {
            Plans = Map.empty
            NextPlanId = 0
            ExecutionCount = 0
            DependencyAnalysisCount = 0
            LastError = None
            TotalPlansCreated = 0
        }

    type ActualState = {
        Plans: Map<PlanId, RMLPlan>
        NextPlanId: int
        ExecutionHistory: (PlanId * int64 * PoolStats) list
        DependencyAnalysis: Map<PlanId, (int * int[])[]>
        LastError: string option
        TotalPlansCreated: int
    } with
        static member Initial = {
            Plans = Map.empty
            NextPlanId = 0
            ExecutionHistory = []
            DependencyAnalysis = Map.empty
            LastError = None
            TotalPlansCreated = 0
        }

    type PlannerCommand =
        | CreatePlan of TriplesMap[] * PlannerConfig
        | ExecuteHotPath of PlanId
        | ExecuteHotPathWithDebugging of PlanId * bool
        | GetDependencyGroups of PlanId
        | GetMemoryStats of PlanId
        | GetTupleInfo of PlanId * int
        | BenchmarkHotPath of PlanId * int
        | CreateStreamingPlan of TriplesMap[] * PlannerConfig

    let genBasicTriplesMap = 
        gen {
            let! id = Gen.choose(1, 1000)
            let! hasClass = Gen.frequency [(7, Gen.constant true); (3, Gen.constant false)]            
            return buildTriplesMap (triplesMap {
                do! setLogicalSource (logicalSource {
                    do! iterator $"$.data[{id}]"
                    do! asJSONPath
                })
                do! setSubjectMap (subjectMap {
                    do! subjectTermMap (templateTermAsIRI $"http://example.org/item/{id}/{{id}}")
                    if hasClass then
                        do! addClass "http://example.org/Item"
                })
                do! addPredicateObjectMap (predicateObjectMap {
                    do! addPredicate "http://example.org/name"
                    do! addObjectMap (objectMap {
                        do! objectTermMap (refTermAsLiteral "name")
                    })
                })
                if hasClass then
                    do! addPredicateObjectMap (predicateObjectMap {
                        do! addPredicate "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                        do! addObject (URI "http://example.org/Item")
                    })
            })
        }

    // Add the missing templateTermAsLiteral function
    let templateTermAsLiteral (tmpl: string) = termMap {
        do! expressionMap (exprMap {
            do! template tmpl
        })
        do! asLiteral
    }

    let genTriplesMapWithJoin = 
        gen {
            let parentMap = buildTriplesMap (triplesMap {
                do! setLogicalSource (logicalSource {
                    do! iterator "$.parents[*]"
                    do! asJSONPath
                })
                do! setSubjectMap (subjectMap {
                    do! subjectTermMap (templateTermAsIRI "http://example.org/parent/{id}")
                })
                do! addPredicateObjectMap (predicateObjectMap {
                    do! addPredicate "http://example.org/name"
                    do! addObjectMap (objectMap {
                        do! objectTermMap (refTermAsLiteral "name")
                    })
                })
            })
            
            let childMap = buildTriplesMap (triplesMap {
                do! setLogicalSource (logicalSource {
                    do! iterator "$.children[*]"
                    do! asJSONPath
                })
                do! setSubjectMap (subjectMap {
                    do! subjectTermMap (templateTermAsIRI "http://example.org/child/{id}")
                })
                do! addPredicateObjectMap (predicateObjectMap {
                    do! addPredicate "http://example.org/hasParent"
                    do! addRefObjectMap parentMap (refObjectMap {
                        do! addJoinCondition (join {
                            do! child "parentId"
                            do! parent "id"
                        })
                    })
                })
            })            
            return [| parentMap; childMap |]
        }

    let genStringPoolStressTriplesMap = 
        gen {
            let! predicateCount = Gen.choose(2, 6)            
            let predicates = [
                "http://example.org/property1"
                "http://example.org/property2"
                "http://example.org/property3"
                "http://example.org/hasValue"
                "http://example.org/relatedTo"
                "http://example.org/describes"
            ]
            
            return buildTriplesMap (triplesMap {
                do! setLogicalSource (logicalSource {
                    do! iterator "$.complex[*].nested[*]"
                    do! asJSONPath
                })
                do! setSubjectMap (subjectMap {
                    do! subjectTermMap (templateTermAsIRI "http://example.org/complex/{id}/{type}/{category}")
                    do! addClass "http://example.org/ComplexItem"
                })
                
                for i in 0..min (predicateCount - 1) (predicates.Length - 1) do
                    do! addPredicateObjectMap (predicateObjectMap {
                        do! addPredicate predicates.[i]
                        do! addObjectMap (objectMap {
                            do! objectTermMap (templateTermAsLiteral $"{{field{i}}}")
                        })
                    })
            })
        }

    let genPlannerConfig = 
        gen {
            let! memoryMode = Gen.elements [LowMemory; Balanced; HighPerformance]
            let! chunkSize = Gen.choose(10, 500)
            let! indexStrategy = Gen.elements [Some NoIndex; Some HashIndex; Some FullIndex; None]
            let! maxMemoryMB = Gen.elements [Some 32; Some 64; Some 128; Some 256; Some 512; None]            
            return {
                MemoryMode = memoryMode
                MaxMemoryMB = maxMemoryMB
                ChunkSize = chunkSize
                IndexStrategy = indexStrategy
                EnableStringPooling = true
                MaxLocalStrings = Some 1000
                MaxGroupPools = Some 10
            }
        }

    let genTriplesMapSet =
        gen {
            let! setType = Gen.choose(1, 4)
            match setType with
            | 1 -> 
                let! map = genBasicTriplesMap
                return [| map |]
            | 2 -> 
                let! maps = genTriplesMapWithJoin
                return maps
            | 3 ->
                let! map = genStringPoolStressTriplesMap
                return [| map |]
            | _ ->
                let! count = Gen.choose(2, 4)
                let! maps = Gen.listOfLength count genBasicTriplesMap
                return Array.ofList maps
        }

    let runCommand (command: PlannerCommand) (state: ActualState) : ActualState =
        try
            match command with
            | CreatePlan (maps, config) when maps.Length > 0 ->
                let plan = createRMLPlan maps config
                let planId = PlanId state.NextPlanId
                {
                    state with 
                        Plans = Map.add planId plan state.Plans
                        NextPlanId = state.NextPlanId + 1
                        TotalPlansCreated = state.TotalPlansCreated + 1
                        LastError = None
                }            
            | ExecuteHotPath planId when Map.containsKey planId state.Plans ->
                let plan = state.Plans.[planId]
                let (time, stats) = PlanUtils.executeWithMonitoring plan
                {
                    state with 
                        ExecutionHistory = (planId, time, stats) :: state.ExecutionHistory
                        LastError = None
                }            
            | ExecuteHotPathWithDebugging (planId, enableLogging) when Map.containsKey planId state.Plans ->
                let plan = state.Plans.[planId]
                let (time, stats) = PlanUtils.executeWithDebugging plan enableLogging
                {
                    state with 
                        ExecutionHistory = (planId, time, stats) :: state.ExecutionHistory
                        LastError = None
                }            
            | GetDependencyGroups planId when Map.containsKey planId state.Plans ->
                let plan = state.Plans.[planId]
                let groupInfo = PlanUtils.getDependencyGroupInfo plan
                {
                    state with 
                        DependencyAnalysis = Map.add planId groupInfo state.DependencyAnalysis
                        LastError = None
                }            
            | GetMemoryStats planId when Map.containsKey planId state.Plans ->
                let plan = state.Plans.[planId]
                let stats = PlanUtils.getMemoryStats plan
                {
                    state with 
                        ExecutionHistory = (planId, 0L, stats) :: state.ExecutionHistory
                        LastError = None
                }            
            | GetTupleInfo (planId, tupleIndex) when Map.containsKey planId state.Plans ->
                let plan = state.Plans.[planId]
                if plan.OrderedMaps.Length > 0 && 
                   plan.OrderedMaps.[0].PredicateTuples.Length > tupleIndex then
                    let tuple = plan.OrderedMaps.[0].PredicateTuples.[tupleIndex]
                    let _ = PlanUtils.getTupleInfo plan tuple
                    let _ = PlanUtils.getTupleFlags tuple
                    { state with LastError = None }
                else
                    { state with LastError = Some "Invalid tuple index" }            
            | BenchmarkHotPath (planId, iterations) when Map.containsKey planId state.Plans ->
                let plan = state.Plans.[planId]
                let (fastTime, slowTime) = PlanUtils.benchmarkHotPath plan iterations
                let stats = PlanUtils.getMemoryStats plan
                {
                    state with 
                        ExecutionHistory = (planId, fastTime, stats) :: state.ExecutionHistory
                        LastError = None
                }            
            | CreateStreamingPlan (maps, config) when maps.Length > 0 ->
                let streamingPlans = createStreamingPlan maps config |> Seq.toArray
                let newPlans = 
                    streamingPlans 
                    |> Array.mapi (fun i plan -> 
                        (PlanId (state.NextPlanId + i), plan))
                    |> Map.ofArray                
                {
                    state with 
                        Plans = Map.fold (fun acc k v -> Map.add k v acc) state.Plans newPlans
                        NextPlanId = state.NextPlanId + streamingPlans.Length
                        TotalPlansCreated = state.TotalPlansCreated + streamingPlans.Length
                        LastError = None
                }            
            | _ ->
                { state with LastError = Some "Invalid command or plan not found" }        
        with ex ->
            { state with LastError = Some ex.Message }

    let updateModel (command: PlannerCommand) (state: ModelState) : ModelState =
        match command with
        | CreatePlan (maps, config) when maps.Length > 0 ->
            let planId = PlanId state.NextPlanId
            {
                state with 
                    Plans = Map.add planId () state.Plans
                    NextPlanId = state.NextPlanId + 1
                    TotalPlansCreated = state.TotalPlansCreated + 1
                    LastError = None
            }        
        | ExecuteHotPath planId | ExecuteHotPathWithDebugging (planId, _) | BenchmarkHotPath (planId, _) 
            when Map.containsKey planId state.Plans ->
            {
                state with 
                    ExecutionCount = state.ExecutionCount + 1
                    LastError = None
            }        
        | GetDependencyGroups planId when Map.containsKey planId state.Plans ->
            {
                state with 
                    DependencyAnalysisCount = state.DependencyAnalysisCount + 1
                    LastError = None
            }        
        | GetMemoryStats planId | GetTupleInfo (planId, _) when Map.containsKey planId state.Plans ->
            { state with LastError = None }        
        | CreateStreamingPlan (maps, config) when maps.Length > 0 ->
            let estimatedChunks = max 1 (maps.Length / config.ChunkSize)
            let newPlanIds = 
                [| for i in 0 .. estimatedChunks - 1 -> 
                    (PlanId (state.NextPlanId + i), ()) |]
                |> Map.ofArray
            
            {
                state with 
                    Plans = Map.fold (fun acc k v -> Map.add k v acc) state.Plans newPlanIds
                    NextPlanId = state.NextPlanId + estimatedChunks
                    TotalPlansCreated = state.TotalPlansCreated + estimatedChunks
                    LastError = None
            }        
        | _ ->
            { state with LastError = Some "Invalid command or plan not found" }

    let genCommand (state: ModelState) : Gen<PlannerCommand> =
        let hasPlans = not (Map.isEmpty state.Plans)
        
        if hasPlans then
            Gen.frequency [
                (4, gen {
                    let! maps = genTriplesMapSet
                    let! config = genPlannerConfig
                    return CreatePlan (maps, config)
                })
                (3, gen {
                    let! planId = Gen.elements (Map.keys state.Plans |> Seq.toList)
                    return ExecuteHotPath planId
                })
                (3, gen {
                    let! planId = Gen.elements (Map.keys state.Plans |> Seq.toList)
                    let! enableLogging = Gen.frequency [(8, Gen.constant false); (2, Gen.constant true)]
                    return ExecuteHotPathWithDebugging (planId, enableLogging)
                })
                (2, gen {
                    let! planId = Gen.elements (Map.keys state.Plans |> Seq.toList)
                    return GetDependencyGroups planId
                })
                (2, gen {
                    let! planId = Gen.elements (Map.keys state.Plans |> Seq.toList)
                    return GetMemoryStats planId
                })
                (1, gen {
                    let! planId = Gen.elements (Map.keys state.Plans |> Seq.toList)
                    let! tupleIndex = Gen.choose(0, 5) // Assume reasonable range
                    return GetTupleInfo (planId, tupleIndex)
                })
                (1, gen {
                    let! planId = Gen.elements (Map.keys state.Plans |> Seq.toList)
                    let! iterations = Gen.choose(1, 5)
                    return BenchmarkHotPath (planId, iterations)
                })
                (1, gen {
                    let! maps = genTriplesMapSet
                    let! config = genPlannerConfig
                    return CreateStreamingPlan (maps, config)
                })
            ]
        else
            gen {
                let! maps = genTriplesMapSet
                let! config = genPlannerConfig
                return CreatePlan (maps, config)
            }

    let statefulProperties = [
        
        testProperty "Planner state transitions are consistent" <| fun () ->
            let maxLength = 20
            
            // Generate a sequence of commands
            let rec generateCommands (state: ModelState) (length: int) (acc: PlannerCommand list) : Gen<PlannerCommand list> =
                if length <= 0 then
                    Gen.constant (List.rev acc)
                else
                    gen {
                        let! command = genCommand state
                        let newState = updateModel command state
                        return! generateCommands newState (length - 1) (command :: acc)
                    }
            
            // Property that checks consistency
            generateCommands ModelState.Initial maxLength []
            |> Gen.map (fun commands ->
                // Run commands on both model and actual state
                let finalModel, finalActual = 
                    commands
                    |> List.fold (fun (modelState, actualState) command ->
                        let newModel = updateModel command modelState
                        let newActual = runCommand command actualState
                        (newModel, newActual)
                    ) (ModelState.Initial, ActualState.Initial)
                
                // Check consistency between model and actual
                finalModel.TotalPlansCreated = finalActual.TotalPlansCreated &&
                Map.count finalModel.Plans = Map.count finalActual.Plans &&
                finalModel.NextPlanId = finalActual.NextPlanId &&
                match finalModel.LastError, finalActual.LastError with
                 | None, None -> true
                 | Some _, Some _ -> true
                 | _ -> false
            )

        testProperty "Plan IDs remain unique and sequential" <| fun () ->
            let maxLength = 15
            
            let rec generateCommands (state: ModelState) (length: int) (acc: PlannerCommand list) =
                if length <= 0 then
                    Gen.constant (List.rev acc)
                else
                    gen {
                        let! command = genCommand state
                        let newState = updateModel command state
                        return! generateCommands newState (length - 1) (command :: acc)
                    }
            
            generateCommands ModelState.Initial maxLength []
            |> Gen.map (fun commands ->
                let finalState = 
                    commands
                    |> List.fold (fun state command -> runCommand command state) ActualState.Initial
                
                let planIds = Map.keys finalState.Plans |> Seq.map (fun (PlanId id) -> id) |> Seq.sort |> Seq.toList
                let expectedIds = [0 .. finalState.NextPlanId - 1]
                planIds = expectedIds
            )

        testProperty "Memory statistics are always valid" <| fun () ->
            let maxLength = 10
            
            let rec generateCommands (state: ModelState) (length: int) (acc: PlannerCommand list) =
                if length <= 0 then
                    Gen.constant (List.rev acc)
                else
                    gen {
                        let! command = genCommand state
                        let newState = updateModel command state
                        return! generateCommands newState (length - 1) (command :: acc)
                    }
            
            generateCommands ModelState.Initial maxLength []
            |> Gen.map (fun commands ->
                let finalState = 
                    commands
                    |> List.fold (fun state command -> runCommand command state) ActualState.Initial
                
                finalState.ExecutionHistory
                |> List.forall (fun (_, time, stats) ->
                    time >= 0L &&
                    stats.MemoryUsageBytes > 0L &&
                    stats.HitRatio >= 0.0 && stats.HitRatio <= 1.0 &&
                    stats.AccessCount >= 0L &&
                    stats.MissCount >= 0L &&
                    stats.AccessCount >= stats.MissCount)
            )
    ]

    let enhancedUnitTests = [
        
        testCase "Single CreatePlan command works correctly" <| fun _ ->
            let maps = [| Gen.sample 0 1 genBasicTriplesMap |> List.head |]
            let config = PlannerConfig.Default
            let command = CreatePlan (maps, config)
            
            let actualResult = runCommand command ActualState.Initial
            let modelResult = updateModel command ModelState.Initial
            
            Expect.equal actualResult.TotalPlansCreated 1 "Should create one plan"
            Expect.equal modelResult.TotalPlansCreated 1 "Model should track one plan"
            Expect.equal (Map.count actualResult.Plans) (Map.count modelResult.Plans) "Plan counts should match"

        testCase "Invalid operations are handled gracefully" <| fun _ ->
            let invalidPlanId = PlanId 999
            let command = ExecuteHotPath invalidPlanId
            
            let result = runCommand command ActualState.Initial
            Expect.isSome result.LastError "Should have error for invalid plan ID"

        testCase "Streaming plans create multiple entries" <| fun _ ->
            let maps = Array.init 10 (fun _ -> Gen.sample 0 1 genBasicTriplesMap |> List.head)
            let config = { PlannerConfig.Default with ChunkSize = 3 }
            let command = CreateStreamingPlan (maps, config)
            
            let result = runCommand command ActualState.Initial
            Expect.isGreaterThan (Map.count result.Plans) 1 "Should create multiple plans"
    ]

    [<Tests>]
    let allStatefulPlannerTests = 
        testList "Modern Stateful Planner Tests" [
            testList "Stateful Properties" statefulProperties
            testList "Enhanced Units" enhancedUnitTests
        ]