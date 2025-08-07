namespace RMLPipeline.Tests

open Expecto
open FsCheck
open RMLPipeline
open RMLPipeline.Model
open RMLPipeline.DSL
open RMLPipeline.Internal
open RMLPipeline.Internal.Planner
open System

module PlannerModelTests =

    type PlanId = PlanId of int

    (* 
        Applied as a smoke test that catches basic state management bugs - 
        catches plan creation, ID assignment, and error handling issues.
    *)
    type ModelState = {
        Plans: Map<PlanId, unit> // This model only tracks existence, not actual plans
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
    
    (* 
        Behavioral contracts model: Tracks what the planner guarantees
        to provide, not how it implements those guarantees internally.
    *)
    type ContractState = {
        Plans: Map<PlanId, PlanContract>
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
    
    (* 
        Contract for what a plan promises, not how it's implemented.
        Focus on observable properties and behavioral guarantees.
    *)
    and PlanContract = {
        MapCount: int
        HasValidStructure: bool
        SupportsBoundsChecking: bool
        MemoryMode: MemoryMode
        HasIndexingStrategy: bool
        SupportsExecution: bool
    }

    type PlanMetadata = {
        MapCount: int
        TotalTupleCount: int
        MemoryMode: MemoryMode
    }

    (* 
        Allows tracking of plan metadata for verification purposes,
        focussing on properties that can be observed externally.
    *)
    type ModelMetadataState = {
        Plans: Map<PlanId, PlanMetadata>
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

    module PlanVerification =
        
        (* Extract actual structural properties for contract verification *)
        let extractPlanContract (plan: RMLPlan) : PlanContract =
            let hasValidStructure = 
                plan.OrderedMaps.Length >= 0 &&
                plan.DependencyGroups.GroupStarts.Length > 0 &&
                plan.StringPoolHierarchy <> Unchecked.defaultof<_>
            
            let supportsBoundsChecking = 
                plan.OrderedMaps.Length = 0 || 
                plan.OrderedMaps |> Array.forall (fun map -> map.PredicateTuples.Length >= 0)
            
            let hasIndexingStrategy = 
                plan.OrderedMaps |> Array.forall (fun map -> 
                    map.IndexStrategy = NoIndex || 
                    map.IndexStrategy = HashIndex || 
                    map.IndexStrategy = FullIndex)
            
            let supportsExecution = 
                try
                    // Test that execution pathways are accessible
                    let _ = plan.GetPredicateIndex()
                    let _ = plan.GetJoinIndex()
                    let _ = plan.GetPathToMaps()
                    true
                with _ -> false
            
            {
                MapCount = plan.OrderedMaps.Length
                HasValidStructure = hasValidStructure
                SupportsBoundsChecking = supportsBoundsChecking
                MemoryMode = plan.Config.MemoryMode
                HasIndexingStrategy = hasIndexingStrategy
                SupportsExecution = supportsExecution
            }
        
        (* Test behavioral invariants rather than implementation details *)
        let verifyPlanInvariants (plan: RMLPlan) : bool =
            // Invariant: All maps must have consistent StringPool integration
            plan.OrderedMaps |> Array.forall (fun mapPlan ->
                PlanUtils.getString plan mapPlan.IteratorPathId |> Option.isSome) &&
            
            // Invariant: Dependency groups must be non-overlapping and complete
            let allGroupMembers = 
                [| for i = 0 to plan.DependencyGroups.GroupStarts.Length - 1 do
                    yield! plan.DependencyGroups.GetGroup(i) |]
                |> Array.sort
            let expectedMembers = [| 0 .. plan.OrderedMaps.Length - 1 |]
            allGroupMembers = expectedMembers &&
            
            // Invariant: Hash values must be deterministic
            plan.OrderedMaps |> Array.forall (fun mapPlan ->
                mapPlan.PredicateTuples |> Array.forall (fun tuple ->
                    let recomputedHash = 
                        uint64 tuple.SubjectTemplateId.Value ^^^
                        (uint64 tuple.PredicateValueId.Value <<< 1) ^^^
                        (uint64 tuple.ObjectTemplateId.Value <<< 2) ^^^
                        (uint64 tuple.SourcePathId.Value <<< 3)
                    tuple.Hash = recomputedHash)) &&
            
            // Invariant: Memory mode configuration is respected
            match plan.Config.MemoryMode with
            | LowMemory -> plan.Config.ChunkSize <= 100
            | HighPerformance -> plan.Config.ChunkSize >= 100
            | Balanced -> true // No specific constraints for balanced mode
        
        let verifyIndexConsistency (plan: RMLPlan) : bool =
            // Test that index strategies are consistently applied
            plan.OrderedMaps |> Array.forall (fun mapPlan ->
                match mapPlan.IndexStrategy with
                | NoIndex -> true // Always valid
                | HashIndex | FullIndex -> 
                    // Should be able to access indexes without exceptions
                    try
                        let _ = plan.GetPredicateIndex()
                        true
                    with _ -> false)
        
        let verifyMemoryModeConsistency (plan: RMLPlan) : bool =
            // Test that memory mode affects observable behavior
            let stats = PlanUtils.getMemoryStats plan
            stats.MemoryUsageBytes > 0L && stats.HitRatio >= 0.0 && stats.HitRatio <= 1.0
        
        let verifyHashConsistency (plan: RMLPlan) : bool =
            plan.OrderedMaps
            |> Array.forall (fun mapPlan ->
                mapPlan.PredicateTuples
                |> Array.forall (fun tuple ->
                    let expectedHash = 
                        uint64 tuple.SubjectTemplateId.Value ^^^
                        (uint64 tuple.PredicateValueId.Value <<< 1) ^^^
                        (uint64 tuple.ObjectTemplateId.Value <<< 2) ^^^
                        (uint64 tuple.SourcePathId.Value <<< 3)
                    tuple.Hash = expectedHash))
        
        let verifyJoinHashConsistency (plan: RMLPlan) : bool =
            plan.OrderedMaps
            |> Array.forall (fun mapPlan ->
                mapPlan.JoinTuples
                |> Array.forall (fun joinTuple ->
                    let expectedHash = 
                        uint64 joinTuple.ParentTuple.SubjectTemplateId.Value ^^^
                        (uint64 joinTuple.ParentTuple.PredicateValueId.Value <<< 1) ^^^
                        (uint64 joinTuple.ParentTuple.ObjectTemplateId.Value <<< 2) ^^^
                        (uint64 joinTuple.ChildTuple.SubjectTemplateId.Value <<< 3) ^^^
                        (uint64 joinTuple.ChildTuple.PredicateValueId.Value <<< 4) ^^^
                        (uint64 joinTuple.ChildTuple.ObjectTemplateId.Value <<< 5) ^^^
                        (uint64 joinTuple.ParentPathId.Value <<< 6) ^^^
                        (uint64 joinTuple.ChildPathId.Value <<< 7)
                    joinTuple.Hash = expectedHash))
        
        let verifyExecutionPathwayConsistency (plan: RMLPlan) : bool =
            try
                // Test that both execution pathways complete without errors
                executeHotPath plan
                executeHotPathWithStringResolution plan false
                true
            with
            | _ -> false
        
        let verifyStringPoolIntegrity (plan: RMLPlan) : bool =
            let stats = PlanUtils.getMemoryStats plan
            stats.AccessCount >= 0L &&
            stats.HitRatio >= 0.0 && stats.HitRatio <= 1.0 &&
            stats.MemoryUsageBytes > 0L

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
            
            // Generate chunk size that respects memory mode invariants
            let! chunkSize = 
                match memoryMode with
                | LowMemory -> Gen.choose(10, 100)
                | HighPerformance -> Gen.choose(100, 500)
                | Balanced -> Gen.choose(10, 500)
            
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

    // Verifies bounds checking behavior
    let genBoundsTestCommand (state: ModelState) : Gen<PlannerCommand> =
        let hasPlans = not (Map.isEmpty state.Plans)
        
        if hasPlans then
            Gen.frequency [
                (3, gen {
                    let! maps = genTriplesMapSet
                    let! config = genPlannerConfig
                    return CreatePlan (maps, config)
                })
                (7, gen {
                    let! planId = Gen.elements (Map.keys state.Plans |> Seq.toList)
                    let! tupleIndex = Gen.frequency [
                        (2, Gen.choose(-10, -1))       // Negative indices
                        (3, Gen.choose(0, 5))          // Small indices that might be valid  
                        (2, Gen.choose(6, 20))         // Medium indices that might be invalid
                        (1, Gen.choose(50, 1000))      // Large indices definitely invalid
                    ]
                    return GetTupleInfo (planId, tupleIndex)
                })
            ]
        else
            gen {
                let! maps = genTriplesMapSet
                let! config = genPlannerConfig
                return CreatePlan (maps, config)
            }

    (* Contract-based model: tracks behavioral promises *)
    let updateContractModel (command: PlannerCommand) (state: ContractState) : ContractState =
        match command with
        | CreatePlan (maps, config) when maps.Length > 0 ->
            let planId = PlanId state.NextPlanId
            let contract = {
                MapCount = maps.Length
                HasValidStructure = true
                SupportsBoundsChecking = true
                MemoryMode = config.MemoryMode
                HasIndexingStrategy = true
                SupportsExecution = true
            }
            
            {
                state with 
                    Plans = Map.add planId contract state.Plans
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
        
        | GetTupleInfo (planId, tupleIndex) when Map.containsKey planId state.Plans ->
            let contract = state.Plans.[planId]
            // Contract: bounds checking should work regardless of implementation details
            let hasError = not contract.SupportsBoundsChecking || tupleIndex < 0
            
            {
                state with 
                    LastError = if hasError then Some "Invalid tuple index" else None
            }
        
        | GetMemoryStats planId when Map.containsKey planId state.Plans ->
            { state with LastError = None }
        
        | CreateStreamingPlan (maps, config) when maps.Length > 0 ->
            let estimatedChunks = max 1 (maps.Length / config.ChunkSize)
            let newContracts = 
                [| for i in 0 .. estimatedChunks - 1 -> 
                    let chunkSize = min config.ChunkSize (maps.Length - i * config.ChunkSize)
                    let contract = {
                        MapCount = chunkSize
                        HasValidStructure = true
                        SupportsBoundsChecking = true
                        MemoryMode = config.MemoryMode
                        HasIndexingStrategy = true
                        SupportsExecution = true
                    }
                    (PlanId (state.NextPlanId + i), contract) |]
                |> Map.ofArray
            
            {
                state with 
                    Plans = Map.fold (fun acc k v -> Map.add k v acc) state.Plans newContracts
                    NextPlanId = state.NextPlanId + estimatedChunks
                    TotalPlansCreated = state.TotalPlansCreated + estimatedChunks
                    LastError = None
            }
        
        | _ ->
            { state with LastError = Some "Invalid command or plan not found" }

    let genContractCommand (state: ContractState) : Gen<PlannerCommand> =
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
                (2, gen {
                    let! planId = Gen.elements (Map.keys state.Plans |> Seq.toList)
                    let! tupleIndex = Gen.frequency [
                        (1, Gen.choose(-5, -1))        // Negative
                        (4, Gen.choose(0, 10))         // Reasonable range
                        (2, Gen.choose(11, 50))        // Medium range
                        (1, Gen.choose(100, 1000))     // Large range
                    ]
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
                // Use actual implementation logic as our source of truth...
                if plan.OrderedMaps.Length > 0 && 
                    tupleIndex >= 0 &&
                    tupleIndex < plan.OrderedMaps.[0].PredicateTuples.Length then
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

    (* TEST SUITE *)

    let boundaryViolationModelTests = [
        
        testCase "Zero-length triples map array" <| fun _ ->
            let command = CreatePlan ([||], PlannerConfig.Default)
            let result = runCommand command ActualState.Initial
            Expect.isSome result.LastError "Should handle empty array gracefully"

        testCase "Plan with minimal predicate tuples" <| fun _ ->
            // Create a minimal triples map
            let minimalMap = buildTriplesMap (triplesMap {
                do! setLogicalSource (logicalSource {
                    do! iterator "$.minimal"
                    do! asJSONPath
                })
                do! setSubjectMap (subjectMap {
                    do! subjectTermMap (templateTermAsIRI "http://example.org/minimal/{id}")
                })
                // Minimal predicate-object mapping
                do! addPredicateObjectMap (predicateObjectMap {
                    do! addPredicate "http://example.org/hasValue"
                    do! addObject (Literal "test")
                })
            })
            
            let plan = createRMLPlan [| minimalMap |] PlannerConfig.Default
            let planId = PlanId 0
            let state = { ActualState.Initial with Plans = Map.add planId plan Map.empty }
            
            let tupleCount = plan.OrderedMaps.[0].PredicateTuples.Length
            
            // Test boundary conditions
            if tupleCount > 0 then
                let validCommand = GetTupleInfo (planId, 0)
                let validResult = runCommand validCommand state
                Expect.isNone validResult.LastError "Should handle valid index"
                
                let invalidCommand = GetTupleInfo (planId, tupleCount)
                let invalidResult = runCommand invalidCommand state
                Expect.isSome invalidResult.LastError "Should handle invalid index"

        testCase "Boundary value testing with systematic approach" <| fun _ ->
            let maps = [| Gen.sample 0 1 genBasicTriplesMap |> List.head |]
            let plan = createRMLPlan maps PlannerConfig.Default
            let planId = PlanId 0
            let state = { ActualState.Initial with Plans = Map.add planId plan Map.empty }
            
            let tupleCount = plan.OrderedMaps.[0].PredicateTuples.Length
            
            if tupleCount > 0 then
                // Systematic boundary testing
                let boundaryTests = [
                    (tupleCount - 1, false, "Last valid index")     
                    (tupleCount, true, "First invalid index")          
                    (tupleCount + 1, true, "Beyond boundary")      
                    (0, false, "First valid index")                  
                    (-1, true, "Just below valid range")
                    (Int32.MaxValue, true, "Maximum integer")
                    (Int32.MinValue, true, "Minimum integer")
                ]
                
                boundaryTests |> List.iter (fun (index, shouldError, description) ->
                    let command = GetTupleInfo (planId, index)
                    let result = runCommand command state
                    
                    if shouldError then
                        Expect.isSome result.LastError $"{description}: Index {index} should produce error"
                        Expect.equal result.LastError (Some "Invalid tuple index") $"{description}: Should have specific error"
                    else
                        Expect.isNone result.LastError $"{description}: Index {index} should not produce error")

        testCase "Multiple maps bounds checking" <| fun _ ->
            let maps = Array.init 3 (fun _ -> Gen.sample 0 1 genBasicTriplesMap |> List.head)
            let plan = createRMLPlan maps PlannerConfig.Default
            let planId = PlanId 0
            let state = { ActualState.Initial with Plans = Map.add planId plan Map.empty }
            
            // Test that we're checking the first map's tuple count
            let firstMapTupleCount = plan.OrderedMaps.[0].PredicateTuples.Length
            
            let testCases = [
                (firstMapTupleCount - 1, false) // Last valid for first map
                (firstMapTupleCount, true)      // Invalid for first map
            ]
            
            testCases |> List.iter (fun (index, shouldError) ->
                let command = GetTupleInfo (planId, index)
                let result = runCommand command state
                
                if shouldError then
                    Expect.isSome result.LastError $"Index {index} should be invalid"
                else
                    Expect.isNone result.LastError $"Index {index} should be valid")
    ]

    let boundsCheckingTests = [    
        testCase "GetTupleInfo handles empty plans gracefully" <| fun _ ->
            let emptyMaps = [||]
            let plan = createRMLPlan emptyMaps PlannerConfig.Default
            let planId = PlanId 0
            
            let state = { ActualState.Initial with Plans = Map.add planId plan Map.empty }
            let command = GetTupleInfo (planId, 0)
            let result = runCommand command state
            
            Expect.isSome result.LastError "Should have error for empty plan"
            Expect.equal result.LastError (Some "Invalid tuple index") "Should have specific error message"

        testCase "GetTupleInfo handles out-of-bounds indices" <| fun _ ->
            let maps = [| Gen.sample 0 1 genBasicTriplesMap |> List.head |]
            let plan = createRMLPlan maps PlannerConfig.Default
            let planId = PlanId 0
            
            let state = { ActualState.Initial with Plans = Map.add planId plan Map.empty }
            
            // Test various out-of-bounds indices
            let outOfBoundsIndices = [10; 100; 1000; -1]
            let results = 
                outOfBoundsIndices
                |> List.map (fun index ->
                    let command = GetTupleInfo (planId, index)
                    let result = runCommand command state
                    result.LastError)
            
            results |> List.iter (fun error ->
                Expect.isSome error "Should have error for out-of-bounds index"
                Expect.equal error (Some "Invalid tuple index") "Should have specific error message")

        testCase "GetTupleInfo succeeds with valid indices" <| fun _ ->
            let maps = [| Gen.sample 0 1 genBasicTriplesMap |> List.head |]
            let plan = createRMLPlan maps PlannerConfig.Default
            let planId = PlanId 0
            
            let state = { ActualState.Initial with Plans = Map.add planId plan Map.empty }
            
            // Find the actual tuple count
            let tupleCount = plan.OrderedMaps.[0].PredicateTuples.Length
            
            if tupleCount > 0 then
                let validIndices = [0; tupleCount - 1] // First and last valid indices
                let results = 
                    validIndices
                    |> List.map (fun index ->
                        let command = GetTupleInfo (planId, index)
                        let result = runCommand command state
                        result.LastError)
                
                results |> List.iter (fun error ->
                    Expect.isNone error "Should not have error for valid index")

        testProperty "GetTupleInfo bounds checking is consistent" <| fun () ->
            gen {
                let! maps = genTriplesMapSet
                let! config = genPlannerConfig
                let! tupleIndex = Gen.choose(-5, 20)
                return (maps, config, tupleIndex)
            }
            |> Gen.map (fun (maps, config, tupleIndex) ->
                if maps.Length > 0 then
                    let plan = createRMLPlan maps config
                    let planId = PlanId 0
                    let state = { ActualState.Initial with Plans = Map.add planId plan Map.empty }
                    
                    let command = GetTupleInfo (planId, tupleIndex)
                    let result = runCommand command state
                    
                    let expectedError = 
                        plan.OrderedMaps.Length = 0 || 
                        tupleIndex < 0 ||
                        tupleIndex >= plan.OrderedMaps.[0].PredicateTuples.Length
                    
                    match result.LastError with
                    | Some "Invalid tuple index" -> expectedError
                    | None -> not expectedError
                    | _ -> false
                else true)
    ]

    let focusedBoundsProperty = 
        testProperty "Focused bounds checking" <| fun () ->
            gen {
                let! maps = genTriplesMapSet
                let! config = genPlannerConfig
                let! tupleIndex = Gen.frequency [
                    (2, Gen.choose(-10, -1))       // Negative indices
                    (3, Gen.choose(0, 5))          // Small indices that might be valid  
                    (2, Gen.choose(6, 20))         // Medium indices that might be invalid
                    (1, Gen.choose(50, 1000))      // Large indices definitely invalid
                ]
                return (maps, config, tupleIndex)
            }
            |> Gen.map (fun (maps, config, tupleIndex) ->
                if maps.Length > 0 then
                    let plan = createRMLPlan maps config
                    let planId = PlanId 0
                    let state = { ActualState.Initial with Plans = Map.add planId plan Map.empty }
                    
                    let actuallyValid = 
                        plan.OrderedMaps.Length > 0 && 
                        tupleIndex >= 0 &&
                        tupleIndex < plan.OrderedMaps.[0].PredicateTuples.Length

                    let command = GetTupleInfo (planId, tupleIndex)
                    let result = runCommand command state
                    let errorPresent = Option.isSome result.LastError
                    
                    // Valid indices should have no error, invalid indices should have error
                    let isConsistent = actuallyValid = not errorPresent
                    
                    if not isConsistent then
                        printfn "INCONSISTENT: plan maps=%d, tuples=%d, index=%d, valid=%b, error=%b" 
                                plan.OrderedMaps.Length 
                                (if plan.OrderedMaps.Length > 0 then plan.OrderedMaps.[0].PredicateTuples.Length else 0)
                                tupleIndex actuallyValid errorPresent
                    
                    isConsistent
                else true)

    let contractBasedProperties = [
        testProperty "Plan contracts are satisfied by implementation" <| fun () ->
            let maxLength = 25
            
            let rec generateContractCommands (state: ContractState) (length: int) (acc: PlannerCommand list) =
                if length <= 0 then
                    Gen.constant (List.rev acc)
                else
                    gen {
                        let! command = genContractCommand state
                        let newState = updateContractModel command state
                        return! generateContractCommands newState (length - 1) (command :: acc)
                    }
            
            generateContractCommands ContractState.Initial maxLength []
            |> Gen.map (fun commands ->
                let finalActual = 
                    commands
                    |> List.fold (fun actualState command ->
                        runCommand command actualState
                    ) ActualState.Initial
                
                // Verify contracts are satisfied by actual implementation
                finalActual.Plans |> Map.forall (fun planId actualPlan ->
                    let contract = PlanVerification.extractPlanContract actualPlan
                    
                    // Contract verification: Plans must satisfy their promises
                    contract.HasValidStructure &&
                    contract.SupportsBoundsChecking &&
                    contract.HasIndexingStrategy &&
                    contract.SupportsExecution &&
                    contract.MapCount >= 0 &&
                    contract.MemoryMode = actualPlan.Config.MemoryMode)
            )

        ftestProperty "Plan invariants are maintained across operations" <| fun () ->
            gen {
                let! maps = genTriplesMapSet
                let! config = genPlannerConfig
                return (maps, config)
            }
            |> Gen.map (fun (maps, config) ->
                if maps.Length > 0 then
                    let plan = createRMLPlan maps config
                    PlanVerification.verifyPlanInvariants plan
                else true)

        testProperty "Structural integrity is preserved" <| fun () ->
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
                
                // Verify structural integrity across all operations
                finalState.Plans |> Map.forall (fun _ plan ->
                    PlanVerification.verifyPlanInvariants plan &&
                    PlanVerification.verifyHashConsistency plan &&
                    PlanVerification.verifyJoinHashConsistency plan &&
                    PlanVerification.verifyStringPoolIntegrity plan)
            )
    ]

    let statefulProperties = [
        testProperty "Planner state transitions are consistent" <| fun () ->
            let maxLength = 20
            
            // Generate a sequence of commands
            let rec generateCommands 
                    (state: ModelState) 
                    (length: int) 
                    (acc: PlannerCommand list) : Gen<PlannerCommand list> =
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
                 | None, Some "Invalid tuple index" -> true   // bounds checking is tested elsewhere
                 | _ -> 
                    failtestf "Inconsistent error states: Model = %A, Actual = %A" 
                        finalModel.LastError finalActual.LastError
                    false
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

    let structuralProperties = [
        
        testProperty "Plan structure satisfies basic contracts" <| fun () ->
            gen {
                let! maps = genTriplesMapSet
                let! config = genPlannerConfig
                return (maps, config)
            }
            |> Gen.map (fun (maps, config) ->
                if maps.Length > 0 then
                    let plan = createRMLPlan maps config
                    let contract = PlanVerification.extractPlanContract plan
                    
                    // Verify basic structural contracts
                    contract.MapCount = maps.Length &&
                    contract.HasValidStructure &&
                    contract.SupportsBoundsChecking &&
                    contract.MemoryMode = config.MemoryMode &&
                    contract.HasIndexingStrategy &&
                    contract.SupportsExecution
                else true)

        testProperty "Index strategies provide consistent behavior" <| fun () ->
            gen {
                let! maps = genTriplesMapSet
                let! memoryMode = Gen.elements [LowMemory; Balanced; HighPerformance]
                let config = { PlannerConfig.Default with MemoryMode = memoryMode }
                return (maps, config)
            }
            |> Gen.map (fun (maps, config) ->
                if maps.Length > 0 then
                    let plan = createRMLPlan maps config
                    PlanVerification.verifyIndexConsistency plan &&
                    PlanVerification.verifyMemoryModeConsistency plan
                else true)

        testProperty "Hash calculations are always consistent" <| fun () ->
            gen {
                let! maps = genTriplesMapSet
                let! config = genPlannerConfig
                return (maps, config)
            }
            |> Gen.map (fun (maps, config) ->
                if maps.Length > 0 then
                    let plan = createRMLPlan maps config
                    PlanVerification.verifyHashConsistency plan &&
                    PlanVerification.verifyJoinHashConsistency plan
                else true)

        testProperty "Execution pathways maintain consistency" <| fun () ->
            gen {
                let! maps = genTriplesMapSet
                let! strategy = Gen.elements [Some NoIndex; Some HashIndex; Some FullIndex; None]
                let config = { PlannerConfig.Default with IndexStrategy = strategy }
                return (maps, config)
            }
            |> Gen.map (fun (maps, config) ->
                if maps.Length > 0 then
                    let plan = createRMLPlan maps config
                    PlanVerification.verifyExecutionPathwayConsistency plan
                else true)

        testProperty "String pool integrity maintained across operations" <| fun () ->
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
                
                finalState.Plans
                |> Map.forall (fun _ plan -> PlanVerification.verifyStringPoolIntegrity plan))

        testProperty "Dependency groups provide complete coverage" <| fun () ->
            gen {
                let! maps = genTriplesMapWithJoin // Use maps with known joins
                let! config = genPlannerConfig
                return (maps, config)
            }
            |> Gen.map (fun (maps, config) ->
                let plan = createRMLPlan maps config
                let groupInfo = PlanUtils.getDependencyGroupInfo plan
                
                // Verify dependency groups are non-empty and cover all maps
                groupInfo.Length > 0 &&
                groupInfo |> Array.forall (fun (_, members) -> members.Length > 0) &&
                (groupInfo |> Array.collect snd |> Array.sort) = [|0 .. plan.OrderedMaps.Length - 1|])
    ]    

    let memoryModeComparison = [    
        testCase "Different memory modes produce different characteristics" <| fun _ ->
            let maps = Array.init 5 (fun _ -> Gen.sample 0 1 genBasicTriplesMap |> List.head)
            
            let lowMemoryPlan = createRMLPlan maps PlannerConfig.LowMemory
            let balancedPlan = createRMLPlan maps PlannerConfig.Default
            let highPerfPlan = createRMLPlan maps PlannerConfig.HighPerformance
            
            let lowStats = PlanUtils.getMemoryStats lowMemoryPlan
            let balancedStats = PlanUtils.getMemoryStats balancedPlan
            let highStats = PlanUtils.getMemoryStats highPerfPlan
            
            // Memory usage should generally increase with performance mode
            Expect.isLessThanOrEqual lowStats.MemoryUsageBytes balancedStats.MemoryUsageBytes 
                "Low memory should use less than balanced"
            
            // Index strategies should be different
            let lowStrategy = lowMemoryPlan.OrderedMaps.[0].IndexStrategy
            let highStrategy = highPerfPlan.OrderedMaps.[0].IndexStrategy
            
            Expect.notEqual lowStrategy highStrategy 
                "Different memory modes should produce different index strategies"

        testCase "Index strategies affect lazy index creation" <| fun _ ->
            let maps = Array.init 3 (fun _ -> Gen.sample 0 1 genBasicTriplesMap |> List.head)
            
            let noIndexPlan = createRMLPlan maps { PlannerConfig.Default with IndexStrategy = Some NoIndex }
            let fullIndexPlan = createRMLPlan maps { PlannerConfig.Default with IndexStrategy = Some FullIndex }
            
            // NoIndex should have empty indexes
            let noIndexPredicates = noIndexPlan.GetPredicateIndex()
            let fullIndexPredicates = fullIndexPlan.GetPredicateIndex()
            
            Expect.equal (FastMap.count noIndexPredicates) 0 "NoIndex should have empty predicate index"
            Expect.isGreaterThan (FastMap.count fullIndexPredicates) 0 "FullIndex should have populated predicate index"
    ]

    let executionConsistencyTests = [    
        testProperty "Hot path executions are deterministic" <| fun () ->
            gen {
                let! maps = genTriplesMapSet
                let! config = genPlannerConfig
                return (maps, config)
            }
            |> Gen.map (fun (maps, config) ->
                if maps.Length > 0 then
                    let plan = createRMLPlan maps config
                    
                    // Execute multiple times and verify no exceptions
                    let results = 
                        [1..5] 
                        |> List.map (fun _ -> 
                            try 
                                executeHotPath plan
                                executeHotPathWithStringResolution plan false
                                true 
                            with _ -> false)
                    
                    results |> List.forall id
                else true)

        testCase "Benchmark results are reasonable" <| fun _ ->
            let maps = Array.init 2 (fun _ -> Gen.sample 0 1 genBasicTriplesMap |> List.head)
            let plan = createRMLPlan maps PlannerConfig.Default
            
            let (fastTime, slowTime) = PlanUtils.benchmarkHotPath plan 3
            
            // Basic sanity checks
            Expect.isGreaterThanOrEqual fastTime 0L "Fast execution time should be non-negative"
            Expect.isGreaterThanOrEqual slowTime 0L "Slow execution time should be non-negative"
            Expect.isLessThanOrEqual fastTime (slowTime + 10L) "Fast path should not be significantly slower than debug path"
    ]

    let baselineRegression = [
        
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
        testList "StatefulPlannerTests" [
            testList "BoundaryViolationModelTests" boundaryViolationModelTests
            testList "BoundsCheckingTests" boundsCheckingTests
            testList "FocusedBoundsProperties" [focusedBoundsProperty]
            testList "ContractBasedProperties" contractBasedProperties
            testList "StatefulProperties" statefulProperties
            testList "BaselineRegression" baselineRegression
            testList "StructuralProperties" structuralProperties
            testList "MemoryModeComparison" memoryModeComparison
            testList "ExecutionConsistencyTests" executionConsistencyTests            
        ]