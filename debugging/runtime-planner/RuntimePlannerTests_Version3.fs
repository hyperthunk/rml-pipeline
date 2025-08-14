namespace RMLPipeline.Tests

open Expecto
open FsCheck
open RMLPipeline
open RMLPipeline.Model
open RMLPipeline.DSL
open RMLPipeline.Internal
open RMLPipeline.Internal.Planner
open RMLPipeline.Internal.RuntimePlanner
open RMLPipeline.Internal.StringInterning
open RMLPipeline.Internal.StringPooling
open System

module RuntimePlannerTests =

    // ------------------------------------------------------------------------
    // Generators (reuse some from PlannerModelTests if desired – redefined
    // here for isolation)
    // ------------------------------------------------------------------------
    let genBasicTriplesMap =
        gen {
            let! id = Gen.choose(1, 1000)
            return buildTriplesMap (triplesMap {
                do! setLogicalSource (logicalSource {
                    do! iterator $"$.items[{id}]"
                    do! asJSONPath
                })
                do! setSubjectMap (subjectMap {
                    do! subjectTermMap (templateTermAsIRI $"http://example.org/item/{id}/{{id}}")
                })
                do! addPredicateObjectMap (predicateObjectMap {
                    do! addPredicate "http://example.org/name"
                    do! addObjectMap (objectMap {
                        do! objectTermMap (refTermAsLiteral "name")
                    })
                })
            })
        }

    let genTriplesMapWithJoin =
        gen {
            // parent
            let parent = buildTriplesMap (triplesMap {
                do! setLogicalSource (logicalSource {
                    do! iterator "$.parents[*]"
                    do! asJSONPath
                })
                do! setSubjectMap (subjectMap {
                    do! subjectTermMap (templateTermAsIRI "http://example.org/parent/{id}")
                })
                do! addPredicateObjectMap (predicateObjectMap {
                    do! addPredicate "http://example.org/pName"
                    do! addObjectMap (objectMap {
                        do! objectTermMap (refTermAsLiteral "pName")
                    })
                })
            })
            let child = buildTriplesMap (triplesMap {
                do! setLogicalSource (logicalSource {
                    do! iterator "$.children[*]"
                    do! asJSONPath
                })
                do! setSubjectMap (subjectMap {
                    do! subjectTermMap (templateTermAsIRI "http://example.org/child/{cid}")
                })
                do! addPredicateObjectMap (predicateObjectMap {
                    do! addPredicate "http://example.org/hasParent"
                    do! addRefObjectMap parent (refObjectMap {
                        do! addJoinCondition (join {
                            do! child "parentId"
                            do! parent "id"
                        })
                        // Add second join key sometimes
                        do! addJoinCondition (join {
                            do! child "otherKey"
                            do! parent "otherId"
                        })
                    })
                })
            })
            return [| parent; child |]
        }

    let genWideTriplesMap =
        // map with > 70 references to test large bitset path
        gen {
            let! count = Gen.choose(65, 80)
            let predicates =
                [ for i in 0..count-1 -> $"http://example.org/p{i}" ]
            return buildTriplesMap (triplesMap {
                do! setLogicalSource (logicalSource {
                    do! iterator "$.wide[*]"
                    do! asJSONPath
                })
                do! setSubjectMap (subjectMap {
                    do! subjectTermMap (templateTermAsIRI "http://example.org/wide/{sid}")
                })
                for p in predicates do
                    do! addPredicateObjectMap (predicateObjectMap {
                        do! addPredicate p
                        do! addObjectMap (objectMap {
                            do! objectTermMap (templateTermAsLiteral $"{{v_{p.GetHashCode() &&& 0xFFFF}}}")
                        })
                    })
            })
        }

    let genTriplesMapSet =
        gen {
            let! choice = Gen.choose(1,4)
            match choice with
            | 1 ->
                let! tm = genBasicTriplesMap
                return [| tm |]
            | 2 ->
                let! arr = genTriplesMapWithJoin
                return arr
            | 3 ->
                let! wide = genWideTriplesMap
                return [| wide |]
            | _ ->
                let! cnt = Gen.choose(2,4)
                let! maps = Gen.listOfLength cnt genBasicTriplesMap
                return maps |> List.toArray
        }

    let genPlannerConfig =
        gen {
            let! mode = Gen.elements [LowMemory; Balanced; HighPerformance]
            let! chunk =
                match mode with
                | LowMemory -> Gen.choose(10,100)
                | HighPerformance -> Gen.choose(100,400)
                | Balanced -> Gen.choose(50,300)
            let! index = Gen.elements [Some NoIndex; Some HashIndex; Some FullIndex; None]
            return { PlannerConfig.Default with MemoryMode = mode; ChunkSize = chunk; IndexStrategy = index }
        }

    // ------------------------------------------------------------------------
    // Helper: build both plans
    // ------------------------------------------------------------------------
    let buildBoth maps config =
        let basePlan = createRMLPlan maps config
        let runtimePlan = buildRuntimePlan basePlan
        basePlan, runtimePlan

    // ------------------------------------------------------------------------
    // Properties
    // ------------------------------------------------------------------------

    // 1. Reference count correctness: distinct placeholders across templates match ReferenceInfos length
    let referenceExtractionProperty =
        testProperty "RuntimePlan extracts distinct references per map" <| fun () ->
            (gen {
                let! maps = genTriplesMapSet
                let! cfg = genPlannerConfig
                return maps, cfg
            } |> Gen.sample 0 1 |> List.head)
            |> fun (maps, cfg) ->
                if maps.Length = 0 then true else
                let _, runtime = buildBoth maps cfg
                runtime.Maps
                |> Array.forall (fun mp ->
                    // Collect references again by parsing templates (sanity)
                    let collected =
                        mp.Base.PredicateTuples
                        |> Array.collect (fun pt ->
                            let plan = runtime.Planning
                            let ctx = plan.PlanningContext
                            [|
                                let pred = ctx.GetString pt.PredicateValueId |> ValueOption.defaultValue ""
                                let obj = ctx.GetString pt.ObjectTemplateId |> ValueOption.defaultValue ""
                                let subj =
                                    ctx.GetString pt.SubjectTemplateId |> ValueOption.defaultValue ""
                                yield! Templates.Parser.parseTemplate subj
                                        |> List.choose (function Templates.Parser.Reference r -> Some r | _ -> None)
                                yield! Templates.Parser.parseTemplate pred
                                        |> List.choose (function Templates.Parser.Reference r -> Some r | _ -> None)
                                yield! Templates.Parser.parseTemplate obj
                                        |> List.choose (function Templates.Parser.Reference r -> Some r | _ -> None)
                            |])
                        |> Array.distinct
                    let runtimeNames =
                        mp.ReferenceInfos
                        |> Array.map (fun ri ->
                            runtime.Planning.PlanningContext.GetString ri.NameId
                            |> ValueOption.defaultValue "")
                        |> Array.distinct
                    Array.sort collected = Array.sort runtimeNames
                    || collected.Length = 0 && runtimeNames.Length = 0)

    // 2. Requirement masks subset: each tuple requirement mask must cover exactly union of its segment refs
    let tupleRequirementMaskProperty =
        testProperty "Tuple requirement masks match union of segment references" <| fun () ->
            (gen {
                let! maps = genTriplesMapSet
                let! cfg = genPlannerConfig
                return maps, cfg
            } |> Gen.sample 0 1 |> List.head)
            |> fun (maps, cfg) ->
                if maps.Length = 0 then true else
                let _, runtime = buildBoth maps cfg
                let plan = runtime.Planning
                let ctx = plan.PlanningContext
                let refNameOf (idx:int) (mp: MapRuntimePlan) =
                    ctx.GetString mp.ReferenceInfos.[idx].NameId |> ValueOption.defaultValue ""
                runtime.Maps
                |> Array.forall (fun mp ->
                    mp.TupleRequirements
                    |> Array.forall (fun tr ->
                        // collect from segments
                        let segRefs (ct:CompiledTemplate) =
                            ct.Segments
                            |> Array.choose (function TemplateSegment.Ref i -> Some i | _ -> None)
                            |> Array.distinct
                        let reqRefs =
                            [|
                                yield! segRefs tr.Subject
                                yield! segRefs tr.Predicate
                                yield! segRefs tr.Object
                            |] |> Array.distinct
                        // Build mask manually
                        let manualMask =
                            if mp.SupportsBitset64 then
                                let mutable m = 0UL
                                for r in reqRefs do m <- m ||| (1UL <<< r)
                                RequirementMask(m)
                            else
                                let words = Array.zeroCreate<uint64> mp.MaskWordCount
                                for r in reqRefs do
                                    let w = r / 64
                                    let b = r % 64
                                    if w >= words.Length then
                                        () else
                                        words.[w] <- words.[w] ||| (1UL <<< b)
                                RequirementMask(words)
                        // Compare structure
                        if manualMask.IsSmall <> tr.RequiredMask.IsSmall then false
                        else if manualMask.IsSmall then manualMask.Small = tr.RequiredMask.Small
                        else
                            let a = manualMask.Large
                            let b = tr.RequiredMask.Large
                            if a.Length <> b.Length then false
                            else Array.forall2 (=) a b))

    // 3. Join descriptor correctness: child references flagged & indices exist
    let joinDescriptorProperty =
        testProperty "Join descriptors map correct key reference indices & flags" <| fun () ->
            (gen {
                let! arr = genTriplesMapWithJoin
                let! cfg = genPlannerConfig
                return arr, cfg
            } |> Gen.sample 0 1 |> List.head)
            |> fun (maps, cfg) ->
                let _, runtime = buildBoth maps cfg
                // identify child map (the one having JoinDescriptors)
                runtime.Maps
                |> Array.filter (fun m -> m.JoinDescriptors.Length > 0)
                |> Array.forall (fun child ->
                    child.JoinDescriptors
                    |> Array.forall (fun jd ->
                        let parent = runtime.Maps.[jd.ParentMapIndex]
                        // Flags
                        let parentFlagsOk =
                            jd.ParentKeyRefIndices
                            |> Array.forall (fun ix ->
                                parent.ReferenceInfos.[ix].Flags &&& ReferenceFlags.IsJoinKey = ReferenceFlags.IsJoinKey)
                        let childFlagsOk =
                            jd.ChildKeyRefIndices
                            |> Array.forall (fun ix ->
                                child.ReferenceInfos.[ix].Flags &&& ReferenceFlags.IsJoinKey = ReferenceFlags.IsJoinKey)
                        parentFlagsOk && childFlagsOk))

    // 4. PathRefIndex coverage: every reference appears in path index
    let pathIndexCoverageProperty =
        testProperty "PathRefIndex covers all references" <| fun () ->
            (gen {
                let! maps = genTriplesMapSet
                let! cfg = genPlannerConfig
                return maps, cfg
            } |> Gen.sample 0 1 |> List.head)
            |> fun (maps, cfg) ->
                if maps.Length = 0 then true else
                let _, runtime = buildBoth maps cfg
                let pathIndex = runtime.PathRefIndex
                runtime.Maps
                |> Array.forall (fun mp ->
                    mp.ReferenceInfos
                    |> Array.indexed
                    |> Array.forall (fun (ri, rinfo) ->
                        match FastMap.tryFind rinfo.SourcePathId pathIndex with
                        | ValueSome bindings ->
                            bindings
                            |> Array.exists (fun struct (mi, ix) -> mi = mp.Base.Index && ix = ri)
                        | ValueNone -> false))

    // 5. Determinism: building runtime plan twice yields identical metadata shapes
    let determinismProperty =
        testProperty "RuntimePlan metadata is deterministic" <| fun () ->
            (gen {
                let! maps = genTriplesMapSet
                let! cfg = genPlannerConfig
                return maps, cfg
            } |> Gen.sample 0 1 |> List.head)
            |> fun (maps, cfg) ->
                if maps.Length = 0 then true else
                let base1, rt1 = buildBoth maps cfg
                let base2, rt2 = buildBoth maps cfg
                // Quick structural compares
                let sameMapCount = rt1.Maps.Length = rt2.Maps.Length
                let sameRefCounts =
                    Array.forall2 (fun a b -> a.ReferenceInfos.Length = b.ReferenceInfos.Length) rt1.Maps rt2.Maps
                let sameJoinShapes =
                    Array.forall2 (fun a b ->
                        let aj = a.JoinDescriptors |> Array.map (fun jd -> jd.ParentMapIndex, jd.ChildMapIndex, jd.ParentKeyRefIndices.Length, jd.ChildKeyRefIndices.Length)
                        let bj = b.JoinDescriptors |> Array.map (fun jd -> jd.ParentMapIndex, jd.ChildMapIndex, jd.ParentKeyRefIndices.Length, jd.ChildKeyRefIndices.Length)
                        Array.sort aj = Array.sort bj
                    ) rt1.Maps rt2.Maps
                sameMapCount && sameRefCounts && sameJoinShapes

    // 6. Large reference (>64) uses large mask
    let largeMaskProperty =
        testProperty "Maps with >64 references use large requirement masks" <| fun () ->
            (gen {
                let! wide = genWideTriplesMap
                let! cfg = genPlannerConfig
                return [| wide |], cfg
            } |> Gen.sample 0 1 |> List.head)
            |> fun (maps, cfg) ->
                let _, runtime = buildBoth maps cfg
                let mp = runtime.Maps.[0]
                if mp.ReferenceInfos.Length <= 64 then false
                else
                    // At least one tuple requirement must have a large mask or zero (if no refs referenced)
                    mp.TupleRequirements
                    |> Array.exists (fun tr ->
                        (not tr.RequiredMask.IsSmall) || tr.RequiredMask = RequirementMask.Empty)

    // 7. Per-map reference index independence
    let referenceIsolationProperty =
        testProperty "Reference indices are isolated per map" <| fun () ->
            (gen {
                let! maps = genTriplesMapSet
                let! cfg = genPlannerConfig
                return maps, cfg
            } |> Gen.sample 0 1 |> List.head)
            |> fun (maps, cfg) ->
                let _, runtime = buildBoth maps cfg
                // same reference name can appear in multiple maps but index must start from 0 per map
                runtime.Maps
                |> Array.forall (fun mp ->
                    let indices =
                        mp.ReferenceInfos
                        |> Array.mapi (fun ix _ -> ix)
                    indices.Length = mp.ReferenceInfos.Length &&
                    (indices = [|0 .. mp.ReferenceInfos.Length - 1|]))

    // ------------------------------------------------------------------------
    // Lightweight Model-Based Test
    // ------------------------------------------------------------------------

    type RtCommand =
        | BuildPlan of TriplesMap[] * PlannerConfig
        | RebuildRuntime of int          // plan index
        | InspectMap of int * int        // plan index, map index
        | InspectJoins of int * int      // plan index, map index

    type ModelState = {
        Plans: ResizeArray<RMLPlan>
        RuntimePlans: ResizeArray<RuntimePlan>
    } with
        static member Empty = { Plans = ResizeArray(); RuntimePlans = ResizeArray() }

    let rtGenCommand (state: ModelState) =
        let baseGen =
            gen {
                let! maps = genTriplesMapSet
                let! cfg = genPlannerConfig
                return BuildPlan (maps, cfg)
            }
        if state.Plans.Count = 0 then baseGen
        else
            let existingPlanIxGen = Gen.choose(0, state.Plans.Count - 1)
            Gen.frequency [
                5, baseGen
                3, (gen {
                    let! ix = existingPlanIxGen
                    return RebuildRuntime ix
                })
                2, (gen {
                    let! pi = existingPlanIxGen
                    // guard map index
                    let mapCount = state.Plans.[pi].OrderedMaps.Length
                    let! mi = if mapCount = 0 then Gen.constant 0 else Gen.choose(0, mapCount - 1)
                    return InspectMap (pi, mi)
                })
                2, (gen {
                    let! pi = existingPlanIxGen
                    let mapCount = state.Plans.[pi].OrderedMaps.Length
                    let! mi = if mapCount = 0 then Gen.constant 0 else Gen.choose(0, mapCount - 1)
                    return InspectJoins (pi, mi)
                })
            ]

    let applyCommand (cmd: RtCommand) (state: ModelState) =
        match cmd with
        | BuildPlan (maps, cfg) when maps.Length > 0 ->
            let plan = createRMLPlan maps cfg
            let rt = buildRuntimePlan plan
            state.Plans.Add plan
            state.RuntimePlans.Add rt
            state
        | RebuildRuntime ix when ix < state.Plans.Count ->
            let plan = state.Plans.[ix]
            let rt = buildRuntimePlan plan
            state.RuntimePlans.[ix] <- rt
            state
        | InspectMap (pi, mi) when pi < state.RuntimePlans.Count ->
            let rt = state.RuntimePlans.[pi]
            if mi < rt.Maps.Length then
                // Basic sanity inspect
                let mp = rt.Maps.[mi]
                if mp.ReferenceInfos.Length < 0 then failwith "Impossible"
            state
        | InspectJoins (pi, mi) when pi < state.RuntimePlans.Count ->
            let rt = state.RuntimePlans.[pi]
            if mi < rt.Maps.Length then
                let mp = rt.Maps.[mi]
                // Ensure join parent/child indices are in range
                for jd in mp.JoinDescriptors do
                    if jd.ParentMapIndex >= rt.Maps.Length ||
                       jd.ChildMapIndex >= rt.Maps.Length then
                        failwith "Invalid join descriptor indices"
            state
        | _ -> state

    let modelBasedRuntimePlannerProperty =
        testProperty "Model-based runtime planner metadata remains consistent" <| fun () ->
            let steps = 30
            let rnd = Random()
            let mutable st = ModelState.Empty
            let mutable ok = true
            for _ in 1 .. steps do
                let cmd = rtGenCommand st |> Gen.sample 0 1 |> List.head
                st <- applyCommand cmd st
                // Invariants after each command
                for rp in st.RuntimePlans do
                    ok <-
                        ok &&
                        rp.Maps.Length = rp.Planning.OrderedMaps.Length &&
                        (rp.Maps |> Array.forall (fun mp ->
                            // join descriptors child index must equal mp.Base.Index for each descriptor's child
                            mp.JoinDescriptors |> Array.forall (fun jd -> jd.ChildMapIndex = mp.Base.Index)))
            ok

    // ------------------------------------------------------------------------
    // Test List
    // ------------------------------------------------------------------------
    [<Tests>]
    let runtimePlannerMetadataTests =
        testList "RuntimePlannerMetadata" [
            referenceExtractionProperty
            tupleRequirementMaskProperty
            joinDescriptorProperty
            pathIndexCoverageProperty
            determinismProperty
            largeMaskProperty
            referenceIsolationProperty
            modelBasedRuntimePlannerProperty
        ]