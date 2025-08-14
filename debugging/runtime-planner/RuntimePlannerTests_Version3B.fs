namespace RMLPipeline.Tests

open Expecto
open FsCheck
open RMLPipeline
open RMLPipeline.Internal.Planner
open RMLPipeline.Internal.RuntimePlanner
open RMLPipeline.Internal
open RMLPipeline.Internal.StringPooling
open RMLPipeline.Internal.StringInterning
open RMLPipeline.Tests.SharedGenerators
open RuntimePlannerDiagnostics

module RuntimePlannerTests =

    // -------- FsCheck Config ---------------------------------------------------
    // Tuned for metadata; adjust via environment vars if needed.
    let private envOr name def =
        match System.Environment.GetEnvironmentVariable name with
        | null | "" -> def
        | v ->
            match System.Int32.TryParse v with
            | true, i -> i
            | _ -> def

    let fsCheckConfig =
        { FsCheckConfig.defaultConfig with
            maxTest = envOr "RUNTIME_RT_MAXTEST" 250
            endSize = envOr "RUNTIME_RT_ENDSIZE" 150
            replay = None }

    let prop name p =
        testPropertyWithConfig fsCheckConfig name p

    // Small pretty printer for failure attachments
    let attachDiag (rt: RuntimePlan) (msg: string) =
        let diag = summarize rt |> diagnosticsToString
        failtestf "%s\n--- Runtime Diagnostics ---\n%s" msg diag

    // -------- Properties -------------------------------------------------------

    // 1. References discovered deterministically
    let referenceExtraction =
        prop "Reference extraction equals parsed placeholders" <| fun () ->
            let maps, cfg = Gen.sample 0 1 genPlanInput |> List.head
            if maps.Length = 0 then true else
            let basePlan = createRMLPlan maps cfg
            let runtime = buildRuntimePlan basePlan
            let success =
                runtime.Maps
                |> Array.forall (fun mp ->
                    let ctx = basePlan.PlanningContext
                    let allParsed =
                        mp.Base.PredicateTuples
                        |> Array.collect (fun pt ->
                            let subStr = ctx.GetString pt.SubjectTemplateId |> ValueOption.defaultValue ""
                            let predStr = ctx.GetString pt.PredicateValueId |> ValueOption.defaultValue ""
                            let objStr = ctx.GetString pt.ObjectTemplateId |> ValueOption.defaultValue ""
                            [|
                                yield! Templates.Parser.parseTemplate subStr |> List.choose (function Templates.Parser.Reference r -> Some r | _ -> None)
                                yield! Templates.Parser.parseTemplate predStr |> List.choose (function Templates.Parser.Reference r -> Some r | _ -> None)
                                yield! Templates.Parser.parseTemplate objStr  |> List.choose (function Templates.Parser.Reference r -> Some r | _ -> None)
                            |])
                        |> Array.distinct
                    let rtNames =
                        mp.ReferenceInfos
                        |> Array.map (fun ri -> ctx.GetString ri.NameId |> ValueOption.defaultValue "")
                        |> Array.distinct
                    Array.sort allParsed = Array.sort rtNames
                    || (allParsed.Length = 0 && rtNames.Length = 0))
            if not success then attachDiag runtime "Reference extraction mismatch"
            success

    // 2. Join descriptors consistent
    let joinDescriptorsConsistent =
        prop "Join descriptors: indices & flags valid" <| fun () ->
            let maps, cfg = Gen.sample 0 1 genTriplesMapWithJoin |> List.head, Gen.sample 0 1 genPlannerConfig |> List.head
            let basePlan = createRMLPlan maps cfg
            let runtime = buildRuntimePlan basePlan
            let ok =
                runtime.Maps
                |> Array.forall (fun mp ->
                    mp.JoinDescriptors
                    |> Array.forall (fun jd ->
                        jd.ChildMapIndex = mp.Base.Index &&
                        jd.ParentMapIndex < runtime.Maps.Length &&
                        jd.ChildKeyRefIndices |> Array.forall (fun ix -> ix < mp.ReferenceInfos.Length &&
                                                                      mp.ReferenceInfos.[ix].Flags &&& ReferenceFlags.IsJoinKey = ReferenceFlags.IsJoinKey) &&
                        let parent = runtime.Maps.[jd.ParentMapIndex]
                        jd.ParentKeyRefIndices |> Array.forall (fun ix -> ix < parent.ReferenceInfos.Length &&
                                                                          parent.ReferenceInfos.[ix].Flags &&& ReferenceFlags.IsJoinKey = ReferenceFlags.IsJoinKey)))
            if not ok then attachDiag runtime "Join descriptor inconsistency"
            ok

    // 3. Large mask when >64 refs
    let largeMaskValidation =
        prop ">64 reference maps use large mask representation" <| fun () ->
            let wide, cfg = Gen.sample 0 1 genWideTriplesMap |> List.head, Gen.sample 0 1 genPlannerConfig |> List.head
            let basePlan = createRMLPlan [|wide|] cfg
            let runtime = buildRuntimePlan basePlan
            let mp = runtime.Maps.[0]
            let ok =
                if mp.ReferenceInfos.Length <= 64 then
                    // Allowed but test expects a wide map
                    false
                else
                    // At least one tuple requirement has Large mask or empty
                    mp.TupleRequirements |> Array.exists (fun tr -> not tr.RequiredMask.IsSmall || tr.RequiredMask = RequirementMask.Empty)
            if not ok then attachDiag runtime "Large mask expectation failure"
            ok

    // 4. Determinism
    let deterministicMetadata =
        prop "Runtime metadata deterministic for same input" <| fun () ->
            let maps, cfg = Gen.sample 0 1 genPlanInput |> List.head
            if maps.Length = 0 then true else
            let bp1 = createRMLPlan maps cfg
            let bp2 = createRMLPlan maps cfg
            let r1 = buildRuntimePlan bp1
            let r2 = buildRuntimePlan bp2
            let structural =
                r1.Maps.Length = r2.Maps.Length &&
                Array.forall2 (fun a b ->
                    a.ReferenceInfos.Length = b.ReferenceInfos.Length &&
                    a.JoinDescriptors.Length = b.JoinDescriptors.Length &&
                    (Array.map (fun jd -> jd.ParentMapIndex, jd.ChildMapIndex, jd.ParentKeyRefIndices.Length, jd.ChildKeyRefIndices.Length) a.JoinDescriptors
                     = Array.map (fun jd -> jd.ParentMapIndex, jd.ChildMapIndex, jd.ParentKeyRefIndices.Length, jd.ChildKeyRefIndices.Length) b.JoinDescriptors)
                ) r1.Maps r2.Maps
            if not structural then
                attachDiag r1 "Determinism failure (first)"
            structural

    // 5. PathRefIndex coverage
    let pathRefIndexCoverage =
        prop "PathRefIndex includes all reference bindings" <| fun () ->
            let maps, cfg = Gen.sample 0 1 genPlanInput |> List.head
            if maps.Length = 0 then true else
            let bp = createRMLPlan maps cfg
            let rt = buildRuntimePlan bp
            let pathIndex = rt.PathRefIndex
            let ok =
                rt.Maps
                |> Array.forall (fun mp ->
                    mp.ReferenceInfos
                    |> Array.indexed
                    |> Array.forall (fun (i, ri) ->
                        match FastMap.tryFind ri.SourcePathId pathIndex with
                        | ValueSome arr -> arr |> Array.exists (fun struct(mi, riIx) -> mi = mp.Base.Index && riIx = i)
                        | ValueNone -> false))
            if not ok then attachDiag rt "PathRefIndex missing reference(s)"
            ok

    // 6. Bitset subset soundness
    let requirementMaskSubset =
        prop "Tuple requirement mask matches segment references (subset test)" <| fun () ->
            let maps, cfg = Gen.sample 0 1 genPlanInput |> List.head
            if maps.Length = 0 then true else
            let bp = createRMLPlan maps cfg
            let rt = buildRuntimePlan bp
            let ctx = bp.PlanningContext
            let ok =
                rt.Maps
                |> Array.forall (fun mp ->
                    mp.TupleRequirements
                    |> Array.forall (fun tr ->
                        let gatherRefs (ct:CompiledTemplate) =
                            ct.Segments
                            |> Array.choose (function TemplateSegment.Ref ix -> Some ix | _ -> None)
                            |> Array.distinct
                        let refs =
                            [|
                                yield! gatherRefs tr.Subject
                                yield! gatherRefs tr.Predicate
                                yield! gatherRefs tr.Object
                            |] |> Array.distinct
                        // Confirm each ref index bit is in mask
                        let bitPresent ix =
                            if mp.SupportsBitset64 then
                                (tr.RequiredMask.Small &&& (1UL <<< ix)) <> 0UL
                            else
                                let w = ix / 64
                                let b = ix % 64
                                w < tr.RequiredMask.Large.Length &&
                                (tr.RequiredMask.Large.[w] &&& (1UL <<< b)) <> 0UL
                        refs |> Array.forall bitPresent))
            if not ok then attachDiag rt "Requirement mask subset failure"
            ok

    // -------- Test List -------------------------------------------------------
    [<Tests>]
    let runtimePlannerMetadata =
        testList "RuntimePlannerMetadata" [
            referenceExtraction
            joinDescriptorsConsistent
            largeMaskValidation
            deterministicMetadata
            pathRefIndexCoverage
            requirementMaskSubset
        ]