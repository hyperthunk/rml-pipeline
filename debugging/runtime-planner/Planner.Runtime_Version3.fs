namespace RMLPipeline.Internal

open System
open System.Runtime.CompilerServices
open RMLPipeline
open RMLPipeline.Core
open RMLPipeline.FastMap.Types
open RMLPipeline.Internal.StringInterning
open RMLPipeline.Internal.StringPooling
open Templates

(*
    Runtime planning metadata (no execution).

    Provides:
      - Reference extraction & indexing
      - Template segmentation
      - Requirement masks (supports >64 refs)
      - JoinDescriptor generation (merged multi-key per parent/child)
      - PathRefIndex (path -> (map, refIndex)[])
*)

[<Flags>]
type ReferenceFlags =
    | None        = 0uy
    | IsJoinKey   = 1uy
    | FromParent  = 2uy     // reserved: reference value carried from parent (future use)

[<Struct>]
type ReferenceInfo = {
    NameId: StringId
    SourcePathId: StringId
    Flags: ReferenceFlags
}

[<Struct>]
type TemplateSegment =
    | Lit  of StringId
    | Ref  of int

[<Struct>]
type CompiledTemplate = { Segments: TemplateSegment[] }

[<Struct>]
type RequirementMask =
    val IsSmall : bool
    val Small : uint64
    val Large : uint64[]
    new (small: uint64) = { IsSmall = true; Small = small; Large = null }
    new (large: uint64[]) = { IsSmall = false; Small = 0UL; Large = large }
    static member Empty = RequirementMask(0UL)
    member inline m.IsSubsetOf(other: RequirementMask) =
        if m.IsSmall && other.IsSmall then
            (other.Small &&& m.Small) = m.Small
        elif m.IsSmall && not other.IsSmall then
            let first = if other.Large.Length > 0 then other.Large.[0] else 0UL
            (first &&& m.Small) = m.Small
        elif not m.IsSmall && other.IsSmall then
            false
        else
            let arrM = m.Large
            let arrO = other.Large
            let mutable ok = true
            let len = arrM.Length
            let mutable i = 0
            while ok && i < len do
                let mm = arrM.[i]
                let oo = if i < arrO.Length then arrO.[i] else 0UL
                if (mm &&& oo) <> mm then ok <- false
                i <- i + 1
            ok
    static member Or(a: RequirementMask, b: RequirementMask) =
        if a.IsSmall && b.IsSmall then RequirementMask(a.Small ||| b.Small)
        elif a.IsSmall && not b.IsSmall then
            let arr = Array.copy b.Large
            if arr.Length > 0 then arr.[0] <- arr.[0] ||| a.Small
            RequirementMask(arr)
        elif not a.IsSmall && b.IsSmall then
            let arr = Array.copy a.Large
            if arr.Length > 0 then arr.[0] <- arr.[0] ||| b.Small
            RequirementMask(arr)
        else
            let len = max a.Large.Length b.Large.Length
            let arr = Array.zeroCreate<uint64> len
            for i=0 to len-1 do
                let av = if i < a.Large.Length then a.Large.[i] else 0UL
                let bv = if i < b.Large.Length then b.Large.[i] else 0UL
                arr.[i] <- av ||| bv
            RequirementMask(arr)

[<Struct>]
type TupleRequirement = {
    TupleIndex: int
    RequiredMask: RequirementMask
    Subject: CompiledTemplate
    Predicate: CompiledTemplate
    Object: CompiledTemplate
}

[<Struct>]
type JoinDescriptor = {
    ParentMapIndex: int
    ChildMapIndex: int
    ParentKeyRefIndices: int[]
    ChildKeyRefIndices: int[]
    HashSeed: uint64
    SuggestedCapacity: int
}

[<Struct>]
type MapRuntimePlan = {
    Base: TriplesMapPlan
    ReferenceInfos: ReferenceInfo[]
    ReferenceIndex: FastMap<StringId,int>
    SupportsBitset64: bool
    MaskWordCount: int
    SubjectTemplate: CompiledTemplate
    TupleRequirements: TupleRequirement[]
    JoinDescriptors: JoinDescriptor[]
}

type PathRefBindings = FastMap<StringId, struct (int * int)[]> // pathId -> (mapIndex, refIndex)[]

type RuntimePlan = {
    Planning: RMLPlan
    Maps: MapRuntimePlan[]
    PathRefIndex: PathRefBindings
}

module internal RuntimePlanBuild =

    // -------- Reference Builder ------------------------------------------------
    type private RefBuilder() =
        let order = System.Collections.Generic.List<StringId>(16)
        let mutable indexMap : FastMap<StringId,int> = FastMap.empty
        member _.Count = order.Count
        member _.AddOrGetIndex sid =
            match FastMap.tryFind sid indexMap with
            | ValueSome ix -> ix
            | ValueNone ->
                let ix = order.Count
                order.Add sid
                indexMap <- FastMap.add sid ix indexMap
                ix
        member _.ToArrayAndMap() = order.ToArray(), indexMap

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline private wordIndex (i:int) = i >>> 6
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let inline private bitMask (i:int) = 1UL <<< (i &&& 63)

    let private allocateMask count =
        if count <= 64 then RequirementMask(0UL)
        else RequirementMask(Array.zeroCreate<uint64> ((count + 63) / 64))

    let private addBit (mask: RequirementMask) (i:int) =
        if mask.IsSmall then
            RequirementMask(mask.Small ||| bitMask i)
        else
            let w = wordIndex i
            if w >= mask.Large.Length then
                let bigger = Array.zeroCreate<uint64> (w+1)
                Array.Copy(mask.Large, bigger, mask.Large.Length)
                bigger.[w] <- bigger.[w] ||| bitMask i
                RequirementMask(bigger)
            else
                mask.Large.[w] <- mask.Large.[w] ||| bitMask i
                mask

    // -------- Template Compilation ---------------------------------------------
    let private compileTemplate (pool: PoolContextScope) (refBuilder: RefBuilder) (template: string) =
        if String.IsNullOrEmpty template then
            { Segments = [||] }, RequirementMask.Empty
        else
            let parts = Templates.Parser.parseTemplate template
            let mutable mask = allocateMask refBuilder.Count
            let segs = System.Collections.Generic.List<TemplateSegment>(parts.Length)
            let mutable upgraded = refBuilder.Count > 64
            for p in parts do
                match p with
                | Templates.Parser.Literal lit ->
                    let lid = pool.InternString(lit, StringAccessPattern.Planning)
                    segs.Add(TemplateSegment.Lit lid)
                | Templates.Parser.Reference refName ->
                    let rid = pool.InternString(refName, StringAccessPattern.Planning)
                    let ix = refBuilder.AddOrGetIndex rid
                    if (not upgraded) && refBuilder.Count > 64 then
                        // upgrade
                        mask <-
                            if mask.IsSmall then
                                let arr = Array.zeroCreate<uint64> ((refBuilder.Count + 63)/64)
                                arr.[0] <- mask.Small
                                RequirementMask(arr)
                            else mask
                        upgraded <- true
                    mask <- addBit mask ix
                    segs.Add(TemplateSegment.Ref ix)
            { Segments = segs.ToArray() }, mask

    // -------- Subject Extraction -----------------------------------------------
    let private subjectTemplateString (tm: TriplesMap) =
        match tm.SubjectMap with
        | Some sm ->
            match sm.SubjectTermMap.ExpressionMap.Template with
            | Some t -> t
            | None ->
                match sm.SubjectTermMap.ExpressionMap.Constant with
                | Some (URI uri) -> uri
                | Some (Literal lit) -> lit
                | Some (BlankNode bn) -> "_:" + bn
                | None -> ""
        | None -> tm.Subject |> Option.defaultValue ""

    let private getStringUnsafe (plan: RMLPlan) (sid: StringId) =
        plan.PlanningContext.GetString sid |> ValueOption.defaultValue ""

    // -------- Tuple Requirements -----------------------------------------------
    let private buildTupleRequirements
        (rmlPlan: RMLPlan)
        (pool: PoolContextScope)
        (refBuilder: RefBuilder)
        (subjectCompiled: CompiledTemplate)
        (subjectMask: RequirementMask)
        (tmap: TriplesMapPlan) =
        let tuples = tmap.PredicateTuples
        let res = Array.zeroCreate<TupleRequirement> tuples.Length
        for i=0 to tuples.Length - 1 do
            let pt = tuples.[i]

            let predicateTemplateStr = getStringUnsafe rmlPlan pt.PredicateValueId
            let predicateCompiled, predicateMask =
                if pt.Flags &&& TupleFlags.IsTemplate <> TupleFlags.None then
                    compileTemplate pool refBuilder predicateTemplateStr
                else
                    { Segments = [| TemplateSegment.Lit pt.PredicateValueId |] }, RequirementMask.Empty

            let objectTemplateStr = getStringUnsafe rmlPlan pt.ObjectTemplateId
            let objectCompiled, objectMask =
                if pt.Flags &&& TupleFlags.IsTemplate <> TupleFlags.None then
                    compileTemplate pool refBuilder objectTemplateStr
                else
                    { Segments = [| TemplateSegment.Lit pt.ObjectTemplateId |] }, RequirementMask.Empty

            let reqMask = RequirementMask.Or(RequirementMask.Or(subjectMask, predicateMask), objectMask)
            res.[i] <-
                { TupleIndex = i
                  RequiredMask = reqMask
                  Subject = subjectCompiled
                  Predicate = predicateCompiled
                  Object = objectCompiled }
        res

    let private finalizeReferenceInfos iteratorPathId (refNameIds: StringId[]) =
        [| for id in refNameIds ->
            { NameId = id
              SourcePathId = iteratorPathId
              Flags = ReferenceFlags.None } |]

    // -------- MapRuntimePlan Construction --------------------------------------
    let buildMapRuntimePlan (rmlPlan: RMLPlan) (tmap: TriplesMapPlan) : MapRuntimePlan =
        let pool = rmlPlan.PlanningContext
        let refBuilder = RefBuilder()
        let subjStr = subjectTemplateString tmap.OriginalMap
        let subjCompiled, subjMask = compileTemplate pool refBuilder subjStr
        let tupleReqs = buildTupleRequirements rmlPlan pool refBuilder subjCompiled subjMask tmap
        let names, indexMap = refBuilder.ToArrayAndMap()
        let infos = finalizeReferenceInfos tmap.IteratorPathId names
        let supports64 = names.Length <= 64
        let maskWords = if supports64 then 1 else (names.Length + 63) / 64
        {
            Base = tmap
            ReferenceInfos = infos
            ReferenceIndex = indexMap
            SupportsBitset64 = supports64
            MaskWordCount = maskWords
            SubjectTemplate = subjCompiled
            TupleRequirements = tupleReqs
            JoinDescriptors = [||]
        }

    // -------- Join Descriptor Enrichment ---------------------------------------
    // Build map: PredicateTuple.Hash -> MapIndex (first occurrence)
    let private buildTupleHashToMap (plan: RMLPlan) =
        let dict = System.Collections.Generic.Dictionary<uint64,int>(capacity = 1024)
        for mp in plan.OrderedMaps do
            for t in mp.PredicateTuples do
                if not (dict.ContainsKey t.Hash) then
                    dict.Add(t.Hash, mp.Index)
        dict

    let private suggestJoinCapacity parentComplexity childComplexity =
        // heuristic
        let score = (parentComplexity + childComplexity) / 40 + 1
        let baseCap = 32
        Math.Min(1 <<< (Math.Min(score, 12)), 4096) * baseCap

    let private hashSeed (parentIx:int) (childIx:int) =
        let mutable h = 14695981039346656037UL
        h <- h ^^^ uint64 parentIx
        h <- h * 1099511628211UL
        h <- h ^^^ uint64 childIx
        h <- h * 1099511628211UL
        h

    let private ensureRef (plan: RMLPlan) (map: MapRuntimePlan) (name: string) (isJoinKey: bool) =
        let pool = plan.PlanningContext
        let nameId = pool.InternString(name, StringAccessPattern.Planning)
        match FastMap.tryFind nameId map.ReferenceIndex with
        | ValueSome ix ->
            // set flag if needed
            let infos = map.ReferenceInfos
            if infos.[ix].Flags &&& ReferenceFlags.IsJoinKey = ReferenceFlags.IsJoinKey then
                map, ix
            else
                let newInfos = Array.copy infos
                newInfos.[ix] <- { newInfos.[ix] with Flags = newInfos.[ix].Flags ||| ReferenceFlags.IsJoinKey }
                { map with ReferenceInfos = newInfos }, ix
        | ValueNone ->
            // append
            let oldInfos = map.ReferenceInfos
            let ix = oldInfos.Length
            let newInfo =
                { NameId = nameId
                  SourcePathId = map.Base.IteratorPathId
                  Flags = (if isJoinKey then ReferenceFlags.IsJoinKey else ReferenceFlags.None) }
            let newInfos = Array.zeroCreate<ReferenceInfo> (ix + 1)
            Array.Copy(oldInfos, newInfos, oldInfos.Length)
            newInfos.[ix] <- newInfo
            let newIndex = FastMap.add nameId ix map.ReferenceIndex
            let supports64 = ix + 1 <= 64
            let maskWords = if supports64 then 1 else (ix + 64) / 64
            { map with
                ReferenceInfos = newInfos
                ReferenceIndex = newIndex
                SupportsBitset64 = supports64
                MaskWordCount = maskWords }, ix

    let attachJoinDescriptors (runtime: RuntimePlan) : RuntimePlan =
        let plan = runtime.Planning
        let tupleHashMap = buildTupleHashToMap plan
        let maps = Array.copy runtime.Maps

        // For collecting (parentIx, (childIx, parentKeyIx, childKeyIx)) raw
        let perChildAcc = Array.init maps.Length (fun _ -> System.Collections.Generic.List<JoinDescriptor>())

        // Iterate child maps (joins stored at child side in TriplesMapPlan.JoinTuples)
        for childPlan in plan.OrderedMaps do
            if childPlan.JoinTuples.Length > 0 then
                let childIx = childPlan.Index
                let mutable childRuntime = maps.[childIx]

                // aggregator: key = parentIx -> (list of (parentRefIx, childRefIx))
                let parentKeyMap =
                    System.Collections.Generic.Dictionary<int,
                        System.Collections.Generic.List<int * int>>()

                for jt in childPlan.JoinTuples do
                    // Resolve parent map index from ParentTuple.Hash
                    let parentIx =
                        match tupleHashMap.TryGetValue jt.ParentTuple.Hash with
                        | true, ix -> ix
                        | _ -> failwithf "Unable to resolve parent map for join tuple hash=%d" jt.ParentTuple.Hash

                    let parentRefName =
                        jt.JoinCondition.Parent |> Option.defaultValue ""
                    let childRefName =
                        jt.JoinCondition.Child |> Option.defaultValue ""

                    if String.IsNullOrEmpty parentRefName || String.IsNullOrEmpty childRefName then
                        () // skip malformed join
                    else
                        // Ensure references exist & flagged
                        let parentRuntime = maps.[parentIx]
                        let parentMut, parentRefIx = ensureRef plan parentRuntime parentRefName true
                        maps.[parentIx] <- parentMut

                        let childMut, childRefIx = ensureRef plan childRuntime childRefName true
                        childRuntime <- childMut

                        let listRef =
                            match parentKeyMap.TryGetValue parentIx with
                            | true, l -> l
                            | _ ->
                                let l = System.Collections.Generic.List<int * int>()
                                parentKeyMap.Add(parentIx, l)
                                l
                        listRef.Add(parentRefIx, childRefIx)

                // Build merged descriptors
                let descriptors =
                    [|
                        for kv in parentKeyMap do
                            let parentIx = kv.Key
                            let pairs = kv.Value |> Seq.distinct |> Seq.toArray
                            // separate parent & child key indices
                            let parentKeys = pairs |> Array.map fst |> Array.distinct
                            let childKeys = pairs |> Array.map snd |> Array.distinct
                            Array.Sort(parentKeys)
                            Array.Sort(childKeys)
                            let parentC = maps.[parentIx].Base.EstimatedComplexity
                            let childC = childRuntime.Base.EstimatedComplexity
                            yield {
                                ParentMapIndex = parentIx
                                ChildMapIndex = childIx
                                ParentKeyRefIndices = parentKeys
                                ChildKeyRefIndices = childKeys
                                HashSeed = hashSeed parentIx childIx
                                SuggestedCapacity = suggestJoinCapacity parentC childC
                            }
                    |]

                maps.[childIx] <- { childRuntime with JoinDescriptors = descriptors }

        { runtime with Maps = maps }

    // -------- PathRefIndex -----------------------------------------------------
    let buildPathRefIndex (runtime: RuntimePlan) =
        let pairs = System.Collections.Generic.List<_>(256)
        for mi = 0 to runtime.Maps.Length - 1 do
            let m = runtime.Maps.[mi]
            for ri = 0 to m.ReferenceInfos.Length - 1 do
                let rinfo = m.ReferenceInfos.[ri]
                pairs.Add(rinfo.SourcePathId, struct (mi, ri))
        pairs.ToArray()
        |> Array.groupBy fst
        |> Array.fold (fun acc (pid, arr) ->
            let values = arr |> Array.map snd
            FastMap.add pid values acc) FastMap.empty

    // -------- Build Pipeline ---------------------------------------------------
    let buildInitialRuntimePlan (plan: RMLPlan) : RuntimePlan =
        let maps = plan.OrderedMaps |> Array.map (buildMapRuntimePlan plan)
        {
            Planning = plan
            Maps = maps
            PathRefIndex = FastMap.empty
        }

    let finalizeRuntimePlan (runtime: RuntimePlan) : RuntimePlan =
        let pathIdx = buildPathRefIndex runtime
        { runtime with PathRefIndex = pathIdx }

module RuntimePlanner =

    open RuntimePlanBuild

    let buildRuntimePlan (plan: RMLPlan) : RuntimePlan =
        plan
        |> buildInitialRuntimePlan
        |> attachJoinDescriptors
        |> finalizeRuntimePlan

    // For debugging stages
    let buildInitial plan = buildInitialRuntimePlan plan
    let enrichJoins runtime = attachJoinDescriptors runtime
    let buildPathBindings runtime = finalizeRuntimePlan runtime