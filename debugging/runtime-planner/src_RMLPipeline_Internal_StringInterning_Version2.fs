namespace RMLPipeline.Internal

module StringInterning =

    open RMLPipeline
    open RMLPipeline.Core
    open RMLPipeline.FastMap.Types
    open System
    open System.Diagnostics
    open System.Runtime.CompilerServices
    open System.Threading

    // ========================================================================================
    // ADD: Helper for promotion delegate
    // ========================================================================================
    type PromoteDelegate = string -> StringId

    // (EXISTING TYPES UNCHANGED ABOVE ... keep all previous definitions)
    // Only showing sections that changed below for brevity.
    // ========================================================================================
    // NOTE: Remove any prior '(* Relies on the caller not to repeatedly intern the same string *)'
    // comment – we now DO deduplicate via EdenIndex + Segment index.
    // ========================================================================================

    // (Keep PoolConfiguration, Stats, PoolStats, WorkerId, DependencyGroupId, IdAllocation, HashIndexing, Promotions, Packing, etc. as before)

    // ---- PATCHES START HERE ----

    module HashIndexing =
        // (retain existing code)
        // NEW: Constant for Eden array type marker (distinct from segment chunk types which start at 1).
        [<Literal>]
        let EdenArrayType = 0uy

    module Packing =
        // (retain existing code in PackedOps and Segments; we will patch Segments below)

        type Segments with

            /// Increment temperature & possibly promote (synchronous path) for an existing location
            member internal this.IncrementTempAndMaybePromote
                    (baseId:int)
                    (segmentIdx:int)
                    (entryIndex:int)
                    (stringIndex:int)
                    (packedEntry:int64)
                    (threshold:int)
                    (promoteFn: PromoteDelegate option)
                    : StringId =
                // CAS loop to increment temperature
                let chunk = this.Chunks.[segmentIdx]
                let mutable original = packedEntry
                let mutable continueLoop = true
                let mutable resultingId = Unchecked.defaultof<StringId>
                while continueLoop do
                    if PackedOps.isPromoted original then
                        // Already promoted, follow redirect
                        let redir = PackedOps.unpackPromotedId original
                        resultingId <- redir
                        continueLoop <- false
                    else
                        let sIdx, temp = PackedOps.unpackStringIndexAndTemp original
                        let nextTemp = if temp < StringId.MaxTemperature then temp + 1 else temp
                        let newId = StringId.CreateWithTemperature(baseId + (segmentIdx * this.ChunkSize) + entryIndex, nextTemp)
                        let newPacked = PackedOps.packStringIndexWithTemp sIdx nextTemp
                        let prev = Interlocked.CompareExchange(&chunk.PackedEntries.[entryIndex], newPacked, original)
                        if prev = original then
                            resultingId <- newId
                            continueLoop <- false
                            // Check promotion post-write
                            if nextTemp >= threshold then
                                match promoteFn with
                                | Some pf ->
                                    let strVal = Volatile.Read(&chunk.StringsArray.[stringIndex])
                                    if not (isNull strVal) then
                                        let promotedId = pf strVal
                                        // Mark entry as promoted redirect
                                        let mutable cur = Volatile.Read(&chunk.PackedEntries.[entryIndex])
                                        let promotedPacked = PackedOps.packPromotedId promotedId
                                        let mutable donePromote = false
                                        while not donePromote do
                                            if PackedOps.isPromoted cur then
                                                donePromote <- true
                                            else
                                                let prev2 = Interlocked.CompareExchange(&chunk.PackedEntries.[entryIndex], promotedPacked, cur)
                                                if prev2 = cur then donePromote <- true
                                                else cur <- prev2
                                        resultingId <- promotedId
                                | None -> ()
                        else
                            original <- prev
                resultingId

            member internal this.ResolvePromotedRedirect (packed:int64) : StringId option =
                if PackedOps.isPromoted packed then
                    Some (PackedOps.unpackPromotedId packed)
                else None

            member internal this.TryFollowRedirect (packed:int64) =
                if PackedOps.isPromoted packed then
                    ValueSome (PackedOps.unpackPromotedId packed)
                else ValueNone

            /// Patched AllocateString to always reuse existing entry when found
            member this.AllocateOrReuseString(str:string, baseId:int, promoteFn: PromoteDelegate option) : StringId =
                // Lookup
                match this.StringIndex.TryGetLocation str with
                | ValueSome loc when loc.ArrayType > 0uy ->
                    if int loc.ArrayType > this.Chunks.Count then
                        this.AllocateNewString(str, baseId) // stale
                    else
                        let segIdx = int loc.ArrayType - 1
                        let chunk = this.Chunks.[segIdx]
                        if loc.EntryIndex < 0 || loc.EntryIndex >= chunk.PackedEntries.Length then
                            this.AllocateNewString(str, baseId)
                        else
                            let packed = Volatile.Read(&chunk.PackedEntries.[loc.EntryIndex])
                            if PackedOps.isPromoted packed then
                                // Follow redirect
                                PackedOps.unpackPromotedId packed
                            else
                                // Validate string index
                                if loc.StringIndex >= 0 && loc.StringIndex < chunk.NextStringIndex then
                                    let stored = Volatile.Read(&chunk.StringsArray.[loc.StringIndex])
                                    if stored = str then
                                        // increment temperature inline
                                        this.IncrementTempAndMaybePromote
                                            baseId
                                            segIdx
                                            loc.EntryIndex
                                            loc.StringIndex
                                            packed
                                            this.PromotionThreshold
                                            promoteFn
                                    else
                                        this.AllocateNewString(str, baseId)
                                else
                                    this.AllocateNewString(str, baseId)
                | _ ->
                    this.AllocateNewString(str, baseId)

            // Patch original AllocateString to call new method
            member this.AllocateString(str:string, baseId:int) : StringId =
                // For backwards compatibility (without promotion delegate for segments internally)
                this.AllocateOrReuseString(str, baseId, None)

    // ========================================================================================
    // POOL (major modifications)
    // ========================================================================================
    type Pool = {
        EdenArray: PackedPoolArray
        mutable EdenOffset: int32
        PostEden: Packing.Segments
        PoolBaseId: int
        Configuration: PoolConfiguration
        mutable LastDecayTime: DateTime
        mutable LastPromotionTime: int64
        EdenPromotionTracker: Promotions.EpochPromotionTracker
        mutable Stats: Stats

        // NEW: Eden String reverse index (lock-free sharded)
        EdenIndex: HashIndexing.StringIndex
        // NEW: Promotion delegate to higher tier (optional)
        PromoteFn: PromoteDelegate option
        // NEW: Promotion threshold (adaptable)
        mutable EdenPromotionThreshold: int
    } with
        static member Create(baseId:int, edenSize:int, config:PoolConfiguration, promote:PromoteDelegate option) =
            let rotationInterval = TimeSpan.FromMilliseconds(config.MinPromotionInterval.TotalMilliseconds / 3.0)
            {
                EdenArray = {
                    PackedEntries = Array.zeroCreate edenSize
                    StringsArray = Array.zeroCreate edenSize
                    NextStringIndex = 0
                }
                EdenOffset = 0
                PostEden = Packing.Segments.Create(baseId + edenSize, config)
                PoolBaseId = baseId
                Configuration = config
                LastDecayTime = DateTime.UtcNow
                LastPromotionTime = 0L
                EdenPromotionTracker = Promotions.EpochPromotionTracker.Create( queueSizeBits = 14, rotationInterval = rotationInterval )
                Stats = { accessCount = 0L; missCount = 0L }
                EdenIndex = HashIndexing.StringIndex.Create(config, HashIndexing.Global) // poolType parameter approximate
                PromoteFn = promote
                EdenPromotionThreshold =
                    if baseId < IdAllocation.GroupPoolBase then config.GroupPromotionThreshold
                    elif baseId < IdAllocation.LocalPoolBase then config.WorkerPromotionThreshold
                    else config.WorkerPromotionThreshold
            }

        member private this.AdaptiveEdenThreshold() =
            // Simple scaling on EdenOffset + PostEden.StringCount
            let edenCount = Volatile.Read(&this.EdenOffset)
            let segCount = Volatile.Read(&this.PostEden.StringCount)
            let total = edenCount + segCount
            let baseThr = this.EdenPromotionThreshold
            let scale =
                1.0 +
                (float total / float this.Configuration.ThresholdScalingInterval) *
                (this.Configuration.ThresholdScalingFactor - 1.0)
            max 2 (int (float baseThr * scale))

        member private this.IncrementEdenTempAndMaybePromote(edenSlot:int, stringIdx:int, packedEntry:int64) : StringId =
            // CAS loop
            let mutable original = packedEntry
            let mutable done = false
            let mutable resultingId = Unchecked.defaultof<_>
            let threshold = this.AdaptiveEdenThreshold()
            while not done do
                if Packing.PackedOps.isPromoted original then
                    resultingId <- Packing.PackedOps.unpackPromotedId original
                    done <- true
                else
                    let sIdx, temp = Packing.PackedOps.unpackStringIndexAndTemp original
                    let nextTemp = if temp < StringId.MaxTemperature then temp + 1 else temp
                    let id = StringId.CreateWithTemperature(this.PoolBaseId + edenSlot, nextTemp)
                    let newPacked = Packing.PackedOps.packStringIndexWithTemp sIdx nextTemp
                    let prev = Interlocked.CompareExchange(&this.EdenArray.PackedEntries.[edenSlot], newPacked, original)
                    if prev = original then
                        resultingId <- id
                        done <- true
                        if nextTemp >= threshold then
                            match this.PromoteFn with
                            | Some pf ->
                                let strVal = Volatile.Read(&this.EdenArray.StringsArray.[stringIdx])
                                if not (isNull strVal) then
                                    let promotedId = pf strVal
                                    // mark promoted (redirect)
                                    let mutable cur = Volatile.Read(&this.EdenArray.PackedEntries.[edenSlot])
                                    let promotedPacked = Packing.PackedOps.packPromotedId promotedId
                                    let mutable marked = false
                                    while not marked do
                                        if Packing.PackedOps.isPromoted cur then marked <- true
                                        else
                                            let prev2 = Interlocked.CompareExchange(&this.EdenArray.PackedEntries.[edenSlot], promotedPacked, cur)
                                            if prev2 = cur then marked <- true
                                            else cur <- prev2
                                    resultingId <- promotedId
                            | None -> ()
                    else
                        original <- prev
            resultingId

        member private this.TryGetEden(str:string) : StringId voption =
            match this.EdenIndex.TryGetLocation str with
            | ValueSome loc when loc.ArrayType = HashIndexing.EdenArrayType ->
                let slot = loc.EntryIndex
                if slot >= 0 && slot < this.EdenArray.PackedEntries.Length then
                    let packed = Volatile.Read(&this.EdenArray.PackedEntries.[slot])
                    if packed <> 0L then
                        if Packing.PackedOps.isPromoted packed then
                            let redir = Packing.PackedOps.unpackPromotedId packed
                            ValueSome redir
                        else
                            // increment temperature in place
                            let id =
                                this.IncrementEdenTempAndMaybePromote(slot, loc.StringIndex, packed)
                            ValueSome id
                    else ValueNone
                else ValueNone
            | _ -> ValueNone

        member private this.AllocateEden(str:string) : StringId =
            let edenIdx = Interlocked.Increment(&this.EdenOffset) - 1
            if edenIdx < this.EdenArray.PackedEntries.Length then
                let stringIdx = Interlocked.Increment(&this.EdenArray.NextStringIndex) - 1
                Volatile.Write(&this.EdenArray.StringsArray.[stringIdx], str)
                let packed = Packing.PackedOps.packStringIndexWithTemp stringIdx 0
                Volatile.Write(&this.EdenArray.PackedEntries.[edenIdx], packed)
                // index add
                this.EdenIndex.AddLocation(str, HashIndexing.EdenArrayType, stringIdx, edenIdx)
                StringId.Create(this.PoolBaseId + edenIdx)
            else
                // Fall back to segments (post-Eden)
                this.PostEden.AllocateOrReuseString(
                    str,
                    this.PoolBaseId + this.EdenArray.PackedEntries.Length,
                    this.PromoteFn
                )

        member this.InternString(str:string) : StringId =
            if String.IsNullOrEmpty str then invalidArg "str" "Cannot intern null/empty"
            // (Optional) process promotion queue in background minimal overhead
            // this.CheckPromotion() |> ignore   // Keep disabled or throttle
            Interlocked.Increment(&this.Stats.accessCount) |> ignore
            match this.TryGetEden str with
            | ValueSome id ->
                id
            | ValueNone ->
                Interlocked.Increment(&this.Stats.missCount) |> ignore
                this.AllocateEden str

        member private this.ResolveRedirect(id:StringId) : string option =
            // A redirect implies we must look in the higher tier; we don't have direct ref here
            // Higher tiers handled by PromoteFn injection logic – here just return None
            None

        member this.TryGetString(id:StringId) : string option =
            let v = id.Value
            if v < this.PoolBaseId then None
            else
                let relative = v - this.PoolBaseId
                if relative < this.EdenArray.PackedEntries.Length then
                    let packed = Volatile.Read(&this.EdenArray.PackedEntries.[relative])
                    if Packing.PackedOps.isPromoted packed then
                        // Follow redirect – we let external tier handle it
                        None
                    else
                        let sIdx,_ = Packing.PackedOps.unpackStringIndexAndTemp packed
                        let strVal = Volatile.Read(&this.EdenArray.StringsArray.[sIdx])
                        if isNull strVal then None else Some strVal
                else
                    // segments
                    this.PostEden.TryGetString(v, this.PoolBaseId + this.EdenArray.PackedEntries.Length)

        member this.GetStringWithTemperature(id:StringId) : (string option * StringId) =
            let v = id.Value
            if v < this.PoolBaseId then None, id
            else
                let relative = v - this.PoolBaseId
                if relative < this.EdenArray.PackedEntries.Length then
                    let packed = Volatile.Read(&this.EdenArray.PackedEntries.[relative])
                    if Packing.PackedOps.isPromoted packed then
                        None, id // Caller will attempt higher tier
                    else
                        let sIdx,temp = Packing.PackedOps.unpackStringIndexAndTemp packed
                        if sIdx < 0 || sIdx >= this.EdenArray.StringsArray.Length then None, id
                        else
                            let strVal = Volatile.Read(&this.EdenArray.StringsArray.[sIdx])
                            if isNull strVal then None, id
                            else
                                let newId = this.IncrementEdenTempAndMaybePromote(relative, sIdx, packed)
                                Some strVal, newId
                else
                    this.PostEden.GetStringWithTemperature(id, this.PoolBaseId + this.EdenArray.PackedEntries.Length)

        member this.CheckPromotion() : Promotions.PromotionCandidate[] =
            // Inline promotions already handled; keep old queue minimal
            [||]

        member this.GetStats() : PoolStats =
            let edenMem =
                this.EdenArray.StringsArray
                |> Array.take (min this.EdenOffset this.EdenArray.StringsArray.Length)
                |> Array.sumBy (fun s -> if isNull s then 0L else int64 (s.Length * 2))
            let segmentMem =
                this.PostEden.Chunks
                |> Seq.sumBy (fun c ->
                    c.StringsArray |> Array.sumBy (fun s -> if isNull s then 0L else int64 (s.Length * 2)))
            let access = Interlocked.Read(&this.Stats.accessCount)
            let misses = Interlocked.Read(&this.Stats.missCount)
            let hitRatio = if access > 0L then float (access - misses) / float access else 0.0
            {
                TotalStrings =
                    this.EdenOffset +
                    this.PostEden.CurrentChunkIndex * this.PostEden.ChunkSize +
                    this.PostEden.ChunkOffsets.[this.PostEden.CurrentChunkIndex]
                MemoryUsageBytes = edenMem + segmentMem
                HitRatio = hitRatio
                AccessCount = access
                MissCount = misses
            }

    // ========================================================================================
    // KEEP REST OF FILE (no removal). Ensure callers updated for new Pool.Create signature.
    // ========================================================================================