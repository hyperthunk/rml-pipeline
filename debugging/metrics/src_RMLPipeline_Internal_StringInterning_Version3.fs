namespace RMLPipeline.Internal

//  NOTE: Instrumentation Patch
//   This version extends the previous implementation with:
//    * Low-overhead atomic counters (lookups, hits, misses, collisions, promotions, redirects, canonicalisations, stash/ring hits).
//    * Observer hub (metrics events) exposed via MetricsHub module (register / unregister / publish).
//    * Snapshot APIs at pool / shard level.
//    * Hook points wired into Eden + Segment allocation, lookup, promotion, redirect & canonicalise.
//   All additions are self-contained, no external dependencies (System.Diagnostics etc).
//
//  Sections with instrumentation additions are marked with:  // INSTRUMENTATION

module StringInterning =

    open RMLPipeline
    open RMLPipeline.Core
    open RMLPipeline.FastMap.Types
    open System
    open System.Diagnostics
    open System.Runtime.CompilerServices
    open System.Threading

    // -----------------------------------------------------------------------------------------
    // Interfaces
    // -----------------------------------------------------------------------------------------

    type PromotionPolicy =
        abstract member shouldPromote : currentTemp:int -> adaptiveThreshold:int -> bool
        abstract member adaptiveThreshold : baseThreshold:int -> currentCount:int -> scalingParams:struct (int * float) -> int

    type DefaultPromotionPolicy() =
        interface PromotionPolicy with
            member _.shouldPromote(currentTemp, adaptiveThreshold) = currentTemp >= adaptiveThreshold
            member _.adaptiveThreshold(baseThreshold, currentCount, struct(interval, factor)) =
                let scale =
                    1.0 + (float currentCount / float interval) * (factor - 1.0)
                max 2 (int (float baseThreshold * scale))

    type StringPromoter =
        abstract member promoteLocalToGroup : string -> StringId
        abstract member promoteLocalDirectToGlobal : string -> StringId
        abstract member promoteGroupToGlobal : string -> StringId
        abstract member promoteNone : string -> StringId

    type NoopPromoter() =
        interface StringPromoter with
            member _.promoteLocalToGroup _ = StringId.Invalid
            member _.promoteLocalDirectToGlobal _ = StringId.Invalid
            member _.promoteGroupToGlobal _ = StringId.Invalid
            member _.promoteNone _ = StringId.Invalid

    // -----------------------------------------------------------------------------------------
    // Instrumentation: Events & Observer Hub
    // -----------------------------------------------------------------------------------------
    // INSTRUMENTATION

    type MetricEvent =
        | Promotion of sourceTier:string * value:string * newId:int
        | Redirect of oldId:int * newId:int
        | CollisionSpike of shard:int * collisions:int64
        | Canonicalised of originalId:int * finalId:int
        | AllocationDuplicate of value:string
        | StashOverflow of shard:int
        | RingOverflowInsert of shard:int
        | SnapshotTaken of timestamp:DateTime * lookups:int64 * hits:int64

    type MetricsObserver =
        abstract member onEvent : MetricEvent -> unit

    module internal MetricsHub =
        // lock-free immutable list swap
        let mutable private observers : MetricsObserver list = []
        let private syncRoot = obj()

        let register (o:MetricsObserver) =
            lock syncRoot (fun () ->
                observers <- o :: observers)

        let unregister (o:MetricsObserver) =
            lock syncRoot (fun () ->
                observers <- observers |> List.filter (fun x -> not (obj.ReferenceEquals(x,o))))

        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let publish (evt:MetricEvent) =
            // Fast path: snapshot reference
            let snapshot = observers
            if not snapshot.IsEmpty then
                for o in snapshot do
                    try o.onEvent evt with _ -> ()

    // -----------------------------------------------------------------------------------------
    // Config / stats
    // -----------------------------------------------------------------------------------------

    type PoolType =
        | Global
        | Group
        | Worker
        | Segment
        | Eden

    type PoolConfiguration = {
        GlobalEdenSize: int
        GroupEdenSize: int
        WorkerEdenSize: int
        InitialChunkSize: int
        SecondaryChunkSize: int
        MaxChunks: int option
        WorkerPromotionThreshold: int
        GroupPromotionThreshold: int
        ChunkGrowthFactor: float
        ResizeThreshold: float
        FieldPathTemperatureFactor: float
        URITemplateTemperatureFactor: float
        LiteralTemperatureFactor: float
        TemperatureDecayFactor: float
        DecayInterval: TimeSpan
        ThresholdScalingFactor: float
        ThresholdScalingInterval: int
        MinPromotionInterval: TimeSpan
        MaxPromotionBatchSize: int
        MaxStashSlotsPerShard: int
        OverflowRingSizePerShard: int
        MaxProbeDepth: int
        EnablePathCompression: bool
    } with
        static member Default =
            { GlobalEdenSize = 50000
              GroupEdenSize = 25000
              WorkerEdenSize = 10000
              InitialChunkSize = 1000
              SecondaryChunkSize = 5000
              MaxChunks = Some 100
              WorkerPromotionThreshold = 50
              GroupPromotionThreshold = 500
              ChunkGrowthFactor = 2.0
              ResizeThreshold = 0.8
              FieldPathTemperatureFactor = 1.5
              URITemplateTemperatureFactor = 2.0
              LiteralTemperatureFactor = 0.5
              TemperatureDecayFactor = 0.5
              DecayInterval = TimeSpan.FromHours 1.0
              ThresholdScalingFactor = 1.2
              ThresholdScalingInterval = 10000
              MinPromotionInterval = TimeSpan.FromMilliseconds 100.
              MaxPromotionBatchSize = 50
              MaxStashSlotsPerShard = 16
              OverflowRingSizePerShard = 64
              MaxProbeDepth = 8
              EnablePathCompression = true }

    [<Struct>]
    type Stats =
        { mutable accessCount: int64
          mutable missCount: int64 }

    [<Struct>]
    type PoolStats =
        { TotalStrings: int
          MemoryUsageBytes: int64
          HitRatio: float
          AccessCount: int64
          MissCount: int64 }

    // INSTRUMENTATION counters (per StringIndex shard aggregated)
    [<Struct>]
    type IndexCounterSnapshot =
        { Lookups: int64
          Hits: int64
          Misses: int64
          Collisions: int64
          StashHits: int64
          RingHits: int64 }

    [<Struct>]
    type PoolInstrumentationSnapshot =
        { Index: IndexCounterSnapshot
          Promotions: int64
          Redirects: int64
          Canonicalisations: int64 }

    [<Struct>]
    type WorkerId = WorkerId of Guid with
        member x.value = let (WorkerId v) = x in v
        static member create () = WorkerId(Guid.NewGuid())

    [<Struct>]
    type DependencyGroupId = DependencyGroupId of int32 with
        member x.value = let (DependencyGroupId v) = x in v

    module IdAllocation =
        let GlobalRuntimeBase = 50000
        let GroupPoolBase = 100000
        let LocalPoolBase = 500000
        let GroupPoolRangeSize = 10000
        let WorkerRangeSize = 1000
        let getGroupPoolBaseId (DependencyGroupId g) = GroupPoolBase + g * GroupPoolRangeSize
        let getLocalPoolBaseId (groupId: DependencyGroupId option) (WorkerId w) =
            match groupId with
            | Some gid ->
                let baseId = getGroupPoolBaseId gid
                let wh = abs (w.GetHashCode()) % 1000
                baseId + GroupPoolRangeSize + (wh * WorkerRangeSize)
            | None ->
                let wh = abs (w.GetHashCode()) % 100000
                LocalPoolBase + (wh * WorkerRangeSize)

    // -----------------------------------------------------------------------------------------
    // HashIndexing with metrics
    // -----------------------------------------------------------------------------------------
    module HashIndexing =

        [<Literal>]
        let private NumShards = 32
        [<Literal>]
        let private ShardMask = 0x1F
        [<Literal>]
        let private ShardShift = 5
        [<Literal>]
        let EdenArrayType = 0uy

        [<Struct>]
        type PackedLocation =
            { ArrayType: byte
              StringIndex: int32
              EntryIndex: int32 }

        let EmptyLocationValue = -1L
        let inline packLocation (arrayType:byte) (stringIndex:int32) (entryIndex:int32) =
            (int64 arrayType <<< 56)
            ||| (int64 (stringIndex &&& 0xFFFFFF) <<< 32)
            ||| (int64 entryIndex)
        let inline unpackLocation (v:int64) =
            { ArrayType = byte (v >>> 56)
              StringIndex = int32 ((v >>> 32) &&& 0xFFFFFFL)
              EntryIndex = int32 (v &&& 0xFFFFFFFFL) }

        let inline fnv1a (s:string) =
            let prime = 1099511628211UL
            let offset = 14695981039346656037UL
            let mutable h = offset
            for i=0 to s.Length-1 do
                h <- h ^^^ uint64 (int s[i])
                h <- h * prime
            h

        [<Struct>]
        type StashEntry =
            { mutable key: string
              mutable hashFragment: uint32
              mutable packed: int64
              mutable state: int32 }

        [<Struct>]
        type RingEntry =
            { mutable key: string
              mutable hashFragment: uint32
              mutable packed: int64 }

        [<Struct>]
        type Shard =
            { Bitmap: int64[]
              BitmapMask: uint64
              PackedLocations: int64[]
              HashFragments: uint32[]
              StringKeys: string[]
              ArrayMask: uint64
              Stash: StashEntry[]
              mutable Ring: RingEntry[]
              mutable RingHead: int32
              MaxProbeDepth: int
              mutable InsertCollisions: int64 }

        type StringIndex(config:PoolConfiguration, poolType:PoolType) =
            // counters
            let mutable lookups = 0L
            let mutable hits = 0L
            let mutable misses = 0L
            let mutable stashHits = 0L
            let mutable ringHits = 0L
            let mutable collisions = 0L

            let expected =
                match poolType with
                | PoolType.Eden
                | PoolType.Global -> config.GlobalEdenSize * 2
                | PoolType.Group -> config.GroupEdenSize * 2
                | PoolType.Worker -> config.WorkerEdenSize * 2
                | PoolType.Segment -> config.InitialChunkSize * 4
            let loadFactor = 0.15
            let perShard = int (float expected / float NumShards / loadFactor)
            let nextPow2 n =
                let mutable p = 1
                while p < n do p <- p <<< 1
                p
            let arrSize = nextPow2 (max 256 perShard)
            let createShard () =
                let bitmapSize = max 64 (arrSize / 64)
                { Bitmap = Array.zeroCreate bitmapSize
                  BitmapMask = uint64 (bitmapSize * 64 - 1)
                  PackedLocations = Array.create arrSize EmptyLocationValue
                  HashFragments = Array.zeroCreate arrSize
                  StringKeys = Array.create arrSize null
                  ArrayMask = uint64 (arrSize - 1)
                  Stash = Array.init config.MaxStashSlotsPerShard (fun _ ->
                        { key = null; hashFragment = 0u; packed = EmptyLocationValue; state = 0 })
                  Ring = Array.init config.OverflowRingSizePerShard (fun _ ->
                        { key = null; hashFragment = 0u; packed = EmptyLocationValue })
                  RingHead = 0
                  MaxProbeDepth = config.MaxProbeDepth
                  InsertCollisions = 0L }
            let shards = Array.init NumShards (fun _ -> createShard())
            member private _.shardIndex (hash:uint64) = int (hash &&& uint64 ShardMask)
            member private _.setBit (shard:Shard) (hash:uint64) =
                let bitIndex = int ((hash >>> ShardShift) &&& shard.BitmapMask)
                let w = bitIndex >>> 6
                let m = 1L <<< (bitIndex &&& 63)
                Interlocked.Or(&shard.Bitmap[w], m) |> ignore
            member private _.bitSet (shard:Shard) (hash:uint64) =
                let bitIndex = int ((hash >>> ShardShift) &&& shard.BitmapMask)
                let w = bitIndex >>> 6
                let m = 1L <<< (bitIndex &&& 63)
                (Volatile.Read(&shard.Bitmap[w]) &&& m) <> 0L

            member this.tryGet (str:string) : PackedLocation voption =
                if String.IsNullOrEmpty str then ValueNone else
                Interlocked.Increment(&lookups) |> ignore
                let h = fnv1a str
                let sIdx = this.shardIndex h
                let shard = shards[sIdx]
                if not (this.bitSet shard h) then
                    Interlocked.Increment(&misses) |> ignore
                    ValueNone
                else
                    let slot = int ((h >>> ShardShift) &&& shard.ArrayMask)
                    let frag = uint32 (h >>> 32)
                    let storedFrag = Volatile.Read(&shard.HashFragments.[slot])
                    let mutable result = ValueNone
                    if storedFrag = frag then
                        let k = Volatile.Read(&shard.StringKeys.[slot])
                        if not (isNull k) && k = str then
                            let packed = Volatile.Read(&shard.PackedLocations.[slot])
                            if packed <> EmptyLocationValue then
                                Interlocked.Increment(&hits) |> ignore
                                result <- ValueSome (unpackLocation packed)
                    if result.IsSome then result else
                    // probe
                    let mutable found = ValueNone
                    let mutable depth = 1
                    let mutable curSlot = slot
                    while found = ValueNone && depth <= shard.MaxProbeDepth do
                        curSlot <- int ((uint64 (curSlot + 1)) &&& shard.ArrayMask)
                        let f2 = Volatile.Read(&shard.HashFragments.[curSlot])
                        if f2 = frag then
                            let k2 = Volatile.Read(&shard.StringKeys.[curSlot])
                            if not (isNull k2) && k2 = str then
                                let packed = Volatile.Read(&shard.PackedLocations.[curSlot])
                                if packed <> EmptyLocationValue then
                                    found <- ValueSome (unpackLocation packed)
                        depth <- depth + 1
                    match found with
                    | ValueSome _ ->
                        Interlocked.Increment(&hits) |> ignore
                        found
                    | ValueNone ->
                        // stash
                        let st = shard.Stash
                        let mutable stashFound = ValueNone
                        let mutable i = 0
                        while stashFound = ValueNone && i < st.Length do
                            if Volatile.Read(&st[i].state) = 1 then
                                if st[i].hashFragment = frag && st[i].key = str then
                                    stashFound <- ValueSome (unpackLocation st[i].packed)
                            i <- i + 1
                        match stashFound with
                        | ValueSome loc ->
                            Interlocked.Increment(&stashHits) |> ignore
                            stashFound
                        | ValueNone ->
                            // ring
                            let ring = shard.Ring
                            let mutable j = 0
                            let mutable ringFound = ValueNone
                            while ringFound = ValueNone && j < ring.Length do
                                let rk = Volatile.Read(&ring[j].key)
                                if not (isNull rk) then
                                    if ring[j].hashFragment = frag && rk = str then
                                        ringFound <- ValueSome (unpackLocation ring[j].packed)
                                j <- j + 1
                            match ringFound with
                            | ValueSome loc ->
                                Interlocked.Increment(&ringHits) |> ignore
                                ringFound
                            | ValueNone ->
                                Interlocked.Increment(&misses) |> ignore
                                ValueNone

            member this.add (str:string) (arrayType:byte) (stringIndex:int32) (entryIndex:int32) =
                if String.IsNullOrEmpty str then () else
                let h = fnv1a str
                let sIdx = this.shardIndex h
                let shard = shards[sIdx]
                this.setBit shard h
                let slot = int ((h >>> ShardShift) &&& shard.ArrayMask)
                let frag = uint32 (h >>> 32)
                let tryClaimSlot (slot:int) =
                    let existingKey = Volatile.Read(&shard.StringKeys.[slot])
                    if isNull existingKey then
                        if Interlocked.CompareExchange(&shard.StringKeys.[slot], str, null) = null then
                            Volatile.Write(&shard.HashFragments.[slot], frag)
                            Thread.MemoryBarrier()
                            Volatile.Write(&shard.PackedLocations.[slot], packLocation arrayType stringIndex entryIndex)
                            true
                        else false
                    else
                        if existingKey = str then
                            Volatile.Write(&shard.PackedLocations.[slot], packLocation arrayType stringIndex entryIndex)
                            true
                        else false
                if not (tryClaimSlot slot) then
                    let mutable placed = false
                    let mutable curSlot = slot
                    let mutable depth = 0
                    while not placed && depth < shard.MaxProbeDepth do
                        curSlot <- int ((uint64 (curSlot + 1)) &&& shard.ArrayMask)
                        placed <- tryClaimSlot curSlot
                        depth <- depth + 1
                    if not placed then
                        let st = shard.Stash
                        let mutable done = false
                        let mutable i = 0
                        while not done && i < st.Length do
                            if Interlocked.CompareExchange(&st[i].state, 1, 0) = 0 then
                                st[i].key <- str
                                st[i].hashFragment <- frag
                                st[i].packed <- packLocation arrayType stringIndex entryIndex
                                done <- true
                            else i <- i + 1
                        if not done then
                            // ring fallback
                            let head = Interlocked.Increment(&shard.RingHead) - 1
                            let idx = head % shard.Ring.Length
                            let rref = &shard.Ring.[idx]
                            rref.key <- str
                            rref.hashFragment <- frag
                            rref.packed <- packLocation arrayType stringIndex entryIndex
                            Interlocked.Increment(&shard.InsertCollisions) |> ignore
                            Interlocked.Increment(&collisions) |> ignore
                            MetricsHub.publish (RingOverflowInsert sIdx)

            member this.tryGetLocation str = this.tryGet str

            member this.addLocation (s,a,b,c) = this.add s a b c

            member this.getCounters() =
                let lk = Volatile.Read &lookups
                let ht = Volatile.Read &hits
                let ms = Volatile.Read &misses
                let col = Volatile.Read &collisions
                let sh = Volatile.Read &stashHits
                let rh = Volatile.Read &ringHits
                { Lookups = lk; Hits = ht; Misses = ms; Collisions = col; StashHits = sh; RingHits = rh }

    // -----------------------------------------------------------------------------------------
    // Promotions (minimal placeholder)
    // -----------------------------------------------------------------------------------------
    module Promotions =
        [<Struct>]
        type PromotionSignal =
            { StringId: StringId
              Temperature: int32
              Timestamp: int64 }
        [<Struct>]
        type PromotionCandidate =
            { Value: string
              Temperature: int32
              OriginalId: StringId }
        type EpochPromotionTracker =
            { mutable dummy:int } with
            static member Create() = { dummy = 0 }
            member _.ProcessPromotions(_resolver) : PromotionCandidate[] = [||]
            member _.SignalPromotion(_id,_t) = true

    // -----------------------------------------------------------------------------------------
    // Packing
    // -----------------------------------------------------------------------------------------
    module Packing =
        [<Struct>]
        type PackedPoolArray =
            { PackedEntries: int64[]
              StringsArray: string[]
              mutable NextStringIndex: int32 }
        module PackedOps =
            let inline isPromoted (v:int64) = v < 0L
            let inline packStringIndexWithTemp (ix:int32) (temp:int32) =
                (int64 temp <<< 47) ||| int64 ix
            let inline unpackStringIndexAndTemp (v:int64) =
                let ix = int32 (v &&& 0x7FFFFFFFFFFFL)
                let t = int32 ((v >>> 47) &&& 0xFFFFL)
                ix, t
            let inline packPromoted (id:StringId) =
                0x8000000000000000L ||| int64 id.Value
            let inline unpackPromoted (v:int64) =
                StringId.Create(int32 (v &&& 0x7FFFFFFFFFFFFFFFL))

    // -----------------------------------------------------------------------------------------
    // StringId
    // -----------------------------------------------------------------------------------------
    [<Struct>]
    type StringId =
        val Value : int
        val Temperature : int
        new (value:int, temp:int) = { Value = value; Temperature = temp }
        static member Create(v:int) = StringId(v,0)
        static member CreateWithTemperature(v:int,t:int) = StringId(v,t)
        static member Invalid = StringId(-1,0)
        static member MaxTemperature = 0xFFFF
        member x.IncrementTemperature() =
            if x.Temperature >= StringId.MaxTemperature then x
            else StringId(x.Value, x.Temperature + 1)
        member x.IsValid = x.Value >= 0

    // -----------------------------------------------------------------------------------------
    // Segments
    // -----------------------------------------------------------------------------------------
    type Segments =
        { Chunks: ResizeArray<Packing.PackedPoolArray>
          ChunkSize: int
          mutable CurrentChunkIndex: int32
          mutable ChunkOffsets: int32[]
          ChunksLock: ReaderWriterLockSlim
          Configuration: PoolConfiguration
          mutable StringCount: int32
          Index: HashIndexing.StringIndex
          PromotionPolicy: PromotionPolicy
          Promoter: StringPromoter option
          BaseId: int
          mutable PromotionThreshold: int
          EnablePathCompression: bool
          mutable Promotions: int64
          mutable Redirects: int64 }

    module Segments =
        let create baseId cfg policy promoter =
            let first =
                { Packing.PackedPoolArray.PackedEntries = Array.zeroCreate cfg.InitialChunkSize
                  StringsArray = Array.zeroCreate cfg.InitialChunkSize
                  NextStringIndex = 0 }
            { Chunks = ResizeArray [first]
              ChunkSize = cfg.InitialChunkSize
              CurrentChunkIndex = 0
              ChunkOffsets = Array.zeroCreate 16
              ChunksLock = new ReaderWriterLockSlim()
              Configuration = cfg
              StringCount = 0
              Index = HashIndexing.StringIndex(cfg, PoolType.Segment)
              PromotionPolicy = policy
              Promoter = promoter
              BaseId = baseId
              PromotionThreshold = cfg.GroupPromotionThreshold
              EnablePathCompression = cfg.EnablePathCompression
              Promotions = 0L
              Redirects = 0L }

        let inline adaptiveThreshold (s:Segments) =
            s.PromotionPolicy.adaptiveThreshold(
                s.PromotionThreshold,
                Volatile.Read(&s.StringCount),
                struct(s.Configuration.ThresholdScalingInterval, s.Configuration.ThresholdScalingFactor))

        let private grow (s:Segments) =
            let idx = s.CurrentChunkIndex + 1
            if idx < s.Chunks.Count then () else
            let lastSize =
                if s.Chunks.Count = 1 then max s.Configuration.SecondaryChunkSize s.ChunkSize
                else
                    let prev = s.Chunks.[s.Chunks.Count-1].PackedEntries.Length
                    int (float prev * s.Configuration.ChunkGrowthFactor) |> min 50000
            let newC =
                { Packing.PackedPoolArray.PackedEntries = Array.zeroCreate lastSize
                  StringsArray = Array.zeroCreate lastSize
                  NextStringIndex = 0 }
            s.Chunks.Add newC
            if idx >= s.ChunkOffsets.Length then
                let bigger = Array.zeroCreate (s.ChunkOffsets.Length * 2)
                Array.Copy(s.ChunkOffsets, bigger, s.ChunkOffsets.Length)
                s.ChunkOffsets <- bigger

        let private allocInNewChunk (s:Segments) (str:string) =
            let idx = Interlocked.Increment(&s.CurrentChunkIndex)
            let offset = Interlocked.Increment(&s.ChunkOffsets.[idx]) - 1
            let chunk = s.Chunks.[idx]
            let si = Interlocked.Increment(&chunk.NextStringIndex) - 1
            Volatile.Write(&chunk.StringsArray.[si], str)
            let packed = Packing.PackedOps.packStringIndexWithTemp si 0
            Volatile.Write(&chunk.PackedEntries.[offset], packed)
            s.Index.addLocation(str, byte(idx+1), si, offset)
            let id = StringId.Create(s.BaseId + (idx * s.ChunkSize) + offset)
            Interlocked.Increment(&s.StringCount) |> ignore
            id

        let private allocate (s:Segments) (str:string) =
            let currentIdx = Volatile.Read(&s.CurrentChunkIndex)
            let offset = Interlocked.Increment(&s.ChunkOffsets.[currentIdx]) - 1
            if offset < s.ChunkSize then
                let chunk = s.Chunks.[currentIdx]
                let si = Interlocked.Increment(&chunk.NextStringIndex) - 1
                Volatile.Write(&chunk.StringsArray.[si], str)
                let packed = Packing.PackedOps.packStringIndexWithTemp si 0
                Volatile.Write(&chunk.PackedEntries.[offset], packed)
                s.Index.addLocation(str, byte(currentIdx+1), si, offset)
                Interlocked.Increment(&s.StringCount) |> ignore
                StringId.Create(s.BaseId + (currentIdx * s.ChunkSize) + offset)
            else
                let mutable readHeld = false
                let mutable writeHeld = false
                try
                    readHeld <- s.ChunksLock.TryEnterUpgradeableReadLock 25
                    if readHeld then
                        if offset < s.ChunkSize then
                            allocate s str
                        else
                            match s.Configuration.MaxChunks with
                            | Some max when s.Chunks.Count >= max ->
                                // duplicate fallback (emergency)
                                MetricsHub.publish (AllocationDuplicate str)
                                StringId.Create(-1)
                            | _ ->
                                writeHeld <- s.ChunksLock.TryEnterWriteLock 50
                                if writeHeld then
                                    grow s
                                    allocInNewChunk s str
                                else
                                    allocInNewChunk s str
                    else
                        allocInNewChunk s str
                finally
                    if writeHeld then s.ChunksLock.ExitWriteLock()
                    if readHeld then s.ChunksLock.ExitUpgradeableReadLock()

        let private incrementTempAndMaybePromote (s:Segments) (segmentIdx:int) (entry:int) (strIdx:int) (packed:int64) =
            let chunk = s.Chunks.[segmentIdx]
            let mutable original = packed
            let mutable done = false
            let mutable result = StringId.Invalid
            let threshold = adaptiveThreshold s
            while not done do
                if Packing.PackedOps.isPromoted original then
                    result <- Packing.PackedOps.unpackPromoted original
                    done <- true
                else
                    let si,temp = Packing.PackedOps.unpackStringIndexAndTemp original
                    let nextTemp = if temp < StringId.MaxTemperature then temp + 1 else temp
                    let newId = StringId.CreateWithTemperature(s.BaseId + (segmentIdx * s.ChunkSize) + entry, nextTemp)
                    let newPacked = Packing.PackedOps.packStringIndexWithTemp si nextTemp
                    let prev = Interlocked.CompareExchange(&chunk.PackedEntries.[entry], newPacked, original)
                    if prev = original then
                        result <- newId
                        done <- true
                        if s.PromotionPolicy.shouldPromote(nextTemp, threshold) then
                            match s.Promoter with
                            | Some promoter ->
                                let strVal = Volatile.Read(&chunk.StringsArray.[strIdx])
                                if not (isNull strVal) then
                                    let promotedId = promoter.promoteGroupToGlobal strVal
                                    if promotedId.IsValid then
                                        Interlocked.Increment(&s.Promotions) |> ignore
                                        let promotedPacked = Packing.PackedOps.packPromoted promotedId
                                        let mutable cur = Volatile.Read(&chunk.PackedEntries.[entry])
                                        let mutable sentinelSet = false
                                        while not sentinelSet do
                                            if Packing.PackedOps.isPromoted cur then sentinelSet <- true
                                            else
                                                let prev2 = Interlocked.CompareExchange(&chunk.PackedEntries.[entry], promotedPacked, cur)
                                                if prev2 = cur then sentinelSet <- true else cur <- prev2
                                        Interlocked.Increment(&s.Redirects) |> ignore
                                        MetricsHub.publish (Promotion("segment", strVal, promotedId.Value))
                                        MetricsHub.publish (Redirect(newId.Value, promotedId.Value))
                                        result <- promotedId
                            | None -> ()
                    else
                        original <- prev
            result

        let allocateOrReuse (s:Segments) (str:string) =
            match s.Index.tryGetLocation str with
            | ValueSome loc when loc.ArrayType > 0uy ->
                let segIdx = int loc.ArrayType - 1
                if segIdx >= s.Chunks.Count then allocate s str else
                let chunk = s.Chunks.[segIdx]
                if loc.EntryIndex < 0 || loc.EntryIndex >= chunk.PackedEntries.Length then allocate s str else
                let packed = Volatile.Read(&chunk.PackedEntries.[loc.EntryIndex])
                if Packing.PackedOps.isPromoted packed then
                    Packing.PackedOps.unpackPromoted packed
                else
                    let stored = Volatile.Read(&chunk.StringsArray.[loc.StringIndex])
                    if stored <> str then allocate s str else
                    incrementTempAndMaybePromote s segIdx loc.EntryIndex loc.StringIndex packed
            | _ -> allocate s str

        let tryGetString (s:Segments) (id:int) =
            let rel = id - s.BaseId
            if rel < 0 then None else
            let segIdx = rel / s.ChunkSize
            let offset = rel % s.ChunkSize
            if segIdx < 0 || segIdx >= s.Chunks.Count then None else
            let chunk = s.Chunks.[segIdx]
            if offset < 0 || offset >= chunk.PackedEntries.Length then None else
            let packed = Volatile.Read(&chunk.PackedEntries.[offset])
            if Packing.PackedOps.isPromoted packed then None
            else
                let si,_ = Packing.PackedOps.unpackStringIndexAndTemp packed
                if si < 0 || si >= chunk.StringsArray.Length then None
                else
                    let strv = Volatile.Read(&chunk.StringsArray.[si])
                    if isNull strv then None else Some strv

        let getStringWithTemp (s:Segments) (id:StringId) =
            let rel = id.Value - s.BaseId
            if rel < 0 then None, id else
            let segIdx = rel / s.ChunkSize
            let offset = rel % s.ChunkSize
            if segIdx < 0 || segIdx >= s.Chunks.Count then None, id else
            let chunk = s.Chunks.[segIdx]
            if offset < 0 || offset >= chunk.PackedEntries.Length then None, id else
            let packed = Volatile.Read(&chunk.PackedEntries.[offset])
            if Packing.PackedOps.isPromoted packed then None, id
            else
                let si,temp = Packing.PackedOps.unpackStringIndexAndTemp packed
                if si < 0 || si >= chunk.StringsArray.Length then None, id else
                let strv = Volatile.Read(&chunk.StringsArray.[si])
                if isNull strv then None, id
                else
                    let newId = incrementTempAndMaybePromote s segIdx offset si packed
                    Some strv, newId

        let getInstrumentation (s:Segments) =
            { Index = s.Index.getCounters()
              Promotions = Volatile.Read(&s.Promotions)
              Redirects = Volatile.Read(&s.Redirects)
              Canonicalisations = 0L }

    // -----------------------------------------------------------------------------------------
    // Pool (Eden + Segments)
    // -----------------------------------------------------------------------------------------

    type Pool =
        { EdenArray: Packing.PackedPoolArray
          mutable EdenOffset: int32
          EdenIndex: HashIndexing.StringIndex
          PostEden: Segments
          PoolBaseId: int
          Config: PoolConfiguration
          PromotionPolicy: PromotionPolicy
          Promoter: StringPromoter option
          mutable Stats: Stats
          mutable Promotions: int64
          mutable Redirects: int64
          mutable Canonicalisations: int64 }

    module Pool =
        let create baseId edenSize cfg policy promoter =
            { EdenArray =
                { Packing.PackedPoolArray.PackedEntries = Array.zeroCreate edenSize
                  StringsArray = Array.zeroCreate edenSize
                  NextStringIndex = 0 }
              EdenOffset = 0
              EdenIndex = HashIndexing.StringIndex(cfg, PoolType.Eden)
              PostEden = Segments.create (baseId + edenSize) cfg policy promoter
              PoolBaseId = baseId
              Config = cfg
              PromotionPolicy = policy
              Promoter = promoter
              Stats = { accessCount = 0L; missCount = 0L }
              Promotions = 0L
              Redirects = 0L
              Canonicalisations = 0L }

        let private adaptiveEdenThreshold (p:Pool) =
            p.PromotionPolicy.adaptiveThreshold(
                if p.PoolBaseId < IdAllocation.GroupPoolBase then p.Config.GroupPromotionThreshold
                elif p.PoolBaseId < IdAllocation.LocalPoolBase then p.Config.WorkerPromotionThreshold
                else p.Config.WorkerPromotionThreshold,
                Volatile.Read(&p.EdenOffset)
                + Volatile.Read(&p.PostEden.StringCount),
                struct(p.Config.ThresholdScalingInterval, p.Config.ThresholdScalingFactor)
            )

        let private incrementEden (p:Pool) (slot:int) (stringIndex:int) (packed:int64) =
            let mutable original = packed
            let mutable done = false
            let mutable result = StringId.Invalid
            let threshold = adaptiveEdenThreshold p
            while not done do
                if Packing.PackedOps.isPromoted original then
                    result <- Packing.PackedOps.unpackPromoted original
                    done <- true
                else
                    let si,temp = Packing.PackedOps.unpackStringIndexAndTemp original
                    let nextTemp = if temp < StringId.MaxTemperature then temp + 1 else temp
                    let newId = StringId.CreateWithTemperature(p.PoolBaseId + slot, nextTemp)
                    let newPacked = Packing.PackedOps.packStringIndexWithTemp si nextTemp
                    let prev = Interlocked.CompareExchange(&p.EdenArray.PackedEntries.[slot], newPacked, original)
                    if prev = original then
                        result <- newId
                        done <- true
                        if p.PromotionPolicy.shouldPromote(nextTemp, threshold) then
                            match p.Promoter with
                            | Some promoter ->
                                let strv = Volatile.Read(&p.EdenArray.StringsArray.[si])
                                if not (isNull strv) then
                                    let promotedId =
                                        if p.PoolBaseId < IdAllocation.GroupPoolBase then
                                            promoter.promoteLocalDirectToGlobal strv
                                        elif p.PoolBaseId < IdAllocation.LocalPoolBase then
                                            promoter.promoteGroupToGlobal strv
                                        else
                                            promoter.promoteNone strv
                                    if promotedId.IsValid then
                                        Interlocked.Increment(&p.Promotions) |> ignore
                                        let promotedPacked = Packing.PackedOps.packPromoted promotedId
                                        let mutable cur = Volatile.Read(&p.EdenArray.PackedEntries.[slot])
                                        let mutable comp = false
                                        while not comp do
                                            if Packing.PackedOps.isPromoted cur then comp <- true
                                            else
                                                let prev2 = Interlocked.CompareExchange(&p.EdenArray.PackedEntries.[slot], promotedPacked, cur)
                                                if prev2 = cur then comp <- true else cur <- prev2
                                        Interlocked.Increment(&p.Redirects) |> ignore
                                        MetricsHub.publish (Promotion("eden", strv, promotedId.Value))
                                        MetricsHub.publish (Redirect(newId.Value, promotedId.Value))
                                        result <- promotedId
                            | None -> ()
                    else
                        original <- prev
            result

        let private edenLookup (p:Pool) (str:string) =
            match p.EdenIndex.tryGetLocation str with
            | ValueSome loc when loc.ArrayType = HashIndexing.EdenArrayType ->
                let slot = loc.EntryIndex
                if slot >= 0 && slot < p.EdenArray.PackedEntries.Length then
                    let packed = Volatile.Read(&p.EdenArray.PackedEntries.[slot])
                    if Packing.PackedOps.isPromoted packed then
                        Packing.PackedOps.unpackPromoted packed
                    else
                        incrementEden p slot loc.StringIndex packed
                else StringId.Invalid
            | _ -> StringId.Invalid

        let private edenAllocate (p:Pool) (str:string) =
            let slot = Interlocked.Increment(&p.EdenOffset) - 1
            if slot < p.EdenArray.PackedEntries.Length then
                let si = Interlocked.Increment(&p.EdenArray.NextStringIndex) - 1
                Volatile.Write(&p.EdenArray.StringsArray.[si], str)
                let packed = Packing.PackedOps.packStringIndexWithTemp si 0
                Volatile.Write(&p.EdenArray.PackedEntries.[slot], packed)
                p.EdenIndex.addLocation(str, HashIndexing.EdenArrayType, si, slot)
                StringId.Create(p.PoolBaseId + slot)
            else
                Segments.allocateOrReuse p.PostEden str

        let intern (p:Pool) (str:string) =
            Interlocked.Increment(&p.Stats.accessCount) |> ignore
            let existing = edenLookup p str
            if existing.IsValid then existing
            else
                Interlocked.Increment(&p.Stats.missCount) |> ignore
                edenAllocate p str

        let canonicalise (p:Pool) (id:StringId) =
            if id.Value < p.PoolBaseId then id
            else
                let rel = id.Value - p.PoolBaseId
                if rel < p.EdenArray.PackedEntries.Length then
                    let packed = Volatile.Read(&p.EdenArray.PackedEntries.[rel])
                    if Packing.PackedOps.isPromoted packed then
                        let finalId = Packing.PackedOps.unpackPromoted packed
                        Interlocked.Increment(&p.Canonicalisations) |> ignore
                        MetricsHub.publish (Canonicalised(id.Value, finalId.Value))
                        StringId.Create(finalId.Value)
                    else id
                else id // segments: (one-hop redirect only for now)

        let tryGet (p:Pool) (id:StringId) =
            if id.Value < p.PoolBaseId then None
            else
                let rel = id.Value - p.PoolBaseId
                if rel < p.EdenArray.PackedEntries.Length then
                    let packed = Volatile.Read(&p.EdenArray.PackedEntries.[rel])
                    if Packing.PackedOps.isPromoted packed then None
                    else
                        let si,_ = Packing.PackedOps.unpackStringIndexAndTemp packed
                        if si < 0 || si >= p.EdenArray.StringsArray.Length then None
                        else
                            let sVal = Volatile.Read(&p.EdenArray.StringsArray.[si])
                            if isNull sVal then None else Some sVal
                else
                    Segments.tryGetString p.PostEden id.Value

        let getWithTemp (p:Pool) (id:StringId) =
            if id.Value < p.PoolBaseId then None, id
            else
                let rel = id.Value - p.PoolBaseId
                if rel < p.EdenArray.PackedEntries.Length then
                    let packed = Volatile.Read(&p.EdenArray.PackedEntries.[rel])
                    if Packing.PackedOps.isPromoted packed then None, id
                    else
                        let si,_ = Packing.PackedOps.unpackStringIndexAndTemp packed
                        if si < 0 || si >= p.EdenArray.StringsArray.Length then None,id
                        else
                            let sVal = Volatile.Read(&p.EdenArray.StringsArray.[si])
                            if isNull sVal then None,id
                            else
                                let newId = incrementEden p rel si packed
                                Some sVal,newId
                else
                    Segments.getStringWithTemp p.PostEden id

        let stats (p:Pool) =
            let edenMem =
                p.EdenArray.StringsArray
                |> Array.take (min p.EdenOffset p.EdenArray.StringsArray.Length)
                |> Array.sumBy (fun s -> if isNull s then 0L else int64 (s.Length * 2))
            let segMem =
                p.PostEden.Chunks
                |> Seq.sumBy (fun c -> c.StringsArray |> Array.sumBy (fun s -> if isNull s then 0L else int64 (s.Length * 2)))
            let access = Volatile.Read(&p.Stats.accessCount)
            let miss = Volatile.Read(&p.Stats.missCount)
            let hr = if access > 0L then float (access - miss)/float access else 0.0
            { TotalStrings =
                p.EdenOffset
                + p.PostEden.CurrentChunkIndex * p.PostEden.ChunkSize
                + p.PostEden.ChunkOffsets.[p.PostEden.CurrentChunkIndex]
              MemoryUsageBytes = edenMem + segMem
              HitRatio = hr
              AccessCount = access
              MissCount = miss }

        let instrumentation (p:Pool) =
            let idx = p.EdenIndex.getCounters()
            let seg = Segments.getInstrumentation p.PostEden
            { Index =
                { Lookups = idx.Lookups + seg.Index.Lookups
                  Hits = idx.Hits + seg.Index.Hits
                  Misses = idx.Misses + seg.Index.Misses
                  Collisions = idx.Collisions + seg.Index.Collisions
                  StashHits = idx.StashHits + seg.Index.StashHits
                  RingHits = idx.RingHits + seg.Index.RingHits }
              Promotions = Volatile.Read(&p.Promotions) + seg.Promotions
              Redirects = Volatile.Read(&p.Redirects) + seg.Redirects
              Canonicalisations = Volatile.Read(&p.Canonicalisations) + seg.Canonicalisations }

    // -----------------------------------------------------------------------------------------
    // Public façade
    // -----------------------------------------------------------------------------------------
    type InternalPoolFacade =
        { Pool: Pool }
        member x.intern s = Pool.intern x.Pool s
        member x.tryGet id = Pool.tryGet x.Pool id
        member x.getWithTemp id = Pool.getWithTemp x.Pool id
        member x.canonicalise id = Pool.canonicalise x.Pool id
        member x.getStats() = Pool.stats x.Pool
        member x.getInstrumentation() = Pool.instrumentation x.Pool