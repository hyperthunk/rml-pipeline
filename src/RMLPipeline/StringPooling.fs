namespace RMLPipeline.Internal

module StringPooling =
    
    open FSharp.HashCollections
    open RMLPipeline
    open RMLPipeline.Core
    open RMLPipeline.FastMap.Types
    open System
    open System.Collections.Concurrent
    open System.Collections.Generic
    open System.Diagnostics
    open System.Threading

    // Configuration for adaptive sizing and thresholds
    type PoolConfiguration = {
        // Eden space sizing
        GlobalEdenSize: int
        GroupEdenSize: int
        WorkerEdenSize: int
        
        // Chunk sizing
        InitialChunkSize: int
        SecondaryChunkSize: int
        MaxChunks: int option
        
        // Promotion thresholds
        WorkerPromotionThreshold: int
        GroupPromotionThreshold: int
        
        // Growth factors
        ChunkGrowthFactor: float
        ResizeThreshold: float  // When to resize (e.g., 0.8 = 80% full)
        
        // Temperature adjustment factors
        FieldPathTemperatureFactor: float
        URITemplateTemperatureFactor: float
        LiteralTemperatureFactor: float
        
        // Temperature decay and threshold scaling
        TemperatureDecayFactor: float  // How much to decay temperatures (e.g., 0.5 = half)
        DecayInterval: TimeSpan        // How often to decay temperatures
        ThresholdScalingFactor: float  // How much to increase thresholds as pools grow
        ThresholdScalingInterval: int  // Increase threshold after every N strings
        
        // Promotion throttling
        MinPromotionInterval: TimeSpan // Minimum time between promotion attempts
        MaxPromotionBatchSize: int     // Maximum strings to promote in one batch
    } with
        static member Default = {
            GlobalEdenSize = 50000
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
            DecayInterval = TimeSpan.FromHours(1.0)
            ThresholdScalingFactor = 1.2
            ThresholdScalingInterval = 10000
            MinPromotionInterval = TimeSpan.FromMilliseconds(100.0)
            MaxPromotionBatchSize = 50
        }
        
        static member LowMemory = {
            GlobalEdenSize = 10000
            GroupEdenSize = 5000
            WorkerEdenSize = 1000
            InitialChunkSize = 500
            SecondaryChunkSize = 1000
            MaxChunks = Some 20
            WorkerPromotionThreshold = 100
            GroupPromotionThreshold = 1000
            ChunkGrowthFactor = 1.5
            ResizeThreshold = 0.9
            FieldPathTemperatureFactor = 1.5
            URITemplateTemperatureFactor = 2.0
            LiteralTemperatureFactor = 0.5
            TemperatureDecayFactor = 0.7  // Less aggressive decay
            DecayInterval = TimeSpan.FromHours(2.0)
            ThresholdScalingFactor = 1.5
            ThresholdScalingInterval = 5000
            MinPromotionInterval = TimeSpan.FromMilliseconds(250.0)
            MaxPromotionBatchSize = 20
        }
        
        static member HighPerformance = {
            GlobalEdenSize = 100000
            GroupEdenSize = 50000
            WorkerEdenSize = 20000
            InitialChunkSize = 5000
            SecondaryChunkSize = 10000
            MaxChunks = None
            WorkerPromotionThreshold = 20
            GroupPromotionThreshold = 200
            ChunkGrowthFactor = 2.5
            ResizeThreshold = 0.75
            FieldPathTemperatureFactor = 1.5
            URITemplateTemperatureFactor = 2.0
            LiteralTemperatureFactor = 0.5
            TemperatureDecayFactor = 0.3  // More aggressive decay
            DecayInterval = TimeSpan.FromMinutes(30.0)
            ThresholdScalingFactor = 1.1
            ThresholdScalingInterval = 20000
            MinPromotionInterval = TimeSpan.FromMilliseconds(50.0)
            MaxPromotionBatchSize = 100
        }

    [<Struct>]
    type Stats = {
        mutable accessCount: int64
        mutable missCount: int64
    }
        
    [<Struct>]
    type PromotionSignal = {
        StringId: StringId
        Temperature: int32
        Timestamp: int64
    }

    [<Struct>]
    type PromotionCandidate = {
        Value: string
        Temperature: int32
    }

    (*
        Circular Buffer for queuing promotion signals.
        Lock-free and degrades fairly gracefully to single-threaded
        processing, with only Interlocked ops as cost.
    *)
    type PromotionQueue = {
        Buffer: PromotionSignal[]
        BufferMask: int32  // For fast modulo
        MinPromotionInterval: TimeSpan
        mutable Head: int64
        mutable Tail: int64
        mutable LastProcessedTime: int64
    } with
        static member Create(sizeBits: int) =
            let size = 1 <<< sizeBits  // Power of 2
            {
                Buffer = Array.zeroCreate size
                BufferMask = size - 1
                MinPromotionInterval = TimeSpan.FromSeconds 0.2
                Head = 0L
                Tail = 0L
                LastProcessedTime = 0L
            }
        
        member this.TryEnqueue(signal: PromotionSignal) : bool =
            let mutable tail = Volatile.Read &this.Tail
            let mutable head = Volatile.Read &this.Head
            
            // Check if queue is full (with one slot buffer)
            if tail - head >= int64 this.Buffer.Length - 1L then
                false  // Queue full, promotion signal dropped (acceptable)
            else
                let index = int (tail &&& int64 this.BufferMask)
                this.Buffer.[index] <- signal
                
                // Memory barrier to ensure write completes before advancing tail
                Thread.MemoryBarrier()
                Interlocked.Increment &this.Tail |> ignore
                true
        
        member this.TryDequeueBatch(maxCount: int) : PromotionSignal[] =
            let now = Stopwatch.GetTimestamp()
            
            // Check minimum interval using atomic swap
            let lastTime = Volatile.Read &this.LastProcessedTime
            let elapsed = now - lastTime
            let minTicks = int64 (this.MinPromotionInterval.TotalMilliseconds * 
                                float Stopwatch.Frequency / 1000.0)
            
            if elapsed < minTicks then
                [||]  // Too soon
            else
                // Try to claim the promotion window
                if Interlocked.CompareExchange(&this.LastProcessedTime, now, lastTime) <> lastTime then
                    [||]  // Another thread is processing
                else
                    // We have exclusive access to dequeue
                    let mutable head = Volatile.Read(&this.Head)
                    let tail = Volatile.Read(&this.Tail)
                    let available = int (tail - head)
                    let toDequeue = min available maxCount
                    
                    if toDequeue > 0 then
                        let results = Array.zeroCreate toDequeue
                        for i = 0 to toDequeue - 1 do
                            let index = int ((head + int64 i) &&& int64 this.BufferMask)
                            results.[i] <- this.Buffer.[index]
                        
                        // Advance head
                        Interlocked.Add(&this.Head, int64 toDequeue) |> ignore
                        results
                    else
                        [||]

    type EpochPromotionTracker = {
        (* There are 3 epochs: 
            1. being written
            2. cooling off
            3. being processed *)
        EpochQueues: PromotionQueue[]
        mutable CurrentWriteEpoch: int32
        mutable LastRotationTime: int64
        RotationInterval: TimeSpan
        ProcessingLock: obj  // Only for the processing thread
    } with
        static member Create(queueSizeBits: int, rotationInterval: TimeSpan) =
            {
                EpochQueues = Array.init 3 (fun _ -> PromotionQueue.Create queueSizeBits)
                CurrentWriteEpoch = 0
                LastRotationTime = Stopwatch.GetTimestamp()
                RotationInterval = rotationInterval
                ProcessingLock = obj()
            }
        
        // Called by any thread to signal a promotion
        member this.SignalPromotion(id: StringId, temp: int32) : bool =
            // Fast path - just read the current epoch and enqueue
            let epoch = Volatile.Read &this.CurrentWriteEpoch
            let queue = this.EpochQueues.[epoch % 3]
            
            let signal = {
                StringId = id
                Temperature = temp
                Timestamp = Stopwatch.GetTimestamp()
            }
            
            queue.TryEnqueue signal
        
        // Check if it's time to rotate epochs
        member private this.CheckRotation() : bool =
            let now = Stopwatch.GetTimestamp()
            let lastRotation = Volatile.Read(&this.LastRotationTime)
            let elapsed = now - lastRotation
            let intervalTicks = int64 (this.RotationInterval.TotalMilliseconds * 
                                    float Stopwatch.Frequency / 1000.0)
            
            if elapsed >= intervalTicks then
                // Try to claim the rotation
                Interlocked.CompareExchange(&this.LastRotationTime, now, lastRotation) = lastRotation
            else
                false
        
        // Process promotions - called periodically by promotion processing thread
        member this.ProcessPromotions(resolver: StringId -> string option) : PromotionCandidate[] =
            // First check if we should rotate
            if this.CheckRotation() then
                Interlocked.Increment(&this.CurrentWriteEpoch) |> ignore
            
            // Only one thread should process at a time
            if Monitor.TryEnter(this.ProcessingLock) then
                try
                    // Process the oldest epoch (2 rotations ago)
                    let currentEpoch = Volatile.Read(&this.CurrentWriteEpoch)
                    let processEpoch = (currentEpoch + 2) % 3 // +2 because we want 2 epochs ago
                    let queue = this.EpochQueues.[processEpoch]
                    
                    // Drain the entire queue
                    let mutable candidates = ResizeArray<PromotionCandidate>()
                    let mutable batch = queue.TryDequeueBatch(1000)
                    
                    while batch.Length > 0 do
                        for signal in batch do
                            match resolver signal.StringId with
                            | Some str ->
                                candidates.Add({ Value = str; Temperature = signal.Temperature })
                            | None -> ()
                        
                        batch <- queue.TryDequeueBatch(1000)
                    
                    // Deduplicate candidates
                    candidates
                    |> Seq.distinctBy (fun c -> c.Value)
                    |> Seq.toArray
                finally
                    Monitor.Exit(this.ProcessingLock)
            else
                [||]  // Another thread is processing

    (* For use in single-threaded execution *)
    type RoundRobinEpochTracker = {
        EpochQueues: PromotionQueue[]
        mutable CurrentEpoch: int32
        mutable RotationCounter: int32
        RotationThreshold: int32  // Rotate after N operations
    } with
        member this.SignalPromotion(id: StringId, str: string, temp: int32) =
            let queue = this.EpochQueues.[this.CurrentEpoch]
            let signal = {
                StringId = id
                Temperature = temp
                Timestamp = Stopwatch.GetTimestamp()
            }
            queue.TryEnqueue signal |> ignore
            
            // Check if we should rotate
            this.RotationCounter <- this.RotationCounter + 1
            if this.RotationCounter >= this.RotationThreshold then
                this.CurrentEpoch <- (this.CurrentEpoch + 1) % 3
                this.RotationCounter <- 0
        
        member this.ProcessPromotions(resolver) =
            // Process the oldest epoch
            let processEpoch = (this.CurrentEpoch + 1) % 3
            let queue = this.EpochQueues.[processEpoch]
            
            // Drain and process
            queue.TryDequeueBatch(Int32.MaxValue)
            |> Array.choose (fun signal -> 
                resolver signal.StringId 
                |> Option.map (fun str -> { Value = str; Temperature = signal.Temperature }))

    [<Flags>]
    type StringAccessPattern =
        | Unknown = 0
        | HighFrequency = 1
        | MediumFrequency = 2
        | LowFrequency = 4
        | Planning = 8
        | Runtime = 16

    [<Struct>]
    type PoolStats = {
        TotalStrings: int
        MemoryUsageBytes: int64
        HitRatio: float
        AccessCount: int64
        MissCount: int64
        AverageTemperature: float  // Average temperature across all strings
        MaxTemperature: int32      // Highest temperature in any chunk
        HotStringsCount: int       // Number of strings above promotion threshold
        PendingPromotions: int64   // Signals for strings awaiting promotion
    } with 
        static member Empty = {
            TotalStrings      = 0
            MemoryUsageBytes  = 0L
            HitRatio          = 0.0
            AccessCount       = 0L
            MissCount         = 0L
            AverageTemperature = 0.0
            MaxTemperature    = 0
            HotStringsCount    = 0
            PendingPromotions  = 0L
        }

    [<Struct>]
    type WorkerId = WorkerId of Guid
        with
        member inline this.Value = let (WorkerId id) = this in id
        static member Create() = WorkerId(Guid.NewGuid())


    [<Struct>]
    type DependencyGroupId = DependencyGroupId of int32
        with
        member inline this.Value = let (DependencyGroupId id) = this in id

    // TODO: make IdAllocation limits available as runtime configuration
    module IdAllocation =
        // Fixed ranges that prevent ID collisions between pool tiers
        let GlobalRuntimeBase = 50000      // Reserve 50K IDs for global runtime strings
        let GroupPoolBase = 100000         // Start group pools at 100K
        let LocalPoolBase = 500000         // Start local pools at 500K
        
        // Range sizes per pool type
        let GroupPoolRangeSize = 10000     // Each group gets 10K ID range
        let WorkerRangeSize = 1000         // Each worker gets 1K ID range
        
        let getGroupPoolBaseId (groupId: DependencyGroupId) : int =
            GroupPoolBase + (groupId.Value * GroupPoolRangeSize)
        
        let getLocalPoolBaseId (groupId: DependencyGroupId option) (workerId: WorkerId) : int =
            match groupId with
            | Some gid ->
                // Deterministic local range within group: GroupBase + LocalOffset + WorkerRange
                let groupBase = getGroupPoolBaseId gid
                let workerHash = abs (workerId.Value.GetHashCode()) % 1000 // Max 1000 workers per group
                groupBase + GroupPoolRangeSize + (workerHash * WorkerRangeSize)
            | None ->
                // Global workers get ranges starting from LocalPoolBase
                let workerHash = abs (workerId.Value.GetHashCode()) % 100000 // Max 100K global workers
                LocalPoolBase + (workerHash * WorkerRangeSize)
    
    (* 
        Thread-safe chunked Array
    *)
    type Segments = {
        Chunks: ResizeArray<string[]>
        ChunkSize: int
        mutable CurrentChunkIndex: int32
        mutable ChunkOffsets: int32[]  // One counter per chunk
        ChunksLock: ReaderWriterLockSlim
        
        // Emergency fallback for high contention scenarios
        EmergencyCache: ConcurrentDictionary<int, string>  // Fallback for timeout cases
        mutable EmergencyCounter: int32
        EmergencyBaseId: int
        
        Configuration: PoolConfiguration
        
        // Temperature tracking with targeted approach
        mutable LastPromotionTime: int64
        mutable PromotionSignalCount: int64

        PromotionTracker: EpochPromotionTracker
        mutable PromotionThreshold: int32
        mutable StringCount: int32  // Track for adaptive threshold
    } with
        static member Create(baseId: int, config: PoolConfiguration) =
            let rotationInterval = 
                // Rotate faster than promotion interval to ensure smooth processing
                TimeSpan.FromMilliseconds(config.MinPromotionInterval.TotalMilliseconds / 3.0)
            {
                Chunks = ResizeArray<string[]>([| Array.zeroCreate config.InitialChunkSize |])
                ChunkSize = config.InitialChunkSize
                CurrentChunkIndex = 0
                ChunkOffsets = Array.zeroCreate 10 // Initial size, will grow as needed
                ChunksLock = new ReaderWriterLockSlim()
                EmergencyCache = ConcurrentDictionary<int, string>()
                EmergencyCounter = 0
                EmergencyBaseId = baseId + 900000  // Reserve high range for emergency
                Configuration = config
                LastPromotionTime = 0L
                PromotionSignalCount = 0L
                PromotionTracker = EpochPromotionTracker.Create(
                    queueSizeBits = 16,  // 64K promotion signals per epoch
                    rotationInterval = rotationInterval
                )
                PromotionThreshold = 
                    if baseId < IdAllocation.GroupPoolBase then 
                        config.GroupPromotionThreshold 
                    else 
                        config.WorkerPromotionThreshold
                StringCount = 0
            }
        
        member this.AllocateString(str: string, baseId: int) : StringId =
            // Try to use current chunk first - completely lock-free
            let currentChunkIdx = Volatile.Read(&this.CurrentChunkIndex)
            let offset = Interlocked.Increment(&this.ChunkOffsets.[currentChunkIdx]) - 1
            
            let newId = 
                if offset < this.ChunkSize then
                    let id = StringId.Create(baseId + (currentChunkIdx * this.ChunkSize) + offset)
                    Volatile.Write(&this.Chunks.[currentChunkIdx].[offset], str)
                    id
                else
                    this.AllocateInNewChunk(str, baseId)
            
            // We leverage an adaptive threshold whilst tracking string allocations 
            Interlocked.Increment(&this.StringCount) |> ignore
            newId

        member this.ProcessPromotions(baseId: int) : PromotionCandidate[] =
            // Create resolver function that looks up strings by ID
            let resolver (id: StringId) =
                this.TryGetString(id.Value, baseId)

            this.PromotionTracker.ProcessPromotions(resolver)

        member inline private this.tryAcquireReadLock() =
                this.ChunksLock.TryEnterUpgradeableReadLock(50)

        member inline private this.tryAcquireWriteLock() =
            this.ChunksLock.TryEnterWriteLock(50)

        member inline private this.releaseReadLock() =
            this.ChunksLock.ExitUpgradeableReadLock()

        member inline private this.releaseWriteLock() =
            this.ChunksLock.ExitWriteLock()

        member inline private this.growChunksArray nextIdx =
            // Calculate next chunk size with growth factor
            let nextChunkSize = 
                if this.Chunks.Count = 1 then
                    this.Configuration.SecondaryChunkSize
                else
                    int (float this.ChunkSize * this.Configuration.ChunkGrowthFactor)
                    |> min 50000  // Cap chunk size at 50K
            
            // Grow chunks array
            this.Chunks.Add(Array.zeroCreate nextChunkSize)
            
            // Ensure offset array has space
            if nextIdx >= this.ChunkOffsets.Length then
                let newSize = this.ChunkOffsets.Length * 2
                let newOffsets = Array.zeroCreate newSize
                Array.Copy(this.ChunkOffsets, newOffsets, this.ChunkOffsets.Length)
                this.ChunkOffsets <- newOffsets

        member inline private this.allocateInNextChunk baseId str =
            // Move to next chunk
            let newChunkIdx = Interlocked.Increment(&this.CurrentChunkIndex)
            let newOffset = Interlocked.Increment(&this.ChunkOffsets.[newChunkIdx]) - 1
            
            // Write string to new location
            let id = StringId.Create(baseId + (newChunkIdx * this.ChunkSize) + newOffset)
            Volatile.Write(&this.Chunks.[newChunkIdx].[newOffset], str)
            
            // Increment string count
            Interlocked.Increment(&this.StringCount) |> ignore
            id
    
        member private this.AllocateInNewChunk(str: string, baseId: int) : StringId =
            // the mutables here are fugly but safe(er)
            let mutable readLockHeld = false
            let mutable writeLockHeld = false
            try
                readLockHeld <- this.ChunksLock.TryEnterUpgradeableReadLock 50

                if readLockHeld then
                    let currentIdx = Volatile.Read(&this.CurrentChunkIndex)
                    let nextIdx = currentIdx + 1
                    
                    // Check if we need a new chunk
                    if nextIdx < this.Chunks.Count then
                        // Another thread already added the chunk
                        this.allocateInNextChunk baseId str
                    else
                        // Need to add a new chunk
                        match this.Configuration.MaxChunks with
                        | Some max when this.Chunks.Count >= max ->
                            // Hit max chunks limit - use emergency allocation
                            this.EmergencyAllocate str
                        | _ ->
                            // Try to acquire write lock
                            writeLockHeld <- this.ChunksLock.TryEnterWriteLock 50
                            if writeLockHeld then
                                try
                                    // Ensure we have enough space in the chunks array
                                    if nextIdx >= this.Chunks.Count then
                                        this.growChunksArray nextIdx
                                    
                                    // Allocate in the new chunk
                                    this.allocateInNextChunk baseId str
                                finally
                                    this.ChunksLock.ExitWriteLock()
                            else
                                // Write lock acquisition failed - emergency allocation
                                this.EmergencyAllocate str
                else 
                    this.EmergencyAllocate str
            finally
                // Always release locks in correct order
                if readLockHeld then this.ChunksLock.ExitUpgradeableReadLock()

        member this.GetAdaptiveThreshold() : int32 =
            let count = Volatile.Read(&this.StringCount)  // Uses the count we're tracking
            let scaleFactor = 
                1.0 + 
                (float count / float this.Configuration.ThresholdScalingInterval) * 
                (this.Configuration.ThresholdScalingFactor - 1.0)
            int32 (float this.PromotionThreshold * scaleFactor)

        member private this.EmergencyAllocate(str: string) : StringId =
            // Guaranteed to succeed even under extreme contention
            let emergencyId = Interlocked.Increment(&this.EmergencyCounter) - 1
            this.EmergencyCache.TryAdd(emergencyId, str) |> ignore
            
            Interlocked.Increment(&this.StringCount) |> ignore
            StringId.Create(this.EmergencyBaseId + emergencyId)
        
        member this.TryGetString(id: int, baseId: int) : string option =
            // Check if it's an emergency ID
            if id >= this.EmergencyBaseId then
                let emergencyIdx = id - this.EmergencyBaseId
                match this.EmergencyCache.TryGetValue(emergencyIdx) with
                | true, str -> Some str
                | false, _ -> None
            else
                // Regular chunk lookup
                let relativeId = id - baseId
                let chunkIdx = relativeId / this.ChunkSize
                let offset = relativeId % this.ChunkSize
                
                if chunkIdx >= 0 && chunkIdx < this.Chunks.Count then
                    let chunk = this.Chunks.[chunkIdx]
                    if offset >= 0 && offset < chunk.Length then
                        let str = Volatile.Read(&chunk.[offset])
                        if not (isNull str) then Some str else None
                    else None
                else None
        
        // Get batch of promotion candidates
        member this.GetPromotionCandidates(maxBatchSize: int) : PromotionCandidate[] =
            // Threshold with scaling applied
            let threshold = this.GetAdjustedPromotionThreshold()
            
            // Find candidates that exceed threshold
            this.StringTemperatures
            |> Seq.filter (fun kvp -> kvp.Value >= threshold)
            |> Seq.truncate maxBatchSize
            |> Seq.map (fun kvp -> { Value = kvp.Key; Temperature = kvp.Value })
            |> Seq.toArray
        
        // Check for strings to promote and return candidates if appropriate
        member this.CheckPromotion() : PromotionCandidate[] =
            // Fast check for any promotion signals (completely lock-free)
            if Interlocked.Read(&this.PromotionSignalCount) > 0L then
                // Check if enough time has passed since last promotion
                let now = Stopwatch.GetTimestamp()
                let elapsed = now - Volatile.Read(&this.LastPromotionTime)
                let minInterval = 
                    this.Configuration.MinPromotionInterval.TotalMilliseconds * 
                    float Stopwatch.Frequency / 1000.0
                    
                if float elapsed > minInterval then
                    // Try to claim promotion by atomically updating timestamp
                    if Interlocked.CompareExchange(&this.LastPromotionTime, now, 
                                                    Volatile.Read(&this.LastPromotionTime)) <> now then
                        // We won the race to do promotion
                        let candidates = this.GetPromotionCandidates(this.Configuration.MaxPromotionBatchSize)
                        
                        if candidates.Length > 0 then
                            // Update signal count atomically
                            Interlocked.Add(&this.PromotionSignalCount, -int64 candidates.Length) |> ignore
                        
                        candidates
                    else
                        // Another thread is handling promotion
                        [||]
                else
                    // Not enough time elapsed since last promotion
                    [||]
            else
                // No promotion needed
                [||]
        
        // Get temperature statistics
        member this.GetTemperatureStats() =
            // Calculate statistics from the string temperatures
            let temperatures = this.StringTemperatures.Values
            if temperatures.Count = 0 then
                0.0, 0, 0
            else
                let mutable sum = 0
                let mutable max = 0
                let mutable hotCount = 0
                let threshold = this.GetAdjustedPromotionThreshold()
                
                for temp in temperatures do
                    sum <- sum + temp
                    max <- Math.Max(max, temp)
                    if temp >= threshold then
                        hotCount <- hotCount + 1
                
                float sum / float temperatures.Count, max, hotCount
        
        interface IDisposable with
            member this.Dispose() = 
                this.ChunksLock.Dispose()

    // Generational pool with eden space and chunked overflow
    type Pool = {
        // Eden space (pre-allocated fixed array)
        EdenArray: string[]
        mutable EdenOffset: int32
        
        // Post-eden space (chunked for growth)
        PostEden: Segments
        
        // Base ID for this pool
        PoolBaseId: int
        
        // Configuration
        Configuration: PoolConfiguration
        
        // Temperature tracking for eden space
        EdenTemperatures: ConcurrentDictionary<string, int32>
        mutable LastDecayTime: DateTime
        mutable LastPromotionTime: int64
        mutable PromotionSignalCount: int64

        EdenPromotionTracker: EpochPromotionTracker
        
        // Stats
        mutable Stats: Stats
    } with
        static member Create(baseId: int, edenSize: int, config: PoolConfiguration) =
            let rotationInterval = 
                TimeSpan.FromMilliseconds(config.MinPromotionInterval.TotalMilliseconds / 3.0)
            {
                EdenArray = Array.zeroCreate edenSize
                EdenOffset = 0
                PostEden = Segments.Create(baseId + edenSize, config)
                PoolBaseId = baseId
                Configuration = config
                EdenTemperatures = ConcurrentDictionary<string, int32>()
                LastDecayTime = DateTime.UtcNow
                LastPromotionTime = 0L
                PromotionSignalCount = 0L
                EdenPromotionTracker = EpochPromotionTracker.Create(
                    queueSizeBits = 14,  // 16K signals for Eden space
                    rotationInterval = rotationInterval
                )
                Stats = { accessCount = 0L; missCount = 0L }
            }
        
        // Get current promotion threshold for eden space, adjusted for size
        member this.GetAdjustedEdenPromotionThreshold() =
            let stringCount = this.EdenTemperatures.Count
            let baseThreshold = 
                if this.PoolBaseId < IdAllocation.GroupPoolBase then 
                    this.Configuration.GroupPromotionThreshold 
                else 
                    this.Configuration.WorkerPromotionThreshold
                    
            let scalingFactor = 
                1.0 + 
                (float stringCount / float this.Configuration.ThresholdScalingInterval) * 
                (this.Configuration.ThresholdScalingFactor - 1.0)
            int (float baseThreshold * scalingFactor)
        
        // Get batch of promotion candidates from eden space
        member private this.GetEdenPromotionCandidates(maxBatchSize: int) : PromotionCandidate[] =
            // Threshold with scaling applied
            let threshold = this.GetAdjustedEdenPromotionThreshold()
            
            // Find candidates that exceed threshold
            this.EdenTemperatures
            |> Seq.filter (fun kvp -> kvp.Value >= threshold)
            |> Seq.truncate maxBatchSize
            |> Seq.map (fun kvp -> { Value = kvp.Key; Temperature = kvp.Value })
            |> Seq.toArray
        
        // Reset temperature for a promoted string in eden space
        member this.ResetEdenTemperature(str: string) =
            this.EdenTemperatures.AddOrUpdate(str, 0, fun _ _ -> 0) |> ignore
        
        // Check both eden and post-eden spaces for promotion candidates
        member this.CheckPromotion() : PromotionCandidate[] =
            // Process both Eden and PostEden promotions
            let edenResolver (id: StringId) =
                let idValue = id.Value
                let relativeId = idValue - this.PoolBaseId
                if relativeId >= 0 && relativeId < this.EdenArray.Length then
                    let str = Volatile.Read &this.EdenArray.[relativeId]
                    if not (isNull str) then Some str else None
                else
                    None
            
            let edenCandidates = this.EdenPromotionTracker.ProcessPromotions edenResolver
            
            // Pass the correct baseId for PostEden
            let postEdenBaseId = this.PoolBaseId + this.EdenArray.Length
            let postEdenCandidates = this.PostEden.ProcessPromotions(postEdenBaseId)
            
            Array.append edenCandidates postEdenCandidates
        
        member this.InternString(str: string) : StringId =
            if isNull str || str.Length = 0 then
                raise (ArgumentException "String cannot be null or empty")
                        
            // Opportunistically check for promotion
            this.CheckPromotion() |> ignore
            
            Interlocked.Increment(&this.Stats.accessCount) |> ignore
            
            // Try eden space first
            let edenIdx = Interlocked.Increment(&this.EdenOffset) - 1
            if edenIdx < this.EdenArray.Length then
                // Eden has space
                let id = StringId.Create(this.PoolBaseId + edenIdx)
                Volatile.Write(&this.EdenArray.[edenIdx], str)
                
                // Update temperature with atomic operation
                this.EdenTemperatures.AddOrUpdate(str, 1, fun _ oldTemp -> 
                    let newTemp = oldTemp + 1
                    
                    // Signal promotion if temperature crosses threshold
                    let threshold = this.GetAdjustedEdenPromotionThreshold()
                    if newTemp = threshold then
                        Interlocked.Increment(&this.PromotionSignalCount) |> ignore
                        
                    newTemp) |> ignore
                    
                Interlocked.Increment(&this.Stats.missCount) |> ignore
                id
            else
                // Eden full, use post-eden segments
                this.PostEden.AllocateString(str, this.PoolBaseId + this.EdenArray.Length)
        
        member this.TryGetString(id: StringId) : string option =
            let idValue = id.Value
            if idValue < this.PoolBaseId then
                None
            else
                let relativeId = idValue - this.PoolBaseId
                if relativeId < this.EdenArray.Length then
                    // In eden space
                    let str = Volatile.Read(&this.EdenArray.[relativeId])
                    if not (isNull str) then Some str else None
                else
                    // In post-eden segments
                    this.PostEden.TryGetString(idValue, this.PoolBaseId + this.EdenArray.Length)
        
        member this.GetStringWithTemperature(id: StringId) : (string option * StringId) =
            let idValue = id.Value
            if idValue < this.PoolBaseId then
                None, id
            else
                let relativeId = idValue - this.PoolBaseId
                if relativeId < this.EdenArray.Length then
                    // In eden space
                    let str = Volatile.Read(&this.EdenArray.[relativeId])
                    if not (isNull str) then 
                        let newId = id.IncrementTemperature()
                        
                        // Check for promotion threshold
                        let threshold = this.GetAdjustedEdenPromotionThreshold()
                        if newId.Temperature = threshold then
                            // Pass id, str, and temperature
                            this.EdenPromotionTracker.SignalPromotion(newId, newId.Temperature) |> ignore
                        
                        Some str, newId
                    else 
                        None, id
                else
                    // In post-eden segments
                    match this.PostEden.TryGetString(idValue, this.PoolBaseId + this.EdenArray.Length) with
                    | Some str ->
                        let newId = id.IncrementTemperature()
                        
                        // Check for promotion threshold
                        let threshold = this.PostEden.GetAdaptiveThreshold()
                        if newId.Temperature = threshold then
                            // Pass id, str, and temperature
                            this.PostEden.PromotionTracker.SignalPromotion(newId, newId.Temperature) |> ignore
                        
                        Some str, newId
                    | None ->
                        None, id
        
        member this.GetStats() : PoolStats =
            let edenMemory = 
                this.EdenArray
                |> Array.take (min this.EdenOffset this.EdenArray.Length)
                |> Array.sumBy (fun s -> if isNull s then 0L else int64 (s.Length * 2))
            
            let segmentMemory = 
                this.PostEden.Chunks
                |> Seq.sumBy (fun chunk ->
                    chunk |> Array.sumBy (fun s -> if isNull s then 0L else int64 (s.Length * 2)))
            
            let accessCount = Interlocked.Read(&this.Stats.accessCount)
            let missCount = Interlocked.Read(&this.Stats.missCount)
            let hitRatio = 
                if accessCount > 0L then
                    float (accessCount - missCount) / float accessCount
                else 0.0
            
            // Get temperature stats
            let edenTemps = this.EdenTemperatures.Values
            let edenAvgTemp = 
                if edenTemps.Count > 0 then
                    let mutable sum = 0
                    for t in edenTemps do sum <- sum + t
                    float sum / float edenTemps.Count
                else 0.0
                
            let edenMaxTemp = 
                if edenTemps.Count > 0 then
                    let mutable max = 0
                    for t in edenTemps do max <- Math.Max(max, t)
                    max
                else 0
                
            let threshold = this.GetAdjustedEdenPromotionThreshold()
            let edenHotCount = 
                if edenTemps.Count > 0 then
                    let mutable count = 0
                    for t in edenTemps do 
                        if t >= threshold then count <- count + 1
                    count
                else 0
            
            let segAvgTemp, segMaxTemp, segHotCount = this.PostEden.GetTemperatureStats()
            
            // Calculate combined temperature stats
            let combinedMaxTemp = max edenMaxTemp segMaxTemp
            let edenCount = this.EdenTemperatures.Count
            let segCount = this.PostEden.StringTemperatures.Count
            let totalCount = edenCount + segCount
            
            let combinedAvgTemp = 
                if totalCount > 0 then
                    (edenAvgTemp * float edenCount + segAvgTemp * float segCount) / 
                    float totalCount
                else 0.0
            
            let pendingPromotions = 
                Interlocked.Read(&this.PromotionSignalCount) + 
                Interlocked.Read(&this.PostEden.PromotionSignalCount)
            
            {
                TotalStrings = this.EdenOffset + this.PostEden.CurrentChunkIndex * this.PostEden.ChunkSize +
                                this.PostEden.ChunkOffsets.[this.PostEden.CurrentChunkIndex]
                MemoryUsageBytes = edenMemory + segmentMemory
                HitRatio = hitRatio
                AccessCount = accessCount
                MissCount = missCount
                AverageTemperature = combinedAvgTemp
                MaxTemperature = combinedMaxTemp
                HotStringsCount = edenHotCount + segHotCount
                PendingPromotions = pendingPromotions
            }

    type GlobalPool = {
        // Immutable planning strings
        PlanningStrings: FastMap<string, StringId>
        PlanningArray: string[]
        PlanningCount: int
        
        // Runtime pool with generational design
        RuntimePool: Pool 
        
        // Stats inline (avoiding indirection)
        mutable Stats: Stats
    } with
        
        member this.InternString(str: string) : StringId =
            // Check planning strings first (immutable, lock-free)
            Interlocked.Increment(&this.Stats.accessCount) |> ignore
            match FastMap.tryFind str this.PlanningStrings with
            | ValueSome id -> id
            | ValueNone ->
                // Use runtime pool
                Interlocked.Increment(&this.Stats.missCount) |> ignore
                this.RuntimePool.InternString(str)
        
        member this.TryGetStringId(str: string) : StringId option =
            match FastMap.tryFind str this.PlanningStrings with
            | ValueSome id -> Some id
            | ValueNone ->
                // For runtime strings, we'd need a reverse lookup
                // This is not efficient, so we return None
                None
        
        member this.GetString(id: StringId) : string option =
            if id.Value < this.PlanningCount then
                // Planning string - direct array access
                Some this.PlanningArray.[id.Value]
            elif id.Value < IdAllocation.GlobalRuntimeBase then
                // Runtime string - no temperature update at this level
                this.RuntimePool.TryGetString(id)
            else
                // Not a global ID
                None

        member this.GetStringWithTemperature(id: StringId) : (string option * StringId) =
            if id.Value < this.PlanningCount then
                // Planning strings don't need temperature tracking
                Some this.PlanningArray.[id.Value], id
            elif id.Value < IdAllocation.GlobalRuntimeBase then
                // Delegate to runtime pool for temperature tracking
                this.RuntimePool.GetStringWithTemperature(id)
            else
                // Not a global ID
                None, id
        
        // Check promotion queue in runtime pool
        member this.CheckPromotion() : PromotionCandidate[] =
            this.RuntimePool.CheckPromotion()
        
        member this.GetStats() : PoolStats = 
            let planningMemory = 
                this.PlanningArray 
                |> Array.sumBy (fun s -> if isNull s then 0L else int64 (s.Length * 2))
            
            let runtimeStats = this.RuntimePool.GetStats()
            
            // Calculate hit ratio for this pool
            // NB: Stats provides atomic access to counters
            let accessCount = Interlocked.Read &this.Stats.accessCount
            let missCount   = Interlocked.Read &this.Stats.missCount
            let hitRatio    = 
                if accessCount > 0L then
                    float (accessCount - missCount) / float accessCount
                else 0.0
            in
            { 
                TotalStrings     = this.PlanningArray.Length + runtimeStats.TotalStrings
                MemoryUsageBytes = planningMemory + runtimeStats.MemoryUsageBytes
                HitRatio         = hitRatio
                AccessCount      = accessCount + runtimeStats.AccessCount
                MissCount        = missCount + runtimeStats.MissCount
                AverageTemperature = runtimeStats.AverageTemperature
                MaxTemperature   = runtimeStats.MaxTemperature
                HotStringsCount  = runtimeStats.HotStringsCount
                PendingPromotions = runtimeStats.PendingPromotions
            }

    (*
        Maintains the string pool for a whole dependency group.
        The least efficient tier, but allows for thread-safe sharing
        while providing a relatively fast path for access across tiers.    
    *)
    type GroupPool = {
        GroupId: DependencyGroupId
        GlobalPool: GlobalPool
        GroupPoolBaseId: int  // Pre-calculated base for this group
        
        // Group pool with generational design
        GroupPool: Pool
        
        // Cache of frequently accessed strings from other tiers
        CrossTierCache: ConcurrentDictionary<StringId, string>

        // Stats recording
        mutable Stats: Stats
    } with
        
        member this.InternString(str: string) : StringId =
            if isNull str || str.Length = 0 then
                raise (ArgumentException "String cannot be null or empty")
            
            // Opportunistically check for promotion
            this.CheckPromotion() |> ignore
            
            Interlocked.Increment(&this.Stats.accessCount) |> ignore
            
            // Check global pool first (it might be a planning string)
            let globalResult = this.GlobalPool.TryGetStringId str
            match globalResult with
            | Some id -> id
            | None ->
                // Use group pool
                Interlocked.Increment(&this.Stats.missCount) |> ignore
                this.GroupPool.InternString(str)
        
        // In GroupPool
        member this.GetString(id: StringId) : string option =
            // Check if it's in our group's range
            if id.Value >= this.GroupPoolBaseId && 
                id.Value < this.GroupPoolBaseId + IdAllocation.GroupPoolRangeSize then
                // It's a group ID - no temperature update at this level
                this.GroupPool.TryGetString(id)
            else
                // Check cross-tier cache first
                match this.CrossTierCache.TryGetValue id with
                | true, str -> Some str
                | false, _ ->
                    // Fetch from the appropriate tier
                    let result = 
                        if id.Value < IdAllocation.GroupPoolBase then
                            this.GlobalPool.GetString id
                        else
                            None  // Would be another group or local pool
                    
                    // Cache the result if found
                    match result with
                    | Some str -> 
                        this.CrossTierCache.TryAdd(id, str) |> ignore
                        Some str
                    | None -> None

        member this.GetStringWithTemperature(id: StringId) : (string option * StringId) =
            // Check if it's in our group's range
            if id.Value >= this.GroupPoolBaseId && 
                id.Value < this.GroupPoolBaseId + IdAllocation.GroupPoolRangeSize then
                // Delegate to group pool for temperature tracking
                this.GroupPool.GetStringWithTemperature(id)
            else
                // For cross-tier access, we don't update temperature
                // (temperature is owned by the tier that allocated the string)
                match this.GetString(id) with
                | Some str -> Some str, id
                | None -> None, id
        
        // Process promotion in group pool and promote to global
        member this.CheckPromotion() : unit =
            let candidates = this.GroupPool.CheckPromotion()
            
            // Promote candidates to global pool
            for candidate in candidates do
                // Intern in global pool
                let _ = this.GlobalPool.InternString candidate.Value
                
                // Reset temperature in group pool
                this.GroupPool.ResetEdenTemperature(candidate.Value)
                this.GroupPool.PostEden.ResetTemperature(candidate.Value)
        
        member this.GetStats() : PoolStats = 
            let groupStats = this.GroupPool.GetStats()
            
            // Calculate hit ratio for this pool
            // NB: Stats provides atomic access to counters
            let access = Interlocked.Read &this.Stats.accessCount
            let miss   = Interlocked.Read &this.Stats.missCount
            let hitRatio = 
                if access > 0L then
                    float (access - miss) / float access
                else 0.0
            in
            { 
                TotalStrings     = groupStats.TotalStrings
                MemoryUsageBytes = groupStats.MemoryUsageBytes
                HitRatio         = hitRatio
                AccessCount      = access + groupStats.AccessCount
                MissCount        = miss + groupStats.MissCount
                AverageTemperature = groupStats.AverageTemperature
                MaxTemperature = groupStats.MaxTemperature
                HotStringsCount = groupStats.HotStringsCount
                PendingPromotions = groupStats.PendingPromotions
            }
        
    (*
        Lock-free local string pool for each worker.
    *)
    type LocalPool = {
        WorkerId: WorkerId
        GroupPool: GroupPool option
        GlobalPool: GlobalPool
        
        // Fast local storage - no synchronization needed
        mutable LocalStrings: FastMap<string, StringId>
        mutable LocalArray: string[]
        
        LocalPoolBaseId: int  // Pre-calculated base ID for this worker
        mutable NextLocalId: int  // TODO: we should shift to int64 for atomic ops
        mutable MaxSize: int
        
        // LRU eviction for bounded size
        AccessOrder: LinkedList<string>  // Tracks access order by string
        mutable StringToNode: FastMap<string, LinkedListNode<string>>  // O(1) node lookup
        
        // Temperature tracking - no thread-safety needed for local pool
        mutable StringTemperatures: FastMap<string, int32>
        mutable HotStrings: FSharp.HashCollections.HashSet<string>
        mutable LastDecayTime: DateTime
        mutable LastPromotionTime: int64
        mutable PromotionSignalCount: int64
        mutable Configuration: PoolConfiguration

        // Stats recording
        mutable Stats: Stats
    } with
        
        member this.InternString(str: string, pattern: StringAccessPattern) : StringId =
            if isNull str || str.Length = 0 then
                raise (ArgumentException "String cannot be null or empty")

            // Opportunistically check for promotion
            this.CheckPromotion()

            (* 
                We can skip the atomic ops here, as we're always in the 
                worker's thread. The miss-count increment is handled by AddLocal. 
            *)
            this.Stats.accessCount <- this.Stats.accessCount + 1L
            
            match pattern with
            | StringAccessPattern.HighFrequency ->
                // Try local first
                match FastMap.tryFind str this.LocalStrings with
                | ValueSome id -> 
                    this.UpdateLRU str
                    
                    // Update temperature (no thread safety needed in worker pool)
                    let currentTemp = 
                        match FastMap.tryFind str this.StringTemperatures with
                        | ValueSome temp -> temp
                        | ValueNone -> 0
                    let newTemp = currentTemp + 1
                    this.StringTemperatures <- FastMap.add str newTemp this.StringTemperatures
                    
                    // Check if this string is hot enough to promote
                    let adjustedThreshold = this.GetAdjustedPromotionThreshold()
                    
                    if newTemp >= adjustedThreshold && not (HashSet.contains str this.HotStrings) then
                        // Mark as hot and signal promotion
                        this.HotStrings <- HashSet.add str this.HotStrings
                        this.PromotionSignalCount <- this.PromotionSignalCount + 1L
                    
                    id
                | ValueNone ->
                    // Check if we have room
                    if FastMap.count this.LocalStrings < this.MaxSize then
                        this.AddLocal str
                    else
                        // Evict LRU and add
                        this.EvictLRU()
                        this.AddLocal str

            | StringAccessPattern.Planning ->
                // Delegate to global pool for planning strings
                this.GlobalPool.InternString str
            
            | _ ->
                // Medium/Low frequency - use group pool if available
                match this.GroupPool with
                | Some pool -> pool.InternString str
                | None -> this.GlobalPool.InternString str

        member this.GetString(id: StringId) : string option =
            // First check if it's in our local range
            if id.Value < 0 then 
                None
            else 
                this.Stats.accessCount <- this.Stats.accessCount + 1L
                if id.Value >= this.LocalPoolBaseId && 
                    id.Value < this.LocalPoolBaseId + IdAllocation.WorkerRangeSize then
                    // It's a local ID                
                    let localIndex = id.Value - this.LocalPoolBaseId
                    if localIndex >= 0 && localIndex < this.LocalArray.Length then
                        Some this.LocalArray.[localIndex]
                    else
                        None
                else
                    // Not local - delegate to appropriate pool
                    if id.Value < IdAllocation.GroupPoolBase then
                        this.GlobalPool.GetString id
                    else
                        match this.GroupPool with
                        | Some pool -> pool.GetString id
                        | None -> this.GlobalPool.GetString id

        member this.GetStringWithTemperature(id: StringId) : (string option * StringId) =
            if id.Value >= this.LocalPoolBaseId && 
                id.Value < this.LocalPoolBaseId + IdAllocation.WorkerRangeSize then
                // Local string
                let localIndex = id.Value - this.LocalPoolBaseId
                if localIndex >= 0 && localIndex < this.LocalArray.Length then
                    let str = this.LocalArray.[localIndex]
                    if not (isNull str) then
                        let newId = id.IncrementTemperature()
                        
                        // Check for promotion
                        let threshold = this.GetAdjustedPromotionThreshold()
                        if newId.Temperature = threshold then
                            // In local pool, we track promotion candidates differently
                            this.HotStrings <- HashSet.add str this.HotStrings
                            this.PromotionSignalCount <- this.PromotionSignalCount + 1L
                        
                        Some str, newId
                    else
                        None, id
                else
                    None, id
            else
                // Delegate to appropriate pool
                if id.Value < IdAllocation.GroupPoolBase then
                    this.GlobalPool.GetStringWithTemperature id
                else
                    match this.GroupPool with
                    | Some pool -> pool.GetStringWithTemperature id
                    | None -> this.GlobalPool.GetStringWithTemperature id

        // Get adjusted promotion threshold based on pool size
        member this.GetAdjustedPromotionThreshold() =
            let stringCount = FastMap.count this.LocalStrings
            let scalingFactor = 
                1.0 + 
                (float stringCount / float this.Configuration.ThresholdScalingInterval) * 
                (this.Configuration.ThresholdScalingFactor - 1.0)
            int (float this.Configuration.WorkerPromotionThreshold * scalingFactor)
        
        // Check for hot strings to promote
        member this.CheckPromotion() : unit =
            // Fast check for any promotion signals
            if this.PromotionSignalCount > 0L then
                // Check if enough time has passed since last promotion
                let now = Stopwatch.GetTimestamp()
                let elapsed = now - this.LastPromotionTime
                let minInterval = 
                    this.Configuration.MinPromotionInterval.TotalMilliseconds * 
                    float Stopwatch.Frequency / 1000.0
                    
                if float elapsed > minInterval then
                    // Update timestamp (no race condition since we're single-threaded)
                    this.LastPromotionTime <- now
                    
                    // Get hot strings to promote
                    let threshold = this.GetAdjustedPromotionThreshold()
                    let hotStrings = 
                        this.HotStrings
                        |> HashSet.toSeq
                        |> Seq.filter (fun str ->
                            match FastMap.tryFind str this.StringTemperatures with
                            | ValueSome temp -> temp >= threshold
                            | ValueNone -> false)
                        |> Seq.truncate this.Configuration.MaxPromotionBatchSize
                        |> Seq.toArray
                    
                    // Promote to group pool if available
                    if hotStrings.Length > 0 then
                        match this.GroupPool with
                        | Some pool -> 
                            for str in hotStrings do
                                // Promote to group
                                let _ = pool.InternString str
                                
                                // Reset temperature
                                this.StringTemperatures <- FastMap.add str 0 this.StringTemperatures
                                this.HotStrings <- HashSet.remove str this.HotStrings
                        | None -> 
                            // No group pool, promote directly to global
                            for str in hotStrings do
                                let _ = this.GlobalPool.InternString str
                                
                                // Reset temperature
                                this.StringTemperatures <- FastMap.add str 0 this.StringTemperatures
                                this.HotStrings <- HashSet.remove str this.HotStrings
                        
                        // Update signal count
                        this.PromotionSignalCount <- this.PromotionSignalCount - int64 hotStrings.Length
        
        // Decay all string temperatures
        member this.DecayTemperatures(decayFactor: float) =
            // Create new temperature map with decayed values
            let mutable newTemps = FastMap.empty
            let mutable newHotStrings = HashSet.empty
            
            for kv in FastMap.toSeq this.StringTemperatures do
                let struct (str, temp) = kv
                let newTemp = int32 (float temp * decayFactor)
                if newTemp > 0 then 
                    newTemps <- FastMap.add str newTemp newTemps
                    
                    // Check if still hot
                    let threshold = this.GetAdjustedPromotionThreshold()
                    if newTemp >= threshold && HashSet.contains str this.HotStrings then
                        newHotStrings <- HashSet.add str newHotStrings
            
            this.StringTemperatures <- newTemps
            this.HotStrings <- newHotStrings
            
            // Update promotion signal count
            this.PromotionSignalCount <- int64 (HashSet.count newHotStrings)
            this.LastDecayTime <- DateTime.UtcNow

        (* NB: Because stats calls can come from another thread, we use atomic ops *)
        member this.GetStats() : PoolStats = 
            let nextLocalId = this.NextLocalId
            let id = max (nextLocalId - 1) 0
            let localMemory = 
                this.LocalArray
                |> Array.take id
                |> Array.sumBy (fun s -> if isNull s then 0L else int64 (s.Length * 2))
            
            // Calculate hit ratio for this pool - although we are single
            // threaded here by convension, we still use atomic ops
            let access = this.Stats.accessCount
            let miss   = this.Stats.missCount
            let hitRatio = 
                if access > 0L then float (access - miss) / float access
                else 0.0
            
            // Calculate temperature stats
            let tempValues = 
                FastMap.fold (fun acc _ temp -> temp :: acc) [] this.StringTemperatures
            
            let avgTemp = 
                if List.isEmpty tempValues then 0.0
                else float (List.sum tempValues) / float tempValues.Length
            
            let maxTemp = 
                if List.isEmpty tempValues then 0
                else List.max tempValues
            
            let hotStringsCount = HashSet.count this.HotStrings
            
            in
            { 
                TotalStrings     = nextLocalId + 1
                MemoryUsageBytes = localMemory
                HitRatio         = hitRatio
                AccessCount      = access
                MissCount        = miss
                AverageTemperature = avgTemp
                MaxTemperature = maxTemp
                HotStringsCount = hotStringsCount
                PendingPromotions = this.PromotionSignalCount
            }

        (* NB: LRU OPS are only called by one thread at a time *)
        
        member private this.AddLocal(str: string) : StringId =
            let currentId = this.NextLocalId
            let newId = StringId.Create(this.LocalPoolBaseId + this.NextLocalId)
            
            this.Stats.missCount <- this.Stats.missCount + 1L
            this.NextLocalId <- this.NextLocalId + 1
            
            if currentId < this.LocalArray.Length then
                this.LocalArray.[currentId] <- str
            else
                // Need to grow local array
                let newSize = min (this.LocalArray.Length * 2) this.MaxSize
                let newArray = Array.zeroCreate newSize
                Array.Copy(this.LocalArray, newArray, this.LocalArray.Length)
                newArray.[currentId] <- str
                this.LocalArray <- newArray
            
            this.LocalStrings <- FastMap.add str newId this.LocalStrings
            
            // Initialize temperature
            this.StringTemperatures <- FastMap.add str 1 this.StringTemperatures
            
            // LRU tracking is essential to the lock-free design
            let node = this.AccessOrder.AddLast(str)
            this.StringToNode <- FastMap.add str node this.StringToNode
            newId
        
        member private this.UpdateLRU(str: string) : unit =
            match FastMap.tryFind str this.StringToNode with
            | ValueSome node ->
                // Move to end (most recently used)
                this.AccessOrder.Remove node
                this.AccessOrder.AddLast node
            | ValueNone -> 
                // Shouldn't happen, but handle gracefully
                let node = this.AccessOrder.AddLast str
                this.StringToNode <- FastMap.add str node this.StringToNode
        
        member private this.EvictLRU() : unit =
            if this.AccessOrder.Count > 0 then
                // Get least recently used (first in list)
                let lruNode = this.AccessOrder.First
                let lruString = lruNode.Value
                
                // Remove from LRU tracking
                this.AccessOrder.RemoveFirst()
                this.StringToNode <- FastMap.remove lruString this.StringToNode
                
                // Remove from temperature tracking
                this.StringTemperatures <- FastMap.remove lruString this.StringTemperatures
                if HashSet.contains lruString this.HotStrings then
                    this.HotStrings <- HashSet.remove lruString this.HotStrings

                // Remove from storage and blank the array slot
                match FastMap.tryFind lruString this.LocalStrings with
                | ValueSome id ->
                    this.LocalStrings <- FastMap.remove lruString this.LocalStrings
                    let localIndex = id.Value - this.LocalPoolBaseId
                    if localIndex >= 0 && localIndex < this.LocalArray.Length then
                        this.LocalArray.[localIndex] <- Unchecked.defaultof<string>
                | ValueNone -> ()

    let createStringPoolHierarchy (planningStrings: string[]) (config: PoolConfiguration) =
        let ps = planningStrings 
                |> Array.mapi (fun i str -> str, StringId i)
                |> Array.fold (fun acc (s, id) -> FastMap.add s id acc) FastMap.empty

        // Global pool with planning strings and runtime pool
        let globalPool = {
            PlanningStrings   = ps
            PlanningArray     = planningStrings
            PlanningCount     = planningStrings.Length
            RuntimePool       = Pool.Create(planningStrings.Length, config.GlobalEdenSize, config)
            Stats = 
                {
                    accessCount = 0L
                    missCount   = 0L
                }
        }

        // Group pool for a dependency group
        let createGroupPool (groupId: DependencyGroupId) =
            let groupBaseId = IdAllocation.getGroupPoolBaseId groupId
            {
                GroupId         = groupId
                GlobalPool      = globalPool
                GroupPoolBaseId = groupBaseId
                GroupPool       = Pool.Create(groupBaseId, config.GroupEdenSize, config)
                CrossTierCache  = ConcurrentDictionary<StringId, string>()
                Stats = 
                    {
                        accessCount = 0L
                        missCount   = 0L
                    }
            }

        // Create local pool for a worker with configuration
        let createLocalPool 
                (workerId: WorkerId) 
                (groupPool: GroupPool option) = 
            let lpBaseId = IdAllocation.getLocalPoolBaseId 
                                (groupPool |> Option.map (fun p -> p.GroupId)) 
                                workerId
            in    
            {
                WorkerId        = workerId
                GroupPool       = groupPool
                GlobalPool      = globalPool
                LocalStrings    = FastMap.empty
                LocalArray      = Array.create config.WorkerEdenSize Unchecked.defaultof<string>
                NextLocalId     = 0
                LocalPoolBaseId = lpBaseId
                MaxSize         = config.WorkerEdenSize
                AccessOrder     = LinkedList<string>()
                StringToNode    = FastMap.empty
                StringTemperatures = FastMap.empty
                HotStrings      = HashSet.empty
                LastDecayTime   = DateTime.UtcNow
                LastPromotionTime = Stopwatch.GetTimestamp()
                PromotionSignalCount = 0L
                Configuration   = config
                Stats = 
                    {
                        accessCount = 0L
                        missCount   = 0L
                    }
            }

        globalPool, createGroupPool, createLocalPool

    type ExecutionContext = {
        WorkerId: WorkerId
        GroupId: DependencyGroupId option
        ContextPool: LocalPool
        ParentPoolRef: GroupPool option
        GlobalPoolRef: GlobalPool
    } with
        static member Create(hierarchy: StringPoolHierarchy, 
                                groupId: DependencyGroupId option, 
                                maxLocalStrings: int option) =
            let workerId = WorkerId.Create()
            let groupPool = groupId |> Option.map hierarchy.GroupPoolBuilder
            let contextPool = 
                let pool = hierarchy.WorkerPoolBuilder workerId groupPool
                match maxLocalStrings with
                | Some max -> { pool with MaxSize = max }
                | None -> pool
            in
            {
                WorkerId = workerId
                GroupId = groupId
                ContextPool = contextPool
                ParentPoolRef = groupPool
                GlobalPoolRef = hierarchy.GlobalPool
            }

    and StringPoolHierarchy = {
        GlobalPool: GlobalPool
        WorkerPoolBuilder: WorkerId -> GroupPool option -> LocalPool
        GroupPoolBuilder: DependencyGroupId -> GroupPool
        WorkerPools: ConcurrentDictionary<WorkerId, LocalPool>
        GroupPools: ConcurrentDictionary<DependencyGroupId, GroupPool>
        Configuration: PoolConfiguration
        mutable LastDecayTime: DateTime
        mutable TotalMemoryUsage: int64
    } with
        
        static member Create(planningStrings: string[], config: PoolConfiguration) =
            let globalPool, createGroupPool, createLocalPool = 
                createStringPoolHierarchy planningStrings config
            {
                GlobalPool = globalPool
                WorkerPoolBuilder = createLocalPool
                GroupPoolBuilder = createGroupPool
                WorkerPools = ConcurrentDictionary<WorkerId, LocalPool>()
                GroupPools = ConcurrentDictionary<DependencyGroupId, GroupPool>()
                Configuration = config
                LastDecayTime = DateTime.UtcNow
                TotalMemoryUsage = 0L
            }
        
        static member Create(planningStrings: string[]) =
            StringPoolHierarchy.Create(planningStrings, PoolConfiguration.Default)
        
        member this.CreateExecutionContext(groupId: DependencyGroupId option, 
                                            maxLocalStrings: int option) : ExecutionContext =
            ExecutionContext.Create(this, groupId, maxLocalStrings)

        member this.GetOrCreateGroupPool(groupId: DependencyGroupId) : GroupPool =
            this.GroupPools.GetOrAdd(groupId, this.GroupPoolBuilder)
        
        member this.GetGlobalStats() : PoolStats =
            this.GlobalPool.GetStats()            
        
        member this.GetAggregateStats() : PoolStats =
            // NB: ignores worker pools for now, as that would 
            // require cleanup logic when contexts are disposed.
            let globalStats = this.GlobalPool.GetStats()
            
            // Only collect stats from group pools (worker pools delegate to these anyway)
            let groupStats = 
                this.GroupPools.Values 
                |> Seq.map (fun pool -> pool.GetStats())
                |> Seq.fold (fun acc stats -> {
                    TotalStrings = acc.TotalStrings + stats.TotalStrings
                    MemoryUsageBytes = acc.MemoryUsageBytes + stats.MemoryUsageBytes
                    HitRatio = 0.0 // Will re-calculate at the end
                    AccessCount = acc.AccessCount + stats.AccessCount
                    MissCount = acc.MissCount + stats.MissCount
                    AverageTemperature = acc.AverageTemperature + stats.AverageTemperature
                    MaxTemperature = max acc.MaxTemperature stats.MaxTemperature
                    HotStringsCount = acc.HotStringsCount + stats.HotStringsCount
                    PendingPromotions = acc.PendingPromotions + stats.PendingPromotions
                }) PoolStats.Empty
            
            let totalAccessCount = globalStats.AccessCount + groupStats.AccessCount
            let totalMissCount = globalStats.MissCount + groupStats.MissCount
            
            let overallHitRatio = 
                if totalAccessCount > 0L then 
                    float (totalAccessCount - totalMissCount) / float totalAccessCount
                else 0.0
                
            // Calculate average temperature weighted by pool size
            let avgTemp = 
                if groupStats.TotalStrings > 0 || globalStats.TotalStrings > 0 then
                    (globalStats.AverageTemperature * float globalStats.TotalStrings + 
                    groupStats.AverageTemperature * float groupStats.TotalStrings) /
                    float (globalStats.TotalStrings + groupStats.TotalStrings)
                else 0.0
            
            in
            {
                TotalStrings = globalStats.TotalStrings + groupStats.TotalStrings
                MemoryUsageBytes = globalStats.MemoryUsageBytes + groupStats.MemoryUsageBytes
                HitRatio = overallHitRatio
                AccessCount = totalAccessCount
                MissCount = totalMissCount
                AverageTemperature = avgTemp
                MaxTemperature = max globalStats.MaxTemperature groupStats.MaxTemperature
                HotStringsCount = globalStats.HotStringsCount + groupStats.HotStringsCount
                PendingPromotions = globalStats.PendingPromotions + groupStats.PendingPromotions
            }
            
        // Check for promotion across all tiers - returns number of strings promoted
        member this.CheckAndPromoteHotStrings() : int =
            // Promote any pending strings in global pool
            let globalCandidates = this.GlobalPool.CheckPromotion()
            
            // Check each group pool for promotion
            let mutable totalPromoted = 0
            for kvp in this.GroupPools do
                let groupPool = kvp.Value
                groupPool.CheckPromotion()
                
            // The local pools self-manage their promotion
            // during normal operations, so we don't need to iterate them here            
            globalCandidates.Length + totalPromoted        

    type PoolContextScope(context: ExecutionContext) =
        member __.Context = context
        
        member __.InternPlanningString(str: string) : StringId =
            context.GlobalPoolRef.InternString str

        member __.InternString(str: string, accessPattern: StringAccessPattern) : StringId =
            context.ContextPool.InternString(str, accessPattern)

        member __.InternString(str: string) : StringId =
            context.ContextPool.InternString(str, StringAccessPattern.MediumFrequency)

        member __.GetString(id: StringId) : string voption =
            match context.ContextPool.GetString id with
            | Some s -> ValueSome s
            | None -> ValueNone
        
        member __.GetStats() : PoolStats =
            context.ContextPool.GetStats()
        
        interface IDisposable with
            member __.Dispose() =
                // No extra cleanup needed - local pools handle their own promotion
                ()

    [<RequireQualifiedAccess>]
    module StringPool =

        /// Initialize the string pool hierarchy with known planning-time strings
        let create (planningStrings: string[]) : StringPoolHierarchy =
            StringPoolHierarchy.Create planningStrings
        
        /// Initialize the string pool hierarchy with configuration
        let createWithConfig (planningStrings: string[]) (config: PoolConfiguration) : StringPoolHierarchy =
            StringPoolHierarchy.Create(planningStrings, config)
        
        /// Create an execution context for a worker
        let createContext 
                (hierarchy: StringPoolHierarchy) 
                (groupId: DependencyGroupId option) 
                (maxLocalStrings: int option) : ExecutionContext =
            hierarchy.CreateExecutionContext(groupId, maxLocalStrings)
        
        /// Create a scoped context for RAII string pooling
        let createScope 
                (hierarchy: StringPoolHierarchy) 
                (groupId: DependencyGroupId option) 
                (maxLocalStrings: int option) : PoolContextScope =
            let context = hierarchy.CreateExecutionContext(groupId, maxLocalStrings)
            new PoolContextScope(context)
        
        /// Get global statistics
        let getGlobalStats (hierarchy: StringPoolHierarchy) : PoolStats =
            hierarchy.GetGlobalStats()
        
        /// Get aggregate statistics across all pools
        let getAggregateStats (hierarchy: StringPoolHierarchy) : PoolStats =
            hierarchy.GetAggregateStats()
        
        /// Check and promote hot strings across the hierarchy
        let checkAndPromoteHotStrings (hierarchy: StringPoolHierarchy) : int =
            hierarchy.CheckAndPromoteHotStrings()
                
        /// Calculate optimal configuration based on RML complexity (for integration with Planner)
        let calculateOptimalConfig (totalComplexity: int) (maxPathDepth: int) (avgPredicatesPerMap: float) : PoolConfiguration =
            let baseConfig = 
                if totalComplexity < 100 then PoolConfiguration.LowMemory
                elif totalComplexity < 500 then PoolConfiguration.Default
                else PoolConfiguration.HighPerformance
            
            // Adjust based on path depth and predicate complexity
            let adjustedEdenSize = 
                baseConfig.GlobalEdenSize + (maxPathDepth * 1000) + int (avgPredicatesPerMap * 100.0)
            
            let adjustedGroupSize =
                baseConfig.GroupEdenSize + (maxPathDepth * 500) + int (avgPredicatesPerMap * 50.0)
            
            let adjustedWorkerThreshold =
                max 20 (maxPathDepth * 5)
            
            let adjustedGroupThreshold =
                max 100 (totalComplexity * 2)
            
            { baseConfig with
                GlobalEdenSize = min 200000 adjustedEdenSize
                GroupEdenSize = min 100000 adjustedGroupSize
                WorkerPromotionThreshold = adjustedWorkerThreshold
                GroupPromotionThreshold = adjustedGroupThreshold
            }
