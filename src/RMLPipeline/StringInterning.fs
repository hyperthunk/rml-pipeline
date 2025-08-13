namespace RMLPipeline.Internal

(*
    Provides a hierarchical implementation of string interning, designed to meet 
    the needs of a parallel processing architecture, while providing efficient 
    memory usage and performance characteristics.

    This is a specialized, bounded, reference-counted string management approach 
    that is more efficient for RML processing patterns than relying solely on .NET's 
    general-purpose, global string interning mechanism.

    Routing is dynamic, based on the requested StringAccessPattern, providing
    automatic fallback between tiers, with preferential checking, where high-frequency 
    requests try local pools first, whilst planning requests route to global pool.

    When a string is interned at a higher tier (Group or Global), it gets a StringId 
    from that tier's range. If wokers require frequent access, the string doesn't move, 
    but rather the worker simply caches the lookup locally.

    This is a three-tiered design that attempts to balances contention, 
    memory usage, and access patterns.

    Planning Strings (Global Pool):
    - Templates like "http://example.org/{id}"
    - Shared across all workers
    - Lock-free reads for planning strings
    - Array-backed for cache efficiency

    Group Strings (Group Pool):
    - Predicates specific to a dependency group
    - Shared within group only - bounded contention within dependency groups
    - Reduces global pool contention

    Worker Strings (Local Pool):
    - Temporary strings during processing
    - Zero contention access for hot paths
    - Bounded size prevents memory bloat

    All strings produced during the planning phase are pre-allocated and immutable.
    The RAII pattern is provided using the PoolContextScope.

    Goals:
    - Lock-free reads from global planning strings
    - Fine-grained locking only where necessary
    - Value types for IDs and patterns
    - Aggressive inlining where beneficial
    - No thread affinity
    - Can be used with both F# async and Hopac (for parallel processing)

    Note: Avoiding coordination overheads & memory optimization.

    1. StringId tells you where the string lives - no searching through tiers

    Leveraging a generational design, we maintain precise ID-to-location mapping:

    Planning strings: 0 to PlanningCount-1
    Global runtime: PlanningCount to GlobalRuntimeBase-1
    Group pool: GroupPoolBase to LocalPoolBase-1
    Local pool: LocalPoolBase onward

    Within each pool, the ID tells us exactly where to look:

    - Eden space or chunked array
    - Which chunk and offset within that chunk

    Even emergency allocations (see below) have deterministic ID ranges, giving 
    us O(1) lookups with a maximum of one redirection.

    2. Cache-friendly lookups - direct array access in the owning tier

    For the primary access patterns:

    - Planning strings maintain direct array access (best cache locality)
    - Eden space strings maintain direct array access (excellent cache locality)
    - Chunked arrays access via one indirection (cache locality within chunks)
    - Chunks are designed to preserve cache-friendliness

    Each chunk is a contiguous array, maintaining good cache properties:

    - Sequential access within chunks is cache-efficient
    - Chunk sizes grow with usage patterns, balancing locality and memory usage
    - Temperature tracking groups hot strings together for promotion

    3. Controlled string duplication during incremental promotion

    Rather than maintaining a strict "one tier only" policy, we implement a controlled 
    promotion mechanism that temporarily allows strings to exist in multiple tiers. 
    Hot strings (frequently accessed) are incrementally promoted to higher tiers while 
    maintaining references in lower tiers until they're naturally evicted.

    The promotion process works through a temperature tracking system:
    - String access increases a temperature counter
    - When temperature exceeds an adaptive threshold, the string is flagged for promotion
    - Promotion is signaled through atomic counters to avoid contention
    - The string is interned in the higher tier while still accessible in the lower tier
    - Lower tier temperature is reset to prevent continuous promotion

    This incremental approach maintains lock-freedom while ensuring hot strings 
    eventually migrate to the most appropriate tier without blocking operations.

    4. Emergency Allocation Mechanism

    To ensure forward progress even in extreme contention scenarios, each tier includes 
    an emergency allocation pathway. If lock acquisition times out during chunk allocation:
    - The string is added to a concurrent dictionary with a special ID range
    - This provides guaranteed forward progress without blocking
    - Emergency allocations are still deterministically addressable
    - Temperature tracking continues to work for emergency-allocated strings

    This mechanism ensures the system remains responsive even under pathological 
    contention conditions, providing a fallback that maintains all core guarantees.

    Commit Comment (TBD):
    Thread Safety and Deadlock Avoidance:

    Earlier versions of the pool utilised snapshots to establish a consistent 
    view of global pool size, such that group pools would know which Ids are
    already taken globally, whether to check the global pool for a string, and
    when to refresh their view of global state.

    Unfortunately it proved difficult to implement snapshots without encountering
    various interwoven deadlock conditions under load testing. We have therefore
    moved to a lock-free design for the pool, which works as follows:

    Core Design Principles

    * Immutable planning strings (these remain from the previous design)
    * Lock-Free runtime additions using only atomic operations
    * Stable ID ranges that maintain our ID allocation strategy
    * Snapshot-Free coordination using atomic counters
    * Incremental (mostly lock-free) promotion of hot strings
    * Emergency allocation pathways for extreme contention scenarios

*)


module StringInterning =

    open RMLPipeline
    open RMLPipeline.Core
    open RMLPipeline.FastMap.Types
    open System
    open System.Diagnostics
    open System.Runtime.CompilerServices
    open System.Threading

    type PoolType = 
        | Global
        | Group
        | Worker
        | Segment
        with 
            static member GlobalPool = Global
            static member GroupPool = Group
            static member WorkerPool = Worker

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
        // Decay settings are only used by the polling thread that manages promotions
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
            MinPromotionInterval = TimeSpan.FromMilliseconds 100.0
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
    type PoolStats = {
        TotalStrings: int
        MemoryUsageBytes: int64
        HitRatio: float
        AccessCount: int64
        MissCount: int64
    } with 
        static member Empty = {
            TotalStrings      = 0
            MemoryUsageBytes  = 0L
            HitRatio          = 0.0
            AccessCount       = 0L
            MissCount         = 0L
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

    (* Efficient, lock-free indexing *)
    module HashIndexing = 

        [<Struct>]
        type IndexStats = {
            mutable Lookups: int64
            mutable Hits: int64
            mutable Misses: int64
            mutable Overwrites: int64
        }

        [<Struct>]
        type FilterStats = {
            TotalLookups: int64
            Hits: int64        
            Misses: int64      
            FalsePositives: int64 
            Collisions: int64  
        }
        
        // Pack/unpack helpers
        [<Struct>]
        type PackedLocation = {
            ArrayType: byte      // 8 bits
            StringIndex: int32   // 24 bits (16M strings per array)
            EntryIndex: int32    // 32 bits
        }
        
        let inline packLocation (arrayType: byte) (stringIndex: int32) (entryIndex: int32) : int64 =
            // Validate ranges
            if stringIndex > 0xFFFFFF then 
                failwith "String index too large"
            
            // Pack: [ArrayType:8][StringIndex:24][EntryIndex:32]
            (int64 arrayType <<< 56) |||
            (int64 (stringIndex &&& 0xFFFFFF) <<< 32) |||
            (int64 entryIndex)
        
        let inline unpackLocation (packed: int64) : PackedLocation =
            {
                ArrayType = byte (packed >>> 56)
                StringIndex = int32 ((packed >>> 32) &&& 0xFFFFFFL)
                EntryIndex = int32 (packed &&& 0xFFFFFFFFL)
            }
        
        (* [<Struct>]
        type HashEntry = {
            /// Upper 32 bits of hash (for fast comparison)
            HashFragment: uint32
            /// Direct location information
            Location: StringLocation
            /// Distance from ideal position (for Robin Hood hashing)
            Distance: int16
            /// For lock-free insertion - 0 means empty slot
            mutable State: int32
        } *)

        let inline fnv1aHash (str: string) : uint64 =
            let prime = 1099511628211UL
            let offset = 14695981039346656037UL
            let mutable hash = offset
            
            for i = 0 to str.Length - 1 do
                hash <- hash ^^^ uint64 (int str.[i])
                hash <- hash * prime
            
            hash

        (* 
            Index from String to PackedLocations. 
            This implementation is lock-free and provides optimistic
            concurrency (with atomic operations allowing for duplicate entries).            
        *)        
        type StringIndex = {
            Bitmap: int64[]          // Bitmap index for fast existence checks
            BitmapMask: uint64       // Mask for indexing into bitmap
                        
            PackedLocations: int64[] // Hash entries for string locations
            HashFragments: uint32[]
            ArrayMask: uint64
            
            // stats housekeeping
            mutable Stats: IndexStats
        } with
            static member Create(config: PoolConfiguration, poolType: PoolType) =
                // Calculate size based on pool type and config
                let expectedSize = 
                    match poolType with
                    | Global -> config.GlobalEdenSize * 2
                    | Group  -> config.GroupEdenSize * 2
                    | Worker -> config.WorkerEdenSize * 2
                    | Segment -> 1000
                
                // Use load factor based on performance profile
                let loadFactor = 
                    match config with
                    | cfg when cfg.GlobalEdenSize >= 50000 -> 0.25  // High performance
                    | cfg when cfg.GlobalEdenSize <= 10000 -> 0.75  // Low memory
                    | _ -> 0.5  // Default
                
                let arraySize = 
                    let size = int (float expectedSize / loadFactor)
                    // Round to power of 2
                    let rec nextPow2 n = 
                        if n &&& (n - 1) = 0 then n else nextPow2 (n + 1)
                    nextPow2 (max 256 size)
                
                // Bitmap size (1 bit per potential string)
                let bitmapSize = max 64 (arraySize / 64)
                {
                    Bitmap = Array.zeroCreate bitmapSize
                    BitmapMask = uint64 (bitmapSize * 64 - 1)
                    PackedLocations = Array.zeroCreate arraySize  // All zeros = invalid
                    HashFragments = Array.zeroCreate arraySize
                    ArrayMask = uint64 (arraySize - 1)
                    Stats = { Lookups = 0L; Hits = 0L; Misses = 0L; Overwrites = 0L }
                }

            (* Set and get the bit used for indexing in index *)
            
            [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
            member inline private this.SetIndexBit(hash: uint64) =
                let bitIndex = int (hash &&& this.BitmapMask)
                let wordIndex = bitIndex >>> 6
                let bitMask = 1L <<< (bitIndex &&& 63)
                Interlocked.Or(&this.Bitmap.[wordIndex], bitMask) |> ignore
            
            [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
            member inline private this.CheckIndexBit(hash: uint64) =
                let bitIndex = int (hash &&& this.BitmapMask)
                let wordIndex = bitIndex >>> 6
                let bitMask = 1L <<< (bitIndex &&& 63)
                (Volatile.Read(&this.Bitmap.[wordIndex]) &&& bitMask) <> 0L
            
            member this.TryGetLocation(str: string) : PackedLocation voption =
                Interlocked.Increment(&this.Stats.Lookups) |> ignore
                
                let hash = fnv1aHash str
                
                if not (this.CheckIndexBit hash) then
                    Interlocked.Increment(&this.Stats.Misses) |> ignore
                    ValueNone
                else
                    let index = int (hash &&& this.ArrayMask)
                    let hashFragment = uint32 (hash >>> 32)
                    
                    let storedFragment = Volatile.Read(&this.HashFragments.[index])
                    if storedFragment = hashFragment then
                        let packed = Volatile.Read(&this.PackedLocations.[index])
                        if packed <> 0L then  // 0 = invalid/empty
                            Interlocked.Increment(&this.Stats.Hits) |> ignore
                            ValueSome (unpackLocation packed)
                        else
                            Interlocked.Increment(&this.Stats.Misses) |> ignore
                            ValueNone
                    else
                        Interlocked.Increment(&this.Stats.Misses) |> ignore
                        ValueNone
            
            member this.AddLocation(
                            str: string, 
                            arrayType: byte, 
                            stringIndex: int32, 
                            entryIndex: int32) =
                let hash = fnv1aHash str
                
                this.SetIndexBit hash
                
                let index = int (hash &&& this.ArrayMask)
                let hashFragment = uint32 (hash >>> 32)
                
                // Check for overwrites
                let oldFragment = Volatile.Read &this.HashFragments.[index]
                if oldFragment <> 0u && oldFragment <> hashFragment then
                    Interlocked.Increment(&this.Stats.Overwrites) |> ignore
                
                // Pack location
                let packed = packLocation arrayType stringIndex entryIndex
                
                // Write atomically
                Volatile.Write(&this.HashFragments.[index], hashFragment)
                Thread.MemoryBarrier()
                Volatile.Write(&this.PackedLocations.[index], packed)
            
            member this.GetStats() =
                let lookups    = Volatile.Read &this.Stats.Lookups
                let hits       = Volatile.Read &this.Stats.Hits
                let overwrites = Volatile.Read &this.Stats.Overwrites

                let hitRate = if lookups > 0L then float hits / float lookups else 0.0
                let overwriteRate = if hits > 0L then float overwrites / float hits else 0.0
                
                hitRate, overwriteRate
        
    (* Promotion Across Tiers *)
    
    module Promotions = 

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
            OriginalId: StringId
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
                    printfn "Skipping dequeue - not enough time since last processing"
                    [||]  // Too soon
                else
                    // Try to claim the promotion window
                    if Interlocked.CompareExchange(&this.LastProcessedTime, now, lastTime) <> lastTime then
                        printfn "Skipping dequeue - another thread is processing"
                        [||]  // Another thread is processing
                    else
                        // We have exclusive access to dequeue
                        let mutable head = Volatile.Read(&this.Head)
                        let tail = Volatile.Read(&this.Tail)
                        let available = int (tail - head)
                        let toDequeue = min available maxCount
                        printfn "Dequeueing %d items" toDequeue
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
                    printfn "Not rotating epochs yet"
                    false
            
            // Process promotions - called periodically by promotion processing thread
            // NOTE: DO NOT CALL this method from any thread that is consuming the pool!
            member this.ProcessPromotions(resolver: StringId -> string option) : PromotionCandidate[] =
                // First check if we should rotate
                if this.CheckRotation() then
                    printfn "Rotating epochs"
                    Interlocked.Increment(&this.CurrentWriteEpoch) |> ignore
                
                // Only one thread should process at a time
                if Monitor.TryEnter this.ProcessingLock then
                    try
                        // Process the oldest epoch (2 rotations ago)
                        let currentEpoch = Volatile.Read(&this.CurrentWriteEpoch)
                        let processEpoch = (currentEpoch + 2) % 3 // +2 because we want 2 epochs ago
                        let queue = this.EpochQueues.[processEpoch]
                        
                        // Drain the entire queue
                        let mutable candidates = ResizeArray<PromotionCandidate>()
                        let mutable batch = queue.TryDequeueBatch 1000
                        
                        while batch.Length > 0 do
                            for signal in batch do
                                match resolver signal.StringId with
                                | Some str ->
                                    candidates.Add 
                                        { 
                                            Value = str
                                            Temperature = signal.Temperature 
                                            OriginalId = signal.StringId
                                        }
                                | None -> ()
                            
                            batch <- queue.TryDequeueBatch 1000
                        
                        // Deduplicate candidates
                        candidates
                        |> Seq.distinctBy (fun c -> c.Value)
                        |> Seq.toArray
                    finally
                        Monitor.Exit this.ProcessingLock
                else
                    printfn "Skipping promotion processing - already in progress"
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
                    |> Option.map (fun str -> 
                        { Value = str
                          Temperature = signal.Temperature 
                          OriginalId = signal.StringId
                        }))
    
    open Promotions

    module Packing = 

        open HashIndexing
        open System.Collections.Concurrent
        
        (*
            Packed Array that allows us to track promotions across tiers.

            Since the promotion logic allows for eventual consistency,
            we suffer one additional atomic read read to check the entry type.       
        *)
        type PackedPoolArray = {
            // Each entry is either:
            // - High bit 0: Lower 63 bits are string array index
            // - High bit 1: Lower 63 bits are promoted StringId value (base ID only)
            PackedEntries: int64[]
            StringsArray: string[]
            mutable NextStringIndex: int32
        }

        [<RequireQualifiedAccess>]
        module PackedOps =
            // For non-promoted entries, we need to store temperature
            // Since temperature is capped at 16 bits, we can pack:
            // Bit 63: 0 (not promoted)
            // Bits 62-47: Temperature (16 bits)  
            // Bits 46-0: String index (47 bits - but stringIndex is int32, so max 31 bits used)
            
            let inline packStringIndexWithTemp (stringIndex: int32) (temperature: int32) =
                if temperature > 0xFFFF then 
                    failwith "Temperature exceeds 16-bit limit"
                if stringIndex < 0 then
                    failwith "String index cannot be negative"
                
                // Pack: [0][Temperature:16][StringIndex:47]
                // Since stringIndex is int32, it will always fit in 47 bits
                (int64 temperature <<< 47) ||| int64 stringIndex
            
            let inline packStringIndex (stringIndex: int32) =
                packStringIndexWithTemp stringIndex 0

            let inline unpackStringIndex (packed: int64) =
                int32 (packed &&& 0x7FFFFFFFFFFFL)

            let inline unpackStringIndexAndTemp (packed: int64) =
                let stringIndex = int32 (packed &&& 0x7FFFFFFFFFFFL)  // Lower 47 bits (fits in int32)
                let temperature = int32 ((packed >>> 47) &&& 0xFFFFL) // Next 16 bits
                stringIndex, temperature
            
            // Existing promoted functions stay the same
            let inline isPromoted (packed: int64) = packed < 0L
            
            let inline packPromotedId (id: StringId) = 
                // High bit 1, lower 63 bits are base ID (no temperature needed)
                0x8000000000000000L ||| int64 id.Value
            
            let inline unpackPromotedId (packed: int64) =
                StringId.Create(int32 (packed &&& 0x7FFFFFFFFFFFFFFFL))

        (* 
            Thread-safe chunked Array
        *)
        type Segments = {
            Chunks: ResizeArray<PackedPoolArray>
            ChunkSize: int
            mutable CurrentChunkIndex: int32
            mutable ChunkOffsets: int32[]  // One counter per chunk
            ChunksLock: ReaderWriterLockSlim
            
            // Emergency fallback for high contention scenarios
            EmergencyCache: ConcurrentDictionary<int, string> 
            mutable EmergencyCounter: int32
            EmergencyBaseId: int
            
            Configuration: PoolConfiguration
            
            // Temperature tracking with targeted approach
            mutable LastPromotionTime: int64
            
            PromotionTracker: EpochPromotionTracker
            mutable PromotionThreshold: int32
            mutable StringCount: int32  // Track for adaptive threshold

            StringIndex: StringIndex
        } with
            static member Create(baseId: int, config: PoolConfiguration) =
                let rotationInterval = 
                    // Rotate faster than promotion interval to ensure smooth processing
                    TimeSpan.FromMilliseconds(config.MinPromotionInterval.TotalMilliseconds / 3.0)
                
                let firstChunk = {
                    PackedEntries = Array.zeroCreate config.InitialChunkSize
                    StringsArray = Array.zeroCreate config.InitialChunkSize
                    NextStringIndex = 0
                }
                
                {
                    Chunks = ResizeArray<PackedPoolArray> [| firstChunk |]
                    ChunkSize = config.InitialChunkSize
                    CurrentChunkIndex = 0
                    ChunkOffsets = Array.zeroCreate 10 // Initial size, will grow as needed
                    ChunksLock = new ReaderWriterLockSlim()
                    EmergencyCache = ConcurrentDictionary<int, string>()
                    EmergencyCounter = 0
                    EmergencyBaseId = baseId + 900000  // Reserve high range for emergency
                    Configuration = config
                    LastPromotionTime = 0L
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
                    StringIndex = StringIndex.Create(config, Segment)
                }
            member inline private this.AllocateNewString(str: string, baseId: int) : StringId =
                let currentChunkIdx = Volatile.Read(&this.CurrentChunkIndex)
                let offset = Interlocked.Increment(&this.ChunkOffsets.[currentChunkIdx]) - 1
                
                if offset < this.ChunkSize then
                    let chunk = this.Chunks.[currentChunkIdx]
                    let stringIdx = Interlocked.Increment(&chunk.NextStringIndex) - 1
                    
                    // Store string
                    Volatile.Write(&chunk.StringsArray.[stringIdx], str)
                    
                    // Pack string index with temperature 0
                    let packed = PackedOps.packStringIndexWithTemp stringIdx 0
                    Volatile.Write(&chunk.PackedEntries.[offset], packed)
                    
                    // Add to index
                    this.StringIndex.AddLocation(str, byte (currentChunkIdx + 1), stringIdx, offset)
                    
                    let id = StringId.Create(baseId + (currentChunkIdx * this.ChunkSize) + offset)
                    Interlocked.Increment(&this.StringCount) |> ignore
                    id
                else
                    this.AllocateInNewChunk(str, baseId)
            
            member this.AllocateString(str: string, baseId: int) : StringId =
                match this.StringIndex.TryGetLocation str with
                | ValueSome location when location.ArrayType > 0uy ->
                    // Potential match - verify it's in our segments
                    if int location.ArrayType > this.Chunks.Count || location.EntryIndex < 0 then
                            this.AllocateNewString(str, baseId)
                    else
                        let segmentIdx = int location.ArrayType - 1
                        let chunk = this.Chunks.[segmentIdx]
                        
                        if location.EntryIndex >= chunk.PackedEntries.Length ||
                           location.StringIndex < 0 ||
                           location.StringIndex >= chunk.NextStringIndex then
                            this.AllocateNewString(str, baseId)
                        else
                            let packed = Volatile.Read(&chunk.PackedEntries.[location.EntryIndex])    
                            if PackedOps.isPromoted packed then
                                // String was promoted - return the promoted ID
                                PackedOps.unpackPromotedId packed
                            else
                                // Unpack string index and current temperature
                                let stringIdx, currentTemp = PackedOps.unpackStringIndexAndTemp packed
                                
                                // Verify string matches
                                let storedStr = Volatile.Read(&chunk.StringsArray.[stringIdx])
                                if storedStr <> str then
                                    this.AllocateNewString(str, baseId)
                                else
                                    // Found! Create ID with current temperature
                                    let baseId = baseId + (segmentIdx * this.ChunkSize) + location.EntryIndex
                                    let id = StringId.CreateWithTemperature(baseId, currentTemp)
                                    let newId = id.IncrementTemperature()
                                    
                                    // Update packed entry with new temperature
                                    let newPacked = PackedOps.packStringIndexWithTemp stringIdx newId.Temperature
                                    Volatile.Write(&chunk.PackedEntries.[location.EntryIndex], newPacked)
                                    
                                    // Check threshold
                                    if newId.Temperature = this.PromotionThreshold then
                                        this.PromotionTracker.SignalPromotion(newId, newId.Temperature) |> ignore
                                    
                                    newId
                | _ ->
                    this.AllocateNewString(str, baseId)

            member this.ProcessPromotions(baseId: int) : PromotionCandidate[] =
                // Create resolver function that looks up strings by ID
                let resolver (id: StringId) =
                    this.TryGetString(id.Value, baseId)

                this.PromotionTracker.ProcessPromotions resolver

            member inline private this.growChunksArray nextIdx =
                // Calculate next array size (can be larger than ChunkSize)
                let nextArraySize = 
                    if this.Chunks.Count = 1 then
                        // For second chunk, use secondary size or ChunkSize, whichever is larger
                        max this.Configuration.SecondaryChunkSize this.ChunkSize
                    else
                        // For subsequent chunks, grow the array size
                        let lastChunk = this.Chunks.[this.Chunks.Count - 1]
                        int (float lastChunk.PackedEntries.Length * this.Configuration.ChunkGrowthFactor)
                        |> min 50000  // Cap array size at 50K
                
                // Create new PackedPoolArray chunk
                let newChunk = {
                    PackedEntries = Array.zeroCreate nextArraySize
                    StringsArray = Array.zeroCreate nextArraySize
                    NextStringIndex = 0
                }
                
                // Add the new chunk
                this.Chunks.Add(newChunk)
                
                // Ensure offset array has space
                if nextIdx >= this.ChunkOffsets.Length then
                    let newSize = this.ChunkOffsets.Length * 2
                    let newOffsets = Array.zeroCreate newSize
                    Array.Copy(this.ChunkOffsets, newOffsets, this.ChunkOffsets.Length)
                    this.ChunkOffsets <- newOffsets

            member inline private this.allocateInNextChunk baseId str =
                // Move to next chunk
                let newChunkIdx = Interlocked.Increment &this.CurrentChunkIndex
                let newOffset = Interlocked.Increment(&this.ChunkOffsets.[newChunkIdx]) - 1
                
                // Get the chunk
                let chunk = this.Chunks.[newChunkIdx]
                
                // Allocate string in the strings array
                let stringIdx = Interlocked.Increment(&chunk.NextStringIndex) - 1
                Volatile.Write(&chunk.StringsArray.[stringIdx], str)
                
                // Pack the string index into the entries array
                let packed = PackedOps.packStringIndex stringIdx
                Volatile.Write(&chunk.PackedEntries.[newOffset], packed)
                
                // Add to the index for retrieval
                this.StringIndex.AddLocation(str, byte (newChunkIdx + 1), stringIdx, newOffset)

                // Create StringId
                let id = StringId.Create(baseId + (newChunkIdx * this.ChunkSize) + newOffset)
                
                // Increment string count
                Interlocked.Increment &this.StringCount |> ignore
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

            // TODO: Re-introduce the adaptive threshold when calculating promotions
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
                
                Interlocked.Increment &this.StringCount |> ignore
                StringId.Create(this.EmergencyBaseId + emergencyId)
            
            member this.TryGetString(id: int, baseId: int) : string option =
                // Check if it's an emergency ID
                if id >= this.EmergencyBaseId then
                    let emergencyIdx = id - this.EmergencyBaseId
                    match this.EmergencyCache.TryGetValue emergencyIdx with
                    | true, str -> Some str
                    | false, _ -> None
                else
                    // Regular chunk lookup with fixed ChunkSize
                    let relativeId = id - baseId
                    let chunkIdx = relativeId / this.ChunkSize
                    let offset = relativeId % this.ChunkSize
                    
                    if chunkIdx >= 0 && chunkIdx < this.Chunks.Count then
                        let chunk = this.Chunks.[chunkIdx]
                        if offset >= 0 && offset < chunk.PackedEntries.Length then
                            let packed = Volatile.Read(&chunk.PackedEntries.[offset])
                            
                            if PackedOps.isPromoted packed then
                                // String was promoted - return None to trigger lookup in higher tier
                                None
                            else
                                // Extract string index (ignore temperature for lookup)
                                let stringIdx, _ = PackedOps.unpackStringIndexAndTemp packed
                                if stringIdx >= 0 && stringIdx < chunk.StringsArray.Length then
                                    let str = Volatile.Read(&chunk.StringsArray.[stringIdx])
                                    if not (isNull str) then Some str else None
                                else 
                                    None
                        else 
                            None
                    else 
                        None

            member this.GetStringWithTemperature(id: StringId, baseId: int) : (string option * StringId) =
                let relativeId = id.Value - baseId
                let chunkIdx = relativeId / this.ChunkSize
                let offset = relativeId % this.ChunkSize
                
                if chunkIdx >= 0 && chunkIdx < this.Chunks.Count then
                    let chunk = this.Chunks.[chunkIdx]
                    if offset >= 0 && offset < chunk.PackedEntries.Length then
                        let packed = Volatile.Read(&chunk.PackedEntries.[offset])
                        
                        if PackedOps.isPromoted packed then
                            None, id
                        else
                            let stringIdx, currentTemp = PackedOps.unpackStringIndexAndTemp packed
                            if stringIdx >= 0 && stringIdx < chunk.StringsArray.Length then
                                let str = Volatile.Read(&chunk.StringsArray.[stringIdx])
                                if not (isNull str) then
                                    // Create new ID with incremented temperature
                                    let newTemp = min (currentTemp + 1) StringId.MaxTemperature
                                    let newId = StringId.CreateWithTemperature(id.Value, newTemp)
                                    
                                    // Update packed entry
                                    let newPacked = PackedOps.packStringIndexWithTemp stringIdx newTemp
                                    Volatile.Write(&chunk.PackedEntries.[offset], newPacked)
                                    
                                    // Check threshold
                                    if newTemp = this.PromotionThreshold then
                                        this.PromotionTracker.SignalPromotion(newId, newTemp) |> ignore
                                    
                                    Some str, newId
                                else
                                    None, id
                            else
                                None, id
                    else
                        None, id
                else
                    match this.TryGetString(id.Value, baseId) with
                    | Some str -> Some str, id.IncrementTemperature()
                    | None -> None, id
            
            interface IDisposable with
                member this.Dispose() = 
                    this.ChunksLock.Dispose()
    
    open HashIndexing
    open Packing

        // Generational pool with eden space and chunked overflow
    type Pool = {
        // Eden space (pre-allocated fixed array)
        EdenArray: PackedPoolArray
        mutable EdenOffset: int32
        
        // Post-eden space (chunked for growth)
        PostEden: Segments
        
        // Base ID for this pool
        PoolBaseId: int
        
        // Configuration
        Configuration: PoolConfiguration
        
        mutable LastDecayTime: DateTime
        mutable LastPromotionTime: int64
        
        EdenPromotionTracker: EpochPromotionTracker
        
        // Stats
        mutable Stats: Stats
    } with
        static member Create(baseId: int, edenSize: int, config: PoolConfiguration) =
            let rotationInterval = 
                TimeSpan.FromMilliseconds(config.MinPromotionInterval.TotalMilliseconds / 3.0)
            {
                EdenArray = {
                    PackedEntries = Array.zeroCreate edenSize
                    StringsArray = Array.zeroCreate edenSize
                    NextStringIndex = 0
                }
                EdenOffset = 0
                PostEden = Segments.Create(baseId + edenSize, config)
                PoolBaseId = baseId
                Configuration = config
                LastDecayTime = DateTime.UtcNow
                LastPromotionTime = 0L
                EdenPromotionTracker = EpochPromotionTracker.Create(
                    queueSizeBits = 14,  // 16K signals for Eden space
                    rotationInterval = rotationInterval
                )
                Stats = { accessCount = 0L; missCount = 0L }
            }
        
        // Get current promotion threshold for eden space, adjusted for size
        (* member this.GetAdjustedEdenPromotionThreshold() =
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
            int (float baseThreshold * scalingFactor) *)
        
        // Check both eden and post-eden spaces for promotion candidates
        member this.CheckPromotion() : PromotionCandidate[] =
            // Process both Eden and PostEden promotions
            let edenResolver (id: StringId) =
                let idValue = id.Value
                let relativeId = idValue - this.PoolBaseId
                if relativeId >= 0 && relativeId < this.EdenArray.PackedEntries.Length then
                    let packed = Volatile.Read(&this.EdenArray.PackedEntries.[relativeId])
                    
                    if PackedOps.isPromoted packed then
                        None
                    else
                        // Get string index (temperature is in the StringId already)
                        let stringIdx = PackedOps.unpackStringIndex packed
                        if stringIdx >= 0 && stringIdx < this.EdenArray.StringsArray.Length then
                            let str = Volatile.Read(&this.EdenArray.StringsArray.[stringIdx])
                            if not (isNull str) then Some str else None
                        else None
                else
                    None
            
            let edenCandidates = this.EdenPromotionTracker.ProcessPromotions edenResolver
            
            let postEdenBaseId = this.PoolBaseId + this.EdenArray.PackedEntries.Length
            let postEdenCandidates = this.PostEden.ProcessPromotions postEdenBaseId
            
            Array.append edenCandidates postEdenCandidates
    
        member this.GetAdaptiveThreshold() : int32 =
            let baseThreshold = 
                if this.PoolBaseId < IdAllocation.GroupPoolBase then 
                    this.Configuration.GroupPromotionThreshold 
                else 
                    this.Configuration.WorkerPromotionThreshold
            
            // We're already reading EdenOffset during allocation
            let edenCount = Volatile.Read(&this.EdenOffset)
            let postEdenCount = Volatile.Read(&this.PostEden.StringCount)
            let totalCount = edenCount + postEdenCount
            
            let scaleFactor = 
                1.0 + 
                (float totalCount / float this.Configuration.ThresholdScalingInterval) * 
                (this.Configuration.ThresholdScalingFactor - 1.0)
            
            int32 (float baseThreshold * scaleFactor)

        (* Relies on the caller not to repeatedly intern the same string *)
        member this.InternString(str: string) : StringId =
            if isNull str || str.Length = 0 then
                raise (ArgumentException "String cannot be null or empty")
                            
            // Opportunistically check for promotion
            // TODO: avoid promotion storms
            this.CheckPromotion() |> ignore
            
            Interlocked.Increment &this.Stats.accessCount |> ignore
            
            let edenIdx = Interlocked.Increment(&this.EdenOffset) - 1
            if edenIdx < this.EdenArray.PackedEntries.Length then
                let stringIdx = Interlocked.Increment(&this.EdenArray.NextStringIndex) - 1
                
                // Store string
                Volatile.Write(&this.EdenArray.StringsArray.[stringIdx], str)
                
                // Pack the string index
                let packed = PackedOps.packStringIndex stringIdx
                Volatile.Write(&this.EdenArray.PackedEntries.[edenIdx], packed)
                
                let id = StringId.Create(this.PoolBaseId + edenIdx)
                Interlocked.Increment &this.Stats.missCount |> ignore
                id
            else
                // Eden full, use post-eden segments
                this.PostEden.AllocateString(str, this.PoolBaseId + this.EdenArray.PackedEntries.Length)
        
        member this.TryGetString(id: StringId) : string option =
            let idValue = id.Value
            if idValue < this.PoolBaseId then
                None
            else
                let relativeId = idValue - this.PoolBaseId
                if relativeId < this.EdenArray.PackedEntries.Length then
                    let packed = Volatile.Read(&this.EdenArray.PackedEntries.[relativeId])
                    
                    if PackedOps.isPromoted packed then
                        // String was promoted - caller should check higher tier
                        None
                    else
                        let stringIdx = PackedOps.unpackStringIndex packed
                        if stringIdx >= 0 && stringIdx < this.EdenArray.StringsArray.Length then
                            let str = Volatile.Read(&this.EdenArray.StringsArray.[stringIdx])
                            if not (isNull str) then Some str else None
                        else None
                else
                    // In post-eden segments
                    this.PostEden.TryGetString(idValue, this.PoolBaseId + this.EdenArray.PackedEntries.Length)
        
        member this.GetStringWithTemperature(id: StringId) : (string option * StringId) =
            let idValue = id.Value
            if idValue < this.PoolBaseId then
                None, id
            else
                let relativeId = idValue - this.PoolBaseId
                if relativeId < this.EdenArray.PackedEntries.Length then
                    let packed = Volatile.Read(&this.EdenArray.PackedEntries.[relativeId])
                    // Check if the string is still in eden space
                    if PackedOps.isPromoted packed then
                        // String was promoted - caller should check higher tier
                        None, id
                    else
                        let stringIdx, currentTemp = PackedOps.unpackStringIndexAndTemp packed
                        if stringIdx >= 0 && stringIdx < this.EdenArray.StringsArray.Length then
                            let str = Volatile.Read(&this.EdenArray.StringsArray.[stringIdx])
                            if not (isNull str) then 
                                // Increment temperature
                                let newTemp = min (currentTemp + 1) StringId.MaxTemperature
                                let newId = StringId.CreateWithTemperature(idValue, newTemp)
                                
                                // Update packed entry with new temperature
                                let newPacked = PackedOps.packStringIndexWithTemp stringIdx newTemp
                                Volatile.Write(&this.EdenArray.PackedEntries.[relativeId], newPacked)
                                
                                // Check threshold
                                let threshold = 
                                    if this.PoolBaseId < IdAllocation.GroupPoolBase then 
                                        this.Configuration.GroupPromotionThreshold 
                                    else 
                                        this.Configuration.WorkerPromotionThreshold
                                
                                if newTemp = threshold then
                                    this.EdenPromotionTracker.SignalPromotion(newId, newTemp) |> ignore
                                
                                Some str, newId
                            else 
                                None, id
                        else None, id
                else
                    // In post-eden segments
                    this.PostEden.GetStringWithTemperature(id, 
                            this.PoolBaseId + this.EdenArray.PackedEntries.Length)
        
        member this.GetStats() : PoolStats =
            let edenMemory = 
                this.EdenArray.StringsArray
                |> Array.take (min this.EdenOffset this.EdenArray.StringsArray.Length)
                |> Array.sumBy (fun s -> if isNull s then 0L else int64 (s.Length * 2))
            
            let segmentMemory = 
                this.PostEden.Chunks
                |> Seq.sumBy (fun chunk ->
                    chunk.StringsArray 
                    |> Array.sumBy (fun s -> if isNull s then 0L else int64 (s.Length * 2)))
            
            let accessCount = Interlocked.Read(&this.Stats.accessCount)
            let missCount = Interlocked.Read(&this.Stats.missCount)
            let hitRatio = 
                if accessCount > 0L then
                    float (accessCount - missCount) / float accessCount
                else 0.0
            
            {
                TotalStrings = this.EdenOffset + this.PostEden.CurrentChunkIndex * this.PostEden.ChunkSize +
                                this.PostEden.ChunkOffsets.[this.PostEden.CurrentChunkIndex]
                MemoryUsageBytes = edenMemory + segmentMemory
                HitRatio = hitRatio
                AccessCount = accessCount
                MissCount = missCount
            }
