namespace RMLPipeline.Internal

(*
Provides a hierarchical implementation of string interning, designed to meet 
the needs of a parallel processing architecture, while providing efficient 
memory usage and performance characteristics.

This is a specialized, bounded, reference-counted string management approach 
that is more efficient for RML processing patterns than relying solely on .NET's 
general-purpose, global string interning mechanism.

Going beyond simple tiered caching, the system uses hierarchical fallback 
with strategic placement.

Routing is dynamic, based on the requested StringAccessPattern, providing
automatic fallback between tiers, with preferential checking, where high-frequency 
requests try local pools first, whilst planning requests route to global pool.

When a string is interned at a higher tier (Group or Global), it gets a StringId 
from that tier's range. If wokers require frequent access, the string doesn't move, 
but rather the worker simply caches the lookup locally.

We leverage fixed ranges of `StringId`, which allows us to avoiding complex 
coordination logic and provides a memory optimization strategy based on the
following principles:

- StringId tells you where the string lives - no searching through tiers required
- No string duplication - each string exists in exactly one tier
- Cache-friendly lookups - callers perform direct array access in the owning tier

> scope.InternString("template", HighFrequency)    // → Local pool (fast)
> scope.InternString("template", Planning)         // → Global pool (shared)
> scope.InternString("template", LowFrequency)     // → Group pool (balanced)

All strings produced during the planning phase are pre-allocated and immutable.
The RAII pattern is provided using the PoolContextScope.

Goals:
- Lock-free reads from global planning strings
- Fine-grained locking only where necessary
- Value types for IDs and patterns
- Aggressive inlining where beneficial
- No thread affinity
- Can be used with both F# async and Hopac (for parallel processing)

Avoiding coordination overheads & memory optimization.

- StringId tells you where the string lives - no searching through tiers
- No string duplication - each string exists in exactly one tier
- Cache-friendly lookups - direct array access in the owning tier

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

Thread Safety and Deadlock Avoidance:

Earlier versions of the pool utilised snapshots to establish a consistent 
view of global pool size, such that group pools would know which Ids are
already taken gloablly, whether to check the global pool for a string, and
when to refresh their view of global state.

Unfortunately it proved difficult to implement snapshots without encountering
various interwoken deadlock conditions under load testing. We have therefore
moved to a lock-free design for the pool, which works as follows:

Core Design Principles

* Immutable planning strings (these remain from the previous design)
* Lock-Free runtime additions using only atomic operations
* Stable ID ranges that maintain our ID allocation strategy
* Snapshot-Free coordination using atomic counters

This approach maintains the performance of planning string lookups, actually
improves the performance of high frequency interning operations (by
reducing lock contention), speeds up cross-tier lookups, and reduces the
convoy effect associated with high contention on the global pool.

*)


module StringInterning =

   open RMLPipeline
   open RMLPipeline.FastMap.Types
   open System
   open System.Collections.Concurrent
   open System.Collections.Generic
   open System.Threading
   open System.Runtime.CompilerServices
   open System.Runtime.InteropServices

   [<Struct>]
   type StringId = StringId of int32
       with 
       member inline this.Value = let (StringId id) = this in id
       static member inline op_Explicit(id: StringId) = id.Value
       static member Invalid = StringId -1
       member inline this.IsValid = this.Value >= 0

   [<Struct>]
   type WorkerId = WorkerId of Guid
       with
       member inline this.Value = let (WorkerId id) = this in id
       static member Create() = WorkerId(Guid.NewGuid())

   [<Struct>]
   type DependencyGroupId = DependencyGroupId of int32
       with
       member inline this.Value = let (DependencyGroupId id) = this in id

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
   } with 
       static member Empty = {
           TotalStrings      = 0
           MemoryUsageBytes  = 0L
           HitRatio          = 0.0
           AccessCount       = 0L
           MissCount         = 0L
       }

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
       }

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

   [<Struct>]
   type Stats = {
       mutable accessCount: int64
       mutable missCount: int64
   }

   // Segmented array for dynamic growth with minimal locking
   type Segments = {
       Chunks: ResizeArray<string[]>
       ChunkSize: int
       mutable CurrentChunkIndex: int32
       ChunkOffsets: int32[]  // One counter per chunk
       mutable ChunkTemperatures: int32[]  // Temperature tracking per chunk
       ChunksLock: ReaderWriterLockSlim
       EmergencyCache: ConcurrentDictionary<int, string>  // Fallback for timeout cases
       mutable EmergencyCounter: int32
       EmergencyBaseId: int
       Configuration: PoolConfiguration
   } with
       static member Create(baseId: int, config: PoolConfiguration) =
           let initialChunks = 10
           {
               Chunks = ResizeArray<string[]>([| Array.zeroCreate config.InitialChunkSize |])
               ChunkSize = config.InitialChunkSize
               CurrentChunkIndex = 0
               ChunkOffsets = Array.zeroCreate initialChunks
               ChunkTemperatures = Array.zeroCreate initialChunks
               ChunksLock = new ReaderWriterLockSlim()
               EmergencyCache = ConcurrentDictionary<int, string>()
               EmergencyCounter = 0
               EmergencyBaseId = baseId + 900000  // Reserve high range for emergency
               Configuration = config
           }
       
       member this.AllocateString(str: string, baseId: int) : StringId =
           // Try to use current chunk first - completely lock-free
           let currentChunkIdx = Volatile.Read(&this.CurrentChunkIndex)
           let offset = Interlocked.Increment(&this.ChunkOffsets.[currentChunkIdx]) - 1
           
           if offset < this.ChunkSize then
               // Common case - chunk has space, no locks needed
               let id = StringId(baseId + (currentChunkIdx * this.ChunkSize) + offset)
               Volatile.Write(&this.Chunks.[currentChunkIdx].[offset], str)
               // Update temperature with atomic increment for safety
               Interlocked.Increment(&this.ChunkTemperatures.[currentChunkIdx]) |> ignore
               id
           else
               // Rare case - need a new chunk
               this.AllocateInNewChunk(str, baseId)
       
       member private this.AllocateInNewChunk(str: string, baseId: int) : StringId =
           let mutable readLockHeld = false
           let mutable writeLockHeld = false
           
           try
               // First attempt upgradeable read lock with timeout
               readLockHeld <- this.ChunksLock.TryEnterUpgradeableReadLock(50)
               if not readLockHeld then
                   // Timeout - use emergency allocation
                   return this.EmergencyAllocate(str)
               
               // Under read lock, check if another thread already added a chunk
               let currentIdx = Volatile.Read(&this.CurrentChunkIndex)
               let nextIdx = currentIdx + 1
               
               // Check if we need a new chunk
               let needNewChunk = nextIdx >= this.Chunks.Count
               
               if needNewChunk then
                   // Check max chunks limit
                   match this.Configuration.MaxChunks with
                   | Some max when this.Chunks.Count >= max ->
                       // Hit max chunks - use emergency allocation
                       return this.EmergencyAllocate(str)
                   | _ -> ()
                   
                   // Only acquire write lock if absolutely needed
                   writeLockHeld <- this.ChunksLock.TryEnterWriteLock(50)
                   if not writeLockHeld then
                       // Write lock timeout - use emergency allocation
                       return this.EmergencyAllocate(str)
                   
                   // Double-check under write lock
                   if nextIdx >= this.Chunks.Count then
                       // Calculate next chunk size (with growth factor)
                       let nextChunkSize = 
                           if this.Chunks.Count = 1 then
                               this.Configuration.SecondaryChunkSize
                           else
                               int (float this.ChunkSize * this.Configuration.ChunkGrowthFactor)
                               |> min 50000  // Cap chunk size at 50K
                       
                       // Actually grow the chunks array
                       this.Chunks.Add(Array.zeroCreate nextChunkSize)
                       
                       // Ensure offset and temperature arrays have space
                       if nextIdx >= this.ChunkOffsets.Length then
                           let newSize = this.ChunkOffsets.Length * 2
                           let newOffsets = Array.zeroCreate newSize
                           let newTemps = Array.zeroCreate newSize
                           Array.Copy(this.ChunkOffsets, newOffsets, this.ChunkOffsets.Length)
                           Array.Copy(this.ChunkTemperatures, newTemps, this.ChunkTemperatures.Length)
                           this.ChunkOffsets <- newOffsets
                           this.ChunkTemperatures <- newTemps
               
               // Now safe to move to next chunk
               let newChunkIdx = Interlocked.Increment(&this.CurrentChunkIndex)
               let newOffset = Interlocked.Increment(&this.ChunkOffsets.[newChunkIdx]) - 1
               
               // Write string to new location
               let id = StringId(baseId + (newChunkIdx * this.ChunkSize) + newOffset)
               Volatile.Write(&this.Chunks.[newChunkIdx].[newOffset], str)
               Interlocked.Increment(&this.ChunkTemperatures.[newChunkIdx]) |> ignore
               id
               
           finally
               // Always release locks in correct order
               if writeLockHeld then this.ChunksLock.ExitWriteLock()
               if readLockHeld then this.ChunksLock.ExitUpgradeableReadLock()
       
       member private this.EmergencyAllocate(str: string) : StringId =
           // Guaranteed to succeed even under extreme contention
           let emergencyId = Interlocked.Increment(&this.EmergencyCounter) - 1
           this.EmergencyCache.TryAdd(emergencyId, str) |> ignore
           StringId(this.EmergencyBaseId + emergencyId)
       
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
       
       // Temperature tracking
       mutable EdenTemperature: int32
       
       // Stats
       mutable Stats: Stats
   } with
       static member Create(baseId: int, edenSize: int, config: PoolConfiguration) =
           {
               EdenArray = Array.zeroCreate edenSize
               EdenOffset = 0
               PostEden = Segments.Create(baseId + edenSize, config)
               PoolBaseId = baseId
               Configuration = config
               EdenTemperature = 0
               Stats = { accessCount = 0L; missCount = 0L }
           }
       
       member this.InternString(str: string) : StringId =
           if isNull str || str.Length = 0 then
               raise (ArgumentException "String cannot be null or empty")
           
           Interlocked.Increment(&this.Stats.accessCount) |> ignore
           
           // Try eden space first
           let edenIdx = Interlocked.Increment(&this.EdenOffset) - 1
           if edenIdx < this.EdenArray.Length then
               // Eden has space
               let id = StringId(this.PoolBaseId + edenIdx)
               Volatile.Write(&this.EdenArray.[edenIdx], str)
               Interlocked.Increment(&this.EdenTemperature) |> ignore
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
           
           {
               TotalStrings = this.EdenOffset + this.PostEden.EmergencyCounter
               MemoryUsageBytes = edenMemory + segmentMemory
               HitRatio = hitRatio
               AccessCount = accessCount
               MissCount = missCount
           }

   type GlobalPool = private {
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
               // Runtime string
               this.RuntimePool.TryGetString(id)
           else
               // Not a global ID!
               None
       
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
           
           Interlocked.Increment(&this.Stats.accessCount) |> ignore
           
           // Check global pool first (it might be a planning string)
           let globalResult = this.GlobalPool.TryGetStringId str
           match globalResult with
           | Some id -> id
           | None ->
               // Use group pool
               Interlocked.Increment(&this.Stats.missCount) |> ignore
               this.GroupPool.InternString(str)
       
       member this.GetString(id: StringId) : string option =
           // Check if it's in our group's range
           if id.Value >= this.GroupPoolBaseId && 
               id.Value < this.GroupPoolBaseId + IdAllocation.GroupPoolRangeSize then
               // It's a group ID
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
                   
                   // Cache the result if found - we don't care about misses at this stage
                   match result with
                   | Some str -> 
                       this.CrossTierCache.TryAdd(id, str) |> ignore
                       Some str
                   | None -> None

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
       mutable NextLocalId: int  // TODO: we should shift to int64 for atomic ops
       LocalPoolBaseId: int  // Pre-calculated base ID for this worker
       MaxSize: int
       
       // LRU eviction for bounded size
       AccessOrder: LinkedList<string>  // Tracks access order by string
       mutable StringToNode: FastMap<string, LinkedListNode<string>>  // O(1) node lookup

       // Stats recording
       mutable Stats: Stats
   } with
       
       member this.InternString(str: string, pattern: StringAccessPattern) : StringId =
           if isNull str || str.Length = 0 then
               raise (ArgumentException "String cannot be null or empty")

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
           in
           { 
               TotalStrings     = nextLocalId + 1
               MemoryUsageBytes = localMemory
               HitRatio         = hitRatio
               AccessCount      = access
               MissCount        = miss
           }

       (* NB: LRU OPS are only called by one thread at a time *)
       
       member private this.AddLocal(str: string) : StringId =
           let currentId = this.NextLocalId
           let newId = StringId (this.LocalPoolBaseId + this.NextLocalId)
           
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
   
       // Create local pool for a worker
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

   and StringPoolHierarchy = private {
       GlobalPool: GlobalPool
       WorkerPoolBuilder: WorkerId -> GroupPool option -> LocalPool
       GroupPoolBuilder: DependencyGroupId -> GroupPool
       WorkerPools: ConcurrentDictionary<WorkerId, LocalPool>
       GroupPools: ConcurrentDictionary<DependencyGroupId, GroupPool>
       Configuration: PoolConfiguration
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
               }) PoolStats.Empty
           
           let totalAccessCount = globalStats.AccessCount + groupStats.AccessCount
           let totalMissCount = globalStats.MissCount + groupStats.MissCount
           
           let overallHitRatio = 
               if totalAccessCount > 0L then 
                   float (totalAccessCount - totalMissCount) / float totalAccessCount
               else 0.0
           in
           {
               TotalStrings = globalStats.TotalStrings + groupStats.TotalStrings
               MemoryUsageBytes = globalStats.MemoryUsageBytes + groupStats.MemoryUsageBytes
               HitRatio = overallHitRatio
               AccessCount = totalAccessCount
               MissCount = totalMissCount
           }
       
       // Promotion mechanism for hot chunks
       member this.PromoteHotChunks() =
           // Promote from worker pools to group pools
           for kvp in this.WorkerPools do
               let workerPool = kvp.Value
               match workerPool.GroupPool with
               | Some groupPool ->
                   // Check if any chunks are hot enough for promotion
                   // Note: Workers don't have segments, but we could track temperature
                   // This is a placeholder for future implementation
                   ()
               | None -> ()
           
           // Promote from group pools to global pool
           for kvp in this.GroupPools do
               let groupPool = kvp.Value
               let segments = groupPool.GroupPool.PostEden
               
               // Check chunk temperatures
               for i = 0 to segments.CurrentChunkIndex do
                   let temp = Volatile.Read(&segments.ChunkTemperatures.[i])
                   if temp > this.Configuration.GroupPromotionThreshold then
                       // Promote strings from this hot chunk to global pool
                       let chunk = segments.Chunks.[i]
                       for str in chunk do
                           if not (isNull str) then
                               this.GlobalPool.InternString(str) |> ignore
                       
                       // Reset temperature after promotion
                       segments.ChunkTemperatures.[i] <- 0

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
               // TODO: Merge useful local strings back to group pool?
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
       
       /// Promote hot chunks across the hierarchy
       let promoteHotChunks (hierarchy: StringPoolHierarchy) : unit =
           hierarchy.PromoteHotChunks()
       
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