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

    type GlobalPool = private {
        // Immutable planning strings
        PlanningStrings: FastMap<string, StringId>
        PlanningArray: string[]
        PlanningCount: int
        
        // Runtime additions - lock-free
        RuntimeStrings: ConcurrentDictionary<string, StringId>
        RuntimeIdToString: ConcurrentDictionary<int, string>
        mutable NextRuntimeId: int32  // Use Interlocked.Increment

        // Stats inline (avoiding indirection)
        mutable Stats: Stats
    } with
        
        member this.InternString(str: string) : StringId =
            // Check planning strings first (immutable, lock-free)
            Interlocked.Increment(&this.Stats.accessCount) |> ignore
            match FastMap.tryFind str this.PlanningStrings with
            | ValueSome id -> id
            | ValueNone ->
                // TODO: resizing...

                // Check/add to runtime strings
                this.RuntimeStrings.GetOrAdd(str, fun s ->
                    Interlocked.Increment(&this.Stats.missCount) |> ignore
                    let newId = Interlocked.Increment(&this.NextRuntimeId) - 1
                    if newId >= IdAllocation.GlobalRuntimeBase then
                        failwith "Global runtime ID space exhausted"                    
                    let id = StringId newId
                    this.RuntimeIdToString.[newId] <- s
                    id)
        
        member this.TryGetStringId(str: string) : StringId option =
            match FastMap.tryFind str this.PlanningStrings with
            | ValueSome id -> Some id
            | ValueNone ->
                match this.RuntimeStrings.TryGetValue(str) with
                | true, id -> Some id
                | false, _ -> None
        
        member this.GetString(id: StringId) : string option =
            if id.Value < this.PlanningCount then
                // Planning string - direct array access
                Some this.PlanningArray.[id.Value]
            elif id.Value < IdAllocation.GlobalRuntimeBase then
                // Less efficient, but also less common for the global tier
                match this.RuntimeIdToString.TryGetValue id.Value with
                | true, str -> Some str
                | false, _ -> None
            else
                // Not a global ID!
                None
        
        member this.GetStats() : PoolStats = 
            let planningMemory = 
                this.PlanningArray 
                |> Array.sumBy (fun s -> if isNull s then 0L else int64 (s.Length * 2))
            
            // TODO: ConcurrentHashMap#Values is potentially a bit expensive, maybe use sampling instead?
            let runtimeMemory = 
                this.RuntimeIdToString.Values
                |> Seq.sumBy (fun s -> if isNull s then 0L else int64 (s.Length * 2))
            
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
                TotalStrings     = this.PlanningArray.Length + this.RuntimeIdToString.Count
                MemoryUsageBytes = planningMemory + runtimeMemory
                HitRatio         = hitRatio
                AccessCount      = accessCount
                MissCount        = missCount
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
        
        // Group-specific runtime strings
        GroupStrings: ConcurrentDictionary<string, StringId>
        GroupIdToString: ConcurrentDictionary<int, string>
        mutable NextGroupId: int32  // Use Interlocked.Increment
        
        // Cache of frequently accessed strings from other tiers
        CrossTierCache: ConcurrentDictionary<StringId, string>

        // Stats recording
        mutable Stats: Stats
    } with
        
        member this.InternString(str: string) : StringId =
            if isNull str || str.Length = 0 then
                raise (ArgumentException "String cannot be null or empty")
            
            Interlocked.Increment(&this.Stats.accessCount) |> ignore
            
            match this.GroupStrings.TryGetValue str with
            | true, id -> id
            | false, _ ->
                // Check global pool first (it might be a planning string)
                let globalResult = this.GlobalPool.TryGetStringId str
                match globalResult with
                | Some id -> id
                | None ->
                    // Atomically add this to the group pool
                    this.GroupStrings.GetOrAdd(str, fun s ->
                        Interlocked.Increment(&this.Stats.missCount) |> ignore
                        let offset = Interlocked.Increment(&this.NextGroupId) - 1
                        if offset >= IdAllocation.GroupPoolRangeSize then
                            failwith $"Group {this.GroupId.Value} ID space exhausted"                        
                        let newId = StringId (this.GroupPoolBaseId + offset)
                        this.GroupIdToString.[newId.Value] <- s
                        newId)
        
        member this.GetString(id: StringId) : string option =
            // Check if it's in our group's range
            if id.Value >= this.GroupPoolBaseId && 
                id.Value < this.GroupPoolBaseId + IdAllocation.GroupPoolRangeSize then
                // It's a group ID
                match this.GroupIdToString.TryGetValue id.Value with
                | true, str -> Some str
                | false, _ -> None
            else
                // Check cross-tier cache first
                match this.CrossTierCache.TryGetValue id with
                | true, str -> Some str
                | false, _ ->
                    // Fetch from the appropriate tier
                    let result = 
                        if id.Value < IdAllocation.GroupPoolBase then
                            this.GlobalPool.GetString(id)
                        else
                            None  // Would be another group or local pool
                    
                    // Cache the result if found - we don't care about misses at this stage
                    match result with
                    | Some str -> 
                        this.CrossTierCache.TryAdd(id, str) |> ignore
                        Some str
                    | None -> None

        member this.GetStats() : PoolStats = 
            let groupMemory = 
                this.GroupIdToString.Values
                    |> Seq.sumBy (fun s -> if isNull s then 0L else int64 (s.Length * 2))
            
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
                TotalStrings     = this.GroupIdToString.Count
                MemoryUsageBytes = groupMemory
                HitRatio         = hitRatio
                AccessCount      = access
                MissCount        = miss
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
                this.GlobalPool.InternString(str)
            
            | _ ->
                // Medium/Low frequency - use group pool if available
                match this.GroupPool with
                | Some pool -> pool.InternString(str)
                | None -> this.GlobalPool.InternString(str)
        
        member this.GetString(id: StringId) : string option =
            // First check if it's in our local range
            if id.Value < 0 then 
                None
            else 
                this.Stats.accessCount <- this.Stats.accessCount + 1L
                if id.Value >= this.LocalPoolBaseId && 
                    id.Value < this.LocalPoolBaseId + IdAllocation.WorkerRangeSize then
                    (* match FastMap.tryFind id.Value this.LocalIdToString with
                    | ValueSome str -> Some str
                    | ValueNone -> None *)
                    
                    // It's a local ID                
                    let localIndex = id.Value - this.LocalPoolBaseId
                    Some this.LocalArray.[localIndex]
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
            
            this.LocalArray.[currentId] <- str
            this.LocalStrings <- FastMap.add str newId this.LocalStrings
            
            (* this.LocalIdToString.[newId.Value] <- str *)
            
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
                    this.LocalArray[id.Value] <- Unchecked.defaultof<string>
                | ValueNone -> ()

    let createStringPoolHierarchy (planningStrings: string[]) =
        let ps = planningStrings 
                |> Array.mapi (fun i str -> str, StringId i)
                |> Array.fold (fun acc (s, id) -> FastMap.add s id acc) FastMap.empty

        // Global pool with planning strings
        let globalPool = {
            PlanningStrings   = ps
            PlanningArray     = planningStrings
            PlanningCount     = planningStrings.Length
            RuntimeStrings    = ConcurrentDictionary<string, StringId>()
            RuntimeIdToString = ConcurrentDictionary<int, string>()
            NextRuntimeId     = planningStrings.Length
            Stats = 
                {
                    accessCount = 0L
                    missCount   = 0L
                }
        }
    
        // Group pool for a dependency group
        let createGroupPool (groupId: DependencyGroupId) =
            {
                GroupId         = groupId
                GlobalPool      = globalPool
                GroupPoolBaseId = IdAllocation.getGroupPoolBaseId groupId
                GroupStrings    = ConcurrentDictionary<string, StringId>()
                GroupIdToString = ConcurrentDictionary<int, string>()
                NextGroupId     = 0
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
                LocalArray      = Array.create 1000 Unchecked.defaultof<string>
                NextLocalId     = 0
                LocalPoolBaseId = lpBaseId
                MaxSize         = 1000
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
                hierarchy.WorkerPoolBuilder workerId groupPool
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
        mutable TotalMemoryUsage: int64
    } with
        
        static member Create(planningStrings: string[]) =
            let globalPool, createGroupPool, createLocalPool = 
                createStringPoolHierarchy planningStrings
            {
                GlobalPool = globalPool
                WorkerPoolBuilder = createLocalPool
                GroupPoolBuilder = createGroupPool
                WorkerPools = ConcurrentDictionary<WorkerId, LocalPool>()
                GroupPools = ConcurrentDictionary<DependencyGroupId, GroupPool>()
                TotalMemoryUsage = 0L
            }
        
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

    type PoolContextScope(context: ExecutionContext) =
        member __.Context = context
        
        member __.InternPlanningString(str: string) : StringId =
            context.GlobalPoolRef.InternString str

        member __.InternString(str: string, accessPattern: StringAccessPattern) : StringId =
            context.ContextPool.InternString(str, accessPattern)

        member __.InternString(str: string) : StringId =
            context.ContextPool.InternString(str, StringAccessPattern.MediumFrequency)

        member __.GetString(id: StringId) : string option =
            context.ContextPool.GetString id
        
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