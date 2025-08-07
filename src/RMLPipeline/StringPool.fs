namespace RMLPipeline.Internal

(*
Rationale

Hierarchical implementation of string interning, designed to meet the needs of
a parallel processing architecture, while providing efficient memory usage 
and performance characteristics.

This specialized, bounded, reference-counted string management approach is 
more efficient for RML processing patterns than relying solely on .NET's 
general-purpose, global string interning mechanism.

We leverage a three-tiered design that attempts to balances contention, 
memory usage, and access patterns.

- The Global Pool provides lock-free reads for planning strings, alongside 
synchronized writes for runtime additions

- The Worker Group Pools provide bounded contention within dependency groups, 
  allowing for efficient sharing of strings across workers in the same group

- Worker Local Pools provide zero-contention access for hot paths

scope.InternString("template", HighFrequency)    // → Local pool (fast)
scope.InternString("template", Planning)         // → Global pool (shared)
scope.InternString("template", LowFrequency)     // → Group pool (balanced)

Routing is dynamic, based on the requested StringAccessPattern, providing
automatic fallback between tiers, with preferential checking, where high-frequency 
requests try local pools first, whilst planning requests route to global pool.

All strings produced during the planning phase are pre-allocated and immutable.
The RAII pattern is provided using the PoolContextScope.

Goals:
- Lock-free reads from global planning strings
- Fine-grained locking only where necessary
- Value types for IDs and patterns
- Aggressive inlining where beneficial
- No thread affinity
- Can be used with both F# async and Hopac (for parallel processing)

*)

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Threading
open System.Runtime.CompilerServices
open RMLPipeline
open RMLPipeline.FastMap.Types

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
    mutable AccessCount: int64
    mutable MissCount: int64
} with
    static member Empty = {
        TotalStrings = 0
        MemoryUsageBytes = 0L
        HitRatio = 0.0
        AccessCount = 0L
        MissCount = 0L
    }

[<Struct>]
type GlobalPoolSnapshot = {
    Version: int64
    StringCount: int
    Checksum: uint64
}

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

type GlobalStringPool = private {
    // Immutable planning-time strings (populated during RML analysis)
    PlanningStrings: FastMap<string, StringId>
    PlanningArray: string[]
    
    // Runtime concurrent additions
    RuntimeStrings: ConcurrentDictionary<string, StringId>
    RuntimeArray: ResizeArray<string>
    RuntimeLock: obj
    
    // Versioning for cache invalidation
    mutable Version: int64
    mutable Stats: PoolStats
    mutable NextRuntimeId: int32
} with
    static member Create(planningStrings: string[]) =
        let planningMap = 
            planningStrings
            |> Array.mapi (fun i str -> (str, StringId i))
            |> Array.fold (fun acc (str, id) -> FastMap.add str id acc) FastMap.empty
        
        {
            PlanningStrings = planningMap
            PlanningArray = planningStrings
            RuntimeStrings = ConcurrentDictionary<string, StringId>()
            RuntimeArray = ResizeArray<string>()
            RuntimeLock = obj()
            Version = 1L
            Stats = PoolStats.Empty
            NextRuntimeId = planningStrings.Length
        }
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.TryGetString(id: StringId) : string voption =
        if id.Value >= 0 && id.Value < this.PlanningArray.Length then
            ValueSome this.PlanningArray.[id.Value]
        else
            let runtimeIndex = id.Value - this.PlanningArray.Length
            if runtimeIndex >= 0 then
                lock this.RuntimeLock (fun () ->
                    if runtimeIndex < this.RuntimeArray.Count then
                        ValueSome this.RuntimeArray.[runtimeIndex]
                    else
                        ValueNone)
            else
                ValueNone
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.TryIntern(str: string) : StringId =
        if isNull str || str.Length = 0 then
            raise (ArgumentException "String cannot be null or empty")
        // Fast path: check planning strings first (lock-free)
        match FastMap.tryFind str this.PlanningStrings with
        | ValueSome id -> 
            Interlocked.Increment(&this.Stats.AccessCount) |> ignore
            id
        | ValueNone ->
            // Check runtime strings
            match this.RuntimeStrings.TryGetValue(str) with
            | (true, id) -> 
                Interlocked.Increment(&this.Stats.AccessCount) |> ignore
                id
            | (false, _) ->
                // Add to runtime strings (synchronized)
                this.InternNewString(str)
    
    member private this.InternNewString(str: string) : StringId =
        lock this.RuntimeLock (fun () ->
            // Double-check after acquiring lock
            match this.RuntimeStrings.TryGetValue(str) with
            | (true, id) -> 
                Interlocked.Increment(&this.Stats.AccessCount) |> ignore
                id
            | (false, _) ->
                // Ensure we don't exceed reserved global range
                if this.NextRuntimeId >= IdAllocation.GlobalRuntimeBase then
                    raise (InvalidOperationException "Global runtime string pool exhausted")
                
                let newId = StringId this.NextRuntimeId
                this.NextRuntimeId <- this.NextRuntimeId + 1
                this.RuntimeArray.Add(str)
                this.RuntimeStrings.[str] <- newId
                Interlocked.Increment(&this.Version) |> ignore
                Interlocked.Increment(&this.Stats.MissCount) |> ignore
                Interlocked.Increment(&this.Stats.AccessCount) |> ignore
                newId)
    
    member this.GetSnapshot() : GlobalPoolSnapshot =
        {
            Version = this.Version
            StringCount = this.PlanningArray.Length + this.RuntimeArray.Count
            Checksum = uint64 this.Version // Simplified checksum
        }
    
    member this.GetStats() : PoolStats = 
        let planningMemory = 
            this.PlanningArray 
            |> Array.sumBy (fun s -> if isNull s then 0L else int64 (s.Length * 2))
        
        let runtimeMemory = 
            lock this.RuntimeLock (fun () ->
                this.RuntimeArray
                |> Seq.sumBy (fun s -> if isNull s then 0L else int64 (s.Length * 2))
            )
        
        // Calculate hit ratio for this pool
        let hitRatio = 
            if this.Stats.AccessCount > 0L then
                float (this.Stats.AccessCount - this.Stats.MissCount) / float this.Stats.AccessCount
            else 0.0
        
        { 
            TotalStrings = this.PlanningArray.Length + this.RuntimeArray.Count
            MemoryUsageBytes = planningMemory + runtimeMemory
            HitRatio = hitRatio
            AccessCount = this.Stats.AccessCount
            MissCount = this.Stats.MissCount
        }

type WorkerGroupPool = private {
    GroupId: DependencyGroupId
    GlobalPoolRef: GlobalStringPool
    mutable BaseSnapshot: GlobalPoolSnapshot
    
    // Group-specific strings with fixed ID range
    GroupStrings: ConcurrentDictionary<string, StringId>
    GroupArray: ResizeArray<string>
    GroupLock: ReaderWriterLockSlim
    GroupBaseId: int
    
    mutable Stats: PoolStats
} with
    static member Create(groupId: DependencyGroupId, globalPool: GlobalStringPool) =
        {
            GroupId = groupId
            GlobalPoolRef = globalPool
            BaseSnapshot = globalPool.GetSnapshot()
            GroupStrings = ConcurrentDictionary<string, StringId>()
            GroupArray = ResizeArray<string>()
            GroupLock = new ReaderWriterLockSlim()
            GroupBaseId = IdAllocation.getGroupPoolBaseId groupId
            Stats = PoolStats.Empty
        }
    
    member this.GetCurrentBaseCount() =
        // Smart refresh: update snapshot if global pool has grown significantly
        let currentSnapshot = this.GlobalPoolRef.GetSnapshot()
        if currentSnapshot.Version > this.BaseSnapshot.Version + 5L then
            this.GroupLock.EnterWriteLock()
            try
                // Double-check after acquiring lock
                let latestSnapshot = this.GlobalPoolRef.GetSnapshot()
                if latestSnapshot.Version > this.BaseSnapshot.Version then
                    this.BaseSnapshot <- latestSnapshot
            finally
                this.GroupLock.ExitWriteLock()
        this.BaseSnapshot.StringCount
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.TryGetString(id: StringId) : string voption =
        if id.Value < 0 then ValueNone
        else
            // Check if this ID belongs to our group pool range
            if id.Value >= this.GroupBaseId && id.Value < this.GroupBaseId + IdAllocation.GroupPoolRangeSize then
                // This is a group pool ID
                let groupIndex = id.Value - this.GroupBaseId
                this.GroupLock.EnterReadLock()
                try
                    if groupIndex >= 0 && groupIndex < this.GroupArray.Count then
                        ValueSome this.GroupArray.[groupIndex]
                    else
                        ValueNone
                finally
                    this.GroupLock.ExitReadLock()
            else
                // Try global pool
                this.GlobalPoolRef.TryGetString(id)
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.TryIntern(str: string, accessPattern: StringAccessPattern) : StringId =
        if isNull str || str.Length = 0 then
            raise (ArgumentException "String cannot be null or empty")
        // For high-frequency strings, try global pool first
        if accessPattern.HasFlag(StringAccessPattern.HighFrequency) || 
           accessPattern.HasFlag(StringAccessPattern.Planning) then
            let globalId = this.GlobalPoolRef.TryIntern(str)
            Interlocked.Increment(&this.Stats.AccessCount) |> ignore
            globalId
        else
            // Check if already in group strings
            match this.GroupStrings.TryGetValue(str) with
            | (true, id) -> 
                Interlocked.Increment(&this.Stats.AccessCount) |> ignore
                id
            | (false, _) ->
                // Try global pool as fallback
                let globalId = this.GlobalPoolRef.TryIntern(str)
                match this.GlobalPoolRef.TryGetString(globalId) with
                | ValueSome _ -> 
                    Interlocked.Increment(&this.Stats.AccessCount) |> ignore
                    globalId
                | ValueNone ->
                    // Add to group-specific strings
                    this.GroupLock.EnterWriteLock()
                    try
                        match this.GroupStrings.TryGetValue(str) with
                        | (true, id) -> 
                            Interlocked.Increment(&this.Stats.AccessCount) |> ignore
                            id
                        | (false, _) ->
                            // Ensure we don't exceed group pool range
                            if this.GroupArray.Count >= IdAllocation.GroupPoolRangeSize then
                                raise (InvalidOperationException $"Group pool {this.GroupId.Value} exhausted")
                            
                            let newId = StringId (this.GroupBaseId + this.GroupArray.Count)
                            this.GroupArray.Add(str)
                            this.GroupStrings.[str] <- newId
                            Interlocked.Increment(&this.Stats.MissCount) |> ignore
                            Interlocked.Increment(&this.Stats.AccessCount) |> ignore
                            newId
                    finally
                        this.GroupLock.ExitWriteLock()
    
    member this.GetStats() : PoolStats = 
        let groupMemory = 
            this.GroupLock.EnterReadLock()
            try
                this.GroupArray
                |> Seq.sumBy (fun s -> if isNull s then 0L else int64 (s.Length * 2))
            finally
                this.GroupLock.ExitReadLock()
        
        // Calculate hit ratio for this pool
        let hitRatio = 
            if this.Stats.AccessCount > 0L then
                float (this.Stats.AccessCount - this.Stats.MissCount) / float this.Stats.AccessCount
            else 0.0
        
        { 
            TotalStrings = this.GroupArray.Count
            MemoryUsageBytes = groupMemory
            HitRatio = hitRatio
            AccessCount = this.Stats.AccessCount
            MissCount = this.Stats.MissCount
        }
    
    member this.ShouldRefresh() : bool =
        let currentSnapshot = this.GlobalPoolRef.GetSnapshot()
        currentSnapshot.Version > this.BaseSnapshot.Version
    
    interface IDisposable with
        member this.Dispose() = this.GroupLock.Dispose()

type WorkerLocalPool = private {
    WorkerId: WorkerId
    GroupPoolRef: WorkerGroupPool option
    GlobalPoolRef: GlobalStringPool
    
    // Fast local lookup with fixed ID range
    LocalStrings: Dictionary<string, StringId>
    LocalArray: string[]
    mutable LocalCount: int
    
    // Configuration
    MaxLocalStrings: int
    LocalPoolBaseId: int  // Fixed base ID for this local pool
    mutable Stats: PoolStats
} with
    
    static member Create(workerId: WorkerId, 
                         globalPool: GlobalStringPool, 
                         groupPool: WorkerGroupPool option, 
                         maxLocalStrings: int option) =
        let maxLocal = min (Option.defaultValue 1000 maxLocalStrings) IdAllocation.WorkerRangeSize
        let localBaseId = 
            groupPool 
            |> Option.map (fun gp -> gp.GroupId)
            |> IdAllocation.getLocalPoolBaseId <| workerId
            
        {
            WorkerId = workerId
            GroupPoolRef = groupPool
            GlobalPoolRef = globalPool
            LocalStrings = Dictionary<string, StringId>(maxLocal)
            LocalArray = Array.zeroCreate maxLocal
            LocalCount = 0
            MaxLocalStrings = maxLocal
            LocalPoolBaseId = localBaseId
            Stats = PoolStats.Empty
        }
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.TryGetString(id: StringId) : string voption =
        if id.Value < 0 then ValueNone
        else
            // Check if this ID belongs to our local pool range
            if id.Value >= this.LocalPoolBaseId && id.Value < this.LocalPoolBaseId + this.LocalCount then
                // This is a local pool ID
                let localIndex = id.Value - this.LocalPoolBaseId
                ValueSome this.LocalArray.[localIndex]
            else
                // Try group pool first (if available)
                match this.GroupPoolRef with
                | Some groupPool -> groupPool.TryGetString(id)
                | None -> this.GlobalPoolRef.TryGetString(id)
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member this.TryIntern(str: string, accessPattern: StringAccessPattern) : StringId =
        if isNull str || str.Length = 0 then
            raise (ArgumentException "String cannot be null or empty")
        // Fast path: check local first for high-frequency strings
        if accessPattern.HasFlag(StringAccessPattern.HighFrequency) then
            match this.LocalStrings.TryGetValue(str) with
            | (true, id) -> 
                this.Stats.AccessCount <- this.Stats.AccessCount + 1L
                id
            | (false, _) when this.LocalCount < this.MaxLocalStrings ->
                // Add to local pool using fixed range
                let newId = StringId (this.LocalPoolBaseId + this.LocalCount)
                this.LocalArray.[this.LocalCount] <- str
                this.LocalStrings.[str] <- newId
                this.LocalCount <- this.LocalCount + 1
                this.Stats.MissCount <- this.Stats.MissCount + 1L
                this.Stats.AccessCount <- this.Stats.AccessCount + 1L
                newId
            | (false, _) ->
                // Local pool full, delegate to parent pools
                this.Stats.AccessCount <- this.Stats.AccessCount + 1L
                this.delegateToParent(str, accessPattern)
        else
            // Medium/low frequency: delegate to parent pools
            this.Stats.AccessCount <- this.Stats.AccessCount + 1L
            this.delegateToParent(str, accessPattern)
    
    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    member private this.delegateToParent(str: string, accessPattern: StringAccessPattern) : StringId =
        match this.GroupPoolRef with
        | Some groupPool -> groupPool.TryIntern(str, accessPattern)
        | None -> this.GlobalPoolRef.TryIntern(str)
    
    member this.GetStats() : PoolStats = 
        let localMemory = 
            this.LocalArray
            |> Array.take this.LocalCount
            |> Array.sumBy (fun s -> if isNull s then 0L else int64 (s.Length * 2))
        
        // Calculate hit ratio for this pool
        let hitRatio = 
            if this.Stats.AccessCount > 0L then
                float (this.Stats.AccessCount - this.Stats.MissCount) / float this.Stats.AccessCount
            else 0.0
        
        { 
            TotalStrings = this.LocalCount
            MemoryUsageBytes = localMemory
            HitRatio = hitRatio
            AccessCount = this.Stats.AccessCount
            MissCount = this.Stats.MissCount
        }

type ExecutionContext = {
    WorkerId: WorkerId
    GroupId: DependencyGroupId option
    ContextPool: WorkerLocalPool
    ParentPoolRef: WorkerGroupPool option
    GlobalPoolRef: GlobalStringPool
} with
    static member Create(globalPool: GlobalStringPool, 
                         groupId: DependencyGroupId option, 
                         maxLocalStrings: int option) =
        let workerId = WorkerId.Create()
        let groupPool = 
            groupId |> Option.map (fun gid -> WorkerGroupPool.Create(gid, globalPool))
        let contextPool = 
            WorkerLocalPool.Create(workerId, globalPool, groupPool, maxLocalStrings)
        
        {
            WorkerId = workerId
            GroupId = groupId
            ContextPool = contextPool
            ParentPoolRef = groupPool
            GlobalPoolRef = globalPool
        }

type PoolContextScope(context: ExecutionContext) =
    
    member this.Context = context
    
    member this.InternString(str: string, accessPattern: StringAccessPattern) : StringId =
        context.ContextPool.TryIntern(str, accessPattern)
    
    member this.InternString(str: string) : StringId =
        context.ContextPool.TryIntern(str, StringAccessPattern.MediumFrequency)
    
    member this.GetString(id: StringId) : string voption =
        context.ContextPool.TryGetString(id)
    
    member this.GetStats() : PoolStats =
        context.ContextPool.GetStats()
    
    interface IDisposable with
        member this.Dispose() =
            // Optional: Merge useful local strings back to group pool
            // Implementation depends on specific performance requirements
            ()

type StringPoolHierarchy = private {
    GlobalPool: GlobalStringPool
    WorkerPools: ConcurrentDictionary<WorkerId, WorkerLocalPool>
    GroupPools: ConcurrentDictionary<DependencyGroupId, WorkerGroupPool>
    mutable TotalMemoryUsage: int64
} with
    
    static member Create(planningStrings: string[]) =
        {
            GlobalPool = GlobalStringPool.Create(planningStrings)
            WorkerPools = ConcurrentDictionary<WorkerId, WorkerLocalPool>()
            GroupPools = ConcurrentDictionary<DependencyGroupId, WorkerGroupPool>()
            TotalMemoryUsage = 0L
        }
    
    member this.CreateExecutionContext(groupId: DependencyGroupId option, maxLocalStrings: int option) : ExecutionContext =
        ExecutionContext.Create(this.GlobalPool, groupId, maxLocalStrings)
    
    member this.GetOrCreateGroupPool(groupId: DependencyGroupId) : WorkerGroupPool =
        this.GroupPools.GetOrAdd(groupId, fun gid -> WorkerGroupPool.Create(gid, this.GlobalPool))
    
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
                HitRatio = 0.0 // Will calculate at the end
                AccessCount = acc.AccessCount + stats.AccessCount
                MissCount = acc.MissCount + stats.MissCount
            }) PoolStats.Empty
        
        let totalAccessCount = globalStats.AccessCount + groupStats.AccessCount
        let totalMissCount = globalStats.MissCount + groupStats.MissCount
        
        let overallHitRatio = 
            if totalAccessCount > 0L then 
                float (totalAccessCount - totalMissCount) / float totalAccessCount
            else 0.0
        
        {
            TotalStrings = globalStats.TotalStrings + groupStats.TotalStrings
            MemoryUsageBytes = globalStats.MemoryUsageBytes + groupStats.MemoryUsageBytes
            HitRatio = overallHitRatio
            AccessCount = totalAccessCount
            MissCount = totalMissCount
        }

[<RequireQualifiedAccess>]
module StringPool =
    
    /// Initialize the string pool hierarchy with known planning-time strings
    let create (planningStrings: string[]) : StringPoolHierarchy =
        StringPoolHierarchy.Create(planningStrings)
    
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