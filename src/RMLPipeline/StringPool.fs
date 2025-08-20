namespace RMLPipeline.Internal

module StringPooling =
    
    open FSharp.HashCollections
    open RMLPipeline
    open RMLPipeline.Core
    open RMLPipeline.FastMap.Types
    open RMLPipeline.Internal.StringInterning
    open RMLPipeline.Internal.StringInterning.Promotions
    open System
    open System.Collections.Concurrent
    open System.Threading

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
                this.RuntimePool.InternString str
        
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
                this.GlobalPool.InternString candidate.Value |> ignore
        
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
        GlobalPool: GlobalPool
        GroupPool: GroupPool option
        
        // Just delegate to a Pool!
        Pool: Pool  
        
        LocalPoolBaseId: int
        Configuration: PoolConfiguration
        
        // Stats recording
        mutable Stats: Stats
    } with
        
        member this.InternString(str: string, pattern: StringAccessPattern) : StringId =
            match pattern with
            | StringAccessPattern.HighFrequency ->
                // Use local pool
                this.Pool.InternString(str)
            | StringAccessPattern.Planning ->
                // Delegate to global
                this.GlobalPool.InternString(str)
            | _ ->
                // Medium/Low frequency - use group pool
                match this.GroupPool with
                | Some pool -> pool.InternString(str)
                | None -> this.GlobalPool.InternString(str)

        member this.GetString(id: StringId) : string option =
            // First check if it's in our local range
            if id.Value < 0 then 
                None
            else 
                if id.Value >= this.LocalPoolBaseId && 
                    id.Value < this.LocalPoolBaseId + IdAllocation.WorkerRangeSize then
                    this.Pool.TryGetString id
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
                this.Pool.GetStringWithTemperature id
            else
                // Delegate to appropriate pool
                if id.Value < IdAllocation.GroupPoolBase then
                    this.GlobalPool.GetStringWithTemperature id
                else
                    match this.GroupPool with
                    | Some pool -> pool.GetStringWithTemperature id
                    | None -> this.GlobalPool.GetStringWithTemperature id

        (* // Get adjusted promotion threshold based on pool size
        member this.GetAdjustedPromotionThreshold() =
            let stringCount = FastMap.count this.LocalStrings
            let scalingFactor = 
                1.0 + 
                (float stringCount / float this.Configuration.ThresholdScalingInterval) * 
                (this.Configuration.ThresholdScalingFactor - 1.0)
            int (float this.Configuration.WorkerPromotionThreshold * scalingFactor) *)
        
        // Check for hot strings to promote
        member this.CheckPromotion() : unit =
            // Fast check for any promotion signals
            this.Pool.CheckPromotion()
            |> ignore

            (* if this.PromotionSignalCount > 0L then
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
                        this.PromotionSignalCount <- this.PromotionSignalCount - int64 hotStrings.Length *)
        
        (* NB: Because stats calls can come from another thread, we use atomic ops *)
        member this.GetStats() : PoolStats = 
            this.Pool.GetStats()

    let createStringPoolHierarchy
            (planningStrings: string[]) 
            (config: PoolConfiguration) =
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
                (edenSize: int32)
                (groupPool: GroupPool option) = 
            let lpBaseId = IdAllocation.getLocalPoolBaseId 
                                (groupPool |> Option.map (fun p -> p.GroupId)) 
                                workerId
            let lpSize = max edenSize config.WorkerEdenSize
            let lpPool = Pool.Create(lpBaseId, lpSize, config)
            in    
            {
                WorkerId        = workerId
                GlobalPool      = globalPool
                GroupPool       = groupPool
                LocalPoolBaseId = lpBaseId
                Pool            = lpPool
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
                match maxLocalStrings with
                | Some max -> hierarchy.WorkerPoolBuilder workerId max groupPool
                | None -> hierarchy.WorkerPoolBuilder workerId 0 groupPool
            in
            {
                WorkerId      = workerId
                GroupId       = groupId
                ContextPool   = contextPool
                ParentPoolRef = groupPool
                GlobalPoolRef = hierarchy.GlobalPool
            }

    and StringPoolHierarchy = {
        GlobalPool: GlobalPool
        WorkerPoolBuilder: WorkerId -> int32 -> GroupPool option -> LocalPool
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
