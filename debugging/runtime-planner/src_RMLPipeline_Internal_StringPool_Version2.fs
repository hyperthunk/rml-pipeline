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

    // PATCH: Provide promotion delegate wiring between tiers.

    // Helper to build a pool with a promotion delegate
    let private makePool (baseId:int) (eden:int) (config:PoolConfiguration) (promote:PromoteDelegate option) =
        Pool.Create(baseId, eden, config, promote)

    type GlobalPool = {
        PlanningStrings: FastMap<string, StringId>
        PlanningArray: string[]
        PlanningCount: int
        RuntimePool: Pool
        mutable Stats: Stats
    } with
        member this.InternString(str:string) : StringId =
            Interlocked.Increment(&this.Stats.accessCount) |> ignore
            match FastMap.tryFind str this.PlanningStrings with
            | ValueSome id -> id
            | ValueNone ->
                // runtime lookup now dedups
                let id = this.RuntimePool.InternString str
                id

        member this.TryGetStringId(str:string) : StringId option =
            FastMap.tryFind str this.PlanningStrings |> ValueOption.toOption

        member this.GetString(id:StringId) : string option =
            if id.Value < this.PlanningCount then
                Some this.PlanningArray.[id.Value]
            elif id.Value < IdAllocation.GlobalRuntimeBase then
                // direct runtime
                match this.RuntimePool.TryGetString id with
                | Some s -> Some s
                | None ->
                    // Potential redirect - nothing higher than global; treat as missing
                    None
            else None

        member this.GetStringWithTemperature(id:StringId) =
            if id.Value < this.PlanningCount then
                Some this.PlanningArray.[id.Value], id
            elif id.Value < IdAllocation.GlobalRuntimeBase then
                this.RuntimePool.GetStringWithTemperature id
            else None, id

        member this.CheckPromotion() = this.RuntimePool.CheckPromotion()

        member this.GetStats() =
            let planningMem =
                this.PlanningArray
                |> Array.sumBy (fun s -> if isNull s then 0L else int64 (s.Length * 2))
            let rtStats = this.RuntimePool.GetStats()
            let access = Interlocked.Read &this.Stats.accessCount
            let miss = Interlocked.Read &this.Stats.missCount
            let hitRatio = if access > 0L then float (access - miss) / float access else 0.0
            {
                TotalStrings = this.PlanningCount + rtStats.TotalStrings
                MemoryUsageBytes = planningMem + rtStats.MemoryUsageBytes
                HitRatio = hitRatio
                AccessCount = access + rtStats.AccessCount
                MissCount = miss + rtStats.MissCount
            }

    type GroupPool = {
        GroupId: DependencyGroupId
        GlobalPool: GlobalPool
        GroupPoolBaseId: int
        GroupPool: Pool
        CrossTierCache: ConcurrentDictionary<StringId,string>
        mutable Stats: Stats
    } with
        member this.InternString(str:string) =
            Interlocked.Increment(&this.Stats.accessCount) |> ignore
            match this.GlobalPool.TryGetStringId str with
            | Some id -> id
            | None ->
                Interlocked.Increment(&this.Stats.missCount) |> ignore
                this.GroupPool.InternString str

        member this.GetString(id:StringId) =
            if id.Value >= this.GroupPoolBaseId &&
               id.Value < this.GroupPoolBaseId + IdAllocation.GroupPoolRangeSize then
                match this.GroupPool.TryGetString id with
                | Some s -> Some s
                | None ->
                    // Could be redirect to global
                    this.GlobalPool.GetString id
            else
                match this.CrossTierCache.TryGetValue id with
                | true, s -> Some s
                | _ ->
                    if id.Value < IdAllocation.GroupPoolBase then
                        let g = this.GlobalPool.GetString id
                        match g with
                        | Some s -> this.CrossTierCache.TryAdd(id,s) |> ignore; g
                        | None -> None
                    else None

        member this.GetStringWithTemperature(id:StringId) =
            if id.Value >= this.GroupPoolBaseId &&
               id.Value < this.GroupPoolBaseId + IdAllocation.GroupPoolRangeSize then
                let s, nid = this.GroupPool.GetStringWithTemperature id
                match s with
                | Some _ -> s, nid
                | None ->
                    // redirect?
                    this.GlobalPool.GetStringWithTemperature id
            else
                match this.GetString id with
                | Some s -> Some s, id
                | None -> None, id

        member this.CheckPromotion() =
            // Inline promotions happen inside pool; nothing to do
            ()

        member this.GetStats() =
            let gp = this.GroupPool.GetStats()
            let a = Interlocked.Read &this.Stats.accessCount
            let m = Interlocked.Read &this.Stats.missCount
            let hr = if a > 0L then float (a - m)/float a else 0.0
            {
                TotalStrings = gp.TotalStrings
                MemoryUsageBytes = gp.MemoryUsageBytes
                HitRatio = hr
                AccessCount = a + gp.AccessCount
                MissCount = m + gp.MissCount
            }

    type LocalPool = {
        WorkerId: WorkerId
        GlobalPool: GlobalPool
        GroupPool: GroupPool option
        Pool: Pool
        LocalPoolBaseId: int
        Configuration: PoolConfiguration
        mutable Stats: Stats
    } with
        member this.InternString(str:string, pattern:StringAccessPattern) =
            match pattern with
            | StringAccessPattern.HighFrequency ->
                this.Pool.InternString str
            | StringAccessPattern.Planning ->
                this.GlobalPool.InternString str
            | _ ->
                match this.GroupPool with
                | Some gp -> gp.InternString str
                | None -> this.GlobalPool.InternString str

        member this.GetString(id:StringId) =
            if id.Value >= this.LocalPoolBaseId &&
               id.Value < this.LocalPoolBaseId + IdAllocation.WorkerRangeSize then
                match this.Pool.TryGetString id with
                | Some s -> Some s
                | None ->
                    // redirect upward
                    match this.GroupPool with
                    | Some gp ->
                        gp.GetString id |> Option.orElse (this.GlobalPool.GetString id)
                    | None -> this.GlobalPool.GetString id
            else
                if id.Value < IdAllocation.GroupPoolBase then
                    this.GlobalPool.GetString id
                else
                    match this.GroupPool with
                    | Some gp -> gp.GetString id
                    | None -> this.GlobalPool.GetString id

        member this.GetStringWithTemperature(id:StringId) =
            if id.Value >= this.LocalPoolBaseId &&
               id.Value < this.LocalPoolBaseId + IdAllocation.WorkerRangeSize then
                let s,nid = this.Pool.GetStringWithTemperature id
                match s with
                | Some _ -> s,nid
                | None ->
                    // redirect
                    match this.GroupPool with
                    | Some gp ->
                        let sg, gid = gp.GetStringWithTemperature id
                        match sg with
                        | Some _ -> sg,gid
                        | None -> this.GlobalPool.GetStringWithTemperature id
                    | None -> this.GlobalPool.GetStringWithTemperature id
            else
                if id.Value < IdAllocation.GroupPoolBase then
                    this.GlobalPool.GetStringWithTemperature id
                else
                    match this.GroupPool with
                    | Some gp -> gp.GetStringWithTemperature id
                    | None -> this.GlobalPool.GetStringWithTemperature id

        member this.CheckPromotion() =
            () // inline

        member this.GetStats() = this.Pool.GetStats()

    // Factory
    let createStringPoolHierarchy (planningStrings:string[]) (config:PoolConfiguration) =
        // Build planning map
        let planningMap =
            planningStrings
            |> Array.mapi (fun i s -> s, StringId i)
            |> Array.fold (fun acc (s,id) -> FastMap.add s id acc) FastMap.empty

        // Global runtime promotion target = None
        let globalRuntimePool =
            let baseId = planningStrings.Length
            makePool baseId config.GlobalEdenSize config None

        let globalPool = {
            PlanningStrings = planningMap
            PlanningArray = planningStrings
            PlanningCount = planningStrings.Length
            RuntimePool = globalRuntimePool
            Stats = { accessCount = 0L; missCount = 0L }
        }

        // Group pool builder
        let createGroupPool (groupId:DependencyGroupId) =
            let baseId = IdAllocation.getGroupPoolBaseId groupId
            // Promotion from group -> global
            let promoteFn : PromoteDelegate =
                fun s -> globalPool.InternString s
            let gpPool = makePool baseId config.GroupEdenSize config (Some promoteFn)
            {
                GroupId = groupId
                GlobalPool = globalPool
                GroupPoolBaseId = baseId
                GroupPool = gpPool
                CrossTierCache = ConcurrentDictionary()
                Stats = { accessCount = 0L; missCount = 0L }
            }

        // Local pool builder
        let createLocalPool (workerId:WorkerId) (edenSize:int32) (groupPool:GroupPool option) =
            let lpBase = IdAllocation.getLocalPoolBaseId (groupPool |> Option.map (fun g -> g.GroupId)) workerId
            let effectiveEden = max edenSize config.WorkerEdenSize
            let promoteFn : PromoteDelegate option =
                match groupPool with
                | Some gp -> Some (fun s -> gp.InternString s) // worker -> group
                | None -> Some (fun s -> globalPool.InternString s) // worker -> global
            let lpPool = makePool lpBase effectiveEden config promoteFn
            {
                WorkerId = workerId
                GlobalPool = globalPool
                GroupPool = groupPool
                Pool = lpPool
                LocalPoolBaseId = lpBase
                Configuration = config
                Stats = { accessCount = 0L; missCount = 0L }
            }

        globalPool, createGroupPool, createLocalPool

    // (Rest of StringPoolHierarchy and helper modules unchanged – only ensure they call updated builders)