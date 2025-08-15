namespace RMLPipeline.Internal

module StringPooling =

    open System
    open System.Collections.Concurrent
    open System.Threading
    open RMLPipeline.Internal.StringInterning

    // PATCH: integrate new Pool + promotion policy & promoter wiring + canonicalise API exposure.

    type GlobalPool =
        { PlanningStrings: FastMap<string,StringId>
          PlanningArray: string[]
          PlanningCount: int
          RuntimePool: InternalPoolFacade
          mutable Stats: Stats }

    type GroupPool =
        { GroupId: DependencyGroupId
          GlobalPool: GlobalPool
          GroupPoolBaseId: int
          GroupRuntime: InternalPoolFacade
          CrossTierCache: ConcurrentDictionary<StringId,string>
          mutable Stats: Stats }

    type LocalPool =
        { WorkerId: WorkerId
          GlobalPool: GlobalPool
          GroupPool: GroupPool option
          LocalRuntime: InternalPoolFacade
          LocalPoolBaseId: int
          mutable Stats: Stats }

    // Promotion policy & promoter implementations

    type DefaultPromoter(globalRef: GlobalPool, groupRef: GroupPool option) =
        interface StringPromoter with
            member _.promoteLocalToGroup s =
                match groupRef with
                | Some gp -> gp.GroupRuntime.intern s
                | None -> globalRef.RuntimePool.intern s
            member _.promoteLocalDirectToGlobal s =
                globalRef.RuntimePool.intern s
            member _.promoteGroupToGlobal s =
                globalRef.RuntimePool.intern s
            member _.promoteNone _ = StringId.Invalid

    let private buildGlobal (planning:string[]) (cfg:PoolConfiguration) (policy:PromotionPolicy) =
        let planningMap =
            planning
            |> Array.mapi (fun i s -> s, StringId.Create i)
            |> Array.fold (fun acc (k,v) -> FastMap.add k v acc) FastMap.empty
        let runtimePool =
            Pool.create planning.Length cfg.GlobalEdenSize cfg policy (Some (DefaultPromoter(Unchecked.defaultof<_>, None) :> StringPromoter))
            |> fun p -> { Pool = p }
        { PlanningStrings = planningMap
          PlanningArray = planning
          PlanningCount = planning.Length
          RuntimePool = runtimePool
          Stats = { accessCount = 0L; missCount = 0L } }

    let private buildGroup (gid:DependencyGroupId) (global:GlobalPool) (cfg:PoolConfiguration) (policy:PromotionPolicy) =
        let baseId = IdAllocation.getGroupPoolBaseId gid
        let promoter = DefaultPromoter(global, None) :> StringPromoter
        let groupPool =
            Pool.create baseId cfg.GroupEdenSize cfg policy (Some promoter)
            |> fun p -> { Pool = p }
        { GroupId = gid
          GlobalPool = global
          GroupPoolBaseId = baseId
          GroupRuntime = groupPool
          CrossTierCache = ConcurrentDictionary()
          Stats = { accessCount = 0L; missCount = 0L } }

    let private buildLocal (wid:WorkerId) (global:GlobalPool) (group:GroupPool option) (cfg:PoolConfiguration) (policy:PromotionPolicy) requestedEden =
        let baseId = IdAllocation.getLocalPoolBaseId (group |> Option.map (fun g -> g.GroupId)) wid
        let edenSize = max requestedEden cfg.WorkerEdenSize
        let promoter = DefaultPromoter(global, group) :> StringPromoter
        let lp =
            Pool.create baseId edenSize cfg policy (Some promoter)
            |> fun p -> { Pool = p }
        { WorkerId = wid
          GlobalPool = global
          GroupPool = group
          LocalRuntime = lp
          LocalPoolBaseId = baseId
          Stats = { accessCount = 0L; missCount = 0L } }

    // Public hierarchy
    type StringPoolHierarchy =
        { Global: GlobalPool
          GroupBuilder: DependencyGroupId -> GroupPool
          LocalBuilder: WorkerId -> int -> GroupPool option -> LocalPool
          GroupPools: ConcurrentDictionary<DependencyGroupId,GroupPool>
          WorkerPools: ConcurrentDictionary<WorkerId,LocalPool>
          Config: PoolConfiguration
          PromotionPolicy: PromotionPolicy }

    module StringPool =

        let createWithConfig (planning:string[]) (cfg:PoolConfiguration) =
            let policy = DefaultPromotionPolicy() :> PromotionPolicy
            let global = buildGlobal planning cfg policy
            { Global = global
              GroupBuilder = (fun gid -> buildGroup gid global cfg policy)
              LocalBuilder = (fun wid eden group -> buildLocal wid global group cfg policy eden)
              GroupPools = ConcurrentDictionary()
              WorkerPools = ConcurrentDictionary()
              Config = cfg
              PromotionPolicy = policy }

        let create (planning:string[]) =
            createWithConfig planning PoolConfiguration.Default

        let getGlobalStats h =
            // runtime + planning
            h.Global.RuntimePool.getStats()

        let getAggregateStats h =
            let gStats = h.Global.RuntimePool.getStats()
            let groups =
                h.GroupPools.Values
                |> Seq.fold (fun (ts,mu,ac,mc) gp ->
                    let st = gp.GroupRuntime.getStats()
                    (ts + st.TotalStrings, mu + st.MemoryUsageBytes, ac + st.AccessCount, mc + st.MissCount))
                    (gStats.TotalStrings, gStats.MemoryUsageBytes, gStats.AccessCount, gStats.MissCount)
            let ts,mu,ac,mc = groups
            let hr = if ac > 0L then float (ac - mc)/float ac else 0.
            { TotalStrings = ts
              MemoryUsageBytes = mu
              HitRatio = hr
              AccessCount = ac
              MissCount = mc }

        type ExecutionContext =
            { Worker: LocalPool
              Group: GroupPool option
              Global: GlobalPool }

        let createExecutionContext h (groupId:DependencyGroupId option) (maxLocal:int option) =
            let group =
                groupId
                |> Option.map(fun gid -> h.GroupPools.GetOrAdd(gid, h.GroupBuilder))
            let workerId = WorkerId.create()
            let eden = defaultArg maxLocal 0
            let local = h.LocalBuilder workerId eden group
            { Worker = local; Group = group; Global = h.Global }

        type PoolContextScope(ctx:ExecutionContext) =
            member _.InternString(str:string, pattern:StringAccessPattern) =
                match pattern with
                | StringAccessPattern.HighFrequency -> ctx.Worker.LocalRuntime.intern str
                | StringAccessPattern.Planning ->
                    match FastMap.tryFind str ctx.Global.PlanningStrings with
                    | ValueSome id -> id
                    | _ -> ctx.Global.RuntimePool.intern str
                | _ ->
                    match ctx.Group with
                    | Some gp -> gp.GroupRuntime.intern str
                    | None -> ctx.Global.RuntimePool.intern str
            member _.InternString(str:string) = ctx.Worker.LocalRuntime.intern str
            member _.Canonicalise(id:StringId) =
                // attempt local -> group -> global
                let loc = ctx.Worker.LocalRuntime.canonicalise id
                if loc.Value <> id.Value then loc
                else
                    match ctx.Group with
                    | Some gp ->
                        let gi = gp.GroupRuntime.canonicalise id
                        if gi.Value <> id.Value then gi
                        else ctx.Global.RuntimePool.canonicalise id
                    | None -> ctx.Global.RuntimePool.canonicalise id
            member _.GetString(id:StringId) : string voption =
                // try local
                match ctx.Worker.LocalRuntime.tryGet id with
                | Some s -> ValueSome s
                | None ->
                    match ctx.Group with
                    | Some gp ->
                        match gp.GroupRuntime.tryGet id with
                        | Some s -> ValueSome s
                        | None ->
                            match ctx.Global.RuntimePool.tryGet id with
                            | Some s -> ValueSome s
                            | None -> ValueNone
                    | None ->
                        match ctx.Global.RuntimePool.tryGet id with
                        | Some s -> ValueSome s
                        | None -> ValueNone
            member _.GetStats() =
                ctx.Worker.LocalRuntime.getStats()

            interface IDisposable with
                member _.Dispose() = ()

        let createScope h gid maxLocal =
            let ctx = createExecutionContext h gid maxLocal
            new PoolContextScope(ctx)