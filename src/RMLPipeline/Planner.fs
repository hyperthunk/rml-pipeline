namespace RMLPipeline.Internal

(*

The planning algorithm aims to provide substantial runtime 
improvements for complex RML mappings with:

- Multiple interdependent triples maps
- Complex join conditions
- Overlapping data paths
- Large-scale processing requirements
- Pre-computed join conditions
- Dependency-aware execution order

The implementation should scale from low-memory streaming scenarios
to high-performance batch processing.

This is achieved through several key optimizations:

- Adaptive index strategies
- Chunked processing for memory-constrained environments
- Lazy evaluation for expensive operations
- Hot path optimizations & compact data structures
- Pre-computed join conditions, to eliminate redundant join analysis during execution
- Dependency-aware ordering, to prevent unnecessary re-processing
- Path-based grouping enables batch processing of related mappings

Parallelization.

Each dependency group represents a cluster of related triples maps.
Groups with no inter-dependencies can be processed completely in parallel,
giving a degree equivalent to the number of independent groups.

Within a group, maps whose dependencies are satisfied can execute 
concurrently, giving a degree of n-maps per group.

For streaming data (JSON/XML/CSV), each independent group can process 
different chunks, yielding a degree of parallelism equivalent to 
CPU core count × number of independent groups

Usage:
open RMLPipeline.Internal.Planner

// Create a plan with full StringPool integration
let triplesMaps = [| (* your RML mappings *) |]
let rmlPlan = createRMLPlan triplesMaps

// Access strings through the StringPool
let firstPlan = rmlPlan.OrderedMaps.[0]
let subjectTemplate = PlanUtils.getSubjectTemplate rmlPlan firstPlan.PredicateTuples.[0]

// Create worker contexts for parallel execution
let group0Context = PlanUtils.createGroupExecutionContext rmlPlan 0
let group1Context = PlanUtils.createGroupExecutionContext rmlPlan 1

// Workers can now use their contexts for efficient string access
use workerScope = new PoolContextScope(group0Context)
let runtimeStringId = workerScope.InternString("http://example.org/dynamic/123", StringAccessPattern.HighFrequency)

// Monitor memory usage
let stats = PlanUtils.getMemoryStats rmlPlan
printfn "Total memory usage: %d KB" (stats.MemoryUsageBytes / 1024L)
printfn "Hit ratio: %.2f%%" (stats.HitRatio * 100.0)

*)

module Planner =

    open System
    open System.Runtime.CompilerServices
    open RMLPipeline
    open RMLPipeline.FastMap.Types
    open RMLPipeline.Model
    open RMLPipeline.Internal.StringInterning

    /// <summary>
    /// Memory modes for the planner, allowing adaptive behavior based on 
    /// different scenarios or available resources.
    /// </summary>
    type MemoryMode =
        | HighPerformance   // Full indexes, all pool tiers, maximum parallelization
        | Balanced          // Selective indexing, group pools, chunked processing  
        | LowMemory         // Streaming, minimal indexes, local pools only

    type IndexStrategy =
        | NoIndex           // No indexing, linear search
        | HashIndex         // Hash-based lookup only
        | FullIndex         // Complete indexing with all optimizations

    [<Flags>]
    type TupleFlags =
        | None = 0uy
        | IsConstant = 1uy
        | HasDatatype = 2uy
        | HasLanguage = 4uy
        | HasJoin = 8uy
        | IsBlankNode = 16uy
        | IsTemplate = 32uy

    type PlannerConfig = {
        MemoryMode: MemoryMode
        MaxMemoryMB: int option
        ChunkSize: int
        IndexStrategy: IndexStrategy option
        EnableStringPooling: bool
        MaxLocalStrings: int option
        MaxGroupPools: int option
    } with
        static member Default = {
            MemoryMode = Balanced
            MaxMemoryMB = Some 256
            ChunkSize = 100
            IndexStrategy = None
            EnableStringPooling = true
            MaxLocalStrings = Some 1000
            MaxGroupPools = Some 10
        }
        
        static member LowMemory = {
            MemoryMode = LowMemory
            MaxMemoryMB = Some 64
            ChunkSize = 50
            IndexStrategy = Some NoIndex
            EnableStringPooling = true
            MaxLocalStrings = Some 200
            MaxGroupPools = Some 2
        }
        
        static member HighPerformance = {
            MemoryMode = HighPerformance
            MaxMemoryMB = Some 1024
            ChunkSize = 1000
            IndexStrategy = Some FullIndex
            EnableStringPooling = true
            MaxLocalStrings = Some 5000
            MaxGroupPools = Some 50
        }

    /// <summary>
    /// Represents the atomic unit of RDF triple generation. 
    /// Each predicate-object mapping can be pre-analyzed and hashed 
    /// for efficient indexing. The Hash field enables O(1) 
    /// lookups and deduplication.
    /// </summary>
    [<Struct>]
    type PredicateTuple = {
        SubjectTemplateId: StringId
        PredicateValueId: StringId
        ObjectTemplateId: StringId
        SourcePathId: StringId
        Hash: uint64
        Flags: TupleFlags
    } with
        member inline this.IsConstant = this.Flags &&& TupleFlags.IsConstant <> TupleFlags.None
        member inline this.HasJoin = this.Flags &&& TupleFlags.HasJoin <> TupleFlags.None
        member inline this.IsTemplate = this.Flags &&& TupleFlags.IsTemplate <> TupleFlags.None
        member inline this.HasDatatype = this.Flags &&& TupleFlags.HasDatatype <> TupleFlags.None
        member inline this.HasLanguage = this.Flags &&& TupleFlags.HasLanguage <> TupleFlags.None
        member inline this.IsBlankNode = this.Flags &&& TupleFlags.IsBlankNode <> TupleFlags.None

    /// <summary>
    /// Captures the join relationships between different triples maps, 
    /// which is crucial for determining execution dependencies and 
    /// enabling parallel processing within dependency groups.
    /// </summary>
    [<Struct>]
    type JoinTuple = {
        ParentTuple: PredicateTuple
        ChildTuple: PredicateTuple
        JoinCondition: Join
        ParentPathId: StringId
        ChildPathId: StringId
        Hash: uint64
    }

    [<Struct>]
    type HotPathTuple = {
        SubjectTemplateId: StringId
        PredicateValueId: StringId
        ObjectTemplateId: StringId
        SourcePathId: StringId
        Hash: uint64
        Flags: TupleFlags
    }

    [<Struct>]
    type DependencyGroups = {
        GroupStarts: int[]
        GroupMembers: int[]
    } with
        member this.GetGroup(groupIndex: int) : int[] =
            if groupIndex >= this.GroupStarts.Length then [||]
            else
                let startIdx = this.GroupStarts.[groupIndex]
                let endIdx = 
                    if groupIndex = this.GroupStarts.Length - 1 then 
                         this.GroupMembers.Length
                    else this.GroupStarts.[groupIndex + 1]
                this.GroupMembers.[startIdx..endIdx-1]

    type HotPathStack = {
        mutable Data: byte[]
        mutable Position: int
        mutable Capacity: int
    } with
        
        static member Create(stackSize: int) =
            {
                Data = Array.zeroCreate<byte> stackSize
                Position = 0
                Capacity = stackSize
            }
        
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        member this.Push(value: uint64) =
            if this.Position + 8 > this.Capacity then
                failwith "Stack overflow"
            let bytes = BitConverter.GetBytes(value)
            Array.Copy(bytes, 0, this.Data, this.Position, 8)
            this.Position <- this.Position + 8
        
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        member this.Pop() : uint64 =
            if this.Position < 8 then
                failwith "Stack underflow"
            this.Position <- this.Position - 8
            BitConverter.ToUInt64(this.Data, this.Position)
        
        member this.IsEmpty = this.Position = 0

    (* Lightweight string resolver for hot path processing.
    Provides direct access to StringPool without disposal overheads. *)
    type HotPathStringResolver(planningContext: PoolContextScope) =
        
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        member __.GetString(stringId: StringId) : string option =
            planningContext.GetString(stringId)
                
        member __.GetStringUnsafe(stringId: StringId) : string =
            match planningContext.GetString stringId with
            | Some str -> str
            | None -> failwith $"StringId {stringId.Value} not found in planning context"

    /// <summary>
    /// The execution plan for a single triples map, containing all the 
    /// metadata needed for optimal scheduling and resource allocation.
    /// </summary>    
    type TriplesMapPlan = {
        OriginalMap: TriplesMap
        Index: int
        IteratorPathId: StringId
        PathSegments: StringId[]
        Dependencies: int[]
        Priority: int
        PredicateTuples: PredicateTuple[]
        JoinTuples: JoinTuple[]
        EstimatedComplexity: int
        IndexStrategy: IndexStrategy
    }

    /// <summary>
    /// The complete RML planning context, encapsulating all triples maps,
    /// dependency groups, and string pool hierarchy.
    /// </summary>
    type RMLPlan = {
        OrderedMaps: TriplesMapPlan[]
        DependencyGroups: DependencyGroups
        Config: PlannerConfig
        StringPoolHierarchy: StringPoolHierarchy
        PlanningContext: PoolContextScope
        
        // Lazy indexes for memory efficiency
        PredicateIndex: Lazy<FastMap<uint64, PredicateTuple[]>>
        JoinIndex: Lazy<FastMap<uint64, JoinTuple[]>>
        PathToMaps: Lazy<FastMap<StringId, int[]>>
        OverlapData: Lazy<struct (int * int * float)[]>
    } with
        member this.GetPredicateIndex() = this.PredicateIndex.Value
        member this.GetJoinIndex() = this.JoinIndex.Value
        member this.GetPathToMaps() = this.PathToMaps.Value
        member this.GetOverlapData() = this.OverlapData.Value  

    (* 
        Hashes a string using the FNV-1a algorithm, since it is 
        fast, minimizes collisions, and is highly suitable for
        inlining to a single instruction across common architectures. 

        NB: refactoring
    *)    
    (* 
    let private hashString (str: string) : uint64 =
        let mutable hash = 14695981039346656037UL
        for i = 0 to str.Length - 1 do
            hash <- hash ^^^ uint64 str.[i]
            hash <- hash * 1099511628211UL
        hash 
    *)

    (* 
        Comprehensively collects all string values that will be used 
        during planning and execution, including iterator paths, 
        subject/predicate/object templates, constants, and reference patterns. 
    *)
    let private extractAllTemplateStrings (triplesMaps: TriplesMap[]) : string[] =
        let strings = ResizeArray<string>()
        
        let addStringIfNotEmpty str =
            if not (String.IsNullOrEmpty str) then
                strings.Add(str)
        
        for triplesMap in triplesMaps do
            // iterator paths
            triplesMap.LogicalSource.SourceIterator 
            |> Option.iter addStringIfNotEmpty
            
            // subject templates
            match triplesMap.SubjectMap with
            | Some sm -> 
                match sm.SubjectTermMap.ExpressionMap.Template with
                | Some template -> addStringIfNotEmpty template
                | None -> 
                    match sm.SubjectTermMap.ExpressionMap.Constant with
                    | Some (URI uri) -> addStringIfNotEmpty uri
                    | Some (Literal lit) -> addStringIfNotEmpty lit
                    | Some (BlankNode bn) -> addStringIfNotEmpty ("_:" + bn)
                    | None -> ()
                    
                // subject reference patterns
                sm.SubjectTermMap.ExpressionMap.Reference
                |> Option.iter (fun ref -> addStringIfNotEmpty ("{" + ref + "}"))
            | None -> 
                triplesMap.Subject |> Option.iter addStringIfNotEmpty
            
            // predicate-object maps
            for pom in triplesMap.PredicateObjectMap do
                // Static predicates
                for predicate in pom.Predicate do
                    addStringIfNotEmpty predicate
                
                // Static objects
                for obj in pom.Object do
                    match obj with
                    | URI uri -> addStringIfNotEmpty uri
                    | Literal lit -> addStringIfNotEmpty lit
                    | BlankNode bn -> addStringIfNotEmpty ("_:" + bn)
                
                // Predicate and object templates
                for predMap in pom.PredicateMap do
                    match predMap.PredicateTermMap.ExpressionMap.Template with
                    | Some template -> addStringIfNotEmpty template
                    | None ->
                        predMap.PredicateTermMap.ExpressionMap.Reference
                        |> Option.iter (fun ref -> addStringIfNotEmpty ("{" + ref + "}"))
                        
                        match predMap.PredicateTermMap.ExpressionMap.Constant with
                        | Some (URI uri) -> addStringIfNotEmpty uri
                        | Some (Literal lit) -> addStringIfNotEmpty lit
                        | Some (BlankNode bn) -> addStringIfNotEmpty ("_:" + bn)
                        | None -> ()
                
                for objMap in pom.ObjectMap do
                    match objMap.ObjectTermMap.ExpressionMap.Template with
                    | Some template -> addStringIfNotEmpty template
                    | None ->
                        objMap.ObjectTermMap.ExpressionMap.Reference
                        |> Option.iter (fun ref -> addStringIfNotEmpty ("{" + ref + "}"))
                        
                        match objMap.ObjectTermMap.ExpressionMap.Constant with
                        | Some (URI uri) -> addStringIfNotEmpty uri
                        | Some (Literal lit) -> addStringIfNotEmpty lit
                        | Some (BlankNode bn) -> addStringIfNotEmpty ("_:" + bn)
                        | None -> ()
        
        strings.ToArray() |> Array.distinct
    
    (*
        Maps with no dependencies get placed in their own individual groups.
        Maps with dependencies get grouped together with their dependencies.
        The algorithm ensures every map appears in exactly one group.

        Maps with no dependencies can run completely independently, while maps 
        with dependencies must be grouped together for proper execution ordering
        in a parallel processing pipeline.
    *)
    let private groupByDependencies (plans: TriplesMapPlan[]) : DependencyGroups =
        let rec buildGroups 
                (remaining: int list) 
                (visited: Set<int>) 
                (groups: int list list) =
            match remaining with
            | [] -> groups |> List.rev
            | index :: rest when Set.contains index visited -> 
                buildGroups rest visited groups
            | index :: rest ->
                let rec collectGroup 
                        (toVisit: int list) 
                        (currentGroup: int list) 
                        (localVisited: Set<int>) =
                    match toVisit with
                    | [] -> (currentGroup |> List.rev, localVisited)
                    | idx :: remaining when Set.contains idx localVisited ->
                        collectGroup remaining currentGroup localVisited
                    | idx :: remaining ->
                        let newGroup = idx :: currentGroup
                        let newVisited = Set.add idx localVisited
                        let dependencies = 
                            if idx < plans.Length then 
                                plans.[idx].Dependencies |> Array.toList
                            else []
                        let unvisitedDeps = 
                            dependencies 
                            |> List.filter (fun dep -> not (Set.contains dep localVisited))
                        collectGroup (unvisitedDeps @ remaining) newGroup newVisited
                
                let (group, groupVisited) = collectGroup [index] [] visited
                buildGroups rest (Set.union visited groupVisited) (group :: groups)
        
        let allGroups = buildGroups [0 .. plans.Length - 1] Set.empty []
        let flatMembers = allGroups |> List.collect id |> List.toArray
        let groupStarts = 
            allGroups 
            |> List.map List.length
            |> List.scan (+) 0
            |> List.take allGroups.Length
            |> List.toArray
        {
            GroupStarts = groupStarts
            GroupMembers = flatMembers
        }
    
    let private detectOverlaps (plans: TriplesMapPlan[]) : struct (int * int * float)[] =
        let rec calculateOverlaps 
                (i: int) 
                (j: int) 
                (acc: struct (int * int * float) list) =
            if i >= plans.Length then acc |> List.rev |> List.toArray
            elif j >= plans.Length then calculateOverlaps (i + 1) (i + 2) acc
            elif i = j then calculateOverlaps i (j + 1) acc
            else
                let plan1 = plans.[i]
                let plan2 = plans.[j]
                
                // Fast StringId comparison instead of string comparison
                let pathOverlap = 
                    if plan1.IteratorPathId = plan2.IteratorPathId then 1.0
                    else 0.0
                
                let predicateOverlap = 
                    let commonHashes = 
                        plan1.PredicateTuples
                        |> Array.map (_.Hash)
                        |> Set.ofArray
                        |> Set.intersect (plan2.PredicateTuples |> Array.map _.Hash |> Set.ofArray)
                    
                    let totalHashes = plan1.PredicateTuples.Length + plan2.PredicateTuples.Length
                    if totalHashes > 0 then
                        float (commonHashes.Count * 2) / float totalHashes
                    else 0.0
                
                let totalOverlap = (pathOverlap + predicateOverlap) / 2.0
                let newAcc = 
                    if totalOverlap > 0.1 then struct (i, j, totalOverlap) :: acc
                    else acc
                
                calculateOverlaps i (j + 1) newAcc
        
        calculateOverlaps 0 1 []
    
    let private chooseIndexStrategy 
            (plan: TriplesMapPlan) 
            (config: PlannerConfig) : IndexStrategy =
        match config.IndexStrategy with
        | Some strategy -> strategy
        | None ->
            match plan.EstimatedComplexity, plan.Dependencies.Length, config.MemoryMode with
            | complexity, deps, LowMemory when complexity < 100 && deps = 0 -> NoIndex
            | complexity, deps, _ when complexity < 50 && deps = 0 -> NoIndex  
            | complexity, deps, _ when complexity < 200 && deps < 3 -> HashIndex
            | _ -> FullIndex
    
    let private extractPredicateTuples 
            (triplesMap: TriplesMap) 
            (mapIndex: int) 
            (poolScope: PoolContextScope) : PredicateTuple[] =
        
        let tuples         = ResizeArray<PredicateTuple>()        
        let iteratorPath   = triplesMap.LogicalSource.SourceIterator |> Option.defaultValue "$"
        let iteratorPathId = poolScope.InternString(iteratorPath, StringAccessPattern.Planning)
        
        let _, subjectTemplateId = 
            match triplesMap.SubjectMap with
            | Some sm -> 
                match sm.SubjectTermMap.ExpressionMap.Template with
                | Some template -> 
                    let id = poolScope.InternString(template, StringAccessPattern.Planning)
                    template, id
                | None -> 
                    match sm.SubjectTermMap.ExpressionMap.Constant with
                    | Some (URI uri) -> 
                        let id = poolScope.InternString(uri, StringAccessPattern.Planning)
                        uri, id
                    | Some (Literal lit) -> 
                        let id = poolScope.InternString(lit, StringAccessPattern.Planning)
                        lit, id
                    | Some (BlankNode bn) -> 
                        let template = "_:" + bn
                        let id = poolScope.InternString(template, StringAccessPattern.Planning)
                        template, id
                    | None -> "", poolScope.InternString("", StringAccessPattern.Planning)
            | None -> 
                let template = triplesMap.Subject |> Option.defaultValue ""
                let id = poolScope.InternString(template, StringAccessPattern.Planning)
                template, id
        
        for pom in triplesMap.PredicateObjectMap do
            // Process static predicates with static objects
            for predicate in pom.Predicate do
                let predicateId = poolScope.InternString(predicate, StringAccessPattern.Planning)
                
                for obj in pom.Object do
                    let objValue, objId, flags = 
                        match obj with
                        | URI uri -> 
                            let id = poolScope.InternString(uri, StringAccessPattern.Planning)
                            uri, id, TupleFlags.IsConstant
                        | Literal lit -> 
                            let id = poolScope.InternString(lit, StringAccessPattern.Planning)
                            lit, id, TupleFlags.IsConstant
                        | BlankNode bn -> 
                            let template = "_:" + bn
                            let id = poolScope.InternString(template, StringAccessPattern.Planning)
                            template, id, TupleFlags.IsConstant ||| TupleFlags.IsBlankNode
                    
                    let hash = uint64 subjectTemplateId.Value ^^^ 
                                (uint64 predicateId.Value <<< 1) ^^^ 
                                (uint64 objId.Value <<< 2) ^^^  // or objTemplateId.Value
                                (uint64 iteratorPathId.Value <<< 3)
                    
                    tuples.Add({
                        SubjectTemplateId = subjectTemplateId
                        PredicateValueId  = predicateId
                        ObjectTemplateId  = objId
                        SourcePathId      = iteratorPathId
                        Hash              = hash
                        Flags             = flags
                    })
                
                // Process static predicates with object templates
                for objMap in pom.ObjectMap do
                    let _, objTemplateId, flags = 
                        match objMap.ObjectTermMap.ExpressionMap.Template with
                        | Some template -> 
                            let id = poolScope.InternPlanningString template
                            template, id, TupleFlags.IsTemplate
                        | None ->
                            match objMap.ObjectTermMap.ExpressionMap.Reference with
                            | Some reference -> 
                                let template = "{" + reference + "}"
                                let id = poolScope.InternPlanningString template
                                template, id, TupleFlags.IsTemplate
                            | None ->
                                match objMap.ObjectTermMap.ExpressionMap.Constant with
                                | Some (URI uri) -> 
                                    let id = poolScope.InternPlanningString uri
                                    uri, id, TupleFlags.IsConstant
                                | Some (Literal lit) -> 
                                    let id = poolScope.InternPlanningString lit
                                    lit, id, TupleFlags.IsConstant
                                | Some (BlankNode bn) -> 
                                    let template = "_:" + bn
                                    let id = poolScope.InternPlanningString template
                                    template, id, TupleFlags.IsConstant ||| TupleFlags.IsBlankNode
                                | None -> "", poolScope.InternPlanningString "", TupleFlags.None

                    let finalFlags = 
                        flags |||
                        (if objMap.Datatype.IsSome then TupleFlags.HasDatatype else TupleFlags.None) |||
                        (if objMap.Language.IsSome then TupleFlags.HasLanguage else TupleFlags.None)
                    
                    let hash = uint64 subjectTemplateId.Value ^^^ 
                                (uint64 predicateId.Value <<< 1) ^^^ 
                                (uint64 objTemplateId.Value <<< 2) ^^^  // or objTemplateId.Value
                                (uint64 iteratorPathId.Value <<< 3)
                    
                    tuples.Add({
                        SubjectTemplateId = subjectTemplateId
                        PredicateValueId  = predicateId
                        ObjectTemplateId  = objTemplateId
                        SourcePathId      = iteratorPathId
                        Hash              = hash
                        Flags             = finalFlags
                    })
        
        tuples.ToArray()
    
    let private extractJoinTuples 
            (triplesMaps: TriplesMap[]) 
            (mapIndex: int) 
            (poolScope: PoolContextScope) : JoinTuple[] =
        let joinTuples = ResizeArray<JoinTuple>()
        let triplesMap = triplesMaps.[mapIndex]
        
        for pom in triplesMap.PredicateObjectMap do
            for refObjMap in pom.RefObjectMap do
                let parentMap = refObjMap.ParentTriplesMap
                let parentIndex = 
                    triplesMaps 
                    |> Array.findIndex (fun tm -> Object.ReferenceEquals(tm, parentMap))
                
                let parentTuples = extractPredicateTuples parentMap parentIndex poolScope
                let childTuples  = extractPredicateTuples triplesMap mapIndex poolScope
                
                for join in refObjMap.JoinCondition do
                    let parentPath   = parentMap.LogicalSource.SourceIterator |> Option.defaultValue "$"
                    let childPath    = triplesMap.LogicalSource.SourceIterator |> Option.defaultValue "$"                    
                    let parentPathId = poolScope.InternString(parentPath, StringAccessPattern.Planning)
                    let childPathId  = poolScope.InternString(childPath, StringAccessPattern.Planning)

                    for parentTuple in parentTuples do
                        for childTuple in childTuples do
                            // Mark both tuples as having joins
                            let parentTupleWithJoin = 
                                { parentTuple with 
                                    Flags = parentTuple.Flags ||| TupleFlags.HasJoin
                                }
                            let childTupleWithJoin = 
                                { childTuple with 
                                    Flags = childTuple.Flags ||| TupleFlags.HasJoin
                                }

                            let hash =  
                                uint64 parentTuple.SubjectTemplateId.Value ^^^
                                    (uint64 parentTuple.PredicateValueId.Value <<< 1) ^^^
                                    (uint64 parentTuple.ObjectTemplateId.Value <<< 2) ^^^
                                    (uint64 childTuple.SubjectTemplateId.Value <<< 3) ^^^
                                    (uint64 childTuple.PredicateValueId.Value <<< 4) ^^^
                                    (uint64 childTuple.ObjectTemplateId.Value <<< 5) ^^^
                                    (uint64 parentPathId.Value <<< 6) ^^^
                                    (uint64 childPathId.Value <<< 7)
                            
                            joinTuples.Add({
                                ParentTuple    = parentTupleWithJoin
                                ChildTuple     = childTupleWithJoin
                                JoinCondition  = join
                                ParentPathId   = parentPathId
                                ChildPathId    = childPathId
                                Hash           = hash
                            })
        
        joinTuples.ToArray()
    
    let private calculateComplexity (triplesMap: TriplesMap) : int =
        let mutable complexity = 0
        
        let iteratorDepth = 
            triplesMap.LogicalSource.SourceIterator 
            |> Option.map (fun path -> path.Split('.').Length)
            |> Option.defaultValue 1
        complexity <- complexity + iteratorDepth * 10
        
        for pom in triplesMap.PredicateObjectMap do
            complexity <- complexity + pom.PredicateMap.Length * 5
            complexity <- complexity + pom.ObjectMap.Length * 5
            complexity <- complexity + pom.RefObjectMap.Length * 20
        
        match triplesMap.SubjectMap with
        | Some sm ->
            complexity <- complexity + sm.Class.Length * 2
            complexity <- complexity + sm.GraphMap.Length * 3
        | None -> ()
        
        complexity
    
    let private createTriplesMapPlan 
            (index: int) 
            (triplesMap: TriplesMap) 
            (poolScope: PoolContextScope) 
            (config: PlannerConfig) : TriplesMapPlan =
        let iteratorPath   = triplesMap.LogicalSource.SourceIterator |> Option.defaultValue "$"
        let iteratorPathId = poolScope.InternString(iteratorPath, StringAccessPattern.Planning)
        
        let pathSegments = 
            iteratorPath.TrimStart('$', '.').Split('.')
            |> Array.map (fun segment -> 
                poolScope.InternString(segment, StringAccessPattern.Planning))
        
        let predicateTuples = extractPredicateTuples triplesMap index poolScope
        let complexity      = calculateComplexity triplesMap
        
        let plan = {
            OriginalMap         = triplesMap
            Index               = index
            IteratorPathId      = iteratorPathId
            PathSegments        = pathSegments
            Dependencies        = [||]      // computed later
            Priority            = 0
            PredicateTuples     = predicateTuples
            JoinTuples          = [||]      // computed later
            EstimatedComplexity = complexity
            IndexStrategy       = NoIndex   // set later
        }
        in { plan with IndexStrategy = chooseIndexStrategy plan config }
    
    (*  Lazy Index Construction *)
    
    let private buildPredicateIndex (plans: TriplesMapPlan[]) : FastMap<uint64, PredicateTuple[]> =
        // Only build indexes for plans that actually need them
        let indexedPlans = plans |> Array.filter (fun p -> p.IndexStrategy <> NoIndex)
        if indexedPlans.Length = 0 then
            FastMap.empty
        else
            indexedPlans
            |> Array.collect (_.PredicateTuples)
            |> Array.groupBy (_.Hash)
            |> Array.fold (fun acc (hash, tuples) ->
                FastMap.add hash tuples acc) FastMap.empty

    let private buildJoinIndex (plans: TriplesMapPlan[]) : FastMap<uint64, JoinTuple[]> =
        // Only build join indexes for FullIndex plans
        let fullIndexPlans = plans |> Array.filter (fun p -> p.IndexStrategy = FullIndex)
        if fullIndexPlans.Length = 0 then
            FastMap.empty
        else
            fullIndexPlans
            |> Array.collect (_.JoinTuples)
            |> Array.groupBy (_.Hash)
            |> Array.fold (fun acc (hash, tuples) ->
                FastMap.add hash tuples acc) FastMap.empty
    
    let private buildPathIndex (plans: TriplesMapPlan[]) : FastMap<StringId, int[]> =
        plans
        |> Array.mapi (fun i plan -> (plan.IteratorPathId, i))
        |> Array.groupBy fst
        |> Array.map (fun (pathId, pairs) -> (pathId, pairs |> Array.map snd))
        |> Array.fold (fun acc (pathId, indices) ->
            FastMap.add pathId indices acc) FastMap.empty
    
    (* 
        Memory Mode Optimized Planning:
        Orchestrates the planning process including string extraction,
        StringPool creation, dependency analysis, and lazy index construction. 
    *)
    let private createMemoryOptimizedPlan (triplesMaps: TriplesMap[]) (config: PlannerConfig) : RMLPlan =
        // Phase 1: Extract strings for StringPool (adapted based on memory mode)
        let planningStrings = 
            match config.MemoryMode with
            | LowMemory -> 
                // Extract only essential strings to minimize space utilisation
                triplesMaps
                |> Array.collect (fun tm -> 
                    [|  yield tm.LogicalSource.SourceIterator |> Option.defaultValue "$"
                        yield! tm.PredicateObjectMap |> List.toArray |> Array.collect (fun pom -> pom.Predicate |> List.toArray) |])
                |> Array.distinct
            | Balanced | HighPerformance ->
                extractAllTemplateStrings triplesMaps
        
        // Phase 2: Create StringPool hierarchy
        let stringPoolHierarchy = StringPool.create planningStrings
        let planningContext = 
            match config.MemoryMode with
            | LowMemory -> StringPool.createScope stringPoolHierarchy None (Some 200)
            | Balanced -> StringPool.createScope stringPoolHierarchy None (Some 1000)
            | HighPerformance -> StringPool.createScope stringPoolHierarchy None (Some 5000)
        
        // Phase 3: Create plans with StringPool integration
        let initialPlans = 
            triplesMaps
            |> Array.mapi (fun i tm -> createTriplesMapPlan i tm planningContext config)
        
        // Phase 4: Compute join tuples and dependencies
        let plansWithJoins = 
            initialPlans
            |> Array.mapi (fun i plan ->
                let joinTuples = extractJoinTuples triplesMaps i planningContext
                { plan with JoinTuples = joinTuples })
        
        // Phase 5: Dependency analysis
        let pathToMapsEager = buildPathIndex plansWithJoins
        let dependencies = Array.zeroCreate plansWithJoins.Length
        
        for i = 0 to plansWithJoins.Length - 1 do
            let plan = plansWithJoins.[i]
            let deps = ResizeArray<int>()
            
            for joinTuple in plan.JoinTuples do
                let parentPathId = joinTuple.ParentPathId
                match FastMap.tryFind parentPathId pathToMapsEager with
                | ValueSome indices ->
                    for idx in indices do
                        if idx <> i then deps.Add(idx)
                | ValueNone -> ()
            
            dependencies.[i] <- deps.ToArray()
        
        let finalPlans = 
            plansWithJoins 
            |> Array.mapi (fun i plan -> { plan with Dependencies = dependencies.[i] })
        
        // Phase 6: Dependency grouping and lazy index creation
        let dependencyGroups = groupByDependencies finalPlans
        
        // Lazy evaluation based on memory mode
        let lazyPredicateIndex = lazy (
            match config.IndexStrategy with
            | Some NoIndex -> FastMap.empty
            | _ -> buildPredicateIndex finalPlans
        )
        
        let lazyJoinIndex = lazy (
            match config.IndexStrategy with
            | Some NoIndex -> FastMap.empty
            | _ -> buildJoinIndex finalPlans
        )
        
        let lazyPathToMaps = lazy pathToMapsEager
        let lazyOverlapData = lazy (detectOverlaps finalPlans)
        
        {
            OrderedMaps         = finalPlans
            DependencyGroups    = dependencyGroups
            Config              = config
            StringPoolHierarchy = stringPoolHierarchy
            PlanningContext     = planningContext
            PredicateIndex      = lazyPredicateIndex
            JoinIndex           = lazyJoinIndex
            PathToMaps          = lazyPathToMaps
            OverlapData         = lazyOverlapData
        }
    
    let executeHotPath (plan: RMLPlan) : unit =
        let stackSize = 
            match plan.Config.MemoryMode with
            | LowMemory -> 4096
            | Balanced -> 8192
            | HighPerformance -> 16384
        
        let stack = HotPathStack.Create stackSize
        
        let predicateIndex = 
            match plan.Config.IndexStrategy with
            | Some NoIndex -> None
            | _ -> 
                let index = plan.GetPredicateIndex()
                if FastMap.isEmpty index then None else Some index
        
        (* tail-call optimised using a fixed size stack *)
        let rec processGroups groupIndex =
            if groupIndex < plan.DependencyGroups.GroupStarts.Length then
                let groupMembers = plan.DependencyGroups.GetGroup(groupIndex)
                processMaps groupMembers 0
                processGroups (groupIndex + 1)        
        and processMaps 
                (groupMembers: int[]) mapIndex =
            if mapIndex < groupMembers.Length then
                let mapPlan = plan.OrderedMaps.[groupMembers.[mapIndex]]                
                match mapPlan.IndexStrategy, predicateIndex with
                | NoIndex, _ -> 
                    processTuplesSequential mapPlan.PredicateTuples 0
                | (HashIndex | FullIndex), Some index -> 
                    processTuplesIndexed mapPlan.PredicateTuples index
                | (HashIndex | FullIndex), None -> 
                    processTuplesSequential mapPlan.PredicateTuples 0                
                processMaps groupMembers (mapIndex + 1)        
        and processTuplesSequential 
                (tuples: PredicateTuple[]) tupleIndex =
            if tupleIndex < tuples.Length then
                let tuple = tuples.[tupleIndex]
                let hotTuple = {
                    HotPathTuple.SubjectTemplateId = 
                        tuple.SubjectTemplateId
                    PredicateValueId = tuple.PredicateValueId
                    ObjectTemplateId = tuple.ObjectTemplateId
                    SourcePathId     = tuple.SourcePathId
                    Hash             = tuple.Hash
                    Flags            = tuple.Flags
                }
                processHotPathTuple hotTuple
                processTuplesSequential tuples (tupleIndex + 1)        
        and processTuplesIndexed 
                (tuples: PredicateTuple[]) 
                (index: FastMap<uint64, PredicateTuple[]>) =
            let processedHashes = ResizeArray<uint64>()
            
            for tuple in tuples do
                if not (processedHashes.Contains(tuple.Hash)) then
                    processedHashes.Add(tuple.Hash)
                    
                    match FastMap.tryFind tuple.Hash index with
                    | ValueSome relatedTuples ->
                        for relatedTuple in relatedTuples do
                            let hotTuple = {
                                HotPathTuple.SubjectTemplateId = 
                                    relatedTuple.SubjectTemplateId
                                PredicateValueId = relatedTuple.PredicateValueId
                                ObjectTemplateId = relatedTuple.ObjectTemplateId
                                SourcePathId     = relatedTuple.SourcePathId
                                Hash             = relatedTuple.Hash
                                Flags            = relatedTuple.Flags
                            }
                            processHotPathTuple hotTuple
                    | ValueNone ->
                        let hotTuple = {
                            HotPathTuple.SubjectTemplateId = 
                                tuple.SubjectTemplateId
                            PredicateValueId = tuple.PredicateValueId
                            ObjectTemplateId = tuple.ObjectTemplateId
                            SourcePathId     = tuple.SourcePathId
                            Hash             = tuple.Hash
                            Flags            = tuple.Flags
                        }
                        processHotPathTuple hotTuple        
        and processHotPathTuple (tuple: HotPathTuple) =
            stack.Push tuple.Hash            
            if tuple.Flags &&& TupleFlags.IsConstant <> TupleFlags.None then
                stack.Pop() |> ignore
            else
                stack.Push(uint64 tuple.SubjectTemplateId.Value)
                stack.Push(uint64 tuple.PredicateValueId.Value)
                stack.Push(uint64 tuple.ObjectTemplateId.Value)
                stack.Push(uint64 tuple.SourcePathId.Value)
                
                let hasJoin = tuple.Flags &&& TupleFlags.HasJoin <> TupleFlags.None
                if hasJoin then
                    let sourceHash    = stack.Pop()
                    let objectHash    = stack.Pop()
                    let predicateHash = stack.Pop()
                    let subjectHash   = stack.Pop()
                    let originalHash  = stack.Pop()

                    let computedHash = 
                        subjectHash ^^^ (predicateHash <<< 1) 
                                    ^^^ (objectHash <<< 2) 
                                    ^^^ (sourceHash <<< 3)
                    ignore (computedHash = originalHash)
                else
                    let sourceHash    = stack.Pop()
                    let objectHash    = stack.Pop()
                    let predicateHash = stack.Pop()
                    let subjectHash   = stack.Pop()
                    let originalHash  = stack.Pop()

                    let computedHash = 
                        subjectHash ^^^ (predicateHash <<< 1) 
                                    ^^^ (objectHash <<< 2) 
                                    ^^^ (sourceHash <<< 3)

                    // TODO: do we actually want to crash here or warn?
                    ignore (computedHash = originalHash)
        
        processGroups 0
    
    /// <summary>
    /// Alternative hot path execution with string resolution enabled for debugging.
    /// Slightly slower but provides access to actual string values for logging/debugging.
    /// </summary>
    let executeHotPathWithStringResolution (plan: RMLPlan) (enableLogging: bool) : unit =
        // Allocate stack space
        let stackSize = 
            match plan.Config.MemoryMode with
            | LowMemory -> 4096 | Balanced -> 8192 | HighPerformance -> 16384
        
        let stack = HotPathStack.Create(stackSize)
        
        // Create string resolver for debugging
        let stringResolver = HotPathStringResolver(plan.PlanningContext)
        let mutable processedTuples = 0
        
        let rec processGroups groupIndex =
            if groupIndex < plan.DependencyGroups.GroupStarts.Length then
                if enableLogging then
                    printfn "[Group %d] Processing dependency group..." groupIndex
                
                let groupMembers = plan.DependencyGroups.GetGroup(groupIndex)
                processMaps groupMembers 0
                processGroups (groupIndex + 1)
        
        and processMaps (groupMembers: int[]) mapIndex =
            if mapIndex < groupMembers.Length then
                let mapPlan = plan.OrderedMaps.[groupMembers.[mapIndex]]
                
                if enableLogging then
                    let iteratorPath = stringResolver.GetString mapPlan.IteratorPathId
                    printfn "  [Map %d] Processing %d tuples (path: %A)" 
                            mapPlan.Index mapPlan.PredicateTuples.Length iteratorPath
                
                processTuples mapPlan.PredicateTuples 0
                processMaps groupMembers (mapIndex + 1)
        
        and processTuples (tuples: PredicateTuple[]) tupleIndex =
            if tupleIndex < tuples.Length then
                let tuple = tuples.[tupleIndex]
                let hotTuple = {
                    HotPathTuple.SubjectTemplateId = tuple.SubjectTemplateId
                    PredicateValueId = tuple.PredicateValueId
                    ObjectTemplateId = tuple.ObjectTemplateId
                    SourcePathId     = tuple.SourcePathId
                    Hash             = tuple.Hash
                    Flags            = tuple.Flags
                }
                
                processHotPathTuple hotTuple
                processedTuples <- processedTuples + 1
                processTuples tuples (tupleIndex + 1)
        
        and processHotPathTuple (tuple: HotPathTuple) =
            stack.Push(tuple.Hash)
            
            if enableLogging && processedTuples % 1000 = 0 then
                let subject   = stringResolver.GetString tuple.SubjectTemplateId
                let predicate = stringResolver.GetString tuple.PredicateValueId
                let object    = stringResolver.GetString tuple.ObjectTemplateId
                printfn "    [Tuple %d] %A -> %A -> %A (flags: %A)" 
                        processedTuples subject predicate object tuple.Flags
            
            if tuple.Flags &&& TupleFlags.IsConstant <> TupleFlags.None then
                stack.Pop() |> ignore
            else
                stack.Push(uint64 tuple.SubjectTemplateId.Value)
                stack.Push(uint64 tuple.PredicateValueId.Value)
                stack.Push(uint64 tuple.ObjectTemplateId.Value)
                stack.Push(uint64 tuple.SourcePathId.Value)
                
                let hasJoin = tuple.Flags &&& TupleFlags.HasJoin <> TupleFlags.None
                if hasJoin then
                    stack.Push(uint64 tuple.SourcePathId.Value)
                    let sourceHash    = stack.Pop()
                    let objectHash    = stack.Pop()
                    let predicateHash = stack.Pop()
                    let subjectHash   = stack.Pop()
                    let originalHash  = stack.Pop()
                    
                    let computedHash = 
                        subjectHash ^^^ (predicateHash <<< 1) 
                                    ^^^ (objectHash <<< 2) 
                                    ^^^ (sourceHash <<< 3)
                    
                    if enableLogging && computedHash <> originalHash then
                        printfn "    [WARNING] Join hash mismatch: computed=%d, original=%d" 
                            computedHash originalHash
                    
                    ignore (computedHash = originalHash)
                else
                    let sourceHash    = stack.Pop()
                    let objectHash    = stack.Pop()
                    let predicateHash = stack.Pop()
                    let subjectHash   = stack.Pop()
                    let originalHash  = stack.Pop()
                    
                    let computedHash = 
                        subjectHash ^^^ (predicateHash <<< 1) 
                                    ^^^ (objectHash <<< 2) 
                                    ^^^ (sourceHash <<< 3)
                    
                    if enableLogging && computedHash <> originalHash then
                        printfn "    [WARNING] Tuple hash mismatch: computed=%d, original=%d" 
                            computedHash originalHash
                    
                    ignore (computedHash = originalHash)
        
        processGroups 0
        
        if enableLogging then
            printfn "Hot path processing completed. Total tuples processed: %d" processedTuples
    
    /// <summary>
    /// Create an optimized plan for the given triples maps
    /// </summary>
    let createRMLPlan (triplesMaps: TriplesMap[]) (config: PlannerConfig) : RMLPlan =
        let validatedConfig = 
            match config.MemoryMode with
            | LowMemory when config.ChunkSize > 100 -> 
                { config with ChunkSize = 100 }
            | HighPerformance when config.ChunkSize < 100 ->
                { config with ChunkSize = 100 }
            | _ -> config
        
        printfn "Creating optimized RML plan with %A memory mode..." validatedConfig.MemoryMode
        let plan = createMemoryOptimizedPlan triplesMaps validatedConfig
        
        let globalStats = StringPool.getGlobalStats plan.StringPoolHierarchy
        printfn "  - %d triples maps" plan.OrderedMaps.Length
        printfn "  - %d dependency groups" plan.DependencyGroups.GroupStarts.Length
        printfn "  - StringPool memory usage: %d KB" (globalStats.MemoryUsageBytes / 1024L)
        printfn "  - StringPool hit ratio: %.2f%%" (globalStats.HitRatio * 100.0)
        
        plan
    
    /// <summary>
    /// Create a streaming plan for memory-constrained environments
    /// </summary>
    let createStreamingPlan (triplesMaps: TriplesMap[]) (config: PlannerConfig) : seq<RMLPlan> =
        let streamConfig = 
            { 
                config with MemoryMode = LowMemory; 
                            ChunkSize = min config.ChunkSize 100 
            }
        seq {
            for chunk in triplesMaps |> Array.chunkBySize streamConfig.ChunkSize do
                yield createRMLPlan chunk streamConfig
        }
    
    module PlanUtils =
        
        // Get the actual string value from a StringId using the plan's context
        let getString (plan: RMLPlan) (stringId: StringId) : string option =
            plan.PlanningContext.GetString stringId
        
        let getMemoryStats (plan: RMLPlan) : PoolStats =
            StringPool.getAggregateStats plan.StringPoolHierarchy
        
        let getTupleInfo (plan: RMLPlan) (tuple: PredicateTuple) : string * string * string =
            let subject = getString plan tuple.SubjectTemplateId |> Option.defaultValue "?"
            let predicate = getString plan tuple.PredicateValueId |> Option.defaultValue "?"
            let object = getString plan tuple.ObjectTemplateId |> Option.defaultValue "?"
            (subject, predicate, object)
        
        /// Get readable tuple flags for debugging
        let getTupleFlags (tuple: PredicateTuple) : string list =
            [
                if tuple.IsConstant then yield "Constant"
                if tuple.IsTemplate then yield "Template"  
                if tuple.HasJoin then yield "HasJoin"
                if tuple.HasDatatype then yield "HasDatatype"
                if tuple.HasLanguage then yield "HasLanguage"
                if tuple.IsBlankNode then yield "BlankNode"
            ]
        
        let executeWithMonitoring (plan: RMLPlan) : int64 * PoolStats =
            let sw = System.Diagnostics.Stopwatch.StartNew()
            executeHotPath plan
            sw.Stop()
            let stats = getMemoryStats plan
            (sw.ElapsedMilliseconds, stats)
        
        let executeWithDebugging (plan: RMLPlan) (enableLogging: bool) : int64 * PoolStats =
            let sw = System.Diagnostics.Stopwatch.StartNew()
            executeHotPathWithStringResolution plan enableLogging
            sw.Stop()
            let stats = getMemoryStats plan
            (sw.ElapsedMilliseconds, stats)
        
        /// Get dependency group information for debugging
        let getDependencyGroupInfo (plan: RMLPlan) : (int * int[])[] =
            [| for i = 0 to plan.DependencyGroups.GroupStarts.Length - 1 do
                let members = plan.DependencyGroups.GetGroup(i)
                yield (i, members) |]
        
        /// Create a lightweight string resolver for custom processing
        let createStringResolver (plan: RMLPlan) : HotPathStringResolver =
            HotPathStringResolver(plan.PlanningContext)
        
        /// Benchmark hot path performance with different configurations
        let benchmarkHotPath (plan: RMLPlan) (iterations: int) : (int64 * int64) =
            // Warm up
            for _ = 1 to 3 do executeHotPath plan
            
            // Benchmark standard hot path
            let sw1 = System.Diagnostics.Stopwatch.StartNew()
            for _ = 1 to iterations do
                executeHotPath plan
            sw1.Stop()
            
            // Benchmark hot path with string resolution
            let sw2 = System.Diagnostics.Stopwatch.StartNew()
            for _ = 1 to iterations do
                executeHotPathWithStringResolution plan false
            sw2.Stop()
            
            (sw1.ElapsedMilliseconds / int64 iterations, sw2.ElapsedMilliseconds / int64 iterations)