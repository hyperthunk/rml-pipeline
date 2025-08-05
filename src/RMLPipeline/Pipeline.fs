namespace RMLPipeline.Execution

module Pipeline =

    open System
    open System.IO
    open System.Collections.Generic
    open System.Collections.Concurrent
    open System.Threading
    open System.Threading.Tasks
    open System.Threading.Channels
    open System.Runtime.CompilerServices
    open Newtonsoft.Json
    open Nessos.Streams
    open RMLPipeline.Model
    open RMLPipeline
    open RMLPipeline.FastMap.Types

    // Planning structures
    [<Struct>]
    type PathHash = PathHash of uint64

    [<Struct>]
    type PredicateKey = {
        Subject: string
        Predicate: string
        Hash: uint64
    }

    [<Struct>]
    type JoinKey = {
        ParentKey: string
        ChildKey: string
        Hash: uint64
    }

    // Token data for streaming
    [<Struct>]
    type StreamToken = {
        TokenType: JsonToken
        Value: obj
        Path: string
        Depth: int
        IsComplete: bool
    }

    // Enhanced token data for improved producer-consumer pattern
    [<Struct>]
    type EnhancedStreamToken = {
        TokenType: JsonToken
        Value: obj
        Path: string
        Depth: int
        IsComplete: bool
        Timestamp: int64
        ProcessorId: int
    }

    // Optimized predicate tuple structure (unchanged)
    type PredicateTuple = {
        SubjectTemplate: string
        PredicateValue: string
        ObjectTemplate: string
        ObjectDatatype: string option
        ObjectLanguage: string option
        IsConstant: bool
        SourcePath: string
        Hash: uint64
    }

    // Join tuple for efficient join processing (unchanged)
    type PredicateJoinTuple = {
        ParentTuple: PredicateTuple
        ChildTuple: PredicateTuple
        JoinCondition: Join
        ParentPath: string
        ChildPath: string
        Hash: uint64
    }

    // Planning structures (unchanged)
    type TriplesMapPlan = {
        OriginalMap: TriplesMap
        Index: int
        IteratorPath: string
        PathSegments: string[]
        Dependencies: int[]
        Priority: int
        PredicateTuples: PredicateTuple[]
        JoinTuples: PredicateJoinTuple[]
        EstimatedComplexity: int
    }

    // Aggregated planning information (unchanged)
    type RMLPlan = {
        OrderedMaps: TriplesMapPlan[]
        PredicateIndex: FastMap<uint64, PredicateTuple[]>
        JoinIndex: FastMap<uint64, PredicateJoinTuple[]>
        PathToMaps: FastMap<string, int[]>
        DependencyGroups: int[][]
        StringPool: string[]
        mutable StringCount: int
    }

    // Physical data structures
    type PredicateTupleTable = {
        Tuples: ConcurrentDictionary<string, Dictionary<string, obj> * PredicateTuple>
        FlushThreshold: int
        mutable CurrentSize: int
    }

    type PredicateJoinTable = {
        JoinTuples: ConcurrentDictionary<uint64, PredicateJoinTuple>
        ParentData: ConcurrentDictionary<string, obj>
        ChildData: ConcurrentDictionary<string, obj>
        mutable CurrentSize: int
    }

    // Streaming infrastructure
    type StreamChannel<'T> = {
        Channel: Channel<'T>
        Writer: ChannelWriter<'T>
        Reader: ChannelReader<'T>
        CancellationToken: CancellationToken
    }

    type StreamingPool = {
        Plan: RMLPlan
        Channels: StreamChannel<StreamToken>[]
        ProcessingTasks: Task<unit>[]
        GlobalPredicateTable: PredicateTupleTable
        GlobalJoinTable: PredicateJoinTable
        Output: TripleOutputStream
        CancellationTokenSource: CancellationTokenSource
    }

    // RML Planning module 
    module RMLPlanner =
                
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let hashString (str: string) : uint64 =
            let mutable hash = 14695981039346656037UL
            for i = 0 to str.Length - 1 do
                hash <- hash ^^^ uint64 str.[i]
                hash <- hash * 1099511628211UL
            hash
        
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let hashPredicateKey (subject: string) (predicate: string) : uint64 =
            let subjectHash = hashString subject
            let predicateHash = hashString predicate
            subjectHash ^^^ (predicateHash <<< 1)
        
        let extractPredicateTuples (triplesMap: TriplesMap) (mapIndex: int) : PredicateTuple[] =
            let tuples = ResizeArray<PredicateTuple>()
            let iteratorPath = triplesMap.LogicalSource.SourceIterator |> Option.defaultValue "$"
            
            // DEBUG: Check if corruption happens here
            printfn "DEBUG-EXTRACT: Original iterator path: '%s'" iteratorPath
            if iteratorPath.Contains("\n") || iteratorPath.Contains("\r") then
                printfn "DEBUG-EXTRACT: *** CORRUPTION DETECTED IN ITERATOR PATH ***"
            
            let subjectTemplate = 
                match triplesMap.SubjectMap with
                | Some sm -> 
                    match sm.SubjectTermMap.ExpressionMap.Template with
                    | Some template -> template
                    | None -> 
                        match sm.SubjectTermMap.ExpressionMap.Constant with
                        | Some (URI uri) -> uri
                        | Some (Literal lit) -> lit
                        | Some (BlankNode bn) -> "_:" + bn
                        | None -> ""
                | None -> 
                    triplesMap.Subject |> Option.defaultValue ""
            
            for pom in triplesMap.PredicateObjectMap do
                for predicate in pom.Predicate do
                    for obj in pom.Object do
                        let objValue, datatype, language = 
                            match obj with
                            | URI uri -> uri, None, None
                            | Literal lit -> lit, None, None
                            | BlankNode bn -> "_:" + bn, None, None
                        
                        let hash = hashPredicateKey subjectTemplate predicate
                        
                        tuples.Add({
                            SubjectTemplate = subjectTemplate
                            PredicateValue = predicate
                            ObjectTemplate = objValue
                            ObjectDatatype = datatype
                            ObjectLanguage = language
                            IsConstant = true
                            SourcePath = iteratorPath
                            Hash = hash
                        })
                    
                    for objMap in pom.ObjectMap do
                        let objTemplate = 
                            match objMap.ObjectTermMap.ExpressionMap.Template with
                            | Some template -> template
                            | None ->
                                match objMap.ObjectTermMap.ExpressionMap.Reference with
                                | Some reference -> "{" + reference + "}"
                                | None ->
                                    match objMap.ObjectTermMap.ExpressionMap.Constant with
                                    | Some (URI uri) -> uri
                                    | Some (Literal lit) -> lit
                                    | Some (BlankNode bn) -> "_:" + bn
                                    | None -> ""
                        
                        let hash = hashPredicateKey subjectTemplate predicate
                        
                        tuples.Add({
                            SubjectTemplate = subjectTemplate
                            PredicateValue = predicate
                            ObjectTemplate = objTemplate
                            ObjectDatatype = objMap.Datatype
                            ObjectLanguage = objMap.Language
                            IsConstant = false
                            SourcePath = iteratorPath
                            Hash = hash
                        })
                
                for predMap in pom.PredicateMap do
                    let predicateTemplate = 
                        match predMap.PredicateTermMap.ExpressionMap.Template with
                        | Some template -> template
                        | None ->
                            match predMap.PredicateTermMap.ExpressionMap.Reference with
                            | Some reference -> "{" + reference + "}"
                            | None ->
                                match predMap.PredicateTermMap.ExpressionMap.Constant with
                                | Some (URI uri) -> uri
                                | Some (Literal lit) -> lit
                                | Some (BlankNode bn) -> "_:" + bn
                                | None -> ""
                    
                    for obj in pom.Object do
                        let objValue, datatype, language = 
                            match obj with
                            | URI uri -> uri, None, None
                            | Literal lit -> lit, None, None
                            | BlankNode bn -> "_:" + bn, None, None
                        
                        let hash = hashPredicateKey subjectTemplate predicateTemplate
                        
                        tuples.Add({
                            SubjectTemplate = subjectTemplate
                            PredicateValue = predicateTemplate
                            ObjectTemplate = objValue
                            ObjectDatatype = datatype
                            ObjectLanguage = language
                            IsConstant = false
                            SourcePath = iteratorPath
                            Hash = hash
                        })
            
            tuples.ToArray()
        
        let extractJoinTuples (triplesMaps: TriplesMap[]) (mapIndex: int) : PredicateJoinTuple[] =
            let joinTuples = ResizeArray<PredicateJoinTuple>()
            let triplesMap = triplesMaps.[mapIndex]
            
            for pom in triplesMap.PredicateObjectMap do
                for refObjMap in pom.RefObjectMap do
                    let parentMap = refObjMap.ParentTriplesMap
                    let parentIndex = 
                        triplesMaps 
                        |> Array.findIndex (fun tm -> Object.ReferenceEquals(tm, parentMap))
                    
                    let parentTuples = extractPredicateTuples parentMap parentIndex
                    let childTuples = extractPredicateTuples triplesMap mapIndex
                    
                    for join in refObjMap.JoinCondition do
                        let parentPath = parentMap.LogicalSource.SourceIterator |> Option.defaultValue "$"
                        let childPath = triplesMap.LogicalSource.SourceIterator |> Option.defaultValue "$"

                        for parentTuple in parentTuples do
                            for childTuple in childTuples do
                                let hash = parentTuple.Hash ^^^ (childTuple.Hash <<< 1)
                                
                                joinTuples.Add({
                                    ParentTuple = parentTuple
                                    ChildTuple = childTuple
                                    JoinCondition = join
                                    ParentPath = parentPath
                                    ChildPath = childPath
                                    Hash = hash
                                })
            
            joinTuples.ToArray()
        
        let calculateComplexity (triplesMap: TriplesMap) : int =
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
        
        let detectOverlaps (plans: TriplesMapPlan[]) : (int * int * float)[] =
            let overlaps = ResizeArray<int * int * float>()
            
            for i = 0 to plans.Length - 1 do
                for j = i + 1 to plans.Length - 1 do
                    let plan1 = plans.[i]
                    let plan2 = plans.[j]
                    
                    let pathOverlap = 
                        if plan1.IteratorPath = plan2.IteratorPath then 1.0
                        elif plan1.IteratorPath.StartsWith(plan2.IteratorPath) || 
                            plan2.IteratorPath.StartsWith(plan1.IteratorPath) then 0.5
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
                    if totalOverlap > 0.1 then
                        overlaps.Add(i, j, totalOverlap)
            
            overlaps.ToArray()
        
        let rec createRMLPlan (triplesMaps: TriplesMap[]) : RMLPlan =
            let initialPlans = 
                triplesMaps
                |> Array.mapi (fun i tm ->
                    let iteratorPath = tm.LogicalSource.SourceIterator |> Option.defaultValue "$"
                    
                    // DEBUG: Check corruption at plan creation
                    printfn "DEBUG-PLAN: Map %d iterator path: '%s'" i iteratorPath
                    if iteratorPath.Contains("\n") || iteratorPath.Contains("\r") then
                        printfn "DEBUG-PLAN: *** CORRUPTION DETECTED AT PLAN CREATION ***"
                    
                    let pathSegments = iteratorPath.TrimStart('$', '.').Split('.')
                    let predicateTuples = extractPredicateTuples tm i
                    let joinTuples = extractJoinTuples triplesMaps i
                    let complexity = calculateComplexity tm
                    
                    {
                        OriginalMap = tm
                        Index = i
                        IteratorPath = iteratorPath
                        PathSegments = pathSegments
                        Dependencies = [||]
                        Priority = 0
                        PredicateTuples = predicateTuples
                        JoinTuples = joinTuples
                        EstimatedComplexity = complexity
                    })
            
            let overlaps = detectOverlaps initialPlans
            let priorities = Array.zeroCreate initialPlans.Length
            
            for (i, j, overlapScore) in overlaps do
                priorities.[i] <- priorities.[i] + int (overlapScore * 100.0)
                priorities.[j] <- priorities.[j] + int (overlapScore * 100.0)
            
            let orderedPlans = 
                initialPlans
                |> Array.mapi (fun i plan -> { plan with Priority = priorities.[i] })
                |> Array.sortBy (fun plan -> plan.Priority + plan.EstimatedComplexity)
            
            let predicateIndex = 
                orderedPlans
                |> Array.collect (_.PredicateTuples)
                |> Array.groupBy (_.Hash)
                |> Array.map (fun (hash, tuples) -> (hash, tuples))
                |> Array.fold (fun acc (hash, tuples) ->
                    FastMap.add hash tuples acc) FastMap.empty<uint64, PredicateTuple[]>
            
            let joinIndex = 
                orderedPlans
                |> Array.collect (_.JoinTuples)
                |> Array.groupBy (_.Hash)
                |> Array.map (fun (hash, tuples) -> (hash, tuples))
                |> Array.fold (fun acc (hash, tuples) ->
                    FastMap.add hash tuples acc) FastMap.empty<uint64, PredicateJoinTuple[]>

            let pathToMaps = 
                orderedPlans
                |> Array.mapi (fun i plan -> (plan.IteratorPath, i))
                |> Array.groupBy fst
                |> Array.map (fun (path, pairs) -> (path, pairs |> Array.map snd))
                |> Array.fold (fun acc (path, indices) ->
                    FastMap.add path indices acc) FastMap.empty<string, int[]>

            let dependencies = Array.zeroCreate orderedPlans.Length
            for i = 0 to orderedPlans.Length - 1 do
                let plan = orderedPlans.[i]
                let deps = ResizeArray<int>()
                
                for joinTuple in plan.JoinTuples do
                    let parentPath = joinTuple.ParentPath
                    match FastMap.tryFind parentPath pathToMaps with
                    | ValueSome indices ->
                        for idx in indices do
                            if idx <> i then deps.Add(idx)
                    | ValueNone -> ()
                
                dependencies.[i] <- deps.ToArray()
            
            let updatedPlans = 
                orderedPlans 
                |> Array.mapi (fun i plan -> { plan with Dependencies = dependencies.[i] })
            
            let dependencyGroups = groupByDependencies updatedPlans
            
            {
                OrderedMaps = updatedPlans
                PredicateIndex = predicateIndex
                JoinIndex = joinIndex
                PathToMaps = pathToMaps
                DependencyGroups = dependencyGroups
                StringPool = Array.zeroCreate 10000
                StringCount = 0
            }
        
        and groupByDependencies (plans: TriplesMapPlan[]) : int[][] =
            let visited = Array.zeroCreate plans.Length
            let groups = ResizeArray<int[]>()
            
            let rec dfs (index: int) (currentGroup: ResizeArray<int>) : unit =
                if not visited.[index] then
                    visited.[index] <- true
                    currentGroup.Add(index)
                    
                    for dep in plans.[index].Dependencies do
                        dfs dep currentGroup
                    
                    for i = 0 to plans.Length - 1 do
                        if Array.contains index plans.[i].Dependencies then
                            dfs i currentGroup
            
            for i = 0 to plans.Length - 1 do
                if not visited.[i] then
                    let group = ResizeArray<int>()
                    dfs i group
                    if group.Count > 0 then
                        groups.Add(group.ToArray())
            
            groups.ToArray()

    // Physical Data Structures
    module PhysicalDataStructures =
        
        let createPredicateTupleTable (flushThreshold: int) : PredicateTupleTable =
            {
                Tuples = ConcurrentDictionary<string, Dictionary<string, obj> * PredicateTuple>()
                FlushThreshold = flushThreshold
                CurrentSize = 0
            }
        
        let createPredicateJoinTable () : PredicateJoinTable =
            {
                JoinTuples = ConcurrentDictionary<uint64, PredicateJoinTuple>()
                ParentData = ConcurrentDictionary<string, obj>()
                ChildData = ConcurrentDictionary<string, obj>()
                CurrentSize = 0
            }
        
        let expandTemplate (template: string) (context: Dictionary<string, obj>) : string =
            if not (template.Contains("{")) then
                template
            else
                let mutable result = template
                for kvp in context do
                    let placeholder = "{" + kvp.Key + "}"
                    if result.Contains(placeholder) then
                        result <- result.Replace(placeholder, kvp.Value.ToString())
                result

        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let addPredicateTuple (table: PredicateTupleTable) (tuple: PredicateTuple) (context: Dictionary<string, obj>) : bool =
            let subject = expandTemplate tuple.SubjectTemplate context
            let predicate = if tuple.IsConstant then tuple.PredicateValue else expandTemplate tuple.PredicateValue context
            let objValue = if tuple.IsConstant then tuple.ObjectTemplate else expandTemplate tuple.ObjectTemplate context
            let key = sprintf "%s|%s|%s" subject predicate objValue

            // Store a COPY of the context to avoid mutation
            let contextCopy = Dictionary<string, obj>(context)
            let added = table.Tuples.TryAdd(key, (contextCopy, tuple))
            if added then
                Interlocked.Increment(&table.CurrentSize) |> ignore
            added
        
        let rec processPredicateTuple (tuple: PredicateTuple) (output: TripleOutputStream) (context: Dictionary<string, obj>) : unit =
            try
                // printfn "EMIT DEBUG: PredicateTuple=%A, Context=%A" tuple context
                let subject = expandTemplate tuple.SubjectTemplate context
                let predicate = if tuple.IsConstant then tuple.PredicateValue else expandTemplate tuple.PredicateValue context
                let objValue = if tuple.IsConstant then tuple.ObjectTemplate else expandTemplate tuple.ObjectTemplate context
                // printfn "EMIT DEBUG: subject=%s, predicate=%s, objValue=%s" subject predicate objValue
                match tuple.ObjectDatatype with
                | Some datatype -> output.EmitTypedTriple(subject, predicate, objValue, Some datatype)
                | None ->
                    match tuple.ObjectLanguage with
                    | Some lang -> output.EmitLangTriple(subject, predicate, objValue, lang)
                    | None -> output.EmitTriple(subject, predicate, objValue)
            with
            | ex -> ()// printfn "EXCEPTION in processPredicateTuple: %A" ex

        let forceFlush (table: PredicateTupleTable) (output: TripleOutputStream) : unit =
            // printfn "FORCE FLUSH: Processing %d tuples" table.CurrentSize
            for KeyValue(_, (context, tuple)) in table.Tuples do
                processPredicateTuple tuple output context
            table.Tuples.Clear()
            table.CurrentSize <- 0
        
        let flushIfNeeded (table: PredicateTupleTable) (output: TripleOutputStream) : unit =
            if table.CurrentSize >= table.FlushThreshold then
                forceFlush table output   

    // Streaming Infrastructure
    module StreamInfrastructure =
    
        open PhysicalDataStructures
        open System.Threading.Channels
        
        let mutable globalTaskCounter = 0
        
        // Create stream channel with proper cancellation
        let createStreamChannel (cancellationToken: CancellationToken) : StreamChannel<StreamToken> =
            let options = UnboundedChannelOptions()
            options.SingleReader <- false
            options.SingleWriter <- false
            let channel = Channel.CreateUnbounded<StreamToken>(options)
            let channelId = Interlocked.Increment(&globalTaskCounter)
            // printfn "[CHANNEL-%d] Created new channel" channelId
            {
                Channel = channel
                Writer = channel.Writer
                Reader = channel.Reader
                CancellationToken = cancellationToken
            }
        
        // Non-blocking token enqueue with cancellation support
        let enqueueToken (channel: StreamChannel<StreamToken>) (token: StreamToken) : bool =
            if not channel.CancellationToken.IsCancellationRequested then
                let result = channel.Writer.TryWrite(token)
                //if result then
                    // printfn "[ENQUEUE] Token written to channel: %s (Complete: %b)" token.Path token.IsComplete
                //else
                    // printfn "[ENQUEUE-FAIL] Failed to write token to channel: %s" token.Path
                result
            else
                // printfn "[ENQUEUE-CANCELLED] Channel cancelled, skipping token: %s" token.Path
                false
        
        // Signal completion to channel
        let completeChannel (channel: StreamChannel<StreamToken>) : unit =
            // printfn "[COMPLETE] Completing channel"
            channel.Writer.Complete()
        
        // Enhanced processing task with extensive debugging
        let createProcessingTask (groupIndex: int) (group: int[]) (pool: StreamingPool) : Task<unit> =
            let taskId = Interlocked.Increment(&globalTaskCounter)
            // printfn "[TASK-%d] Starting processing task for group %d with channels: %A" taskId groupIndex group
            
            task {
                try
                    let! _ = Task.WhenAll(
                        group |> Array.map (fun mapIndex ->
                            task {
                                let channelTaskId = Interlocked.Increment(&globalTaskCounter)
                                // printfn "[CHANNEL-TASK-%d] Starting channel processor for map %d" channelTaskId mapIndex
                                
                                let channel = pool.Channels.[mapIndex]
                                let plan = pool.Plan.OrderedMaps.[mapIndex]
                                
                                // printfn "[CHANNEL-TASK-%d] Channel reader created, starting enumeration" channelTaskId
                                
                                let reader = channel.Reader.ReadAllAsync(pool.CancellationTokenSource.Token)
                                use enumerator = reader.GetAsyncEnumerator(pool.CancellationTokenSource.Token)
                                
                                let mutable tokenCount = 0
                                let mutable hasMore = true
                                
                                while hasMore && not pool.CancellationTokenSource.Token.IsCancellationRequested do
                                    // printfn "[CHANNEL-TASK-%d] Waiting for next token..." channelTaskId
                                    let! moveNext = enumerator.MoveNextAsync()
                                    hasMore <- moveNext
                                    
                                    if hasMore then
                                        tokenCount <- tokenCount + 1
                                        let token = enumerator.Current
                                        // printfn "[CHANNEL-TASK-%d] Received token #%d: %s (Complete: %b)" channelTaskId tokenCount token.Path token.IsComplete
                                        
                                        if token.IsComplete then
                                            // printfn "[CHANNEL-TASK-%d] Processing completion token" channelTaskId
                                            let context = Dictionary<string, obj>()
                                            
                                            match token.Value with
                                            | :? Dictionary<string, obj> as dict ->
                                                // printfn "[CHANNEL-TASK-%d] Extracting %d context items" channelTaskId dict.Count
                                                for kvp in dict do
                                                    context.[kvp.Key] <- kvp.Value
                                            | _ -> 
                                                // printfn "[CHANNEL-TASK-%d] No context data in token" channelTaskId
                                            
                                            // Process predicate tuples with cancellation checking
                                            // printfn "[CHANNEL-TASK-%d] Processing %d predicate tuples" channelTaskId plan.PredicateTuples.Length
                                            plan.PredicateTuples
                                            |> Array.iteri (fun i tuple ->
                                                if not pool.CancellationTokenSource.Token.IsCancellationRequested then
                                                    // printfn "[CHANNEL-TASK-%d] Processing predicate tuple %d/%d" channelTaskId (i+1) plan.PredicateTuples.Length
                                                    if addPredicateTuple pool.GlobalPredicateTable tuple context then
                                                        flushIfNeeded pool.GlobalPredicateTable pool.Output
                                                else
                                                    ()
                                                    // printfn "[CHANNEL-TASK-%d] Cancellation requested during predicate processing" channelTaskId
                                            )
                                            
                                            // Process join tuples with cancellation checking
                                            // printfn "[CHANNEL-TASK-%d] Processing %d join tuples" channelTaskId plan.JoinTuples.Length
                                            plan.JoinTuples
                                            |> Array.iter (fun joinTuple ->
                                                if not pool.CancellationTokenSource.Token.IsCancellationRequested then
                                                    processPredicateTuple joinTuple.ParentTuple pool.Output context
                                                    processPredicateTuple joinTuple.ChildTuple pool.Output context
                                                else
                                                    ()
                                                    // printfn "[CHANNEL-TASK-%d] Cancellation requested during join processing" channelTaskId
                                            )
                                        else
                                            ()
                                            // printfn "[CHANNEL-TASK-%d] Skipping non-completion token" channelTaskId
                                    else
                                        ()
                                        // printfn "[CHANNEL-TASK-%d] No more tokens, channel completed" channelTaskId
                                
                                // printfn "[CHANNEL-TASK-%d] Channel processing completed. Processed %d tokens" channelTaskId tokenCount
                            }))
                    
                    // printfn "[TASK-%d] All channel tasks completed for group %d" taskId groupIndex
                    return ()
                with
                | :? OperationCanceledException -> 
                    // printfn "[TASK-%d] Processing cancelled for group %d" taskId groupIndex
                    return ()
                | ex -> 
                    // printfn "[TASK-%d] Error in processing group %d: %A" taskId groupIndex ex
                    pool.CancellationTokenSource.Cancel()
                    return ()
            }
    
    open StreamInfrastructure

    // Stream Processor
    module StreamProcessor =
    
        open StreamInfrastructure
        open PhysicalDataStructures
        
        // Initialize streaming pool with proper cancellation
        let mutable globalPoolCounter = 0
    
        let createStreamingPool (triplesMaps: TriplesMap[]) (output: TripleOutputStream) : StreamingPool =
            // DEBUG: Check TriplesMap inputs
            for i, tm in triplesMaps |> Array.indexed do
                let iteratorPath = tm.LogicalSource.SourceIterator |> Option.defaultValue "$"
                printfn "DEBUG-POOL-INPUT: TriplesMap %d iterator: '%s'" i iteratorPath
                if iteratorPath.Contains("\n") || iteratorPath.Contains("\r") then
                    printfn "DEBUG-POOL-INPUT: *** CORRUPTION IN INPUT TRIPLES MAP ***"
            
            let plan = RMLPlanner.createRMLPlan triplesMaps
            
            // DEBUG: Check plan paths after creation
            printfn "DEBUG-POOL-PLAN: PathToMaps after creation:"
            FastMap.iter (fun path indices -> 
                printfn "DEBUG-POOL-PLAN: Path '%s' -> %A" path indices
                if path.Contains("\n") || path.Contains("\r") then
                    printfn "DEBUG-POOL-PLAN: *** CORRUPTION IN PLAN PATH ***"
            ) plan.PathToMaps
            
            // DEBUG: Show what the planner created
            printfn "DEBUG: Plan created with %d maps" plan.OrderedMaps.Length
            for i, mapPlan in plan.OrderedMaps |> Array.indexed do
                printfn "DEBUG: Map %d - Iterator: '%s', Predicates: %d" 
                    i mapPlan.IteratorPath mapPlan.PredicateTuples.Length
            
            printfn "DEBUG: PathToMaps contains:"
            FastMap.iter (fun path indices -> 
                printfn "  - '%s' -> %A" path indices) plan.PathToMaps
            
            let cancellationTokenSource = new CancellationTokenSource()
            let channels = Array.init plan.OrderedMaps.Length (fun i -> 
                // printfn "[POOL-%d] Creating channel %d" poolId i
                createStreamChannel cancellationTokenSource.Token)
            
            let globalPredicateTable = createPredicateTupleTable 1000
            let globalJoinTable = createPredicateJoinTable()
            
            // printfn "[POOL-%d] Creating %d processing tasks" poolId plan.DependencyGroups.Length
            let processingTasks = 
                plan.DependencyGroups
                |> Array.mapi (fun groupIdx group ->
                    // printfn "[POOL-%d] Creating task for group %d with %d maps" poolId groupIdx group.Length
                    createProcessingTask groupIdx group {
                        Plan = plan
                        Channels = channels
                        ProcessingTasks = [||]
                        GlobalPredicateTable = globalPredicateTable
                        GlobalJoinTable = globalJoinTable
                        Output = output
                        CancellationTokenSource = cancellationTokenSource
                    })
            
            // printfn "[POOL-%d] Streaming pool created successfully" poolId
            {
                Plan = plan
                Channels = channels
                ProcessingTasks = processingTasks
                GlobalPredicateTable = globalPredicateTable
                GlobalJoinTable = globalJoinTable
                Output = output
                CancellationTokenSource = cancellationTokenSource
            }

        
        // Process JSON token with clean channel operations
        let processJsonToken (pool: StreamingPool) (tokenType: JsonToken) (value: obj) (currentPath: string) (accumulatedData: Dictionary<string, obj>) : unit =
            if pool.CancellationTokenSource.Token.IsCancellationRequested then
                () 
            else
                let token = {
                    TokenType = tokenType
                    Value = value
                    Path = currentPath
                    Depth = currentPath.Split('.').Length
                    IsComplete = (tokenType = JsonToken.EndObject || tokenType = JsonToken.EndArray)
                }
                
                // DEBUG: Show what paths we're looking for vs what we have
                match FastMap.tryFind currentPath pool.Plan.PathToMaps with
                | ValueSome mapIndices ->
                    printfn "DEBUG: Found path '%s' -> maps %A" currentPath mapIndices
                    for mapIndex in mapIndices do
                        let channel = pool.Channels.[mapIndex]
                        let finalToken = 
                            if token.IsComplete then
                                printfn "DEBUG: Sending completion token with %d context items" 
                                    (match accumulatedData with 
                                    | null -> 0 
                                    | dict -> dict.Count)
                                { token with Value = accumulatedData :> obj }
                            else
                                token
                        enqueueToken channel finalToken |> ignore
                | ValueNone -> 
                    printfn "DEBUG: No mapping found for path '%s'" currentPath
                    
                    // DEBUG: Show what paths ARE in the plan
                    if currentPath.Contains("people") then
                        printfn "DEBUG: Available paths in plan:"
                        FastMap.iter (fun path indices -> 
                            printfn "  - '%s' -> %A" path indices) pool.Plan.PathToMaps

    // Main RML Stream Processor
    module RMLStreamProcessor =
        
        open StreamProcessor

        let processRMLStream 
            (reader: JsonTextReader) 
            (triplesMaps: TriplesMap[]) 
            (output: TripleOutputStream) : Task<unit> =
            
            let rec parseLoop 
                (pool: StreamingPool)
                (pathStack: ResizeArray<string>)
                (objectStack: ResizeArray<Dictionary<string, obj>>)
                (currentProperty: string)
                (isInArray: bool)
                (arrayDepth: int)
                (contextStack: ResizeArray<bool * string * int>) =
                            
                if reader.Read() then
                    let buildCurrentPath () =
                        if pathStack.Count = 0 then 
                            "$"
                        else
                            let basePath = "$." + String.Join(".", pathStack)
                            if isInArray then basePath + "[*]" else basePath

                    let currentPath = buildCurrentPath()

                    match reader.TokenType with
                    | JsonToken.StartObject ->
                        objectStack.Add(Dictionary<string, obj>())
                        // DON'T process token here for nested objects
                        parseLoop pool pathStack objectStack currentProperty isInArray arrayDepth contextStack

                    | JsonToken.PropertyName ->
                        let propName = reader.Value :?> string
                        if not isInArray then
                            pathStack.Add(propName)
                        parseLoop pool pathStack objectStack propName isInArray arrayDepth contextStack

                    | JsonToken.String 
                    | JsonToken.Integer
                    | JsonToken.Float 
                    | JsonToken.Boolean ->
                        let newCurrentProperty =
                            if not (String.IsNullOrEmpty currentProperty) && objectStack.Count > 0 then
                                objectStack.[objectStack.Count - 1].[currentProperty] <- reader.Value
                                ""
                            else currentProperty
                        parseLoop pool pathStack objectStack newCurrentProperty isInArray arrayDepth contextStack

                    | JsonToken.EndObject ->
                        let endPath = buildCurrentPath()
                        let context =
                            if objectStack.Count > 0 then
                                let ctx = objectStack.[objectStack.Count - 1]
                                objectStack.RemoveAt(objectStack.Count - 1)
                                ctx
                            else
                                Dictionary<string, obj>()
                        
                        // ONLY process completion tokens for array items
                        if isInArray then
                            processJsonToken pool reader.TokenType context endPath context
                        
                        if not isInArray && pathStack.Count > 0 then
                            pathStack.RemoveAt(pathStack.Count - 1)
                        parseLoop pool pathStack objectStack currentProperty isInArray arrayDepth contextStack

                    | JsonToken.StartArray ->
                        contextStack.Add((isInArray, currentProperty, arrayDepth))
                        let newIsInArray = true
                        let newArrayDepth = arrayDepth + 1
                        // DON'T process StartArray token
                        parseLoop pool pathStack objectStack currentProperty newIsInArray newArrayDepth contextStack

                    | JsonToken.EndArray ->
                        // DON'T process EndArray token
                        let (newIsInArray, newCurrentProperty, newArrayDepth) =
                            if contextStack.Count > 0 then
                                let (prevIsArray, prevProperty, prevArrayDepth) = contextStack.[contextStack.Count - 1]
                                contextStack.RemoveAt(contextStack.Count - 1)
                                (prevIsArray, prevProperty, prevArrayDepth)
                            else
                                (false, "", 0)
                        if pathStack.Count > 0 then
                            pathStack.RemoveAt(pathStack.Count - 1)
                        parseLoop pool pathStack objectStack newCurrentProperty newIsInArray newArrayDepth contextStack

                    | _ ->
                        parseLoop pool pathStack objectStack currentProperty isInArray arrayDepth contextStack
                else
                    () // end of parsing        

            let sessionId = Interlocked.Increment(&globalPoolCounter)
            // printfn "[SESSION-%d] Starting RML stream processing" sessionId

            task {
                reader.SupportMultipleContent <- true
                reader.DateParseHandling <- DateParseHandling.None

                let pool = createStreamingPool triplesMaps output
                
                try
                    let pathStack = ResizeArray<string>()
                    let objectStack = ResizeArray<Dictionary<string, obj>>()
                    let contextStack = ResizeArray<bool * string * int>()
                    parseLoop pool pathStack objectStack "" false 0 contextStack

                    for channel in pool.Channels do
                        completeChannel channel

                    // Create a timeout cancellation token
                    use timeoutCts = new CancellationTokenSource(TimeSpan.FromMinutes(10.0))
                    use combinedCts = CancellationTokenSource.CreateLinkedTokenSource(pool.CancellationTokenSource.Token, timeoutCts.Token)
                    
                    try
                        // Wait for processing with combined cancellation
                        let! _ = Task.WhenAll(pool.ProcessingTasks).WaitAsync(combinedCts.Token)
                        // printfn "Processing completed successfully"
                        ()
                    with
                    | :? OperationCanceledException when timeoutCts.Token.IsCancellationRequested ->
                        // printfn "Processing timed out, cancelling..."
                        pool.CancellationTokenSource.Cancel()
                    | :? OperationCanceledException ->
                        // printfn "Processing was cancelled"

                    PhysicalDataStructures.forceFlush pool.GlobalPredicateTable output
                finally
                    pool.CancellationTokenSource.Cancel()
                    pool.CancellationTokenSource.Dispose()
            }
