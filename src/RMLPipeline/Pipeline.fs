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
    type StreamChannel = {
        TokenQueue: ConcurrentQueue<StreamToken>
        SignalEvent: ManualResetEventSlim
        IsCompleted: bool ref
        mutable IsStarted: bool
    }

    // Enhanced streaming infrastructure using .NET Channels API
    type EnhancedStreamChannel = {
        Channel: Channel<EnhancedStreamToken>
        Reader: ChannelReader<EnhancedStreamToken>
        Writer: ChannelWriter<EnhancedStreamToken>
        mutable IsCompleted: bool
        mutable IsStarted: bool
        ProcessorId: int
    }

    type StreamingPool = {
        Plan: RMLPlan
        Channels: StreamChannel[]
        ProcessingTasks: Task<unit>[]
        GlobalPredicateTable: PredicateTupleTable
        GlobalJoinTable: PredicateJoinTable
        Output: TripleOutputStream
    }

    // Enhanced streaming pool using .NET Channels API
    type EnhancedStreamingPool = {
        Plan: RMLPlan
        Channels: EnhancedStreamChannel[]
        ProcessingTasks: Task<unit>[]
        GlobalPredicateTable: PredicateTupleTable
        GlobalJoinTable: PredicateJoinTable
        Output: TripleOutputStream
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
                            |> Set.intersect (plan2.PredicateTuples |> Array.map (_.Hash) |> Set.ofArray)
                        
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
                printfn "EMIT DEBUG: PredicateTuple=%A, Context=%A" tuple context
                let subject = expandTemplate tuple.SubjectTemplate context
                let predicate = if tuple.IsConstant then tuple.PredicateValue else expandTemplate tuple.PredicateValue context
                let objValue = if tuple.IsConstant then tuple.ObjectTemplate else expandTemplate tuple.ObjectTemplate context
                printfn "EMIT DEBUG: subject=%s, predicate=%s, objValue=%s" subject predicate objValue
                match tuple.ObjectDatatype with
                | Some datatype -> output.EmitTypedTriple(subject, predicate, objValue, Some datatype)
                | None ->
                    match tuple.ObjectLanguage with
                    | Some lang -> output.EmitLangTriple(subject, predicate, objValue, lang)
                    | None -> output.EmitTriple(subject, predicate, objValue)
            with
            | ex -> printfn "EXCEPTION in processPredicateTuple: %A" ex

        let forceFlush (table: PredicateTupleTable) (output: TripleOutputStream) : unit =
            printfn "FORCE FLUSH: Processing %d tuples" table.CurrentSize
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
        
        // Create stream channel for non-blocking communication
        let createStreamChannel () : StreamChannel =
            {
                TokenQueue = ConcurrentQueue<StreamToken>()
                SignalEvent = new ManualResetEventSlim(false)
                IsCompleted = ref false
                IsStarted = false
            }
        
        // Non-blocking token enqueue
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let enqueueToken (channel: StreamChannel) (token: StreamToken) : unit =
            channel.TokenQueue.Enqueue(token)
            channel.SignalEvent.Set()
        
        // Signal completion to channel
        let completeChannel (channel: StreamChannel) : unit =
            channel.IsCompleted := true
            channel.SignalEvent.Set()
        
        // Create processing stream for dependency group using ParStream
        let createProcessingStream (groupIndex: int) (group: int[]) (pool: StreamingPool) : ParStream<unit> =
            // Create token stream from channels
            let tokenSeq = seq {
                let mutable shouldContinue = true
                while shouldContinue do
                    let mutable foundToken = false
                    
                    // Check all channels in this group for tokens
                    for mapIndex in group do
                        let channel = pool.Channels.[mapIndex]
                        let mutable token = Unchecked.defaultof<StreamToken>
                        
                        if channel.TokenQueue.TryDequeue(&token) then
                            foundToken <- true
                            yield (mapIndex, token)
                    
                    if not foundToken then
                        // Wait for signal or completion
                        let allCompleted = 
                            group |> Array.forall (fun idx -> !(pool.Channels.[idx].IsCompleted))
                        
                        if allCompleted then
                            shouldContinue <- false
                        else
                            // Wait for any channel in group to signal
                            let events = group |> Array.map (fun idx -> pool.Channels.[idx].SignalEvent.WaitHandle)
                            WaitHandle.WaitAny(events, 100) |> ignore
                            
                            // Reset signals
                            for idx in group do
                                pool.Channels.[idx].SignalEvent.Reset()
            }
            
            tokenSeq
            |> ParStream.ofSeq
            |> ParStream.map (fun (mapIndex, token) ->
                let plan = pool.Plan.OrderedMaps.[mapIndex]
                
                if token.IsComplete then
                    // Process accumulated data for this iterator
                    let context = Dictionary<string, obj>()
                    
                    // Extract data from token value if it's a dictionary
                    match token.Value with
                    | :? Dictionary<string, obj> as dict ->
                        for kvp in dict do
                            context.[kvp.Key] <- kvp.Value
                    | _ -> ()
                    
                    // Process predicate tuples
                    for tuple in plan.PredicateTuples do
                        if addPredicateTuple pool.GlobalPredicateTable tuple context then
                            flushIfNeeded pool.GlobalPredicateTable pool.Output
                    
                    // Process join tuples
                    for joinTuple in plan.JoinTuples do
                        processPredicateTuple joinTuple.ParentTuple pool.Output context
                        processPredicateTuple joinTuple.ChildTuple pool.Output context
            )
    open StreamInfrastructure

    // Enhanced Stream Processor using .NET Channels API
    module EnhancedStreamProcessor =
        
        open PhysicalDataStructures
        
        // Create enhanced stream channel using .NET Channels API
        let createEnhancedStreamChannel (processorId: int) : EnhancedStreamChannel =
            let channel = Channel.CreateUnbounded<EnhancedStreamToken>()
            {
                Channel = channel
                Reader = channel.Reader
                Writer = channel.Writer
                IsCompleted = false
                IsStarted = false
                ProcessorId = processorId
            }
        
        // Non-blocking token enqueue for enhanced channel
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let enqueueEnhancedToken (channel: EnhancedStreamChannel) (token: EnhancedStreamToken) : ValueTask<bool> =
            if not channel.IsCompleted then
                channel.Writer.TryWrite(token) |> ValueTask.FromResult
            else
                ValueTask.FromResult(false)
        
        // Signal completion to enhanced channel
        let completeEnhancedChannel (channel: EnhancedStreamChannel) : unit =
            if not channel.IsCompleted then
                channel.IsCompleted <- true
                channel.Writer.Complete()
        
        // Create processing stream for enhanced channels
        let createEnhancedProcessingStream (groupIndex: int) (group: int[]) (pool: EnhancedStreamingPool) : Task<unit> =
            Task.Run<unit>(fun () ->
                let mutable shouldContinue = true
                while shouldContinue do
                    let mutable hasData = false
                    
                    // Check all channels in this group for tokens
                    for mapIndex in group do
                        let channel = pool.Channels.[mapIndex]
                        let mutable token = Unchecked.defaultof<EnhancedStreamToken>
                        
                        if channel.Reader.TryRead(&token) then
                            hasData <- true
                            
                            if token.IsComplete then
                                shouldContinue <- false
                            else
                                // Process the token
                                let context = Dictionary<string, obj>()
                                
                                // Process predicate tuples from all ordered maps
                                for mapPlan in pool.Plan.OrderedMaps do
                                    for tuple in mapPlan.PredicateTuples do
                                        if addPredicateTuple pool.GlobalPredicateTable tuple context then
                                            flushIfNeeded pool.GlobalPredicateTable pool.Output
                                    
                                    // Process join tuples for this map
                                    for joinTuple in mapPlan.JoinTuples do
                                        processPredicateTuple joinTuple.ParentTuple pool.Output context
                                        processPredicateTuple joinTuple.ChildTuple pool.Output context
                    
                    if not hasData then
                        // Check if all channels in group are completed
                        let allCompleted = group |> Array.forall (fun idx -> pool.Channels.[idx].IsCompleted)
                        if allCompleted then
                            shouldContinue <- false
                        else
                            // Brief pause to yield CPU before checking again
                            SpinWait.SpinUntil(fun () -> true) |> ignore
            )
        
        // Initialize enhanced streaming pool with channels for each dependency group
        let createEnhancedStreamingPool (triplesMaps: TriplesMap[]) (output: TripleOutputStream) : EnhancedStreamingPool =
            let plan = RMLPlanner.createRMLPlan triplesMaps
            let channels = Array.init plan.OrderedMaps.Length (fun i -> createEnhancedStreamChannel i)
            let globalPredicateTable = createPredicateTupleTable 1000
            let globalJoinTable = createPredicateJoinTable()
            
            let processingTasks = 
                plan.DependencyGroups
                |> Array.mapi (fun groupIdx group ->
                    createEnhancedProcessingStream groupIdx group 
                        {
                            Plan = plan
                            Channels = channels
                            ProcessingTasks = [||] // Will be set after creation
                            GlobalPredicateTable = globalPredicateTable
                            GlobalJoinTable = globalJoinTable
                            Output = output
                        })
            
            {
                Plan = plan
                Channels = channels
                ProcessingTasks = processingTasks
                GlobalPredicateTable = globalPredicateTable
                GlobalJoinTable = globalJoinTable
                Output = output
            }
        
        // Process JSON token using enhanced channels
        let processEnhancedJsonToken (pool: EnhancedStreamingPool) (tokenType: JsonToken) (value: obj) (currentPath: string) (accumulatedData: Dictionary<string, obj>) : unit =
            
            // Create enhanced token with timestamp and processor info
            let createEnhancedToken (isComplete: bool) : EnhancedStreamToken =
                {
                    TokenType = tokenType
                    Value = value
                    Path = currentPath
                    Depth = currentPath.Split('/').Length - 1
                    IsComplete = isComplete
                    Timestamp = DateTime.UtcNow.Ticks
                    ProcessorId = Thread.CurrentThread.ManagedThreadId
                }
            
            // Route to appropriate channels based on path matching
            for mapIndex in 0 .. pool.Plan.OrderedMaps.Length - 1 do
                let map = pool.Plan.OrderedMaps.[mapIndex]
                if currentPath.StartsWith map.IteratorPath then
                    let channel = pool.Channels.[mapIndex]
                    
                    // Mark channel as started and enqueue token
                    if not channel.IsStarted then
                        channel.IsStarted <- true
                    
                    let finalToken = createEnhancedToken false
                    
                    // Try to write the token (this fixes the compilation error with TryRead/TryWrite usage)
                    let writeResult = channel.Writer.TryWrite(finalToken)
                    if not writeResult then
                        printfn "Warning: Could not write token to channel %d" mapIndex

    // Stream Processor
    module StreamProcessor =
        
        open StreamInfrastructure
        open PhysicalDataStructures
        
        // Initialize streaming pool with channels for each dependency group
        let createStreamingPool (triplesMaps: TriplesMap[]) (output: TripleOutputStream) : StreamingPool =
            let plan = RMLPlanner.createRMLPlan triplesMaps
            let channels = Array.init plan.OrderedMaps.Length (fun _ -> createStreamChannel())
            let globalPredicateTable = createPredicateTupleTable 1000
            let globalJoinTable = createPredicateJoinTable()
            
            // Create processing tasks for each dependency group
            let processingTasks = 
                plan.DependencyGroups
                |> Array.mapi (fun groupIdx group ->
                    Task.Run<unit>(fun () ->
                        let processingStream = 
                            createProcessingStream groupIdx group 
                                { 
                                    Plan = plan
                                    Channels = channels
                                    ProcessingTasks = [||] // Will be set after creation
                                    GlobalPredicateTable = globalPredicateTable
                                    GlobalJoinTable = globalJoinTable
                                    Output = output
                                }

                        // Start lazy processing - only begins when tokens arrive
                        processingStream |> ParStream.iter id
                    ))
            
            {
                Plan = plan
                Channels = channels
                ProcessingTasks = processingTasks
                GlobalPredicateTable = globalPredicateTable
                GlobalJoinTable = globalJoinTable
                Output = output
            }
        
        // Process JSON token and route to appropriate streams
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let processJsonToken (pool: StreamingPool) (tokenType: JsonToken) (value: obj) (currentPath: string) (accumulatedData: Dictionary<string, obj>) : unit =
            let token = {
                TokenType = tokenType
                Value = value
                Path = currentPath
                Depth = currentPath.Split('.').Length
                IsComplete = (tokenType = JsonToken.EndObject || tokenType = JsonToken.EndArray)
            }
            
            // DEBUG: Print path matching attempt
            printfn "Trying to match path: '%s'" currentPath
            
            // Route to appropriate channels based on path matching
            match FastMap.tryFind currentPath pool.Plan.PathToMaps with
            | ValueSome mapIndices ->
                printfn "MATCH FOUND! Path '%s' matched indices: %A" currentPath mapIndices
                for mapIndex in mapIndices do
                    let channel = pool.Channels.[mapIndex]
                    
                    // Mark channel as started and enqueue token
                    if not channel.IsStarted then
                        channel.IsStarted <- true
                    
                    // For completion tokens, include accumulated data
                    let finalToken = 
                        if token.IsComplete then
                            { token with Value = accumulatedData :> obj }
                        else
                            token
                    
                    enqueueToken channel finalToken
            | ValueNone -> 
                printfn "NO MATCH for path: '%s'" currentPath

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
                        printfn "StartObject - Path: '%s'" currentPath
                        processJsonToken pool reader.TokenType reader.Value currentPath (if objectStack.Count > 0 then objectStack.[objectStack.Count - 1] else null)
                        parseLoop pool pathStack objectStack currentProperty isInArray arrayDepth contextStack

                    | JsonToken.PropertyName ->
                        let propName = reader.Value :?> string
                        if not isInArray then
                            pathStack.Add(propName)
                        printfn "PropertyName: '%s', current path: '%s', isInArray: %b" propName (buildCurrentPath()) isInArray
                        parseLoop pool pathStack objectStack propName isInArray arrayDepth contextStack

                    | JsonToken.String 
                    | JsonToken.Integer
                    | JsonToken.Float 
                    | JsonToken.Boolean ->
                        // Just update property, do NOT emit any triples here!
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
                        processJsonToken pool reader.TokenType context endPath context
                        if not isInArray && pathStack.Count > 0 then
                            pathStack.RemoveAt(pathStack.Count - 1)
                        parseLoop pool pathStack objectStack currentProperty isInArray arrayDepth contextStack

                    | JsonToken.StartArray ->
                        contextStack.Add((isInArray, currentProperty, arrayDepth))
                        let newIsInArray = true
                        let newArrayDepth = arrayDepth + 1
                        let arrayPath = buildCurrentPath()
                        printfn "StartArray - Path: '%s'" arrayPath
                        processJsonToken pool reader.TokenType reader.Value arrayPath (if objectStack.Count > 0 then objectStack.[objectStack.Count - 1] else null)
                        parseLoop pool pathStack objectStack currentProperty newIsInArray newArrayDepth contextStack

                    | JsonToken.EndArray ->
                        let endPath = buildCurrentPath()
                        printfn "EndArray - Path: '%s'" endPath
                        processJsonToken pool reader.TokenType null endPath (if objectStack.Count > 0 then objectStack.[objectStack.Count - 1] else null)
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

            task {
                reader.SupportMultipleContent <- true
                reader.DateParseHandling <- DateParseHandling.None

                // Step 1: Initialize streaming pool (pre-processing phase)
                let pool = createStreamingPool triplesMaps output

                printfn "=== PathToMaps Index ==="
                pool.Plan.PathToMaps
                |> FastMap.iter (fun path indices ->
                    printfn "Path: '%s' -> Indices: %A" path indices)
                printfn "========================"                

                // Step 2: Run the recursive parser
                let pathStack = ResizeArray<string>()
                let objectStack = ResizeArray<Dictionary<string, obj>>()
                let contextStack = ResizeArray<bool * string * int>()
                parseLoop pool pathStack objectStack "" false 0 contextStack

                // Step 3: Signal completion to all channels
                for channel in pool.Channels do
                    completeChannel channel

                // Step 4: Wait for all processing to complete
                let! _ = Task.WhenAll(pool.ProcessingTasks)

                // Step 5: Final flush
                PhysicalDataStructures.forceFlush pool.GlobalPredicateTable output
            }

    // Usage example (updated)
    module RMLStreamUsage =
        
        open RMLStreamProcessor
        
        let runOptimizedRMLProcessing() =
            task {
                let json = """{"people": [{"id": "1", "name": "John Doe"}, {"id": "2", "name": "Jane Smith"}]}"""
                use reader = new JsonTextReader(new StringReader(json))
                
                let triplesMaps = [| (* RML.Model.TriplesMap definitions *) |]
                let output = 
                    {
                        new TripleOutputStream with
                            member _.EmitTypedTriple(subject, predicate, objValue, datatype) =
                                printfn "Typed Triple: %s %s %s %A" subject predicate objValue datatype
                            member _.EmitLangTriple(subject, predicate, objValue, lang) =
                                printfn "Lang Triple: %s %s %s @%s" subject predicate objValue lang
                            member _.EmitTriple(subject, predicate, objValue) =
                                printfn "Triple: %s %s %s" subject predicate objValue
                    }
                
                do! processRMLStream reader triplesMaps output
                do! processRMLStream reader triplesMaps output
            }

    // Enhanced RML Stream Usage (demonstrates compilation errors)
    module EnhancedRMLStreamUsage =
        
        open RMLStreamProcessor
        // Import the EnhancedStreamProcessor module to fix compilation errors
        open EnhancedStreamProcessor
        
        let runEnhancedRMLProcessing() =
            task {
                let json = """{"people": [{"id": "1", "name": "John Doe"}, {"id": "2", "name": "Jane Smith"}]}"""
                use reader = new JsonTextReader(new StringReader(json))
                
                let triplesMaps = [| (* RML.Model.TriplesMap definitions *) |]
                let output = 
                    {
                        new TripleOutputStream with
                            member _.EmitTypedTriple(subject, predicate, objValue, datatype) =
                                printfn "Enhanced Typed Triple: %s %s %s %A" subject predicate objValue datatype
                            member _.EmitLangTriple(subject, predicate, objValue, lang) =
                                printfn "Enhanced Lang Triple: %s %s %s @%s" subject predicate objValue lang
                            member _.EmitTriple(subject, predicate, objValue) =
                                printfn "Enhanced Triple: %s %s %s" subject predicate objValue
                    }
                
                // This demonstrates compilation errors from the problem statement:
                
                // Create enhanced streaming pool (fixes line 1011 - missing function reference)
                let pool = createEnhancedStreamingPool triplesMaps output
                
                // Use the pool to process some sample data
                processEnhancedJsonToken pool JsonToken.StartObject null "/people" (Dictionary<string, obj>())
                
                // Demonstrate Channel Reader TryRead error (line 734-735 from problem statement) 
                let testChannelUsage () =
                    let channel = createEnhancedStreamChannel 1
                    let mutable token = Unchecked.defaultof<EnhancedStreamToken>
                    
                    // This would show the error: "Type constraint mismatch with 'bool * EnhancedStreamToken' vs 'bool'"
                    let success = channel.Reader.TryRead(&token)  // Correct usage: returns bool, takes out parameter
                    
                    // This would show the error: "ChannelReader doesn't have 'Current' property"
                    // let currentToken = channel.Reader.Current  // WRONG - ChannelReader doesn't have Current property
                    
                    if success then
                        printfn "Read token: %A" token
                    
                    // Complete the channel (line 1011 from problem statement)
                    completeEnhancedChannel channel
                
                testChannelUsage()
            }            