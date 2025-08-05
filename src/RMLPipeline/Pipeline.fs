namespace RMLPipeline.Execution

module Pipeline =

    open System
    open System.IO
    open System.Collections.Generic
    open System.Collections.Concurrent
    open System.Threading
    open System.Threading.Tasks
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

    // Holds the token value
    type TokenValue =
    | StringValue of string
    | IntegerValue of int64
    | FloatValue of float
    | BooleanValue of bool
    | DateTimeValue of DateTime
    | CompletionData of Dictionary<string, obj>
    | StructuralToken  // For StartObject, StartArray, etc. that don't carry data

    // Token data for streaming
    [<Struct>]
    type StreamToken = {
        TokenType: JsonToken
        Value: TokenValue
        Path: string
        Depth: int
        IsComplete: bool
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
        Tuples: ConcurrentDictionary<uint64, PredicateTuple>
        DuplicateCheck: ConcurrentDictionary<string, bool>
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

    type StreamingPool = {
        Plan: RMLPlan
        Channels: StreamChannel[]
        ProcessingTasks: Task[]
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
                Tuples = ConcurrentDictionary<uint64, PredicateTuple>()
                DuplicateCheck = ConcurrentDictionary<string, bool>()
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
            printfn "EXPANDING TEMPLATE: '%s' with context: %A" template context  // DEBUG
            if not (template.Contains("{")) then
                printfn "NO PLACEHOLDERS, returning: '%s'" template  // DEBUG
                template
            else
                let mutable result = template
                for kvp in context do
                    let placeholder = "{" + kvp.Key + "}"
                    if result.Contains(placeholder) then
                        let oldResult = result
                        result <- result.Replace(placeholder, kvp.Value.ToString())
                        printfn "REPLACED '%s' -> '%s' in '%s'" placeholder (kvp.Value.ToString()) oldResult  // DEBUG
                printfn "FINAL EXPANDED: '%s'" result  // DEBUG
                result
        
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let addPredicateTuple (table: PredicateTupleTable) (tuple: PredicateTuple) (data: Dictionary<string, obj>) : bool =
            // Expand templates with actual data to create the real triple key
            let expandedSubject = expandTemplate tuple.SubjectTemplate data
            let expandedPredicate = if tuple.IsConstant then tuple.PredicateValue else expandTemplate tuple.PredicateValue data
            let expandedObject = if tuple.IsConstant then tuple.ObjectTemplate else expandTemplate tuple.ObjectTemplate data
            
            // Use the EXPANDED values for deduplication, not the templates
            let key = sprintf "%s|%s|%s" expandedSubject expandedPredicate expandedObject
            
            printfn "DEDUPLICATION CHECK: Key='%s'" key  // DEBUG
            
            if table.DuplicateCheck.ContainsKey(key) then
                printfn "DUPLICATE FOUND: Skipping key '%s'" key  // DEBUG
                false
            else
                printfn "NEW TRIPLE: Adding key '%s'" key  // DEBUG
                table.DuplicateCheck.TryAdd(key, true) |> ignore
                table.Tuples.TryAdd(tuple.Hash, tuple) |> ignore
                Interlocked.Increment(&table.CurrentSize) |> ignore
                true
        
        let rec flushIfNeeded (table: PredicateTupleTable) (output: TripleOutputStream) (context: Dictionary<string, obj>) : unit =
            if table.CurrentSize >= table.FlushThreshold then
                for kvp in table.Tuples do
                    let tuple = kvp.Value
                    processPredicateTuple tuple output context
                
                table.Tuples.Clear()
                table.DuplicateCheck.Clear()
                table.CurrentSize <- 0
        
        and processPredicateTuple (tuple: PredicateTuple) (output: TripleOutputStream) (context: Dictionary<string, obj>) : unit =
            try
                printfn "PROCESSING PREDICATE TUPLE: %A" tuple  // DEBUG

                let subject = expandTemplate tuple.SubjectTemplate context
                let predicate = if tuple.IsConstant then tuple.PredicateValue else expandTemplate tuple.PredicateValue context
                let objValue = if tuple.IsConstant then tuple.ObjectTemplate else expandTemplate tuple.ObjectTemplate context
                
                printfn "EMITTING TRIPLE: S='%s', P='%s', O='%s'" subject predicate objValue  // DEBUG

                match tuple.ObjectDatatype with
                | Some datatype -> 
                    printfn "TYPED TRIPLE EMISSION"  // DEBUG
                    output.EmitTypedTriple(subject, predicate, objValue, Some datatype)
                | None ->
                    match tuple.ObjectLanguage with
                    | Some lang -> 
                        printfn "LANG TRIPLE EMISSION"  // DEBUG
                        output.EmitLangTriple(subject, predicate, objValue, lang)
                    | None -> 
                        printfn "SIMPLE TRIPLE EMISSION"  // DEBUG
                        output.EmitTriple(subject, predicate, objValue)
            with
            | ex -> 
                printfn "EXCEPTION in processPredicateTuple: %s" ex.Message  // DEBUG
                ()
        
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
                    printfn "PROCESSING COMPLETION TOKEN for map %d" mapIndex
                    
                    let context = Dictionary<string, obj>()
                    
                    match token.Value with
                    | CompletionData dict ->
                        printfn "EXTRACTED COMPLETION DATA: %A with %d items" dict dict.Count
                        for kvp in dict do
                            context.[kvp.Key] <- kvp.Value
                    | StringValue s ->
                        printfn "UNEXPECTED: Got StringValue in completion token: %s" s
                    | IntegerValue i ->
                        printfn "UNEXPECTED: Got IntegerValue in completion token: %d" i
                    | FloatValue f ->
                        printfn "UNEXPECTED: Got FloatValue in completion token: %f" f
                    | BooleanValue b ->
                        printfn "UNEXPECTED: Got BooleanValue in completion token: %b" b
                    | DateTimeValue dt ->
                        printfn "UNEXPECTED: Got DateTimeValue in completion token: %A" dt
                    | StructuralToken ->
                        printfn "COMPLETION TOKEN WITH STRUCTURAL DATA ONLY"
                    
                    printfn "FINAL CONTEXT: %A" context  // DEBUG
                    printfn "PREDICATE TUPLES COUNT: %d" plan.PredicateTuples.Length  // DEBUG
                    
                    // Process predicate tuples
                    for tuple in plan.PredicateTuples do
                        if addPredicateTuple pool.GlobalPredicateTable tuple context then
                            flushIfNeeded pool.GlobalPredicateTable pool.Output context
                    
                    // Process join tuples
                    for joinTuple in plan.JoinTuples do
                        processPredicateTuple joinTuple.ParentTuple pool.Output context
                        processPredicateTuple joinTuple.ChildTuple pool.Output context
            )
    open StreamInfrastructure

    // Enhanced Streaming Infrastructure with Producer-Consumer Pattern
    module EnhancedStreamInfrastructure =
        
        open PhysicalDataStructures
        open System.Threading.Channels
        
        // Add unique token ID for deduplication
        [<Struct>]
        type EnhancedStreamToken = {
            TokenId: uint64  // Unique identifier
            TokenType: JsonToken
            Value: TokenValue
            Path: string
            Depth: int
            IsComplete: bool
            MapIndex: int  // Which map this token is specifically for
        }
        
        // Enhanced channel with proper producer-consumer semantics
        type EnhancedStreamChannel = {
            Channel: Channel<EnhancedStreamToken>
            Writer: ChannelWriter<EnhancedStreamToken>
            Reader: ChannelReader<EnhancedStreamToken>
            IsCompleted: bool ref
            mutable ProcessedTokens: Set<uint64>  // Track processed token IDs
        }

        type EnhancedStreamingPool = {
            Plan: RMLPlan
            Channels: EnhancedStreamChannel[]
            ProcessingTasks: Task[]
            GlobalPredicateTable: PredicateTupleTable
            GlobalJoinTable: PredicateJoinTable
            Output: TripleOutputStream
        }
        
        // Global token ID generator
        let tokenIdGenerator = ref 0UL

        let generateTokenId () =
            System.Threading.Interlocked.Increment(tokenIdGenerator)
        
        // Create enhanced channel
        let createEnhancedStreamChannel () : EnhancedStreamChannel =
            let options = UnboundedChannelOptions()
            options.SingleReader <- true  // Only one consumer per channel
            options.SingleWriter <- false  // Multiple producers allowed
            let channel = Channel.CreateUnbounded<EnhancedStreamToken>(options)
            
            {
                Channel = channel
                Writer = channel.Writer
                Reader = channel.Reader
                IsCompleted = ref false
                ProcessedTokens = Set.empty
            }
        
        // Enhanced token enqueue with deduplication
        let enqueueEnhancedToken (channel: EnhancedStreamChannel) (token: EnhancedStreamToken) : bool =
            if not (Set.contains token.TokenId channel.ProcessedTokens) then
                let success = channel.Writer.TryWrite(token)
                if success then
                    channel.ProcessedTokens <- Set.add token.TokenId channel.ProcessedTokens
                success
            else
                false  // Already processed
        
        // Signal completion to enhanced channel
        let completeEnhancedChannel (channel: EnhancedStreamChannel) : unit =
            channel.IsCompleted := true
            channel.Writer.Complete()
        
        // Create single-consumer processing task for each channel
        let createChannelProcessor (mapIndex: int) (channel: EnhancedStreamChannel) (pool: StreamingPool) : Task =
            Task.Run(fun () ->
                task {
                    let plan = pool.Plan.OrderedMaps.[mapIndex]
                    
                    // Single consumer loop - each token processed exactly once
                    while not (!(channel.IsCompleted)) || channel.Reader.Count > 0 do
                        let! hasToken = channel.Reader.WaitToReadAsync()
                        
                        if hasToken then
                            while channel.Reader.TryRead() do
                                let token = channel.Reader.Current
                                
                                // Process only completion tokens for this specific map
                                if token.IsComplete && token.MapIndex = mapIndex then
                                    printfn "PROCESSING COMPLETION TOKEN %d for map %d (UNIQUE)" token.TokenId mapIndex
                                    
                                    let context = Dictionary<string, obj>()
                                    
                                    match token.Value with
                                    | CompletionData dict ->
                                        printfn "EXTRACTED COMPLETION DATA: %A with %d items" dict dict.Count
                                        for kvp in dict do
                                            context.[kvp.Key] <- kvp.Value
                                    | _ -> ()
                                    
                                    printfn "FINAL CONTEXT: %A" context
                                    printfn "PREDICATE TUPLES COUNT: %d" plan.PredicateTuples.Length
                                    
                                    // Process predicate tuples (this is where actual work happens)
                                    for tuple in plan.PredicateTuples do
                                        if addPredicateTuple pool.GlobalPredicateTable tuple context then
                                            flushIfNeeded pool.GlobalPredicateTable pool.Output context
                                    
                                    // Process join tuples
                                    for joinTuple in plan.JoinTuples do
                                        processPredicateTuple joinTuple.ParentTuple pool.Output context
                                        processPredicateTuple joinTuple.ChildTuple pool.Output context
                }
            )

    // Stream Processor
    module StreamProcessor =
        
        open EnhancedStreamInfrastructure
        open PhysicalDataStructures
        
        // Initialize streaming pool with channels for each dependency group
        let createEnhancedStreamingPool (triplesMaps: TriplesMap[]) (output: TripleOutputStream) : EnhancedStreamingPool =
            let plan = RMLPlanner.createRMLPlan triplesMaps
            let channels = Array.init plan.OrderedMaps.Length (fun _ -> createEnhancedStreamChannel())
            let globalPredicateTable = createPredicateTupleTable 1000
            let globalJoinTable = createPredicateJoinTable()
            
            // Create a single processing task per channel (one consumer per channel)
            let processingTasks = 
                channels
                |> Array.mapi (fun mapIndex channel ->
                    createChannelProcessor mapIndex channel 
                        {
                            Plan = plan
                            Channels = [||]  // Not used in new design
                            ProcessingTasks = [||]
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
        
        let processEnhancedJsonToken (pool: EnhancedStreamingPool) (tokenType: JsonToken) (value: obj) (currentPath: string) (accumulatedData: Dictionary<string, obj>) : unit =
            // Only process completion tokens to avoid unnecessary work
            if tokenType = JsonToken.EndObject || tokenType = JsonToken.EndArray then
                let tokenId = generateTokenId()
                
                printfn "Trying to match path: '%s'" currentPath
                
                // Route to appropriate channels based on path matching
                match FastMap.tryFind currentPath pool.Plan.PathToMaps with
                | ValueSome mapIndices ->
                    printfn "MATCH FOUND! Path '%s' matched indices: %A" currentPath mapIndices
                    
                    // Create ONE token per matching map (not shared tokens)
                    for mapIndex in mapIndices do
                        let channel = pool.Channels.[mapIndex]
                        
                        let enhancedToken = {
                            TokenId = tokenId + uint64 mapIndex  // Unique ID per map
                            TokenType = tokenType
                            Value = CompletionData(Dictionary<string, obj>(accumulatedData))
                            Path = currentPath
                            Depth = currentPath.Split('.').Length
                            IsComplete = true
                            MapIndex = mapIndex
                        }
                        
                        let enqueued = enqueueEnhancedToken channel enhancedToken
                        printfn "ENQUEUED TOKEN %d for map %d: %b" enhancedToken.TokenId mapIndex enqueued
                        
                | ValueNone -> 
                    printfn "NO MATCH for path: '%s'" currentPath

        // Process JSON token and route to appropriate streams
        [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
        let processJsonToken (pool: StreamingPool) (tokenType: JsonToken) (value: obj) (currentPath: string) (accumulatedData: Dictionary<string, obj>) : unit =
            let tokenValue = 
                match tokenType with
                | JsonToken.String -> 
                    StringValue(value :?> string)
                | JsonToken.Integer -> 
                    IntegerValue(value :?> int64)
                | JsonToken.Float -> 
                    FloatValue(value :?> float)
                | JsonToken.Boolean -> 
                    BooleanValue(value :?> bool)
                | JsonToken.Date -> 
                    DateTimeValue(value :?> DateTime)
                | JsonToken.EndObject | JsonToken.EndArray -> 
                    CompletionData(Dictionary<string, obj>(accumulatedData))
                | JsonToken.StartObject | JsonToken.StartArray | JsonToken.PropertyName ->
                    StructuralToken
                | _ -> 
                    StructuralToken  // Default for other tokens
            
            let token = {
                TokenType = tokenType
                Value = tokenValue
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

                    printfn "ENQUEUING TOKEN: Type=%A, IsComplete=%b, Value=%A" 
                        tokenType token.IsComplete value
                    
                    // Mark channel as started and enqueue token
                    if not channel.IsStarted then
                        channel.IsStarted <- true
                    
                    // For completion tokens, include accumulated data
                    let finalToken = 
                        if token.IsComplete then
                            printfn "COMPLETION TOKEN DATA: %A" accumulatedData  // DEBUG                
                            { token with Value = (CompletionData accumulatedData) }
                        else
                            token
                    
                    enqueueToken channel finalToken
            | ValueNone -> 
                printfn "NO MATCH for path: '%s'" currentPath

    // Main RML Stream Processor
    module RMLStreamProcessor =
        
        open StreamProcessor
        
        // In RMLStreamProcessor module, replace the existing function:
        let processRMLStreamEnhanced 
                (reader: JsonTextReader) 
                (triplesMaps: TriplesMap[]) 
                (output: TripleOutputStream) : Task<unit> =
            
            task {
                reader.SupportMultipleContent <- true
                reader.DateParseHandling <- DateParseHandling.None
                
                // Step 1: Initialize enhanced streaming pool
                let pool = EnhancedStreamProcessor.createEnhancedStreamingPool triplesMaps output
                
                // DEBUG: Print what paths are in the PathToMaps index
                printfn "=== PathToMaps Index ==="
                pool.Plan.PathToMaps
                |> FastMap.iter (fun path indices ->
                    printfn "Path: '%s' -> Indices: %A" path indices)
                printfn "========================"
                
                // Step 2: [Keep your existing JSON parsing logic exactly as is]
                let pathStack = ResizeArray<string>()
                let accumulatedData = Dictionary<string, obj>()
                let mutable currentProperty = ""
                let mutable isInArray = false
                let mutable arrayDepth = 0
                let contextStack = ResizeArray<bool * string * int>()
                
                let buildCurrentPath () =
                    if pathStack.Count = 0 then 
                        "$"
                    else
                        let basePath = "$." + String.Join(".", pathStack)
                        if isInArray then basePath + "[*]" else basePath
                
                try
                    // Step 3: Use enhanced token processing
                    while reader.Read() do
                        let currentPath = buildCurrentPath()
                        
                        match reader.TokenType with
                        | JsonToken.StartObject ->
                            printfn "StartObject - Path: '%s'" currentPath
                            // Only process structural tokens if needed
                            
                        | JsonToken.StartArray ->
                            contextStack.Add((isInArray, currentProperty, arrayDepth))
                            isInArray <- true
                            arrayDepth <- arrayDepth + 1
                            let arrayPath = buildCurrentPath()
                            printfn "StartArray - Path: '%s'" arrayPath
                            
                        | JsonToken.PropertyName ->
                            let propName = reader.Value :?> string
                            currentProperty <- propName
                            
                            if not isInArray then
                                pathStack.Add(propName)
                            
                            printfn "PropertyName: '%s', current path: '%s', isInArray: %b" propName (buildCurrentPath()) isInArray
                            
                        | JsonToken.EndObject ->
                            let endPath = buildCurrentPath()
                            printfn "EndObject - Path: '%s'" endPath
                            
                            // ONLY process completion tokens
                            EnhancedStreamProcessor.processEnhancedJsonToken pool reader.TokenType reader.Value endPath accumulatedData
                            
                            if not isInArray && pathStack.Count > 0 then
                                pathStack.RemoveAt(pathStack.Count - 1)
                            
                        | JsonToken.EndArray ->
                            let endPath = buildCurrentPath()
                            printfn "EndArray - Path: '%s'" endPath
                            
                            // ONLY process completion tokens
                            EnhancedStreamProcessor.processEnhancedJsonToken pool reader.TokenType reader.Value endPath accumulatedData
                            
                            // [Keep your existing cleanup logic]
                            if contextStack.Count > 0 then
                                let (prevIsArray, prevProperty, prevArrayDepth) = contextStack.[contextStack.Count - 1]
                                contextStack.RemoveAt(contextStack.Count - 1)
                                isInArray <- prevIsArray
                                currentProperty <- prevProperty
                                arrayDepth <- prevArrayDepth
                            else
                                isInArray <- false
                                arrayDepth <- 0
                            
                            if pathStack.Count > 0 then
                                pathStack.RemoveAt(pathStack.Count - 1)
                            
                            accumulatedData.Clear()
                            
                        | JsonToken.String | JsonToken.Integer | JsonToken.Float | JsonToken.Boolean | JsonToken.Date ->
                            if not (String.IsNullOrEmpty currentProperty) then
                                let typedValue = 
                                    match reader.TokenType with
                                    | JsonToken.String -> box (reader.Value :?> string)
                                    | JsonToken.Integer -> box (reader.Value :?> int64) 
                                    | JsonToken.Float -> box (reader.Value :?> float)
                                    | JsonToken.Boolean -> box (reader.Value :?> bool)
                                    | JsonToken.Date -> box (reader.Value :?> DateTime)
                                    | _ -> reader.Value
                                
                                accumulatedData.[currentProperty] <- typedValue
                                currentProperty <- ""
                            
                            printfn "Value token - Path: '%s', Value: %A" currentPath reader.Value
                            
                        | _ -> ()
                    
                    // Step 4: Signal completion to all enhanced channels
                    for channel in pool.Channels do
                        completeEnhancedChannel channel
                    
                    // Step 5: Wait for all processing to complete
                    do! Task.WhenAll(pool.ProcessingTasks)
                    
                    // Step 6: Final flush
                    PhysicalDataStructures.flushIfNeeded pool.GlobalPredicateTable output (Dictionary<string, obj>())
                    
                with
                | ex -> return failwithf "Error processing RML stream: %s" ex.Message
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
                
                do! processRMLStreamEnhanced reader triplesMaps output
            }