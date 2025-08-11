Architectural Analysis: Parallel RML Processing Pipeline
Executive Summary
Your RML processing pipeline demonstrates sophisticated engineering with its adaptive indexing, hierarchical string interning, and dependency-aware planning. The transition to a pluggable parallelism architecture is well-motivated, particularly given the distinct characteristics of RML workloads (mix of CPU-bound template expansion and I/O-bound data streaming). The key insight from your planner is that dependency groups naturally partition the workload into parallelizable units with well-defined synchronization boundaries.
Current Architecture Strengths

1. Dependency-Aware Planning
The DependencyGroups structure elegantly captures the parallelization opportunities:

Independent groups can execute fully in parallel
Within groups, maps with satisfied dependencies can run concurrently
The planner pre-computes all coordination points, eliminating runtime dependency analysis

2. Memory Efficiency

StringPool hierarchy minimizes allocation pressure
Compact tuple representations with bit flags
Lazy index construction based on actual usage patterns

3. Hot Path Optimization
The stack-based hot path execution demonstrates understanding of CPU cache dynamics and branch prediction.
Architectural Recommendations
Core Abstraction: The Execution Context
fsharp/// Core abstraction for pluggable execution strategies
type IExecutionStrategy<'TState, 'TMsg, 'TResult> =
    abstract member Schedule: ExecutionUnit<'TState, 'TMsg> -> ExecutionToken
    abstract member ScheduleBatch: ExecutionUnit<'TState, 'TMsg>[] -> ExecutionToken[]
    abstract member Coordinate: CoordinationPoint -> Async<unit>
    abstract member Complete: ExecutionToken -> Async<'TResult>
    abstract member CancelAll: unit -> unit

/// Represents a unit of work that can be scheduled
and ExecutionUnit<'TState, 'TMsg> = {
    UnitId: ExecutionUnitId
    GroupId: DependencyGroupId
    State: 'TState
    Process: 'TState -> 'TMsg -> 'TState * Effect<'TMsg> list
    Dependencies: ExecutionUnitId[]
}

/// Effects that execution units can produce
and Effect<'TMsg> =
    | EmitTriple of subject: StringId * predicate: StringId * object: StringId
    | SendMessage of target: ExecutionUnitId * message: 'TMsg
    | RequestJoin of joinSpec: JoinSpecification
    | Complete
Proposed API Structure
1. Pipeline Builder API
fsharpmodule Pipeline =
    
    /// Pipeline configuration
    type PipelineConfig = {
        Strategy: ExecutionStrategyType
        MaxParallelism: int
        ChunkSize: int
        BackpressureThreshold: int
        StringPoolConfig: StringPoolConfig
    }
    
    /// Main pipeline builder
    type PipelineBuilder = {
        Plan: RMLPlan
        Config: PipelineConfig
        InputStreams: IAsyncEnumerable<StreamToken> list
        OutputSinks: ITripleSink list
    }
    
    /// Fluent API for pipeline construction
    let create (plan: RMLPlan) =
        { Plan = plan
          Config = PipelineConfig.Default
          InputStreams = []
          OutputSinks = [] }
    
    let withStrategy strategy builder =
        { builder with Config = { builder.Config with Strategy = strategy } }
    
    let addInputStream stream builder =
        { builder with InputStreams = stream :: builder.InputStreams }
    
    let addOutputSink sink builder =
        { builder with OutputSinks = sink :: builder.OutputSinks }
    
    /// Build and return executable pipeline
    let build (builder: PipelineBuilder) : IPipeline =
        match builder.Config.Strategy with
        | Synchronous -> SynchronousPipeline(builder) :> IPipeline
        | AsyncTask -> AsyncTaskPipeline(builder) :> IPipeline
        | Hopac -> HopacPipeline(builder) :> IPipeline
2. Synchronous Baseline Implementation
fsharpmodule SynchronousExecution =
    
    type SynchronousStrategy() =
        let mutable executionQueue = Queue<ExecutionUnit<_,_>>()
        let mutable completedUnits = HashSet<ExecutionUnitId>()
        
        interface IExecutionStrategy<'TState, 'TMsg, 'TResult> with
            member _.Schedule unit =
                executionQueue.Enqueue(unit)
                ExecutionToken(unit.UnitId)
            
            member _.ScheduleBatch units =
                units |> Array.map (fun u -> 
                    executionQueue.Enqueue(u)
                    ExecutionToken(u.UnitId))
            
            member _.Coordinate point = async {
                // Synchronous coordination is immediate
                match point with
                | JoinPoint(parent, child) ->
                    // Direct lookup in shared state
                    ()
                | Barrier(units) ->
                    // Check all units completed
                    ()
            }
            
            member _.Complete token = async {
                // Process queue until token completes
                while not (completedUnits.Contains(token.UnitId)) do
                    if executionQueue.Count > 0 then
                        let unit = executionQueue.Dequeue()
                        // Process unit synchronously
                        processUnit unit
                        completedUnits.Add(unit.UnitId) |> ignore
                return Unchecked.defaultof<'TResult>
            }
3. Streaming Interface
fsharp/// Abstraction for data sources
type IDataStream =
    abstract member ReadAsync: unit -> Async<StreamToken option>
    abstract member Path: string
    abstract member Close: unit -> unit

/// Abstraction for triple output
type ITripleSink =
    abstract member WriteTriple: subject: string * predicate: string * object: string -> Async<unit>
    abstract member WriteTypedTriple: subject: string * predicate: string * object: string * datatype: string -> Async<unit>
    abstract member Flush: unit -> Async<unit>
    abstract member Close: unit -> Async<unit>

/// Stream coordinator for managing backpressure
type StreamCoordinator(config: BackpressureConfig) =
    let mutable pendingWrites = 0L
    let mutable totalReads = 0L
    
    member _.ShouldPauseReading() =
        pendingWrites > config.MaxPendingWrites
    
    member _.RecordRead() =
        Interlocked.Increment(&totalReads) |> ignore
    
    member _.RecordWrite() =
        Interlocked.Decrement(&pendingWrites) |> ignore
Parallelization Strategy
1. Group-Level Parallelism
fsharptype GroupExecutor<'TState, 'TMsg> = {
    GroupId: DependencyGroupId
    Maps: TriplesMapPlan[]
    SharedState: IGroupState<'TState>
    LocalStringPool: WorkerGroupPool
}

/// Execute independent groups in parallel
let executeGroups (plan: RMLPlan) (strategy: IExecutionStrategy<_,_,_>) =
    plan.DependencyGroups.GroupStarts
    |> Array.mapi (fun groupIdx _ ->
        let group = plan.DependencyGroups.GetGroup(groupIdx)
        let executor = createGroupExecutor plan groupIdx
        strategy.Schedule executor)
    |> strategy.ScheduleBatch
2. Map-Level Parallelism Within Groups
fsharp/// Fine-grained parallelism within dependency groups
let executeMapsConcurrently (group: GroupExecutor<_,_>) (strategy: IExecutionStrategy<_,_,_>) =
    let readyMaps = Queue<TriplesMapPlan>()
    let executing = HashSet<int>()
    let completed = HashSet<int>()
    
    let rec scheduleReady () =
        group.Maps
        |> Array.filter (fun map ->
            not (executing.Contains map.Index) &&
            not (completed.Contains map.Index) &&
            map.Dependencies |> Array.forall completed.Contains)
        |> Array.iter (fun map ->
            executing.Add(map.Index) |> ignore
            readyMaps.Enqueue(map)
            strategy.Schedule (createMapExecutor map group.SharedState))
    
    scheduleReady()
Testing Strategy
1. Model-Based Testing Foundation
fsharpmodule ModelBasedTests =
    
    /// Abstract model of pipeline execution
    type PipelineModel = {
        ProcessedTokens: Set<StreamToken>
        GeneratedTriples: Set<string * string * string>
        CompletedMaps: Set<int>
        StateInvariants: StateInvariant list
    }
    
    /// Properties that must hold regardless of execution strategy
    type PipelineProperty =
        | DeterministicOutput  // Same input produces same triples
        | NoDuplicateTriples    // Each triple generated exactly once
        | CompleteDependencies  // Dependencies complete before dependents
        | MemoryBounded        // Memory usage stays within limits
    
    /// Generate test scenarios
    let generateScenario (config: Gen<PipelineConfig>) : Property =
        gen {
            let! plan = Arb.generate<RMLPlan>
            let! tokens = Gen.listOf Arb.generate<StreamToken>
            let! strategy = Gen.elements [Synchronous; AsyncTask; Hopac]
            
            return testPipelineExecution plan tokens strategy
        }
        |> Prop.forAll
2. Differential Testing
fsharp/// Compare execution strategies for consistency
let differentialTest (plan: RMLPlan) (input: StreamToken list) =
    let strategies = [
        SynchronousStrategy() :> IExecutionStrategy<_,_,_>
        AsyncTaskStrategy() :> IExecutionStrategy<_,_,_>
        HopacStrategy() :> IExecutionStrategy<_,_,_>
    ]
    
    let results = 
        strategies
        |> List.map (fun strategy ->
            let pipeline = Pipeline.create plan 
                          |> Pipeline.withStrategy strategy
                          |> Pipeline.build
            pipeline.Execute input)
    
    // All strategies should produce identical output
    results |> List.pairwise |> List.forall (fun (a, b) -> 
        Set.ofList a = Set.ofList b)
Performance Considerations
1. Work Stealing for Load Balancing
fsharptype WorkStealingScheduler<'TWork>(workerCount: int) =
    let workers = Array.init workerCount (fun i -> 
        { WorkerId = WorkerId i
          LocalQueue = ConcurrentQueue<'TWork>()
          StealableQueue = ConcurrentQueue<'TWork>() })
    
    member _.TrySteal(fromWorker: WorkerId) : 'TWork option =
        workers.[fromWorker.Value].StealableQueue.TryDequeue()
2. Adaptive Chunking
fsharptype AdaptiveChunker(initialSize: int) =
    let mutable currentSize = initialSize
    let mutable throughputHistory = Queue<float>(10)
    
    member _.AdjustChunkSize(throughput: float) =
        throughputHistory.Enqueue(throughput)
        if throughputHistory.Count >= 10 then
            throughputHistory.Dequeue() |> ignore
            let avgThroughput = throughputHistory |> Seq.average
            // Adjust chunk size based on throughput trends
            if avgThroughput < targetThroughput * 0.8 then
                currentSize <- max 1 (currentSize / 2)
            elif avgThroughput > targetThroughput * 1.2 then
                currentSize <- min maxChunkSize (currentSize * 2)
Research References & Considerations

Dataflow Programming Models: The TPL Dataflow library patterns are highly relevant. Consider the TransformBlock/ActionBlock patterns for your execution units.
Join Calculus: Since you're considering Hopac, the join calculus formalism provides theoretical backing for your coordination points. See "The Reflexive CHAM and the Join-Calculus" (Fournet & Gonthier).
Stream Fusion: GHC's stream fusion optimizations could inspire your hot path processing. The paper "Stream Fusion: From Lists to Streams to Nothing at All" provides insights.
Differential Dataflow: Frank McSherry's work on differential dataflow could optimize incremental RML processing scenarios.
LMAX Disruptor Pattern: For high-throughput scenarios, the ring buffer pattern could replace channels between execution units.

Implementation Priority

Phase 1: Synchronous baseline with comprehensive model-based tests
Phase 2: Extract interface, implement async/Task backend
Phase 3: Implement Hopac backend with advanced coordination
Phase 4: Performance optimizations (work stealing, adaptive chunking)
Phase 5: Monitoring and observability layer

Key Design Decisions

StringId everywhere: Continue using StringId rather than strings in execution units to maintain memory efficiency.
Effect-based execution: The Effect discriminated union provides a functional approach to side effects that's easily testable.
Backpressure as first-class concern: Built into the StreamCoordinator to prevent memory exhaustion.
Group-local string pools: Maintains your StringPool hierarchy benefits while enabling parallel execution.

This architecture provides the pluggability you need while maintaining the performance characteristics of your current implementation. The synchronous baseline ensures correctness through testing, while the pluggable strategies allow performance optimization for different workload characteristics.