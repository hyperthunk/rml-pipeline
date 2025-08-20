namespace RMLPipeline.Internal

module Streaming =

    open RMLPipeline.FastMap.Types
    open RMLPipeline.Core
    open Newtonsoft.Json
    open System.Threading.Channels

    [<Struct>]
    type WorkerId = WorkerId of int
        with 
        member this.Value = let (WorkerId v) = this in v
        override this.ToString() = $"Worker-{this.Value}"

    [<Struct>]
    type ChannelId = ChannelId of int
    
    type StreamToken = {
        TokenType: JsonToken
        Value: RMLValue  // Type-safe instead of obj
        Path: string
        Depth: int
        IsComplete: bool
    }

    type CoordinationData<'TData> =
        | JoinRequest of parentPath: string * childPath: string * joinCondition: string
        | DependencyComplete of workerId: WorkerId * completedPath: string
        | StateSync of state: 'TData

    type WorkerMessage<'TData> =
        | ProcessData of StreamToken * 'TData
        | CoordinationRequest of requestId: uint64 * coordinationData: CoordinationData<'TData>
        | PoisonPill
        | Heartbeat
    
    type WorkerResponse<'TResult> =
        | DataProcessed of workerId: WorkerId * result: ProcessingResult<'TResult>
        | CoordinationResponse of workerId: WorkerId * requestId: uint64 * response: CoordinationResult<'TResult>
        | WorkerCompleted of workerId: WorkerId * finalStats: WorkerStats
        | WorkerError of workerId: WorkerId * error: WorkerError
    and ProcessingResult<'TResult> =
        | TriplesGenerated of triples: (string * string * string) list
        | ContextUpdated of newContext: Context
        | ForwardToWorker of targetWorkerId: WorkerId * message: WorkerMessage<'TResult>
        | DataForwarded of data: 'TResult
        | NoAction
    and CoordinationResult<'TResult> =
        | JoinDataReady of joinedContext: Context
        | DependencyAcknowledged
        | StateUpdated of newState: 'TResult
    and WorkerStats = {
        MessagesProcessed: int64
        TriplesGenerated: int64
        ProcessingTicksElapsed: int64  // Use Environment.TickCount64 instead of DateTime
    }
    and WorkerError =
        | ProcessingError of message: string * exn: exn option
        | ChannelError of message: string
        | CoordinationTimeout of requestId: uint64

    // Type-safe worker finite state machine
    type WorkerFSM<'TState, 'TData, 'TResult> = {
        WorkerId: WorkerId
        CurrentState: 'TState
        StateTransition: 'TState -> WorkerMessage<'TData> -> 'TState * WorkerResponse<'TResult> list
        IsTerminalState: 'TState -> bool
    }

    // Channel infrastructure with type safety
    type WorkerChannel<'TData, 'TResult> = {
        Inbox: ChannelWriter<WorkerMessage<'TData>>
        Outbox: ChannelReader<WorkerResponse<'TResult>>
        IsCompleted: bool
    }

    type ChannelOptions = {
        Capacity: int
        AllowSynchronousContinuations: bool
        SingleReader: bool
        SingleWriter: bool
    }

    // Path-based routing (supporting existing Pipeline.fs use case)
    type PathRoutingTable = {
        PathToWorkers: FastMap<string, WorkerId array>
        WorkerPaths: FastMap<WorkerId, string array>
    }