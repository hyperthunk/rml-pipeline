namespace RMLPipeline

open System
open RMLPipeline.FastMap.Types
open Newtonsoft.Json

module Core =

    // State monad ish thing
    type State<'S, 'T> = State of ('S -> 'T * 'S)
    
    // thanks for the inspo, Haskell :)
    let runState (State f) state = f state
    let evalState (State f) state = fst (f state)
    let execState (State f) state = snd (f state)
    
    // State modification
    let get<'S> : State<'S, 'S> = State (fun s -> (s, s))
    let put<'S> (newState: 'S) : State<'S, unit> = State (fun _ -> ((), newState))
    let modify<'S> (f: 'S -> 'S) : State<'S, unit> = State (fun s -> ((), f s))
    
    // State computation expressions
    type StateBuilder() =
        member _.Return(x) = State (fun s -> (x, s))
        member _.ReturnFrom(m) = m
        member _.Bind(State f, k) = State (fun s -> 
            let (a, s') = f s
            let (State g) = k a
            g s')
        member _.Zero() = State (fun s -> ((), s))
        member _.Combine(State f, State g) = State (fun s ->
            let (_, s') = f s
            g s')
        member _.Delay(f) = State (fun s -> runState (f()) s)
        member _.For(seq, body) = 
            let folder state item = 
                let (_, newState) = runState (body item) state
                newState
            State (fun s -> ((), Seq.fold folder s seq))
        member _.While(guard, body) =
            let rec loop s =
                if guard() then
                    let (_, s') = runState body s
                    loop s'
                else ((), s)
            State loop

    let state = StateBuilder()

    // Lifting functions for convenience
    let lift f = State (fun s -> (f s, s))
    let lift2 f a b = state {
        let! x = a
        let! y = b
        return f x y
    }

    (* Scalar (e.g., attribute/column) value handling *)       
    type RMLValue =
        | StringValue of string
        | IntValue of int64        // Use int64 to handle all integer types
        | FloatValue of double
        | BoolValue of bool
        | DateTimeValue of DateTime
        | DecimalValue of decimal
        | ArrayValue of RMLValue array
        | ObjectValue of FastMap<string, RMLValue>
        | NullValue

        member this.IsNull =
            match this with NullValue -> true | _ -> false
            
        member this.AsString() =
            match this with
            | StringValue s -> s
            | IntValue i -> i.ToString()
            | FloatValue f -> f.ToString()
            | BoolValue b -> b.ToString().ToLowerInvariant()
            | DateTimeValue dt -> dt.ToString("yyyy-MM-ddTHH:mm:ss")
            | DecimalValue d -> d.ToString()
            | NullValue -> ""
            | ArrayValue _ -> "RMLValue Array"
            | ObjectValue _ -> "RMLValue Map"
            
        member this.TryAsString() =
            try Some (this.AsString()) with _ -> None
            
        static member TryParse(value: obj) : RMLValue option =
            match value with
            | null -> Some NullValue
            | :? string as s -> Some (StringValue s)
            | :? int as i -> Some (IntValue (int64 i))
            | :? int64 as i -> Some (IntValue i)
            | :? float as f -> Some (FloatValue f)
            | :? bool as b -> Some (BoolValue b)
            | :? DateTime as dt -> Some (DateTimeValue dt)
            | :? decimal as d -> Some (DecimalValue d)
            | _ -> None

    let rmlValueOf<'t> (value: 't) : RMLValue option =
        match box value with
        // | :? array<byte> as bytes -> BinValue bytes |> Some
        | :? bool as b -> BoolValue b |> Some
        | :? string as s -> StringValue s |> Some
        | :? int as i -> IntValue i |> Some
        | :? int64 as l -> IntValue l |> Some
        | :? float as f -> FloatValue f |> Some
        | :? decimal as d -> DecimalValue d |> Some
        | _ -> None

    (* RML String Interning Types *)
    
    [<Struct>]
    type StringId = StringId of int64
        with 
        member inline this.Value = 
            let (StringId id) = this in int32 (id &&& 0xFFFFFFFFL)
        
        static member op_Explicit(id: StringId) = id.Value

        member inline this.IsValid = this.Value >= 0

        member inline this.Temperature = 
            let (StringId id) = this in int32 (id >>> 32)
        
        /// Increment temperature by one. NB: wraps to zero if we overflow int32
        member inline this.IncrementTemperature() : StringId =
            let (StringId id) = this
            // Atomic increment of temperature bits
            let newId = id + (1L <<< 32)
            if int32 (newId >>> 32) < 0 then
                // If we've overflowed, reset temperature to 1
                // NB: to keep this inline friendly we have to repeat outselves...
                StringId (id &&& 0xFFFFFFFFL ||| (1L <<< 32))         
            else
                StringId newId
        
        member inline this.WithTemperature(temp: int32) =
            let (StringId id) = this
            let maskedId = id &&& 0xFFFFFFFFL
            StringId (maskedId ||| (int64 temp <<< 32))
        
        static member inline Create(id: int32) : StringId = 
            StringId (int64 id)
        
        static member Invalid = StringId -1L

    (* RML Planning Types *)

    [<Struct>]
    type Context = {
        Values: FastMap<string, RMLValue>
        Metadata: ContextMetadata
    }
    
    and ContextMetadata = {
        Path: string
        Depth: int
        // Timestamp: int64
        SourceToken: JsonToken option
    }
    
    module Context =
        let empty = {
            Values = FastMap.empty
            Metadata = {
                Path = ""
                Depth = 0
                // Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                SourceToken = None
            }
        }
        
        let create (path: string) (values: (string * RMLValue) list) = {
            Values = List.fold (fun acc (k, v) -> FastMap.add k v acc) FastMap.empty values
            Metadata = {
                Path = path
                Depth = path.Split('.').Length
                // Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                SourceToken = None
            }
        }
        
        let tryFind (key: string) (context: Context) : RMLValue option =
            FastMap.tryFind key context.Values |> Option.ofValueOption
            
        let add (key: string) (value: RMLValue) (context: Context) : Context =
            { context with Values = FastMap.add key value context.Values }

        let merge (ctx1: Context) (ctx2: Context) : Context =
            let mergedValues = 
                FastMap.fold (fun acc k v -> FastMap.add k v acc) ctx1.Values ctx2.Values
            { ctx1 with Values = mergedValues }
            
        let toStringMap (context: Context) : FastMap<string, string> =
            context.Values
            |> FastMap.fold (fun acc k value -> FastMap.add k (value.AsString()) acc) FastMap.empty
    
    (* Template Expansion Types *)

    // Template expansion errors
    type TemplateError =
        | MissingPlaceholder of placeholder: string
        | InvalidValueType of placeholder: string * expected: string * actual: string
        | TemplateParseError of template: string * error: string
    
    // Template expansion result
    type TemplateResult =
        | Success of string
        | Error of TemplateError
    