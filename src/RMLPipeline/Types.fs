namespace RMLPipeline

open System
open RMLPipeline.FastMap.Types
open Newtonsoft.Json

module Core =
    
    // Primary value type - similar to your MorkSharp Scalar but optimized for RML
    type RMLValue =
        | StringValue of string
        | IntValue of int64        // Use int64 to handle all integer types
        | FloatValue of double
        | BoolValue of bool
        | DateTimeValue of DateTime
        | DecimalValue of decimal
        | NullValue
        | ArrayValue of RMLValue array
        | ObjectValue of FastMap<string, RMLValue>

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

    // Type-safe context - replaces Dictionary<string, obj>
    [<Struct>]
    type Context = {
        Values: FastMap<string, RMLValue>
        Metadata: ContextMetadata
    }
    
    and ContextMetadata = {
        Path: string
        Depth: int
        Timestamp: int64
        SourceToken: JsonToken option
    }
    
    module Context =
        let empty = {
            Values = FastMap.empty
            Metadata = {
                Path = ""
                Depth = 0
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                SourceToken = None
            }
        }
        
        let create (path: string) (values: (string * RMLValue) list) = {
            Values = List.fold (fun acc (k, v) -> FastMap.add k v acc) FastMap.empty values
            Metadata = {
                Path = path
                Depth = path.Split('.').Length
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
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

    // Type-safe stream token
    [<Struct>]
    type TypedStreamToken = {
        TokenType: JsonToken
        Value: RMLValue
        Path: string
        Depth: int
        IsComplete: bool
        Context: Context option
    }
    
    module TypedStreamToken =
        let create tokenType value path isComplete context = {
            TokenType = tokenType
            Value = value
            Path = path
            Depth = path.Split('.').Length
            IsComplete = isComplete
            Context = context
        }
        
        (* let fromLegacyToken (legacyToken: StreamToken) : TypedStreamToken =
            let rmlValue = 
                RMLValue.TryParse(legacyToken.Value) 
                |> Option.defaultValue NullValue
            {
                TokenType = legacyToken.TokenType
                Value = rmlValue
                Path = legacyToken.Path
                Depth = legacyToken.Depth
                IsComplete = legacyToken.IsComplete
                Context = None
            } *)