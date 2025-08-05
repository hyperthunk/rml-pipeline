namespace RMLPipeline

open System
open RMLPipeline.FastMap.Types
open Newtonsoft.Json

module Core =
       
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

    // Type-safe context - replaces Dictionary<string, obj>
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

    // Type-safe stream token
    [<Struct>]
    type StreamToken = {
        TokenType: JsonToken
        Value: RMLValue
        Path: string
        Depth: int
        IsComplete: bool
        Context: Context option
    }
    
    let mkStreamToken tokenType value path isComplete context = {
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

    // Template expansion errors
    type TemplateError =
        | MissingPlaceholder of placeholder: string
        | InvalidValueType of placeholder: string * expected: string * actual: string
        | TemplateParseError of template: string * error: string
    
    // Template expansion result
    type TemplateResult =
        | Success of string
        | Error of TemplateError
    

    // Enhanced predicate tuple with type safety
    [<Struct>]
    type PredicateTuple = {
        SubjectTemplate: string
        PredicateValue: string
        ObjectTemplate: string
        ObjectDatatype: XsdType option
        ObjectLanguage: string option
        IsConstant: bool
        SourcePath: string
        Hash: uint64
        ExpectedValueTypes: RMLValueType Set  // What types we expect in the context
    }
    
    and RMLValueType =
        | StringType
        | IntType  
        | FloatType
        | BoolType
        | DateTimeType
        | DecimalType
        | ArrayType of RMLValueType
        | ObjectType
    
    and XsdType =
        | XsdString
        | XsdInt
        | XsdFloat
        | XsdBoolean
        | XsdDateTime
        | XsdDecimal
        | XsdDouble
        | XsdAnyUri
        | XsdDate
        | XsdTime
        | XsdDuration
        | XsdBase64Binary
        | XsdHexBinary
        | XsdAnyType
    
    // Type-safe triple generation
    type TripleGenerationResult =
        | TripleGenerated of subject: string * predicate: string * object: string * datatype: XsdType option
        | TripleGenerationError of error: TemplateError