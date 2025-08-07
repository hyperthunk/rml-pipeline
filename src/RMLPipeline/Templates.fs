namespace RMLPipeline

module Templates =

    open RMLPipeline.FastMap.Types
    open RMLPipeline.Core
    open System
    open System.Text

    // Pre-compiled for performance
    module private Constants =
        let OpenBrace = '{'
        let CloseBrace = '}'
        let Backslash = '\\'

    // RML-compliant template parser
    module private Parser =
        
        type ParseResult = 
            | Literal of string
            | Reference of string

        let inline isValidReferenceChar (c: char) =
            Char.IsLetterOrDigit(c) || c = '_' || c = '-' || c = '.'

        // Tail-recursive parser that handles escaping correctly
        let parseTemplate (template: string) =
            if String.IsNullOrEmpty(template) then
                [Literal ""]
            else
                let length = template.Length
                
                let rec parseChars (pos: int) (currentLiteral: char list) (acc: ParseResult list) =
                    if pos >= length then
                        // End of input - flush any remaining literal
                        if List.isEmpty currentLiteral then
                            List.rev acc
                        else
                            List.rev (Literal (String(currentLiteral |> List.rev |> Array.ofList)) :: acc)
                    else
                        let c = template.[pos]
                        
                        if c = Constants.Backslash && pos + 1 < length then
                            let nextChar = template.[pos + 1]
                            if nextChar = Constants.OpenBrace || nextChar = Constants.CloseBrace || nextChar = Constants.Backslash then
                                // Valid escape sequence - add escaped character to literal
                                parseChars (pos + 2) (nextChar :: currentLiteral) acc
                            else
                                // Not a valid escape, treat backslash as literal
                                parseChars (pos + 1) (c :: currentLiteral) acc
                        
                        elif c = Constants.OpenBrace then
                            // Found opening brace - flush literal and parse reference
                            let accWithLiteral = 
                                if List.isEmpty currentLiteral then
                                    acc
                                else
                                    Literal (String(currentLiteral |> List.rev |> Array.ofList)) :: acc
                            
                            parseReference (pos + 1) [] accWithLiteral
                        
                        else
                            // Regular character - add to current literal
                            parseChars (pos + 1) (c :: currentLiteral) acc

                and parseReference (pos: int) (refChars: char list) (acc: ParseResult list) =
                    if pos >= length then
                        // Unclosed reference - treat the whole thing as literal
                        let literalChars = Constants.OpenBrace :: (List.rev refChars)
                        let literal = String(literalChars |> Array.ofList)
                        parseChars pos [literal.[literal.Length - 1]] (Literal (literal.Substring(0, literal.Length - 1)) :: acc)
                    else
                        let c = template.[pos]
                        
                        if c = Constants.Backslash && pos + 1 < length then
                            let nextChar = template.[pos + 1]
                            if nextChar = Constants.CloseBrace || nextChar = Constants.OpenBrace || nextChar = Constants.Backslash then
                                // Valid escape in reference
                                parseReference (pos + 2) (nextChar :: refChars) acc
                            else
                                // Invalid escape in reference - treat as malformed
                                let literalChars = Constants.OpenBrace :: (List.rev refChars) @ [c]
                                let literal = String(literalChars |> Array.ofList)
                                parseChars (pos + 1) [] (Literal literal :: acc)
                        
                        elif c = Constants.CloseBrace then
                            // Found closing brace
                            if List.isEmpty refChars then
                                // Empty reference {} - treat as literal
                                parseChars (pos + 1) [Constants.CloseBrace; Constants.OpenBrace] acc
                            else
                                let refName = String(refChars |> List.rev |> Array.ofList)
                                parseChars (pos + 1) [] (Reference refName :: acc)
                        
                        elif isValidReferenceChar c then
                            // Valid reference character
                            parseReference (pos + 1) (c :: refChars) acc
                        
                        else
                            // Invalid character in reference - treat as malformed literal
                            let literalChars = Constants.OpenBrace :: (List.rev refChars) @ [c]
                            let literal = String(literalChars |> Array.ofList)
                            parseChars (pos + 1) [] (Literal literal :: acc)
                
                parseChars 0 [] []

    // IRI-safe encoding according to RFC 3987
    module private IRIEncoding =
        
        let needsEncoding (c: char) =
            match c with
            | ' ' | '<' | '>' | '"' | '{' | '}' | '|' | '\\' | '^' | '`' -> true
            | _ when int c < 32 || int c > 126 -> true
            | _ -> false
        
        let encodeForIRI (value: string) =
            if String.IsNullOrEmpty(value) then
                value
            else
                let rec checkNeedsEncoding (pos: int) =
                    if pos >= value.Length then
                        false
                    elif needsEncoding value.[pos] then
                        true
                    else
                        checkNeedsEncoding (pos + 1)
                
                if not (checkNeedsEncoding 0) then
                    value
                else
                    let rec encodeChars (pos: int) (acc: char list) =
                        if pos >= value.Length then
                            String(acc |> List.rev |> Array.ofList)
                        else
                            let c = value.[pos]
                            if needsEncoding c then
                                let encoded = sprintf "%%%02X" (int c)
                                let encodedChars = encoded.ToCharArray() |> Array.toList |> List.rev
                                encodeChars (pos + 1) (encodedChars @ acc)
                            else
                                encodeChars (pos + 1) (c :: acc)
                    
                    encodeChars 0 []

    // Tail-recursive template expansion
    let expandTemplate (template: string) (context: Context) : TemplateResult =
        if String.IsNullOrEmpty template then
            Success ""
        else
            try
                let parts = Parser.parseTemplate template                
                let rec expandParts (remaining: Parser.ParseResult list) (acc: string list) =
                    match remaining with
                    | [] -> Success (String.Concat(List.rev acc))                    
                    | Parser.Literal text :: rest -> expandParts rest (text :: acc)                    
                    | Parser.Reference refName :: rest ->
                        match Context.tryFind refName context with
                        | Some value ->
                            match value with
                            | NullValue ->
                                // RML spec: if any reference is NULL, return NULL
                                // TODO: this should account for the reference forumulation!
                                expandParts rest ("" :: acc)
                            | _ -> 
                                // TODO: this should account for the reference forumulation!
                                match value.TryAsString() with
                                | Some stringValue ->
                                    expandParts rest (stringValue :: acc)
                                | None ->
                                    Error (InvalidValueType(refName, "string", value.GetType().Name))
                        | None ->
                            Error (MissingPlaceholder refName)                
                expandParts parts []                
            with ex ->
                Error (TemplateParseError(template, ex.Message))

    // Tail-recursive template expansion with IRI encoding
    let expandTemplateAsIRI (template: string) (context: Context) : TemplateResult =
        if String.IsNullOrEmpty(template) then
            Success ""
        else
            try
                let parts = Parser.parseTemplate template
                
                let rec expandPartsAsIRI (remaining: Parser.ParseResult list) (acc: string list) =
                    match remaining with
                    | [] -> 
                        Success (String.Concat(List.rev acc))
                    
                    | Parser.Literal text :: rest ->
                        let encodedText = IRIEncoding.encodeForIRI text
                        expandPartsAsIRI rest (encodedText :: acc)
                    
                    | Parser.Reference refName :: rest ->
                        match Context.tryFind refName context with
                        | Some value ->
                            match value with
                            | NullValue ->
                                Error (MissingPlaceholder refName)
                            | _ ->
                                match value.TryAsString() with
                                | Some stringValue ->
                                    let encodedValue = IRIEncoding.encodeForIRI stringValue
                                    expandPartsAsIRI rest (encodedValue :: acc)
                                | None ->
                                    Error (InvalidValueType(refName, "string", value.GetType().Name))
                        | None ->
                            Error (MissingPlaceholder refName)
                
                expandPartsAsIRI parts []
                
            with ex ->
                Error (TemplateParseError(template, ex.Message))

    // Safe template expansion with fallback
    let expandTemplateWithFallback (template: string) (context: Context) (fallback: string) : string =
        match expandTemplate template context with
        | Success result -> result
        | Error _ -> fallback
    
    // Batch template expansion for performance using tail recursion
    let expandTemplatesBatch (templates: string array) (contexts: Context array) : TemplateResult array =
        let pairs = Array.allPairs templates contexts
        let results = Array.zeroCreate pairs.Length
        
        let rec expandPairs (index: int) =
            if index >= pairs.Length then
                results
            else
                let (template, context) = pairs.[index]
                results.[index] <- expandTemplate template context
                expandPairs (index + 1)
        
        expandPairs 0