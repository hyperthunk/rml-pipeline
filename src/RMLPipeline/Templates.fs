namespace RMLPipeline

module Templates =

    open RMLPipeline.FastMap.Types
    open RMLPipeline.Core

    // Type-safe template expansion
    let expandTemplate (template: string) (context: Context) : TemplateResult =
        if not (template.Contains("{")) then
            Success template
        else
            try
                let mutable result = template
                let mutable errors = []
                
                // Find all placeholders
                let placeholderRegex = System.Text.RegularExpressions.Regex(@"\{([^}]+)\}")
                let matches = placeholderRegex.Matches(template)
                
                for match_ in matches do
                    let placeholder = match_.Groups.[1].Value
                    let fullPlaceholder = match_.Value
                    
                    match Context.tryFind placeholder context with
                    | Some value ->
                        match value.TryAsString() with
                        | Some stringValue ->
                            result <- result.Replace(fullPlaceholder, stringValue)
                        | None ->
                            errors <- InvalidValueType(placeholder, "string", value.GetType().Name) :: errors
                    | None ->
                        errors <- MissingPlaceholder placeholder :: errors
                
                match errors with
                | [] -> Success result
                | firstError :: _ -> Error firstError
                
            with ex ->
                Error (TemplateParseError(template, ex.Message))
    
    // Safe template expansion with fallback
    let expandTemplateWithFallback (template: string) (context: Context) (fallback: string) : string =
        match expandTemplate template context with
        | Success result -> result
        | Error _ -> fallback
    
    // Batch template expansion for performance
    let expandTemplatesBatch (templates: string array) (contexts: Context array) : TemplateResult array =
        Array.allPairs templates contexts
        |> Array.map (fun (template, context) -> expandTemplate template context)