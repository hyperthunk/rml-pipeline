namespace RMLPipeline.Tests

open Expecto
open FsCheck
open System
open System.Collections.Generic
open Newtonsoft.Json
open RMLPipeline
open RMLPipeline.Core
open RMLPipeline.Templates
open RMLPipeline.FastMap.Types

module TemplateTests =

    // Custom generators for RMLValue types
    module Generators =
        
        let genSafeString = 
            Gen.elements ["hello"; "world"; "test"; "value"; ""; "123"; "test_value"; "a-b-c"]
            
        let genLargeString =
            Gen.choose (100, 1000)
            |> Gen.map (fun length -> String.replicate length "x")
            
        let genStringWithSpecialChars =
            Gen.elements ["hello world"; "test@example.com"; "http://example.com"; "{placeholder}"; "multi\nline"; "unicode: éñ"]
            
        let genAllStrings = Gen.oneof [genSafeString; genLargeString; genStringWithSpecialChars]

        let genInt64 : Gen<int64> = 
            Gen.choose (Int32.MinValue, Int32.MaxValue)
            |> Gen.map int64

        let genFloat : Gen<float> =
            let fmin : int = -1000
            let fmax : int = 1000
            Gen.oneof [
                Gen.map (fun (x: int) -> float x) (Gen.choose (fmin, fmax))
                Gen.constant Double.NaN
                Gen.constant Double.PositiveInfinity
                Gen.constant Double.NegativeInfinity
                Gen.constant 0.0
            ]
            
        let genDecimal = 
            // Use a safe range for decimal generation
            Gen.oneof [
                Gen.choose (-1000000, 1000000) |> Gen.map decimal
                Gen.choose (0, 999) |> Gen.map (fun x -> decimal x / 100m)  // Generate fractional decimals
                Gen.constant 0m
                Gen.constant 1m
                Gen.constant -1m
                Gen.constant 123.456m
            ]
            
        let genDateTime = 
            Gen.oneof [
                Gen.constant DateTime.MinValue
                Gen.constant DateTime.MaxValue
                Gen.constant (DateTime(2023, 1, 1))
                Gen.constant DateTime.UtcNow
                Gen.constant (DateTime(2020, 6, 15, 10, 30, 0))
                Gen.constant (DateTime(2025, 12, 31, 23, 59, 59))
                Gen.constant (DateTime(2022, 3, 15, 14, 30, 45))
                Gen.constant (DateTime(2024, 7, 4, 9, 15, 20))
                Gen.constant (DateTime(2021, 11, 28, 18, 45, 10))
                // Generate by adding random days to a base date
                Gen.choose (0, 365 * 10)  // 10 years worth of days
                |> Gen.map (fun days -> DateTime(2020, 1, 1).AddDays(float days))
            ]
            
        let genBool = Gen.elements [true; false]
        
        let rec genRMLValue maxDepth =
            if maxDepth <= 0 then
                Gen.oneof [
                    genAllStrings |> Gen.map StringValue
                    genInt64 |> Gen.map IntValue
                    genFloat |> Gen.map FloatValue
                    genBool |> Gen.map BoolValue
                    genDateTime |> Gen.map DateTimeValue
                    genDecimal |> Gen.map DecimalValue
                    Gen.constant NullValue
                ]
            else
                Gen.oneof [
                    genAllStrings |> Gen.map StringValue
                    genInt64 |> Gen.map IntValue
                    genFloat |> Gen.map FloatValue
                    genBool |> Gen.map BoolValue
                    genDateTime |> Gen.map DateTimeValue
                    genDecimal |> Gen.map DecimalValue
                    Gen.constant NullValue
                    Gen.listOfLength 3 (genRMLValue (maxDepth - 1)) |> Gen.map (List.toArray >> ArrayValue)
                    Gen.listOfLength 3 (Gen.zip genSafeString (genRMLValue (maxDepth - 1)))
                        |> Gen.map (fun pairs -> 
                            pairs 
                            |> List.fold (fun acc (k, v) -> FastMap.add k v acc) FastMap.empty
                            |> ObjectValue)
                ]
        
        let genRMLValueShallow = genRMLValue 2
        let genRMLValueDeep = genRMLValue 5
        
        let genContext = 
            Gen.listOf (Gen.zip genSafeString genRMLValueShallow)
            |> Gen.map (fun pairs -> Context.create "$.test" pairs)
            
        let genContextWithPlaceholders placeholders =
            placeholders
            |> List.map (fun placeholder -> Gen.map (fun value -> (placeholder, value)) genRMLValueShallow)
            |> Gen.sequence
            |> Gen.map (fun pairs -> Context.create "$.test" pairs)
            
        let genTemplate = 
            Gen.oneof [
                Gen.constant "simple template"
                Gen.constant "Hello {name}!"
                Gen.constant "{value}"
                Gen.constant "prefix_{id}_suffix"
                Gen.constant "{first} {last}"
                Gen.constant "No placeholders here"
                Gen.constant ""
                Gen.constant "{missing_placeholder}"
                Gen.constant "Multiple {a} and {b} placeholders"
                Gen.constant "Nested {{not_a_placeholder}}"
                Gen.constant "{unclosed_placeholder"
                Gen.constant "closed_but_not_opened}"
            ]

    // Arbitraries for FsCheck
    type TestArbitraries =
        static member RMLValue() = 
            Arb.fromGen Generators.genRMLValueShallow
            
        static member Context() = 
            Arb.fromGen Generators.genContext
            
        static member Template() = 
            Arb.fromGen Generators.genTemplate

    // Test data for specific scenarios
    let testData = [
        ("simple string", StringValue "hello", "hello")
        ("integer", IntValue 42L, "42")
        ("float", FloatValue 3.14, "3.14")
        ("boolean true", BoolValue true, "true")
        ("boolean false", BoolValue false, "false")
        ("null", NullValue, "")
        ("datetime", DateTimeValue (DateTime(2023, 1, 1, 12, 0, 0)), "2023-01-01T12:00:00")
        ("decimal", DecimalValue 123.45m, "123.45")
    ]

    // Helper functions for JSON parsing simulation
    let simulateJsonParsing (jsonString: string) : (string * obj) list =
        try
            let jsonObj = JsonConvert.DeserializeObject<Dictionary<string, obj>>(jsonString)
            jsonObj |> Seq.map (fun kvp -> kvp.Key, kvp.Value) |> Seq.toList
        with
        | _ -> []

    let convertJsonValueToRMLValue (value: obj) : RMLValue option =
        match value with
        | null -> Some NullValue
        | :? string as s -> Some (StringValue s)
        | :? int64 as i -> Some (IntValue i)
        | :? float as f -> Some (FloatValue f)
        | :? double as d -> Some (FloatValue d)
        | :? bool as b -> Some (BoolValue b)
        | :? DateTime as dt -> Some (DateTimeValue dt)
        | :? decimal as d -> Some (DecimalValue d)
        | _ -> None

    let createJsonTestContext (jsonString: string) : Context option =
        try
            let pairs = simulateJsonParsing jsonString
            let rmlPairs = 
                pairs
                |> List.choose (fun (k, v) -> 
                    convertJsonValueToRMLValue v
                    |> Option.map (fun rmlV -> (k, rmlV)))
            if rmlPairs.Length > 0 then
                Some (Context.create "$.test" rmlPairs)
            else
                None
        with
        | _ -> None

    // Property-based tests
    let propertyTests = [
        testProperty "RMLValue AsString never throws" <| fun (value: RMLValue) ->
            try
                let _ = value.AsString()
                true
            with
            | _ -> false

        testProperty "RMLValue TryAsString is safe" <| fun (value: RMLValue) ->
            match value.TryAsString() with
            | Some _ -> true
            | None -> true

        testProperty "RMLValue.TryParse round trip for supported types" <| fun (s: string) (i: int64) (f: float) (b: bool) ->
            let stringResult = RMLValue.TryParse(s) |> Option.map (_.AsString())
            let intResult = RMLValue.TryParse(i) |> Option.map (_.AsString())
            let floatResult = RMLValue.TryParse(f) |> Option.map (_.AsString())
            let boolResult = RMLValue.TryParse(b) |> Option.map (_.AsString())
            
            stringResult.IsSome && intResult.IsSome && floatResult.IsSome && boolResult.IsSome

        testProperty "Context operations are pure" <| fun (key: string) (value: RMLValue) ->
            let ctx1 = Context.empty
            let ctx2 = Context.add key value ctx1
            let ctx3 = Context.add key value ctx2
            
            Context.tryFind key ctx2 = Context.tryFind key ctx3

        testProperty "Context merge is associative" <| fun (ctx1: Context) (ctx2: Context) (ctx3: Context) ->
            let merged1 = Context.merge (Context.merge ctx1 ctx2) ctx3
            let merged2 = Context.merge ctx1 (Context.merge ctx2 ctx3)
            
            // Compare the values maps
            let values1 = merged1.Values
            let values2 = merged2.Values
            FastMap.keys values1 = FastMap.keys values2 &&
            FastMap.fold (
                fun acc k v -> 
                    acc && Context.tryFind k merged1 = Context.tryFind k merged2) true values1

        testProperty "Template expansion with empty context never throws" <| fun (template: string) ->
            try
                let result = expandTemplate template Context.empty
                match result with
                | Success _ -> true
                | Error _ -> true
            with
            | _ -> false

        testProperty "Template expansion with matching context succeeds" <| fun (placeholder: string) (value: RMLValue) ->
            let template = sprintf "{%s}" placeholder
            let context = Context.create "test" [(placeholder, value)]
            
            match expandTemplate template context with
            | Success result -> 
                match value.TryAsString() with
                | Some expected -> result = expected
                | None -> true // If value can't convert to string, expansion should still succeed with some representation
            | Error _ -> 
                // Error is acceptable if value can't be converted to string
                value.TryAsString().IsNone

        testProperty "Template without placeholders returns original" <| fun (template: string) ->
            not (template.Contains("{")) ==> lazy (
                match expandTemplate template Context.empty with
                | Success result -> result = template
                | Error _ -> false
            )
    ]

    // Specific test cases
    let specificTests = [
        testCase "RMLValue AsString basic types" <| fun _ ->
            testData
            |> List.iter (fun (name, value, expected) ->
                let actual = value.AsString()
                Expect.equal actual expected (sprintf "Failed for %s" name))

        testCase "Context creation and retrieval" <| fun _ ->
            let context = Context.create "$.test" [
                ("name", StringValue "John")
                ("age", IntValue 30L)
                ("active", BoolValue true)
            ]
            
            Expect.equal (Context.tryFind "name" context) (Some (StringValue "John")) "Should find name"
            Expect.equal (Context.tryFind "age" context) (Some (IntValue 30L)) "Should find age"
            Expect.equal (Context.tryFind "active" context) (Some (BoolValue true)) "Should find active"
            Expect.equal (Context.tryFind "missing" context) None "Should not find missing"

        testCase "Template expansion success cases" <| fun _ ->
            let context = Context.create "$.test" [
                ("name", StringValue "Alice")
                ("count", IntValue 5L)
                ("price", FloatValue 19.99)
            ]
            
            let testCases = [
                ("Hello {name}!", "Hello Alice!")
                ("{count} items", "5 items")
                ("Price: ${price}", "Price: $19.99")
                ("No placeholders", "No placeholders")
                ("", "")
            ]
            
            testCases
            |> List.iter (fun (template, expected) ->
                match expandTemplate template context with
                | Success actual -> Expect.equal actual expected (sprintf "Template: %s" template)
                | Error err -> failtest (sprintf "Unexpected error for template '%s': %A" template err))

        testCase "Template expansion error cases" <| fun _ ->
            let context = Context.create "$.test" [("existing", StringValue "value")]
            
            let errorCases = [
                ("Hello {missing}!", MissingPlaceholder "missing")
            ]
            
            errorCases
            |> List.iter (fun (template, expectedError) ->
                match expandTemplate template context with
                | Success result -> failtest (sprintf "Expected error for template '%s', got: %s" template result)
                | Error actualError -> 
                    match actualError, expectedError with
                    | MissingPlaceholder actual, MissingPlaceholder expected -> 
                        Expect.equal actual expected "Should have correct missing placeholder"
                    | _ -> failtest (sprintf "Wrong error type for template '%s': %A" template actualError))

        testCase "Template expansion with fallback" <| fun _ ->
            let context = Context.empty
            let template = "Hello {missing}!"
            let fallback = "Default message"
            
            let result = expandTemplateWithFallback template context fallback
            Expect.equal result fallback "Should return fallback for missing placeholder"

        testCase "Batch template expansion" <| fun _ ->
            let templates = [|"Hello {name}!"; "Age: {age}"; "{missing}"|]
            let contexts = [|
                Context.create "$.test1" [("name", StringValue "Alice")]
                Context.create "$.test2" [("age", IntValue 25L)]
                Context.create "$.test3" [("other", StringValue "value")]
            |]
            
            let results = expandTemplatesBatch templates contexts
            
            Expect.equal results.Length (templates.Length * contexts.Length) "Should have all combinations"
            
            // Check specific combinations
            let successCount = 
                results 
                |> Array.filter (function Success _ -> true | Error _ -> false)
                |> Array.length
                
            Expect.isGreaterThan successCount 0 "Should have some successful expansions"

        testCase "Complex template with multiple placeholders" <| fun _ ->
            let context = Context.create "$.test" [
                ("first", StringValue "John")
                ("last", StringValue "Doe")
                ("title", StringValue "Mr.")
            ]
            
            let template = "{title} {first} {last}"
            match expandTemplate template context with
            | Success result -> Expect.equal result "Mr. John Doe" "Should expand all placeholders"
            | Error err -> failtest (sprintf "Unexpected error: %A" err)

        testCase "Template with nested braces" <| fun _ ->
            let context = Context.create "$.test" [("value", StringValue "test")]
            let template = "{{not_placeholder}} {value}"
            
            match expandTemplate template context with
            | Success result -> Expect.equal result "{{not_placeholder}} test" "Should not expand nested braces"
            | Error err -> failtest (sprintf "Unexpected error: %A" err)
    ]

    // JSON parsing simulation tests
    let jsonSimulationTests = [
        testCase "JSON string value simulation" <| fun _ ->
            let json = """{"name": "Alice", "city": "London"}"""
            match createJsonTestContext json with
            | Some context ->
                let template = "Hello {name} from {city}!"
                match expandTemplate template context with
                | Success result -> Expect.equal result "Hello Alice from London!" "Should expand JSON-parsed values"
                | Error err -> failtest (sprintf "Template expansion failed: %A" err)
            | None -> failtest "Failed to parse JSON context"

        testCase "JSON number value simulation" <| fun _ ->
            let json = """{"count": 42, "price": 19.99}"""
            match createJsonTestContext json with
            | Some context ->
                let template = "{count} items for ${price}"
                match expandTemplate template context with
                | Success result -> Expect.stringContains result "42" "Should contain count"
                | Error err -> failtest (sprintf "Template expansion failed: %A" err)
            | None -> failtest "Failed to parse JSON context"

        testCase "JSON boolean value simulation" <| fun _ ->
            let json = """{"active": true, "verified": false}"""
            match createJsonTestContext json with
            | Some context ->
                let template = "Active: {active}, Verified: {verified}"
                match expandTemplate template context with
                | Success result -> Expect.equal result "Active: true, Verified: false" "Should expand boolean values"
                | Error err -> failtest (sprintf "Template expansion failed: %A" err)
            | None -> failtest "Failed to parse JSON context"

        testCase "JSON null value simulation" <| fun _ ->
            let json = """{"value": null, "name": "test"}"""
            match createJsonTestContext json with
            | Some context ->
                let template = "Value: {value}, Name: {name}"
                match expandTemplate template context with
                | Success result -> Expect.equal result "Value: , Name: test" "Should handle null as empty string"
                | Error err -> failtest (sprintf "Template expansion failed: %A" err)
            | None -> failtest "Failed to parse JSON context"

        testCase "JSON complex object simulation - should handle gracefully" <| fun _ ->
            let json = """{"simple": "value", "complex": {"nested": "data"}, "array": [1, 2, 3]}"""
            match createJsonTestContext json with
            | Some context ->
                let template = "Simple: {simple}"
                match expandTemplate template context with
                | Success result -> Expect.equal result "Simple: value" "Should handle simple values from complex JSON"
                | Error err -> failtest (sprintf "Template expansion failed: %A" err)
            | None -> failtest "Failed to parse JSON context"

        testCase "Invalid JSON simulation - should fail gracefully" <| fun _ ->
            let json = """{"invalid": json}"""
            match createJsonTestContext json with
            | Some _ -> failtest "Should not successfully parse invalid JSON"
            | None -> () // Expected to fail

        testCase "Empty JSON simulation" <| fun _ ->
            let json = """{}"""
            match createJsonTestContext json with
            | Some context ->
                let template = "Hello {missing}!"
                match expandTemplate template context with
                | Success _ -> failtest "Should not succeed with missing placeholder"
                | Error (MissingPlaceholder "missing") -> () // Expected
                | Error other -> failtest (sprintf "Wrong error type: %A" other)
            | None -> failtest "Should successfully parse empty JSON"
    ]

    // Edge case tests
    let edgeCaseTests = [
        testCase "Large string values" <| fun _ ->
            let largeString = String.replicate 10000 "x"
            let value = StringValue largeString
            let result = value.AsString()
            Expect.equal result largeString "Should handle large strings"

        testCase "Special characters in templates" <| fun _ ->
            let context = Context.create "$.test" [
                ("special", StringValue "hello@world.com")
                ("unicode", StringValue "café")
            ]
            
            let template = "Email: {special}, Unicode: {unicode}"
            match expandTemplate template context with
            | Success result -> 
                Expect.stringContains result "hello@world.com" "Should contain email"
                Expect.stringContains result "café" "Should contain unicode"
            | Error err -> failtest (sprintf "Unexpected error: %A" err)

        testCase "Extreme numeric values" <| fun _ ->
            let context = Context.create "$.test" [
                ("maxInt", IntValue Int64.MaxValue)
                ("minInt", IntValue Int64.MinValue)
                ("nan", FloatValue Double.NaN)
                ("infinity", FloatValue Double.PositiveInfinity)
            ]
            
            let template = "Max: {maxInt}, Min: {minInt}, NaN: {nan}, Inf: {infinity}"
            match expandTemplate template context with
            | Success result -> 
                Expect.stringContains result (Int64.MaxValue.ToString()) "Should contain max int"
                Expect.stringContains result (Int64.MinValue.ToString()) "Should contain min int"
            | Error err -> failtest (sprintf "Unexpected error: %A" err)

        testCase "Template with only braces" <| fun _ ->
            let context = Context.create "$.test" [("", StringValue "empty_key")]
            let template = "{}"
            
            match expandTemplate template context with
            | Error (MissingPlaceholder "") -> () // Expected - empty placeholder
            | _ -> failtest "Should fail for empty placeholder"

        testCase "Context with empty key" <| fun _ ->
            let context = Context.create "$.test" [("", StringValue "value")]
            let template = "{}"
            
            match expandTemplate template context with
            | Success result -> Expect.equal result "value" "Should handle empty key"
            | Error (MissingPlaceholder "") -> () // Also acceptable
            | Error other -> failtest (sprintf "Wrong error type: %A" other)
    ]

    // Performance and stress tests
    let performanceTests = [
        testCase "Large context performance" <| fun _ ->
            let largeContext = 
                [1..1000]
                |> List.map (fun i -> (sprintf "key%d" i, StringValue (sprintf "value%d" i)))
                |> Context.create "$.test"
                
            let template = "Found: {key500}"
            let sw = System.Diagnostics.Stopwatch.StartNew()
            
            match expandTemplate template largeContext with
            | Success result -> 
                sw.Stop()
                Expect.equal result "Found: value500" "Should find value in large context"
                Expect.isLessThan sw.ElapsedMilliseconds 100L "Should be reasonably fast"
            | Error err -> failtest (sprintf "Unexpected error: %A" err)

        testCase "Many placeholders performance" <| fun _ ->
            let context = Context.create "$.test" [
                ("a", StringValue "1"); ("b", StringValue "2"); ("c", StringValue "3")
                ("d", StringValue "4"); ("e", StringValue "5")
            ]
            
            let template = String.replicate 100 "{a}{b}{c}{d}{e}"
            let sw = System.Diagnostics.Stopwatch.StartNew()
            
            match expandTemplate template context with
            | Success result -> 
                sw.Stop()
                Expect.stringContains result "12345" "Should expand all placeholders"
                Expect.isLessThan sw.ElapsedMilliseconds 100L "Should be reasonably fast"
            | Error err -> failtest (sprintf "Unexpected error: %A" err)
    ]

    // Combine all tests
    [<Tests>]
    let allTemplateTests = 
        testList "Template and Type System Tests" [
            testList "Property-based tests" propertyTests
            testList "Specific functionality tests" specificTests  
            testList "JSON parsing simulation tests" jsonSimulationTests
            testList "Edge case tests" edgeCaseTests
            testList "Performance tests" performanceTests
        ]

    // Register arbitraries for FsCheck
    do Arb.register<TestArbitraries>() |> ignore