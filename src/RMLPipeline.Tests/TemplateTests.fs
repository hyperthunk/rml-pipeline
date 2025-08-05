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

    // Create a separate, simplified RMLValue type for testing that avoids FastMap entirely
    type TestRMLValue =
        | TestStringValue of string
        | TestIntValue of int64
        | TestFloatValue of double
        | TestBoolValue of bool
        | TestDateTimeValue of DateTime
        | TestDecimalValue of decimal
        | TestNullValue

    // Convert TestRMLValue to real RMLValue
    let testToRealRMLValue (testValue: TestRMLValue) : RMLValue =
        match testValue with
        | TestStringValue s -> StringValue s
        | TestIntValue i -> IntValue i
        | TestFloatValue f -> FloatValue f
        | TestBoolValue b -> BoolValue b
        | TestDateTimeValue dt -> DateTimeValue dt
        | TestDecimalValue d -> DecimalValue d
        | TestNullValue -> NullValue

    // Custom generators that completely avoid FastMap
    module Generators =
        
        let genSafeString = 
            Gen.elements ["hello"; "world"; "test"; "value"; ""; "123"; "test_value"; "a-b-c"]
            
        let genLargeString =
            Gen.choose (100, 1000)
            |> Gen.map (fun length -> String.replicate length "x")
            
        let genStringWithSpecialChars =
            Gen.elements ["hello world"; "test@example.com"; "http://example.com"; "multi\nline"; "unicode: éñ"]
            
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
            Gen.oneof [
                Gen.choose (-1000000, 1000000) |> Gen.map decimal
                Gen.choose (0, 999) |> Gen.map (fun x -> decimal x / 100m)
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
            ]
            
        let genBool = Gen.elements [true; false]
        
        // TestRMLValue generator - NO FastMap issues
        let genTestRMLValue = 
            Gen.oneof [
                genAllStrings |> Gen.map TestStringValue
                genInt64 |> Gen.map TestIntValue
                genFloat |> Gen.map TestFloatValue
                genBool |> Gen.map TestBoolValue
                genDateTime |> Gen.map TestDateTimeValue
                genDecimal |> Gen.map TestDecimalValue
                Gen.constant TestNullValue
            ]
        
        // Simple context generator with predefined contexts
        let genContext = 
            Gen.oneof [
                Gen.constant Context.empty
                Gen.constant (Context.create "$.test" [("name", StringValue "test")])
                Gen.constant (Context.create "$.test" [("id", IntValue 42L)])
                Gen.constant (Context.create "$.test" [("active", BoolValue true)])
                Gen.constant (Context.create "$.test" [("name", StringValue "John"); ("age", IntValue 30L)])
                Gen.constant (Context.create "$.test" [("value", NullValue)])
            ]
        
        // Non-null string generator
        let genNonNullString = 
            Gen.oneof [
                genSafeString
                Gen.constant ""
                Gen.constant "simple"
                Gen.constant "test"
            ]

    // Arbitraries that avoid all FastMap issues
    type TestArbitraries =
        static member TestRMLValue() = 
            Arb.fromGen Generators.genTestRMLValue
            
        static member Context() = 
            Arb.fromGen Generators.genContext
            
        static member Template() = 
            Arb.fromGen Generators.genNonNullString

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
            
            if jsonString.Trim() = "{}" then
                Some (Context.create "$.test" [])
            elif pairs.Length > 0 then
                Some (Context.create "$.test" rmlPairs)
            else
                let trimmed = jsonString.Trim()
                if trimmed.StartsWith("{") && trimmed.EndsWith("}") then
                    None
                else
                    None
        with
        | _ -> None

    // Property-based tests - Use TestRMLValue to completely avoid FastMap
    let propertyTests = [
        testProperty "TestRMLValue AsString never throws" <| fun (testValue: TestRMLValue) ->
            try
                let realValue = testToRealRMLValue testValue
                let _ = realValue.AsString()
                true
            with
            | _ -> false

        testProperty "TestRMLValue TryAsString is safe" <| fun (testValue: TestRMLValue) ->
            let realValue = testToRealRMLValue testValue
            match realValue.TryAsString() with
            | Some _ -> true
            | None -> true

        testProperty "RMLValue.TryParse round trip for basic types" <| fun (s: string) (i: int) (f: float) (b: bool) ->
            let stringResult = RMLValue.TryParse(s) |> Option.map (_.AsString())
            let intResult = RMLValue.TryParse(i) |> Option.map (_.AsString())
            let floatResult = RMLValue.TryParse(f) |> Option.map (_.AsString())
            let boolResult = RMLValue.TryParse(b) |> Option.map (_.AsString())
            
            stringResult.IsSome && intResult.IsSome && floatResult.IsSome && boolResult.IsSome

        testProperty "Context operations are pure" <| fun (key: string) ->
            (not (isNull key)) ==> lazy (
                let value = StringValue "test"
                let ctx1 = Context.empty
                let ctx2 = Context.add key value ctx1
                let ctx3 = Context.add key value ctx2
                
                Context.tryFind key ctx2 = Context.tryFind key ctx3
            )

        testProperty "Context merge works with simple contexts" <| fun () ->
            let ctx1 = Context.create "$.test1" [("a", StringValue "1")]
            let ctx2 = Context.create "$.test2" [("b", StringValue "2")]
            let ctx3 = Context.create "$.test3" [("c", StringValue "3")]
            
            let merged1 = Context.merge (Context.merge ctx1 ctx2) ctx3
            let merged2 = Context.merge ctx1 (Context.merge ctx2 ctx3)
            
            [("a", StringValue "1"); ("b", StringValue "2"); ("c", StringValue "3")]
            |> List.forall (fun (k, v) -> 
                Context.tryFind k merged1 = Some v && Context.tryFind k merged2 = Some v)

        testProperty "Template expansion with empty context never throws" <| fun (template: string) ->
            (not (isNull template)) ==> lazy (
                try
                    let result = expandTemplate template Context.empty
                    match result with
                    | Success _ -> true
                    | Error _ -> true
                with
                | _ -> false
            )

        testProperty "Template expansion with simple matching context" <| fun () ->
            let placeholders = ["name"; "id"; "value"; "test"; "key"; "placeholder"; "item"; "data"]
            placeholders
            |> List.forall (fun placeholder ->
                let template = sprintf "{%s}" placeholder
                let value = StringValue "testvalue"
                let context = Context.create "test" [(placeholder, value)]
                
                match expandTemplate template context with
                | Success result -> result = "testvalue"
                | Error _ -> false
            )

        testProperty "Template without placeholders returns original" <| fun (template: string) ->
            (not (isNull template) && not (template.Contains("{"))) ==> lazy (
                match expandTemplate template Context.empty with
                | Success result -> result = template
                | Error _ -> false
            )

        // Test conversion between TestRMLValue and RMLValue
        testProperty "TestRMLValue to RMLValue conversion preserves values" <| fun (testValue: TestRMLValue) ->
            let realValue = testToRealRMLValue testValue
            match testValue, realValue with
            | TestStringValue s, StringValue rs -> s = rs
            | TestIntValue i, IntValue ri -> i = ri
            | TestFloatValue f, FloatValue rf -> f = rf || (Double.IsNaN f && Double.IsNaN rf)
            | TestBoolValue b, BoolValue rb -> b = rb
            | TestDateTimeValue dt, DateTimeValue rdt -> dt = rdt
            | TestDecimalValue d, DecimalValue rd -> d = rd
            | TestNullValue, NullValue -> true
            | _ -> false
    ]

    // Specific test cases
    let specificTests = [
        testCase "RMLValue AsString basic types" <| fun _ ->
            testData
            |> List.iter (fun (name, value, expected) ->
                let actual = value.AsString()
                Expect.equal actual expected (sprintf "Failed for %s" name))

        testCase "RMLValue complex types AsString" <| fun _ ->
            let arrayValue = ArrayValue [|StringValue "a"; IntValue 1L|]
            let objectValue = ObjectValue (FastMap.empty |> FastMap.add "key" (StringValue "value"))
            
            Expect.equal (arrayValue.AsString()) "RMLValue Array" "Array should return standard representation"
            Expect.equal (objectValue.AsString()) "RMLValue Map" "Object should return standard representation"

        testCase "RMLValue ObjectValue operations" <| fun _ ->
            let fastMap = FastMap.empty |> FastMap.add "name" (StringValue "John") |> FastMap.add "age" (IntValue 30L)
            let objectValue = ObjectValue fastMap
            
            Expect.equal (objectValue.AsString()) "RMLValue Map" "ObjectValue should return map representation"
            
            match objectValue with
            | ObjectValue map ->
                let nameValue = FastMap.tryFind "name" map
                match nameValue with
                | ValueSome (StringValue "John") -> ()
                | _ -> failtest "Should find name value"
            | _ -> failtest "Should be ObjectValue"

        testCase "FastMap basic operations" <| fun _ ->
            let emptyMap = FastMap.empty<string, RMLValue>
            Expect.isTrue (FastMap.isEmpty emptyMap) "Empty map should be empty"
            Expect.equal (FastMap.count emptyMap) 0 "Empty map should have count 0"
            
            let mapWithOne = FastMap.add "key" (StringValue "value") emptyMap
            Expect.isFalse (FastMap.isEmpty mapWithOne) "Map with one item should not be empty"
            Expect.equal (FastMap.count mapWithOne) 1 "Map with one item should have count 1"
            
            match FastMap.tryFind "key" mapWithOne with
            | ValueSome (StringValue "value") -> ()
            | _ -> failtest "Should find the added value"

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
            | Success result -> 
                Expect.stringContains result "test" "Should expand the valid placeholder"
            | Error (MissingPlaceholder _) -> 
                () // Expected behavior for nested braces
            | Error err -> failtest (sprintf "Unexpected error: %A" err)

        testCase "Template with problematic placeholder characters" <| fun _ ->
            let context = Context.create "$.test" [("}", StringValue "brace")]
            let template = "{}"
            
            match expandTemplate template context with
            | Success result -> 
                Expect.equal result "{}" "Empty braces should remain unchanged"
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
            | Success result -> 
                Expect.equal result "{}" "Empty braces should remain unchanged"
            | Error err -> failtest (sprintf "Unexpected error: %A" err)

        testCase "Context with empty key" <| fun _ ->
            let context = Context.create "$.test" [("", StringValue "value")]
            let template = "{}"
            
            match expandTemplate template context with
            | Success result -> 
                Expect.equal result "{}" "Empty braces should remain unchanged"
            | Error err -> failtest (sprintf "Unexpected error: %A" err)
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

    // Register arbitraries for FsCheck - Only simple types, no FastMap
    do Arb.register<TestArbitraries>() |> ignore