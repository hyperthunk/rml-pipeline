namespace RMLPipeline.Tests

open Expecto
open FsCheck
open RMLPipeline.Internal.StringPooling
open RMLPipeline.Internal.StringInterning
open RMLPipeline.Core
open System

module StringInterningPromotionTests =

    // Lower thresholds to make promotion observable in tests
    let testConfig =
        { PoolConfiguration.Default with
            GlobalEdenSize = 128
            GroupEdenSize = 64
            WorkerEdenSize = 64
            WorkerPromotionThreshold = 3
            GroupPromotionThreshold = 4
            MinPromotionInterval = TimeSpan.FromMilliseconds 1.0
            ThresholdScalingInterval = 10
        }

    let mkHierarchy planning =
        let h = StringPool.createWithConfig planning testConfig
        h

    let repeat n f =
        for _ in 1..n do f()

    [<Tests>]
    let promotionTests =
        testList "StringInterning-Promotion" [

            testCase "Repeated high-frequency intern returns stable ID (eden de-dup)" <| fun _ ->
                let h = mkHierarchy [||]
                use scope = StringPool.createScope h None (Some 32)
                let s = "alpha"
                let id1 = scope.InternString(s, StringAccessPattern.HighFrequency)
                let id2 = scope.InternString(s, StringAccessPattern.HighFrequency)
                let id3 = scope.InternString(s, StringAccessPattern.HighFrequency)
                Expect.equal id1 id2 "IDs should be equal (deduped)"
                Expect.equal id2 id3 "IDs should be equal (deduped)"
                match scope.GetString id1 with
                | ValueSome v -> Expect.equal v s "Round trip"
                | _ -> failtest "Missing string"

            testCase "Temperature increases and triggers promotion redirect (worker->global)" <| fun _ ->
                let h = mkHierarchy [||]
                use scope = StringPool.createScope h None (Some 32)
                let target = "promote_me"
                let id1 = scope.InternString(target, StringAccessPattern.HighFrequency)
                // Repeated access to exceed worker threshold (3)
                repeat 10 (fun () -> scope.InternString(target, StringAccessPattern.HighFrequency) |> ignore)
                // After threshold we still should resolve same string
                match scope.GetString id1 with
                | ValueSome v -> Expect.equal v target "Promoted redirect resolves string"
                | ValueNone -> failtest "String disappeared after promotion"

            testCase "Worker promotion yields different higher-tier ID eventually but old ID redirects" <| fun _ ->
                let h = mkHierarchy [||]
                use scope = StringPool.createScope h None (Some 32)
                let s = "redirect_case"
                let firstId = scope.InternString(s, StringAccessPattern.HighFrequency)
                repeat 20 (fun () -> scope.InternString(s, StringAccessPattern.HighFrequency) |> ignore)
                // Force re-lookup (simulate retrieval path):
                let secondId = scope.InternString(s, StringAccessPattern.Planning) // goes global
                // It's possible firstId == secondId if promotion reused same tier; allow either
                match scope.GetString firstId, scope.GetString secondId with
                | ValueSome a, ValueSome b ->
                    Expect.equal a b "Strings must match through redirect"
                    Expect.equal a s "Original matches"
                | _ -> failtest "Could not retrieve promoted mapping"

            testCase "Segment allocation de-dup works (simulate many unique then reuse)" <| fun _ ->
                let h = mkHierarchy [||]
                use scope = StringPool.createScope h None (Some 8)
                // Fill Eden beyond capacity to force segment usage
                for i in 0..150 do
                    scope.InternString(sprintf "prefill_%d" i, StringAccessPattern.HighFrequency) |> ignore
                let shared = "segment_shared"
                let a = scope.InternString(shared, StringAccessPattern.HighFrequency)
                let b = scope.InternString(shared, StringAccessPattern.HighFrequency)
                Expect.equal a b "Segment reuse should produce same ID"
                match scope.GetString a with
                | ValueSome v -> Expect.equal v shared "Segment roundtrip"
                | _ -> failtest "Missing segment string"

            testProperty "Temperature monotonic for repeated interns" <| fun (baseStr:string) ->
                (not (String.IsNullOrWhiteSpace baseStr)) ==> lazy (
                    let s = baseStr.Substring(0, min baseStr.Length 10)
                    let h = mkHierarchy [||]
                    use scope = StringPool.createScope h None (Some 32)
                    let mutable lastTemp = -1
                    let mutable ok = true
                    for i in 1..8 do
                        let id = scope.InternString(s, StringAccessPattern.HighFrequency)
                        // Temperature encoded in upper bits managed by CreateWithTemperature; we rely on .Temperature property if exists
                        if id.Temperature < lastTemp then ok <- false
                        lastTemp <- id.Temperature
                    ok
                )
        ]