namespace RMLPipeline.Tests

open Expecto
open RMLPipeline.Internal.StringPooling
open RMLPipeline.Internal.StringInterning
open RMLPipeline.Internal.JoinKeyCanonicaliser

module JoinKeyCanonicaliserTests =

    [<Tests>]
    let tests =
        testList "JoinKeyCanonicaliser" [

            testCase "Empty key gives zero hash" <| fun _ ->
                use scope = StringPool.StringPool.createScope (StringPool.StringPool.create [||]) None None
                let h = canonicalisedJoinHash scope [||]
                Expect.equal h 0UL "Empty should hash to 0"

            testCase "Stable canonical hash for repeated IDs" <| fun _ ->
                let hierarchy = StringPool.StringPool.create [| "http://x.org/a"; "http://x.org/b" |]
                use scope = StringPool.StringPool.createScope hierarchy None (Some 16)
                let idA = scope.InternString("dynValueA", StringAccessPattern.HighFrequency)
                let idB = scope.InternString("dynValueB", StringAccessPattern.HighFrequency)
                // Trigger some promotions / temperature increments
                for _ in 1..10 do
                    scope.InternString("dynValueA", StringAccessPattern.HighFrequency) |> ignore
                    scope.InternString("dynValueB", StringAccessPattern.HighFrequency) |> ignore
                let h1 = canonicalisedJoinHash scope [| idA; idB |]
                let h2 = canonicalisedJoinHash scope [| idA; idB |]
                Expect.equal h1 h2 "Hash must be stable for same logical values"

            testCase "Order independent hash differs from ordered hash when sequence differs" <| fun _ ->
                let h = StringPool.StringPool.create [||]
                use scope = StringPool.StringPool.createScope h None (Some 8)
                let ids =
                    [| "k1"; "k2"; "k3" |]
                    |> Array.map (fun s -> scope.InternString(s, StringAccessPattern.HighFrequency))
                let hOrdered = canonicalisedJoinHash scope ids
                let permuted = [| ids.[2]; ids.[0]; ids.[1] |]
                let hPermuted = canonicalisedJoinHash scope permuted
                let hOrderInd1 = canonicalisedOrderIndependentHash scope ids
                let hOrderInd2 = canonicalisedOrderIndependentHash scope permuted
                Expect.notEqual hOrdered hPermuted "Raw order-dependent hash should differ"
                Expect.equal hOrderInd1 hOrderInd2 "Order-independent hash should be identical"
        ]