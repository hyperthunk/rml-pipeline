// OPTIONAL: Example of updating the original PlannerModelTests to import shared generators
// If you decide to refactor the original file, replace its generator definitions
// with `open RMLPipeline.Tests.SharedGenerators` and remove duplicates.
// This file is illustrative and NOT a full replacement.

namespace RMLPipeline.Tests

open Expecto
open FsCheck
open RMLPipeline.Internal.Planner
open RMLPipeline.Tests.SharedGenerators

module PlannerModelTestsUpdatedNote =
    [<Tests>]
    let smoke =
        testList "PlannerModelTestsUpdatedNote" [
            testCase "Shared generators accessible" <| fun _ ->
                let maps, cfg = Gen.sample 0 1 genPlanInput |> List.head
                if maps.Length > 0 then
                    let plan = createRMLPlan maps cfg
                    Expect.isGreaterThan plan.OrderedMaps.Length 0 "Plan should contain maps"
        ]