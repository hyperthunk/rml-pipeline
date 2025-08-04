namespace RMLPipeline.Tests

    module TestRunner =
        open Expecto

            open Expecto
            open RMLPipeline.Tests.VocabSuite

            [<EntryPoint>]
            let main argv =
                // Tests.runTestsInAssemblyWithCLIArgs [] argv
                tests
                |> Test.shuffle defaultConfig.joinWith.asString
                |> runTestsWithCLIArgs [] [||]
