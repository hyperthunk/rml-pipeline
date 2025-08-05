namespace RMLPipeline.Tests

    module TestRunner =
        open Expecto

            open Expecto
            open RMLPipeline.Tests.VocabSuite
            open RMLPipeline.Tests.PipelineModelBased

            [<EntryPoint>]
            let main argv =
                let allTests = testList "All RML Tests" [
                    allComputationRMLTests
                    pipelinePropertyTests
                    pipelineModelBasedTests                    
                ]
                
                allTests
                |> Test.shuffle defaultConfig.joinWith.asString
                |> runTestsWithCLIArgs [] argv