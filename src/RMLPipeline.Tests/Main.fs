namespace RMLPipeline.Tests

    module TestRunner =
        open Expecto

            open Expecto
            open RMLPipeline.Tests.VocabSuite
            open RMLPipeline.Tests.TemplateTests
            open RMLPipeline.Tests.StringPoolTests
            // open RMLPipeline.Tests.PipelineModelBased

            [<EntryPoint>]
            let main argv =
                (* let allTests = testList "All RML Tests" [
                    allComputationRMLTests
                    pipelinePropertyTests
                    pipelineModelBasedTests                    
                ] *)

                let allTests = 
                    testList "All RML Tests" [
                        (* allComputationRMLTests
                        allTemplateTests *)
                        allStringPoolTests
                    ]
                
                allComputationRMLTests
                |> Test.shuffle defaultConfig.joinWith.asString
                |> runTestsWithCLIArgs [] argv