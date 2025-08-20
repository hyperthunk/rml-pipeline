namespace RMLPipeline.Tests

    module TestRunner =
        open Expecto
        open RMLPipeline.Tests.VocabSuite
        open RMLPipeline.Tests.TemplateTests
        open RMLPipeline.Tests.StringInterningTests
        open RMLPipeline.Tests.StringPoolIntegrationTests
        open RMLPipeline.Tests.PlannerModelTests
        open RMLPipeline.Tests.TypeLevelTests
        open RMLPipeline.Tests.BitmapIndexTests
        // open RMLPipeline.Tests.PipelineModelBased

        [<EntryPoint>]
        let main argv =
            (* let allTests = testList "All RML Tests" [
                allComputationRMLTests
                pipelinePropertyTests
                pipelineModelBasedTests                    
            ] *)

            let allTests = 
                testList "RMLPipeline" [
                    typeLevelTests
                    bitmapIndexTests
                    allStringInterningTests
                    allStringPoolIntegrationTests
                    allComputationRMLTests
                    allTemplateTests
                    allStatefulPlannerTests
                ]
            
            allTests
            |> runTestsWithCLIArgs [] argv