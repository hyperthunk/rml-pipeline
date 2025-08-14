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


            (*
            
                cover123 a Cover
                cover123 clauseName "abc"
                cover123 hasLimit limit123            
            
            *)

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