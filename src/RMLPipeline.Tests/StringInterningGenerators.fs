namespace RMLPipeline.Tests

open RMLPipeline.Core
open RMLPipeline.Internal.StringInterning
open RMLPipeline.Internal.StringPooling
open System
open System.Threading
open System.Collections.Generic
open System.Collections.Concurrent
open System.Diagnostics
open FsCheck
open Expecto
open FSharp.HashCollections

/// Generators and arbitraries for testing string interning components
module StringInterningGenerators =
    // String content generators
    let genShortString = 
        Gen.choose(1, 20)
        |> Gen.map (fun len -> String.replicate len "a")
        
    let genMediumString = 
        Gen.choose(30, 100)
        |> Gen.map (fun len -> String.replicate len "b")
        
    let genLongString = 
        Gen.choose(200, 1000)
        |> Gen.map (fun len -> String.replicate len "c")
    
    let genSpecialCharsString =
        Gen.elements [
            "with space"
            "with-hyphen"
            "with_underscore"
            "with.dot"
            "with{braces}"
            "with/slash"
            "with\\"
            "with\"quotes\""
            "with'apostrophes'"
            "with\nnewline"
            "with\ttab"
            "with\rreturn"
        ]
    
    let genUnicodeString =
        Gen.elements [
            "Unicode: ñáéíóú"
            "Emoji: 🔍🔎🔥💧"
            "Math: ∑∏√∂∆"
            "CJK: 你好世界"
            "Arabic: مرحبا"
            "Greek: Γειά σου"
            "Russian: Привет"
        ]
    
    // Combined string generator
    let genTestString =
        Gen.oneof [
            genShortString
            genMediumString
            genLongString
            genSpecialCharsString
            genUnicodeString
            // Common strings seen in RML mappings
            Gen.constant "http://example.org/resource"
            Gen.constant "$.people[*]"
            Gen.constant "$.transactions[*].lineItems[*]"
            Gen.constant "http://xmlns.com/foaf/0.1/name"
            Gen.constant "{id}"
            Gen.constant "name"
        ]
    
    // StringId generator
    let genValidStringId =
        Gen.choose(0, 10000000)
        |> Gen.map StringId.Create
        
    let genInvalidStringId =
        Gen.elements [-1; -42; Int32.MinValue]
        |> Gen.map StringId.Create

    // Configuration generators
    let genEdenSize = Gen.choose(50, 10000)
    let genChunkSize = Gen.choose(10, 5000)
    let genPromotionThreshold = Gen.choose(5, 100)
    
    let genPoolConfiguration : Gen<PoolConfiguration> =
        gen {
            let! globalEdenSize = genEdenSize
            let! groupEdenSize = genEdenSize |> Gen.map (fun s -> s / 2)
            let! workerEdenSize = genEdenSize |> Gen.map (fun s -> s / 4)
            let! initialChunkSize = genChunkSize
            let! secondaryChunkSize = genChunkSize |> Gen.map (fun s -> s * 2)
            let! maxChunks = Gen.frequency [(8, Gen.map Some (Gen.choose(5, 100))); (2, Gen.constant None)]
            let! workerPromotionThreshold = genPromotionThreshold
            let! groupPromotionThreshold = genPromotionThreshold |> Gen.map (fun t -> t * 5)
            let! chunkGrowthFactor = Gen.choose(12, 30) |> Gen.map (fun f -> float f / 10.0)
            let! resizeThreshold = Gen.choose(5, 9) |> Gen.map (fun t -> float t / 10.0)
            
            let! promotionInterval = Gen.choose(10, 500) |> Gen.map float
            let! maxPromotionBatchSize = Gen.choose(5, 100)
            
            return {
                GlobalEdenSize = globalEdenSize
                GroupEdenSize = groupEdenSize
                WorkerEdenSize = workerEdenSize
                InitialChunkSize = initialChunkSize
                SecondaryChunkSize = secondaryChunkSize
                MaxChunks = maxChunks
                WorkerPromotionThreshold = workerPromotionThreshold
                GroupPromotionThreshold = groupPromotionThreshold
                ChunkGrowthFactor = chunkGrowthFactor
                ResizeThreshold = resizeThreshold
                FieldPathTemperatureFactor = 1.5
                URITemplateTemperatureFactor = 2.0
                LiteralTemperatureFactor = 0.5
                TemperatureDecayFactor = 0.5
                DecayInterval = TimeSpan.FromHours(1.0)
                ThresholdScalingFactor = 1.2
                ThresholdScalingInterval = 10000
                MinPromotionInterval = TimeSpan.FromMilliseconds(promotionInterval)
                MaxPromotionBatchSize = maxPromotionBatchSize
            }
        }
    
    // Access pattern generator
    let genStringAccessPattern =
        Gen.elements [
            StringAccessPattern.HighFrequency
            StringAccessPattern.MediumFrequency
            StringAccessPattern.LowFrequency
            StringAccessPattern.Planning
            StringAccessPattern.Runtime
        ]
    
    // Dependency group ID generator
    let genDependencyGroupId =
        Gen.choose(0, 100)
        |> Gen.map DependencyGroupId
    
    // Worker ID generator
    let genWorkerId =
        Gen.constant (WorkerId.Create())
    
    // Concurrent scenario generator
    let genConcurrentScenario =
        gen {
            let! threadCount = Gen.choose(2, 8)
            let! opsPerThread = Gen.choose(10, 100)
            let! stringCount = Gen.choose(10, 100)
            let! accessPattern = genStringAccessPattern
            
            return {|
                ThreadCount = threadCount
                OperationsPerThread = opsPerThread
                StringCount = stringCount
                AccessPattern = accessPattern
            |}
        }
    
    // Promotion scenario generator
    let genPromotionScenario =
        gen {
            let! hotStringCount = Gen.choose(5, 20)
            let! coldStringCount = Gen.choose(10, 50)
            let! accessesPerHotString = Gen.choose(20, 100)
            let! threshold = Gen.choose(10, 50)
            
            return {|
                HotStringCount = hotStringCount
                ColdStringCount = coldStringCount
                AccessesPerHotString = accessesPerHotString
                PromotionThreshold = threshold
            |}
        }
    
    // FSCheck arbitraries
    type StringInterningArbitraries =
        static member TestString() = Arb.fromGen genTestString
        static member StringId() = Arb.fromGen genValidStringId
        static member PoolConfiguration() : Arbitrary<PoolConfiguration> = 
            { new Arbitrary<PoolConfiguration>() with
                    member _.Generator = genPoolConfiguration
                    member _.Shrinker _ = Seq.empty
            }
        
        static member StringAccessPattern() = Arb.fromGen genStringAccessPattern
        static member DependencyGroupId() = Arb.fromGen genDependencyGroupId
        static member WorkerId() = Arb.fromGen genWorkerId
        static member ConcurrentScenario() = Arb.fromGen genConcurrentScenario
        static member PromotionScenario() = Arb.fromGen genPromotionScenario
    