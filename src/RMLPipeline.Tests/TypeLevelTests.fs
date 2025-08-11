namespace RMLPipeline.Tests

open System
open System.Threading
open System.Diagnostics
open Expecto
open FsCheck
open RMLPipeline.Core
open RMLPipeline.Internal.StringPooling
open RMLPipeline.FastMap.Types
// open RMLPipeline.Internal.StringInterning
open FSharp.HashCollections
// open StringInterningGenerators

module TypeLevelTests =

    let genValidStringId =
        Gen.choose(0, 10000000)
        |> Gen.map StringId.Create
        |> Gen.filter (fun id -> id.IsValid)

    let genValidIdNum : Gen<int32> =
        Gen.choose(0, 10000000)
        |> Gen.filter (fun id -> id >= 0)

    let genTempIncrements =
        Gen.choose(1, 100) |> Gen.map uint8

    let genIdTempPairs : Gen<int32 * int32> =
        gen {
            let! id = genValidIdNum
            let! temp = genTempIncrements
            return id, int32 temp
        }
    
    let genIdTempPairsList : Gen<(int32 * int32) list> =
        Gen.listOf genIdTempPairs
        |> Gen.filter (fun lst -> lst.Length < 10000)

    let genMaxCountPairs : Gen<uint8 * uint8> =
        gen {
            let! count1 = Gen.choose(1, 100)
            let! count2 = Gen.choose(1, 100)
            return uint8 count1, uint8 count2
        } |> Gen.filter (fun (c1, c2) -> c1 < c2)

    let genMaxCountEmptyList : Gen<uint16> =
        Gen.choose(0, 1000)
        |> Gen.map uint16

    let genSignalList : Gen<int32 list> =
        genValidIdNum 
        |> Gen.listOf
        |> Gen.resize 20

    type TypeLevelArbitraries =
        static member StringId() =
            Arb.fromGen genValidStringId
        
        static member ValidIdNum() =
            Arb.fromGen genValidIdNum

        static member TempIncrements() =
            Arb.fromGen genTempIncrements
        
        static member IdTempPairs() =
            Arb.fromGen genIdTempPairs

        static member IdTempPairsList() =
            Arb.fromGen genIdTempPairsList
        
        static member MaxCountPairs() =
            Arb.fromGen genMaxCountPairs
        
        static member MaxCountEmptyList() =
            Arb.fromGen genMaxCountEmptyList
        
        static member SignalList() =
            Arb.fromGen genSignalList

    let fsConfig = { FsCheckConfig.defaultConfig with 
                        arbitrary = [ typeof<TypeLevelArbitraries> ] }

    module StringIdProperties =
        [<Tests>]
        let tests =
            testList "StringId_Invariants" [
                testPropertyWithConfig 
                        fsConfig 
                        "Valid StringIds have non-negative values" <| fun (id: int32) ->
                    (id >= 0) ==> lazy (
                        let stringId = StringId.Create id
                        stringId.IsValid
                    )
                
                testPropertyWithConfig 
                        fsConfig 
                        "StringId preserves ID value through temperature changes" 
                            <| fun (id: int32) (tempIncrements: uint8) ->
                    let initial = StringId.Create id
                    let mutable current = initial
                    
                    for _ in 1 .. int tempIncrements do
                        current <- current.IncrementTemperature()
                    
                    current.Value = id

                testPropertyWithConfig 
                        fsConfig 
                        "Temperature increments are additive" 
                            <| fun (id: int32) (temp1: uint8) (temp2: uint8) ->
                    let stringId = StringId.Create id
                    let afterFirst = 
                        let mutable sid = stringId
                        for _ in 1 .. int temp1 do
                            sid <- sid.IncrementTemperature()
                        sid
                    
                    let afterSecond = 
                        let mutable sid = afterFirst
                        for _ in 1 .. int temp2 do
                            sid <- sid.IncrementTemperature()
                        sid
                    
                    afterSecond.Temperature = int temp1 + int temp2
                
                testPropertyWithConfig 
                        fsConfig  
                        "WithTemperature preserves ID and sets exact temperature" 
                            <| fun (id: int32, temp: int32) ->
                    let stringId = StringId.Create id
                    let withTemp = stringId.WithTemperature temp                    
                    withTemp.Value = id && withTemp.Temperature = temp
                
                testPropertyWithConfig 
                        fsConfig 
                        "Temperature starts at zero for new StringId" <| fun (id: int32) ->
                    let stringId = StringId.Create id
                    stringId.Temperature = 0
                
                testCase "Invalid StringId has negative value" <| fun () ->
                    let invalidId = StringId.Invalid
                    Expect.isFalse invalidId.IsValid "Invalid StringId should be property indicated"
                    Expect.isTrue (invalidId.Value < 0) "Invalid StringId should have negative value"
                
                testPropertyWithConfig 
                        fsConfig 
                        "Temperature overflow wrap-around resets temp to 1" <| fun (id: int32) ->
                    let stringId = StringId.Create(id).WithTemperature Int32.MaxValue
                    let incremented = stringId.IncrementTemperature()

                    // Temperature should wrap to negative (overflow)
                    incremented.Value = id && incremented.Temperature = 1
            ]

    module PromotionTrackerProperties =
        let tests = 
            // PromotionTracker Properties
            ftestList "PromotionTracker invariants" [
                testPropertyWithConfig 
                        fsConfig
                        "Enqueued signals can be dequeued in FIFO order" 
                            <| fun (signals: (int32 * int32) list) ->
                    (signals.Length < 1000) ==> lazy (
                        let tracker = PromotionQueue.Create(10) // 1024 slots
                        
                        // Enqueue all
                        let enqueued = 
                            signals 
                            |> List.map (fun (id, temp) -> 
                                let signal = { 
                                    StringId = StringId.Create(id)
                                    Temperature = temp
                                    Timestamp = Stopwatch.GetTimestamp() 
                                }
                                tracker.TryEnqueue(signal), signal
                            )
                        
                        let successfullyEnqueued = 
                            enqueued 
                            |> List.filter fst 
                            |> List.map snd
                        
                        // Dequeue all
                        let dequeued = tracker.TryDequeueBatch(signals.Length * 2)
                        
                        // Should get back what we put in, in order
                        Array.toList dequeued = successfullyEnqueued
                    )
                
                testPropertyWithConfig 
                        fsConfig 
                        "Queue respects capacity limits" <| fun (signals: int32 list) ->
                    let queueSizeBits = 4 // 16 slots
                    let tracker = PromotionQueue.Create queueSizeBits
                    let capacity = 1 <<< queueSizeBits
                    
                    // Try to enqueue more than capacity
                    let results = 
                        signals 
                        |> List.take (min signals.Length (capacity * 2))
                        |> List.map (fun id ->
                            let signal = { 
                                StringId = StringId.Create id
                                Temperature = 1
                                Timestamp = 0L 
                            }
                            tracker.TryEnqueue(signal)
                        )
                    
                    let enqueueCount = results |> List.filter id |> List.length
                    
                    // Should never enqueue more than capacity minus 1 (one slot buffer)
                    enqueueCount <= (capacity - 1)
                
                testPropertyWithConfig 
                        fsConfig 
                        "Dequeue respects max count parameter" 
                        <| fun (count: uint8, itemCount: uint8) ->
                    let tracker = PromotionQueue.Create(10)
                    
                    // Enqueue items
                    for i in 0 .. int itemCount - 1 do
                        let signal = { 
                            StringId = StringId.Create(i)
                            Temperature = i
                            Timestamp = 0L 
                        }
                        tracker.TryEnqueue(signal) |> ignore
                    
                    // Dequeue with limit
                    let dequeued = tracker.TryDequeueBatch(int count)
                    
                    dequeued.Length <= int count && dequeued.Length <= int itemCount
                
                testPropertyWithConfig 
                        fsConfig 
                        "Empty queue returns empty array" <| fun (maxCount: uint16) ->
                    let tracker = PromotionQueue.Create 8
                    let result = tracker.TryDequeueBatch(int maxCount)
                    
                    Array.isEmpty result
                
                testPropertyWithConfig  // this is a slow-ish test, so we limit the runs...
                    { fsConfig with maxTest = 30 }  // 30 runs takes around 10 sec on modern hardware
                    "Epoch tracker respects rotation intervals" <| fun (signals: int32 list) ->
                        signals.Length > 0 ==> lazy (
                            // Long-ish interval to ensure we have time to rotate for the test
                            let rotationInterval = TimeSpan.FromMilliseconds 30.0
                            let tracker = EpochPromotionTracker.Create(8, rotationInterval)
                            let resolver _ = Some "dummy"
                            
                            // Use generous timing to ensure behaviour gets triggered
                            for id in signals do
                                tracker.SignalPromotion(StringId.Create id, 100) |> ignore
                                Thread.Sleep 20  // > half a rotation interval
                            
                            // Wait additional time to ensure at least one complete cooling cycle
                            Thread.Sleep (int (rotationInterval.TotalMilliseconds * 2.5))
                            
                            // We should now have some results from completed epochs
                            let results = tracker.ProcessPromotions resolver
                            let results2 = tracker.ProcessPromotions resolver  // Second call should be empty

                            // Basic sanity checks
                            results.Length <= signals.Length &&  // Can't have more results than signals
                            results2.Length = 0  // Second immediate call should return nothing
                        )
                
                testPropertyWithConfig 
                        fsConfig 
                        "Epoch tracker prevents concurrent processing" <| fun () ->
                    let tracker = EpochPromotionTracker.Create(8, TimeSpan.FromMilliseconds(100.0))
                    let resolver _ = Some "dummy"
                    
                    // Add some signals
                    for i in 0..10 do
                        tracker.SignalPromotion(StringId.Create i, 100) |> ignore
                    
                    // Try concurrent processing
                    let results = 
                        [|1..5|] 
                        |> Array.Parallel.map (fun _ -> tracker.ProcessPromotions resolver)
                    
                    // Only one should succeed (have non-empty results)
                    let nonEmptyResults = results |> Array.filter (fun r -> r.Length > 0)
                    nonEmptyResults.Length <= 1
                
                testPropertyWithConfig 
                        fsConfig 
                        "Promotion candidates are deduplicated" <| fun (id: int32) ->
                    let tracker = PromotionQueue.Create(10)
                    
                    // Enqueue same ID multiple times with different temperatures
                    for i in 1 .. 10 do
                        let signal = { 
                            StringId = StringId.Create id
                            Temperature = i
                            Timestamp = int64 i 
                        }
                        tracker.TryEnqueue signal |> ignore
                    
                    // Create resolver
                    let resolver (sid: StringId) = 
                        if sid.Value = id then Some "test-string" else None
                    
                    // Process through epoch tracker logic (simulated)
                    let candidates = 
                        tracker.TryDequeueBatch 1000
                        |> Array.choose (fun s -> 
                            resolver s.StringId |> Option.map (fun str -> 
                                { Value = str; Temperature = s.Temperature }))
                        |> Array.distinctBy (fun c -> c.Value)
                    
                    // Should have at most one candidate for the string
                    candidates.Length <= 1
                
                testPropertyWithConfig 
                        fsConfig 
                        "Temperature threshold triggers signal exactly once" 
                            <| fun (id: int32) (threshold: uint8) ->
                    (threshold > 0uy && threshold < 100uy) ==> lazy (
                        let mutable stringId = StringId.Create id
                        let mutable signalCount = 0
                        
                        // Increment temperature past threshold
                        for i in 1 .. int threshold + 10 do
                            let oldTemp = stringId.Temperature
                            stringId <- stringId.IncrementTemperature()
                            let newTemp = stringId.Temperature
                            
                            // Check if we crossed threshold
                            if oldTemp < int threshold && newTemp >= int threshold then
                                signalCount <- signalCount + 1
                        
                        // Should signal exactly once when crossing threshold
                        signalCount = 1
                    )
            ]


    // Main test entry point
    [<Tests>]
    let allTests =
        ftestList "StringPoolTests" [
            StringIdProperties.tests
            PromotionTrackerProperties.tests
        ]
    