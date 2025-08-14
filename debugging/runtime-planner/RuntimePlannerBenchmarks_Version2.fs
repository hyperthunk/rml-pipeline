namespace RMLPipeline.Benchmarks

open System
open Expecto
open RMLPipeline.Internal.Planner
open RMLPipeline.Internal.RuntimePlanner
open RMLPipeline.Tests.SharedGenerators
open FsCheck

(*
    Lightweight benchmark harness (not micro-benchmark; aims for relative signals).

    Configure via environment variables:
      RT_BENCH_SIZES = comma separated counts (e.g. "1,5,10")
      RT_BENCH_ITER = iterations per size (default 5)
      RT_BENCH_MODE = LowMemory|Balanced|HighPerformance|Mixed (default Mixed)
*)

module RuntimePlannerBenchmarks =

    let private parseSizes () =
        let raw = Environment.GetEnvironmentVariable "RT_BENCH_SIZES"
        if String.IsNullOrWhiteSpace raw then [|1;5;10|]
        else
            raw.Split(',', StringSplitOptions.RemoveEmptyEntries)
            |> Array.choose (fun s -> match Int32.TryParse s.Trim() with | true, v when v>0 -> Some v | _ -> None)
            |> fun arr -> if arr.Length=0 then [|1;5;10|] else arr

    let private iterations () =
        match Int32.TryParse (Environment.GetEnvironmentVariable "RT_BENCH_ITER") with
        | true, v when v>0 -> v
        | _ -> 5

    let private modes () =
        match Environment.GetEnvironmentVariable "RT_BENCH_MODE" with
        | null | "" | "Mixed" ->
            [| LowMemory; Balanced; HighPerformance |]
        | "LowMemory" -> [|LowMemory|]
        | "Balanced" -> [|Balanced|]
        | "HighPerformance" -> [|HighPerformance|]
        | _ -> [| LowMemory; Balanced; HighPerformance |]

    let private timeAction f =
        let sw = Diagnostics.Stopwatch.StartNew()
        let r = f()
        sw.Stop()
        sw.ElapsedMilliseconds, r

    let private sampleMaps n =
        Array.init n (fun _ -> Gen.sample 0 1 SharedGenerators.genBasicTriplesMap |> List.head)

    let benchmarkCase size mode =
        testCase $"RuntimePlanner Benchmark size={size} mode={mode}" <| fun _ ->
            let maps = sampleMaps size
            let cfg =
                { PlannerConfig.Default with MemoryMode = mode }
            // Warm-up
            ignore (createRMLPlan maps cfg |> RuntimePlanner.buildRuntimePlan)

            let iters = iterations()
            let mutable totalPlan = 0L
            let mutable totalRuntime = 0L
            for _ in 1 .. iters do
                let planMs, plan =
                    timeAction (fun () -> createRMLPlan maps cfg)
                let runtimeMs, _ =
                    timeAction (fun () -> RuntimePlanner.buildRuntimePlan plan)
                totalPlan <- totalPlan + planMs
                totalRuntime <- totalRuntime + runtimeMs

            let avgPlan = float totalPlan / float iters
            let avgRuntime = float totalRuntime / float iters
            printfn "[Benchmark] size=%d mode=%A iterations=%d avgPlan=%.2fms avgRuntimeMeta=%.2fms"
                    size mode iters avgPlan avgRuntime

    [<Tests>]
    let benchmarks =
        let sizes = parseSizes()
        let ms = modes()
        testList "RuntimePlannerBenchmarks" [
            for s in sizes do
                for m in ms do
                    yield benchmarkCase s m
        ]