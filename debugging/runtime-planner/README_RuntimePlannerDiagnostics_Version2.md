# Runtime Planner Metadata & Diagnostics

This addition provides:

## Shared Generators
`SharedGenerators.fs` centralises all FsCheck generators for TriplesMaps and PlannerConfig.  
Both the original planner tests and runtime planner tests can now consume identical distributions.

## Diagnostics
`RuntimePlannerDiagnostics.fs` offers:
- `summarize : RuntimePlan -> RuntimePlanDiagnostics`
- `diagnosticsToString : RuntimePlanDiagnostics -> string`

Used in properties to attach detailed failure context (map indices, ref counts, join shapes, mask modes).

## Tests
`RuntimePlannerTests.fs` focuses purely on metadata invariants:
- Reference extraction
- Join descriptor integrity
- Large mask handling (>64 refs)
- Determinism
- PathRefIndex coverage
- Requirement mask subset correctness

Each failing property dumps a rich diagnostic block.

## Benchmarks
`RuntimePlannerBenchmarks.fs` measures:
- Base `createRMLPlan` time
- Runtime metadata build time

Environment variables:
- `RT_BENCH_SIZES` e.g. `1,5,10`
- `RT_BENCH_ITER` iterations per size (default 5)
- `RT_BENCH_MODE` `LowMemory|Balanced|HighPerformance|Mixed`

Example:
```
RT_BENCH_SIZES=1,10,25 RT_BENCH_ITER=10 RT_BENCH_MODE=Mixed dotnet test --filter RuntimePlannerBenchmarks
```

## Integration Steps
1. Add the new files to your test project.
2. Optionally remove duplicated generators from `PlannerModelTests.fs`, replacing them with:
   ```fsharp
   open RMLPipeline.Tests.SharedGenerators
   ```
3. Run:
   ```
   dotnet test --filter RuntimePlannerMetadata
   dotnet test --filter RuntimePlannerBenchmarks
   ```

## Next
When execution logic is implemented, extend diagnostics to include:
- Join key hashing counts
- Waiter queue depth
- Reference readiness transition counts

And add stress mode beyond `FsCheck --stress-test` by generating very large synthetic mappings using existing generator scaffolding.