# Metrics & Instrumentation Design

## Goals
- Zero mandatory external dependencies.
- Lock-free / low contention counters.
- Pull-based snapshots (user calls).
- Optional push-style observation via a minimal observer hub.
- Extensible for users who want System.Diagnostics.Metrics, EventSource, OpenTelemetry, Prometheus, etc.

## Counter Model
All counters are monotonic `int64` updated with `Interlocked.Increment` or `Volatile.Read`.  
Snapshots are approximate (no global transaction); acceptable for operational telemetry.

### Core Counters
| Counter | Source | Meaning |
|---------|--------|---------|
| lookups | HashIndex | Total key lookups (Eden + segments) |
| hits | HashIndex | Successful immediate or probed finds |
| misses | HashIndex | Lookup miss leading to allocation path |
| collisions | HashIndex | Insert collisions requiring ring fallback |
| stashHits | HashIndex | Lookup satisfied in the stash |
| ringHits | HashIndex | Lookup satisfied in the overflow ring |
| promotions | Pool / Segments | Strings promoted to higher tier |
| redirects | Pool / Segments | Redirect markers written |
| canonicalisations | Pool | Calls that returned a promoted final ID |

## Snapshots
`StringPoolMetrics.snapshotGlobal` – global-only  
`StringPoolMetrics.snapshotHierarchy` – aggregates global + all active group pools.

Returned structure:

PoolAggregateSnapshot = { Timestamp TotalLookups TotalHits TotalMisses TotalCollisions TotalStashHits TotalRingHits TotalPromotions TotalRedirects TotalCanonicalisations GlobalHitRatio }


## Observer Hub
`MetricsHub`:
- `register : MetricsObserver -> unit`
- `unregister : MetricsObserver -> unit`
- `publish : MetricEvent -> unit`
- Events emitted:
  - `Promotion(tier,value,newId)`
  - `Redirect(oldId,newId)`
  - `CollisionSpike(shard,collisions)` (future hook)
  - `Canonicalised(originalId,finalId)`
  - `AllocationDuplicate(value)` (segment emergency duplicate fallback)
  - `RingOverflowInsert(shard)`
  - `SnapshotTaken(timestamp,lookups,hits)`

No queue/backpressure; observers must be fast or offload work asynchronously.

## Optional Package Structure

Recommend splitting integration points into *add-on* NuGet packages:

| Package | Assembly | Purpose | Dependencies |
|---------|----------|---------|--------------|
| RMLPipeline.Core | (existing) | Core planning & execution & interning | None |
| RMLPipeline.Metrics | (new) | (Already in core) Snapshot/Hub – lightweight | None |
| RMLPipeline.Metrics.Diagnostics | Optional | System.Diagnostics.Metrics wrappers | `System.Diagnostics.DiagnosticSource` |
| RMLPipeline.Metrics.EventSource | Optional | ETW / LTTng EventSource provider | `System.Diagnostics.Tracing` |
| RMLPipeline.Metrics.OpenTelemetry | Optional | Export snapshots to OTEL MeterProvider | `OpenTelemetry`, `OpenTelemetry.Metrics` |
| RMLPipeline.Metrics.Prometheus | Optional | HTTP scraping (text exposition) | `prometheus-net` or user-provided |
| RMLPipeline.Metrics.Exporters | Optional | Common exporter base classes | Minimal |

### Suggested Design Pattern
1. Core library exposes pure APIs & events (already provided).
2. Each optional package:
   - Subscribes to `MetricsHub`.
   - Maintains internal accumulation / transformation.
   - Exposes configuration (e.g., export interval).
3. Users opt-in by referencing only the packages they need.

### Example: System.Diagnostics.Metrics Adapter (Pseudo)

```F#
let meter = new Meter("RMLPipeline.StringPool", "1.0.0") let lookups = meter.CreateObservableCounter("stringpool_lookups", fun _ -> [ Measurement(snapshot.TotalLookups) ]) ...
```

A hosted timer calls `snapshotHierarchy`.

### Example: EventSource
Define `EventSource` subclass in adapter package; on `SnapshotTaken` call `Write` with appropriate payload.

### Example: OpenTelemetry
Use `ObservableGauge` & `ObservableCounter` bound to snapshot snapshots.  
If high-frequency, consider caching snapshot for short interval to avoid overhead.

## Performance Considerations
- `Interlocked` increments are typically faster than contended locks; a few dozen counters have negligible impact.
- Snapshot cost is O(number of group pools); worker pools intentionally ignored by default to prevent scanning large ephemeral sets.
- Observer hub cost is proportional to observer count; each event iterates snapshot list.

## Customisation
Extend `PoolConfiguration` with flags:
- `EnableMetrics: bool` (can skip event publishing).
- `CollisionSpikeThreshold: int64` (emit `CollisionSpike` when exceeded).
Add later if needed.

## Testing Strategy
- Unit tests: verify snapshot monotonic growth.
- Property tests: simulate random operations, ensure hitRatio within expected bounds (0..1).
- Concurrency tests: high parallel interns + promotions – snapshot does not throw, counters non-negative.

## Future Enhancements
- Shard-level detail enumeration function.
- Delta snapshots (maintain previous snapshot for rate calculations).
- Histograms (allocation size, ring usage bursts) – would require external metrics library (hence optional package).
- Automatic collision spike detection & suggestion for resizing.

---

## Minimal Consumer Example

```F#
let hierarchy = StringPool.StringPool.create [| "a"; "b" |] let snap1 = StringPoolMetrics.snapshotHierarchy hierarchy printfn "Lookups=%d HitRatio=%.2f" snap1.TotalLookups snap1.GlobalHitRatio
```

Observer:
```F#
type ConsoleObserver() = interface MetricsObserver with member .onEvent evt = match evt with | Promotion(tier,v,id) -> printfn "[PROMO] %s %s -> %d" tier v id | SnapshotTaken(,lookups,hits) -> printfn "[SNAP] lookups=%d hits=%d" lookups hits | _ -> ()

MetricsHub.register (ConsoleObserver())
```


---

This approach keeps the core light while allowing rich operational telemetry for those who want it. Let me know if you’d like a concrete adapter example next (e.g., System.Diagnostics.Metrics or OpenTelemetry). 
