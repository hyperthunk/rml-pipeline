namespace RMLPipeline.Internal

open System
open RMLPipeline.Internal.StringInterning
open RMLPipeline.Internal.StringPooling

(*
    StringPoolMetrics
    -----------------
    Pull-based snapshot utilities aggregating instrumentation from the
    hierarchical pools without introducing external dependencies.

    Design:
      - Hierarchy-wide snapshot merges global + group pools.
      - Worker pool instrumentation not aggregated by default (ephemeral);
        could be added by tracking active contexts (optional later).
      - All counters are monotonic (since process start) – consumers
        compute deltas or rates themselves.

    EXTENSION POINTS:
      * Add per-shard enumeration if shard-level hotspots required
      * Emit MetricEvent.SnapshotTaken via MetricsHub

*)

[<Struct>]
type PoolAggregateSnapshot = {
    Timestamp: DateTime
    TotalLookups: int64
    TotalHits: int64
    TotalMisses: int64
    TotalCollisions: int64
    TotalStashHits: int64
    TotalRingHits: int64
    TotalPromotions: int64
    TotalRedirects: int64
    TotalCanonicalisations: int64
    GlobalHitRatio: float
}

module StringPoolMetrics =

    let private safeDivide a b =
        if b = 0L then 0.0 else float a / float b

    let snapshotGlobal (hierarchy: StringPoolHierarchy) =
        let globalInstr = hierarchy.Global.RuntimePool.getInstrumentation()
        let gLook = globalInstr.Index.Lookups
        let gHits = globalInstr.Index.Hits
        let gMiss = globalInstr.Index.Misses
        let ratio = safeDivide (gHits) (max 1L gLook)
        {
            Timestamp = DateTime.UtcNow
            TotalLookups = gLook
            TotalHits = gHits
            TotalMisses = gMiss
            TotalCollisions = globalInstr.Index.Collisions
            TotalStashHits = globalInstr.Index.StashHits
            TotalRingHits = globalInstr.Index.RingHits
            TotalPromotions = globalInstr.Promotions
            TotalRedirects = globalInstr.Redirects
            TotalCanonicalisations = globalInstr.Canonicalisations
            GlobalHitRatio = ratio
        }

    let snapshotHierarchy (hierarchy: StringPoolHierarchy) =
        let g = snapshotGlobal hierarchy
        // Aggregate group pools
        let mutable lookups = g.TotalLookups
        let mutable hits = g.TotalHits
        let mutable misses = g.TotalMisses
        let mutable collisions = g.TotalCollisions
        let mutable stashHits = g.TotalStashHits
        let mutable ringHits = g.TotalRingHits
        let mutable promotions = g.TotalPromotions
        let mutable redirects = g.TotalRedirects
        let mutable canonical = g.TotalCanonicalisations

        for gp in hierarchy.GroupPools.Values do
            let instr = gp.GroupRuntime.getInstrumentation()
            lookups <- lookups + instr.Index.Lookups
            hits <- hits + instr.Index.Hits
            misses <- misses + instr.Index.Misses
            collisions <- collisions + instr.Index.Collisions
            stashHits <- stashHits + instr.Index.StashHits
            ringHits <- ringHits + instr.Index.RingHits
            promotions <- promotions + instr.Promotions
            redirects <- redirects + instr.Redirects
            canonical <- canonical + instr.Canonicalisations

        let snapshot = {
            Timestamp = DateTime.UtcNow
            TotalLookups = lookups
            TotalHits = hits
            TotalMisses = misses
            TotalCollisions = collisions
            TotalStashHits = stashHits
            TotalRingHits = ringHits
            TotalPromotions = promotions
            TotalRedirects = redirects
            TotalCanonicalisations = canonical
            GlobalHitRatio = safeDivide hits (max 1L lookups)
        }

        // Publish event (lightweight)
        StringInterning.MetricsHub.publish (MetricEvent.SnapshotTaken (snapshot.Timestamp, snapshot.TotalLookups, snapshot.TotalHits))
        snapshot