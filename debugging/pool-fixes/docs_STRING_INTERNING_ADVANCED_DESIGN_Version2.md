# Advanced String Interning Design (HashIndex + Redirect Path Compression)
## notes

### Canonicalise before inserting into shared hash structures

If at execution time you begin interning ad hoc derived strings (e.g., dynamically built URIs, datatype expansions, language tags), calling canonicalise before inserting them into a shared hash structure (like a join cache or predicate emission table) will reduce potential duplication across tiers.

### Join keys & hashing.

If join keys are built from the raw string values (via GetString) nothing changes. 

If you later decide to hash on StringId directly, be aware that a non-canonical runtime ID might later get promoted (redirect). 

In that scenario either:
  Use canonicalise(id) before hashing (preferred), or 
  Accept that the same logical string could appear under multiple IDs in the join cache (correctness preserved but possible duplication).

For current planner design (pre-hash join indices computed at planning and runtime key built from resolved field values) no action is required.

### Optional optimization:

If you plan to widen dynamic runtime string usage (e.g., function execution in FNML leading to many value expansions), consider pooling canonicalised IDs for repeated emission to reduce the ring fallback usage in high-churn scenarios.

### Resize Race & Shard Resizing Risk Assessment
What we have now:

Fixed-size sharded hash index per tier (Eden, Segments) with:
Primary array sized with low target load factor (≈15%).
Bounded Robin Hood probe depth.
Fixed-size stash (e.g., 16).
Fixed-size ring overflow (e.g., 64).
No live resizing / rehashing path; no shard version swapping.
What the “resize race” enhancement would add:

Allocate a larger shard (arrays + stash + ring).
Populate new arrays.
Atomic pointer swap to the new shard structure.
Old shard left for deferred reclamation (RCU / epoch-based or ref counting) to avoid ABA or dangling reads.
How serious is the risk without resizing?

A. Functional correctness risk: LOW.

Even at very high collision rates the index guarantees forward progress: worst-case it falls back to stash, then ring, or (in absolute worst pathological saturation) duplicates a string (allocates new ID). Correctness (string → string) is preserved; duplicates are allowed by design with low probability.
B. Performance risk: MODERATE under these conditions:

Actual unique string count per shard significantly exceeds capacity planning (e.g., you sized for ~100k but workloads push several hundred thousand truly distinct runtime strings).
Adversarial or skewed hashing (lots of keys sharing hash fragment region) increases stash + ring hit frequency.
Very small configured stash/ring sizes relative to unpredictable workload diversity.
Consequences:

Increased fallback path executions.
More ring scans (O(R) with R = ring size, small constant like 64 but repeated often).
Higher duplicate ratio if ring entries are overwritten before reuse detection.
Slightly higher allocation pressure because duplicates reduce effective dedup memory savings.
C. Latency profile impact:

The 99th percentile lookup latency could grow if many lookups escalate beyond primary array + short probe path.
Still bounded: (MaxProbeDepth + StashSize + RingSize) is constant. With defaults (8 + 16 + 64) you have at most ~88 slots examined per miss path— acceptable but more expensive than the ideal O(1–3) typical path.
D. Memory impact:

Duplicate string entries inflate memory (strings stored again in Eden or segments).
For RML workloads that reuse many template-based fragments, you likely stay within expected range; worst-case if the data source produces many distinct literal values (e.g., large free-text fields) you will accumulate more unique strings anyway— resizing primarily helps speed, not total memory (unique strings dominate allocation).
E. When do you actually need dynamic resizing? Indicators:

Collision metrics: (InsertCollisions / TotalInserts) consistently above, say, 5–10%.
Stash occupancy near 100% for sustained periods (e.g., >90% full over X seconds).
Ring hit ratio (ringHits / lookups) > a small threshold (e.g., 1–2%).
Duplicate detection rate (if you add instrumentation comparing canonicalise usage) trending upward.
F. Race risk specifically from not implementing resizing now:

There is no race window that returns WRONG strings— only potential duplicates or fallback scanning cost.
Without resizing there is no DAU-dependent concurrency hazard like ABA or torn reads because arrays are immutable after allocation except for slot-level atomic writes.
G. If a shard saturates completely (extreme case):

Additional distinct strings fall immediately to ring fallback or allocate duplicates.
Still safe, but efficiency degrades.
Implementation effort vs benefit trade-off:

Adding RCU-style resizing adds complexity (tracking old references, hazard-pointer or epoch scheme).
If your workload’s cardinality and string churn are well understood (RML typical: many reused field names, paths, IRIs, moderate variety of literal values), you can defer resizing until metrics prove necessary.
Recommendation:

Instrument first (expose per-shard counters: collisions, stashUsed, ringWrites, ringHits).
Set alert thresholds (e.g., collisions/lookups > 0.05).
Only implement resizing if thresholds are exceeded in realistic load tests.
If you do implement resizing:

Use a per-shard “resize lock” (lightweight spin + backoff).
Readers read shard pointer once; never traverse old pointer after slot operations.
After atomic swap, either:
Keep old shard in a per-thread retire list with epoch fences, or
Use reference counting (increment at function entry, decrement at exit; free when count=0).
Complexity cost: medium; not urgent unless metrics justify.
Summary Risk Table:

Risk Dimension	Severity Without Resize	Notes
Correctness	Low	Duplicates tolerated; no wrong-string returns expected.
Average Latency	Low	Primary path remains fast under typical load.
Tail Latency (P99)	Moderate	Increases if fallback paths frequently used.
Memory Overhead	Moderate (workload-dependent)	More duplicates if high churn & no resize.
Engineering Complexity	High (if resizing added)	Delay until metrics justify.

### Recommendation

Proceed with planner/runtime integration as-is.
Add metrics overlay (collisions, stash occupancy, ring hits) and expose via logs or counters.
(Optional) In join or emission caches, canonicalise IDs if you see promotions causing differing IDs of same logical string.
Set simple guardrails: if (collisions/lookups) > 0.05 or (ringHits/lookups) > 0.01 in sustained scenarios, schedule a resizing feature implementation.
Defer shard resizing until empirical evidence demands it.

## Overview

This document describes the advanced implementation for:

1. Lock‑free(ish) hash indexing without `ConcurrentDictionary` overflow.
2. Promotion redirects & (current single-level) path compression.
3. Canonicalisation API.
4. Testing via FsCheck state-machine.
5. Race patterns & mitigations.

---

## 1. Hash Indexing (Overflow Removal)

### Structure
Per shard:
- Primary arrays (open addressing + Robin Hood bounded probe)
  - `StringKeys[]`
  - `HashFragments[]`
  - `PackedLocations[]`
  - `Bitmap` (fast existence bit)
- Stash (fixed-size array)
- Ring overflow (fixed-size array)

### Insert Algorithm
1. Compute hash, slot.
2. Try claim slot (CAS on `StringKeys[slot]`).
3. If collision and different key:
   - Bounded Robin Hood probe up to `MaxProbeDepth`.
4. If still not placed:
   - Attempt stash claim (CAS on `state`).
5. If stash full:
   - Insert into ring with atomic increment head (overwrite oldest).

### Lookup
1. Bitmap quick fail.
2. Slot compare fragment + key.
3. Probe (bounded).
4. Stash scan (small constant size).
5. Ring scan (bounded).

### Properties
- No global locks.
- Bounded worst-case cost.
- Accepts duplicates only if all fallback structures saturated (extremely rare).

---

## 2. Promotion + Redirects

### Basic Promotion Flow
- Local -> Group -> Global.
- Promotion policy decides when based on temperature vs adaptive threshold.
- When promoting:
  1. Allocate new ID in upper tier (dedup there).
  2. Mark lower-tier slot with redirect sentinel (high bit set, packed canonical ID).
- Subsequent lookups:
  - If slot promoted, return canonical ID (or escalate).
- Path compression (future multi-level) can compress chains by rewriting intermediate redirect slots to final ID.

Current patch: at most one redirect level per tier boundary.

---

## 3. Canonicalisation

`canonicalise(id)`:
- Returns canonical (final) ID if redirect.
- Temperature metadata is **not** preserved intentionally—canonical char semantics > local thermodynamics.

Use cases:
- Join caches want stable / canonical IDs.
- Dedup counting.

---

## 4. Testing (State Machine)

We leverage FsCheck `Machine<Model,Env>`:
- Operations: Intern / Reintern / Canonicalise / RoundTrip / TempExercise / StatsSnapshot / InternMany.
- Model tracks:
  - Known (string -> last ID)
  - Reverse (ID -> string)
- Postconditions enforce invariants.

Key invariants:
- Round-trip integrity.
- Canonicalisation returns valid ID.
- Temperatures monotonic (approx via repeated interns).
- ID stability before promotion.

---

## 5. Race Patterns & Solutions

| Race | Description | Mitigation |
|------|-------------|-----------|
| Duplicate Eden allocate | Two threads miss & allocate same string | EdenIndex lookup (pre), atomic slot claim |
| Promotion vs increment | CAS on packed entry updates temperature or sees promoted sentinel | Loop restart on CAS failure; sentinel short-circuits |
| Multi promotion | Two threads promote concurrently | Upper-tier dedup & CAS redirect ensures single redirect written |
| Redirect chain growth | Potential multi-level path | (Current) single-level; future: path compression rewriting |
| Hash collision storms | Adversarial keys hitting same slots | Stash + ring fallback, bounded; duplicates tolerated only as last resort |
| Stash overflow spin | Repeated CAS fails on stash | Ring fallback guarantees forward progress |
| Emergency allocation (segments) | Chunk growth contention | Controlled lock acquisition with fallback allocate |

---

## 6. Metrics (Future Extensions)

Recommended counters (placeholders):
- `redirectResolutions`
- `redirectCompressed`
- `promotionsTriggered`
- `stashOccupancyHighWater`
- `ringInsertions`

---

## 7. Future Enhancements

1. Full path compression (multi-hop flatten).
2. Background reconciliation for ring duplicates (optional).
3. Memory reclamation / resizing under high churn.
4. Pluggable promotion policies per tier.
5. Telemetry integration for shard collision density.

---

## 8. Limitations

- Current redirect implementation does not flatten across >1 tier automatically (single hop).
- Temperature encoded ID loses original temperature after canonicalisation.
- Ring buffer scan is O(k) with small constant; heavy adversarial loads could increase latency slightly.

---

## 9. Conclusion

This patch brings the implementation in line with the original design goals:
- Near lock-free, bounded collision handling.
- Predictable and stable ID semantics.
- Observable promotion and redirect logic.
- Comprehensive property-based and model-based testing support.

For additional features (multi-level compression, advanced metrics), incremental patches can follow without API breaks.