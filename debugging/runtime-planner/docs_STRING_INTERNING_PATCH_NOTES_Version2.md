# String Interning Patch Notes (Promotion + Stable IDs)

## Overview
This patch implements the originally intended semantics:
- Eden de-duplication via a lock-free sharded `StringIndex`
- Stable IDs for repeated interns in the same tier (unless promotion redirect)
- In-place temperature increments via CAS
- Synchronous promotion with redirect marking (lower-tier entry becomes a redirect pointing to higher-tier ID)
- Segment (post-Eden) reuse and de-dup supported through existing index
- Adaptive promotion threshold scaling
- New property tests verifying stability, promotion, redirect correctness, and temperature monotonicity

## Key Changes
1. Added `EdenIndex` to `Pool` and lookup-first logic in `InternString`.
2. CAS loop for temperature increments (Eden + Segments).
3. Promotion delegate (`PromoteDelegate`) injected so:
   - Local pool promotes to Group or Global
   - Group pool promotes to Global
   - Global runtime has no higher tier (promotion disabled)
4. Redirect marking using high-bit sentinel (negative packed entry).
5. Retrieval (`TryGetString` / `GetStringWithTemperature`) tolerates redirect by returning `None` at lower tier so higher tier resolution path is used (Group/Local contexts chain upwards).
6. Segment path unifies allocation and reuse: `AllocateOrReuseString`.
7. New tests in `StringInterningPromotionTests.fs`.

## Test Additions
- Stable ID test (`Repeated high-frequency intern returns stable ID`)
- Promotion redirect test (`Temperature increases and triggers promotion redirect`)
- Redirect integrity test (old ID still resolves correct string)
- Segment de-dup test
- Temperature monotonic property test

## Follow-Up / TODO
- Optional: Enhance redirect retrieval to directly follow chain rather than returning None and relying on caller tier logic.
- Replace `ConcurrentDictionary` in overflow collision path of `HashIndexing.StringIndex` with custom lock-free secondary structure if collisions become significant.
- Add background reconciliation for emergency allocations (not yet implemented).
- Refine adaptive thresholds with moving averages or percentile histograms for more stable promotion pacing.

---