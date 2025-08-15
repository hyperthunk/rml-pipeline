# Join Key Canonicalisation

## Purpose
Join caches / hash tables benefit from using canonical (promotion-stable) identifiers:
- Prevents duplicate buckets for the same logical string that has moved tiers (local → group → global).
- Improves join hit ratio and reduces memory fragmentation in join state tables.

## API
`JoinKeyCanonicaliser` exposes:

| Function | Description |
|----------|-------------|
| `canonicaliseIds scope ids` | Applies pool cascade canonicalisation per id. |
| `hashCanonicalIds ids` | Computes deterministic FNV‑1a based 64‑bit hash. |
| `canonicalisedJoinHash scope ids` | Combines canonicalisation + hashing (ordered). |
| `canonicalisedJoinHash2 scope parent child` | Parent + child arrays hashed together (ordered). |
| `canonicalisedOrderIndependentHash scope ids` | Sorts canonical IDs before hashing (commutative key). |

## When To Use Ordered vs Order Independent
- Ordered: Typical foreign-key style joins where key component order is part of schema.
- Order Independent: Un-ordered composite sets (e.g., symmetric relationships, bag-like keys).

## Hash Choice
FNV‑1a 64-bit with a light avalanche step:
- Fast, stable, good enough distribution for moderate key volumes.
- If cryptographic or stronger mixing needed, upgrade to XXH3_64 or a widen/merge pattern (pluggable).

## Promotion Interplay
- A lower-tier ID may become a redirect. Canonicalise eliminates obsolete intermediate IDs.
- Temperature is intentionally discarded in canonical ID (semantic identity only).

## Edge Cases
- Empty key array → hash = 0UL (explicitly defined).
- Duplicate IDs allowed; repeated canonical ID hashing still stable.

## Testing
`JoinKeyCanonicaliserTests`:
- Validates empty case.
- Checks stability under repeated promotion attempts.
- Confirms order sensitivity (ordered variant) vs invariance (order-independent variant).

---