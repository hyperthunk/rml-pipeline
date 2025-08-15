namespace RMLPipeline.Internal

(*
    JoinKeyCanonicaliser
    --------------------
    Utility helpers for (future) execution phase join processing that
    normalise (canonicalise) StringIds before they are hashed / placed
    into a join cache.

    Rationale:
      - Promotion can redirect a lower–tier StringId to a higher–tier
        canonical StringId. If join caches are keyed directly on the
        non–canonical ID, the same logical value can occupy multiple
        buckets, increasing memory and weakening locality.
      - Canonicalising reduces duplicate entries and increases the
        likelihood of cache hits on joins involving hot strings.

    Design:
      - We do NOT force consumers to canonicalise inside core planner
        metadata (that stays pure / planning-time).
      - At runtime (execution layer) the caller passes a StringPooling
        scope (PoolContextScope) which already encapsulates the cascade
        (local -> group -> global) and exposes Canonicalise.
      - We canonicalise IDs ONLY (no re-resolution of string content).
        We retain temperature if useful downstream (current canonicalise
        loses temperature by design; treat temperature as ephemeral).

    Hashing:
      - Provided FNV-1a 64-bit helper for key stability.
      - Key structure example: length-prefixed list of 32-bit halves of
        canonical IDs OR direct folding. Here we combine by FNV on the
        canonical integer value (little endian bytes) for simplicity.

    NOTE: This module is intentionally decoupled from any future executor
    so it can be unit tested now.

*)

open System
open System.Runtime.CompilerServices
open RMLPipeline.Internal.StringPooling
open RMLPipeline.Internal.StringInterning

[<RequireQualifiedAccess>]
module JoinKeyCanonicaliser =

    [<MethodImpl(MethodImplOptions.AggressiveInlining)>]
    let private fnv1a64 (data:int) (seed:uint64) =
        // simple 4-byte feed
        let prime = 1099511628211UL
        let mutable h = seed
        // little-endian expansion of int
        let b0 = byte (data &&& 0xFF)
        let b1 = byte ((data >>> 8) &&& 0xFF)
        let b2 = byte ((data >>> 16) &&& 0xFF)
        let b3 = byte ((data >>> 24) &&& 0xFF)
        h <- (h ^^^ uint64 b0) * prime
        h <- (h ^^^ uint64 b1) * prime
        h <- (h ^^^ uint64 b2) * prime
        h <- (h ^^^ uint64 b3) * prime
        h

    /// Canonicalise each StringId via the pool scope.
    let canonicaliseIds (scope: StringPool.StringPool.PoolContextScope) (ids: StringId[]) =
        ids |> Array.map scope.Canonicalise

    /// Compute a stable 64-bit join key hash from canonicalised IDs.
    /// Empty array => 0UL.
    let hashCanonicalIds (canonicalIds: StringId[]) =
        let mutable h = 14695981039346656037UL
        for id in canonicalIds do
            h <- fnv1a64 id.Value h
        // final avalanche (xorshift mix) for better distribution
        h <- h ^^^ (h >>> 32)
        h <- h * 0x9E3779B185EBCA87UL
        h <- h ^^^ (h >>> 29)
        h

    /// Convenience working from raw (possibly non-canonical) IDs.
    let canonicalisedJoinHash (scope: StringPool.StringPool.PoolContextScope) (ids: StringId[]) =
        ids |> canonicaliseIds scope |> hashCanonicalIds

    /// Pair-based (e.g. parent/child join keys)
    let canonicalisedJoinHash2 (scope: StringPool.StringPool.PoolContextScope) (parent: StringId[]) (child: StringId[]) =
        let total = parent.Length + child.Length
        let tmp = Array.zeroCreate<StringId> total
        if parent.Length > 0 then Array.Copy(parent, 0, tmp, 0, parent.Length)
        if child.Length > 0 then Array.Copy(child, 0, tmp, parent.Length, child.Length)
        canonicalisedJoinHash scope tmp

    /// Derive a composite sorted-key hash (useful if key order should NOT matter).
    /// Caller decides if ordering invariance is desired; we sort by int value.
    let canonicalisedOrderIndependentHash (scope: StringPool.StringPool.PoolContextScope) (ids: StringId[]) =
        let c = canonicaliseIds scope ids
        Array.sortInPlaceBy (fun (x:StringId) -> x.Value) c
        hashCanonicalIds c