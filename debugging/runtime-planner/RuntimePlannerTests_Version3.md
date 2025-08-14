# Runtime Planner Metadata Test Suite

This suite validates ONLY metadata production (no execution):

Included Property Categories:
1. Reference Extraction:
   Ensures the set of distinct placeholder names parsed from subject/predicate/object templates equals the ReferenceInfos length per map.

2. Tuple Requirement Masks:
   Verifies each TupleRequirement.RequiredMask represents precisely the union of references in its (Subject, Predicate, Object) compiled segments.

3. Join Descriptors:
   Confirms parent & child key reference indices exist, join key references are flagged with IsJoinKey, and indices remain in range.

4. PathRefIndex Coverage:
   Every ReferenceInfo appears at least once in the PathRefIndex under its SourcePathId.

5. Determinism:
   Re-building a RuntimePlan with identical inputs yields structurally identical metadata (map counts, reference counts, join shapes).

6. Large Reference Sets:
   When a map has >64 references, at least one tuple requirement uses a "large" RequirementMask (multi-word) or is empty (no refs).

7. Reference Isolation:
   Reference indices are zero-based per map (no cross-map reuse of index values meaning something globally).

8. Model-Based Sequence:
   A lightweight bespoke state machine issues sequences of commands (build, rebuild, inspect) and maintains invariants after each step.

## Extending

When execution logic is added:
- Additional properties should verify readiness bitset updates, join key hashing determinism, and absence of cross-plan contamination.

## Dependencies

Requires:
- Expecto
- FsCheck (>= 10.2.3)
- Existing Planner & DSL modules.

```fsharp
#r "Expecto.dll"
#r "FsCheck.dll"
```

Run:
dotnet test