# RMLPipeline Architecture

The design of the RMLPipeline library aims to solve for the core challenges in RML processing workloads by addressesing the fundamental challenges of template explosion, repeated patterns, and complex joins, while maintaining flexibility to adapt to different workload characteristics through configurable memory modes and adaptive indexing strategies.

Built to fit modern CPU architectures, memory hierarchies, and the specific characteristics of RML workloads, this library has been heavily influenced by RML-Mapper, RML-Streamer, RDFizer, and the work of countless other brilliant minds.

## Core Architecture - The RML Processing Challenge
RML processing presents a number of performance challenges:

- Template Explosion: A single template such as "http://example.org/{id}/{type}" can generate millions of unique strings
- Repeated Patterns: The same predicate and URI patterns can appear across multiple mappings
- Join Complexity: Reference object maps may require coordination of data across various iteration paths
- Memory Pressure: Naive string handling can cause excessive GC pressure in a garbage collected langauage, and cache misses remain a constant threat

Whilst F# may not seem like that obvious choice - Rust would be a great fit for this problem space, for example - wide availability of supporting libraries from the .NET ecosystem, along with a strong .NET presence in enterprise IT estates, all go in its favour.

## Planning

The planning algorithm performs dependency analysis of the transitive closure of dependencies, placing maps with no dependencies into independent groups, and maps with shared dependencies into the same group. This is primarily an enabler for parallelism, since independent groups can run with no synchronization, whilst intra-group dependencies that require coordination, can still parallelize non-dependent maps.

This also allows us to pre-computed our coordination requirements, since all synchronization points are known at planning time.


### Design Decisions - Layering
Inspired heavily by the work of Enrique Iglesias &al (TODO: cite refs), we propose a carefully layered architecture that supports pre-processing RML mappings. These can be seen as "compile time" or "buld time" activities, since they only need to be performed once by the mapping library and can be kept in memory thereafter.

┌─────────────────────────────────────────┐
│         RML Mapping Definition          │
└────────────────┬────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────┐
│      Planning Phase (Compile-time)      │
│  • String extraction & interning        │
│  • Dependency analysis                  │
│  • Join pre-computation                 │
│  • Index strategy selection             │
└────────────────┬────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────┐
│       Execution Phase (Runtime)         │
│  • Hot path processing                  │
│  • Streaming data ingestion             │
│  • Triple generation                    │
└─────────────────────────────────────────┘

### Design Decisions - String Interning
Our interning implementation uses a three-tier hierarchy, optimized for different access patterns and contention levels. 

- Tier 1: Global Pool
    - Strings discovered/process during planning are immutable after initialization, enabling lock-free concurrent reads
    - Fast lookups: (using O(1) ID-to-string resolution via direct array indexing)
    - Strings discovered at runtime use a separate structure to avoid polluting the read-hot path
- Tier 2: Worker Group Pool 
    - Each dependency group has its own pool, limiting contention to workers within the group
    - Most operations are reads (string lookups), so we optimize via `ReaderWriterLockSlim`
    - Strings specific to a processing group don't pollute the global namespace
- Tier 3: Worker Local Pool 
    - Each worker has exclusive access to its local pool, avoiding any contention
    - Small, fixed-size arrays are optimised to fit in L1/L2 cache
    - High-frequency strings stay in the local pool, making for a "fast path"

In addition, we avoid lookups on string values, instead using a struct to hold a 32bit integer key for them. (NB: it has been pointed out that we need to manage limits here, but I suspect for now we would exhaust system resources long before hitting Int32.MaxValue with our string ids).

Since structs are stack allocated by the CLR by default, we avoid allocation on the managed heap, and associated chasing of pointers. Multiple StringIds can also fit in a single cache line, and can be stored inline, directly in arrays without indirection (boxing).

We follow a fixed range allocation strategy for interning, that eliminates the need for coordination. This removes any need for global counter contention and provides O(1) tier access to string values. Lookups on strings are routed based on a provided `StringAccessPattern`, which allows "hot" strings to stay local and remain in CPU cache, makes shared strings accessible, and planning strings globally available. Strings can be gradually promoted, moving from local to group to global as needed.

Finally, we leverage the sizing of our ID data structures for maximum cache efficiency at runtime:

```F#
[<Struct>]
type PredicateTuple = {
    SubjectTemplateId: StringId
    PredicateValueId: StringId
    ObjectTemplateId: StringId
    SourcePathId: StringId
    Hash: uint64
    Flags: TupleFlags
}
```

This struct, for example, has been carefully designed for cache efficiency, being sized to exactly 24 bytes to fit in half a cache line, and naturally aligned for SIMD operations.

### Optimising The "Hot" Path
We fall back on low level programming to optimise our processing path during execution. This is mostly achieved through the use of carefully optimized data structures, especially the `HotPathTuple`:


Modern CPUs have multiple cache levels with drastically different access latencies, with the difference between L1 or L2 cache (at ~1 to ~3 ns) and Main Memory (around ~300 cycles, or ~75 ns), being several orders of magnitude.

Since x86-64 processors use 64-byte cache lines, the hot path execution optimization exploits cache line dynamics to keep data as close to the cache as possible:

```F#
[<Struct>]
type HotPathTuple = {
    SubjectTemplateId: StringId  // 4 bytes
    PredicateValueId: StringId   // 4 bytes
    ObjectTemplateId: StringId   // 4 bytes
    SourcePathId: StringId       // 4 bytes
    Hash: uint64                 // 8 bytes
    Flags: TupleFlags            // 1 byte
}  // Total: 25 bytes, padding to 32 bytes
```

Two `HotPathTuple` structs will fit in a single cache line, enabling the processor to fetch two tuples with one memory access. These are allocated onto the stack, and processed using Tail Call Optimised recursive algorithms, that ensure only a single stack frame is required for a given path. This provides us with several more benefits:

- Spatial locality: Stack data is held contiguously in memory
- Temporal locality: Recently accessed (i.e., pushed) data is far more likely to be accessed (i.e., popped) soon
- Predictable access: The CPU prefetcher can anticipate access patterns with ease
- Minimal allocations: A pre-allocated buffer avoids runtime allocation costs and, in .NET, GC cycles

The use of bit flags is also a "hot path" optimisation, aimed at aiding the CPU with branch prediction (TODO: reference). For example, consider the following checks:

```F#
member inline this.IsConstant = 
    this.Flags &&& TupleFlags.IsConstant <> TupleFlags.None
```

No branching takes place here, since bitwise operations don't cause branch mispredictions. The code is entirely vectorizable, such that the compiler can (hopefully) leverage SIMD instructions.

In additionk, when branches are needed, they follow patterns, optimized for prefetching:

```F#
let processGroups groupIndex =
    let groupMembers = plan.DependencyGroups.GetGroup(groupIndex)
    processMaps groupMembers 0
```

These patterns provide the benefit of sequential access, enabling the use of hardware prefetching - since the CPU should detect sequential access and prefetch ahead - reducing TLB lookups, and leveraging strong cache line utilization, since  full cache lines are used before eviction takes place.


## Outstanding Tasks/Issues

- parallel processing pipeline
- test harness using larger data-sets
- flushing/releasing strings, tokens, ids
- parsing RML & YARRRML
- outbound streams (DB endpoints, dotnetRDF, etc)