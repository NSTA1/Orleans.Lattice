# Orleans.Lattice

A distributed [B+ tree](https://en.wikipedia.org/wiki/B%2B_tree) library built on [Microsoft Orleans](https://learn.microsoft.com/dotnet/orleans/), designed for scalable ordered key-value storage across a cluster.

![CI](https://github.com/NSTA1/Orleans.Lattice/actions/workflows/ci.yml/badge.svg)
![Publish](https://github.com/NSTA1/Orleans.Lattice/actions/workflows/publish.yml/badge.svg)
[![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice)](https://www.nuget.org/packages/Orleans.Lattice)

## What is it?

Orleans.Lattice gives you a **sorted, durable key-value store** that runs
entirely as a set of Orleans grains — no external database, no coordinator
service, no external queue. Keys are `string` and values are `byte[]` at
the core API; typed value helpers (see `TypedLatticeExtensions`) layer
automatic serialization on top — `System.Text.Json` by default, or any
`ILatticeSerializer<T>` you supply — so callers never have to hand-roll
`byte[]` conversions. You get
point lookups, deletes, ordered key scans (forward, reverse, and
range-bounded), bulk loading, snapshots, and tree aliasing, all
horizontally distributed across a cluster of silos.

The system is designed around a few core properties:

* **Self-organising under load.** Keys are hash-sharded across a virtual
  slot space mapped onto physical shards. When a shard gets hot, an
  autonomic monitor splits it online — no downtime, no lost writes, no
  coordination protocol. Cold shards stay cheap.
* **Strongly consistent from the outside.** Point reads, writes, and
  ordered scans always see a consistent view of the data, even while
  shards are splitting underneath. A concurrent `CountAsync` during a
  mid-split workload will return the exact number of live keys.
* **Crash-safe by construction.** Every multi-step operation — splits,
  promotions, bulk grafts, snapshots — persists its intent before any
  side effect. A silo crash mid-operation is recovered by the next
  reminder tick, replaying the exact same idempotent work.
* **Eventually convergent under failure.** State merges use hybrid
  logical clocks and last-writer-wins CRDTs. Storage faults, stale
  routing, and interrupted splits cannot corrupt data; once the fault
  window closes, the tree converges to the correct state.
* **No locks, no consensus round-trips.** All conflict resolution is
  algebraic (commutative, associative, idempotent merges). There is no
  Paxos, no Raft, no distributed lock manager.

Behavior is validated end-to-end by a pair of [chaos tests](docs/chaos-tests.md)
that hammer a live cluster with concurrent reads, writes, scans, and
shard splits — optionally with random storage-write faults — and assert
both live consistency and eventual convergence.

The name comes from its use of **lattice-based state primitives** —
mathematical structures where merges are commutative, associative, and
idempotent. This is what makes operations conflict-free and recoverable
without distributed locks or coordination protocols.

## Key Properties

| Property | How |
|---|---|
| **Adaptive shard splitting** | Hot shards split online via shadow-write + drain + swap + reject phases. Fully transparent: no downtime, no dropped writes, no coordination protocol. An autonomic monitor detects hot shards and triggers splits; shard splitting is not part of the public API and cannot be invoked externally. |
| **Atomic writes** | `SetManyAtomicAsync` provides all-or-nothing guarantees across multiple keys. If a write fails, the system automatically compensates using last-writer-wins with hybrid logical clocks. |
| **Bulk loading** | One-shot bottom-up build or streaming `IAsyncEnumerable` ingestion with per-shard parallel flushing. Both modes are idempotent and retryable. |
| **Conflict-free** | All state merges are monotonic. Concurrent writes to the same key resolve via last-writer-wins with hybrid logical clocks. |
| **Crash-safe splits** | Every node split uses a two-phase pattern with persisted intent. Interrupted splits resume automatically on the next access. |
| **Fast reads** | A `[StatelessWorker]` cache grain per silo serves reads via delta replication from the primary leaf. Cache misses cost a single version-vector comparison. |
| **Fault-tolerant** | Validated end-to-end by a parametrized fault-injection chaos test: random storage-write failures during concurrent reads, writes, scans, and splits converge to the correct state once faults stop. |
| **Resize** | Change `MaxLeafKeys` or `MaxInternalChildren` on an existing tree. Takes an offline snapshot to a new physical tree, swaps the alias, and soft-deletes the old data. The tree is unavailable during the snapshot phase but immediately accessible after the swap. Undoable within the retention window. |
| **Scalable writes** | Keys are hash-sharded across a configurable number of independent sub-trees (default 64). No single-root bottleneck. Shards split further at runtime as load grows. |
| **Strongly-consistent scans** | `CountAsync`, `KeysAsync`, and `EntriesAsync` return the exact live key set even during concurrent adaptive shard splits, via per-slot reconciliation against a monotonic `ShardMap.Version` and bounded optimistic retry. |
| **Snapshots** | Create a point-in-time copy of a tree: offline (locked - tree unavailable during snapshot process) or online (best-effort), with optional sizing overrides for the destination. |
| **Soft delete & recovery** | Trees can be soft-deleted with a configurable retention window. Recovery restores full access; purge permanently removes all data. |
| **Tombstone cleanup** | Reminder-driven compaction removes expired tombstones shard-by-shard, with crash-safe progress tracking. |
| **Tree registry** | An internal registry tree tracks all user trees, per-tree config overrides, and tree aliasing — enabling enumeration, resize, and snapshot without external metadata. |

## Quick Start

See the [API Reference](docs/api.md) for setup instructions, silo configuration, and full usage examples.

## Documentation

Detailed design documentation is split by concept:

| Document | Contents |
|---|---|
| [API Reference](docs/api.md) | Public `ILattice` interface, batch operations, options, serializable types |
| [Architecture](docs/architecture.md) | Grain layers, sharding, root promotion, bounded retry, grain mapping, capacity |
| [Atomic Writes](docs/atomic-writes.md) | `SetManyAtomicAsync` all-or-nothing guarantees, saga coordinator, compensation via LWW, crash recovery |
| [Benchmarks](docs/benchmarks.md) | Prerequisites, running benchmarks, interpreting results |
| [Bulk Loading](docs/bulk-loading.md) | One-shot build, streaming ingestion, two-phase graft, recovery guarantees |
| [Chaos Tests](docs/chaos-tests.md) | Happy-path and fault-injection chaos integration tests, invariants proven, recovery surfaces exercised |
| [Configuration](docs/configuration.md) | Options reference, per-tree overrides, immutability constraints, storage provider |
| [Read Caching](docs/caching.md) | Delta-based `[StatelessWorker]` cache, split-aware pruning |
| [Shard Splitting](docs/shard-splitting.md) | Adaptive online splits, shadow-write design, autonomic monitor, suppression rules, scan semantics during splits, tunables |
| [Snapshots](docs/snapshots.md) | Offline and online snapshot modes, crash safety, sizing overrides |
| [State Primitives](docs/state-primitives.md) | Hybrid logical clocks, LWW registers, monotonic split state, version vectors, state deltas |
| [Tombstone Compaction](docs/tombstone-compaction.md) | Reminder-driven cleanup, grace periods, configuration |
| [Tree Deletion](docs/tree-deletion.md) | Soft delete, recovery, manual purge, deferred purge |
| [Tree Registry](docs/tree-registry.md) | Internal registry tree, automatic registration, config priority, tree enumeration |
| [Tree Sizing](docs/tree-sizing.md) | Per-provider storage limits, leaf/internal node size estimation, sizing recommendations, resizing existing trees |
| [Tree Structure](docs/tree-structure.md) | Internal/leaf node layout, two-phase leaf splits, idempotent split propagation |

## Performance Characteristics

Orleans.Lattice inherits the asymptotic properties of a [B+ tree](https://en.wikipedia.org/wiki/B%2B_tree). In a single shard containing *n* keys with branching factor *b*:

| Operation | Time Complexity | What it means |
|---|---|---|
| Point read (`GetAsync`) | O(log<sub>b</sub> n) | Finding a key requires visiting one grain per tree level — typically 1–3 hops for millions of keys. |
| Insert / update (`SetAsync`) | O(log<sub>b</sub> n) | Same traversal as a read, plus an occasional split that propagates upward (amortised O(1) extra work). |
| Delete (`DeleteAsync`) | O(log<sub>b</sub> n) | Writes a tombstone at the leaf; no rebalancing. Tombstones are compacted in the background. |
| Ordered scan (`KeysAsync`) | O(n) | Leaves are linked — once the first leaf is found, iteration walks sibling pointers without revisiting internal nodes. |
| Count (`CountAsync`) | O(n / b) | Visits every leaf across all shards but skips internal nodes. |
| Space | O(n) | Each key-value pair is stored exactly once in a leaf node. Internal nodes hold only separator keys. |

**In plain terms:** because each node can hold ~128 children (the default branching factor), the tree is extremely shallow. A shard with two million keys is only three levels deep, so a single-key lookup crosses just three grains. Adding more data makes the tree wider, not deeper — doubling the key count adds at most one extra level. Scans are efficient because all values live in the leaves, which are chained together, so iterating a range never backtracks.

With sharding, the *n* in each shard is reduced by a factor of `ShardCount` (default 64), making per-shard trees even shallower. The trade-off is that cross-shard operations (`CountAsync`, `KeysAsync`, `EntriesAsync`) scatter-gather across all shards and merge the results.

## Contributing

Contributions are welcome! To get started:

1. Fork the repository and create a feature branch from `main`.
2. Make your changes and ensure all existing tests pass.
3. Add tests for any new functionality.
4. Open a pull request with a clear description of the change and the problem it solves.

Please open an issue first to discuss significant changes or new features before starting work.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
