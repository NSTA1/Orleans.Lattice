# Orleans.Lattice

A distributed [B+ tree](https://en.wikipedia.org/wiki/B%2B_tree) library built on [Microsoft Orleans](https://learn.microsoft.com/dotnet/orleans/), designed for scalable ordered key-value storage across a cluster.

![CI](https://github.com/NSTA1/Orleans.Lattice/actions/workflows/ci.yml/badge.svg)
![Publish](https://github.com/NSTA1/Orleans.Lattice/actions/workflows/publish.yml/badge.svg)
[![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice)](https://www.nuget.org/packages/Orleans.Lattice)

## What is it?

Orleans.Lattice gives you a **sorted key-value store** that runs as a set of Orleans grains — no external database required. Keys are `string`, values are `byte[]`. You get point lookups, deletes, ordered key scans, and bulk loading, all distributed across a cluster of silos.

The name comes from its use of **lattice-based state primitives** (hybrid logical clocks, last-writer-wins registers, version vectors) — mathematical structures where merges are commutative, associative, and idempotent. This means operations are conflict-free, crash-safe, and recoverable without distributed locks or coordination protocols.

## Key Properties

| Property | How |
|---|---|
| **Bulk loading** | One-shot bottom-up build or streaming `IAsyncEnumerable` ingestion with per-shard parallel flushing. Both modes are idempotent and retryable. |
| **Conflict-free** | All state merges are monotonic. Concurrent writes to the same key resolve via last-writer-wins with hybrid logical clocks. |
| **Crash-safe splits** | Every node split uses a two-phase pattern with persisted intent. Interrupted splits resume automatically on the next access. |
| **Fast reads** | A `[StatelessWorker]` cache grain per silo serves reads via delta replication from the primary leaf. Cache misses cost a single version-vector comparison. |
| **Resize** | Change `MaxLeafKeys` or `MaxInternalChildren` on an existing tree. Takes an offline snapshot to a new physical tree, swaps the alias, and soft-deletes the old data. The tree is unavailable during the snapshot phase but immediately accessible after the swap. Undoable within the retention window. |
| **Scalable writes** | Keys are hash-sharded across 64 independent sub-trees (configurable). No single-root bottleneck. |
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
| [Benchmarks](docs/benchmarks.md) | Prerequisites, running benchmarks, interpreting results |
| [Bulk Loading](docs/bulk-loading.md) | One-shot build, streaming ingestion, two-phase graft, recovery guarantees |
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

## Contributing

Contributions are welcome! To get started:

1. Fork the repository and create a feature branch from `main`.
2. Make your changes and ensure all existing tests pass.
3. Add tests for any new functionality.
4. Open a pull request with a clear description of the change and the problem it solves.

Please open an issue first to discuss significant changes or new features before starting work.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
