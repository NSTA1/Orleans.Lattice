# Orleans.Lattice

![CI](https://github.com/NSTA1/Orleans.Lattice/actions/workflows/ci.yml/badge.svg)
![Publish](https://github.com/NSTA1/Orleans.Lattice/actions/workflows/publish.yml/badge.svg)
[![NuGet](https://img.shields.io/nuget/v/Orleans.Lattice)](https://www.nuget.org/packages/Orleans.Lattice)

## What is it?

Orleans.Lattice is a **sorted, durable, horizontally-scalable key-value store** embedded in your Orleans cluster.

Keys are `string`, values are `byte[]`, and typed-value helpers layer automatic serialization on top. No external database, no coordinator service, no external queue.

It supports:

- Point reads, writes, deletes, and per-entry TTL.
- Ordered key and entry scans - forward, reverse, and range-bounded.
- Multi-key atomic writes with all-or-nothing visibility.
- Bulk loading from one-shot batches or streaming `IAsyncEnumerable` sources.
- Durable, resumable cursors that survive silo failovers and client restarts.
- Online resize, online reshard, and online snapshots (offline mode also available).
- Soft delete with a configurable retention window, and undo of resize within the window.
- Per-tree event stream, diagnostics, and `System.Diagnostics.Metrics` instruments.
- Optional cross-cluster replication via the sibling [`Orleans.Lattice.Replication`](docs/lattice.replication/replication.md) package.

The name comes from its use of **lattice-based state primitives** - mathematical structures where merges are commutative, associative, and idempotent - which is what makes the system conflict-free and recoverable without distributed locks or consensus.

## Core Properties

- **Self-organising under load.** Hot regions of the keyspace re-balance themselves online - no downtime, no lost writes, no coordination protocol. Cold regions stay cheap.
- **Strongly consistent from the outside.** Point reads, writes, and ordered scans always see a consistent view of the data, even while the cluster is rebalancing underneath. See [Consistency](docs/lattice/consistency.md) for the per-operation guarantee matrix.
- **Crash-safe by construction.** A silo crash at any point - mid-write, mid-split, mid-snapshot, mid-bulk-load - is recovered without operator intervention and without data loss.
- **Eventually convergent under failure.** Storage faults, stale routing, and interrupted operations cannot corrupt data; once the fault window closes, the tree converges to the correct state.
- **No locks, no consensus round-trips.** No Paxos, no Raft, no distributed lock manager. All conflict resolution is algebraic.

Behaviour is validated end-to-end by a suite of [chaos tests](docs/lattice/chaos-tests.md) that hammer a live cluster with concurrent reads, writes, scans, splits, resizes, and reshards - optionally with random storage-write faults - and assert both live consistency and eventual convergence.

## Features

| Feature | What it gives you | Docs |
|---|---|---|
| **Adaptive shard splitting** | Hot shards rebalance themselves online, transparently to callers. No downtime, no dropped writes, no externally-visible API. | [Shard Splitting](docs/lattice/shard-splitting.md) |
| **Atomic writes** | `SetManyAtomicAsync` provides all-or-nothing semantics across multiple keys - locally, across shards, and across replicating clusters. No reader ever observes a partial-set state. | [Atomic Writes](docs/lattice/atomic-writes.md) |
| **Bulk loading** | One-shot bottom-up build or streaming `IAsyncEnumerable` ingestion. Idempotent and retryable. | [Bulk Loading](docs/lattice/bulk-loading.md) |
| **Conflict-free merges** | Concurrent writes converge deterministically. | [State Primitives](docs/lattice/state-primitives.md) |
| **Cross-cluster replication** | Active-active replication between Orleans clusters. Any cluster can write to any tree; concurrent updates converge deterministically, and atomic multi-key writes remain all-or-nothing on every peer. | [Replication](docs/lattice.replication/replication.md) |
| **Diagnostics** | `DiagnoseAsync` returns a per-tree health snapshot: per-shard depth, live keys, tombstones, hotness, and recent splits. | [Diagnostics](docs/lattice/diagnostics.md) |
| **Durable cursors** | Server-checkpointed iterators that survive silo failovers, client restarts, and topology changes. Resume from the last yielded key automatically. | [Durable Cursors](docs/lattice/durable-cursors.md) |
| **Events** | Per-tree `LatticeTreeEvent` Orleans stream with operation-id correlation. | [Events](docs/lattice/events.md) |
| **Fast reads** | Per-silo read cache served via delta replication from the primary leaf. | [Read Caching](docs/lattice/caching.md) |
| **Fault-tolerant** | Validated end-to-end against parametrised fault injection. | [Chaos Tests](docs/lattice/chaos-tests.md) |
| **Metrics** | `System.Diagnostics.Metrics` instruments published on the `orleans.lattice` meter, ready for OpenTelemetry subscription. | [Metrics](docs/lattice/metrics.md) |
| **Online reshard** | Grow-only online migration of the physical shard count. | [Online Reshard](docs/lattice/online-reshard.md) |
| **Projection rebuild** | Cross-silo divergence detection with policy-driven recovery. | [Projection Rebuild](docs/lattice/projection-rebuild.md) |
| **Resize** | Change `MaxLeafKeys` or `MaxInternalChildren` on a live tree, undoable within the retention window. | [Tree Sizing](docs/lattice/tree-sizing.md) |
| **Retry policy** | Opt-in retry surface for transient storage faults with caller-supplied idempotency keys. Library default is zero ambient cost. | [Retry Policy](docs/lattice/retry-policy.md) |
| **Scalable writes** | Keys are sharded across many independent sub-trees. No single-root bottleneck. | [Architecture](docs/lattice/architecture.md) |
| **Snapshots** | Point-in-time copy of a tree - offline (source locked) or online (source available). | [Snapshots](docs/lattice/snapshots.md) |
| **Snapshot cursors** | Zero-observable-writes server-checkpointed iterators: every page reflects the tree state captured at open time, isolated from foreground writes, sagas, range deletes, and replication. | [Snapshot Cursors](docs/lattice/snapshot-cursors.md) |
| **Soft delete & recovery** | Trees can be soft-deleted with a configurable retention window. Recovery restores full access; purge permanently removes all data. | [Tree Deletion](docs/lattice/tree-deletion.md) |
| **Strongly-consistent scans** | `CountAsync`, `ScanKeysAsync`, and `ScanEntriesAsync` return the exact live key set even during concurrent rebalancing. | [Consistency](docs/lattice/consistency.md) |
| **Tombstone cleanup** | Background reaping of expired tombstones with crash-safe progress tracking. | [Tombstone Compaction](docs/lattice/tombstone-compaction.md) |
| **Tree registry** | Built-in enumeration of all user trees and their per-tree config overrides - no external metadata store required. | [Tree Registry](docs/lattice/tree-registry.md) |
| **TTL on `SetAsync`** | Per-entry time-to-live with absolute server-side expiry, preserved verbatim across splits, snapshots, resize, and replication. | [TTL](docs/lattice/ttl.md) |

## Quick Start

Register Lattice on a silo. `AddLattice` registers the grain catalogue, the grain storage provider (via the supplied callback), and the in-memory write-ahead-log backend in a single call:

```csharp verify
siloBuilder.AddLattice((silo, storageName) =>
    silo.AddMemoryGrainStorage(storageName));

// AddLattice registers the in-memory WAL by default - swap for a durable backend in production.

// elsewhere - on the client or inside a grain - resolve a tree by name and write a key:
var lattice = grainFactory.GetGrain<ILattice>("my-tree");
await lattice.SetAsync("hello", "world"u8.ToArray());
```

For production, swap the in-memory WAL for a durable backend - e.g. Azure Table Storage from the sibling package:

```csharp verify
siloBuilder
    .AddLattice((silo, storageName) => silo.AddMemoryGrainStorage(storageName))
    .AddAzureTableWalStorage(opts =>
    {
        opts.ConnectionString = "DefaultEndpointsProtocol=https;...";
    });
```

Add cross-cluster replication on top by registering `AddLatticeReplication(...)` alongside the WAL. See the [`Orleans.Lattice.Replication` overview](docs/lattice.replication/replication.md) for the full multi-cluster setup.

For full setup details, silo configuration options, and complete usage examples, see the [API Reference](docs/lattice/api.md). For runnable sample projects exercising `ILattice`, see [Samples](docs/lattice/samples.md).

## Reference

Use these documents for day-to-day use and operations:

- [API Reference](docs/lattice/api.md) - the public `ILattice` interface, batch operations, options, and serializable types.
- [Configuration](docs/lattice/configuration.md) - options reference, per-tree overrides, immutability constraints, storage provider.
- [Compression](docs/lattice/compression.md) - the public `ILatticeCompressor` seam, `AddLatticeCompressor` registration, tag-space partitioning, and how to plug in a custom algorithm.
- [Samples](docs/lattice/samples.md) - runnable sample projects exercising `ILattice`.
- [Benchmarks](docs/lattice/benchmarks.md) - prerequisites, running benchmarks, interpreting results.

For internals (the "how"):

- [Architecture](docs/lattice/architecture.md) - grain layers, sharding, root promotion, grain mapping, capacity.
- [Tree Structure](docs/lattice/tree-structure.md) - internal/leaf node layout, two-phase leaf splits, idempotent split propagation.
- [Tree Storage](docs/lattice/tree-storage.md) - per-provider storage limits, node size estimation, sizing recommendations.
- [WAL](docs/lattice/wal.md) - write-ahead log as the sole foreground-commit durability boundary.
- [WAL Causal+](docs/lattice/wal-causal-plus.md) - causal+ entry-schema extension, dependency satisfaction, snapshot semantics.
- [WAL Storage Providers](docs/lattice/wal-storage-providers.md) - `IWalStorageProvider` durability seam, in-memory default, optional Azure Table backend.

## Releases

Each publishable package is released by pushing a Git tag whose prefix is the literal folder name under `src/`, joined to the version with `-v`. The publish workflow auto-discovers the matching csproj and test project - adding a new package only requires creating `src/<name>/` and (optionally) `test/<name>/`, no workflow edits.

| Source folder | Tag pattern | NuGet package id |
|---|---|---|
| `src/lattice/` | `lattice-v<X.Y.Z>` | `Orleans.Lattice` |
| `src/lattice.replication/` | `lattice.replication-v<X.Y.Z>` | `Orleans.Lattice.Replication` |
| `src/lattice.replication.grpc/` | `lattice.replication.grpc-v<X.Y.Z>` | `Orleans.Lattice.Replication.Grpc` |
| `src/lattice.storage.azuretable/` | `lattice.storage.azuretable-v<X.Y.Z>` | `Orleans.Lattice.Storage.AzureTable` |
| `src/lattice.dashboards/` | `lattice.dashboards-v<X.Y.Z>` | `Orleans.Lattice.Dashboards` |

All packages publish from the same monorepo and version-lock together. Cross-package `<ProjectReference>` declarations pack as `>= <Version>` floors automatically, so a tag of `lattice.replication-v<X.Y.Z>` produces a NuGet package whose `Orleans.Lattice` dependency resolves to `>= <X.Y.Z>`.

To cut a release:

```powershell
git tag lattice.replication.grpc-v<X.Y.Z>
git push origin lattice.replication.grpc-v<X.Y.Z>
```

The publish workflow then runs the chaos and deterministic test suites for that package, packs with `-p:PackageVersion=<X.Y.Z>`, pushes to NuGet via OIDC, and creates a GitHub Release with auto-generated notes.

## Performance Characteristics

Orleans.Lattice inherits the asymptotic properties of a [B+ tree](https://en.wikipedia.org/wiki/B%2B_tree). In a single shard containing *n* keys with branching factor *b*:

| Operation | Time Complexity |
|---|---|
| Point read (`GetAsync`) | O(log<sub>b</sub> n) |
| Insert / update (`SetAsync`) | O(log<sub>b</sub> n) |
| Delete (`DeleteAsync`) | O(log<sub>b</sub> n) |
| Ordered scan (`ScanKeysAsync`) | O(n) |
| Count (`CountAsync`) | O(n / b) |
| Space | O(n) |

With the default branching factor (~128 children per node), a shard with two million keys is only three levels deep, so a single-key lookup crosses just three grains. Sharding (default 64) reduces per-shard *n* further; cross-shard operations scatter-gather across all shards.

## Contributing

Contributions are welcome! To get started:

1. Fork the repository and create a feature branch from `main`.
2. Make your changes and ensure all existing tests pass.
3. Add tests for any new functionality.
4. Open a pull request with a clear description of the change and the problem it solves.

Please open an issue first to discuss significant changes or new features before starting work.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
