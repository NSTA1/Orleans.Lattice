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
- Multi-key atomic writes with all-or-nothing visibility - within a tree, and across multiple trees.
- Bulk loading from one-shot batches or streaming `IAsyncEnumerable` sources.
- Durable, resumable cursors that survive silo failovers and client restarts.
- Online resize, online reshard, and online snapshots (offline mode also available).
- Soft delete with a configurable retention window, and undo of resize within the window.
- Per-tree event stream, diagnostics, and `System.Diagnostics.Metrics` instruments.
- Optional cross-cluster replication via the sibling [`Orleans.Lattice.Replication`](docs/lattice.replication/README.md) package.

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
| **Atomic writes** | `SetManyAtomicAsync` provides all-or-nothing semantics across multiple keys - locally, across shards, and across replicating clusters. An `IGrainFactory.SetManyAtomicAsync` overload extends the same all-or-nothing visibility to a batch spanning multiple trees. No reader ever observes a partial-set state. | [Atomic Writes](docs/lattice/atomic-writes.md) |
| **Bulk loading** | One-shot bottom-up build or streaming `IAsyncEnumerable` ingestion. Idempotent and retryable. | [Bulk Loading](docs/lattice/bulk-loading.md) |
| **Conflict-free merges** | Concurrent writes converge deterministically. | [State Primitives](docs/lattice/state-primitives.md) |
| **Cross-cluster replication** | Active-active replication between Orleans clusters. Any cluster can write to any tree; concurrent updates converge deterministically, and atomic multi-key writes remain all-or-nothing on every peer. | [Replication](docs/lattice.replication/README.md) |
| **Diagnostics** | `DiagnoseAsync` returns a per-tree health snapshot: per-shard depth, live keys, tombstones, hotness, and recent splits. | [Diagnostics](docs/lattice/diagnostics.md) |
| **Durable cursors** | Server-checkpointed iterators that survive silo failovers, client restarts, and topology changes. Resume from the last yielded key automatically. | [Durable Cursors](docs/lattice/durable-cursors.md) |
| **Events** | Per-tree `LatticeTreeEvent` Orleans stream with operation-id correlation. | [Events](docs/lattice/events.md) |
| **Fast reads** | Per-silo read cache served via delta replication from the primary leaf. | [Read Caching](docs/lattice/caching.md) |
| **Fault-tolerant** | Validated end-to-end against parametrised fault injection. | [Chaos Tests](docs/lattice/chaos-tests.md) |
| **Metrics** | `System.Diagnostics.Metrics` instruments published on the `orleans.lattice` meter, ready for OpenTelemetry subscription. | [Metrics](docs/lattice/metrics.md) |
| **Online reshard** | Grow-only online migration of the physical shard count. | [Online Reshard](docs/lattice/online-reshard.md) |
| **Performance** | Approximate single-silo throughput and per-call latency for point reads, point writes, multi-key batches, and atomic sagas, measured against real Azure Tables. | [Performance: single-silo guide](docs/lattice/performance-single-silo.md) |
| **Predicate operations** | Filter typed reads, conditional writes, atomic batches, scans, cursors, and range deletes with an ordinary `Expression<Func<T, bool>>` evaluated server-side; only matching keys or values cross the wire. | [Predicate Operations](docs/lattice/predicated-operations.md) |
| **Projection rebuild** | Cross-silo divergence detection with policy-driven recovery. | [Projection Rebuild](docs/lattice/projection-rebuild.md) |
| **Queues** | Typed, cluster-internal FIFO queues backed by a reserved system tree, with optional bounded FIFO eviction. | [Queues](docs/lattice/queues.md) |
| **Resize** | Change `MaxLeafKeys` or `MaxInternalChildren` on a live tree, undoable within the retention window. | [Tree Sizing](docs/lattice/tree-sizing.md) |
| **Retry policy** | Opt-in retry surface for transient storage faults with caller-supplied idempotency keys. Library default is zero ambient cost. | [Retry Policy](docs/lattice/retry-policy.md) |
| **Scalable writes** | Keys are sharded across many independent sub-trees. No single-root bottleneck. | [Architecture](docs/lattice/architecture.md) |
| **Snapshots** | Point-in-time copy of a tree - offline (source locked) or online (source available). | [Snapshots](docs/lattice/snapshots.md) |
| **Snapshot cursors** | Zero-observable-writes server-checkpointed iterators: every page reflects the tree state captured at open time, isolated from foreground writes, sagas, range deletes, and replication. | [Snapshot Cursors](docs/lattice/snapshot-cursors.md) |
| **Soft delete & recovery** | Trees can be soft-deleted with a configurable retention window. Recovery restores full access; purge permanently removes all data. | [Tree Deletion](docs/lattice/tree-deletion.md) |
| **State model** | WAL is canonical; leaf state row holds topology + checkpoint only; CRDT keys use delta-only producer-side mutation. | [State Model](docs/lattice/state-model.md) |
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

Add cross-cluster replication on top by registering `AddLatticeReplication(...)` alongside the WAL. See the [`Orleans.Lattice.Replication` overview](docs/lattice.replication/README.md) for the full multi-cluster setup.

For full setup details, silo configuration options, and complete usage examples, see the [API Reference](docs/lattice/api.md). For runnable sample projects exercising `ILattice`, see [Samples](docs/lattice/samples.md).

## Reference

Use these documents for day-to-day use and operations:

- [API Reference](docs/lattice/api.md) - the public `ILattice` interface, batch operations, options, and serializable types.
- [Configuration](docs/lattice/configuration.md) - options reference, per-tree overrides, immutability constraints, storage provider.
- [Predicate Operations](docs/lattice/predicated-operations.md) - server-side predicate push-down for typed reads, conditional and atomic writes, scans, cursors, and range deletes.
- [Queues](docs/lattice/queues.md) - the public `ILatticeQueue<T>` cluster-internal FIFO primitive, bounded-queue eviction, and throughput guidance.
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
- [WAL Tuning](docs/lattice/wal-tuning.md) - how `WalMaxPendingBatches` and `WalPartitions` interact with a durable backend's throughput envelope; default sizing rules and the storage-account ceiling above which the cap stops helping.
- [WAL Saturation Signal](docs/lattice/wal-saturation-signal.md) - the per-tree, three-state back-pressure surface (`IWalSaturationSignal`, `IWalSaturationObserver`) that lets callers throttle offered load before silent queueing on the writer-side admission gate.

For feature tracking (the "what's planned / what shipped"):

- [Core Feature Index](docs/lattice/features.md) - grouped, issue-linked index of the core `Orleans.Lattice` package's tracked features, fixes, and gaps.
- [Replication Feature Index](docs/lattice.replication/features.md) - grouped, issue-linked index of the `Orleans.Lattice.Replication` package's tracked features, fixes, and gaps.

For replication operations (the "how do I run it"):

- [Automatic Drift Remediation](docs/lattice.replication/automatic-drift-remediation.md) - operator playbook for the opt-in anti-entropy stack: default-off posture, end-to-end opt-in configuration, the consolidated metrics surface, and a failure-mode matrix.

Feature planning is managed on [GitHub Issues](https://github.com/NSTA1/Orleans.Lattice/issues), not in roadmap files. The feature-index pages above summarize and link to those issues.

## Releases

See [CHANGELOG.md](CHANGELOG.md) for the per-version notes and [docs/RELEASING.md](docs/RELEASING.md) for the per-package tag-and-publish protocol.


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
