# Orleans.Lattice

A **sorted, durable, horizontally-scalable key-value store** embedded directly in your [Microsoft Orleans](https://learn.microsoft.com/dotnet/orleans/) cluster. Keys are `string`, values are `byte[]`, and typed-value helpers layer automatic serialization on top. No external database, no coordinator service, no external queue.

The keyspace is sharded across many independent, self-balancing [B+ trees](https://en.wikipedia.org/wiki/B%2B_tree) whose state is backed by **lattice-based CRDT primitives** - merges that are commutative, associative, and idempotent - which is what lets the store converge deterministically without distributed locks or consensus.

## What it gives you

- **Point operations** - reads, writes, deletes, and per-entry TTL, all `O(log n)`.
- **Ordered scans** - forward, reverse, and range-bounded key and entry iteration.
- **Atomic multi-key writes** - all-or-nothing visibility within a tree and across multiple trees; no reader ever observes a partial batch.
- **Predicate push-down** - filter typed reads, conditional writes, scans, cursors, and range deletes with an ordinary `Expression<Func<T, bool>>` evaluated server-side.
- **Durable, resumable cursors** - server-checkpointed iterators that survive silo failover and client restart.
- **Snapshots** - point-in-time copies (offline or online) and snapshot-isolated cursors.
- **Materialised & history views** - WAL-driven projections, aggregations, and per-key revision history.
- **Bulk loading** - one-shot bottom-up build or streaming `IAsyncEnumerable` ingestion; idempotent and retryable.
- **Online resize & reshard** - change node fan-out or physical shard count on a live tree, without downtime.
- **Tag indexes, typed queues, pluggable compression, and metrics** on the `orleans.lattice` meter.

## Core properties

- **Self-organising under load** - hot regions of the keyspace rebalance themselves online, transparently to callers.
- **Strongly consistent from the outside** - point reads, writes, and ordered scans always see a consistent view, even mid-rebalance.
- **Crash-safe by construction** - a silo crash mid-write, mid-split, mid-snapshot, or mid-bulk-load is recovered without operator intervention or data loss.
- **No locks, no consensus round-trips** - all conflict resolution is algebraic.

## Quick start

Register Lattice on a silo. `AddLattice` wires up the grain catalogue, the grain storage provider (via the supplied callback), and the in-memory write-ahead log in one call:

```csharp
siloBuilder.AddLattice((silo, storageName) =>
    silo.AddMemoryGrainStorage(storageName));

// elsewhere - on the client or inside a grain - resolve a tree by name and write a key:
var lattice = grainFactory.GetGrain<ILattice>("my-tree");
await lattice.SetAsync("hello", "world"u8.ToArray());
```

For production, swap the in-memory WAL for a durable backend such as [Orleans.Lattice.Storage.AzureTable](https://www.nuget.org/packages/Orleans.Lattice.Storage.AzureTable), and add cross-cluster replication with [Orleans.Lattice.Replication](https://www.nuget.org/packages/Orleans.Lattice.Replication).

## Documentation

Full documentation, the complete feature matrix, the `ILattice` API reference, and runnable samples are on [GitHub](https://github.com/NSTA1/Orleans.Lattice#readme).

## License

MIT. See [LICENSE](https://github.com/NSTA1/Orleans.Lattice/blob/main/LICENSE).
