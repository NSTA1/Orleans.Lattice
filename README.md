# Orleans.Lattice

A distributed [B+ tree](https://en.wikipedia.org/wiki/B%2B_tree) library built on [Microsoft Orleans](https://learn.microsoft.com/dotnet/orleans/), designed for scalable ordered key-value storage across a cluster.

## What is it?

Orleans.Lattice gives you a **sorted key-value store** that runs as a set of Orleans grains — no external database required. Keys are `string`, values are `byte[]`. You get point lookups, deletes, ordered key scans, and bulk loading, all distributed across a cluster of silos.

The name comes from its use of **lattice-based state primitives** (hybrid logical clocks, last-writer-wins registers, version vectors) — mathematical structures where merges are commutative, associative, and idempotent. This means operations are conflict-free, crash-safe, and recoverable without distributed locks or coordination protocols.

## Key Properties

| Property | How |
|---|---|
| **Scalable writes** | Keys are hash-sharded across 64 independent sub-trees (configurable). No single-root bottleneck. |
| **Crash-safe splits** | Every node split uses a two-phase pattern with persisted intent. Interrupted splits resume automatically on the next access. |
| **Conflict-free** | All state merges are monotonic. Concurrent writes to the same key resolve via last-writer-wins with hybrid logical clocks. |
| **Fast reads** | A `[StatelessWorker]` cache grain per silo serves reads via delta replication from the primary leaf. Cache misses cost a single version-vector comparison. |
| **Bulk loading** | One-shot bottom-up build or streaming `IAsyncEnumerable` ingestion with per-shard parallel flushing. Both modes are idempotent and retryable. |
| **Tombstone cleanup** | Reminder-driven compaction removes expired tombstones shard-by-shard, with crash-safe progress tracking. |

## Quick Start

```csharp
// Silo configuration
siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));

// Client code
var tree = grainFactory.GetGrain<ILattice>("my-tree");

await tree.SetAsync("customer-123", Encoding.UTF8.GetBytes("Alice"));
byte[]? value = await tree.GetAsync("customer-123");
bool deleted = await tree.DeleteAsync("customer-123");

// Ordered key scan
await foreach (var key in tree.KeysAsync())
    Console.WriteLine(key);

// Bulk load
var entries = data.Select(d => KeyValuePair.Create(d.Key, d.ValueBytes)).ToList();
await tree.BulkLoadAsync(entries);

// Delete entire tree (soft delete with deferred purge)
await tree.DeleteTreeAsync();
```

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
| [State Primitives](docs/state-primitives.md) | Hybrid logical clocks, LWW registers, monotonic split state, version vectors, state deltas |
| [Tombstone Compaction](docs/tombstone-compaction.md) | Reminder-driven cleanup, grace periods, configuration |
| [Tree Deletion](docs/tree-deletion.md) | Soft delete, recovery, manual purge, deferred purge |
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
