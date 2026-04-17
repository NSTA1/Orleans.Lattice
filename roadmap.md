# Orleans.Lattice Roadmap

Potential improvements and new features, organized by category.

## API Enhancements

- [x] **F-001 — Range Delete (`DeleteRangeAsync`)**: Delete all keys within a lexicographic range (`startInclusive`, `endExclusive`) in a single call by walking the leaf chain and tombstoning matching entries in bulk.
- [ ] **F-002 — `CountAsync` / `CountPerShardAsync`**: Return the number of live keys without streaming them across the wire. A per-shard variant would aid diagnostics and load-balancing analysis.
- [ ] **F-003 — `GetOrSetAsync` (conditional write)**: Set a key only if it does not already exist, avoiding a read-then-write roundtrip. The leaf grain can short-circuit when a live value is present.
- [ ] **F-004 — Typed value helpers**: A thin generic wrapper `ILattice<T>` or extension methods that accept serializer/deserializer delegates (or default to `System.Text.Json`) to eliminate per-caller `byte[]` boilerplate.
- [ ] **F-005 — `EntriesAsync` (key + value scan)**: Stream `KeyValuePair<string, byte[]>` in sorted order, complementing the existing `KeysAsync`. Useful for exports, migrations, and analytics without a separate `GetAsync` per key.

## Performance & Scalability

- [ ] **F-006 — Parallel shard pre-fetch for `KeysAsync`**: Double-buffer the k-way merge by pre-fetching the next page from each shard in parallel, hiding per-shard latency during ordered scans.
- [ ] **F-007 — Leaf-level write batching**: Coalesce concurrent `SetAsync` calls targeting the same leaf into a single `WriteStateAsync` (similar to a WAL flush group) to reduce storage I/O under write-heavy workloads.
- [ ] **F-008 — Adaptive shard splitting**: Allow a hot shard to split into two at runtime without a full offline resize, enabling the tree to scale with workload growth.
- [ ] **F-009 — Warm cache on silo startup**: Optionally pre-warm `LeafCacheGrain` activations for recently-accessed leaves after a silo restart to reduce cold-start read-latency spikes.

## Reliability & Observability

- [ ] **F-010 — Metrics / telemetry integration**: Expose `System.Diagnostics.Metrics` counters and histograms (reads, writes, splits, cache hit/miss ratio, tombstone count, compaction duration, scan latency) for OpenTelemetry-compatible dashboards.
- [ ] **F-011 — Tree diagnostics (`DiagnoseAsync`)**: Return per-shard health information — depth, live key count, tombstone ratio, pending splits/promotions — via an `ILatticeAdmin` interface or a method on `ILattice`.
- [ ] **F-012 — Event notifications / observers**: Publish tree events (key written, tree deleted, split occurred, compaction completed) via an `ILatticeObserver` interface or Orleans Streams integration for event-driven architectures.

## Feature Additions

- [ ] **F-013 — Per-key TTL (expiring keys)**: Accept an optional `TimeSpan ttl` on `SetAsync`. Expired keys are treated as tombstoned during reads and cleaned up by existing compaction infrastructure. Requires an `ExpiresAtTicks` field on `LwwValue`.
- [ ] **F-014 — Compare-and-swap (CAS)**: Optimistic concurrency via `SetIfVersionAsync(key, value, expectedVersion)` — the write succeeds only if the current entry's HLC matches, enabling safe read-modify-write patterns without distributed locks.
- [ ] **F-015 — Secondary index / tag support**: Associate tags with keys and query by tag. Implementable as a secondary Lattice tree mapping `tag → Set<key>`, maintained alongside the primary write.
- [ ] **F-016 — Online (non-blocking) resize**: Copy shards incrementally while the tree remains available (similar to `SnapshotMode.Online`), with only a brief lock for the final alias swap, to reduce maintenance downtime.
- [ ] **F-017 — Merge trees (`MergeAsync`)**: Merge all entries from a source tree into the current tree using LWW semantics. Useful for combining snapshots, migrating data, or rejoining forked datasets.

## Documentation & Developer Experience

- [ ] **F-018 — Migration guide**: Document how to migrate data from external stores (Redis, SQL, Cosmos DB) into Lattice using the streaming bulk-load API, including key-design and value-serialization best practices.
- [ ] **F-019 — Troubleshooting guide (`docs/troubleshooting.md`)**: Cover common issues — storage provider exceptions from oversized grains, split storms, slow scans, stale cache behavior — and how to interpret diagnostic data.
- [ ] **F-020 — Sample applications (`samples/`)**: End-to-end examples (e.g. ASP.NET Core session store, leaderboard, distributed configuration service) showing real integration patterns beyond the Quick Start snippet.
