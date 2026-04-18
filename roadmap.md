# Orleans.Lattice Roadmap

Potential improvements and new features, organized by category.

## API Enhancements

- [x] **F-001 — Range Delete (`DeleteRangeAsync`)**: Delete all keys within a lexicographic range (`startInclusive`, `endExclusive`) in a single call by walking the leaf chain and tombstoning matching entries in bulk.
- [x] **F-002 — `CountAsync` / `CountPerShardAsync`**: Return the number of live keys without streaming them across the wire. A per-shard variant would aid diagnostics and load-balancing analysis.
- [x] **F-003 — `GetOrSetAsync` (conditional write)**: Set a key only if it does not already exist, avoiding a read-then-write roundtrip. The leaf grain can short-circuit when a live value is present.
- [x] **F-004 — Typed value helpers**: A thin generic wrapper `ILattice<T>` or extension methods that accept serializer/deserializer delegates (or default to `System.Text.Json`) to eliminate per-caller `byte[]` boilerplate.
- [x] **F-005 — `EntriesAsync` (key + value scan)**: Stream `KeyValuePair<string, byte[]>` in sorted order, complementing the existing `KeysAsync`. Useful for exports, migrations, and analytics without a separate `GetAsync` per key.
- [ ] **F-026 — Operation status queries**: Expose completion checks for long-running maintenance operations on `ILattice` so callers can poll for completion without reaching into coordination grains. Methods: `IsMergeCompleteAsync()`, `IsSnapshotCompleteAsync()`, `IsResizeCompleteAsync()`. Each delegates to the corresponding coordination grain's persisted state (`!InProgress && Complete`). Returns `true` when no operation has ever been initiated (vacuously complete). Since only one operation of each type can be in progress per tree, the tree ID is the implicit operation handle — no new identifier needed. Callers that need synchronous completion can poll these methods; callers that prefer fire-and-forget rely on the existing reminder-based crash recovery to drive the operation to completion.

## Performance & Scalability

- [x] **F-006 — Leaf-side continuation filtering for `EntriesAsync`**: Pass the continuation token down to the leaf grain as an `afterExclusive` parameter so it filters entries at the source, avoiding unnecessary `byte[]` value serialization across the grain boundary during forward pagination.
- [x] **F-007 — Leaf-side continuation filtering for `KeysAsync`**: Apply the same `afterExclusive` leaf-side filtering optimization from F-006 to `GetSortedKeysBatchAsync`, eliminating shard-level skip loops for key-only scans.
- [x] **F-008 — Reverse-scan leaf-side filtering**: Add a `beforeExclusive` parameter to leaf `GetKeysAsync` / `GetEntriesAsync` so reverse pagination can also filter at the leaf, avoiding unnecessary serialization for both keys and entries in `GetSortedKeysBatchReverseAsync` / `GetSortedEntriesBatchReverseAsync`.
- [x] **F-009 — Parallel shard pre-fetch for `KeysAsync`**: Double-buffer the k-way merge by pre-fetching the next page from each shard in parallel, hiding per-shard latency during ordered key scans. Controlled via `LatticeOptions.PrefetchKeysScan` or the per-call `prefetch` parameter.
- [ ] **F-024 — Parallel shard pre-fetch for `EntriesAsync`**: Extend the F-009 pre-fetch strategy to `EntriesAsync`. Because entries carry `byte[]` values, pre-fetched pages increase in-flight memory proportionally to `shardCount × pageSize × avgValueSize`. The implementation should be gated behind a dedicated option (e.g. `PrefetchEntriesScan`) or an optional `prefetch` parameter on `EntriesAsync` so callers can opt in only when the memory trade-off is acceptable.
- [ ] **F-010 — Leaf-level write batching**: Coalesce concurrent `SetAsync` calls targeting the same leaf into a single `WriteStateAsync` (similar to a WAL flush group) to reduce storage I/O under write-heavy workloads.
- [ ] **F-011 — Adaptive shard splitting**: Allow a hot shard to split into two at runtime without a full offline resize, enabling the tree to scale with workload growth.
- [ ] **F-012 — Warm cache on silo startup**: Optionally pre-warm `LeafCacheGrain` activations for recently-accessed leaves after a silo restart to reduce cold-start read-latency spikes.
- [ ] **F-027 — Leaf-grouped merge routing**: `ShardRootGrain.MergeManyAsync` currently traverses the B+ tree once per entry, sending a single-entry dictionary to each leaf. Group entries by target leaf before calling `MergeManyAsync`, reducing tree traversals from O(n) to O(leaves) and collapsing multiple `WriteStateAsync` calls per leaf into one.

## Reliability & Observability

- [ ] **F-013 — Metrics / telemetry integration**: Expose `System.Diagnostics.Metrics` counters and histograms (reads, writes, splits, cache hit/miss ratio, tombstone count, compaction duration, scan latency) for OpenTelemetry-compatible dashboards.
- [ ] **F-014 — Tree diagnostics (`DiagnoseAsync`)**: Return per-shard health information — depth, live key count, tombstone ratio, pending splits/promotions — via an `ILatticeAdmin` interface or a method on `ILattice`.
- [ ] **F-015 — Event notifications / observers**: Publish tree events (key written, tree deleted, split occurred, compaction completed) via an `ILatticeObserver` interface or Orleans Streams integration for event-driven architectures.

## Feature Additions

- [ ] **F-016 — Per-key TTL (expiring keys)**: Accept an optional `TimeSpan ttl` on `SetAsync`. Expired keys are treated as tombstoned during reads and cleaned up by existing compaction infrastructure. Requires an `ExpiresAtTicks` field on `LwwValue`.
- [ ] **F-017 — Compare-and-swap (CAS)**: Optimistic concurrency via `SetIfVersionAsync(key, value, expectedVersion)` — the write succeeds only if the current entry's HLC matches, enabling safe read-modify-write patterns without distributed locks.
- [ ] **F-018 — Secondary index / tag support**: Associate tags with keys and query by tag. Implementable as a secondary Lattice tree mapping `tag → Set<key>`, maintained alongside the primary write.
- [ ] **F-019 — Online (non-blocking) resize**: Copy shards incrementally while the tree remains available (similar to `SnapshotMode.Online`), with only a brief lock for the final alias swap, to reduce maintenance downtime.
- [x] **F-020 — Merge trees (`MergeAsync`)**: Merge all entries from a source tree into the current tree using LWW semantics. Useful for combining snapshots, migrating data, or rejoining forked datasets.
- [ ] **F-025 — Continuous merge (`ContinuousMergeAsync`)**: Build on F-020 to support incremental, ongoing merge from one or more source trees. Uses `VersionVector` to track a per-source high-water mark so each merge cycle transfers only entries newer than the last. Requires a delta-aware leaf scan (`GetEntriesNewerThanAsync(HybridLogicalClock threshold)`) and a merge-state tracking grain (or shard-root state) to persist the stored vector per source tree. Convergence is guaranteed by `LwwValue.Merge` and replay-safety by the idempotent pointwise-max join of `VersionVector.Merge`. Should also replace the current full-shard drain in `TreeMergeGrain.MergeShardAsync` with a chunked or cursor-based leaf-chain iteration to avoid loading all entries into memory at once.

## Documentation & Developer Experience

- [ ] **F-021 — Migration guide**: Document how to migrate data from external stores (Redis, SQL, Cosmos DB) into Lattice using the streaming bulk-load API, including key-design and value-serialization best practices.
- [ ] **F-022 — Troubleshooting guide (`docs/troubleshooting.md`)**: Cover common issues — storage provider exceptions from oversized grains, split storms, slow scans, stale cache behavior — and how to interpret diagnostic data.
- [ ] **F-023 — Sample applications (`samples/`)**: End-to-end examples (e.g. ASP.NET Core session store, leaderboard, distributed configuration service) showing real integration patterns beyond the Quick Start snippet.
