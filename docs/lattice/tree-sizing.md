# Tree Sizing

This document covers how to change the structural sizing
(`MaxLeafKeys`, `MaxInternalChildren`) of an existing tree via
`ILattice.ResizeAsync`, along with its phase machine, undo window, and
operational considerations.

For storage-provider limits, per-grain state-size estimation, and
sizing recommendations by provider, see
[Tree Storage](tree-storage.md).

For shard-count growth (`ILattice.ReshardAsync`), see
[Online Reshard](online-reshard.md).

> **Structural sizing is registry-pinned, not option-configured.**
> `MaxLeafKeys`, `MaxInternalChildren`, and `ShardCount` live on the
> tree's registry entry, not on `LatticeOptions`. The canonical
> defaults (128 / 128 / 64) are seeded into the registry the first time
> the tree's options are resolved. After seeding, the pin is the sole
> source of structural truth - every grain reads it from the registry.
> The only supported mutation paths are `ResizeAsync` (leaf / internal
> capacity) and `ReshardAsync` (shard count); both run online and
> update the pin atomically. To start a tree with non-default sizing,
> call `ResizeAsync` / `ReshardAsync` on the freshly-created empty tree
> (empty-tree fast-path - no coordinator machinery) or pre-register the
> pin via [`ILatticeTreeAdmin.CreateTreeAsync`](../lattice.api.treeadmin/README.md),
> which honours the sizing only when it first creates the tree.

## Resizing an Existing Tree

If you need to change `MaxLeafKeys` or `MaxInternalChildren` on a tree
that already contains data, use the `ResizeAsync` API:

```csharp verify
var tree = grainFactory.GetGrain<ILattice>("my-tree");
await tree.ResizeAsync(newMaxLeafKeys: 256, newMaxInternalChildren: 64);
```

### How it works

Resize runs **online**: reads and writes remain available throughout. A whole-tree shape change cannot be done in place - every leaf and every internal node has to be re-paginated at the new fan-out - so `ResizeAsync` drains the source into a freshly-provisioned destination physical tree and then atomically swaps the registry alias.

1. **Provision destination** - the resize coordinator creates a destination physical tree ID (e.g. `my-tree/resized/{operationId}`) registered with the new `MaxLeafKeys` / `MaxInternalChildren` and the source's pinned `ShardCount`. The destination is given no shard map of its own: the snapshot copies each source shard, and shadow-forwards its live writes, to the destination shard with the same index, covering shard indices `0` to `ShardCount - 1`. A shard that an adaptive split allocated above that range is neither copied nor shadow-forwarded - a split gives its target shard an index above every index allocated so far and leaves the pinned `ShardCount` unchanged - so a resize of a tree whose shard map routes keys to such a shard does not carry the keys that shard holds.
2. **Snapshot with shadow forwarding** - the source tree runs under `SnapshotMode.Online`. Before drain begins, every source shard root enters its draining shadow-forward phase, and each of its mutation paths - `SetAsync` (with or without a TTL), `GetOrSetAsync`, `SetIfVersionAsync`, `SetManyAsync` and its predicated form, `DeleteAsync`, `DeleteRangeAsync`, the batched merge path, and the terminal (commit or abort) of an atomic-write saga such as `SetManyAtomicAsync` - runs the local write and a parallel forward to the corresponding destination shard (`SetIfVersionAsync` forwards only once its local compare-and-set has succeeded). The typed CRDT delta paths (`ApplyCrdtDeltaAsync`, `ApplyCrdtDeltaManyAsync` and the typed accessors built on them) are not forwarded, so a CRDT delta applied to a source shard after that shard has been drained does not reach the destination.
3. **Drain** - the snapshot coordinator reads each source shard's live entries and merges them into the destination shard with a last-writer-wins merge, draining up to `LatticeOptions.MaxConcurrentDrains` shards at a time (default 4). Tombstoned and expired entries are skipped; each copied entry keeps its source HLC timestamp and any remaining TTL, with the same absolute expiry. As each shard finishes draining it is marked drained; its live forwards continue until swap.
4. **Swap** - the logical tree's registry entry is rewritten with the new sizing and the pinned `ShardCount`. The tree's own configuration overrides - `PublishEvents`, projection digest maintenance and its latch, history retention, and the cache value-byte and WAL retained-byte ceilings - are carried over, while the old physical tree's shard map, split allocation mark and WAL layout are dropped (`UndoResizeAsync` restores the original entry): the logical tree then routes by the default shard map, which matches the destination's index-for-index copy, and the destination's own registry entry carries its WAL layout. An adaptive split, though, leaves the keys it moved in place on the shard that gave them up - hidden there from reads - and the copy carries them over, so after the swap a key that already existed when a split moved it reads the value it held at that moment rather than its current one. The registry alias then atomically points the logical tree ID at the destination. Once the alias is set, each source shard (again indices `0` to `ShardCount - 1`) enters its rejecting phase, in which every read or write that still reaches the old physical tree fails with an internal stale-routing signal. The routing tier behind `ILattice` catches that signal, drops its cached alias, re-resolves through the registry, and retries the call against the destination, so callers do not see the transition as an error.
5. **Cleanup** - the old physical tree is soft-deleted. It will be purged automatically after the configured `SoftDeleteDuration` (default 72 hours), leaving `UndoResizeAsync` viable until then. On a tree's first resize the old physical tree's ID is the logical tree ID itself, so the purge reclaims its shards but leaves the logical tree's registry entry - its alias, sizing and configuration - and its tombstone compaction schedule in place.

Each phase transition is persisted. If the silo crashes mid-resize, reminder-anchored `TreeResizeGrain` and `TreeSnapshotGrain` reactivate and resume from the last completed phase; source shards retain their `ShadowForwardState` across activations, so live forwards continue uninterrupted.

### LWW convergence - why shadow forwarding is safe

Every entry carries a hybrid-logical-clock timestamp and all writes flow through a last-writer-wins comparator. That makes the shadow-forward path commutative: whether a live write arrives at the destination before or after the drain reader copies the same key, the destination converges to the entry with the higher HLC. Consequently:

- The parallel `local ∥ forward` write is **not** a two-phase commit. If local succeeds and forward fails, the client sees failure; the next idempotent retry lands on both trees. Writes that briefly land on the source only are captured by the drain reader and re-delivered with their original HLCs.
- The drain uses `MergeManyAsync` (not `BulkLoadRawAsync`) because shadow-forwarded writes can populate destination shards ahead of the drain batch. LWW merge absorbs the race; a bulk-load would error on non-empty destination shards. (The offline snapshot path still uses `BulkLoadRawAsync` - source shards are locked before drain so the destination is guaranteed empty.)

### Cache invalidation

Different physical trees produce different leaf grain IDs, which automatically create fresh `LeafCacheGrain` instances. No explicit cache flush is needed after the alias swap. See [Read Caching](caching.md#cache-invalidation-via-tree-aliasing) for details.

### Undo resize

A resize can be undone while it is still running, at any phase, and afterwards
for as long as the old tree remains inside the soft-delete window:

```csharp verify
var tree = grainFactory.GetGrain<ILattice>("my-tree");
await tree.UndoResizeAsync();
```

`UndoResizeAsync` is phase-aware:

- **Before swap** (`Phase == Snapshot`) - aborts the snapshot coordinator, clears every source shard's `ShadowForwardState`, deletes the half-built destination tree, and returns the source to a fully-writable state. No alias was ever set, so clients never observed the destination.
- **After swap** (`Phase ∈ { Swap, Reject, Cleanup }`) - removes the alias, restores the original registry configuration, clears any residual `Rejecting` phase on source shards, defensively aborts any post-swap snapshot still attached, and deletes the new snapshot tree. The old physical tree is recovered from soft-delete only when it was actually soft-deleted: `Cleanup` is the only phase that deletes it, and it does so at the very end, so throughout `Swap` and `Reject` - and in `Cleanup` until the delete lands - the old tree is still live and is simply left alone.

Once the soft-delete window expires and the old tree is purged, the resize can no longer be undone.

### Important considerations

- **Availability:** reads and writes continue throughout. The per-shard rejecting window at swap is absorbed by the routing tier, which re-resolves the alias and retries a call that reaches a rejecting shard against the destination - callers observe at most one internal retry, not an error.
- **Storage:** both the old and new physical trees exist simultaneously until the old tree is purged. Plan for approximately 2× the tree's storage usage during this window. See [Tree Storage](tree-storage.md) for per-provider capacity considerations.
- **Hot-path cost during drain:** every write between `BeginShadowForwardAsync` and swap pays one extra grain hop for the parallel forward. For same-cluster destinations this is in the millisecond range. Prefer off-peak windows for large resizes even though they are online.
- **Concurrency cap:** `LatticeOptions.MaxConcurrentDrains` (default 4) bounds the number of concurrent per-shard drains `TreeSnapshotGrain` dispatches. Mirrors `MaxConcurrentMigrations` for reshard.
- **Idempotency:** calling `ResizeAsync` again with the same parameters while a resize is in progress is a no-op. Calling with different parameters throws `InvalidOperationException`.
- **Registry is the source of truth:** the new sizing is persisted in the tree registry - on the new physical tree's entry when the resize creates it, and on the logical tree's entry at the swap - and every structural grain reads its sizing from the registry. You do not need to update `LatticeOptions` in silo configuration separately - `LatticeOptions` no longer exposes `MaxLeafKeys` / `MaxInternalChildren` / `ShardCount`.
- **Empty-tree fast-path:** if the tree has no live entries yet, `ResizeAsync` and `ReshardAsync` update the registry pin in-place and return immediately without activating the coordinator machinery. This is the recommended way to start a tree with non-default sizing.
- **Validation:** `newMaxLeafKeys` must be at least 2 and `newMaxInternalChildren` at least 3; smaller values throw `ArgumentOutOfRangeException` before anything is persisted.
- **Interlocks:** while a resize is in flight, the autonomic split monitor suppresses splits on the tree and `ReshardAsync` throws `InvalidOperationException`; likewise, `ResizeAsync` throws `InvalidOperationException` while a reshard is in flight. If you need both, let the reshard complete first, then resize.
- **`ShardCount` cannot be resized via `ResizeAsync`.** Changing shard count requires re-hashing all keys, which `ResizeAsync` does not support. Use `ReshardAsync` for that; it runs online by driving the adaptive shard-split primitive (shadow-write, drain, swap, reject). See [Online Reshard](online-reshard.md).

### Manual trigger (testing)

In integration tests, the existing test harnesses call `ITreeResizeGrain` directly to drive resize passes synchronously. This grain interface is **declared `internal`** - consumer assemblies cannot reference or invoke it. Use `ILattice.ResizeAsync` for all non-test scenarios; it delegates to `ITreeResizeGrain` internally and exposes `ILattice.IsResizeCompleteAsync()` for progress polling.

## See also

- [Tree Storage](tree-storage.md) - storage-provider limits, grain-state size estimation, per-provider sizing recommendations, default-configuration assessment, key trade-offs.
- [Online Reshard](online-reshard.md) - growing the physical shard count online.
- [Snapshots](snapshots.md) - the underlying drain primitive used by `ResizeAsync`.
- [Tree Registry](tree-registry.md) - the registry entry that pins `MaxLeafKeys`, `MaxInternalChildren`, and `ShardCount` per tree.
- [Consistency](consistency.md) - consistency guarantees of `ResizeAsync` and `UndoResizeAsync`.
