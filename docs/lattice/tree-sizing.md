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

1. **Provision destination** - the resize coordinator creates a destination physical tree ID (e.g. `my-tree/resized/{operationId}`) registered with the new `MaxLeafKeys` / `MaxInternalChildren`, the source's pinned `ShardCount`, and the logical tree's shard map and split allocation mark, so every virtual slot routes to the same physical shard index on both trees. The snapshot copies each source shard, and shadow-forwards its live writes, to the destination shard with the same index, covering shard indices `0` to `ShardCount - 1` and every index the shard map routes to - including a shard an adaptive split allocated above the pinned count (a split gives its target shard an index above every index allocated so far and leaves the pinned `ShardCount` unchanged). The shard map is captured when the resize starts; the autonomic split monitor starts no split while the resize is in flight.
2. **Snapshot with shadow forwarding** - the source tree runs under `SnapshotMode.Online`. Before drain begins, every source shard root enters its draining shadow-forward phase, and each of its mutation paths - `SetAsync` (with or without a TTL), `GetOrSetAsync`, `SetIfVersionAsync`, `SetManyAsync` and its predicated form, `DeleteAsync`, `DeleteRangeAsync`, the batched merge path, and the terminal (commit or abort) of an atomic-write saga such as `SetManyAtomicAsync` - runs the local write and a parallel forward to the corresponding destination shard (`SetIfVersionAsync` forwards only once its local compare-and-set has succeeded). The typed CRDT delta paths (`ApplyCrdtDeltaAsync`, `ApplyCrdtDeltaManyAsync` and the typed accessors built on them) and bulk appends (`BulkAppendChunkAsync` and the streaming `BulkLoadAsync` extension) are not forwarded, so one that reaches a source shard after the copy has read past the key it writes does not reach the destination.
3. **Drain** - the snapshot coordinator reads each source shard's live entries and merges them into the destination shard with a last-writer-wins merge, draining up to `LatticeOptions.MaxConcurrentDrains` shards at a time (default 4). Tombstoned and expired entries are skipped; each copied entry keeps its source HLC timestamp and any remaining TTL, with the same absolute expiry. An entry is copied only when the shard map routes its key to the shard it was read from: an adaptive split leaves the keys it moved in place on the shard that gave them up - hidden there from reads - and those stale copies are left behind rather than carried over, so every key reaches the destination at its current value. As each shard finishes draining it is marked drained; its live forwards continue until swap. The resize coordinator drives the drain in wall-clock-bounded slices: each call copies for at most `LatticeOptions.BackgroundDrainMaxDuration` (capped at 10 seconds, and 10 seconds when that option is zero), persists every shard's resume key, and returns, so no call outlives the caller's response timeout or holds the snapshot's turn against its keepalive reminder. The next phase tick resumes each shard from its persisted key, so a large or contended tree converges in time proportional to the work rather than to the number of retries ([#3904](https://github.com/NSTA1/Orleans.Lattice/issues/3904)).
4. **Swap** - the logical tree's registry entry is rewritten with the new sizing and the pinned `ShardCount`. The tree's own configuration overrides - `PublishEvents`, projection digest maintenance and its latch, history retention, and the cache value-byte and WAL retained-byte ceilings - are carried over, and so are the shard map and split allocation mark the copy followed, read from the destination's own registry entry and re-stamped with a newer map version so every cached router observes the change; the old physical tree's WAL layout is dropped, since the destination's own registry entry carries its own (`UndoResizeAsync` restores the original entry). The registry alias then atomically points the logical tree ID at the destination. Like every alias assignment, that step is first put to the host's [ownership guard](tree-registry.md#ownership-bounded-aliasing), which allows it unless the host registers an ownership provider; the apps package's provider allows it too, because the destination is recorded as derived from the tree. A guard that refuses it stops the resize at the swap, which it retries on every tick until the guard allows it or the resize is undone - by then the logical tree's registry entry has already been rewritten with the new sizing, and undoing the resize restores it. Once the alias is set, each source shard the snapshot shadow-forwarded (the same set as step 1, split-added shards included) enters its rejecting phase, in which every read or write that still reaches the old physical tree fails with an internal stale-routing signal. The routing tier behind `ILattice` catches that signal, drops its cached alias, re-resolves through the registry, and retries the call against the destination, so callers do not see the transition as an error.
5. **Cleanup** - the old physical tree is retired: its shards are soft-deleted as physical maintenance, which publishes no `TreeDeleted` or `TreePurged` event and leaves the logical tree reading as not deleted (see [Tree Deletion](tree-deletion.md#retiring-a-resized-trees-original-copy)). It will be purged automatically after the configured `SoftDeleteDuration` (default 72 hours), leaving `UndoResizeAsync` viable until then. On a tree's first resize the old physical tree's ID is the logical tree ID itself, so the purge reclaims its shards but leaves the logical tree's registry entry - its alias, sizing and configuration - and its tombstone compaction schedule in place. On a later resize the old physical tree is the previous resize's copy; its own registry entry is first given the logical tree's shard map and split allocation mark, because a split writes those to the logical tree's entry only, so the retirement - and an undo's recovery - reaches every shard a split added to it.

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
- **Interlocks:** while a resize is in flight, the autonomic split monitor suppresses splits on the tree and `ReshardAsync` throws `InvalidOperationException`; likewise, `ResizeAsync` throws `InvalidOperationException` while a reshard is in flight. If you need both, let the reshard complete first, then resize. A resize and its undo also hold the tree's alias for as long as they run, so `DeleteTreeAsync` throws `InvalidOperationException` meanwhile, and `ResizeAsync` or `UndoResizeAsync` throws it on a tree that is deleted or has a delete pending, or while a shadow-cutover restore or schema remediation holds the alias.
- **`ShardCount` cannot be resized via `ResizeAsync`.** Changing shard count requires re-hashing all keys, which `ResizeAsync` does not support. Use `ReshardAsync` for that; it runs online by driving the adaptive shard-split primitive (shadow-write, drain, swap, reject). See [Online Reshard](online-reshard.md).

### Manual trigger (testing)

In integration tests, the existing test harnesses call `ITreeResizeGrain` directly to drive resize passes synchronously. This grain interface is **declared `internal`** - consumer assemblies cannot reference or invoke it. Use `ILattice.ResizeAsync` for all non-test scenarios; it delegates to `ITreeResizeGrain` internally and exposes `ILattice.IsResizeCompleteAsync()` for progress polling.

## See also

- [Tree Storage](tree-storage.md) - storage-provider limits, grain-state size estimation, per-provider sizing recommendations, default-configuration assessment, key trade-offs.
- [Online Reshard](online-reshard.md) - growing the physical shard count online.
- [Snapshots](snapshots.md) - the underlying drain primitive used by `ResizeAsync`.
- [Tree Registry](tree-registry.md) - the registry entry that pins `MaxLeafKeys`, `MaxInternalChildren`, and `ShardCount` per tree.
- [Consistency](consistency.md) - consistency guarantees of `ResizeAsync` and `UndoResizeAsync`.
