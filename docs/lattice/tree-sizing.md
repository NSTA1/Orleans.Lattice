# Tree Sizing

This document covers how to change the structural sizing
(`MaxLeafKeys`, `MaxInternalChildren`) of an existing tree via
`ILattice.ResizeAsync`, along with its phase machine, undo window, and
operational considerations.

For storage-provider limits, per-grain state-size estimation, and
sizing recommendations by provider, see
[Tree Storage](tree-storage.md).

For changing the shard count (`ILattice.ReshardAsync`), see
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
4. **Swap** - the logical tree's registry entry is rewritten with the new sizing and the pinned `ShardCount`. The tree's own configuration overrides - `PublishEvents`, projection digest maintenance and its latch, history retention, and the cache value-byte and WAL retained-byte ceilings - are carried over, and the old physical tree's WAL layout is dropped, since the destination's own registry entry carries its own (`UndoResizeAsync` restores the original entry). Next, each source shard the snapshot shadow-forwarded (the same set as step 1, split-added shards included) enters its rejecting phase, in which a read or write that still reaches the old physical tree fails with an internal stale-routing signal; an operation a shard had already accepted is still mirrored to the destination, so the destination holds every write the old tree took. The fence does not refuse an atomic-write saga bound to the old tree - its prepared batch and its commit or abort - which the old tree still takes and mirrors, through the step 5 soft delete until the purge, so a batch in flight across the swap lands whole on both copies and an undo never brings back a copy holding part of one ([#4369](https://github.com/NSTA1/Orleans.Lattice/issues/4369)). Only then does the registry alias point the logical tree ID at the destination, in the same registry write that takes the shard map and split allocation mark the copy followed from the destination's own registry entry, re-stamped with a newer map version so every cached router observes the change: a reader resolving routing on either side of that write sees one physical tree with the map that describes its shards, never the old tree with the copy's map. The order matters: the old tree receives no writes once the alias has moved, so a routing activation whose cached alias predates the swap must not be able to read it after the destination has taken one - it would return a committed batch at its predecessor. The routing tier behind `ILattice` catches the stale-routing signal, drops its cached alias, re-resolves through the registry, and retries the call, so callers do not see the transition as an error; a call that lands between the fence and the alias flip retries until the flip lands. Like every alias assignment, the flip is first put to the host's [ownership guard](tree-registry.md#ownership-bounded-aliasing), which allows it unless the host registers an ownership provider; the apps package's provider allows it too, because the destination is recorded as derived from the tree. A flip the guard refuses, or that fails, lifts the fence again unless the registry shows the alias did move, so the old tree goes on serving; the resize stays at the swap, which it retries - fence included - on every tick until the guard allows it or the resize is undone. By then the logical tree's registry entry has already been rewritten with the new sizing, and undoing the resize restores it. Bulk loads and appends (`BulkLoadAsync`, `BulkAppendChunkAsync` and the streaming `BulkLoadAsync` extension) are the exception: the rejecting phase does not refuse them and the routing tier does not retry them. Each call resolves the tree's routing afresh before it writes, so a call made after the swap goes to the destination, but one already in flight when the alias swaps is applied to the old physical tree, where nothing copies it to the destination and the old tree's purge removes it.
5. **Cleanup** - the old physical tree is retired: its shards are soft-deleted as physical maintenance, which publishes no `TreeDeleted` or `TreePurged` event and leaves the logical tree reading as not deleted (see [Tree Deletion](tree-deletion.md#retiring-a-resized-trees-original-copy)). It will be purged automatically after the configured `SoftDeleteDuration` (default 72 hours), leaving `UndoResizeAsync` viable until then. On a tree's first resize the old physical tree's ID is the logical tree ID itself, so the purge reclaims its shards but leaves the logical tree's registry entry - its alias, sizing and configuration - and its tombstone compaction schedule in place. On a later resize the old physical tree is the previous resize's copy; its own registry entry is first given the logical tree's shard map and split allocation mark, because a split writes those to the logical tree's entry only, so the retirement - and an undo's recovery - reaches every shard a split added to it.

The tree-admin resize status (`ILatticeTreeAdmin.GetResizeStatusAsync`, the `tree_resize_status` tool) reports the step a running resize has reached - `Copy`, `Swap`, `RejectOldShards` or `RetireOldCopy`, or `Undo` while an accepted undo unwinds - and its progress in work units: one per shard the copy drains, then one for each of the three steps after the copy. An unwind reports no units.

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

// The undo is accept-then-poll: follow an unwind that outlasted the call.
while (await tree.IsResizeUndoPendingAsync())
{
    await Task.Delay(TimeSpan.FromSeconds(1));
}
```

`UndoResizeAsync` is **accept-then-poll**. It persists the undo intent and is
admitted even while a resize phase is in flight - a large snapshot slice, for
instance - rather than queueing behind the phase it exists to stop. The resize
coordinator observes the intent at its next phase or snapshot-slice boundary
(the snapshot is driven in wall-clock-bounded slices, so that boundary is at most
a few seconds away) and runs the unwind below. The call then waits a bounded time,
well inside the default 30-second response timeout, and returns either once the
unwind has finished or with it still accepted and unwinding; in the second case
`IsResizeUndoPendingAsync` reports `true` until it lands, and the tree-admin
`tree_resize_status` read reports `undoRequested`. Retrying while an undo is pending
is acknowledged again rather than refused, and a retry after the unwind finished is
refused with a message naming the resize that was already undone, so a slow first
undo is never mistaken for a failed one. An unwind that cannot be applied - the old
tree has already been purged, say - is withdrawn, the resize carries on as if the
undo had not been asked for, and the call that is waiting for it throws
`InvalidOperationException` with the reason. While an accepted undo is pending,
`ResizeAsync` throws `InvalidOperationException` rather than start or re-affirm a
resize the coordinator is about to unwind.

The unwind is phase-aware:

- **Before swap** (still in the copy step) - aborts the snapshot coordinator, clears the shadow forwarding on every source shard, discards the half-built destination tree, and returns the source to a fully-writable state. No alias was ever set, so clients never observed the destination. An undo accepted while a snapshot slice was running is unwound from here even if that slice finished the copy: the alias is never swapped onto a copy the operator asked to discard.
- **After swap** (the `Swap`, `RejectOldShards` or `RetireOldCopy` step) - first arms every shard of the resized copy to redirect a router that still addresses it onto the old tree, then points the alias back at the old physical tree in the same registry write as the shard map that describes it (so no reader pairs the old tree with the resized copy's map, or routes to the logical id's retired shards after a second resize), and only then clears the rejecting phase on source shards. The order mirrors the forward swap's fence-before-flip, so the two copies never serve the logical tree at once: a call routed while the undo runs retries until the copy the alias names takes it ([#4453](https://github.com/NSTA1/Orleans.Lattice/issues/4453)), and a swap back that fails releases the redirect again unless the alias did move. The undo then restores the original registry configuration, defensively aborts any post-swap snapshot still attached, and discards the new snapshot tree. The old physical tree is recovered from soft-delete only when it was actually soft-deleted: `RetireOldCopy` is the only step that deletes it, and it does so at the very end, so throughout `Swap` and `RejectOldShards` - and in `RetireOldCopy` until the delete lands - the old tree is still live and is simply left alone.

Once the soft-delete window expires and the old tree is purged, the resize can no longer be undone.

Either way the destination is discarded rather than merely deleted: its shards are marked deleted and purged after the soft-delete window like any retired copy, but its write-ahead-log retention - every leaf materialiser pin held against it, and its log - is released at once, and it can never be recovered. See [Discarding an undone resize's copy](tree-deletion.md#discarding-an-undone-resizes-copy).

### Important considerations

- **Availability:** reads and writes continue throughout. The rejecting window at swap is absorbed by the routing tier, which re-resolves the alias and retries a call that reaches a rejecting shard - callers observe internal retries for the moment between the fence and the alias flip, not an error. Bulk loads and appends are the exception (see the swap step above).
- **Storage:** both the old and new physical trees exist simultaneously until the old tree is purged. Plan for approximately 2× the tree's storage usage during this window. See [Tree Storage](tree-storage.md) for per-provider capacity considerations.
- **Hot-path cost during drain:** every write between `BeginShadowForwardAsync` and swap pays one extra grain hop for the parallel forward. For same-cluster destinations this is in the millisecond range. Prefer off-peak windows for large resizes even though they are online.
- **Concurrency cap:** `LatticeOptions.MaxConcurrentDrains` (default 4) bounds the number of concurrent per-shard drains `TreeSnapshotGrain` dispatches. Mirrors `MaxConcurrentMigrations` for reshard.
- **Idempotency:** calling `ResizeAsync` again with the same parameters while a resize is in progress is a no-op. Calling with different parameters throws `InvalidOperationException`.
- **Registry is the source of truth:** the new sizing is persisted in the tree registry - on the new physical tree's entry when the resize creates it, and on the logical tree's entry at the swap - and every structural grain reads its sizing from the registry. You do not need to update `LatticeOptions` in silo configuration separately - `LatticeOptions` no longer exposes `MaxLeafKeys` / `MaxInternalChildren` / `ShardCount`.
- **Empty-tree fast-path:** if the tree has no live entries yet - as an emptiness probe bounded by `LatticeOptions.EmptyTreeProbeBudget` (default 10 seconds) observes it; an inconclusive probe reads as not empty - `ResizeAsync` and `ReshardAsync` update the registry pin in-place and return immediately without activating the coordinator machinery. This is the recommended way to start a tree with non-default sizing.
- **Validation:** `newMaxLeafKeys` must be at least 2 and `newMaxInternalChildren` at least 3; smaller values throw `ArgumentOutOfRangeException` before anything is persisted.
- **Interlocks:** while a resize is in flight, the autonomic split monitor suppresses splits on the tree, automatic over-split healing admits no new fold, and `ReshardAsync` throws `InvalidOperationException` - except on an observably empty tree, which it re-pins without checking (see [Online Reshard](online-reshard.md#semantics)); likewise, `ResizeAsync` throws `InvalidOperationException` while a reshard is in flight. A resize and the shard migrations that move virtual slots between shards - an adaptive split and an online consolidation - also exclude each other, because the resize fixes the shards it copies and fences, and the map it carries at the swap, when it starts: `ResizeAsync` throws `InvalidOperationException` while a split or consolidation is in flight on any shard of the tree, and a split or fold refuses to start - or, if it opened its shadow-write window after the resize started, backs out (a split started by hand throws `InvalidOperationException`; one resumed after a restart is abandoned before its drain) - while the resize is in flight, while an undo of it is pending or running, and after it completes for as long as the old physical tree still mirrors into the destination, which it does through the soft-delete window until the purge. A split of the destination in that window would move keys that the old tree's shard-for-shard mirror, and an atomic batch still bound to the old tree, cannot follow. So a migration can never commit a map the resize does not carry ([#4452](https://github.com/NSTA1/Orleans.Lattice/issues/4452)). The same hold applies to `ReshardAsync`, which is made of splits and folds: after a resize it throws `InvalidOperationException` until the resize is undone or `SoftDeleteDuration` has passed and the previous copy has been purged, so reshard before you resize rather than straight after. The check fails closed: if the resize coordinator cannot answer, for instance during a rolling upgrade from a build that predates it, the split or fold is refused and the autonomic drivers propose it again later. If you need both, let the reshard complete first, then resize. A resize and its undo also hold the tree's alias for as long as they run, so `DeleteTreeAsync` throws `InvalidOperationException` meanwhile, and `ResizeAsync` or `UndoResizeAsync` throws it on a tree that is deleted or has a delete pending, or while a shadow-cutover restore or schema remediation holds the alias.
- **`ShardCount` cannot be resized via `ResizeAsync`.** Changing shard count requires re-hashing all keys, which `ResizeAsync` does not support. Use `ReshardAsync` for that; it runs online, driving the adaptive shard-split primitive (shadow-write, drain, swap, reject) to grow the count and online shard consolidation, its inverse, to shrink it. See [Online Reshard](online-reshard.md).

### Manual trigger (testing)

In integration tests, the existing test harnesses call the internal resize coordinator directly to drive resize passes synchronously; its run-to-completion undo likewise takes the coordinator's turn and so waits behind an in-flight phase. The coordinator is **declared `internal`** - consumer assemblies cannot reference or invoke it. Use `ILattice.ResizeAsync` and `ILattice.UndoResizeAsync` for all non-test scenarios; they delegate to the coordinator (`UndoResizeAsync` through the accept-then-poll undo described above), and `ILattice.IsResizeCompleteAsync()` and `ILattice.IsResizeUndoPendingAsync()` poll their progress.

## See also

- [Tree Storage](tree-storage.md) - storage-provider limits, grain-state size estimation, per-provider sizing recommendations, default-configuration assessment, key trade-offs.
- [Online Reshard](online-reshard.md) - growing or shrinking the physical shard count online.
- [Snapshots](snapshots.md) - the underlying drain primitive used by `ResizeAsync`.
- [Tree Registry](tree-registry.md) - the registry entry that pins `MaxLeafKeys`, `MaxInternalChildren`, and `ShardCount` per tree.
- [Consistency](consistency.md) - consistency guarantees of `ResizeAsync` and `UndoResizeAsync`.
