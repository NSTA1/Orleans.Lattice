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
> `TreeRegistryEntry`, not on `LatticeOptions`. Canonical defaults come
> from `LatticeConstants` (128 / 128 / 64) and are seeded into the
> registry on first tree use. After seeding, the pin is the sole source
> of structural truth — all grains read it via
> `LatticeOptionsResolver`. The only supported mutation paths are
> `ResizeAsync` (leaf / internal capacity) and `ReshardAsync` (shard
> count); both run online and update the pin atomically. To start a
> tree with non-default sizing, call `ResizeAsync` / `ReshardAsync` on
> the freshly-created empty tree (empty-tree fast-path — no coordinator
> machinery) or pre-register the pin via
> `ILatticeRegistry.RegisterAsync`.

## Resizing an Existing Tree

If you need to change `MaxLeafKeys` or `MaxInternalChildren` on a tree
that already contains data, use the `ResizeAsync` API:

```csharp verify
var tree = grainFactory.GetGrain<ILattice>("my-tree");
await tree.ResizeAsync(newMaxLeafKeys: 256, newMaxInternalChildren: 64);
```

### How it works

Resize runs **online**: reads and writes remain available throughout. A whole-tree shape change cannot be done in place — every leaf and every internal node has to be re-paginated at the new fan-out — so `ResizeAsync` drains the source into a freshly-provisioned destination physical tree and then atomically swaps the registry alias.

1. **Provision destination** — `TreeResizeGrain` creates a destination physical tree ID (e.g. `my-tree/resized/{operationId}`) registered with the new `MaxLeafKeys` / `MaxInternalChildren`. The destination inherits the source's `ShardCount` and a copy of its `ShardMap`, so slot *x* on the source routes to the same physical shard index on the destination.
2. **Snapshot with shadow forwarding** — the source tree runs under `SnapshotMode.Online`. Before drain begins, every source `ShardRootGrain` is placed in `ShadowForwardPhase.Draining` via `BeginShadowForwardAsync`, and every mutation path (`SetAsync`, `SetManyAsync`, `SetManyAtomicAsync`, `DeleteAsync`, `DeleteManyAsync`, `DeleteRangeAsync`, `BulkLoadAsync`, and saga compensation) runs the local write and a parallel forward to the corresponding destination shard.
3. **Drain** — `TreeSnapshotGrain` reads each source shard's current entries and merges them into the destination via `IShardRootGrain.MergeManyAsync` (bounded by `LatticeOptions.MaxConcurrentDrains`, default 4). Raw `LwwEntry` payloads flow through verbatim: HLC timestamps, tombstone flags, and TTL (`ExpiresAtTicks`) are preserved. When every shard has finished draining, its shadow state transitions to `Drained`; live forwards continue until swap.
4. **Swap** — the registry entry is updated with the new sizing and `SetAliasAsync` atomically points the logical tree ID at the destination. Once complete, each source shard transitions to `ShadowForwardPhase.Rejecting` and subsequent writes against the old physical tree throw `StaleTreeRoutingException`. The stateless-worker `LatticeGrain` catches this exception, re-resolves via the registry, and retries transparently against the destination — callers never observe the transition.
5. **Cleanup** — the old physical tree is soft-deleted. It will be purged automatically after the configured `SoftDeleteDuration` (default 72 hours), leaving `UndoResizeAsync` viable until then.

Each phase transition is persisted. If the silo crashes mid-resize, reminder-anchored `TreeResizeGrain` and `TreeSnapshotGrain` reactivate and resume from the last completed phase; source shards retain their `ShadowForwardState` across activations, so live forwards continue uninterrupted.

### LWW convergence — why shadow forwarding is safe

Every entry carries a hybrid-logical-clock timestamp and all writes flow through a last-writer-wins comparator. That makes the shadow-forward path commutative: whether a live write arrives at the destination before or after the drain reader copies the same key, the destination converges to the entry with the higher HLC. Consequently:

- The parallel `local ∥ forward` write is **not** a two-phase commit. If local succeeds and forward fails, the client sees failure; the next idempotent retry lands on both trees. Writes that briefly land on the source only are captured by the drain reader and re-delivered with their original HLCs.
- The drain uses `MergeManyAsync` (not `BulkLoadRawAsync`) because shadow-forwarded writes can populate destination shards ahead of the drain batch. LWW merge absorbs the race; a bulk-load would error on non-empty destination shards. (The offline snapshot path still uses `BulkLoadRawAsync` — source shards are locked before drain so the destination is guaranteed empty.)

### Cache invalidation

Different physical trees produce different leaf grain IDs, which automatically create fresh `LeafCacheGrain` instances. No explicit cache flush is needed after the alias swap. See [Read Caching](caching.md#cache-invalidation-via-tree-aliasing) for details.

### Undo resize

During the soft-delete window the resize can be undone:

```csharp verify
var tree = grainFactory.GetGrain<ILattice>("my-tree");
await tree.UndoResizeAsync();
```

`UndoResizeAsync` is phase-aware:

- **Before swap** (`Phase == Snapshot`) — aborts the snapshot coordinator, clears every source shard's `ShadowForwardState`, deletes the half-built destination tree, and returns the source to a fully-writable state. No alias was ever set, so clients never observed the destination.
- **After swap** (`Phase ∈ { Swap, Reject, Cleanup }`) — recovers the old physical tree, removes the alias, restores the original registry configuration, clears any residual `Rejecting` phase on source shards, defensively aborts any post-swap snapshot still attached, and deletes the new snapshot tree.

Once the soft-delete window expires and the old tree is purged, the resize can no longer be undone.

### Important considerations

- **Availability:** reads and writes continue throughout. The per-shard `Rejecting` window at swap is absorbed by `LatticeGrain`'s `StaleTreeRoutingException` retry — callers observe at most one internal retry, not an error.
- **Storage:** both the old and new physical trees exist simultaneously until the old tree is purged. Plan for approximately 2× the tree's storage usage during this window. See [Tree Storage](tree-storage.md) for per-provider capacity considerations.
- **Hot-path cost during drain:** every write between `BeginShadowForwardAsync` and swap pays one extra grain hop for the parallel forward. For same-cluster destinations this is in the millisecond range. Prefer off-peak windows for large resizes even though they are online.
- **Concurrency cap:** `LatticeOptions.MaxConcurrentDrains` (default 4) bounds the number of concurrent per-shard drains `TreeSnapshotGrain` dispatches. Mirrors `MaxConcurrentMigrations` for reshard.
- **Idempotency:** calling `ResizeAsync` again with the same parameters while a resize is in progress is a no-op. Calling with different parameters throws `InvalidOperationException`.
- **Registry is the source of truth:** the new sizing values are persisted on the `TreeRegistryEntry` and read through `LatticeOptionsResolver` by every structural grain. You do not need to update `LatticeOptions` in silo configuration separately — `LatticeOptions` no longer exposes `MaxLeafKeys` / `MaxInternalChildren` / `ShardCount`.
- **Empty-tree fast-path:** if the tree has no live entries yet, `ResizeAsync` and `ReshardAsync` update the registry pin in-place and return immediately without activating the coordinator machinery. This is the recommended way to start a tree with non-default sizing.
- **Interlocks:** while a resize is in flight, `HotShardMonitorGrain` suppresses autonomic splits on the tree and `ReshardAsync` throws `InvalidOperationException`. Run reshard first if you need both, then resize.
- **`ShardCount` cannot be resized via `ResizeAsync`.** Changing shard count requires re-hashing all keys, which `ResizeAsync` does not support. Use `ReshardAsync` for that; it runs online via its own shadow-write + swap primitive. See [Online Reshard](online-reshard.md).

### Manual trigger (testing)

In integration tests, the existing test harnesses call `ITreeResizeGrain` directly to drive resize passes synchronously. This grain is **guarded by `InternalGrainGuardFilter`** — external callers (including application code and `dotnet tool` wrappers) cannot invoke it. Use `ILattice.ResizeAsync` for all non-test scenarios; it delegates to `ITreeResizeGrain` internally and exposes `ILattice.IsResizeCompleteAsync()` for progress polling.

## See also

- [Tree Storage](tree-storage.md) — storage-provider limits, grain-state size estimation, per-provider sizing recommendations, default-configuration assessment, key trade-offs.
- [Online Reshard](online-reshard.md) — growing the physical shard count online.
- [Snapshots](snapshots.md) — the underlying drain primitive used by `ResizeAsync`.
- [Tree Registry](tree-registry.md) — the registry entry that pins `MaxLeafKeys`, `MaxInternalChildren`, and `ShardCount` per tree.
- [Consistency](consistency.md) — consistency guarantees of `ResizeAsync` and `UndoResizeAsync`.
