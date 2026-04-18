# Lattice Public API Reference

Lattice is a distributed, CRDT-based B+ tree built on [Microsoft Orleans](https://learn.microsoft.com/dotnet/orleans/). It exposes a single entry-point grain interface, `ILattice`, that routes operations to sharded internal storage. See [Architecture](architecture.md) for the full grain layer design.

## Setup

Install the NuGet package:

```shell
dotnet add package Orleans.Lattice
```

Add the namespace import:

```csharp
using Orleans.Lattice;
```

Register Lattice on the silo, providing a storage provider:

```csharp
siloBuilder.AddLattice((silo, storageName) =>
    silo.AddMemoryGrainStorage(storageName));
```

Per-tree options can be configured with the `ConfigureLattice` extension method (see [Configuration](configuration.md) for the full options reference and per-tree override semantics):

```csharp
siloBuilder.ConfigureLattice("my-tree", o =>
{
    o.MaxLeafKeys = 256;
    o.ShardCount = 32;
});
```

## `ILattice`

Obtain an `ILattice` grain from the grain factory using the tree's logical name as the string key:

```csharp
var tree = grainFactory.GetGrain<ILattice>("my-tree");
```

### Runtime Operations

These methods are used during normal application flow to read, write, and enumerate data. They are safe to call concurrently and do not affect tree availability.

#### Single-Key

| Method | Signature | Description |
|--------|-----------|-------------|
| `GetAsync` | `Task<byte[]?> GetAsync(string key)` | Returns the value for `key`, or `null` if absent or tombstoned. Reads are served via a [stateless-worker cache](caching.md); `CacheTtl` controls how long cached entries are served before refreshing from the primary leaf. |
| `GetWithVersionAsync` | `Task<VersionedValue> GetWithVersionAsync(string key)` | Returns the value and its `HybridLogicalClock` version for `key`, or a default `VersionedValue` with `null` value and zero version when absent/tombstoned. Use the returned version with `SetIfVersionAsync` for optimistic concurrency (CAS). Reads directly from the primary leaf (not cached) to ensure version freshness. |
| `ExistsAsync` | `Task<bool> ExistsAsync(string key)` | Returns `true` if `key` exists and is not tombstoned. |
| `SetAsync` | `Task SetAsync(string key, byte[] value)` | Inserts or updates the value for `key`. |
| `SetIfVersionAsync` | `Task<bool> SetIfVersionAsync(string key, byte[] value, HybridLogicalClock expectedVersion)` | Sets `key` to `value` only if the entry's current `HybridLogicalClock` matches `expectedVersion`. Returns `true` if the write was applied, `false` if the version did not match (another writer updated the key). For a new key, pass `HybridLogicalClock.Zero` as the expected version. Enables safe read-modify-write patterns without distributed locks. |
| `GetOrSetAsync` | `Task<byte[]?> GetOrSetAsync(string key, byte[] value)` | Sets `key` to `value` only if the key does not already exist (or is tombstoned). Returns the existing value when the key is live, or `null` when the value was newly written. Avoids a read-then-write roundtrip by short-circuiting at the leaf grain. |
| `DeleteAsync` | `Task<bool> DeleteAsync(string key)` | Tombstones `key`. Returns `true` if it was live. Tombstones are removed by [background compaction](tombstone-compaction.md). |

#### Batch

| Method | Signature | Description |
|--------|-----------|-------------|
| `GetManyAsync` | `Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys)` | Fetches multiple keys in parallel across shards. Missing/tombstoned keys are omitted from the result. |
| `SetManyAsync` | `Task SetManyAsync(List<KeyValuePair<string, byte[]>> entries)` | Inserts or updates multiple entries in parallel across shards. |
| `DeleteRangeAsync` | `Task<int> DeleteRangeAsync(string startInclusive, string endExclusive)` | Tombstones all live keys in the lexicographic range [`startInclusive`, `endExclusive`] by walking the leaf chain in each shard. Returns the total number of keys tombstoned. |
| `CountAsync` | `Task<int> CountAsync()` | Returns the total number of live (non-tombstoned) keys across all shards by fanning out in parallel. **Strongly consistent during shard splits**: uses per-slot reconciliation against the registry's monotonic `ShardMap.Version` plus bounded optimistic retry (`LatticeOptions.MaxScanRetries`, default 3) so the result reflects the exact live key set even when concurrent splits move slots between shards mid-scan. Throws `InvalidOperationException` if the topology keeps mutating beyond the retry budget. |
| `CountPerShardAsync` | `Task<IReadOnlyList<int>> CountPerShardAsync()` | Returns the number of live keys in each shard as an ordered list (index = shard index). Useful for diagnostics and load-balancing analysis. |

#### Enumeration

| Method | Signature | Description |
|--------|-----------|-------------|
| `KeysAsync` | `IAsyncEnumerable<string> KeysAsync(string? startInclusive, string? endExclusive, bool reverse, bool? prefetch)` | Streams live keys in strict lexicographic order via paginated k-way merge across shards. When `prefetch` is `true` (or `null` with `PrefetchKeysScan` enabled), the next page from each shard is fetched in parallel while the current page is consumed, hiding grain-call latency during large scans. **Strongly consistent and strictly ordered during shard splits (F-032)**: when a shard reports moved-away slots or the `ShardMap` version advances mid-scan, the orchestrator drains the affected slots from their current owners into a sorted in-memory cursor and injects it into the same k-way merge priority queue, so output remains globally sorted end-to-end. A per-call `HashSet<string>` suppresses duplicates across pre- and post-swap views. Reconciliation is bounded by `LatticeOptions.MaxScanRetries`; throws `InvalidOperationException` if the topology keeps mutating beyond that budget (see [Scan reliability](#scan-reliability) below). See [`docs/shard-splitting.md`](shard-splitting.md). |
| `EntriesAsync` | `IAsyncEnumerable<KeyValuePair<string, byte[]>> EntriesAsync(string? startInclusive, string? endExclusive, bool reverse)` | Streams live key-value entries in strict lexicographic key order via paginated k-way merge across shards. Useful for exports, migrations, and analytics without a separate `GetAsync` per key. **Strongly consistent and strictly ordered during shard splits (F-032)** with the same reconciliation-cursor injection algorithm as `KeysAsync`. Subject to the same `InvalidOperationException` contract on retry exhaustion — see [Scan reliability](#scan-reliability). |

### Scan reliability

`CountAsync`, `KeysAsync`, and `EntriesAsync` use bounded optimistic
retry (`LatticeOptions.MaxScanRetries`, default 3) to reconcile against
concurrent shard splits. If the shard topology keeps mutating after
every reconciliation step, the scan throws
`InvalidOperationException` rather than returning a silently
incomplete result.

Under the default configuration (`MaxConcurrentAutoSplits = 2`,
`HotShardSplitCooldown = 2 minutes`) this is not a realistic
operational concern: splits are rate-limited well below the retry
budget. Point operations (`GetAsync` / `SetAsync` / `DeleteAsync` /
`SetIfVersionAsync`) transparently retry on `StaleShardRoutingException`
during shard-map swaps and never surface this exception to callers.

Callers running multi-minute export scans in aggressively split-prone
workloads have three options:

1. Raise `LatticeOptions.MaxScanRetries` — cheap, addresses transient
   topology churn.
2. Wrap the scan in an application-level retry with exponential backoff
   and resume from the last successfully yielded key (using
   `startInclusive`).
3. Wait for **G-003 (Stateful cursor grain)** — server-side checkpointed
   iteration designed for long-running exports that survive topology
   changes, silo failover, and client restarts without caller retry
   code.

### Maintenance Operations

These methods manage tree structure and lifecycle. Several of them **take the tree offline** — reads and writes will throw `InvalidOperationException` while the operation is in progress. Plan maintenance windows accordingly.

#### Bulk Loading

| Method | Signature | Description |
|--------|-----------|-------------|
| `BulkLoadAsync` | `Task BulkLoadAsync(IReadOnlyList<KeyValuePair<string, byte[]>> entries)` | Bottom-up bulk load into an empty tree. Entries are sorted internally. Throws if any shard already has data. See [Bulk Loading](bulk-loading.md) for streaming ingestion and recovery guarantees. |

#### Tree Lifecycle

| Method | Signature | Description |
|--------|-----------|-------------|
| `TreeExistsAsync` | `Task<bool> TreeExistsAsync()` | Returns `true` if this tree is registered in the internal [tree registry](tree-registry.md). |
| `GetAllTreeIdsAsync` | `Task<IReadOnlyList<string>> GetAllTreeIdsAsync()` | Returns all registered tree IDs in sorted order. System trees (prefixed with `_lattice_`) are excluded. Physical trees created by `ResizeAsync` and `SnapshotAsync` are included. |
| `DeleteTreeAsync` | `Task DeleteTreeAsync()` | Soft-deletes the tree. Data is retained for `SoftDeleteDuration` before purge. Idempotent. ⚠️ **Takes the tree offline** — all reads and writes will throw `InvalidOperationException` until the tree is recovered. See [Tree Deletion](tree-deletion.md). |
| `RecoverTreeAsync` | `Task RecoverTreeAsync()` | Recovers a soft-deleted tree before purge completes. |
| `PurgeTreeAsync` | `Task PurgeTreeAsync()` | Immediately purges a soft-deleted tree without waiting for the retention window. ⚠️ **Permanently destroys all data** — this cannot be undone. |

#### Resize

| Method | Signature | Description |
|--------|-----------|-------------|
| `ResizeAsync` | `Task ResizeAsync(int newMaxLeafKeys, int newMaxInternalChildren)` | Resizes the tree by creating an offline snapshot with new sizing into a new physical tree, swapping the tree alias, and soft-deleting the old data. The tree is unavailable during the snapshot phase but immediately accessible after the swap. Undoable within the `SoftDeleteDuration` retention window. See [Tree Sizing](tree-sizing.md#resizing-an_existing_tree). ⚠️ **Takes the tree offline** during the snapshot phase. |
| `UndoResizeAsync` | `Task UndoResizeAsync()` | Undoes the most recent resize by recovering the old physical tree, removing the alias, restoring the original registry configuration, and deleting the new snapshot tree. Only available while the old tree is still within its `SoftDeleteDuration` window (before purge completes). |

#### Merge

| Method | Signature | Description |
|--------|-----------|-------------|
| `MergeAsync` | `Task MergeAsync(string sourceTreeId)` | Merges all entries from `sourceTreeId` into this tree using LWW semantics, preserving original timestamps. For each key present in both trees, the entry with the higher `HybridLogicalClock` timestamp wins. Tombstones are also merged, ensuring deletes propagate correctly. The source tree remains unmodified. Source and target trees may have different shard counts — entries are re-hashed to the correct target shard during merge. See [Architecture](architecture.md) for details on the CRDT merge primitives. |

#### Snapshots

| Method | Signature | Description |
|--------|-----------|-------------|
| `SnapshotAsync` | `Task SnapshotAsync(string destinationTreeId, SnapshotMode mode, int? maxLeafKeys, int? maxInternalChildren)` | Creates a point-in-time copy of the tree into `destinationTreeId`. In `Offline` mode the source is locked during the copy; in `Online` mode it remains available (best-effort consistency). Optional sizing overrides apply to the destination tree. ⚠️ **`Offline` mode takes the tree offline** — all reads and writes throw `InvalidOperationException` until the snapshot completes and shards are unmarked. See [Snapshots](snapshots.md). |

#### Operation Status

| Method | Signature | Description |
|--------|-----------|-------------|
| `IsMergeCompleteAsync` | `Task<bool> IsMergeCompleteAsync()` | Returns `true` if no merge operation is in progress — either the most recent merge has completed or no merge has ever been initiated (vacuously complete). |
| `IsSnapshotCompleteAsync` | `Task<bool> IsSnapshotCompleteAsync()` | Returns `true` if no snapshot operation is in progress — either the most recent snapshot has completed or no snapshot has ever been initiated (vacuously complete). |
| `IsResizeCompleteAsync` | `Task<bool> IsResizeCompleteAsync()` | Returns `true` if no resize operation is in progress — either the most recent resize has completed or no resize has ever been initiated (vacuously complete). |

## `SnapshotMode`

Controls source-tree availability during a snapshot operation.

| Value | Description |
|-------|-------------|
| `Offline` | Source tree is locked (marked deleted) during the copy, guaranteeing a fully consistent snapshot. |
| `Online` | Source tree remains available. The result is a best-effort point-in-time copy — concurrent writes may cause minor inconsistencies across shards. |

## `LatticeExtensions`

| Method | Description |
|--------|-------------|
| `BulkLoadAsync(IAsyncEnumerable<...>, IGrainFactory, int chunkSize)` | Streaming bulk load for large datasets. Input **must** be pre-sorted in ascending key order. Routing is resolved via `ILattice.GetRoutingAsync()` so the per-tree `ShardMap` is honoured. See [Bulk Loading](bulk-loading.md). |
| `BulkLoadAsync(..., int shardCount, int chunkSize)` *(obsolete)* | Legacy streaming overload that takes an explicit `shardCount`. Bypasses the persisted `ShardMap` and will mis-route entries on trees with non-default maps. Use the overload above. |

## `TypedLatticeExtensions`

Extension methods that serialize/deserialize values via an `ILatticeSerializer<T>`, eliminating per-caller `byte[]` boilerplate. Each method has two overloads: one accepting an explicit serializer and one that defaults to `JsonLatticeSerializer<T>` (System.Text.Json with UTF-8 encoding).

```csharp
// Default (System.Text.Json):
await tree.SetAsync("user:1", new User("Alice", 30));
var user = await tree.GetAsync<User>("user:1");

// Custom serializer:
var serializer = new JsonLatticeSerializer<User>(new JsonSerializerOptions { ... });
await tree.SetAsync("user:1", new User("Alice", 30), serializer);

// Compare-and-swap (CAS):
var versioned = await tree.GetWithVersionAsync<User>("user:1");
var updated = versioned.Value! with { Age = 31 };
bool success = await tree.SetIfVersionAsync("user:1", updated, versioned.Version);
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `GetAsync<T>` | `Task<T?> GetAsync<T>(this ILattice, string key, ILatticeSerializer<T>)` | Returns the deserialized value for `key`, or `default` if absent/tombstoned. |
| `GetWithVersionAsync<T>` | `Task<Versioned<T>> GetWithVersionAsync<T>(this ILattice, string key, ILatticeSerializer<T>)` | Returns the deserialized value and its `HybridLogicalClock` version. Returns a `Versioned<T>` with `default` value and zero version when absent/tombstoned. |
| `GetOrSetAsync<T>` | `Task<T?> GetOrSetAsync<T>(this ILattice, string key, T value, ILatticeSerializer<T>)` | Sets `key` to `value` only if absent/tombstoned. Returns the existing deserialized value when live, or `default` when newly written. |
| `SetAsync<T>` | `Task SetAsync<T>(this ILattice, string key, T value, ILatticeSerializer<T>)` | Serializes and stores `value` under `key`. |
| `SetIfVersionAsync<T>` | `Task<bool> SetIfVersionAsync<T>(this ILattice, string key, T value, HybridLogicalClock expectedVersion, ILatticeSerializer<T>)` | Serializes and conditionally writes `value` only if the entry's current version matches `expectedVersion`. Returns `true` if applied. |
| `GetManyAsync<T>` | `Task<Dictionary<string, T>> GetManyAsync<T>(this ILattice, List<string> keys, ILatticeSerializer<T>)` | Fetches and deserializes multiple keys. Missing/tombstoned keys are omitted. |
| `SetManyAsync<T>` | `Task SetManyAsync<T>(this ILattice, List<KeyValuePair<string, T>> entries, ILatticeSerializer<T>)` | Serializes and inserts/updates multiple entries in parallel. |
| `BulkLoadAsync<T>` | `Task BulkLoadAsync<T>(this ILattice, IReadOnlyList<KeyValuePair<string, T>> entries, ILatticeSerializer<T>)` | Serializes and bulk-loads entries into an empty tree. |
| `EntriesAsync<T>` | `IAsyncEnumerable<KeyValuePair<string, T>> EntriesAsync<T>(this ILattice, ILatticeSerializer<T>, string?, string?, bool)` | Streams live entries, deserializing values via the provided serializer. |

Each method also has a parameterless overload that defaults to `JsonLatticeSerializer<T>.Default`.

## `ILatticeSerializer<T>`

Implement this interface to provide a custom serialization strategy. The library ships with `JsonLatticeSerializer<T>` as the default.

| Member | Signature | Description |
|--------|-----------|-------------|
| `Serialize` | `byte[] Serialize(T value)` | Converts a value to bytes for storage. |
| `Deserialize` | `T Deserialize(byte[] bytes)` | Converts bytes back to a value. |

## `LatticeOptions`

See [Configuration](configuration.md) for detailed guidance on each option, immutability constraints, and per-tree overrides via the [tree registry](tree-registry.md).

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `MaxLeafKeys` | `int` | 128 | Max keys per leaf before splitting. |
| `MaxInternalChildren` | `int` | 128 | Max children per internal node before splitting. |
| `ShardCount` | `int` | 64 | Number of independent shards. |
| `VirtualShardCount` | `int` | 4096 | Size of the virtual shard space. Keys hash into `[0, VirtualShardCount)` and a per-tree `ShardMap` collapses virtual slots onto physical shards. Must be ≥ `ShardCount` and divisible by `ShardCount`. |
| `KeysPageSize` | `int` | 512 | Keys per page in `KeysAsync` pagination. |
| `TombstoneGracePeriod` | `TimeSpan` | 24 h | Minimum age before a tombstone is eligible for compaction. `InfiniteTimeSpan` disables compaction. |
| `SoftDeleteDuration` | `TimeSpan` | 72 h | Retention window after soft-delete before purge fires. |
| `CacheTtl` | `TimeSpan` | `TimeSpan.Zero` | Minimum time between delta refreshes in `LeafCacheGrain`. Zero means refresh on every read. |
| `PrefetchKeysScan` | `bool` | `false` | When `true`, `KeysAsync` pre-fetches the next page from each shard in the background. Overridable per-call via the `prefetch` parameter. |
| `AutoSplitEnabled` | `bool` | `true` | Master switch for autonomic shard splitting. When `false`, `HotShardMonitorGrain` will not trigger any splits. |
| `HotShardOpsPerSecondThreshold` | `int` | 200 | Ops/sec on a single shard that triggers an adaptive split. |
| `HotShardSampleInterval` | `TimeSpan` | 30 s | How often `HotShardMonitorGrain` polls shard hotness counters. |
| `HotShardSplitCooldown` | `TimeSpan` | 2 min | Minimum time between consecutive splits of the same shard. |
| `MaxConcurrentAutoSplits` | `int` | 2 | Maximum in-flight adaptive splits per tree. |
| `SplitDrainBatchSize` | `int` | 1024 | Entries per batch during the shadow-write drain phase of a split. |
| `AutoSplitMinTreeAge` | `TimeSpan` | 60 s | Minimum tree age before the hot-shard monitor begins sampling. Prevents splits during initial bulk-load bursts. |
| `MaxScanRetries` | `int` | 3 | Maximum bounded-retry passes for `CountAsync` / `KeysAsync` / `EntriesAsync` when the shard topology changes mid-scan. |

## Serializable Types

All serializable types carry stable `[Alias]` attributes (prefixed with `ol.`) to ensure wire-format compatibility across versions and prevent collisions when Lattice is hosted alongside other Orleans grains.

Public types below are annotated with `[EditorBrowsable(EditorBrowsableState.Never)]` — they remain `public` for Orleans code generation but are hidden from IntelliSense because they are internal implementation details not intended for direct use.

| Type | Alias | Visibility | Description |
|------|-------|------------|-------------|
| `HybridLogicalClock` | `ol.hlc` | public (hidden) | Hybrid logical clock for conflict-free timestamps. See [State Primitives](state-primitives.md). |
| `LwwValue<T>` | `ol.lwv` | public (hidden) | Last-writer-wins register. |
| `VersionVector` | `ol.vv` | public (hidden) | Causal version vector (pointwise-max merge). |
| `StateDelta` | `ol.sd` | public (hidden) | Delta of changed entries for replication. |
| `SplitResult` | `ol.sr` | public (hidden) | Result of a node split (promoted key + new sibling). |
| `KeysPage` | `ol.kp` | public (hidden) | Paginated batch of keys from a shard scan. |
| `EntriesPage` | `ol.ep` | public (hidden) | Paginated batch of key-value entries from a shard scan. |
| `TreeRegistryEntry` | `ol.tre` | public (hidden) | Per-tree metadata record (config overrides, physical tree alias). |
| `SnapshotMode` | `ol.snm` | public | Enum: `Offline`, `Online`. Controls source-tree availability during a snapshot. |
| `TreeResizeState` | `ol.trs` | internal | Persistent state tracking resize progress across phases. |
| `ResizePhase` | `ol.rp` | internal | Enum: `Snapshot`, `Swap`, `Cleanup`. |
| `TreeSnapshotState` | `ol.tss` | internal | Persistent state tracking snapshot progress across shards. |
| `SnapshotPhase` | `ol.snp` | internal | Enum: `Locking`, `Copying`, `Unlocking`, `Completed`. |
| `TreeDeletionState` | `ol.tds` | internal | Persistent state for soft-delete / purge tracking. |
| `TreeMergeState` | `ol.tms` | internal | Persistent state tracking merge progress across source shards. |
| `CasResult` | `ol.cas` | public (hidden) | Result of a compare-and-swap operation (success, current version, optional split). |
| `VersionedValue` | `ol.vvl` | public (hidden) | A `byte[]` value paired with its `HybridLogicalClock` version for CAS reads. |
| `Versioned<T>` | `ol.ver` | public (hidden) | A typed value paired with its `HybridLogicalClock` version (used by typed extensions). |
| `ShardHotness` | `ol.sh` | public (hidden) | Volatile shard hotness counters (reads, writes, window) returned by `IShardRootGrain.GetHotnessAsync()`. |
| `ShardMap` | `ol.sm` | public (hidden) | Per-tree mapping from virtual shard slots to physical shard indices. Persisted on `TreeRegistryEntry`. |
| `RoutingInfo` | `ol.ri` | public (hidden) | Per-activation routing snapshot returned by `ILattice.GetRoutingAsync()`: physical tree id plus the resolved `ShardMap`. Used by infrastructure (e.g. streaming bulk load) that must route to the same physical shards as `LatticeGrain`. |
| `ShardCountResult` | `ol.scr` | internal | Per-shard count plus the set of virtual slots the shard observed in its `MovedAwaySlots` table during the count. Used by `IShardRootGrain.CountWithMovedAwayAsync` to coordinate strongly-consistent scans during shard splits. |

## Internal Grain Access Control

Lattice exposes a single public entry-point — `ILattice`. All other grain interfaces (`IShardRootGrain`, `IBPlusLeafGrain`, `IBPlusInternalGrain`, `ILeafCacheGrain`, `ILatticeRegistry`, `ITombstoneCompactionGrain`, `ITreeDeletionGrain`, `ITreeResizeGrain`, `ITreeSnapshotGrain`) are internal implementation details.

Two mechanisms prevent accidental direct use of internal grains:

1. **IntelliSense exclusion** — All internal grain interfaces and public serializable model types (e.g. `HybridLogicalClock`, `SplitResult`, `KeysPage`, `LatticeConstants`) are annotated with `[EditorBrowsable(EditorBrowsableState.Never)]`, hiding them from auto-complete in IDEs. They remain `public` for Orleans code generation but are invisible during normal development.

2. **Grain call filters** — `AddLattice` registers a pair of grain call filters:
   - An **outgoing filter** (`LatticeCallContextFilter`) resolves the current grain context at call time via `IGrainContextAccessor` and checks whether the calling grain implements a Lattice interface (via direct `Type.IsAssignableFrom`). If so, it stamps the outgoing call with a `RequestContext` token. If the caller is **not** a Lattice grain, the filter **clears** the token to prevent it from leaking through a non-Lattice intermediary.
   - An **incoming filter** (`InternalGrainGuardFilter`) rejects calls to internal Lattice grains that do not carry the token, throwing `InvalidOperationException`.

   External client calls never carry the token and are blocked. Calls from non-Lattice grains co-hosted in the same silo are also blocked because the outgoing filter only stamps calls originating from Lattice grains. Both filters cache `Type.IsAssignableFrom` results in a `ConcurrentDictionary<Type, bool>`, so repeated calls from the same grain type cost a single dictionary lookup (~20 ns) rather than a linear scan. All type checks use direct .NET type comparison — there is no dependency on Orleans grain-type name conventions.

> **Note:** The token is not a security credential — it prevents accidental misuse, not malicious access. A determined caller within the silo process could set the `RequestContext` value manually.
