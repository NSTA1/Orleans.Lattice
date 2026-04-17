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
| `ExistsAsync` | `Task<bool> ExistsAsync(string key)` | Returns `true` if `key` exists and is not tombstoned. |
| `SetAsync` | `Task SetAsync(string key, byte[] value)` | Inserts or updates the value for `key`. |
| `GetOrSetAsync` | `Task<byte[]?> GetOrSetAsync(string key, byte[] value)` | Sets `key` to `value` only if the key does not already exist (or is tombstoned). Returns the existing value when the key is live, or `null` when the value was newly written. Avoids a read-then-write roundtrip by short-circuiting at the leaf grain. |
| `DeleteAsync` | `Task<bool> DeleteAsync(string key)` | Tombstones `key`. Returns `true` if it was live. Tombstones are removed by [background compaction](tombstone-compaction.md). |

#### Batch

| Method | Signature | Description |
|--------|-----------|-------------|
| `GetManyAsync` | `Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys)` | Fetches multiple keys in parallel across shards. Missing/tombstoned keys are omitted from the result. |
| `SetManyAsync` | `Task SetManyAsync(List<KeyValuePair<string, byte[]>> entries)` | Inserts or updates multiple entries in parallel across shards. |
| `DeleteRangeAsync` | `Task<int> DeleteRangeAsync(string startInclusive, string endExclusive)` | Tombstones all live keys in the lexicographic range [`startInclusive`, `endExclusive`] by walking the leaf chain in each shard. Returns the total number of keys tombstoned. |
| `CountAsync` | `Task<int> CountAsync()` | Returns the total number of live (non-tombstoned) keys across all shards by fanning out in parallel. |
| `CountPerShardAsync` | `Task<IReadOnlyList<int>> CountPerShardAsync()` | Returns the number of live keys in each shard as an ordered list (index = shard index). Useful for diagnostics and load-balancing analysis. |

#### Enumeration

| Method | Signature | Description |
|--------|-----------|-------------|
| `KeysAsync` | `IAsyncEnumerable<string> KeysAsync(string? startInclusive, string? endExclusive, bool reverse)` | Streams live keys in lexicographic order via paginated k-way merge across shards. |
| `EntriesAsync` | `IAsyncEnumerable<KeyValuePair<string, byte[]>> EntriesAsync(string? startInclusive, string? endExclusive, bool reverse)` | Streams live key-value entries in lexicographic key order via paginated k-way merge across shards. Useful for exports, migrations, and analytics without a separate `GetAsync` per key. |

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
| `ResizeAsync` | `Task ResizeAsync(int newMaxLeafKeys, int newMaxInternalChildren)` | Resizes the tree by creating an offline snapshot with new sizing into a new physical tree, swapping the tree alias, and soft-deleting the old data. The tree is unavailable during the snapshot phase but immediately accessible after the swap. Undoable within the `SoftDeleteDuration` retention window. See [Tree Sizing](tree-sizing.md#resizing-an-existing-tree). ⚠️ **Takes the tree offline** during the snapshot phase. |
| `UndoResizeAsync` | `Task UndoResizeAsync()` | Undoes the most recent resize by recovering the old physical tree, removing the alias, restoring the original registry configuration, and deleting the new snapshot tree. Only available while the old tree is still within its `SoftDeleteDuration` window (before purge completes). |

#### Snapshots

| Method | Signature | Description |
|--------|-----------|-------------|
| `SnapshotAsync` | `Task SnapshotAsync(string destinationTreeId, SnapshotMode mode, int? maxLeafKeys, int? maxInternalChildren)` | Creates a point-in-time copy of the tree into `destinationTreeId`. In `Offline` mode the source is locked during the copy; in `Online` mode it remains available (best-effort consistency). Optional sizing overrides apply to the destination tree. ⚠️ **`Offline` mode takes the tree offline** — all reads and writes throw `InvalidOperationException` until the snapshot completes and shards are unmarked. See [Snapshots](snapshots.md). |

#### Resize

| Method | Signature | Description |
|--------|-----------|-------------|
| `ResizeAsync` | `Task ResizeAsync(int newMaxLeafKeys, int newMaxInternalChildren)` | Resizes the tree by taking an offline snapshot with the new sizing, then swapping the tree alias so reads and writes target the resized tree. The old physical tree is soft-deleted. ⚠️ **Takes the tree offline** during the snapshot phase — all reads and writes throw `InvalidOperationException` until the alias swap completes. See [Tree Sizing](tree-sizing.md) for sizing guidance. |
| `UndoResizeAsync` | `Task UndoResizeAsync()` | Undoes the most recent resize by recovering the old physical tree, removing the alias, restoring the original registry configuration, and deleting the snapshot tree. Only available while the old tree is within its `SoftDeleteDuration` window. |

## `SnapshotMode`

Controls source-tree availability during a snapshot operation.

| Value | Description |
|-------|-------------|
| `Offline` | Source tree is locked (marked deleted) during the copy, guaranteeing a fully consistent snapshot. |
| `Online` | Source tree remains available. The result is a best-effort point-in-time copy — concurrent writes may cause minor inconsistencies across shards. |

## `LatticeExtensions`

| Method | Description |
|--------|-------------|
| `BulkLoadAsync(IAsyncEnumerable<...>, IGrainFactory, int shardCount, int chunkSize)` | Streaming bulk load for large datasets. Input **must** be pre-sorted in ascending key order. See [Bulk Loading](bulk-loading.md). |

## `TypedLatticeExtensions`

Extension methods that serialize/deserialize values via an `ILatticeSerializer<T>`, eliminating per-caller `byte[]` boilerplate. Each method has two overloads: one accepting an explicit serializer and one that defaults to `JsonLatticeSerializer<T>` (System.Text.Json with UTF-8 encoding).

```csharp
// Default (System.Text.Json):
await tree.SetAsync("user:1", new User("Alice", 30));
var user = await tree.GetAsync<User>("user:1");

// Custom serializer:
var serializer = new JsonLatticeSerializer<User>(new JsonSerializerOptions { ... });
await tree.SetAsync("user:1", new User("Alice", 30), serializer);
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `GetAsync<T>` | `Task<T?> GetAsync<T>(this ILattice, string key, ILatticeSerializer<T>)` | Returns the deserialized value for `key`, or `default` if absent/tombstoned. |
| `GetOrSetAsync<T>` | `Task<T?> GetOrSetAsync<T>(this ILattice, string key, T value, ILatticeSerializer<T>)` | Sets `key` to `value` only if absent/tombstoned. Returns the existing deserialized value when live, or `default` when newly written. |
| `SetAsync<T>` | `Task SetAsync<T>(this ILattice, string key, T value, ILatticeSerializer<T>)` | Serializes and stores `value` under `key`. |
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
| `KeysPageSize` | `int` | 512 | Keys per page in `KeysAsync` pagination. |
| `TombstoneGracePeriod` | `TimeSpan` | 24 h | Minimum age before a tombstone is eligible for compaction. `InfiniteTimeSpan` disables compaction. |
| `SoftDeleteDuration` | `TimeSpan` | 72 h | Retention window after soft-delete before purge fires. |
| `CacheTtl` | `TimeSpan` | `TimeSpan.Zero` | Minimum time between delta refreshes in `LeafCacheGrain`. Zero means refresh on every read. |

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
| `TombstoneCompactionState` | `ol.tcs` | internal | Persistent state for tombstone compaction progress. |

## Internal Grain Access Control

Lattice exposes a single public entry-point — `ILattice`. All other grain interfaces (`IShardRootGrain`, `IBPlusLeafGrain`, `IBPlusInternalGrain`, `ILeafCacheGrain`, `ILatticeRegistry`, `ITombstoneCompactionGrain`, `ITreeDeletionGrain`, `ITreeResizeGrain`, `ITreeSnapshotGrain`) are internal implementation details.

Two mechanisms prevent accidental direct use of internal grains:

1. **IntelliSense exclusion** — All internal grain interfaces and public serializable model types (e.g. `HybridLogicalClock`, `SplitResult`, `KeysPage`, `LatticeConstants`) are annotated with `[EditorBrowsable(EditorBrowsableState.Never)]`, hiding them from auto-complete in IDEs. They remain `public` for Orleans code generation but are invisible during normal development.

2. **Grain call filters** — `AddLattice` registers a pair of grain call filters:
   - An **outgoing filter** (`LatticeCallContextFilter`) resolves the current grain context at call time via `IGrainContextAccessor` and checks whether the calling grain implements a Lattice interface (via direct `Type.IsAssignableFrom`). If so, it stamps the outgoing call with a `RequestContext` token. If the caller is **not** a Lattice grain, the filter **clears** the token to prevent it from leaking through a non-Lattice intermediary.
   - An **incoming filter** (`InternalGrainGuardFilter`) rejects calls to internal Lattice grains that do not carry the token, throwing `InvalidOperationException`.

   External client calls never carry the token and are blocked. Calls from non-Lattice grains co-hosted in the same silo are also blocked because the outgoing filter only stamps calls originating from Lattice grains. Both filters cache `Type.IsAssignableFrom` results in a `ConcurrentDictionary<Type, bool>`, so repeated calls from the same grain type cost a single dictionary lookup (~20 ns) rather than a linear scan. All type checks use direct .NET type comparison — there is no dependency on Orleans grain-type name conventions.

> **Note:** The token is not a security credential — it prevents accidental misuse, not malicious access. A determined caller within the silo process could set the `RequestContext` value manually.
