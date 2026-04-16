# Lattice Public API Reference

Lattice is a distributed, CRDT-based B+ tree built on [Microsoft Orleans](https://learn.microsoft.com/dotnet/orleans/). It exposes a single entry-point grain interface, `ILattice`, that routes operations to sharded internal storage.

## Setup

```csharp
siloBuilder.AddLattice((silo, storageName) =>
    silo.AddMemoryGrainStorage(storageName));
```

Per-tree options can be configured with the `ConfigureLattice` extension method:

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

### Single-Key Operations

| Method | Signature | Description |
|--------|-----------|-------------|
| `GetAsync` | `Task<byte[]?> GetAsync(string key)` | Returns the value for `key`, or `null` if absent or tombstoned. |
| `ExistsAsync` | `Task<bool> ExistsAsync(string key)` | Returns `true` if `key` exists and is not tombstoned. |
| `SetAsync` | `Task SetAsync(string key, byte[] value)` | Inserts or updates the value for `key`. |
| `DeleteAsync` | `Task<bool> DeleteAsync(string key)` | Tombstones `key`. Returns `true` if it was live. |

### Batch Operations

| Method | Signature | Description |
|--------|-----------|-------------|
| `GetManyAsync` | `Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys)` | Fetches multiple keys in parallel across shards. Missing/tombstoned keys are omitted from the result. |
| `SetManyAsync` | `Task SetManyAsync(List<KeyValuePair<string, byte[]>> entries)` | Inserts or updates multiple entries in parallel across shards. |
| `BulkLoadAsync` | `Task BulkLoadAsync(IReadOnlyList<KeyValuePair<string, byte[]>> entries)` | Bottom-up bulk load into an empty tree. Entries are sorted internally. Throws if any shard already has data. |

### Enumeration

| Method | Signature | Description |
|--------|-----------|-------------|
| `KeysAsync` | `IAsyncEnumerable<string> KeysAsync(string? startInclusive, string? endExclusive, bool reverse)` | Streams live keys in lexicographic order via paginated k-way merge across shards. |

### Tree Lifecycle

| Method | Signature | Description |
|--------|-----------|-------------|
| `DeleteTreeAsync` | `Task DeleteTreeAsync()` | Soft-deletes the tree. Data is retained for `SoftDeleteDuration` before purge. Idempotent. |
| `RecoverTreeAsync` | `Task RecoverTreeAsync()` | Recovers a soft-deleted tree before purge completes. |
| `PurgeTreeAsync` | `Task PurgeTreeAsync()` | Immediately purges a soft-deleted tree without waiting for the retention window. |

## `LatticeExtensions`

| Method | Description |
|--------|-------------|
| `BulkLoadAsync(IAsyncEnumerable<...>, IGrainFactory, int shardCount, int chunkSize)` | Streaming bulk load for large datasets. Input **must** be pre-sorted in ascending key order. |

## `LatticeOptions`

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

| Type | Alias | Description |
|------|-------|-------------|
| `HybridLogicalClock` | `ol.hlc` | Hybrid logical clock for conflict-free timestamps. |
| `LwwValue<T>` | `ol.lwv` | Last-writer-wins register. |
| `VersionVector` | `ol.vv` | Causal version vector (pointwise-max merge). |
| `StateDelta` | `ol.sd` | Delta of changed entries for replication. |
| `SplitResult` | `ol.sr` | Result of a node split (promoted key + new sibling). |
| `KeysPage` | `ol.kp` | Paginated batch of keys from a shard scan. |
