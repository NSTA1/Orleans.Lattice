# Configuration

## Registering Lattice

Every silo must call `AddLattice` to register the grain storage provider that Lattice grains use internally:

```csharp
siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
```

The `name` parameter is the storage provider name (`"lattice"`). Replace `AddMemoryGrainStorage` with any Orleans storage provider (Azure Blob, Azure Table, ADO.NET, etc.) for durable storage.

## Setting Options

Lattice uses the standard .NET [named options](https://learn.microsoft.com/dotnet/core/extensions/options#named-options-support-using-iconfigurenamedoptions) pattern. Each tree resolves its options by name (the tree ID passed to `GetGrain<ILattice>(treeId)`).

### Global defaults

Use `ConfigureLattice` without a tree name to set defaults that apply to every tree unless overridden:

```csharp
siloBuilder.ConfigureLattice(o =>
{
    o.MaxLeafKeys = 256;
    o.ShardCount = 32;
});
```

### Per-tree overrides

Pass a tree name to override specific options for a single tree:

```csharp
siloBuilder.ConfigureLattice("high-throughput-tree", o =>
{
    o.ShardCount = 128;
    o.MaxLeafKeys = 64;
});

siloBuilder.ConfigureLattice("archive-tree", o =>
{
    o.TombstoneGracePeriod = Timeout.InfiniteTimeSpan; // disable compaction
});
```

Per-tree overrides are layered on top of the global defaults. Only the properties you set in the override are changed; everything else inherits from the global configuration.

## Options Reference

| Option | Type | Default | Safe to change after data exists? |
|---|---|---|---|
| `ShardCount` | `int` | 64 | **No** |
| `MaxLeafKeys` | `int` | 128 | **No** |
| `MaxInternalChildren` | `int` | 128 | **No** |
| `KeysPageSize` | `int` | 512 | Yes |
| `TombstoneGracePeriod` | `TimeSpan` | 24 hours | Yes |
| `SoftDeleteDuration` | `TimeSpan` | 72 hours | Yes |
| `CacheTtl` | `TimeSpan` | `TimeSpan.Zero` (refresh on every read) | Yes |

### `ShardCount`

The number of independent sub-trees the key space is divided into. Each key is assigned to a shard via `XxHash32(key) % ShardCount`. More shards mean more write parallelism (each shard is an independent grain with its own lock-free write path) but also more grains and a wider scatter-gather for global key scans.

> **⚠️ Do not change after data exists.** Changing `ShardCount` changes the hash-to-shard mapping. Keys already stored under the old shard count will no longer be routable — reads will miss, writes will create duplicates in the wrong shard, and the tree will be in an inconsistent state. Choose a shard count before first use and keep it fixed for the lifetime of the tree.

### `MaxLeafKeys`

The maximum number of key-value entries a leaf node holds before it splits. Smaller values produce more frequent splits (more grains, shallower leaves) while larger values reduce grain count at the cost of larger per-grain state.

> **⚠️ Do not change after data exists.** Existing leaves were split based on the original threshold. Lowering the value does not retroactively split over-full leaves, and raising it does not merge under-full ones. The result is an unbalanced tree with unpredictable split behaviour. If new leaves split at a different threshold than existing ones, parent internal nodes may accumulate children unevenly.

### `MaxInternalChildren`

The maximum number of child references an internal node holds before it splits. This controls the branching factor of the tree above the leaf level.

> **⚠️ Do not change after data exists.** The same reasoning as `MaxLeafKeys` applies — existing internal nodes were split at the original threshold and will not be rebalanced.

### `KeysPageSize`

The number of keys returned per page during ordered key scans (`KeysAsync`). Larger pages reduce the number of grain calls at the cost of larger messages. This is a performance tuning knob and does not affect tree structure.

This option can be changed freely at any time. It takes effect on the next `KeysAsync` call.

### `TombstoneGracePeriod`

How long a deleted key's tombstone is retained before it becomes eligible for permanent removal by the compaction process. The grace period exists so that all cache replicas (`LeafCacheGrain` activations across silos) have time to observe the delete via delta replication before the tombstone disappears.

Set to `Timeout.InfiniteTimeSpan` to disable tombstone compaction entirely. This is useful for trees where deletes are rare or where tombstone accumulation is acceptable.

```csharp
// Compact aggressively (12 hours)
siloBuilder.ConfigureLattice(o => o.TombstoneGracePeriod = TimeSpan.FromHours(12));

// Disable compaction for a specific tree
siloBuilder.ConfigureLattice("archive-tree", o =>
{
    o.TombstoneGracePeriod = Timeout.InfiniteTimeSpan;
});
```

This option can be changed freely at any time. The new grace period takes effect on the next compaction reminder tick. The reminder interval is automatically set to match the grace period (clamped to a minimum of 1 minute, the Orleans reminder floor).

### `SoftDeleteDuration`

How long a soft-deleted tree's data is retained in storage before being permanently purged. During this window the tree is inaccessible — all reads and writes throw `InvalidOperationException` — but its grain state still exists in the storage provider. After the duration elapses, a grain reminder triggers a full purge that walks every shard, clears all leaf and internal node state, and deactivates each grain.

Set to `TimeSpan.Zero` for immediate purge on the next reminder tick (clamped to a 1-minute minimum by the Orleans reminder floor).

```csharp
// Retain deleted trees for 7 days
siloBuilder.ConfigureLattice(o => o.SoftDeleteDuration = TimeSpan.FromDays(7));

// Immediate purge for a specific tree
siloBuilder.ConfigureLattice("ephemeral-tree", o =>
{
    o.SoftDeleteDuration = TimeSpan.Zero;
});
```

This option can be changed freely at any time. The new duration takes effect on the next deletion. Changing it does not affect trees that have already been deleted.

### `CacheTtl`

Minimum time between consecutive delta refreshes from the primary leaf in the `LeafCacheGrain`. When set to `TimeSpan.Zero` (the default), every read triggers a delta refresh — the version-vector comparison on the primary is cheap but the RPC overhead remains. Setting a non-zero value allows the cache to serve reads from its local dictionary without contacting the primary, trading freshness for lower read latency.

```csharp
// Allow up to 100 ms of staleness for lower read latency
siloBuilder.ConfigureLattice(o => o.CacheTtl = TimeSpan.FromMilliseconds(100));

// Per-tree: aggressive freshness for a real-time tree
siloBuilder.ConfigureLattice("realtime", o =>
{
    o.CacheTtl = TimeSpan.Zero; // refresh on every read (default)
});
```

This option can be changed freely at any time. The new TTL takes effect on the next read. A value of `TimeSpan.Zero` preserves the original behaviour (refresh on every read).

## Storage Provider Name

Lattice grains use the storage provider named `"lattice"` (exposed as `LatticeOptions.StorageProviderName`). The `AddLattice` extension method passes this name to your storage registration delegate. In advanced scenarios where you register storage directly, use this constant to ensure the provider name matches:

```csharp
siloBuilder.AddMemoryGrainStorage(LatticeOptions.StorageProviderName);
```

## Full Example

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.UseOrleans(silo =>
{
    silo.UseLocalhostClustering();

    // Register Lattice with Azure Blob storage
    silo.AddLattice((silo, name) =>
        silo.AddAzureBlobGrainStorage(name, options =>
        {
            options.BlobServiceClient = new BlobServiceClient(connectionString);
        }));

    // Global defaults
    silo.ConfigureLattice(o =>
    {
        o.ShardCount = 64;
        o.MaxLeafKeys = 128;
        o.MaxInternalChildren = 128;
        o.KeysPageSize = 1024;
        o.TombstoneGracePeriod = TimeSpan.FromHours(12);
        o.SoftDeleteDuration = TimeSpan.FromHours(72);
    });

    // Per-tree: higher shard count for a high-write tree
    silo.ConfigureLattice("events", o =>
    {
        o.ShardCount = 128;
    });

    // Per-tree: disable compaction for an append-only tree
    silo.ConfigureLattice("audit-log", o =>
    {
        o.TombstoneGracePeriod = Timeout.InfiniteTimeSpan;
    });
});
```
