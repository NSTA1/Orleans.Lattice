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
| `VirtualShardCount` | `int` | 4096 | **No** |
| `MaxLeafKeys` | `int` | 128 | **No** |
| `MaxInternalChildren` | `int` | 128 | **No** |
| `KeysPageSize` | `int` | 512 | Yes |
| `TombstoneGracePeriod` | `TimeSpan` | 24 hours | Yes |
| `SoftDeleteDuration` | `TimeSpan` | 72 hours | Yes |
| `CacheTtl` | `TimeSpan` | `TimeSpan.Zero` (refresh on every read) | Yes |
| `PrefetchKeysScan` | `bool` | `false` | Yes |
| `AutoSplitEnabled` | `bool` | `true` | Yes |
| `HotShardOpsPerSecondThreshold` | `int` | 200 | Yes |
| `HotShardSampleInterval` | `TimeSpan` | 30 seconds | Yes |
| `HotShardSplitCooldown` | `TimeSpan` | 2 minutes | Yes |
| `MaxConcurrentAutoSplits` | `int` | 2 | Yes |
| `SplitDrainBatchSize` | `int` | 1024 | Yes |
| `AutoSplitMinTreeAge` | `TimeSpan` | 60 seconds | Yes |
| `MaxScanRetries` | `int` | 3 | Yes |

### `ShardCount`

The number of independent sub-trees the key space is divided into. Each key is assigned to a shard via the per-tree [`ShardMap`](tree-registry.md#shard-map) — by default `ShardMap.CreateDefault(VirtualShardCount, ShardCount)` produces an identity mapping that is bit-for-bit equivalent to legacy `XxHash32(key) % ShardCount` routing. More shards mean more write parallelism (each shard is an independent grain with its own lock-free write path) but also more grains and a wider scatter-gather for global key scans.

> **⚠️ Do not change after data exists.** Changing `ShardCount` changes the default shard map. Keys already stored under the old shard count will no longer be routable — reads will miss, writes will create duplicates in the wrong shard, and the tree will be in an inconsistent state. Choose a shard count before first use and keep it fixed for the lifetime of the tree.

### `VirtualShardCount`

The size of the virtual shard space (default 4096). Keys hash into `[0, VirtualShardCount)` and the per-tree [`ShardMap`](tree-registry.md#shard-map) collapses ranges of virtual slots onto physical shards. This indirection decouples logical key routing from the physical shard count, enabling future adaptive shard splitting without rehashing existing keys.

`VirtualShardCount` must be ≥ `ShardCount` and divisible by `ShardCount` (validated at startup). The divisibility constraint guarantees that the default identity map preserves legacy `hash % ShardCount` routing exactly.

> **⚠️ Do not change after data exists.** Changing `VirtualShardCount` changes the virtual-slot a key hashes to. Persisted shard maps reference virtual slots, so altering this value invalidates all existing routing. Choose a value before first use and keep it fixed.

### `MaxLeafKeys`

The maximum number of key-value entries a leaf node holds before it splits. Smaller values produce more frequent splits (more grains, shallower leaves) while larger values reduce grain count at the cost of larger per-grain state.

> **⚠️ Do not change in configuration after data exists.** Existing leaves were split based on the original threshold. Lowering the value does not retroactively split over-full leaves, and raising it does not merge under-full ones. The result is an unbalanced tree with unpredictable split behaviour. To safely change this value on a tree with data, use [`ResizeAsync`](tree-sizing.md#resizing-an-existing-tree) which creates an offline snapshot of the tree with the new sizing and swaps the tree alias.

### `MaxInternalChildren`

The maximum number of child references an internal node holds before it splits. This controls the branching factor of the tree above the leaf level.

> **⚠️ Do not change in configuration after data exists.** The same reasoning as `MaxLeafKeys` applies — existing internal nodes were split at the original threshold and will not be rebalanced. Use [`ResizeAsync`](tree-sizing.md#resizing-an-existing-tree) to safely change this value.

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

### `PrefetchKeysScan`

When enabled, `KeysAsync` pre-fetches the next page from each shard in the background while the current page is being consumed by the k-way merge. This hides per-shard grain-call latency and can significantly reduce wall-clock time for large scans across many shards.

```csharp
// Enable globally
siloBuilder.ConfigureLattice(o => o.PrefetchKeysScan = true);
```

Pre-fetch can also be controlled per-call via the `prefetch` parameter on `KeysAsync`, which overrides the global option:

```csharp
// Override for a single call regardless of global setting
await foreach (var key in tree.KeysAsync(prefetch: true))
{
    // ...
}
```

Because each pre-fetched page is held in memory until consumed, callers that abort iteration early (e.g. `Take(n)`) pay for pages they never read. For bounded scans, leave this disabled or pass `prefetch: false` explicitly.

This option can be changed freely at any time.

### `AutoSplitEnabled`

Master switch for [adaptive shard splitting](shard-splitting.md). When `true` (the default), `HotShardMonitorGrain` periodically polls shard hotness counters and triggers splits when a shard's ops/sec exceeds `HotShardOpsPerSecondThreshold`. When `false`, no autonomic splits occur; the shard count remains fixed at `ShardCount`.

This option can be changed freely at any time. The change takes effect on the next `HotShardMonitorGrain` reminder tick.

### `HotShardOpsPerSecondThreshold`

The ops/sec threshold on a single shard that triggers an adaptive split (default: 200). Lowering this value makes the system more aggressive about splitting; raising it allows shards to absorb more load before splitting.

This option can be changed freely at any time.

### `HotShardSampleInterval`

How often `HotShardMonitorGrain` polls every shard's hotness counters (default: 30 seconds). Shorter intervals increase detection responsiveness at the cost of more grain calls.

This option can be changed freely at any time.

### `HotShardSplitCooldown`

Minimum time between consecutive splits of the same shard (default: 2 minutes). Prevents rapid re-splitting before the post-split load distribution has stabilised.

This option can be changed freely at any time.

### `MaxConcurrentAutoSplits`

Maximum number of in-flight adaptive splits per tree (default: 2). Because `HotShardMonitorGrain` is keyed per tree, this limit is enforced independently per tree in a multi-tree cluster.

This option can be changed freely at any time.

### `SplitDrainBatchSize`

Number of entries per batch during the shadow-write drain phase of an adaptive split (default: 1024). Larger batches reduce the number of drain rounds but increase per-round memory and storage I/O.

This option can be changed freely at any time.

### `AutoSplitMinTreeAge`

Minimum tree age before the hot-shard monitor begins sampling (default: 60 seconds). Prevents splits during initial bulk-load bursts that would otherwise appear as sustained hot-shard traffic.

This option can be changed freely at any time.

### `MaxScanRetries`

Maximum bounded-retry passes for `CountAsync`, `KeysAsync`, and `EntriesAsync` when the shard topology changes mid-scan (default: 3). If the topology keeps mutating after every reconciliation step, the scan throws `InvalidOperationException` rather than returning a silently incomplete result. Under the default split rate-limits (`MaxConcurrentAutoSplits = 2`, `HotShardSplitCooldown = 2 minutes`), exhausting 3 retries is not a realistic operational concern. See [Scan reliability](api.md#scan-reliability).

This option can be changed freely at any time.

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
