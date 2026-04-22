# Configuration

## Registering Lattice

Every silo must call `AddLattice` to register the grain storage provider that Lattice grains use internally:

```csharp verify
siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
```

The `name` parameter is the storage provider name (`"lattice"`). Replace `AddMemoryGrainStorage` with any Orleans storage provider (Azure Blob, Azure Table, ADO.NET, etc.) for durable storage.

## Setting Options

Lattice uses the standard .NET [named options](https://learn.microsoft.com/dotnet/core/extensions/options#named-options-support-using-iconfigurenamedoptions) pattern. Each tree resolves its options by name (the tree ID passed to `GetGrain<ILattice>(treeId)`).

### Global defaults

Use `ConfigureLattice` without a tree name to set defaults that apply to every tree unless overridden:

```csharp verify
siloBuilder.ConfigureLattice(o =>
{
    o.CacheTtl = TimeSpan.FromMilliseconds(100);
    o.TombstoneGracePeriod = TimeSpan.FromHours(6);
});
```

### Per-tree overrides

Pass a tree name to override specific options for a single tree:

```csharp verify
siloBuilder.ConfigureLattice("high-throughput-tree", o =>
{
    o.HotShardOpsPerSecondThreshold = 500;
    o.PrefetchKeysScan = true;
});

siloBuilder.ConfigureLattice("archive-tree", o =>
{
    o.TombstoneGracePeriod = Timeout.InfiniteTimeSpan; // disable compaction
});
```

Per-tree overrides are layered on top of the global defaults. Only the properties you set in the override are changed; everything else inherits from the global configuration.

> **Structural sizing is pinned per-tree in the registry, not in `LatticeOptions`.** `MaxLeafKeys`, `MaxInternalChildren`, and `ShardCount` are seeded into the `TreeRegistryEntry` on first tree use from canonical defaults in `LatticeConstants` (128 / 128 / 64) and are mutable only through [`ILattice.ResizeAsync`](tree-sizing.md#resizing-an_existing_tree) and `ILattice.ReshardAsync`. This prevents accidental divergence between the layout a tree was built with and a later configuration change. For capacity-planning guidance and per-provider limits see [Tree Storage](tree-storage.md).

> **The virtual shard space is a hard-coded constant** (`LatticeConstants.DefaultVirtualShardCount = 4096`). It is not a `LatticeOptions` property because changing it would invalidate every persisted `ShardMap` (slots are referenced by integer index). The virtual space is deliberately generous; the real ceiling on useful shard counts is scan fan-out and activation cost.

## Options Reference

| Option | Type | Default | Safe to change after data exists? |
|---|---|---|---|
| `KeysPageSize` | `int` | 512 | Yes |
| `TombstoneGracePeriod` | `TimeSpan` | 24 hours | Yes |
| `SoftDeleteDuration` | `TimeSpan` | 72 hours | Yes |
| `CacheTtl` | `TimeSpan` | `TimeSpan.Zero` (refresh on every read) | Yes |
| `PrefetchKeysScan` | `bool` | `false` | Yes |
| `PrefetchEntriesScan` | `bool` | `false` | Yes |
| `AutoSplitEnabled` | `bool` | `true` | Yes |
| `HotShardOpsPerSecondThreshold` | `int` | 200 | Yes |
| `HotShardSampleInterval` | `TimeSpan` | 30 seconds | Yes |
| `HotShardSplitCooldown` | `TimeSpan` | 2 minutes | Yes |
| `MaxConcurrentAutoSplits` | `int` | 2 | Yes |
| `MaxConcurrentMigrations` | `int` | 4 | Yes |
| `MaxConcurrentDrains` | `int` | 4 | Yes |
| `SplitDrainBatchSize` | `int` | 1024 | Yes |
| `AutoSplitMinTreeAge` | `TimeSpan` | 60 seconds | Yes |
| `MaxScanRetries` | `int` | 3 | Yes |
| `CursorIdleTtl` | `TimeSpan` | 48 hours | Yes |
| `AtomicWriteRetention` | `TimeSpan` | 48 hours | Yes |
| `VersionVectorRetention` | `TimeSpan` | `InfiniteTimeSpan` (disabled) | Yes |
| `DiagnosticsCacheTtl` | `TimeSpan` | 5 seconds | Yes |

### Structural sizing (registry-pinned)

`MaxLeafKeys`, `MaxInternalChildren`, and `ShardCount` used to live on `LatticeOptions` but are now pinned per-tree on the `TreeRegistryEntry`. They are seeded from `LatticeConstants` on first tree use (defaults 128 / 128 / 64) and can be changed through:

- `ILattice.ResizeAsync(newMaxLeafKeys, newMaxInternalChildren)` — see [Tree Sizing](tree-sizing.md#resizing-an_existing_tree). Runs online; empty-tree fast-path if no data exists.
- `ILattice.ReshardAsync(newShardCount)` — see [Online Reshard](online-reshard.md). Grow-only unless the tree is empty (fast-path).
- Pre-registering the pin explicitly before first use via `ILatticeRegistry.RegisterAsync(treeId, new TreeRegistryEntry { MaxLeafKeys = …, MaxInternalChildren = …, ShardCount = … })`.

### Virtual shard space (constant)

The virtual shard space is fixed at `LatticeConstants.DefaultVirtualShardCount = 4096` for every tree. Keys hash into `[0, 4096)` and the per-tree [`ShardMap`](tree-registry.md#shard-map) collapses ranges of virtual slots onto physical shards. This indirection decouples logical key routing from the physical shard count, enabling adaptive shard splitting without rehashing existing keys.

The pinned `ShardCount` must divide 4096 evenly for the default identity map to preserve `hash % ShardCount` routing exactly; this invariant is validated at use time by `ShardMap.CreateDefault`. The value is a compile-time constant — changing it in source would invalidate every persisted `ShardMap` and is treated as a breaking wire-format change.

### `KeysPageSize`

The number of keys returned per page during ordered key scans (`ScanKeysAsync`). Larger pages reduce the number of grain calls at the cost of larger messages. This is a performance tuning knob and does not affect tree structure.

This option can be changed freely at any time. It takes effect on the next `ScanKeysAsync` call.

### `TombstoneGracePeriod`

How long a deleted key's tombstone is retained before it becomes eligible for permanent removal by the compaction process. The grace period exists so that all cache replicas (`LeafCacheGrain` activations across silos) have time to observe the delete via delta replication before the tombstone disappears.

Set to `Timeout.InfiniteTimeSpan` to disable tombstone compaction entirely. This is useful for trees where deletes are rare or where tombstone accumulation is acceptable.

```csharp verify
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

```csharp verify
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

```csharp verify
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

When enabled, `ScanKeysAsync` pre-fetches the next page from each shard in the background while the current page is being consumed by the k-way merge. This hides per-shard grain-call latency and can significantly reduce wall-clock time for large scans across many shards.

```csharp verify
// Enable globally
siloBuilder.ConfigureLattice(o => o.PrefetchKeysScan = true);
```

Pre-fetch can also be controlled per-call via the `prefetch` parameter on `ScanKeysAsync`, which overrides the global option:

```csharp verify
// Override for a single call regardless of global setting
await foreach (var key in tree.ScanKeysAsync(prefetch: true))
{
    // ...
}
```

Because each pre-fetched page is held in memory until consumed, callers that abort iteration early (e.g. `Take(n)`) pay for pages they never read. For bounded scans, leave this disabled or pass `prefetch: false` explicitly.

This option can be changed freely at any time.

### `PrefetchEntriesScan`

When enabled, `ScanEntriesAsync` pre-fetches the next page from each shard in the background while the current page is being consumed by the k-way merge. This hides per-shard grain-call latency and can significantly reduce wall-clock time for large scans across many shards.

```csharp verify
// Enable globally
siloBuilder.ConfigureLattice(o => o.PrefetchEntriesScan = true);
```

Pre-fetch can also be controlled per-call via the `prefetch` parameter on `ScanEntriesAsync`, which overrides the global option:

```csharp verify
// Override for a single call regardless of global setting
await foreach (var entry in tree.ScanEntriesAsync(prefetch: true))
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

### `MaxConcurrentMigrations`

Maximum number of concurrent active-tombstone migrations per tree (default: 4). Helps limit the burst I/O load during bulk-deletes. Each migration drains a tombstone's shadow-write in the background.

This option can be changed freely at any time.

### `MaxConcurrentDrains`

Maximum number of concurrent shadow-write drains per tree (default: 4). Helps limit the burst I/O load during adaptive splits. Each drain transfers a split shard's data to the new location in the background.

This option can be changed freely at any time.

### `SplitDrainBatchSize`

Number of entries per batch during the shadow-write drain phase of an adaptive split (default: 1024). Larger batches reduce the number of drain rounds but increase per-round memory and storage I/O.

This option can be changed freely at any time.

### `AutoSplitMinTreeAge`

Minimum tree age before the hot-shard monitor begins sampling (default: 60 seconds). Prevents splits during initial bulk-load bursts that would otherwise appear as sustained hot-shard traffic.

This option can be changed freely at any time.

### `MaxScanRetries`

Maximum bounded-retry passes for `CountAsync`, `ScanKeysAsync`, and `ScanEntriesAsync` when the shard topology changes mid-scan (default: 3). If the topology keeps mutating after every reconciliation step, the scan throws `InvalidOperationException` rather than returning a silently incomplete result. Under the default split rate-limits (`MaxConcurrentAutoSplits = 2`, `HotShardSplitCooldown = 2 minutes`), exhausting 3 retries is not a realistic operational concern. See [Scan reliability](api.md#scan-reliability).

This option can be changed freely at any time.

### `CursorIdleTtl`

Sliding idle timeout for stateful cursors opened via `OpenKeyCursorAsync` / `OpenEntryCursorAsync` / `OpenDeleteRangeCursorAsync` (default: 48 hours). Each successful cursor step refreshes the reminder; if it fires without intervening activity the cursor grain clears its persisted state, unregisters the reminder, and deactivates. Minimum effective interval is **1 minute** (Orleans reminder granularity); smaller values are clamped to the floor. Set `Timeout.InfiniteTimeSpan` to disable automatic cleanup — cursors then live until `CloseCursorAsync` is called. See [Durable Cursors](durable-cursors.md).

This option can be changed freely at any time.

### `AtomicWriteRetention`

Retention window for completed `SetManyAtomicAsync` saga state (default: 48 hours). After a saga reaches a terminal state, its coordinator grain retains its persisted progress for this window so duplicate submissions with the same operation ID are idempotent. A retention reminder fires at the end of the window and clears the state. Set `Timeout.InfiniteTimeSpan` to disable automatic cleanup. See [Atomic Writes](atomic-writes.md).

This option can be changed freely at any time.

### `VersionVectorRetention`

How long to retain version vectors for deleted keys (default: `InfiniteTimeSpan`, disabled). When a key is deleted, its version vector is retained in the `LeafCacheGrain` for this duration to support historical scans. After the retention window, the vector is expunged from the cache.

This option can be changed freely at any time.

### `DiagnosticsCacheTtl`

How long the internal diagnostics grain caches a `TreeDiagnosticReport` before assembling a fresh sample (default: 5 seconds). `ILattice.DiagnoseAsync` is an admin-rate API; caching coalesces repeat callers (e.g. dashboards polling every few seconds) so that a single fan-out walks every shard rather than one per call.

Shallow (`deep: false`) and deep (`deep: true`) reports are cached independently. The cache is invalidated immediately when an adaptive split commits, so the next call after a topology change always returns a fresh report.

Set to `TimeSpan.Zero` to disable caching entirely — every call assembles a new report. This is useful in tests or for tight polling scenarios where staleness is unacceptable.

```csharp verify
// Disable caching for a debug tree
siloBuilder.ConfigureLattice("debug-tree", o => o.DiagnosticsCacheTtl = TimeSpan.Zero);
```

This option can be changed freely at any time. The new TTL takes effect on the next `DiagnoseAsync` call.

### `PublishEvents`

Silo-wide default for tree-event publication (default: `false`). When the effective setting resolves to `true`, the silo fires metadata-only `LatticeTreeEvent` notifications on a per-tree Orleans stream as writes, splits, compactions, snapshots, resizes, reshards, and tree-lifecycle transitions occur. Subscribers consume the stream via `LatticeExtensions.SubscribeToEventsAsync` on the cluster client. See [Events](events.md) for the event contract, delivery semantics, and provider requirements.

The effective setting is resolved per tree as follows:

1. If the tree's registry entry has a per-tree override (set via `ILattice.SetPublishEventsEnabledAsync(bool?)`), that value wins.
2. Otherwise the silo-wide `LatticeOptions.PublishEvents` is used.

This means `PublishEvents` acts as the **global default**: it applies to every tree — user trees *and* system-internal trees (those prefixed with `_lattice_`, e.g. the registry tree itself) — that has not set an override. System trees never consult the registry for their own override (that would deadlock the registry activation), so for them the silo option is always authoritative. In practice system trees produce very low event volume and are rarely of interest to subscribers; leave this at `false` unless you specifically want to observe them.

Propagation of a per-tree override is best-effort across silos: the activation that handled `SetPublishEventsEnabledAsync` observes the change immediately, while other activations refresh their cached value within a few seconds. Writes in flight at the moment of the change may emit events under the previous setting.

Publication itself is fire-and-forget: a missing stream provider, a serialization failure, or a downstream queue error is logged at `Warning` and swallowed — write-path latency and availability are unaffected by subscriber health.

This option can be changed freely at any time.

## Storage Provider Name

Lattice grains use the storage provider named `"lattice"` (exposed as `LatticeOptions.StorageProviderName`). The `AddLattice` extension method passes this name to your storage registration delegate. In advanced scenarios where you register storage directly, use this constant to ensure the provider name matches:

```csharp verify
siloBuilder.AddMemoryGrainStorage(LatticeOptions.StorageProviderName);
```

## Full Example

```csharp verify
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
        o.KeysPageSize = 1024;
        o.TombstoneGracePeriod = TimeSpan.FromHours(12);
        o.SoftDeleteDuration = TimeSpan.FromHours(72);
    });

    // Per-tree: enable prefetch for a scan-heavy tree
    silo.ConfigureLattice("events", o =>
    {
        o.PrefetchKeysScan = true;
    });

    // Per-tree: disable compaction for an append-only tree
    silo.ConfigureLattice("audit-log", o =>
    {
        o.TombstoneGracePeriod = Timeout.InfiniteTimeSpan;
    });
});
