# Configuration

> **Compression** has no core `LatticeOptions` knobs. The seam itself - the `ILatticeCompressor` contract, the registration helpers, the tag-space partitioning, and the shared-dictionary opt-in - is documented in [`compression.md`](compression.md). The per-consumer option keys live in their owning project's configuration doc: replication framing-tail compression in [Orleans.Lattice.Replication configuration](../lattice.replication/configuration.md#efficiency-bundle-dedup-and-compression), and stored WAL payload compression in [Orleans.Lattice.Storage.AzureTable configuration](../lattice.storage.azuretable/configuration.md#compression-options). The compression **algorithm and Zstd level are safe to change after data already exists**: stored payloads are self-describing and read back by their own per-row tag, so a level/algorithm change applies only to newly written data while existing rows decode unchanged.

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

> **Structural sizing is pinned per-tree in the registry, not in `LatticeOptions`.** `MaxLeafKeys`, `MaxInternalChildren`, and `ShardCount` are seeded into the `TreeRegistryEntry` on first tree use from canonical defaults in `LatticeConstants` (128 / 128 / 64) and are mutable only through [`ILattice.ResizeAsync`](tree-sizing.md#resizing-an-existing-tree) and `ILattice.ReshardAsync`. This prevents accidental divergence between the layout a tree was built with and a later configuration change. For capacity-planning guidance and per-provider limits see [Tree Storage](tree-storage.md).

> **The virtual shard space is a hard-coded constant** (`LatticeConstants.DefaultVirtualShardCount = 4096`). It is not a `LatticeOptions` property because changing it would invalidate every persisted `ShardMap` (slots are referenced by integer index). The virtual space is deliberately generous; the real ceiling on useful shard counts is scan fan-out and activation cost.

## Options Reference

| Option | Type | Default | Safe to change after data exists? |
|---|---|---|---|
| [`ActivationReadyTimeout`](#activationreadytimeout) | `TimeSpan` | 15 seconds | Yes (on next seed) |
| [`AdmissionAdvisoryBytes`](#admissionadvisorybytes) | `long?` | `null` (advisory dry-run off) | Yes |
| [`AdmissionAdvisoryLiveKeys`](#admissionadvisorylivekeys) | `long?` | `null` (advisory dry-run off) | Yes |
| [`AtomicActionRetention`](#atomicactionretention) | `TimeSpan` | 48 hours | Yes |
| [`AtomicWriteRetention`](#atomicwriteretention) | `TimeSpan` | 48 hours | Yes |
| [`AutoSplitEnabled`](#autosplitenabled) | `bool` | `true` | Yes |
| [`AutoSplitMinTreeAge`](#autosplitmintreeage) | `TimeSpan` | 60 seconds | Yes |
| [`CacheTtl`](#cachettl) | `TimeSpan` | `TimeSpan.Zero` (refresh on every read) | Yes |
| [`CompactionLeafBatchSize`](#compactionleafbatchsize) | `int` | 64 | Yes |
| [`CompactionShardTickInterval`](#compactionshardtickinterval) | `TimeSpan` | 500 milliseconds | Yes |
| [`CompactionTriggerCooldown`](tombstone-compaction.md) | `TimeSpan` | 5 minutes | Yes |
| [`CursorIdleTtl`](#cursoridlettl) | `TimeSpan` | 48 hours | Yes |
| [`DiagnosticsCacheTtl`](#diagnosticscachettl) | `TimeSpan` | 5 seconds | Yes |
| [`DigestCoalescingWindowMs`](#digestcoalescingwindowms) | `int` | 5 (measured sweet spot) | Yes |
| [`DigestPublishTimeout`](#digestpublishtimeout) | `TimeSpan` | 15 seconds | Yes (on next publish) |
| [`DirtyLeafFlushIntervalMs`](tombstone-compaction.md#dirtyleafflushintervalms) | `int` | 50 (ms) | Yes |
| [`EventStreamProviderName`](#eventstreamprovidername) | `string` | `"Default"` | Yes (on next publish) |
| [`HotShardOpsPerSecondThreshold`](#hotshardopspersecondthreshold) | `int` | 200 | Yes |
| [`HotShardSampleInterval`](#hotshardsampleinterval) | `TimeSpan` | 30 seconds | Yes |
| [`HotShardSplitCooldown`](#hotshardsplitcooldown) | `TimeSpan` | 2 minutes | Yes |
| [`KeysPageSize`](#keyspagesize) | `int` | 512 | Yes |
| [`LeafAccessModelFlushIntervalMs`](#leafcacheprewarmcount) | `int` | 30000 (ms) | Yes (on next activation) |
| [`LeafCachePreWarmCount`](#leafcacheprewarmcount) | `int` | 0 (disabled) | Yes (on next activation) |
| [`LeafProjectionRetention`](#leafprojectionretention) | `TimeSpan` | 7 days | Yes |
| [`LeafSnapshotMargin`](projection-rebuild.md) | `double` | 0.30 | Yes |
| [`LeafSnapshotReClassifyEveryNCheckpoints`](projection-rebuild.md) | `int` | 64 | Yes |
| [`MaintainProjectionDigest`](#maintainprojectiondigest) | `bool` | `true` | Yes |
| [`MaterialiserCheckpointEntries`](#materialisercheckpointentries) | `int` | 5000 | Yes |
| [`MaterialiserCheckpointInterval`](#materialisercheckpointinterval) | `TimeSpan` | 5 seconds | Yes |
| [`MaxAtomicActionArgsBytes`](#maxatomicactionargsbytes) | `int` | 32 KiB (32 768) | Yes |
| [`MaxAtomicActionSteps`](#maxatomicactionsteps) | `int` | 64 | Yes |
| [`MaxCacheValueBytes`](#maxcachevaluebytes) | `long?` | `null` (unbounded mirror) | Yes |
| [`MaxConcurrentAutoSplits`](#maxconcurrentautosplits) | `int` | 2 | Yes |
| [`MaxClusterConcurrentAutoSplits`](#maxclusterconcurrentautosplits) | `int?` | `null` (gate disabled) | Yes |
| [`MaxConcurrentDrains`](#maxconcurrentdrains) | `int` | 4 | Yes |
| [`MaxConcurrentMigrations`](#maxconcurrentmigrations) | `int` | 4 | Yes |
| [`MaxConcurrentSnapshotCaptures`](#maxconcurrentsnapshotcaptures) | `int` | 4 | Yes |
| [`MaxConcurrentStorageUsageSurfaces`](#maxconcurrentstorageusagesurfaces) | `int` | 16 | Yes |
| [`MaxConcurrentStorageUsageTrees`](#maxconcurrentstorageusagetrees) | `int` | 8 | No (cluster-wide) |
| [`MaxCursorSnapshotPinTtl`](#maxcursorsnapshotpinttl) | `TimeSpan` | 7 days | Yes |
| [`MaxEstimatedBytes`](#maxestimatedbytes) | `long?` | `null` (unbounded) | Yes |
| [`MaxKeyLength`](#maxkeylength) | `int?` | `null` (unbounded) | Yes |
| [`MaxLeafEntriesBeforeForcedCompaction`](tombstone-compaction.md) | `int` | 0 (disabled) | Yes |
| [`MaxLeafReplayEntries`](#maxleafreplayentries) | `int` | 10 000 | Yes |
| [`MaxLiveKeys`](#maxlivekeys) | `long?` | `null` (unbounded) | Yes |
| [`MaxPinnedSagaDecisions`](#maxpinnedsagadecisions) | `int` | 100 000 | Yes |
| [`MaxScanRetries`](#maxscanretries) | `int` | 3 | Yes |
| [`MaxSnapshotReplayEntries`](snapshot-cursors.md) | `long` | 10 000 000 | Yes |
| [`MaxValueSizeBytes`](#maxvaluesizebytes) | `int?` | `null` (unbounded) | Yes |
| [`MinTombstoneRatioForCompaction`](tombstone-compaction.md) | `double` | 0.0 (disabled) | Yes |
| [`PrefetchEntriesScan`](#prefetchentriesscan) | `bool` | `false` | Yes |
| [`PrefetchKeysScan`](#prefetchkeysscan) | `bool` | `false` | Yes |
| [`ProjectionRebuildPolicy`](#projectionrebuildpolicy) | enum | `SnapshotThenWal` | Yes |
| [`PublishEvents`](#publishevents) | `bool` | `false` | Yes |
| [`QueueCapacity`](queues.md) | `int?` | `null` (unbounded) | Yes |
| [`RetryPolicy`](retry-policy.md) | `ILatticeRetryPolicy?` | `null` (no retry) | Yes |
| [`ShardForwardTimeout`](#shardforwardtimeout) | `TimeSpan` | 15 seconds | Yes (on next forward) |
| [`EmptyTreeProbeBudget`](#emptytreeprobebudget) | `TimeSpan` | 10 seconds | Yes (on next reshard or resize) |
| [`ShedSnapshotOpensWhenSaturated`](#shedsnapshotopenswhensaturated) | `bool` | `true` | Yes |
| [`SnapshotBaselineTtl`](snapshot-cursors.md#baseline-ttl-leak-guard) | `TimeSpan` | 6 hours | Yes |
| [`SnapshotLeafIdleTtl`](snapshot-cursors.md) | `TimeSpan` | 30 minutes | Yes |
| [`SoftDeleteDuration`](#softdeleteduration) | `TimeSpan` | 72 hours | Yes |
| [`SplitDrainBatchSize`](#splitdrainbatchsize) | `int` | 1024 | Yes |
| [`StorageUsageCacheTtl`](#storageusagecachettl) | `TimeSpan` | 10 seconds | Yes |
| [`StorageUsagePollInterval`](#storageusagepollinterval) | `TimeSpan` | 15 seconds | No (global; read from the default options) |
| [`StorageUsageDeepPollInterval`](#storageusagedeeppollinterval) | `TimeSpan` | `TimeSpan.Zero` (disabled) | No (global; read from the default options) |
| [`StorageUsageRollupBudget`](#storageusagerollupbudget) | `TimeSpan` | 20 seconds | No (cluster-wide; read from the default options) |
| [`TombstoneGracePeriod`](#tombstonegraceperiod) | `TimeSpan` | 24 hours | Yes |
| [`TxDecisionRetention`](#txdecisionretention) | `TimeSpan` | 60 seconds | Yes |
| [`VersionVectorRetention`](#versionvectorretention) | `TimeSpan` | `InfiniteTimeSpan` (disabled) | Yes |
| [`WalAppendDispatchTimeout`](#walappenddispatchtimeout) | `TimeSpan` | 30 seconds | Yes |
| [`WalBytePressureReclaimTarget`](#walbytepressurereclaimtarget) | `double` | 0.8 | Yes |
| [`WalDrainBudget`](#waldrainbudget) | `TimeSpan` | 75 seconds | Yes |
| [`WalMaxBatchBytes`](#walmaxbatchbytes) | `long` | 4 MiB | Yes |
| [`WalMaxBatchEntries`](#walmaxbatchentries) | `int` | 100 | Yes |
| [`WalFlushPreflightTimeout`](#walflushpreflighttimeout) | `TimeSpan` | 5 seconds | Yes |
| [`WalFlushTimeout`](#walflushtimeout) | `TimeSpan` | 15 seconds | Yes |
| [`WalGcInterval`](#walgcinterval) | `TimeSpan` | 1 hour (enabled) | No (global; read from the default options) |
| [`WalMaterialiserMaxConcurrentReplays`](#walmaterialisermaxconcurrentreplays) | `int` | `0` (auto = `Environment.ProcessorCount`) | Yes |
| [`WalMaterialiserPinFlushIntervalMs`](#walmaterialiserpinflushintervalms) | `int` | 250 | Yes |
| [`WalMaterialiserPinShards`](#walmaterialiserpinshards) | `int` | 8 | No (durable-store migration; see below) |
| [`WalMaxPendingBatches`](#walmaxpendingbatches) | `int` | 16 | Yes |
| [`WalMaxRetainedBytes`](#walmaxretainedbytes) | `long?` | `null` (disabled) | Yes |
| [`WalPartitions`](#walpartitions) | `int` | 8 | No (per-tree, pinned on first WAL write) |
| [`WalRetention`](#walretention) | `TimeSpan?` | `null` (disabled) | Yes |
| [`WalReplayMaxRecordsPerTurn`](#walreplaymaxrecordsperturn) | `int` | 256 | Yes |
| [`WalSaturationDispatchTimeoutThreshold`](#walsaturationdispatchtimeoutthreshold) | `int` | 1 | Yes |
| [`WalSaturationFlushLatencySampleWindows`](#walsaturationflushlatencysamplewindows) | `int` | 3 | Yes |
| [`WalSaturationFlushLatencyThreshold`](#walsaturationflushlatencythreshold) | `TimeSpan?` | `null` (disabled) | Yes |
| [`WalSaturationMaterialiserLagSampleWindows`](#walsaturationmaterialiserlagsamplewindows) | `int` | 3 | Yes |
| [`WalSaturationMaterialiserLagThreshold`](#walsaturationmaterialiserlagthreshold) | `TimeSpan?` | 30 seconds | Yes |
| [`WalSaturationProviderFailureRateThreshold`](#walsaturationproviderfailureratethreshold) | `int` | 1 | Yes |
| [`WalSaturationRecoveryWindow`](#walsaturationrecoverywindow) | `TimeSpan` | 1 second | Yes |
| [`WalSaturationSampleInterval`](#walsaturationsampleinterval) | `TimeSpan` | 200 milliseconds | Yes |
| [`WalSaturationThrottledRatio`](#walsaturationthrottledratio) | `double` | 0.75 | Yes |
| [`WalAdmissionSaturationWaitBudget`](#waladmissionsaturationwaitbudget) | `TimeSpan` | 5 seconds | Yes |
| [`WalThrottledAdmissionPace`](#walthrottledadmissionpace) | `TimeSpan` | 25 milliseconds | Yes |
| [`WalStorageProvider`](wal-storage-providers.md) | `Func<string, IWalStorageProvider>?` | `null` (DI default) | Yes |

### Structural sizing (registry-pinned)

`MaxLeafKeys`, `MaxInternalChildren`, and `ShardCount` used to live on `LatticeOptions` but are now pinned per-tree on the `TreeRegistryEntry`. They are seeded from `LatticeConstants` on first tree use (defaults 128 / 128 / 64) and can be changed through:

- `ILattice.ResizeAsync(newMaxLeafKeys, newMaxInternalChildren)` - see [Tree Sizing](tree-sizing.md#resizing-an-existing-tree). Runs online; empty-tree fast-path if no data exists.
- `ILattice.ReshardAsync(newShardCount)` - see [Online Reshard](online-reshard.md). Grow-only unless the tree is empty (fast-path).
- Pre-registering the pin explicitly before first use via `ILatticeRegistry.RegisterAsync(treeId, new TreeRegistryEntry { MaxLeafKeys = …, MaxInternalChildren = …, ShardCount = … })`.

### Virtual shard space (constant)

The virtual shard space is fixed at `LatticeConstants.DefaultVirtualShardCount = 4096` for every tree. Keys hash into `[0, 4096)` and the per-tree [`ShardMap`](tree-registry.md#shard-map) collapses ranges of virtual slots onto physical shards. This indirection decouples logical key routing from the physical shard count, enabling adaptive shard splitting without rehashing existing keys.

The pinned `ShardCount` must divide 4096 evenly for the default identity map to preserve `hash % ShardCount` routing exactly; this invariant is validated at use time by `ShardMap.CreateDefault`. The value is a compile-time constant - changing it in source would invalidate every persisted `ShardMap` and is treated as a breaking wire-format change.

### `AdmissionAdvisoryBytes`

Optional non-enforcing advisory ceiling, in bytes, on a tree's estimated retained storage, used to right-size [`MaxEstimatedBytes`](#maxestimatedbytes) before turning enforcement on. `null` (the default) disables the byte advisory dry-run signal. When set it must be at least `1` (validated at startup). A tree over this ceiling is flagged by the `orleans.lattice.admission.over_advisory` gauge, and every write that *would* have been rejected at this ceiling increments `orleans.lattice.admission.would_reject` (dimension `bytes`) - but no write is ever rejected. Resolvable per tree.

```csharp verify
// Dry-run a 512 MiB byte ceiling: no rejections, just the would-reject signal.
siloBuilder.ConfigureLattice("bulk-ingest", o => o.AdmissionAdvisoryBytes = 512L * 1024 * 1024);
```

See [Metrics](metrics.md#per-tree-admission-control) for the advisory-first-then-enforce adoption workflow.

### `AdmissionAdvisoryLiveKeys`

Optional non-enforcing advisory ceiling on a tree's live (non-tombstone) key count, used to right-size [`MaxLiveKeys`](#maxlivekeys) before turning enforcement on. `null` (the default) disables the live-key advisory dry-run signal. When set it must be at least `1` (validated at startup). Drives the same non-rejecting `orleans.lattice.admission.over_advisory` and `orleans.lattice.admission.would_reject` (dimension `keys`) signals as [`AdmissionAdvisoryBytes`](#admissionadvisorybytes), for the key dimension. Resolvable per tree.

```csharp verify
// Dry-run a 1,000,000 live-key ceiling before enforcing it.
siloBuilder.ConfigureLattice("bulk-ingest", o => o.AdmissionAdvisoryLiveKeys = 1_000_000);
```

### `AtomicActionRetention`

Retention window for a terminal atomic-action saga's coordinator state (default: 48 hours). Once an `IAtomicActionGrain.ExecuteAsync` plan reaches a terminal outcome, the coordinator retains its persisted progress for this window so that re-issuing the same operation id returns the memoized outcome rather than re-running the plan. After the window expires the coordinator clears its state and a re-issue starts a fresh saga. Minimum effective interval is **1 minute** (Orleans reminder granularity). Set `Timeout.InfiniteTimeSpan` to retain saga state indefinitely. This is the atomic-*action* analogue of the atomic-*write* retention below and is configured independently of it. See [Atomic Actions](atomic-action.md).

### `AtomicWriteRetention`

Retention window for completed `SetManyAtomicAsync` saga state (default: 48 hours). After a saga reaches a terminal state, its coordinator grain retains its persisted progress for this window so duplicate submissions with the same operation ID are idempotent. A retention reminder fires at the end of the window and clears the state. Minimum effective interval is **1 minute** (Orleans reminder granularity); smaller non-infinite values are effectively floored at that granularity. Set `Timeout.InfiniteTimeSpan` to disable automatic cleanup. See [Atomic Writes](atomic-writes.md).

This option can be changed freely at any time.

### `AutoSplitEnabled`

Master switch for [adaptive shard splitting](shard-splitting.md). When `true` (the default), `HotShardMonitorGrain` periodically polls shard hotness counters and triggers splits when a shard's ops/sec exceeds `HotShardOpsPerSecondThreshold`. When `false`, no autonomic splits occur; the shard count remains fixed at `ShardCount`.

This option can be changed freely at any time. The change takes effect on the next `HotShardMonitorGrain` reminder tick.

### `AutoSplitMinTreeAge`

Minimum tree age before the hot-shard monitor begins sampling (default: 60 seconds). Prevents splits during initial bulk-load bursts that would otherwise appear as sustained hot-shard traffic.

This option can be changed freely at any time.

### `CacheTtl`

Minimum time between consecutive delta refreshes from the primary leaf in the `LeafCacheGrain`. When set to `TimeSpan.Zero` (the default), every read triggers a delta refresh - the version-vector comparison on the primary is cheap but the RPC overhead remains. Setting a non-zero value allows the cache to serve reads from its local dictionary without contacting the primary, trading freshness for lower read latency.

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

### `CompactionLeafBatchSize`

Maximum number of leaves the tombstone-compaction coordinator visits within a single shard before yielding for one `CompactionShardTickInterval`. Default **64 leaves**, floor **1**. The leaf walk resumes on the next timer tick from a persisted in-shard cursor, so progress survives silo crashes the same way the shard cursor does. This is the **dominant control on peak concurrent leaf activations during a pass**: with batching, peak activations are bounded by `CompactionLeafBatchSize * (CollectionAge / CompactionShardTickInterval)` regardless of tree size. The default 64 reproduces pre-batching behaviour exactly on shards with <= 64 leaves (the common case). Values below 1 are clamped to 1 with a one-shot warning per tree per process. Snapshotted at pass start; changing the option mid-pass does not reshape the in-flight pass.

```csharp verify
// Cut peak concurrent leaf activations by yielding more aggressively
// within each shard. Trades pass wall-clock for activation headroom.
siloBuilder.ConfigureLattice("activation-sensitive-tree", o => o.CompactionLeafBatchSize = 16);
```

For the relationship between `CompactionLeafBatchSize` and `CompactionShardTickInterval`, the multiplicative activation-pressure bound, and the worked-example table, see **[Tombstone Compaction - `CompactionLeafBatchSize`](tombstone-compaction.md#compactionleafbatchsize)**.

### `CompactionShardTickInterval`

Gap inserted between consecutive per-shard ticks during a tombstone-compaction pass. Default **500 milliseconds**, floor **100 milliseconds**. The cadence is a scheduler-fairness knob and the **dominant control on activation pressure during a pass** - lowering it shortens the pass but raises the peak concurrent leaf activation count. The cadence is snapshotted at the start of each pass and can be changed freely at any time; the next pass picks up the new value. The default was lowered from 2 s to 500 ms once the dirty-leaves fast path landed - on a tree with no recent deletes a pass activates only the shard root grains, so the tighter cadence is safe to ship by default.

For the worked-example trade-off table, the activation-pressure model, the relationship to `GrainCollectionOptions.CollectionAge`, and operator-triage guidance (use `ILattice.CompactShardAsync` for "compact one shard fast"), see **[Tombstone Compaction - `CompactionShardTickInterval`](tombstone-compaction.md#compactionshardtickinterval)**.

### `CursorIdleTtl`

Sliding idle timeout for stateful cursors opened via `OpenKeyCursorAsync` / `OpenEntryCursorAsync` / `OpenDeleteRangeCursorAsync` (default: 48 hours). Each successful cursor step refreshes the reminder; if it fires without intervening activity the cursor grain clears its persisted state, unregisters the reminder, and deactivates. Minimum effective interval is **1 minute** (Orleans reminder granularity); smaller values are clamped to the floor. Set `Timeout.InfiniteTimeSpan` to disable automatic cleanup - cursors then live until `CloseCursorAsync` is called. See [Durable Cursors](durable-cursors.md).

This option can be changed freely at any time.

### `DiagnosticsCacheTtl`

How long the internal diagnostics grain caches a `TreeDiagnosticReport` before assembling a fresh sample (default: 5 seconds). `ILattice.DiagnoseAsync` is an admin-rate API; caching coalesces repeat callers (e.g. dashboards polling every few seconds) so that a single fan-out walks every shard rather than one per call.

Shallow (`deep: false`) and deep (`deep: true`) reports are cached independently. The cache is invalidated immediately when an adaptive split commits, so the next call after a topology change always returns a fresh report.

Set to `TimeSpan.Zero` to disable caching entirely - every call assembles a new report. This is useful in tests or for tight polling scenarios where staleness is unacceptable.

```csharp verify
// Disable caching for a debug tree
siloBuilder.ConfigureLattice("debug-tree", o => o.DiagnosticsCacheTtl = TimeSpan.Zero);
```

This option can be changed freely at any time. The new TTL takes effect on the next `DiagnoseAsync` call.

### `DigestCoalescingWindowMs`

How long (in milliseconds) a `BPlusLeafGrain` defers a pending cross-grain projection-digest publish to its parent internal node, coalescing multiple per-mutation publishes into a single hop (default: `5` - the c2-xxviii measured sweet spot at the c2-iii operating point, a 27% drop in caller-visible `SetAsync` p50). When set to a positive value, the first dirty mutation arms a one-shot grain timer; subsequent mutations arriving within the window observe the pending timer and skip rescheduling, so N writes share one cross-grain publish to the parent. The leaf's persisted `ProjectionHash` still advances per-mutation (cold-reactivation replay invariant preserved); only the cross-grain publish to the parent is deferred.

Coalescing is scoped to the per-write hot path (`SetAsync`, `SetManyAsync`, `DeleteAsync`, `DeleteRangeAsync`); structural events (leaf split, projection rebuild, saga terminal apply, tombstone-reap compaction, CRDT merge, checkpoint flush) bypass the window and publish synchronously so operator-tooling oracles (e.g. `RebuildLeafProjectionAsync` followed by `GetLeafProjectionDigestAsync`) observe post-publish state without a settle delay.

Set to `0` to restore the synchronous-publish shape on every path - useful for tests that issue read-after-write digest oracles against a parent internal node within the same task continuation, or for operators whose downstream consumers depend on bit-exact synchronous publish timing.

```csharp verify
// Restore the synchronous-publish shape for a test tree
siloBuilder.ConfigureLattice("test-tree", o => o.DigestCoalescingWindowMs = 0);
```

This option can be changed freely at any time. The new value takes effect on the next mutation on each leaf; pending timers from the prior value drain at the prior cadence.

### `EventStreamProviderName`

Name of the Orleans stream provider Lattice publishes `LatticeTreeEvent` notifications onto (default: `"Default"`). The same name must be configured on every silo (publishers) and on the client (subscribers); register the provider via the standard `siloBuilder.AddMemoryStreams("Default")` / equivalent durable-stream extension. Only consulted when `PublishEvents` is `true`.

This option can be changed freely at any time. The new value takes effect on the next publish.

### `HotShardOpsPerSecondThreshold`

The ops/sec threshold on a single shard that triggers an adaptive split (default: 200). Lowering this value makes the system more aggressive about splitting; raising it allows shards to absorb more load before splitting.

This option can be changed freely at any time.

### `HotShardSampleInterval`

How often `HotShardMonitorGrain` polls every shard's hotness counters (default: 30 seconds). Shorter intervals increase detection responsiveness at the cost of more grain calls.

This option can be changed freely at any time.

### `HotShardSplitCooldown`

Minimum time between consecutive splits of the same shard (default: 2 minutes). Prevents rapid re-splitting before the post-split load distribution has stabilised.

This option can be changed freely at any time.

### `KeysPageSize`

The number of keys returned per page during ordered key scans (`ScanKeysAsync`). Larger pages reduce the number of grain calls at the cost of larger messages. This is a performance tuning knob and does not affect tree structure.

This option can be changed freely at any time. It takes effect on the next `ScanKeysAsync` call.

### `LeafCachePreWarmCount`

Number of `LeafCacheGrain` activations each shard root primes when
[`ILattice.WarmUpAsync`](api.md) runs (default: `0`, which disables the feature
entirely). Paired with `LeafAccessModelFlushIntervalMs`, which is the coalescing
window (in milliseconds, default `30000`) for persisting the ranking model.

`LeafCacheGrain` is a `[StatelessWorker]` read-through cache. After a silo
restart every activation is cold, so the first read of each leaf pays activation
plus a full delta pull from its primary leaf - a latency spike concentrated on
exactly the leaves a skewed workload reads most. Setting a positive count asks
each shard root to pay that cost up front, at warm-up, off the critical path of
the first real read.

The ranking is not a recency list. Each shard root keeps a bounded **histogram**
of the leaves its reads route to: every routed cache read increments the target
leaf's visit count. At warm-up the histogram is ranked by observed read
frequency - "what fraction of reads land here", rather than "what happened to be
touched last". Under a skewed or cyclic key distribution the two disagree
sharply: a leaf touched once just before shutdown outranks a genuinely hot leaf
under recency, but not under frequency. Measured on held-out synthetic traces
(train on the first half, score against the hot set of the second), frequency
recovered 96% of the true hot set on a Zipf-skewed trace and 98% on a
cyclic/sequential one, against 56% and 53% for a recency list of the same size.

A first-order Markov chain over leaf identities, ranked by personalised
PageRank, was implemented and measured first. It never beat the plain histogram
on any trace - it lost by 12.5 points on the skewed trace and 3.1 on the cyclic
one - because for a chain fitted to a single observed trajectory the empirical
visit vector is already stationary for the fitted matrix, so the ranking pass
reproduced its own input. The transition rows were removed rather than carried
at roughly 100 KB resident per shard-root activation for no ranking benefit.

Bounds, all fixed and independent of the tree's key space:

| Bound | Value | Effect |
|-------|-------|--------|
| Resident tracked leaves | 256 leaves | Coldest 25% pruned when exceeded. |
| Persisted leaves | 64 leaves | Upper bound on `LeafCachePreWarmCount`. |
| Persisted snapshot size | roughly 3 KB | Rides inside the shard root's own state. |
| Pre-warm fan-out concurrency | 8 in flight per shard | Bounded like the shard fan-out it nests inside. |

Operational characteristics:

- **Opt-in and default-off.** At `0` no access is tracked, nothing is persisted,
  and warm-up behaves exactly as it did before the option existed. Valid values
  are `0` to `64` inclusive; a value outside that range fails options validation
  at startup.
- **Zero read-path cost when disabled**, and an O(1) allocation-free record when
  enabled. The read path never awaits a storage write: a grain timer persists the
  model at most once per `LeafAccessModelFlushIntervalMs`, and only when it has
  changed. Clean deactivation flushes once more.
- **Best-effort.** A failure to prime any individual leaf is swallowed and
  counted in `orleans.lattice.warmup.leaf_cache.prewarmed`; pre-warm can never
  fail `WarmUpAsync`.
- **Correct silo locality.** The shard root is the only caller of the leaf cache,
  so the stateless-worker activations warm-up creates land on the silo that will
  serve the subsequent reads.
- **Loss bound.** An ungraceful silo kill loses at most one flush window of
  observations. A missing or stale model simply pre-warms fewer (or less useful)
  leaves - it can never produce an incorrect read.

Set `LeafAccessModelFlushIntervalMs` to `0` to persist the model only on clean
deactivation: free under read load, but the model is lost entirely on an
ungraceful kill.

See [Metrics](metrics.md) for the three instruments this feature publishes.

Both options can be changed freely at any time; they take effect on each shard
root's next activation.

### `LeafProjectionRetention`

Maximum age beyond which a cold leaf's persisted projection is treated as stale, forcing the snapshot-then-WAL recovery path on activation (default: 7 days). Defends against a leaf that has been silent long enough for the WAL to be trimmed past its persisted checkpoint without explicit detection. Set to `Timeout.InfiniteTimeSpan` to disable the age-based trigger; the offset-gap trigger (`MaxLeafReplayEntries`) and the WAL-trim trigger continue to apply.

This option can be changed freely at any time.

### `MaintainProjectionDigest`

Controls whether each leaf maintains the per-mutation XOR fold and publishes a `ChildDigestSnapshot` upward to its internal-node ancestors after every write (default: `true`).

When `true` (the default), `ILattice.GetLeafProjectionDigestAsync` returns a pre-folded `O(1)` shard aggregate that operators and chaos tests can poll to detect cross-silo drift. Each leaf mutation costs one in-memory XOR over the entry's contribution plus a `ChildDigestSnapshot` publish to the parent internal node, which in turn rewrites its `SubtreeProjectionHash` row and (if it changed) publishes upward to its own parent - the cost is `O(treeHeight)` writes per mutation.

When `false`, leaf mutations take a trimmed path: they LWW-merge the value and bump the delivery sequence but skip both the XOR fold and the upward publication. The persisted `ProjectionHash` is left untouched. `ILattice.GetLeafProjectionDigestAsync` then fast-fails with `InvalidOperationException` at the public surface rather than returning a stale aggregate. Recommended for write-amplification-sensitive deployments that rely on audit logs or external reconciliation for cross-silo state-equivalence and do not poll the digest.

```csharp verify
// Global opt-out:
siloBuilder.ConfigureLattice(opts => opts.MaintainProjectionDigest = false);

// Or per-tree:
siloBuilder.ConfigureLattice("audited-tree", opts =>
{
    opts.MaintainProjectionDigest = false;
});
```

**Disabling is a one-way operation per tree.** The first mutation that lands while maintenance is disabled stamps an irreversible registry latch (`TreeRegistryEntry.ProjectionDigestPermanentlyDisabled`) on the tree. Once the latch is set, every subsequent activation resolves `MaintainProjectionDigest` as `false` regardless of the per-tree override or the silo-wide default, and `ILattice.GetLeafProjectionDigestAsync` keeps throwing. The latch exists because the digest is an XOR-fold aggregate: any mutation accepted while maintenance was off permanently invalidates the persisted aggregate, and silently re-engaging maintenance would publish a known-stale digest as if it were authoritative. The only way to re-engage digest maintenance for a latched tree is to rebuild it (or its leaf range) from scratch under a fresh registry entry.

**System trees (those whose id begins with `_lattice_`) are always resolved as `false`** regardless of configuration, because system trees are not replicated and have no cross-silo drift-detection consumer.

**Per-tree precedence.** When a per-tree override is set on the registry entry (`TreeRegistryEntry.MaintainProjectionDigest`), it overrides the silo-wide default; the latch overrides both.

See [Projection Rebuild](projection-rebuild.md#opting-out-of-digest-maintenance) for the cost model and the WAL-storage rationale.

### `MaterialiserCheckpointEntries`

Entry-count threshold above which a pending materialiser checkpoint is force-flushed to durable storage even if `MaterialiserCheckpointInterval` has not elapsed (default: 5 000). Together with `MaterialiserCheckpointInterval` this bounds replay cost on a worst-case crash: at most `MaterialiserCheckpointEntries` mutations have to be replayed against the projection on activation.

This option can be changed freely at any time.

### `MaterialiserCheckpointInterval`

How long the leaf-projection materialiser may defer persisting an advancing checkpoint offset before flushing it to durable storage (default: 5 seconds). Combined with `MaterialiserCheckpointEntries`, this controls coalescing of materialiser-side high-water-mark writes: the checkpoint is persisted as soon as **either** threshold is met. Set to `TimeSpan.Zero` to persist on every advance (every-entry mode - strict RTO at the cost of one extra storage write per commit). Set to `Timeout.InfiniteTimeSpan` to disable time-based flushing and rely solely on the entry-count threshold.

A graceful deactivation always force-flushes a pending checkpoint, so a clean silo shutdown loses no progress regardless of interval. A worst-case crash loses up to `MaterialiserCheckpointInterval` × steady-state apply rate of replay work on restart.

```csharp verify
// Strict RTO: checkpoint on every advance.
siloBuilder.ConfigureLattice("strict-tree", o => o.MaterialiserCheckpointInterval = TimeSpan.Zero);
```

This option can be changed freely at any time.

### `MaxAtomicActionArgsBytes`

The maximum size, in bytes, of a single custom step's argument payload within an atomic-action plan (default: 32 KiB). A step whose payload exceeds the bound is rejected before the saga starts, so a wire- or storage-supplied payload cannot bloat persisted saga state without bound. Must be positive. See [Atomic Actions](atomic-action.md).

### `MaxAtomicActionSteps`

The maximum number of steps an atomic-action plan submitted to `IAtomicActionGrain.ExecuteAsync` may contain (default: 64). A plan exceeding the bound is rejected before the saga starts, so a pathological plan cannot pin an activation for an unbounded time. Must be positive. See [Atomic Actions](atomic-action.md).

### `MaxCacheValueBytes`

Optional upper bound, in bytes, on the resident **value-payload** memory a single `LeafCacheGrain` activation may hold in its read-through mirror. `null` (the default) leaves the mirror unbounded - it grows to a faithful 1:1 copy of the primary leaf's live entry set, which is the lowest-latency configuration but scales per-silo per-tree memory linearly with the touched-leaf entry count.

When set to a positive value, the cache evicts **value payloads only** (never whole rows) in least-recently-used order once the sum of resident `byte[]` payload lengths would exceed the budget. The per-row metadata envelope (timestamp, delivery-sequence position, tombstone / migration flags, expiry) is always retained, so eviction cannot violate the cursor-based delta-refresh, pending-key, moved-away, or migrated-entry contracts described in [Read Caching](caching.md#value-payload-eviction). A value read that lands on an evicted payload transparently delegates to the primary leaf for the authoritative bytes (one RPC) and is counted as a cache miss; existence checks are answered from the retained metadata with no RPC; hot keys stay resident and continue to serve from memory. Only the value payload is bounded - the retained envelope metadata (tens of bytes per row) is not counted against this budget, so plan for a small fixed overhead per live key on top of the configured budget.

```csharp verify
// Cap each cache activation at 256 MiB of resident value payloads.
siloBuilder.ConfigureLattice(o => o.MaxCacheValueBytes = 256L * 1024 * 1024);

// Per-tree: leave a low-cardinality hot tree unbounded (default) but
// bound a large cold-scan tree so a full sweep cannot pin its whole
// value set in every silo's cache.
siloBuilder.ConfigureLattice("cold-archive", o => o.MaxCacheValueBytes = 32L * 1024 * 1024);
```

Intended as deploy-time configuration; the budget is re-read on each cache refresh so a running silo honours option changes, but toggling it on a warm activation only bounds payloads merged after the change. A `null` budget preserves the original unbounded behaviour.

### `MaxConcurrentAutoSplits`

Maximum number of in-flight adaptive splits per tree (default: 2). Because `HotShardMonitorGrain` is keyed per tree, this limit is enforced independently per tree in a multi-tree cluster.

This option can be changed freely at any time.

### `MaxClusterConcurrentAutoSplits`

Optional cluster-wide ceiling on the total number of autonomic splits that may be in flight concurrently across **all** trees (default: `null` - disabled). Because `HotShardMonitorGrain` is keyed per tree, `MaxConcurrentAutoSplits` only bounds one tree's splits; in a multi-tenant or many-tree cluster the summed drain I/O from every tree splitting at once can saturate the storage provider even though no single tree exceeds its own cap. Set a positive value to opt in to a singleton admission gate that caps the aggregate concurrent split count.

The cluster ceiling is enforced **in addition to** each tree's `MaxConcurrentAutoSplits` and can only ever **lower** the number of splits a tree triggers, never raise it. When left at its `null` default the gate is entirely off the path: no cluster singleton activates and the monitor issues no extra RPC per tick, so behaviour is identical to running without the option. Admission uses a per-tree heartbeat model: each monitor re-reports its tree's authoritative in-flight split count every pass and the gate expires any footprint that stops being refreshed, so a silo that crashes mid-split has its share of the ceiling reclaimed at expiry instead of wedging splitting cluster-wide.

Per-group tuning composes naturally: low-traffic tree groups clamp their own `MaxConcurrentAutoSplits` down through named options, while a single global `MaxClusterConcurrentAutoSplits` bounds the aggregate.

```csharp verify
// Opt in to a cluster-wide ceiling of 4 concurrent autonomic splits,
// regardless of how many trees are hot at once.
siloBuilder.ConfigureLattice(o => o.MaxClusterConcurrentAutoSplits = 4);

// A low-traffic tree group additionally clamps its own per-tree cap to 1;
// all such trees still share the single global ceiling above.
siloBuilder.ConfigureLattice("cold-archive", o => o.MaxConcurrentAutoSplits = 1);
```

Watch `orleans.lattice.split.in_flight` (summed across the `tree` tag) to size the ceiling, and `orleans.lattice.split.admission.deferred` to see whether it is binding. This option can be changed freely at any time.

### `MaxConcurrentDrains`

Maximum number of concurrent shadow-write drains per tree (default: 4). Helps limit the burst I/O load during adaptive splits. Each drain transfers a split shard's data to the new location in the background.

This option can be changed freely at any time.

### `MaxConcurrentMigrations`

Maximum number of concurrent active-tombstone migrations per tree (default: 4). Helps limit the burst I/O load during bulk-deletes. Each migration drains a tombstone's shadow-write in the background.

This option can be changed freely at any time.

### `MaxConcurrentSnapshotCaptures`

Maximum number of shard roots that opening a snapshot-isolated (point-in-time) cursor may block on their per-shard baseline capture at once (default: 4). Opening such a cursor freezes a baseline on every physical shard root; each capture walks that shard's whole leaf chain and materialises its rows on the shard root's non-reentrant turn, so fanning the capture out to every shard simultaneously blocks every shard root at once - starving cross-cluster replication applies and reads queued on those same roots. Bounding the fan-out keeps all but this many shard roots free while the open proceeds in waves. Lower values reduce the per-open blast radius at the cost of a longer open; higher values open faster but block more shard roots at once. The captured baseline and its point-in-time consistency are identical under any cap - only the dispatch schedule changes. Values below 1 are clamped to 1.

This option can be changed freely at any time; a new value applies to the next snapshot-cursor open.

### `MaxConcurrentStorageUsageTrees`

Maximum number of trees a cluster-wide storage-usage roll-up samples concurrently (default: 8). Applies to `ILatticeAdmin.GetTotalStorageUsageAsync`, `ILatticeAdmin.RefreshStorageUsageAsync`, and the background poller's `ILatticeAdmin.PollWalUsageAsync`.

The roll-up is a **two-level** fan-out and the levels multiply: every tree sampled concurrently fans out again to its own shard roots and WAL partitions, bounded by [`MaxConcurrentStorageUsageSurfaces`](#maxconcurrentstorageusagesurfaces). Left unbounded, a cluster of 90 trees at the default 64 shards and 8 WAL partitions dispatches roughly `90 x (64 + 8) = 6,480` grain calls in a single burst that all race one 30 s Orleans response deadline, so the roll-up fails wholesale with response timeouts instead of merely taking longer. Bounding both levels caps the peak at `MaxConcurrentStorageUsageTrees x MaxConcurrentStorageUsageSurfaces` (128 by default) and makes the roll-up degrade in *latency* instead.

Raising it shortens a roll-up on a large, healthy cluster; lowering it further reduces the burst a roll-up imposes on silos serving live traffic. The aggregated figures are identical under any bound - only the dispatch schedule changes - and the per-tree ordering in `ClusterStorageUsageReport.Trees` follows the registry's sort order regardless. Values below 1 are clamped to 1.

This is a cluster-wide knob read from the default (unnamed) options by the admin grain that drives the roll-up; per-tree overrides do not apply, because the grain is not keyed by tree. It can be changed freely at any time; a new value applies to the next roll-up.

### `MaxConcurrentStorageUsageSurfaces`

Maximum number of per-tree storage surfaces - shard roots plus WAL partitions - that a single tree's storage-usage aggregator queries concurrently (default: 16). Applies to `ILattice.GetStorageUsageAsync` and every path that reaches it, including the cluster roll-up.

The bound spans both surface kinds **jointly**, so a tree never has more than this many usage reads outstanding regardless of how its shard count and `WalPartitions` divide. A wide tree (the default shard count is 64) would otherwise dispatch every shard-root read at once even for a single-tree report. This is the inner level of the two-level fan-out described under [`MaxConcurrentStorageUsageTrees`](#maxconcurrentstorageusagetrees).

The report is byte-for-byte identical under any bound - only the dispatch schedule changes. Values below 1 are clamped to 1.

This option can be changed freely at any time; a new value applies to the next storage-usage fan-out.

```csharp verify
// Halve the cluster-wide roll-up burst on a silo that also serves
// latency-sensitive traffic: 4 x 8 = 32 concurrent calls at peak.
siloBuilder.ConfigureLattice(o =>
{
    o.MaxConcurrentStorageUsageTrees = 4;
    o.MaxConcurrentStorageUsageSurfaces = 8;
});

// A single very wide tree can narrow its own surface fan-out further
// without changing the cluster-wide roll-up bound.
siloBuilder.ConfigureLattice("wide-archive", o => o.MaxConcurrentStorageUsageSurfaces = 4);
```

### `ShedSnapshotOpensWhenSaturated`

Whether opening a snapshot-isolated (point-in-time) cursor is shed fast with a retryable `LatticeSaturatedException` when the tree's per-silo WAL saturation signal reports `Saturated` at the moment of the open, before the per-shard baseline capture is fanned out (default: `true`). A snapshot open freezes and materialises every shard's leaf chain on the non-reentrant shard roots - heavier than a single write - so admitting one into an already-saturated tree piles that work onto roots collapsing under write back-pressure, starving replication applies and reads queued on the same roots, and a client that retries on the resulting timeout sustains a scan storm. With the option enabled the open is refused at admission: the caller receives a typed, retryable back-pressure error and the fan-out never starts. Only `Saturated` (the "pause new appends" regime) sheds; a `Throttled` tree is unaffected and stays browsable, mirroring the atomic-write saga's quiesce gate. Over the state-API / Explorer surface the refusal is mapped to gRPC `ResourceExhausted` and the Explorer surfaces a plain "this table is very busy, try again" message rather than the raw fault. Set to `false` to restore the prior behaviour where a snapshot open always proceeds regardless of the saturation regime. See [Snapshot Cursors](snapshot-cursors.md).

This option can be changed freely at any time; a new value applies to the next snapshot-cursor open.

### `MaxCursorSnapshotPinTtl`

Hard upper bound on how long the per-tree `ITxRegistryGrain` will retain the saga-decision snapshot captured by a point-in-time durable cursor (default: 7 days). A live point-in-time cursor slides this TTL on every `Next*Async`; a stalled cursor that misses the slide will eventually have its pin reaped by the registry, after which the next call surfaces `LatticeCursorSnapshotExpiredException` and the cursor must be reopened.

The cap exists so a forgotten point-in-time cursor cannot stall registry-tombstone pruning forever. Set `Timeout.InfiniteTimeSpan` to disable the registry-side cap entirely - cursor lifetime then depends solely on `CursorIdleTtl` and on `MaxPinnedSagaDecisions`. See [Durable Cursors - Point-in-time cursors](durable-cursors.md#point-in-time-cursors).

This option can be changed freely at any time.

### `MaxEstimatedBytes`

Optional enforcing cap, in bytes, on a tree's estimated retained storage (the same figure the `orleans.lattice.storage.total_bytes` gauge reports: WAL rows plus snapshot blobs plus leaf/shard-root state). `null` (the default) leaves estimated bytes unbounded; enforcement is strictly **opt-in**. When set it must be at least `1` (validated at startup). Once the tree's cached estimated-byte footprint reaches the cap, a locally-authored write is rejected with a `LatticeQuotaExceededException` carrying the `bytes` dimension.

The cap is **best-effort and approximate**: it is evaluated against a cached, eventually-consistent per-tree aggregate (the same TTL-coalesced aggregator that backs the storage-usage gauges), never a per-write fan-out, so concurrent cross-shard writes can overshoot it slightly before the aggregate refreshes, and a freshly-activated tree **fails open** (accepts writes) until its first sample lands. Replication and atomic-write-saga apply paths bypass the cap, so an incoming replicated write is never rejected. Resolvable per tree.

```csharp verify
// Cap the "bulk-ingest" tree at 1 GiB of estimated retained storage.
siloBuilder.ConfigureLattice("bulk-ingest", o => o.MaxEstimatedBytes = 1024L * 1024 * 1024);
```

Prefer the advisory-first workflow: dry-run with [`AdmissionAdvisoryBytes`](#admissionadvisorybytes) and watch `orleans.lattice.admission.would_reject` before promoting to this enforcing cap. See [Metrics](metrics.md#per-tree-admission-control). This option can be changed freely at any time; a new value takes effect on the next write.

### `MaxKeyLength`

Optional upper bound on the number of characters in a key accepted by the `ILattice` write surface - `SetAsync` (and its TTL overload), `SetIfVersionAsync`, `GetOrSetAsync`, `SetManyAsync`, and the CRDT delta-apply path (default: `null`, unbounded). When set, a write whose key is longer than this bound is rejected with an `ArgumentException` before any shard work, so a client cannot drive unbounded heap growth by writing pathologically large keys. Leaving it `null` preserves the historical unbounded behaviour; when set it must be at least `1`.

```csharp verify
siloBuilder.ConfigureLattice(o => o.MaxKeyLength = 1024);
```

This option can be changed freely at any time. It is enforced per write, so a new value takes effect on the next write.

### `MaxLeafReplayEntries`

Maximum number of WAL entries a cold leaf is permitted to replay against its projection at activation time before the leaf falls back to the snapshot-then-WAL recovery path indicated by `ProjectionRebuildPolicy` (default: 10 000). Bounds activation latency for a leaf whose persisted checkpoint has fallen far behind the WAL head; see [Projection Rebuild](projection-rebuild.md) for the full trigger set.

```csharp verify
siloBuilder.ConfigureLattice(o => o.MaxLeafReplayEntries = 100_000);
```

This option can be changed freely at any time. The new value takes effect on the next leaf activation.

### `MaxLiveKeys`

Optional enforcing cap on the number of live (non-tombstone) keys a single tree may hold. `null` (the default) leaves the live-key count unbounded; enforcement is strictly **opt-in**. When set it must be at least `1` (validated at startup). Once the tree's cached live-key count reaches the cap, a locally-authored write is rejected with a `LatticeQuotaExceededException` carrying the `keys` dimension.

Shares the **best-effort / approximate**, fail-open, replication-bypassing semantics of [`MaxEstimatedBytes`](#maxestimatedbytes): the cap is compared against the cached, eventually-consistent per-tree aggregate (never a per-write fan-out), so concurrent cross-shard writes can overshoot it slightly, a freshly-activated tree accepts writes until its first sample lands, and replicated / saga-applied writes are never rejected. A time-expired entry that compaction has not yet reaped counts as live until the next deep re-anchor, so the cap can bite slightly early - never late. Resolvable per tree.

```csharp verify
// Cap the "sessions" tree at 5,000,000 live keys.
siloBuilder.ConfigureLattice("sessions", o => o.MaxLiveKeys = 5_000_000);
```

Prefer the advisory-first workflow: dry-run with [`AdmissionAdvisoryLiveKeys`](#admissionadvisorylivekeys) and watch `orleans.lattice.admission.would_reject` before promoting to this enforcing cap. See [Metrics](metrics.md#per-tree-admission-control). This option can be changed freely at any time; a new value takes effect on the next write.

### `MaxPinnedSagaDecisions`

Registry-wide footprint cap on the number of saga decisions that may be pinned across all live point-in-time cursors on a single tree (default: 100 000). `OpenKeyCursorAsync` / `OpenEntryCursorAsync` opened with `pointInTime: true` consult the registry: if accepting the new snapshot would push the pinned-decision count past this cap, the open call throws `LatticeCursorRegistryPinExhaustedException` and no pin is installed. Existing pinned cursors continue paging.

Sized for a tree carrying a steady-state in-flight-saga set in the low thousands plus a handful of overlapping long-running point-in-time cursors. Raise if a workload routinely opens many concurrent multi-day point-in-time cursors against a saga-heavy tree; lower if a single tree must keep registry footprint tightly bounded.

This option can be changed freely at any time.

### `MaxScanRetries`

Maximum bounded-retry passes for `CountAsync`, `ScanKeysAsync`, and `ScanEntriesAsync` when the shard topology changes mid-scan (default: 3). If the topology keeps mutating after every reconciliation step, the scan throws `InvalidOperationException` rather than returning a silently incomplete result. Under the default split rate-limits (`MaxConcurrentAutoSplits = 2`, `HotShardSplitCooldown = 2 minutes`), exhausting 3 retries is not a realistic operational concern. See [Scan reliability](api.md#scan-reliability).

This option can be changed freely at any time.

### `MaxValueSizeBytes`

Optional upper bound, in bytes, on the size of a value (or CRDT delta) accepted by the `ILattice` write surface - `SetAsync` (and its TTL overload), `SetIfVersionAsync`, `GetOrSetAsync`, `SetManyAsync`, and the CRDT delta-apply path (default: `null`, unbounded). When set, a write whose value exceeds this many bytes is rejected with an `ArgumentException` before any shard work, so a client cannot drive unbounded heap growth by writing pathologically large values. Leaving it `null` preserves the historical unbounded behaviour; when set it must be at least `1`.

```csharp verify
siloBuilder.ConfigureLattice(o => o.MaxValueSizeBytes = 1024 * 1024);
```

This option can be changed freely at any time. It is enforced per write, so a new value takes effect on the next write.

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

### `ProjectionRebuildPolicy`

Selects the recovery strategy a leaf grain takes when one of the fall-off-log triggers fires (default: `SnapshotThenWal`):

| Value | Behaviour |
|---|---|
| `SnapshotThenWal` | Drains the per-leaf snapshot, persists the snapshot offset as the new checkpoint, then tail-replays the remaining WAL slice. Reliable: works even when the WAL has been trimmed below the leaf's previous checkpoint. |
| `FullRebuildFromWal` | Replays from the absolute tail of the WAL. Fails fast with `LeafProjectionStaleException` if the WAL has been trimmed and a complete history is unavailable. Diagnostic. |
| `Fail` | Surfaces `LeafProjectionStaleException` at activation and waits for an operator-driven rebuild. |

This option can be changed freely at any time.

### `PublishEvents`

When `true`, Lattice publishes `LatticeTreeEvent` notifications on the Orleans stream namespace `orleans.lattice.events` covering per-key writes, atomic-write completions, splits, compactions, snapshots, resizes, reshards, and tree-lifecycle transitions (default: `false`, opt-in per tree). Consumers subscribe via `LatticeExtensions.SubscribeToEventsAsync`. Publication is fire-and-forget and log-and-swallow, so a missing or misconfigured stream provider never breaks the write path. Per-tree overrides applied via `ILattice.SetPublishEventsEnabledAsync` are persisted on the tree's registry entry and override the silo-wide default. See [Events](events.md).

This option can be changed freely at any time. Per-tree overrides take effect on the publishing activation immediately; other activations refresh within a few seconds.

### `ShardForwardTimeout`

Hard ceiling on how long a single outbound shard-to-shard write forward may run before it is cancelled and surfaced to callers as a `TimeoutException` (default: 15 seconds). It bounds both the online-resize shadow forward and the adaptive-split migration forward.

During a reshard swap the destination shard's ownership is changing, and Orleans can reject the outbound forward message and leave the caller-side `await` neither completing nor faulting. Without a ceiling the forwarding turn never returns, the lattice grain's per-shard fan-out saturates at its in-flight limit, and the whole write pipeline wedges with no fault and no activation recycle. With the ceiling the parked forward is abandoned and the turn faults cleanly with a `TimeoutException`, which the existing transient-exception retry envelope on every mutation path catches and re-runs against refreshed routing once the swap has settled. Abandoning a forward never loses data: convergence on the destination shard is independently guaranteed by last-writer-wins plus the split coordinator's authoritative leaf-chain drain (the Drain phase and the Complete-phase final drain).

Set to `InfiniteTimeSpan` to disable the ceiling and restore the historical unbounded-await behaviour; the options validator rejects any other non-positive value.

### `EmptyTreeProbeBudget`

Ceiling on the emptiness probe that reshard and resize initiation run before taking their empty-tree fast paths (default: 10 seconds).

`ReshardAsync` and `ResizeAsync` each need only a boolean - "does this tree hold any live key?" - to decide whether they can repin registry state directly instead of starting a migration coordinator. Both used to answer it with `CountAsync`, a strongly-consistent whole-tree fan-out that walks every leaf chain, then discards its result and retries whenever the shard map moves under it, giving up only once [`MaxScanRetries`](#maxscanretries) is exhausted. Initiation is exactly when that map is most likely to be churning, so an unbounded exact count could consume the caller's whole response budget and time the operation out before it had started.

Both now probe existence directly, OR-ing a short-circuiting per-shard check that stops at the first non-empty leaf. That needs no reconciliation against a moving shard map: a count must reconcile because a key migrating between shards is briefly visible on both the source and the destination, which double-counts, whereas a split only ever *moves* keys - never creating, destroying, or leaving one present on neither side - so a key that exists is seen by at least one shard wherever the split has got to, and seeing it twice still just means "a key exists".

This budget is the remaining backstop for a probe that parks rather than returns. The answer is deliberately one-sided: it may report non-empty while the last keys migrate away, but never empty while a key exists anywhere, and only "empty" unlocks a fast path - so the one consequential direction cannot be wrong. Every inconclusive outcome (the budget elapsing, or a shard faulting) is reported as "not empty", so initiation simply proceeds down the normal coordinator path.

Set to `InfiniteTimeSpan` to wait indefinitely; the options validator rejects any other non-positive value.

This option can be changed freely at any time. The new value takes effect on the next reshard or resize.

### `ActivationReadyTimeout`

Hard ceiling on how long a `ShardRootGrain`'s one-time activation-readiness seed may run before it is abandoned and surfaced to the preparing turn as a `TimeoutException` (default: 15 seconds). The seed is the chain of cross-grain awaits a brand-new or freshly-reactivated shard runs the first time it prepares for an operation: the defensive `state.ReadStateAsync` re-read, the tree-registry `RegisterAsync`, the deterministic root-leaf init pair, and the initial shard-state write.

This seed runs while the shard holds its non-reentrant activation gate. During a startup reshard or a membership change Orleans can reject or park one of those messages (the target registry or leaf activation is not yet visible) and leave the caller-side `await` neither completing nor faulting. Without a ceiling the parked seed pins the gate, every interleaved read/write turn on the activation stalls behind it, the lattice grain's per-shard fan-out saturates at its in-flight limit, and the whole write pipeline wedges with no fault and no activation recycle until the caller-side Orleans response deadline (default 3 minutes) expires. With the ceiling the parked seed is abandoned and the turn faults cleanly with a `TimeoutException`, which the existing transient-exception retry envelope on every mutation path catches and re-runs against refreshed routing or registration once the startup reshard has settled. Abandoning a parked seed never loses data or double-registers: every cross-grain step is idempotent on retry, and a failed shard-state write reverts the in-memory seed so the retry re-runs cleanly.

Set to `InfiniteTimeSpan` to disable the ceiling and restore the historical unbounded-await behaviour; the options validator rejects any other non-positive value.

When the deadline fires, the seed throws `ShardActivationTimeoutException` (publicly visible, derived from `TimeoutException`). The exception is retriable by construction - every cross-grain step in the seed is idempotent on retry - and every public `ILattice` operator that drives the seed transparently absorbs up to two consecutive occurrences before propagating to the caller, so external code generally does not need to special-case the cold-start race. Coverage spans the per-key read / write surface (via the central stale-routing envelope), multi-shard fan-outs (with per-shard wraps so a single shard's seed-timeout retries only that shard, not every sibling), per-tree coordinator entry points (resize / reshard / snapshot / merge / delete / recover / purge / bulk-load / compaction / projection-rebuild), the saga path (`SetManyAtomicAsync`), the digest path (`GetLeafProjectionDigestAsync`), and the per-shard warmup probe. Callers that want to detect or instrument the absorbed retries explicitly can catch the typed exception (it carries `TreeId`, `ShardIndex`, and `TimeoutSeconds` slots for per-occurrence attribution).

This option can be changed freely at any time. The new value takes effect on the next seed.

### `DigestPublishTimeout`

Hard ceiling on how long a single internal-node upward digest publish may run before it is abandoned and surfaced to the holding turn as a `TimeoutException` (default: 15 seconds). It bounds the `ChildDigestSnapshot` propagation that a `BPlusInternalGrain` issues to its parent after folding a child's digest.

The publish is a cross-grain RPC awaited while the internal node holds its non-reentrant split gate, and it recurses up the internal-node chain toward the shard root. A parent that is itself mid-mutation can leave the await neither completing nor faulting, pinning the gate on that activation with no ceiling so every subsequent mutating turn back-pressures behind it. With the ceiling the parked publish is abandoned and the turn faults with a `TimeoutException`, releasing the gate. Abandoning a publish never drifts the digest count: the publish never partially applied at the parent, the digest is staleness-tolerant, and the next mutation's dirty-flag publish re-drives convergence. A non-zero `orleans.lattice.internal.digest_publish.timeouts` counter surfaces the condition.

Set to `InfiniteTimeSpan` to disable the ceiling and restore the historical unbounded-await behaviour; the options validator rejects any other non-positive value.

This option can be changed freely at any time. The new value takes effect on the next publish.

### `SoftDeleteDuration`

How long a soft-deleted tree's data is retained in storage before being permanently purged. During this window the tree is inaccessible - all reads and writes throw `InvalidOperationException` - but its grain state still exists in the storage provider. After the duration elapses, a grain reminder triggers a full purge that walks every shard, clears all leaf and internal node state, and deactivates each grain.

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

### `SplitDrainBatchSize`

Number of entries per batch during the shadow-write drain phase of an adaptive split (default: 1024). Larger batches reduce the number of drain rounds but increase per-round memory and storage I/O.

This option can be changed freely at any time.

### `StorageUsageCacheTtl`

Cache lifetime for `ILattice.GetStorageUsageAsync` reports (default: 10 seconds). The per-tree storage-usage aggregator fans out across the tree's shards and WAL partitions to assemble a byte-accurate `TreeStorageUsageReport`; this TTL coalesces repeat callers (dashboard scrapes, the background poller, and direct API calls) so a single fan-out serves a whole window. Set to `TimeSpan.Zero` to disable caching - every call fans out fresh. See [Tree Storage](tree-storage.md#runtime-measurement).

```csharp verify
// Hold storage reports for 30 s to cut dashboard fan-out cost
siloBuilder.ConfigureLattice("metrics-tree", o => o.StorageUsageCacheTtl = TimeSpan.FromSeconds(30));
```

This option can be changed freely at any time.

### `StorageUsagePollInterval`

Cadence at which every silo's background storage-usage poller calls `ILatticeAdmin.PollWalUsageAsync` so the WAL-bytes and over-threshold storage gauges (`lattice.storage.wal_bytes`, `lattice.storage.policy.over_threshold`) populate automatically, without any caller having to invoke `ILattice.GetStorageUsageAsync`. Default 15 seconds. The poll path is **leaf-free**: it activates only WAL partition grains, so idle trees stay cold. The poll fans out to every registered tree's aggregator; because each aggregator is a single cluster-wide activation, its publish lands on its own host silo's metrics sink, so a tree contributes its series on exactly one silo and a cross-silo `sum by (tree)` counts it once. Running the poller on every silo is intentional - it needs no leader election, and the aggregator's `StorageUsageCacheTtl` coalesces redundant polls from sibling silos. When a tree's aggregator migrates to another silo, the old silo stops refreshing that series and it expires from its sink after a staleness horizon (four poll intervals, floored at 60 seconds), so the tree never double-counts across scrape targets. The snapshot-bytes, leaf-state-bytes, and total-bytes gauges are **not** refreshed by this poll; see [`StorageUsageDeepPollInterval`](#storageusagedeeppollinterval).

This is a **global** knob read from the default (unnamed) options; per-tree overrides do not apply. Set to `TimeSpan.Zero` or a negative value to disable the poller - the gauges then populate only when the public storage-usage API is called.

```csharp verify
// Poll every 5 s for tighter dashboard freshness
siloBuilder.ConfigureLattice(o => o.StorageUsagePollInterval = TimeSpan.FromSeconds(5));

// Disable the poller (gauges populate only on explicit API calls)
siloBuilder.ConfigureLattice(o => o.StorageUsagePollInterval = TimeSpan.Zero);
```

This option is read once when the poller starts on each silo.

### `StorageUsageDeepPollInterval`

Optional cadence at which the same background poller *also* drives the **deep** storage gauges - `lattice.storage.snapshot_bytes`, `lattice.storage.leaf_state_bytes`, and `lattice.storage.total_bytes` - by calling the non-force `ILatticeAdmin.GetTotalStorageUsageAsync`. The faster [`StorageUsagePollInterval`](#storageusagepollinterval) poll refreshes only the WAL-bytes surface (it touches only WAL partition grains); this deep poll additionally reads each shard root's incrementally-maintained byte totals. That read is **O(1) per shard root** - it never walks the leaf chain or activates per-leaf snapshot grains - so it activates only the shard roots and never pins idle leaves resident. It never invokes the operator-only force-refresh (`ILatticeAdmin.RefreshStorageUsageAsync`) that re-walks every leaf.

Defaults to `TimeSpan.Zero`, which **disables** the deep poll: the snapshot / leaf-state / total-bytes gauges then populate only on demand via `ILattice.GetStorageUsageAsync` or the operator-driven `ILatticeAdmin.RefreshStorageUsageAsync`. Set a positive value - typically a small multiple of `StorageUsagePollInterval` - to keep the deep gauges live on a dashboard. A value at or below `TimeSpan.Zero` disables it. Like `StorageUsagePollInterval`, this is a **global** knob read from the default (unnamed) options; per-tree overrides do not apply. The sink's staleness horizon is sized off the slower of the two cadences, so a deep series survives a few missed deep polls before expiring after a real migration.

```csharp verify
// Refresh the deep storage gauges once a minute (WAL bytes still refresh
// on the faster StorageUsagePollInterval cadence).
siloBuilder.ConfigureLattice(o => o.StorageUsageDeepPollInterval = TimeSpan.FromSeconds(60));

// Leave disabled (default): deep gauges populate only on explicit API calls.
siloBuilder.ConfigureLattice(o => o.StorageUsageDeepPollInterval = TimeSpan.Zero);
```

This option is read once when the poller starts on each silo.

### `StorageUsageRollupBudget`

Wall-clock budget a cluster-wide storage-usage roll-up may spend sampling trees before it stops dispatching and returns what it has (default: 20 seconds). Applies to `ILatticeAdmin.GetTotalStorageUsageAsync` and `ILatticeAdmin.RefreshStorageUsageAsync`.

Bounding the fan-out with [`MaxConcurrentStorageUsageTrees`](#maxconcurrentstorageusagetrees) and [`MaxConcurrentStorageUsageSurfaces`](#maxconcurrentstorageusagesurfaces) caps the *burst* a roll-up imposes, but it cannot cap the *total* work: a deep refresh re-walks every shard of every tree, so a large enough catalogue cannot be sampled inside one Orleans response deadline however gently it is dispatched. Without a budget the whole call then fails on the deadline and the caller learns nothing at all. With one, the trees sampled so far report real figures, the remainder report as not-answered, and `ClusterStorageUsageReport.Partial` is set - the same "an honest flagged lower bound beats a silently wrong or absent answer" rule the per-surface reporting follows.

Set it comfortably below the response deadline of the transport carrying the call, so the truncated report can still be returned. A non-positive value **disables** the budget, restoring run-to-completion behaviour.

This is a cluster-wide knob read from the default (unnamed) options by the admin grain that drives the roll-up; per-tree overrides do not apply, because that grain is not keyed by tree.

```csharp verify
// Allow a larger catalogue longer to sample before the roll-up truncates.
siloBuilder.ConfigureLattice(o => o.StorageUsageRollupBudget = TimeSpan.FromSeconds(45));

// Disable the budget: sample every tree however long it takes.
siloBuilder.ConfigureLattice(o => o.StorageUsageRollupBudget = TimeSpan.Zero);
```

This option can be changed freely at any time; a new value applies to the next roll-up.
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

### `TxDecisionRetention`

Retention window for a completed saga's commit/abort decision in the per-tree `ITxRegistryGrain` after the saga calls `ForgetAsync` (default: 60 seconds). The registry stamps a `ForgottenAt` tombstone instead of evicting the decision; for the duration of the window `GetStatusAsync` / `GetStatusManyAsync` / `SnapshotAsync` continue to surface the decision so that a process which installs a *new* pending bucket on that txid *after* the saga's terminal fan-out can still resolve the verdict and apply the terminal directly.

The primary race the window guards is the retroactive shadow-forward sweep at the start of an adaptive shard split: the split coordinator replays every in-flight prepared mutation from the source leaves into the destination shard's `_pendingTx` buckets, and its post-sweep cleanup pass resolves any orphan bucket whose terminal has already broadcast by reading the retained verdict. Without retention, a saga that completed microseconds before the sweep installed its pending bucket would leave a destination-shard orphan with no recoverable outcome.

Tombstones are physically purged on the next `ForgetAsync` / `MarkCommittedAsync` / `MarkAbortedAsync` call against the registry (inline `PruneExpired` pass). Set `TimeSpan.Zero` to restore the pre-tombstone immediate-evict semantic (legacy behaviour; reintroduces the orphan risk - reserved for tests or trees with `AutoSplitEnabled = false`). Increase beyond 60 s only if your operational profile produces sweep durations longer than that (very large shards under sustained write load, cascading split storms).

This option can be changed freely at any time.

### `VersionVectorRetention`

How long to retain version vectors for deleted keys (default: `InfiniteTimeSpan`, disabled). When a key is deleted, its version vector is retained in the `LeafCacheGrain` for this duration to support historical scans. After the retention window, the vector is expunged from the cache.

There is an advisory lower bound, `LatticeOptions.DefaultMinVersionVectorRetention` (1 hour). It is **not enforced** - it is provided only as a reference constant - but a finite `VersionVectorRetention` below it is typically unsafe on networks where clock skew exceeds the window, because pruning may then drop entries that are still causally relevant (a short-retention replica keeps reinstating entries from a long-retention peer).

This option can be changed freely at any time.

### `WalBytePressureReclaimTarget`

Low-water fraction of `WalMaxRetainedBytes` that disarms the advisory byte-pressure policy (default: `0.8`), providing hysteresis so a tree hovering near the ceiling is not trimmed on every GC pass. The policy *arms* when retained WAL crosses the full ceiling (high-water) and re-triggers a byte-pressure trim on each pass until a trim drives retained bytes at or below `WalMaxRetainedBytes * WalBytePressureReclaimTarget` (low-water), at which point it disarms. While disarmed, growth that stays inside the `(low-water, ceiling]` band does not re-trigger. The value is clamped to the interval `(0, 1]`. Ignored when `WalMaxRetainedBytes` is `null`. See [WAL](wal.md) and [Tree Storage](tree-storage.md).

This option can be changed freely at any time. The new value takes effect on the next GC tick.

### `WalMaxBatchBytes`

Maximum byte budget the WAL partition grain accumulates into a single storage flush (default: **4 MiB**, `4L * 1024 * 1024`). Whichever of `WalMaxBatchBytes` or `WalMaxBatchEntries` is reached first triggers the flush. Measured against the *exact* serialised size of each `WalRecord` under the WAL grain's wire format - the per-entry encoder walks every field through the same Orleans-binary codec the storage provider sees, and the bytes it produces are handed straight to `IWalStorageProvider.AppendEncodedBatchAsync`, so the grain pays exactly one encode per append and the budget is an exact ceiling suitable for sizing against the Azure Table Storage 4 MB transactional-batch limit (which has zero tolerance for under-counts).

This option can be changed freely at any time. The new value takes effect on the next batch boundary.

### `WalMaxBatchEntries`

Maximum number of WAL entries the partition grain coalesces into a single storage flush (default: 100). Lower values reduce per-entry flush latency at the cost of throughput. Whichever of `WalMaxBatchEntries` or `WalMaxBatchBytes` is reached first triggers the flush.

This option can be changed freely at any time. The new value takes effect on the next batch boundary.

### `WalFlushTimeout`

Hard ceiling on how long a single per-shard WAL flush may run before it is cancelled and surfaced to callers as a `TimeoutException` (default: 15 seconds). The ceiling covers both the storage-provider append and the post-failure tail resync.

Bounding the flush is what keeps a provider call that hangs indefinitely - for example against a partition left half-activated by a placement/reshard race - from pinning its in-flight slot forever. Without a ceiling the hung slot is never removed from the in-flight chain, the chain saturates at `WalMaxPendingBatches`, and every subsequent append back-pressures behind a flush that can never settle (a steady-state stall with no fault and no activation recycle). With the ceiling the hung flush faults cleanly, the existing failure handler resynchronises the dense-offset tail from the provider, drains the chain, and callers retry.

The default of 15 seconds sits above the Azure Tables SDK's worst-case legitimate retry envelope under sustained throttling (~10 seconds: three exponential backoffs plus the call times), so a healthy flush never trips it, yet well below the SDK's per-try network timeout so a true hang is still caught and the wedged shard self-heals promptly. Set to `InfiniteTimeSpan` to disable the ceiling and restore the historical unbounded-await behaviour; the options validator rejects any other non-positive value.

This option can be changed freely at any time. The new value takes effect on the next flush.

### `WalAppendDispatchTimeout`

Hard ceiling on how long the per-tree WAL writer (`WalCommitLogWriter`) will wait on a single outbound `IWalShardGrain.AppendBatchAsync` / `AppendAsync` dispatch before abandoning the await and surfacing a `TimeoutException` to the caller (default: 30 seconds).

The dispatch is the writer-side cross-grain RPC into the per-shard WAL grain - it is the outermost observable seam on the write pipeline and was historically unbounded on the writer side, so a wedged shard activation would hold every caller's dispatch parked until the Orleans response deadline (default 3 minutes) expired (a 180-second blind hang with no per-shard attribution). Bounding the dispatch converts that blind hang into a structured fault with per-shard counter attribution (the `orleans.lattice.wal.append_dispatch.timeouts` counter, tagged `tree` and `shard`), so a wedged shard surfaces immediately and the request pipeline releases its slot rather than back-filling behind the wedge.

This option does **not** fix the wedge mechanism itself - the grain-side flush / activation deadlines already bound their own regions - it bounds the symptom on the writer side and makes every wedge attributable to a specific `(tree, shard)` in O(timeout) instead of O(response timeout) time.

The default of 30 seconds sits above the legitimate envelope of a fully-saturated dispatch (one healthy flush + headroom), yet well below the Orleans response timeout so a true park is caught and surfaced promptly. Set to `InfiniteTimeSpan` to disable the ceiling and restore the historical unbounded-await behaviour; the options validator rejects any other non-positive value.

This option can be changed freely at any time. The new value takes effect on the next dispatch.

### `WalFlushPreflightTimeout`

Hard ceiling on how long a per-shard WAL `FlushAsync` may spend in its preflight region (the synchronous setup and initial scheduler yield that precede the bounded provider call) before the flush is abandoned and the slot drains (default: 5 seconds).

The preflight region is normally microseconds, but if the activation's grain scheduler never resumes the post-yield continuation (e.g. a startup reshard / membership change parked the activation, a non-cooperative work item is hogging the scheduler, or the activation is being torn down mid-flush) the slot sits in `_inFlight` with no deadline armed - the existing `WalFlushTimeout` only covers the provider call, which has not yet been issued - and the chain saturates at `WalMaxPendingBatches` with no fault and no activation recycle. With the ceiling the parked preflight faults cleanly as a `TimeoutException` routed through the normal failure handler, the slot drains, and the `orleans.lattice.wal.flush.preflight.timeouts` counter (tagged `tree` and `shard`) attributes the trip to the affected partition.

The default of 5 seconds is orders of magnitude above the legitimate microsecond envelope, yet small enough that a genuinely stalled scheduler is caught before the writer-side dispatch deadline (`WalAppendDispatchTimeout`) trips. Set to `InfiniteTimeSpan` to disable the ceiling and restore the historical unbounded-await behaviour; the options validator rejects any other non-positive value.

This option can be changed freely at any time. The new value takes effect on the next flush.

### `WalDrainBudget`

Hard ceiling on how long a per-shard WAL grain's `OnDeactivateAsync` drain may run before the remaining in-flight slots are force-faulted and the chain is released so the activation can finish tearing down (default: 75 seconds = `5 * WalFlushTimeout`). Bounds the host-level SIGTERM drain so the silo's shutdown accounting (the benchmark host's `FINAL` line, an `IHostApplicationLifetime.ApplicationStopping` cancellation source) always settles within bounded time of the SIGTERM, regardless of whether the underlying storage provider is healthy.

Defends against the saturating-storage-account wedge: when the provider call's await is parked behind an SDK retry loop in pre-attempt back-off, the existing per-flush `WalFlushTimeout` may not fire promptly (the SDK observes cancellation only between attempts, not during back-off), so a chain with N in-flight slots can hold the deactivation indefinitely. With this budget the drain:

1. Signals every in-flight flush's cancellation token at entry (the per-activation drain `CancellationTokenSource` is linked into each per-flush deadline at flush construction, so a single `Cancel()` cancels every in-flight provider call in one shot);
2. Awaits the chain to settle naturally for up to `WalDrainBudget`;
3. Force-faults any slot that has not unlinked when the budget expires, with a typed `TimeoutException` faulted onto every parked ack `TaskCompletionSource` so callers parked on `AppendAsync` / `AppendBatchAsync` are released rather than parking through the rest of host shutdown.

The `orleans.lattice.wal.shard.drain.budget.expirations` counter and `orleans.lattice.wal.shard.drain.budget.force_faulted_slots` histogram (both tagged `tree` and `shard`) attribute every budget-driven force-fault per partition. A zero counter on a healthy drain; any non-zero rate identifies a shard whose provider call could not be cancelled inside the budget.

The default of 75 seconds is `5 * WalFlushTimeout` - sized so a healthy chain with cap = 16 in-flight flushes has time to drain naturally (each flush is itself bounded by `WalFlushTimeout`) while a wedged chain still surfaces within a bounded window of the SIGTERM. Set to `InfiniteTimeSpan` to disable the ceiling and restore the historical unbounded-drain behaviour; the options validator rejects any other non-positive value.

This option can be changed freely at any time. The new value takes effect on the next deactivation.

### `WalSaturationSampleInterval`

Cadence at which the silo-scoped sampler that backs `IWalSaturationSignal` and `IWalSaturationObserver` recomputes the per-tree saturation state from the writer-side admission gate and the recent dispatch-timeout-trip rate (default: 200 ms). A shorter interval lowers the worst-case transition latency observers see (the bound is one sample interval beyond the underlying signal crossing the threshold) at the cost of slightly more timer-driven sampler work. The 200 ms default keeps subscribers well within the one-second transition-latency promise on the public surface while keeping the sampler at a negligible CPU footprint on an idle silo. Set to `InfiniteTimeSpan` to disable the sampler entirely - every tree's signal stays `Healthy` forever, the observable gauge reports `0` for any tree it has observed, and `IWalSaturationObserver` callbacks never fire. The options validator rejects any other non-positive value. See [WAL Saturation Signal](wal-saturation-signal.md).

This option can be changed freely at any time. The new value takes effect on the next sampler tick.

### `WalSaturationThrottledRatio`

Per-partition admission-depth ratio (in `[0.0, 1.0]`) at or above which the saturation signal raises a tree to `WalSaturationState.Throttled` (default: 0.75). Computed as `in_flight / WalMaxPendingBatches` on each partition; the tree's state is the worst case across its partitions. Below the ratio the tree stays `Healthy`; at or above the ratio it advances to `Throttled`; at the cap with a non-empty wait queue (or when the dispatch-timeout rate crosses `WalSaturationDispatchTimeoutThreshold`) it advances to `Saturated`. The 0.75 default leaves a 25%-of-cap headroom for callers to slow down before the cap pins. Must be in the inclusive range `[0.0, 1.0]`; `NaN` is rejected.

This option can be changed freely at any time. The new value takes effect on the next sampler tick.

### `WalSaturationDispatchTimeoutThreshold`

Minimum number of `orleans.lattice.wal.append_dispatch.timeouts` trips observed within a single `WalSaturationSampleInterval` window that raises a tree to `WalSaturationState.Saturated` regardless of admission-semaphore depth (default: 1). Captures the dispatch-deadline failure-tail of the saturation regime (parked dispatches abandoned because a downstream shard wedged) in addition to the admission-depth fast signal. Raise it on dashboards where occasional single trips are expected without operator concern; the value is per-window, so `WalSaturationDispatchTimeoutThreshold = 3` with a 200 ms sample interval permits up to 14 trips/second steady-state before flagging Saturated. Must be greater than or equal to 1.

This option can be changed freely at any time. The new value takes effect on the next sampler tick.

### `WalSaturationProviderFailureRateThreshold`

Minimum number of provider-side commit failures (any exception surfaced from a downstream `IWalShardGrain.AppendAsync` / `AppendBatchAsync` dispatch other than the writer-side `TimeoutException` already captured by `WalSaturationDispatchTimeoutThreshold`) observed within a single `WalSaturationSampleInterval` window that raises a tree to `WalSaturationState.Saturated` regardless of admission-semaphore depth and dispatch-timeout trips (default: 1). Captures the third saturation regime the writer side cannot otherwise surface: a downstream storage provider whose commit calls return quickly (so neither the admission depth nor the dispatch deadline crosses the threshold) but terminally fail at a high rate, e.g. an Azure Tables single-account 409-Conflict burst where the SDK retry races a server-side-already-committed transaction.

Without this input the caller saw the failure tail (a `SetAsync` / `SetManyAsync` faulted) but the per-tree saturation signal stayed `Healthy` and any back-pressure consumer (the bench TCP reader, an upstream load balancer) had no leading-edge surface to slow down before the leak became visible at the operator level. Caller-driven cancellation paths are excluded from the counter (an `OperationCanceledException` whose token matches the caller's cancellation token is not counted) so a healthy caller-side abandonment never inflates the saturation signal.

Set to `0` to disable the trigger entirely (matches the `Timeout.InfiniteTimeSpan` sentinel on the sample-interval option). The validator rejects any other negative value. This option can be changed freely at any time; the new value takes effect on the next sampler tick.

### `WalSaturationFlushLatencyThreshold`

Per-provider-flush wall-clock latency at or above which the WAL writer increments a per-(tree, shard) flush-latency trip counter that feeds the saturation classifier (default: `null`, disabled). When the threshold is set and the classifier observes a non-zero delta on the trip counter in each of the last `WalSaturationFlushLatencySampleWindows` sample windows in a row, the tree is upgraded to `WalSaturationState.Saturated` regardless of admission-semaphore depth, dispatch-timeout trips, or provider-failure trips.

Captures the **small-batch blind spot** the existing three Saturated inputs cannot see: a workload that issues many small `SetAsync` calls against a saturating storage account never fills the per-partition admission semaphore (every batch is one entry, in-flight is rarely above 1-2), never trips `WalAppendDispatchTimeout` (the dispatch returns quickly with a slow but successful provider flush), and never tallies a `WalSaturationProviderFailureRateThreshold` trip (the flush succeeds, just slowly). The flush-latency input observes the same regime via the per-flush wall-clock cost. Sizing guidance: pick a threshold a few times your steady-state p99 provider flush latency (e.g. 500 ms when steady-state p99 is ~80 ms) so the trip counter stays at zero during healthy traffic and only ticks under genuine saturation.

The input is purely additive. Leaving the threshold at its default `null` is a zero-cost no-op: the writer skips the trip-counter increment entirely and the classifier behaves exactly as it shipped before the input was introduced. Must be positive when set; the validator rejects `TimeSpan.Zero` and any negative value.

This option can be changed freely at any time. The new value takes effect on the next provider flush.

### `WalSaturationFlushLatencySampleWindows`

Number of consecutive `WalSaturationSampleInterval` sample windows that must each observe a non-zero `WalSaturationFlushLatencyThreshold` trip-counter delta before the classifier upgrades the tree to `WalSaturationState.Saturated` (default: 3). Acts as the noise floor for the flush-latency input - a single slow provider flush in an otherwise healthy window is normal jitter; three consecutive windows each containing at least one slow flush is the leading edge of a saturation regime. At the default 200 ms `WalSaturationSampleInterval` the minimum sustained-slow-flush duration that triggers `Saturated` is ~`3 * 200 ms = 600 ms`.

Set lower (minimum 1) to make the input more sensitive at the cost of more transient classifier flaps; set higher to lengthen the sustained-slow regime the classifier requires before flagging. Has no effect when `WalSaturationFlushLatencyThreshold` is left at its default `null`. The validator rejects values less than 1.

This option can be changed freely at any time. The new value takes effect on the next sampler tick.

### `WalSaturationMaterialiserLagThreshold`

Leaf-materialiser drain-lag duration at or above which the saturation sampler records a pressure level that feeds the classifier (default: 30 seconds; set to `null` to disable the input entirely). On every sampler tick (default 200 ms) the sampler measures the drain lag directly from in-memory state as the WAL head wall-clock timestamp (the newest routed entry's HLC, tracked by the commit-log writer) minus the slowest in-memory leaf-materialiser cursor frontier (the cursor-registry minimum), clamped at zero - a head-relative measure, so an idle but caught-up tree (the frontier reaches the head) reads zero and never trips, and a never-checkpointed block pin is treated as zero lag rather than the full head age. Because the lag is recomputed live each tick rather than at a WAL GC pass, the input engages immediately on a write spike instead of waiting for the next collection. When the lag stays at or above this threshold for `WalSaturationMaterialiserLagSampleWindows` consecutive sampler windows the tree is held at `WalSaturationState.Throttled`. Unlike the dispatch-timeout, provider-failure, and flush-latency inputs, the drain-lag input never escalates to `Saturated`: it is a pure back-off that slows callers without ever tripping the writer admission gate's `LatticeSaturatedException` fast-fail. The resulting `Throttled` state engages the local WAL writer's per-append pacing (`WalThrottledAdmissionPace`) on the single-silo write path and the replication receiver flow control when a replicating peer exists, so upstream writers slow before the drain backlog grows unbounded.

Captures the **drain-path blind spot** the flush-latency and admission inputs cannot see: a write burst can be accepted and flushed quickly (healthy flush latency, shallow admission depth) yet outrun the rate at which leaf materialisers project committed WAL entries into the tree, so the durable backlog and its pin floor grow while every other saturation input reads healthy. Sizing guidance: pick a threshold a few times your steady-state materialiser drain lag so the observation stays quiet during healthy traffic and only fires when projection genuinely falls behind ingest.

The input is purely additive. Leaving the threshold at its default `null` is a zero-cost no-op: the sampler skips the lag computation and the classifier behaves exactly as before. Must be positive when set; the validator rejects `TimeSpan.Zero` and any negative value.

This option can be changed freely at any time. The new value takes effect on the next sampler tick.

### `WalSaturationMaterialiserLagSampleWindows`

Number of consecutive saturation-sampler windows that must each observe a materialiser drain-lag level at or above `WalSaturationMaterialiserLagThreshold` before the classifier holds the tree at `WalSaturationState.Throttled` (default: 3). Acts as the noise floor for the drain-lag input, mirroring `WalSaturationFlushLatencySampleWindows`, so a single sampler tick cannot flip the regime.

Set lower (minimum 1) to make the input more sensitive at the cost of more transient classifier flaps; set higher to lengthen the sustained-lag regime the classifier requires before flagging. Has no effect when `WalSaturationMaterialiserLagThreshold` is set to `null`. The validator rejects values less than 1.

This option can be changed freely at any time. The new value takes effect on the next sampler tick.

### `WalSaturationRecoveryWindow`

Window after the most-recently observed `WalSaturationState.Saturated` transition during which the classifier holds a tree at or above `Throttled` even if the current sampler tick's per-partition depth observation would otherwise classify it as `Healthy` (default: 1 second). Defends against bursty per-partition WAL drain where one partition fills to cap, drains entirely in the next tick, and the next partition fills - the per-tick `max(depth_ratio)` across partitions oscillates between `~1.0` and `~0.0` within a single sampler period and the classifier would otherwise flap `Healthy <-> Saturated` at the sampler cadence with `Throttled` never observed as a stable state. With the window in effect, callers see `Throttled` persist as the natural lead-up and fall-back regime around saturation episodes; the canonical TCP / queue ingest reader pattern can take the advisory `Throttled` action (yield-per-line, lower-priority dispatch) for measurable durations rather than seeing only the binary pause-or-go pattern.

The window does NOT affect the `Healthy -> Saturated` transition latency - `Saturated` still fires immediately on the current tick's at-cap condition, so the public saturation-signal surface's bound (transition latency under one `WalSaturationSampleInterval`) is preserved. It does NOT affect the recovery path either: once the window elapses AND the current tick observes no saturation pressure, the tree drops to `Healthy` and any pending `IWalSaturationSignal.WaitForHealthyAsync` completes; the window only delays the recovery by the configured value.

Set to `TimeSpan.Zero` to disable the upgrade entirely and restore the per-tick classifier behaviour the sampler shipped with. Set to `Timeout.InfiniteTimeSpan` to hold `Throttled` forever after the first `Saturated` observation - useful for tests that want a sticky `Throttled` floor without arming a wall-clock dependency, or for defensive deployments that prefer the saturation regime to be sticky. The validator rejects any other negative value.

This option can be changed freely at any time. The new value takes effect on the next sampler tick (or the next tick that would otherwise upgrade a tree, if the tree was Saturated more than `WalSaturationRecoveryWindow` ago).

### `WalAdmissionSaturationWaitBudget`

Wall-clock budget the WAL writer admission gate (`WalCommitLogWriter` -> `PartitionTracker.AcquireAsync`) spends parked on `IWalSaturationSignal.WaitForHealthyAsync` before refusing a dispatch with [`LatticeSaturatedException`](api.md#saturation-back-pressure---latticesaturatedexception) when the per-tree saturation signal stays `Saturated` past the budget (default: 5 seconds). Closes the consumer-coverage gap where the admission semaphore was previously signal-blind: under the storage-account 409-Conflict regime the classifier raised `Saturated` many times before the first observable failure, but every new dispatch still admitted into the semaphore and parked at the cap, taking the full `WalAppendDispatchTimeout` (default 30 seconds) to surface as `TimeoutException` instead of the configured shorter budget.

Mechanically: before each `_admission.WaitAsync` the tracker calls `signal.GetCurrentState(treeId)`. On `Healthy` / `Throttled` the check is a single concurrent-dictionary lookup and the caller proceeds directly into the semaphore (no allocation, no extra await). On `Saturated` the tracker awaits `WaitForHealthyAsync` bounded by this budget; if the signal recovers within the budget the caller proceeds into the semaphore as normal, if the budget expires with the tree still `Saturated` the tracker throws `LatticeSaturatedException` carrying the originating tree id, so the caller can detect the saturation regime via a single `is` check instead of waiting out the full `WalAppendDispatchTimeout`. A borderline-recovery race (the wait expires AND the signal recovered between the wait expiring and the re-check firing) is suppressed: the tracker re-reads the signal once after budget expiry and proceeds without refusal when the tree is observed `Healthy`.

The budget should be shorter than `WalAppendDispatchTimeout` (so the saturation refusal wins over the dispatch timeout) and longer than one `WalSaturationSampleInterval` (so a transient classifier flap does not surface as a refusal). The default (5 seconds) leaves `WalAppendDispatchTimeout`'s 30-second default as a strict outer bound and gives the storage account a realistic recovery window for the canonical 409-Conflict burst (typical recovery 1-3 seconds once offered load drops). Refusals are counted on the `orleans.lattice.wal.writer.append.admission_saturation_refusals` counter (tagged `tree`, `partition`), distinct from the dispatch-timeout counter (`admission_timeouts`) and the drain-release counter (`drain.releases`).

Set to `TimeSpan.Zero` to disable the admission-gate saturation check entirely (the historical pre-admission-gate behaviour). Set to `Timeout.InfiniteTimeSpan` to wait forever on `WaitForHealthyAsync`. The validator rejects any other negative value.

This option can be changed freely at any time. The new value takes effect on the next admission acquire (which is per-dispatch on the WAL writer hot path).

### `WalThrottledAdmissionPace`

Per-append pacing delay the WAL writer applies on the local admission path while the per-tree saturation signal reports `WalSaturationState.Throttled` (default: 25 milliseconds; set to `TimeSpan.Zero` to disable local pacing). This is what gives the drain-lag (and any other `Throttled`-mapped) back-pressure teeth on the **single-silo local-write path**, where there is no remote replication sender to drip-feed and the Saturated-only `WalAdmissionSaturationWaitBudget` gate never engages. Before each dispatch admits into the per-partition admission semaphore the writer reads the signal once; on `Throttled` it awaits a single bounded `Task.Delay` of this duration, pacing the local producer so the materialiser drain can catch up.

It is a pure back-off: it never throws, and it never escalates to `LatticeSaturatedException` - a `Throttled` tree slows callers, it does not fault them. The pacing is a no-op when no saturation signal is registered (single-node / unit-test writers), when the signal reports `Healthy` (a single concurrent-dictionary lookup, no await), and on `Saturated` (the separate admission gate already governs the dispatch, so the pace is skipped to avoid double-charging the caller). Caller-supplied cancellation surfaces as `OperationCanceledException`; a writer drain request short-circuits the pace silently so shutdown is never slowed.

Sizing guidance: the delay is applied per accepted append while the tree is `Throttled`, so it bounds the local single-silo append rate to roughly `1 / WalThrottledAdmissionPace` per partition during back-pressure (the default 25 ms caps a back-pressured partition at ~40 appends/second). Raise it to slow producers harder when the materialiser drain is the bottleneck; lower it (or set `TimeSpan.Zero`) when you would rather rely solely on the replication-side flow control. The validator rejects negative values.

This option can be changed freely at any time. The new value takes effect on the next admission acquire (per-dispatch on the WAL writer hot path).

### `WalMaxPendingBatches`

Maximum number of in-flight storage-provider flushes the partition grain admits concurrently (default: 16, the measured Azure Tables Standard sweet spot at 4,000 keys/s offered load on Standard_D4as_v5). Raising this value increases pipeline depth against the storage provider - the next caller can enqueue a new flush as soon as the in-flight count drops below the cap, rather than waiting for the head of the in-flight chain to settle. The previous default was 8; raising it to 16 produced a +57% increase in steady-state silo throughput at the 4k:5 rung with no reliability regression.

Set to `1` to restore the historical single-in-flight shape (strict ordering against the provider; no pipeline depth). Most workloads on durable backing stores benefit from the default; the strict-ordering shape is useful only when targeting a provider whose ordering guarantees are weaker than per-request linearisability.

The flush-cap-reached cutover backs off the calling task by awaiting the in-flight head, so the cap also acts as the natural back-pressure ceiling against caller fan-in. Raising the cap above what the storage provider can usefully serve in parallel degrades latency without improving throughput - more concurrent flushes compete for the same provider budget and grow each flush's slow-tail wait. At the canonical `WalPartitions = 8` the combined fan-out is `8 * 16 = 128` concurrent flushes against the provider, which is at the edge of a single Azure Tables Standard storage account's sustained throughput budget; see [WAL Tuning](wal-tuning.md) for the envelope above which the storage account becomes the binding constraint and the recovery path (`WalPartitions` fan-out across accounts, not a higher per-partition cap).

This option can be changed freely at any time. The new value takes effect on the next batch boundary.

### `WalMaxRetainedBytes`

Optional advisory ceiling on retained WAL bytes per tree (default: `null`, disabled). When set, each `ILatticeWalGc.RunOnceAsync` pass samples retained bytes before and after its safe trim; if the pre-trim total exceeds the ceiling the policy schedules a byte-pressure trim (surfaced as the `lattice.storage.policy.trim.triggered` counter and `LatticeWalGcReport.BytePressureTriggered`), trimming toward `WalMaxRetainedBytes * WalBytePressureReclaimTarget`. The policy is **advisory only**: the GC never trims past the safe frontier (the slowest consumer's cursor and any `WalRetention` floor) to honour it, so a tree pinned by a lagging consumer can remain over the ceiling - `LatticeWalGcReport.BytePressureOverThreshold` and the `lattice.storage.policy.over_threshold` gauge report that condition. `null` disables the policy. See [WAL](wal.md) and [Tree Storage](tree-storage.md).

This option can be changed freely at any time. The new value takes effect on the next GC tick.

### `WalPartitions`

**Per-tree, pinned on first WAL write.** Existing trees pin the value in force at first WAL write into the tree registry, so a silo-wide default change is non-breaking for already-registered trees - they continue to fan across whatever partition count they were created with. New trees pick up the current default unless an operator override is configured.

**Activation-time replay is partition-aware.** The leaf grain's activation-time materialiser iterates `[0, WalPartitions)` and runs an independent fall-off-log classification, slice read, and projection-checkpoint advance per partition. Per-partition checkpoints persist into the `LeafNodeState.ProjectionCheckpointOffsetsByPartition` slot (`long[]?`, additive `[Id]`), with partition 0 also mirrored into the legacy scalar `ProjectionCheckpointOffset` slot so a downgrade to a host that has never observed multi-partition state still reads a valid single-partition shape. Per-partition cursor consumer ids take the form `_lattice_materialiser_{treeId}_{leafGrainId}_{partition}` so the per-shard WAL GC trims each partition independently against its own slowest consumer; on `WalPartitions = 1` the legacy unsuffixed shape `_lattice_materialiser_{treeId}_{leafGrainId}` is preserved for wire compatibility with hosts that have never enabled multi-partition replay.

Must be `>= 1`. Values below 1 fail option validation at silo start.

### `WalMaterialiserPinShards`

Number of durable-pin grain activations the per-tree leaf-materialiser checkpoint floor is spread across (default: 8). Each active leaf reports its per-WAL-partition projection frontier as a durable pin so the WAL garbage collector never trims past an entry a leaf has not yet projected. Historically every leaf in a tree funnelled its pin into a single per-tree grain, so a leaf-birth or split storm serialized `O(leaves x partitions)` durable writes through one activation and that activation became the bottleneck that wedged the drain path. Sharding spreads the load: each `consumerId` deterministically maps to one shard so the monotonic-max merge stays correct, and the WAL GC fans its read across every shard (plus the legacy single-key shape) so no trim floor is lost.

Set to `1` to restore the historical single-activation shape. Changing this value is a **durable-store migration**: pins written under the previous shard count remain readable because the GC dual-reads every shard activation plus the legacy unsharded key during the transition, but a deliberate rollout (drain, change, redeploy) is recommended rather than flipping it on a hot cluster. Must be `>= 1`; the validator rejects values below 1.

### `WalMaterialiserPinFlushIntervalMs`

Debounce window, in milliseconds, over which a shard's durable pin writes are coalesced behind a grain-timer flush (default: 250 ms). Within the window the shard advances its in-memory monotonic-max frontier on every advancing report but persists at most one durable `WriteStateAsync`, collapsing a report burst into one durable write per shard per window. The coalescing only ever retains **more** WAL than an immediate write would (the persisted floor lags the in-memory floor by at most one window), so it is always GC-safe.

Set to `0` to disable coalescing so every advancing report persists synchronously, matching the historical shape. Must be `>= 0`; the validator rejects negative values.

This option can be changed freely at any time. The new value takes effect on the next pin report.

### `WalMaterialiserMaxConcurrentReplays`

Per-silo ceiling on the number of leaf grains that may run their activation-time WAL replay concurrently (default: `0`, which resolves to `Environment.ProcessorCount` at runtime). A mass reactivation (for example after a `docker restart` or a silo rejoin) can otherwise stampede the scheduler as every reactivating leaf replays its WAL backlog at once; the ceiling makes the surplus queue on a process-wide gate and drain in waves instead. A no-op activation (a leaf with no tree binding) consumes no permit.

Set to a positive value to pin the ceiling explicitly. Must be `>= 0`; the validator rejects negative values.

### `WalReplayMaxRecordsPerTurn`

Number of WAL records a single activation-time replay projects before yielding the Orleans turn cooperatively (`await Task.Yield()`), so a long replay does not monopolise the activation's turn and starve other grain calls on the same activation (default: 256). This is distinct from the cross-RPC `ReplaySliceBudget` slicing; it bounds the synchronous run length **within** a single replay turn.

Set to `0` to disable the cooperative yield so replay runs to completion without voluntarily yielding (the historical shape). Must be `>= 0`; the validator rejects negative values.

### `WalGcInterval`

Cadence at which the per-silo core WAL garbage-collection scheduler runs a `ILatticeWalGc.RunOnceAsync` pass over **every** registered tree (default: 1 hour, **enabled**). The core library ships the WAL garbage collector, but historically only drove it for *replicated* trees (via the replication package's per-tree maintenance grain). That left two retention gaps: a durable-WAL host that runs **without** the replication package never trimmed its WAL at all, and every **non-replicated** tree in a replicated host was never collected - both grew without bound, and `WalRetention` was inert for them. The built-in scheduler closes the gap by collecting every registered tree, replicated or not, so `WalRetention` is effective out of the box.

A pass is retention housekeeping, not a latency-sensitive operation. Its cost scales with `trees × WalPartitions` storage reads (a head scan plus a trim per partition) and runs on every silo, so the default cadence is deliberately coarse to keep storage cost low (one fan-out per silo per hour). A host that needs a tighter disk bound - a high write rate paired with a small `WalRetention` - can lower it; `TimeSpan.Zero` (or any non-positive value) **disables** the scheduler entirely, restoring the historical caller-driven behaviour.

The first pass is not run at silo start: it is staggered by a random offset of half to one full interval, so the silo finishes activating before the scheduler adds scan/trim I/O and a rolling cluster restart does not align every silo's fan-out into a correlated I/O storm.

```csharp verify
// Tighten the cadence on a high-write durable-WAL host, or disable it.
siloBuilder.ConfigureLattice(o => o.WalGcInterval = TimeSpan.FromMinutes(5));
siloBuilder.ConfigureLattice(o => o.WalGcInterval = TimeSpan.Zero); // disable
```

The scheduler composes with the replication maintenance grain (which collects replicated trees on its own faster cadence): `RunOnceAsync` and the underlying WAL `TrimAsync` are idempotent, and the pass never trims past the minimum consumer cursor or the leaf-materialiser checkpoint floor, so a tree collected by both drivers is trimmed safely and it never over-trims. A per-tree GC failure is logged and skipped without stalling the rest of the pass. This is a **global** knob read from the default (unnamed) options; per-tree overrides do not apply. It is read once when the scheduler starts; change it before silo start to take effect.

### `WalRetention`

Optional wall-clock hard ceiling on WAL retention (default: `null`, disabled). When set, the WAL garbage collector trims entries whose HLC wall-clock is older than `now - WalRetention` regardless of consumer cursor position, bounding worst-case disk usage even when a registered consumer is hopelessly behind. The lagging consumer then "falls off the log" on its next read, surfacing the gap to the auto-bootstrap trigger (replication-side concern). When `null`, the GC predicate is purely `min(consumer cursors)`, and a lagging consumer pins the WAL until it catches up. Must be strictly greater than `TimeSpan.Zero` when set.

This option can be changed freely at any time. The new value takes effect on the next GC tick.

## Storage Provider Name

Lattice grains use the storage provider named `"lattice"` (exposed as `LatticeOptions.StorageProviderName`). The `AddLattice` extension method passes this name to your storage registration delegate. In advanced scenarios where you register storage directly, use this constant to ensure the provider name matches:

```csharp verify
siloBuilder.AddMemoryGrainStorage(LatticeOptions.StorageProviderName);
```

## Replication options

Cross-cluster replication is configured by the
`Orleans.Lattice.Replication` package, not by `LatticeOptions`. The
full options reference - including `ReplicatedTrees`, `ReplicationPeers`,
`ShipDoorbellEnabled`, the backoff triple, and the maintenance cadence
knobs - lives on `LatticeReplicationOptions` and is documented in
[Replication drivers](../lattice.replication/replication-drivers.md).
Peer membership in particular has its own resolution model (topology
seam vs. `ReplicationPeers` projection) covered in
[Peer configuration](../lattice.replication/replication-drivers.md#peer-configuration-topology-vs-replicationpeers).

The replication receiver also consumes this file's WAL-saturation options
indirectly: with `AddLatticeReplication`, a receiver translates its local
WAL-saturation state (driven by `WalSaturationThrottledRatio` and the other
`WalSaturation*` knobs above) into sender backoff hints **by default**. That
mapping is tuned with the separate `WalSaturationReceiverFlowControlOptions`,
documented in
[Receiver flow control](../lattice.replication/receiver-flow-control.md#built-in-wal-saturation-policy).

## Materialised view options

Materialised views are configured per view name on a separate options type,
`LatticeViewOptions`, not on `LatticeOptions`. Set defaults or per-view overrides
with `ConfigureLatticeView` (the view-name overload targets one view; the
no-name overload sets the default applied to every view):

```csharp verify
siloBuilder.ConfigureLatticeView("adults", options =>
{
    options.BatchSize = 512;
    options.CoalesceWindow = TimeSpan.FromMilliseconds(100);
});
```

| Option | Default | Meaning |
|--------|---------|---------|
| `BatchSize` | 256 | Maximum WAL entries read from each source partition per drain pass. |
| `CoalesceWindow` | 50 ms | Period of the background drain timer. |
| `SourceIdentityBackstopInterval` | 30 s | Safety-net interval after which the maintainer re-resolves its source tree's physical identity from the registry when no alias-change notification has arrived. In steady state the source binding is event-driven, so this backstop only covers a missed push. Must be greater than zero. |
| `AggregationFanout` | 1 | Aggregation views only: shards each group's accumulator into this many sub-accumulators hashed on the source key, merged at read. |
| `AggregationMaxGroupEntries` | 0 | Aggregation views only: when greater than zero, bounds each `Min` / `Max` / `SetUnion` group shard (approximate mode). 0 keeps every group exact. |
| `MaxStagedTransactions` | 1024 | Maximum in-flight atomic-write transactions buffered before the backstop forces a rebuild. |
| `MaxStagedBytes` | 64 MiB | Maximum buffered prepared-entry payload before the backstop forces a rebuild. |
| `ReadHandleCacheTtl` | 1 s | How long an `ILatticeView` handle caches the resolved live view tree id. Bounds the post-swap read-staleness window. |
| `OldGenerationReclaimGrace` | 5 s | How long a swapped-out view tree is retained before reclamation. Must exceed `ReadHandleCacheTtl`. |
| `CrossTreeReadinessTimeout` | 5 s | Cross-tree atomic visibility only: how long a completed cross-tree batch waits for every present participant view before degrading to per-tree atomicity. Must be greater than zero. |
| `ReplicationMode` | `DeriveLocally` | How the view tree is made available across clusters. `ShipView` requires the replication package. |
| `ShipViewProducerClusterId` | `null` | Required only when `ShipView` replicates both source and view trees. The stable, case-sensitive replication cluster id of the single producer. |
| `MaxLagBudget` | 0 | Maximum committed-but-unapplied source entries before the view is force-evicted (WAL unpinned and rebuilt). 0 disables eviction. Must not be negative. |
| `LagEvictionCooldown` | 30 s | Minimum interval between two lag-budget evictions of the same view. Has no effect when `MaxLagBudget` is 0. |
| `ObeySourceBackpressure` | `true` | Whether the maintainer throttles its own drain when the source tree's WAL is under saturation back-pressure (smaller batch + deferred ticks). Set to `false` to always drain at full rate. |
| `ThrottledBatchRatio` | 0.5 | Fraction of `BatchSize` drained per pass while the source is `Throttled`. Clamped to `[0, 1]`; the effective batch is clamped to `[1, BatchSize]`. |
| `ThrottledPauseMs` | 50 | Milliseconds background drain ticks are skipped after a pass that saw a `Throttled` source. `<= 0` disables the deferral. |
| `SaturatedBatchSize` | 16 | Drip-feed batch drained per pass while the source is `Saturated`. Clamped to `[1, BatchSize]`. |
| `SaturatedPauseMs` | 500 | Milliseconds background drain ticks are skipped after a pass that saw a `Saturated` source. `<= 0` disables the deferral. |

See [Materialised views](materialised-views.md) for the full behaviour of each
option, including what registrations a view needs (`AddLattice` +
`AddLatticeViews`, the latter folding in `AddWalCursorRegistry`).

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
