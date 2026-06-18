# Configuration

> **Compression options** (replication framing tail, future WAL/storage payloads) are documented separately - see [`compression.md`](compression.md) for the seam, the registration helpers, the tag-space partitioning, and the relevant option keys. The compression **algorithm and Zstd level are safe to change after data already exists**: stored payloads are self-describing and read back by their own per-row tag, so a level/algorithm change applies only to newly written data while existing rows decode unchanged.

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
| [`LeafProjectionRetention`](#leafprojectionretention) | `TimeSpan` | 7 days | Yes |
| [`LeafSnapshotMargin`](projection-rebuild.md) | `double` | 0.30 | Yes |
| [`LeafSnapshotReClassifyEveryNCheckpoints`](projection-rebuild.md) | `int` | 64 | Yes |
| [`MaintainProjectionDigest`](#maintainprojectiondigest) | `bool` | `true` | Yes |
| [`MaterialiserCheckpointEntries`](#materialisercheckpointentries) | `int` | 5000 | Yes |
| [`MaterialiserCheckpointInterval`](#materialisercheckpointinterval) | `TimeSpan` | 5 seconds | Yes |
| [`MaxConcurrentAutoSplits`](#maxconcurrentautosplits) | `int` | 2 | Yes |
| [`MaxConcurrentDrains`](#maxconcurrentdrains) | `int` | 4 | Yes |
| [`MaxConcurrentMigrations`](#maxconcurrentmigrations) | `int` | 4 | Yes |
| [`MaxCursorSnapshotPinTtl`](#maxcursorsnapshotpinttl) | `TimeSpan` | 7 days | Yes |
| [`MaxLeafEntriesBeforeForcedCompaction`](tombstone-compaction.md) | `int` | 0 (disabled) | Yes |
| [`MaxLeafReplayEntries`](#maxleafreplayentries) | `int` | 10 000 | Yes |
| [`MaxPinnedSagaDecisions`](#maxpinnedsagadecisions) | `int` | 100 000 | Yes |
| [`MaxScanRetries`](#maxscanretries) | `int` | 3 | Yes |
| [`MaxSnapshotReplayEntries`](snapshot-cursors.md) | `long` | 10 000 000 | Yes |
| [`MinTombstoneRatioForCompaction`](tombstone-compaction.md) | `double` | 0.0 (disabled) | Yes |
| [`PrefetchEntriesScan`](#prefetchentriesscan) | `bool` | `false` | Yes |
| [`PrefetchKeysScan`](#prefetchkeysscan) | `bool` | `false` | Yes |
| [`ProjectionRebuildPolicy`](#projectionrebuildpolicy) | enum | `SnapshotThenWal` | Yes |
| [`PublishEvents`](#publishevents) | `bool` | `false` | Yes |
| [`RetryPolicy`](retry-policy.md) | `ILatticeRetryPolicy?` | `null` (no retry) | Yes |
| [`ShardForwardTimeout`](#shardforwardtimeout) | `TimeSpan` | 15 seconds | Yes (on next forward) |
| [`SnapshotLeafIdleTtl`](snapshot-cursors.md) | `TimeSpan` | 30 minutes | Yes |
| [`SoftDeleteDuration`](#softdeleteduration) | `TimeSpan` | 72 hours | Yes |
| [`SplitDrainBatchSize`](#splitdrainbatchsize) | `int` | 1024 | Yes |
| [`StorageUsageCacheTtl`](#storageusagecachettl) | `TimeSpan` | 10 seconds | Yes |
| [`StorageUsagePollInterval`](#storageusagepollinterval) | `TimeSpan` | 15 seconds | No (global; read from the default options) |
| [`StorageUsageDeepPollInterval`](#storageusagedeeppollinterval) | `TimeSpan` | `TimeSpan.Zero` (disabled) | No (global; read from the default options) |
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
| [`WalMaxPendingBatches`](#walmaxpendingbatches) | `int` | 16 | Yes |
| [`WalMaxRetainedBytes`](#walmaxretainedbytes) | `long?` | `null` (disabled) | Yes |
| [`WalPartitions`](#walpartitions) | `int` | 8 | No (per-tree, pinned on first WAL write) |
| [`WalRetention`](#walretention) | `TimeSpan?` | `null` (disabled) | Yes |
| [`WalSaturationDispatchTimeoutThreshold`](#walsaturationdispatchtimeoutthreshold) | `int` | 1 | Yes |
| [`WalSaturationFlushLatencySampleWindows`](#walsaturationflushlatencysamplewindows) | `int` | 3 | Yes |
| [`WalSaturationFlushLatencyThreshold`](#walsaturationflushlatencythreshold) | `TimeSpan?` | `null` (disabled) | Yes |
| [`WalSaturationProviderFailureRateThreshold`](#walsaturationproviderfailureratethreshold) | `int` | 1 | Yes |
| [`WalSaturationRecoveryWindow`](#walsaturationrecoverywindow) | `TimeSpan` | 1 second | Yes |
| [`WalSaturationSampleInterval`](#walsaturationsampleinterval) | `TimeSpan` | 200 milliseconds | Yes |
| [`WalSaturationThrottledRatio`](#walsaturationthrottledratio) | `double` | 0.75 | Yes |
| [`WalAdmissionSaturationWaitBudget`](#waladmissionsaturationwaitbudget) | `TimeSpan` | 5 seconds | Yes |
| [`WalStorageProvider`](wal-storage-providers.md) | `Func<string, IWalStorageProvider>?` | `null` (DI default) | Yes |

### Structural sizing (registry-pinned)

`MaxLeafKeys`, `MaxInternalChildren`, and `ShardCount` used to live on `LatticeOptions` but are now pinned per-tree on the `TreeRegistryEntry`. They are seeded from `LatticeConstants` on first tree use (defaults 128 / 128 / 64) and can be changed through:

- `ILattice.ResizeAsync(newMaxLeafKeys, newMaxInternalChildren)` - see [Tree Sizing](tree-sizing.md#resizing-an-existing-tree). Runs online; empty-tree fast-path if no data exists.
- `ILattice.ReshardAsync(newShardCount)` - see [Online Reshard](online-reshard.md). Grow-only unless the tree is empty (fast-path).
- Pre-registering the pin explicitly before first use via `ILatticeRegistry.RegisterAsync(treeId, new TreeRegistryEntry { MaxLeafKeys = …, MaxInternalChildren = …, ShardCount = … })`.

### Virtual shard space (constant)

The virtual shard space is fixed at `LatticeConstants.DefaultVirtualShardCount = 4096` for every tree. Keys hash into `[0, 4096)` and the per-tree [`ShardMap`](tree-registry.md#shard-map) collapses ranges of virtual slots onto physical shards. This indirection decouples logical key routing from the physical shard count, enabling adaptive shard splitting without rehashing existing keys.

The pinned `ShardCount` must divide 4096 evenly for the default identity map to preserve `hash % ShardCount` routing exactly; this invariant is validated at use time by `ShardMap.CreateDefault`. The value is a compile-time constant - changing it in source would invalidate every persisted `ShardMap` and is treated as a breaking wire-format change.

### `AtomicWriteRetention`

Retention window for completed `SetManyAtomicAsync` saga state (default: 48 hours). After a saga reaches a terminal state, its coordinator grain retains its persisted progress for this window so duplicate submissions with the same operation ID are idempotent. A retention reminder fires at the end of the window and clears the state. Set `Timeout.InfiniteTimeSpan` to disable automatic cleanup. See [Atomic Writes](atomic-writes.md).

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

### `MaxConcurrentAutoSplits`

Maximum number of in-flight adaptive splits per tree (default: 2). Because `HotShardMonitorGrain` is keyed per tree, this limit is enforced independently per tree in a multi-tree cluster.

This option can be changed freely at any time.

### `MaxConcurrentDrains`

Maximum number of concurrent shadow-write drains per tree (default: 4). Helps limit the burst I/O load during adaptive splits. Each drain transfers a split shard's data to the new location in the background.

This option can be changed freely at any time.

### `MaxConcurrentMigrations`

Maximum number of concurrent active-tombstone migrations per tree (default: 4). Helps limit the burst I/O load during bulk-deletes. Each migration drains a tombstone's shadow-write in the background.

This option can be changed freely at any time.

### `MaxCursorSnapshotPinTtl`

Hard upper bound on how long the per-tree `ITxRegistryGrain` will retain the saga-decision snapshot captured by a point-in-time durable cursor (default: 7 days). A live point-in-time cursor slides this TTL on every `Next*Async`; a stalled cursor that misses the slide will eventually have its pin reaped by the registry, after which the next call surfaces `LatticeCursorSnapshotExpiredException` and the cursor must be reopened.

The cap exists so a forgotten point-in-time cursor cannot stall registry-tombstone pruning forever. Set `Timeout.InfiniteTimeSpan` to disable the registry-side cap entirely - cursor lifetime then depends solely on `CursorIdleTtl` and on `MaxPinnedSagaDecisions`. See [Durable Cursors - Point-in-time cursors](durable-cursors.md#point-in-time-cursors).

This option can be changed freely at any time.

### `MaxLeafReplayEntries`

Maximum number of WAL entries a cold leaf is permitted to replay against its projection at activation time before the leaf falls back to the snapshot-then-WAL recovery path indicated by `ProjectionRebuildPolicy` (default: 10 000). Bounds activation latency for a leaf whose persisted checkpoint has fallen far behind the WAL head; see [Projection Rebuild](projection-rebuild.md) for the full trigger set.

```csharp verify
siloBuilder.ConfigureLattice(o => o.MaxLeafReplayEntries = 100_000);
```

This option can be changed freely at any time. The new value takes effect on the next leaf activation.

### `MaxPinnedSagaDecisions`

Registry-wide footprint cap on the number of saga decisions that may be pinned across all live point-in-time cursors on a single tree (default: 100 000). `OpenKeyCursorAsync` / `OpenEntryCursorAsync` opened with `pointInTime: true` consult the registry: if accepting the new snapshot would push the pinned-decision count past this cap, the open call throws `LatticeCursorRegistryPinExhaustedException` and no pin is installed. Existing pinned cursors continue paging.

Sized for a tree carrying a steady-state in-flight-saga set in the low thousands plus a handful of overlapping long-running point-in-time cursors. Raise if a workload routinely opens many concurrent multi-day point-in-time cursors against a saga-heavy tree; lower if a single tree must keep registry footprint tightly bounded.

This option can be changed freely at any time.

### `MaxScanRetries`

Maximum bounded-retry passes for `CountAsync`, `ScanKeysAsync`, and `ScanEntriesAsync` when the shard topology changes mid-scan (default: 3). If the topology keeps mutating after every reconciliation step, the scan throws `InvalidOperationException` rather than returning a silently incomplete result. Under the default split rate-limits (`MaxConcurrentAutoSplits = 2`, `HotShardSplitCooldown = 2 minutes`), exhausting 3 retries is not a realistic operational concern. See [Scan reliability](api.md#scan-reliability).

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
| `AggregationFanout` | 1 | Aggregation views only: shards each group's accumulator into this many sub-accumulators hashed on the source key, merged at read. |
| `AggregationMaxGroupEntries` | 0 | Aggregation views only: when greater than zero, bounds each `Min` / `Max` / `SetUnion` group shard (approximate mode). 0 keeps every group exact. |
| `MaxStagedTransactions` | 1024 | Maximum in-flight atomic-write transactions buffered before the backstop forces a rebuild. |
| `MaxStagedBytes` | 64 MiB | Maximum buffered prepared-entry payload before the backstop forces a rebuild. |
| `ReadHandleCacheTtl` | 1 s | How long an `ILatticeView` handle caches the resolved live view tree id. Bounds the post-swap read-staleness window. |
| `OldGenerationReclaimGrace` | 5 s | How long a swapped-out view tree is retained before reclamation. Must exceed `ReadHandleCacheTtl`. |
| `CrossTreeReadinessTimeout` | 5 s | Cross-tree atomic visibility only: how long a completed cross-tree batch waits for every present participant view before degrading to per-tree atomicity. Must be greater than zero. |
| `ReplicationMode` | `DeriveLocally` | How the view tree is made available across clusters. `ShipView` requires the replication package. |
| `MaxLagBudget` | 0 | Maximum committed-but-unapplied source entries before the view is force-evicted (WAL unpinned and rebuilt). 0 disables eviction. Must not be negative. |
| `LagEvictionCooldown` | 30 s | Minimum interval between two lag-budget evictions of the same view. Has no effect when `MaxLagBudget` is 0. |

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
