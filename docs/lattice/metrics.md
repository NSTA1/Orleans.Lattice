# Metrics

Orleans.Lattice publishes runtime telemetry through [`System.Diagnostics.Metrics`](https://learn.microsoft.com/dotnet/core/diagnostics/metrics), so any
OpenTelemetry-compatible exporter (Prometheus, OTLP, Azure Monitor, Datadog,
etc.) can subscribe once to the library's meter and receive every instrument
without per-metric wiring.

## Meter

All instruments are owned by a single static `Meter` exposed via
`Orleans.Lattice.LatticeMetrics`:

| Member | Value |
|---|---|
| `LatticeMetrics.MeterName` | `orleans.lattice` |
| `LatticeMetrics.Meter` | the `Meter` instance (exposed for reference-based subscription in tests) |

The name is pinned by a regression test (`LatticeMetrics_meter_name_is_orleans_lattice`) so it cannot drift.

## Tag conventions

Every Lattice instrument carries a consistent set of low-cardinality tags:

| Tag key | Applies to | Value |
|---|---|---|
| `tree` | every instrument | Logical tree id (`ILattice.TreeId`) |
| `shard` | shard-level instruments only | Physical shard index as an `int` |
| `operation` | `orleans.lattice.leaf.scan.duration` only | `keys` or `entries` |
| `step` | `orleans.lattice.leaf.commit.duration` only | `wal`, `apply`, `observer`, or `digest` |
| `stage` | `orleans.lattice.set.stage.duration`, `orleans.lattice.set_many.stage.duration`, `orleans.lattice.get.stage.duration`, `orleans.lattice.get_many.stage.duration`, `orleans.lattice.saga.broadcast.shard.stage.duration` | Sub-stage name within an envelope - see each instrument below |
| `phase` | `orleans.lattice.provider.commit.duration`, `orleans.lattice.provider.retry.exhausted`, `orleans.lattice.provider.idempotent_replays` | `phase1` (per-batch partition txn) or `phase2` (manifest partition txn); `idempotent_replays` is always `phase1` |
| `status` | `orleans.lattice.provider.retry.attempts`, `orleans.lattice.provider.retry.exhausted`, `orleans.lattice.provider.retry.short_circuited` | Azure Tables HTTP status string (e.g. `503`, `429`; `0` for a transport-level failure with no HTTP exchange; `unknown` when the SDK surfaced no status) |
| `outcome` | `orleans.lattice.atomic_write.completed`, `orleans.lattice.atomic_write.duration`, `orleans.lattice.atomic_write.batch_size`, `orleans.lattice.leaf.replay.duration`, `orleans.lattice.leaf.replay.entries`, `orleans.lattice.compaction.leaves.visited` | Discriminator - see each instrument below |
| `kind` | `orleans.lattice.coordinator.completed`, `orleans.lattice.tree.lifecycle`, `orleans.lattice.events.published` | Discriminator - see each instrument below |
| `trigger` | `orleans.lattice.compaction.pass.duration`, `orleans.lattice.compaction.leaves.visited` | `reminder`, `ratio`, `size`, or `operator` |
| `path` | `orleans.lattice.compaction.leaves.visited` | `walk` or `dirty-set` |
| `reason` | `orleans.lattice.events.dropped`, `orleans.lattice.storage.policy.trim_triggered`, `orleans.lattice.storage.wal.compression_skipped` | `missing_provider` or `publish_error` (events.dropped); `byte_pressure` (policy.trim_triggered); `below_threshold`, `inflation_guard`, or `disabled` (wal.compression_skipped) |
| `config` | `orleans.lattice.config.changed` | Configuration dimension name (e.g. `publish_events`) |
| `wal_partitions` | every `orleans.lattice.wal.*` histogram | `LatticeOptions.WalPartitions` at the time of activation (Phase A attribution) |
| `wal_max_pending_batches` | every `orleans.lattice.wal.*` histogram | `LatticeOptions.WalMaxPendingBatches` at the time of activation (Phase A attribution) |
| `pipeline_phase2` | `orleans.lattice.provider.commit.duration` | `true` or `false`, reflecting `AzureTableWalStorageOptions.PipelinePhaseTwoCommits` |
| `shard_count` | `orleans.lattice.warmup.duration` | Per-tree physical-shard-root probe fan-out |

Leaf grain ids are **not** emitted as a tag - in a large tree they would produce
unbounded tag cardinality. All leaf instruments are aggregated to the tree level.

## Instrument catalog

### Shard-level (sourced from `ShardRootGrain`)

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.shard.reads` | `Counter<long>` | `{op}` | Read **operations** served by a shard root (`GetAsync`, `ExistsAsync`, scan, count, etc.). One increment per operation, not per record returned. Tagged `tree` and `shard`. |
| `orleans.lattice.shard.writes` | `Counter<long>` | `{op}` | Write **operations** served by a shard root (`SetAsync`, `DeleteAsync`, `MergeManyAsync`, `BulkLoadAsync`, etc.). One increment per operation: a batched or bulk call (`SetManyAsync`, `MergeManyAsync`, `DeleteRangeAsync`, `SetManyWherePredicateAsync`, `BulkLoadAsync`, `BulkLoadRawAsync`, `BulkAppendAsync`) counts **once regardless of entry count**, so a 5000-record import advances this by only the number of bulk operations. Use `orleans.lattice.shard.records_written` for the record rate. Tagged `tree` and `shard`. |
| `orleans.lattice.shard.records_written` | `Counter<long>` | `{record}` | Individual **records** written by a shard root - the per-record companion to `orleans.lattice.shard.writes`. Incremented by 1 on a single-key write, by the batch size on `SetManyAsync` / `MergeManyAsync` / bulk load, and by the affected count on `DeleteRangeAsync` / `SetManyWherePredicateAsync` (published once the operation succeeds). Plot both: their ratio is the effective batch size. Tagged `tree` and `shard`. |
| `orleans.lattice.shard.digest_reads` | `Counter<long>` | `{op}` | Projection-digest reads served by a shard root - one increment per `IShardRootGrain.GetShardProjectionDigestAsync` call. A whole-tree poll of `ILattice.GetLeafProjectionDigestAsync` produces exactly `shardCount` increments; a higher rate signals a regression that fell back to walking every leaf. Tagged `tree` and `shard`. |
| `orleans.lattice.shard.splits_committed` | `Counter<long>` | `{split}` | Adaptive shard-split commits - fired once per successful `ShardMap` swap from `TreeShardSplitGrain.FinaliseAsync`. |
| `orleans.lattice.split.retroactive_forward.entries` | `Counter<long>` | `{entry}` | Pending prepared mutations retroactively shadow-forwarded from a source shard's leaf chain into the destination shard's `_pendingTx` buckets at the start of an adaptive split's `BeginShadowWrite` phase. Tagged `tree` and `shard` (source). |
| `orleans.lattice.split.retroactive_forward.duration` | `Histogram<double>` | `ms` | Wall-clock duration of the retroactive prepared-mutation sweep before the split coordinator transitions to the `Drain` phase. Tagged `tree` and `shard` (source). |
| `orleans.lattice.split.in_flight` | `Histogram<long>` | `{split}` | Per-tree count of autonomic splits in flight, sampled once every `HotShardMonitorGrain` pass (derived from each shard's authoritative `IsSplitting` status). Tagged `tree`. Emitted every pass **regardless** of whether the cluster-wide split gate (`MaxClusterConcurrentAutoSplits`) is enabled; compute the cluster aggregate as a `sum` across the `tree` tag to size the ceiling and decide whether the gate is needed. |
| `orleans.lattice.split.candidates_suppressed` | `Counter<long>` | `{shard}` | Hot, otherwise-eligible shards that could not trigger an autonomic split this pass because a concurrency cap (the per-tree `MaxConcurrentAutoSplits` or the cluster-wide `MaxClusterConcurrentAutoSplits`) was already reached. Tagged `tree`. Emitted regardless of whether the gate is enabled; chronically non-zero across many trees while `split.in_flight` is high signals aggregate drain pressure the per-tree cap alone cannot see. |
| `orleans.lattice.split.admission.deferred` | `Counter<long>` | `{shard}` | Otherwise-eligible autonomic splits held back specifically by the **cluster-wide** admission gate (no cluster headroom left under `MaxClusterConcurrentAutoSplits`). Tagged `tree` and `reason` (`cluster_cap`). Flat-zero means the ceiling never binds (pure overhead - raise it or leave the gate off); sustained non-zero alongside rising hot-shard latency means the ceiling is set too low and is starving legitimate elasticity. |

To derive ops/sec, compute the rate of `shard.reads + shard.writes` at the
collector; the same underlying counters back the internal hotness monitor that
drives autonomic splitting.

### Leaf-level (sourced from `BPlusLeafGrain`)

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.leaf.write.duration` | `Histogram<double>` | `ms` | Duration of `IPersistentState.WriteStateAsync` calls from a leaf grain - i.e. storage-provider write latency. |
| `orleans.lattice.leaf.commit.duration` | `Histogram<double>` | `ms` | Per-step latency on the `BPlusLeafGrain` commit path. Tagged `step=wal` (commit-log-writer append), `step=apply` (in-memory projection apply + state persist), `step=observer` (`IMutationObserver` fan-out), or `step=digest` (awaited cross-grain `OnChildDigestPublishedAsync` RPC to the parent internal node). Emitted from every foreground commit-path code path (single-key `SetAsync` / `DeleteAsync`, per-leaf `DeleteRangeAsync`, prepared-write commit) so operators can attribute latency between durability, projection, observer overhead, and the parent-digest publish hop independently. Cold / structural digest publishes (leaf-split topology, projection-checkpoint flush, saga terminal, tombstone-reap compaction, cross-shard merge) are deliberately excluded so the histogram stays scoped to the per-write pipeline. |
| `orleans.lattice.leaf.commit.in_flight` | `Histogram<int>` | `{commit}` | Concurrent foreground commits in flight on a single leaf at the moment a new commit enters the commit path. Tagged `tree` only (no shard tag - leaves are routed per-key so the same leaf grain can be addressed by multiple shards under online reshard). A flat-zero series indicates the leaf never serialises overlapping commits (its `[AlwaysInterleave]` mutator surface keeps the turn token released); a sustained tail tracks contention from saga-driven prepare arrivals or producer-side fan-in. |
| `orleans.lattice.leaf.scan.duration` | `Histogram<double>` | `ms` | Duration of leaf-level range scans. Tagged `operation=keys` (from `GetKeysAsync`) or `operation=entries` (from `GetEntriesAsync`). |
| `orleans.lattice.leaf.compaction.duration` | `Histogram<double>` | `ms` | Duration of `CompactTombstonesAsync` on a single leaf. |
| `orleans.lattice.leaf.tombstones.reaped` | `Counter<long>` | `{tombstone}` | Tombstones (from explicit `DeleteAsync` / `DeleteRangeAsync`) permanently removed by compaction. |
| `orleans.lattice.leaf.tombstones.created` | `Counter<long>` | `{tombstone}` | Tombstones newly written by `DeleteAsync` (1) or `DeleteRangeAsync` (N). |
| `orleans.lattice.leaf.tombstones.expired` | `Counter<long>` | `{tombstone}` | Live entries reaped by compaction because their per-entry TTL (set via `SetAsync(key, value, TimeSpan)`) elapsed past the configured grace period. Separate from `reaped` so operators can distinguish TTL churn from explicit-delete throughput. |
| `orleans.lattice.compaction.pass.duration` | `Histogram<double>` | `ms` | End-to-end `RunCompactionPassAsync` wall-clock duration. Tagged `tree`, `trigger=reminder` (baseline reminder-driven sweep), `trigger=ratio` (policy trigger fired by `MinTombstoneRatioForCompaction`), `trigger=size` (policy trigger fired by `MaxLeafEntriesBeforeForcedCompaction`), or `trigger=operator` (out-of-cycle pass requested via `ILattice.CompactShardAsync`). |
| `orleans.lattice.compaction.leaves.visited` | `Counter<long>` | `{leaf}` | Per-leaf compaction outcome. Tagged `tree`, `outcome=reaped` (at least one tombstone physically removed) or `outcome=noop` (leaf had pending tombstones still inside the grace window), `path=walk` (legacy chain walk) or `path=dirty-set` (dirty-leaves fast path) when a path is in scope, and - when a policy-trigger pass is in flight - `trigger=reminder|ratio|size|operator`. |
| `orleans.lattice.compaction.shard.retries` | `Counter<long>` | `{retry}` | Coordinator-level transient retries during `RunCompactionPassAsync`. A non-zero rate that does not lead to skips is healthy resilience; a sustained rate is worth investigating. |
| `orleans.lattice.compaction.shard.skipped` | `Counter<long>` | `{shard}` | Shards the coordinator gave up on after exhausting retries. **Any non-zero rate is alert-worthy** - the affected shard's tombstones will not be reaped until the next pass. |
| `orleans.lattice.compaction.shard.dirty_leaves` | `Histogram<long>` | `{leaf}` | Per-shard dirty-leaf snapshot size at the moment the compaction coordinator enters a shard. Tagged `tree`. A flat-zero series indicates the dirty-leaves fast path saw nothing to do and the coordinator fell back to the legacy chain walk for that shard. |
| `orleans.lattice.leaf.tombstone.ratio` | `Histogram<double>` | `{ratio}` | Tombstone-to-live-entry ratio sampled per mutation when `MinTombstoneRatioForCompaction` is enabled. The p95 is the headroom signal for the ratio policy threshold. |
| `orleans.lattice.leaf.splits` | `Counter<long>` | `{split}` | Leaf-node splits triggered by `MaxLeafKeys` overflow. |
| `orleans.lattice.leaf.digest.publishes` | `Counter<long>` | `{publish}` | Leaf-side projection-digest publish decisions, partitioned by path. Tagged `tree` and `path`, where `path` is one of: `coalesced_scheduled` (a fresh one-shot timer was registered - first dirtying mutation inside a new coalescing window), `coalesced_skipped` (a dirtying mutation arrived while a coalesced publish was already pending, so the cross-grain hop was deferred onto the existing window - this is the `publishes saved` surface that justifies the coalescing default), `coalesced_fired` (the coalescing timer tick issued the cross-grain `OnChildDigestPublishedAsync` RPC - one per window per leaf unless an inline publish or graceful flush cancelled the timer first), `inline` (the leaf issued the cross-grain publish synchronously - either because `DigestCoalescingWindowMs` is zero, the timer registration failed in a test harness, or a structural caller routed through `PublishDigestUpwardInlineAsync`), or `deactivation_flush` (the leaf's graceful `OnDeactivateAsync` drained a pending coalesced publish before the activation tore down). The headline coalescing invariant - `N writes inside one window produce one cross-grain hop` - translates to `coalesced_scheduled + coalesced_fired` per window regardless of write count, with `coalesced_skipped` absorbing the remaining `N - 1` dirtying mutations. A regression to the c2-xxix shape (resolver dropping `DigestCoalescingWindowMs` on the floor) shows up as zero `coalesced_*` increments and `inline` growing in lockstep with writes - the propagation-guard regression gate catches the property-level drop, this counter catches the behaviour-level drop. |
| `orleans.lattice.leaf.replay.duration` | `Histogram<double>` | `ms` | Activation-time leaf-projection replay duration. Tagged `outcome=tail` (caught up by replaying the slice `(checkpoint, head]`), `outcome=snapshot_then_wal` (a fall-off-log trigger fired and the snapshot-then-WAL recovery path was taken), or `outcome=full_rebuild` (`ProjectionRebuildPolicy.FullRebuildFromWal`). Emitted on every activation that performs WAL replay. See [Projection Rebuild](projection-rebuild.md) and [Write-Ahead Log](wal.md). |
| `orleans.lattice.leaf.replay.entries` | `Counter<long>` | `{entry}` | Mutations seen by activation-time leaf-projection replay. Tagged `outcome=applied` (fed to `ILeafProjection.Apply`) or `outcome=skipped` (filtered by the leaf's key-range responsibility before reaching `Apply`). |
| `orleans.lattice.leaf.activation_replays` | `Counter<long>` | `{replay}` | Activation-time leaf-materialiser replays started, counted once per activation that begins a replay. Tagged `tree`. |
| `orleans.lattice.leaf.activation_replays_over_budget` | `Counter<long>` | `{replay}` | Activation-time leaf replays that exceeded `MaxLeafReplayEntries` (or `LeafProjectionRetention`) while the WAL still covered the whole needed window. These converge normally - they are only longer than the budget anticipated - so this is a capacity signal, not a fault. Tagged `tree`. |
| `orleans.lattice.leaf.activation_cursor_publish_failures` | `Counter<long>` | `{failure}` | Activation-time eager cursor-publish failures, emitted when the post-replay cursor publish fails during a leaf activation. Tagged `tree`. |
| `orleans.lattice.materialiser.drain_lag` | `Histogram<double>` | `ms` | Leaf-materialiser drain lag, in milliseconds, sampled by the per-tree WAL garbage-collection pass. Tagged `tree`. |
| `orleans.lattice.materialiser.pin.durable_writes` | `Counter<long>` | `{write}` | Durable writes to the leaf-materialiser pin store. Tagged by birth / coalesced outcome. |

### Snapshot cursors (sourced from `SnapshotLeafGrain` / snapshot-cursor open path)

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.snapshot.replay.duration` | `Histogram<double>` | `ms` | Per-shard WAL-replay duration observed during a zero-observable-writes snapshot-leaf open. Emitted by `SnapshotLeafGrain` after a successful replay over `[0, capturedOffset)`. Tagged `tree` and `shard`. |
| `orleans.lattice.snapshot.replay.entries` | `Counter<long>` | `{entry}` | WAL entries consumed by the snapshot-leaf replay engine; one increment per `CommitLogSliceEntry` processed (including filtered / skipped records, because they contribute to wall-clock replay cost). Tagged `tree` and `shard`. |
| `orleans.lattice.snapshot.pins` | `UpDownCounter<long>` | `{pin}` | Live WAL-retention pins held by open snapshot cursors. Incremented on `OpenSnapshotKeyCursorAsync` / `OpenSnapshotEntryCursorAsync` after a successful pin report; decremented on close or idle-TTL eviction. Tagged `tree`. |

### WAL garbage collector (sourced from `LatticeWalGc`)

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.wal.entries_trimmed` | `Counter<long>` | `{entry}` | WAL entries removed by the per-tree garbage collector. Emitted only after a pass that trims at least one entry (a zero-trim pass produces no measurement). Tagged `tree`. |
| `orleans.lattice.storage.wal_bytes` | `ObservableGauge<long>` | `By` | Retained WAL bytes for the tree. Observed lazily from the per-tree storage-usage aggregator's last-known report (coalesced behind `StorageUsageCacheTtl`); a tree on a provider that does not support byte accounting reports no data rather than `0`. Tagged `tree`. |
| `orleans.lattice.storage.snapshot_bytes` | `ObservableGauge<long>` | `By` | Snapshot blob bytes for the tree. Tagged `tree`. |
| `orleans.lattice.storage.leaf_state_bytes` | `ObservableGauge<long>` | `By` | Summed leaf/shard-root state bytes for the tree. Tagged `tree`. |
| `orleans.lattice.storage.total_bytes` | `ObservableGauge<long>` | `By` | Sum of the three storage surfaces (`wal_bytes` + `snapshot_bytes` + `leaf_state_bytes`) for the tree. Tagged `tree`. |
| `orleans.lattice.storage.policy.over_threshold` | `ObservableGauge<long>` | `{tree}` | `1` when the tree's retained WAL bytes currently breach the advisory ceiling (`LatticeOptions.WalMaxRetainedBytes`), else `0`. Advisory only - the byte-pressure policy never trims past the safe consumer frontier, so the gauge stays at `1` while a lagging consumer pins the bytes. Tagged `tree`. |
| `orleans.lattice.storage.policy.trim_triggered` | `Counter<long>` | `{trim}` | Incremented once per WAL GC pass whose pre-trim retained bytes exceed the advisory ceiling, so the policy schedules a byte-pressure trim. Tagged `tree` and `reason` (`byte_pressure`). Not emitted when the policy is disabled or the WAL provider does not support byte accounting. |
| `orleans.lattice.storage.policy.bytes_reclaimed` | `Counter<long>` | `By` | WAL bytes freed by a byte-pressure-triggered trim pass (pre-trim minus post-trim retained bytes). Zero-reclaim passes (a lagging consumer pinned every byte) do not emit. Tagged `tree`. |
| `orleans.lattice.storage.wal.uncompressed_bytes` | `Counter<long>` | `By` | Pre-compression encoded WAL payload bytes a compressing Azure Table WAL provider committed for the tree, summed once per append batch. Emitted only by a provider configured with row compression; a non-compressing provider produces no measurement. Tagged `tree`. |
| `orleans.lattice.storage.wal.stored_bytes` | `Counter<long>` | `By` | Post-compression WAL payload bytes actually stored for the tree, summed once per append batch. Dividing this by `uncompressed_bytes` gives the realised compression ratio; `1 -` that ratio is the savings. Tagged `tree`. |
| `orleans.lattice.storage.wal.compression_skipped` | `Counter<long>` | `{row}` | WAL rows the compressing provider stored verbatim instead of compressed. Tagged `tree` and `reason`: `below_threshold` (payload shorter than `CompressionMinPayloadBytes`), `inflation_guard` (compressing did not shrink the payload), or `disabled` (compression off for the row). |

### Per-tree admission control

Opt-in, fail-open per-tree quota enforcement. These per-tree instruments are on the `orleans.lattice` meter and tagged `tree`; the two counters (`admission.rejected`, `admission.would_reject`) and the `admission.utilization` gauge additionally carry a low-cardinality `dimension` tag whose value is `keys` or `bytes`. The observable gauges are observed lazily from the same cached, TTL-coalesced per-tree aggregate that backs the storage-usage gauges, so they cost nothing on the write path; they populate once the per-tree storage-usage aggregator has sampled the tree (which happens on any `GetStorageUsageAsync` call, whenever `StorageUsageDeepPollInterval` is enabled, and automatically whenever a cap or advisory ceiling is configured, because the write guard refreshes the aggregate at most once per `StorageUsageCacheTtl`).

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.admission.live_keys` | `ObservableGauge<long>` | `{key}` | Current live (non-tombstone) key count for the tree - the figure compared against `LatticeOptions.MaxLiveKeys`. Best-effort / eventually-consistent (assembled from each shard root's incrementally-maintained live-key total, re-anchored on a deep refresh); a time-expired entry not yet reaped by compaction counts as live until the next re-anchor. Tagged `tree`. |
| `orleans.lattice.admission.estimated_bytes` | `ObservableGauge<long>` | `By` | Current estimated retained bytes for the tree - the figure compared against `LatticeOptions.MaxEstimatedBytes`. Aliases `storage.total_bytes` (WAL rows + snapshot blobs + leaf/shard-root state). Tagged `tree`. |
| `orleans.lattice.admission.over_advisory` | `ObservableGauge<long>` | `{tree}` | `1` when the tree currently exceeds an advisory ceiling (`AdmissionAdvisoryLiveKeys` or `AdmissionAdvisoryBytes`), else `0`. Emitted only for a tree that has set at least one advisory ceiling. Non-enforcing: no write is rejected. Tagged `tree`. |
| `orleans.lattice.admission.would_reject` | `Counter<long>` | `{write}` | Incremented once per write that *would* have been rejected at an advisory ceiling - the dry-run blast radius of a candidate cap. No write is rejected. Tagged `tree` and `dimension` (`keys` or `bytes`). |
| `orleans.lattice.admission.utilization` | `ObservableGauge<double>` | ratio | Current / ceiling utilisation per dimension. The denominator prefers the enforcing cap (`MaxLiveKeys` / `MaxEstimatedBytes`) and falls back to the advisory ceiling when only that is set; a series is emitted only for a dimension whose ceiling is configured. At or above `1` the enforcing cap is being hit. Tagged `tree` and `dimension` (`keys` or `bytes`). |
| `orleans.lattice.admission.rejected` | `Counter<long>` | `{write}` | Incremented once per write actually rejected by an enforced cap (one `LatticeQuotaExceededException` thrown). Tagged `tree` and `dimension` (`keys` or `bytes`). |

**Semantics.** Admission control is strictly opt-in: every cap and ceiling defaults to `null` (unbounded), and a tree with none set pays nothing and never takes a grain hop on the write path. The cap is enforced against the cached per-tree aggregate, never a per-write fan-out, so it is **best-effort and approximate**: concurrent cross-shard writes can overshoot a cap slightly before the aggregate refreshes, and enforcement **fails open** (never rejects) until the tree's first sample lands after activation. Replication and atomic-write-saga apply paths bypass enforcement, so an incoming replicated write is never rejected. On an enforcing breach the write throws `LatticeQuotaExceededException` (dimension `keys` or `bytes`, with the observed `Current` and configured `Limit`), a recoverable back-off signal.

**Advisory-first-then-enforce adoption workflow.**

1. **Observe.** With no cap set, watch `admission.live_keys` and `admission.estimated_bytes` (enable `StorageUsageDeepPollInterval` so the gauges populate without a cap configured) to learn each tree's steady-state footprint.
2. **Set an advisory ceiling.** Configure `AdmissionAdvisoryLiveKeys` and/or `AdmissionAdvisoryBytes` at a candidate ceiling. This rejects nothing.
3. **Right-size.** Watch `admission.over_advisory` (which trees sit over the candidate ceiling) and the `admission.would_reject` rate (how many writes a promoted cap would reject, by dimension) to tune the ceiling until the dry-run blast radius is acceptable.
4. **Promote and enforce.** Move the tuned value to `MaxLiveKeys` and/or `MaxEstimatedBytes`. Watch `admission.utilization` (how close each tree runs to its cap) and the `admission.rejected` rate (writes actually throttled) to confirm enforcement behaves as intended.

### Compression dictionary auto-training

These instruments are emitted only when auto-training of the Zstandard compression dictionary is enabled. They expose training cadence and the realised compression of the most recent training probe.

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.compress.dictionary.training_runs` | `Counter<long>` | `{run}` | Auto-training dictionary pass attempts. Tagged `outcome` (`trained`, `skipped_insufficient_samples`, or `skipped_cadence`). |
| `orleans.lattice.compress.dictionary.trained_bytes_in` | `Counter<long>` | `By` | No-dictionary (plain Zstd) baseline compressed bytes of the training probe, summed per successful auto-training pass. |
| `orleans.lattice.compress.dictionary.trained_bytes_out` | `Counter<long>` | `By` | Trained-dictionary compressed bytes of the training probe, summed per successful auto-training pass. Dividing this by `trained_bytes_in` gives the realised dictionary compression ratio. |
| `orleans.lattice.compress.dictionary.active_version` | `ObservableGauge<long>` | `{version}` | Currently active auto-trained dictionary id (`0` = none trained yet). |
| `orleans.lattice.compress.dictionary.reservoir_fill` | `ObservableGauge<long>` | - | Auto-training reservoir occupancy. Tagged `kind` (`samples` or `bytes`). |

### Cache (sourced from `LeafCacheGrain`)

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.cache.hits` | `Counter<long>` | `{hit}` | `LeafCacheGrain` reads served by a live, cached entry. |
| `orleans.lattice.cache.misses` | `Counter<long>` | `{miss}` | `LeafCacheGrain` reads that did not find a live cached entry. |

### Foreground write envelopes (sourced from `LatticeGrain`)

The foreground write paths (`SetAsync`, `SetManyAsync`) are wrapped at the
caller-visible boundary with envelope + per-sub-stage histograms so the
end-to-end per-call wall-clock cost can be attributed to its constituent
spans without per-call wire-format change. Pair these with the per-leaf
`leaf.commit.duration` and per-WAL `wal.*` instruments to walk the full
path from `ILattice` entry through Orleans RPC down to the storage
provider.

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.set.duration` | `Histogram<double>` | `ms` | End-to-end caller-visible wall-clock duration of one `LatticeGrain.SetAsync` call. Tagged `tree`. |
| `orleans.lattice.set.stage.duration` | `Histogram<double>` | `ms` | Per-sub-stage wall-clock duration of one `LatticeGrain.SetAsync` call. Tagged `tree` and `stage=gate` (atomic-batch gate), `route` (shard routing), `shard` (the cross-grain `ShardRootGrain` RPC envelope - this is the dominant cell at the c2-iii operating point), or `publish` (event-stream dispatch). |
| `orleans.lattice.set_many.duration` | `Histogram<double>` | `ms` | End-to-end caller-visible wall-clock duration of one `LatticeGrain.SetManyAsync` call. Tagged `tree`. |
| `orleans.lattice.set_many.stage.duration` | `Histogram<double>` | `ms` | Per-sub-stage wall-clock duration of one `LatticeGrain.SetManyAsync` call. Tagged `tree` and `stage=gate` (atomic-batch gate), `route` (per-entry routing), `bucket` (per-shard bucket build), `fanout` (the cross-shard `Task.WhenAll` - the dominant cell under saturated traffic), or `events` (event-stream dispatch). |

### Foreground read envelopes (sourced from `LatticeGrain`)

The foreground read paths (`GetAsync`, `GetManyAsync`, `ExistsAsync`,
`GetWithVersionAsync`) are wrapped at the caller-visible boundary with
envelope histograms (and, for the point and batched reads, per-sub-stage
decomposition) so an `ILattice` consumer can measure true per-call read
latency without falling back to the silo's per-batch ingest envelope. Pair
these with the per-leaf `leaf.scan.duration` (range scans) and per-shard
`shard.reads` counter (count, not latency) for a complete picture of the
read pipeline.

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.get.duration` | `Histogram<double>` | `ms` | End-to-end caller-visible wall-clock duration of one `LatticeGrain.GetAsync` call (includes routing resolution, the shard RPC, and any stale-routing retries). Tagged `tree`. |
| `orleans.lattice.get.stage.duration` | `Histogram<double>` | `ms` | Per-sub-stage wall-clock duration of one `LatticeGrain.GetAsync` call. Tagged `tree` and `stage=route` (per-attempt `GetShardGrainAsync` resolution) or `shard` (per-attempt `IShardRootGrain.GetAsync` RPC). Under a stale-routing storm a single envelope produces multiple `route` and `shard` observations so the histograms attribute the retry cost. |
| `orleans.lattice.get_many.duration` | `Histogram<double>` | `ms` | End-to-end caller-visible wall-clock duration of one `LatticeGrain.GetManyAsync` call (includes routing, per-key bucketing, per-shard parallel fan-out, the registry-snapshot double-check, and any stale-routing retries). Tagged `tree`. |
| `orleans.lattice.get_many.stage.duration` | `Histogram<double>` | `ms` | Per-sub-stage wall-clock duration of one `LatticeGrain.GetManyAsync` call. Tagged `tree` and `stage=route` (`GetRoutingAsync` fetch), `bucket` (per-key shard bucketing), `fanout` (registry snapshot + cross-shard `Task.WhenAll`), or `merge` (topology- and snap2-stability checks + final dictionary materialise). Recorded once per stage per inner snapshot-retry attempt so a snapshot- or topology-retry storm is visible as a multiplicative bump on the same envelope. |
| `orleans.lattice.exists.duration` | `Histogram<double>` | `ms` | End-to-end caller-visible wall-clock duration of one `LatticeGrain.ExistsAsync` call. Tagged `tree`. Lower-traffic than `get.duration` in typical workloads but published for symmetry so a dashboard tile can confirm probe activity. |
| `orleans.lattice.get_with_version.duration` | `Histogram<double>` | `ms` | End-to-end caller-visible wall-clock duration of one `LatticeGrain.GetWithVersionAsync` call. Tagged `tree`. Lower-traffic than `get.duration` in typical workloads but published for symmetry so a dashboard tile can confirm versioned-read activity. |

### Shard-root `SetManyAsync` decomposition (sourced from `ShardRootGrain`)

Inside the `LatticeGrain.SetManyAsync` `stage=fanout` span, every shard runs
its own `ShardRootGrain.SetManyAsync` slice. The instruments below split
that per-shard slice into the local-apply work, the online-resize
shadow-forward path, and the per-leaf RPC fan-out. Together with
`leaf.commit.duration` per step, this gives an end-to-end attribution from
the lattice grain boundary down to the leaf commit pipeline.

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.shard_root.set_many.local_apply.duration` | `Histogram<double>` | `ms` | Wall-clock ms inside the local-apply path of `ShardRootGrain.SetManyAsync`: from receipt of a per-shard slice until every per-leaf `SetManyAsync` dispatched by `SetManyLocalOnlyAsync` returns. Tagged `tree` and `shard`. Includes per-leaf RPC scheduling, leaf turn-queue wait, leaf commit, WAL append, and WAL phase-2 commit. Excludes the lattice-grain's bucket build (covered by `set_many.stage`) and excludes the online-resize shadow-forward path (covered separately below). |
| `orleans.lattice.shard_root.set_many.shadow_forward.duration` | `Histogram<double>` | `ms` | Wall-clock ms inside the online-resize shadow-forward path of `ShardRootGrain.SetManyAsync`, when the shard's slice is concurrently forwarded to a destination shard during an adaptive split. Tagged `tree` and `shard`. Zero on shards not currently splitting. |
| `orleans.lattice.shard_root.set_many.leaf_rpc.duration` | `Histogram<double>` | `ms` | Per-leaf wall-clock ms inside one `IBPlusLeafGrain.SetManyAsync` RPC dispatched by the shard-root local-apply fan-out. Tagged `tree` and `shard`. The gap between this and `shard_root.set_many.local_apply.duration` is the max-of-N tail across the parallel per-leaf calls. |
| `orleans.lattice.shard_root.forward.timeouts` | `Counter<long>` | `{timeout}` | Count of outbound shard-to-shard write forwards (the online-resize shadow forward and the adaptive-split migration forward) abandoned after exceeding `ShardForwardTimeout`. Tagged `tree`. A non-zero value indicates a forward parked against a sibling shard whose ownership was changing during a reshard swap; the forward was faulted as a `TimeoutException` so the foreground write pipeline could make progress and the operation be retried against refreshed routing. Expected to be zero in steady state. |
| `orleans.lattice.shard_root.activation_ready.timeouts` | `Counter<long>` | `{timeout}` | Count of `ShardRootGrain` activation-readiness seeds abandoned after exceeding `ActivationReadyTimeout`. Tagged `tree`. A non-zero value indicates a first-activation seed (registry registration or root-leaf init) parked - typically because a startup reshard or membership change left the target activation not-yet-visible - and was faulted as a `TimeoutException` so the held activation gate could release and the foreground write pipeline make progress, with the seed retried against refreshed routing. Expected to be zero in steady state. |
| `orleans.lattice.internal.digest_publish.timeouts` | `Counter<long>` | `{timeout}` | Count of internal-node upward digest publishes (the `ChildDigestSnapshot` propagation from a `BPlusInternalGrain` to its parent) abandoned after exceeding `DigestPublishTimeout`. Tagged `tree`. A non-zero value indicates a publish parked against a parent internal node that was mid-mutation; the publish was faulted as a `TimeoutException` so the holding turn released the non-reentrant split gate instead of pinning it. The digest is staleness-tolerant, so the next mutation's publish re-drives convergence. Expected to be zero in steady state. |
| `orleans.lattice.wal.append_dispatch.timeouts` | `Counter<long>` | `{timeout}` | Count of writer-side WAL shard dispatches (`WalCommitLogWriter.AppendForPartitionAsync` / `AppendAsync`) abandoned after exceeding `WalAppendDispatchTimeout`. Tagged `tree` and `shard`. The dispatch is the writer-side cross-grain RPC into the per-shard WAL grain; it was historically unbounded on the writer side, so a wedged shard activation would hold every caller's dispatch parked until the Orleans response deadline (default 3 minutes) expired. A non-zero value attributes the wedge to a specific `(tree, shard)` pair in O(`WalAppendDispatchTimeout`) time rather than O(response timeout) time, and the parked dispatch is faulted as a `TimeoutException` so the request pipeline releases its slot. Sustained non-zero counts on a specific `(tree, shard)` identify the wedged shard for follow-up investigation. Expected to be zero in steady state. |
| `orleans.lattice.wal.flush.preflight.timeouts` | `Counter<long>` | `{timeout}` | Count of per-shard WAL `FlushAsync` preflight regions (the synchronous setup and initial scheduler yield that precede the bounded provider call) abandoned after exceeding `WalFlushPreflightTimeout`. Tagged `tree` and `shard`. The preflight region is normally microseconds; a non-zero count indicates the activation's grain scheduler did not resume the flush's post-yield continuation within the deadline, leaving the in-flight slot pinned with no provider-call deadline armed (`WalFlushTimeout` only covers the provider call itself, which has not yet been issued). The faulted preflight surfaces as a `TimeoutException` routed through the normal failure handler, the slot drains, and this counter attributes the trip. Sustained non-zero counts indicate the activation's scheduler is being held by a startup reshard / membership change, a non-cooperative work item, or a mid-flush activation tear-down. Expected to be zero in steady state. |
| `orleans.lattice.wal.shard.deactivate.in_flight` | `Histogram<long>` | `{slot}` | Per-WAL-shard in-flight slot count observed at `OnDeactivateAsync` time. Tagged `tree` and `shard`. Recorded exactly once per `OnDeactivateAsync` call. A zero observation is the healthy steady-state shape (the grain drained cleanly); a non-zero observation means the activation was torn down with in-flight flushes still pending, the slot population that defines the post-#568 residual phase-1/activation wedge fingerprint. Combined with `orleans.lattice.wal.flush.preflight.timeouts`, a deactivation with non-zero in-flight count immediately followed by a preflight timeout on a successor activation is the smoking gun for the "mid-call deactivation orphan" hypothesis. |
| `orleans.lattice.wal.shard.drain.budget.expirations` | `Counter<long>` | `{expiration}` | Count of per-shard `WalShardGrain` deactivation drains that exceeded `WalDrainBudget` and had to force-fault one or more in-flight slots so the activation could finish tearing down. Tagged `tree` and `shard`. Reliability intent: under a saturating-storage-account wedge, the provider call's await can park behind an SDK retry loop in pre-attempt back-off where the per-flush `WalFlushTimeout` deadline does not fire promptly (the SDK observes cancellation only between attempts, not during back-off), so a chain with N in-flight slots could otherwise hold the deactivation indefinitely. With the drain budget the deactivation force-faults any slot that has not unlinked within the deadline; this counter names the wedged shard so operators can attribute the trip without source-walking the silo log. Zero on a healthy drain; any non-zero rate identifies a shard whose provider call could not be cancelled inside the drain budget. |
| `orleans.lattice.wal.shard.drain.budget.force_faulted_slots` | `Histogram<long>` | `{slot}` | Per-WAL-shard in-flight slot count force-faulted by a deactivation drain after `WalDrainBudget` expired. Tagged `tree` and `shard`. Recorded exactly once per drain that hit the budget; the value is the number of slots that had not unlinked when the budget fired and were force-faulted to release the activation. Only fires on the `wal.shard.drain.budget.expirations` path, so the histogram's count and the counter's count are the same number. |
| `orleans.lattice.wal.shard.start_flush.calls` | `Counter<long>` | `{call}` | Count of `WalShardGrain.StartFlush` invocations per `(tree, shard)`. Incremented once at the top of every `StartFlush` call, including the follow-on flushes a completing flush kicks off. Tagged `tree` and `shard`. Diagnostic intent: under the residual phase-1/activation WAL wedge, if `start_flush.calls` keeps incrementing throughout the wedge then new flushes ARE being kicked off, so the wedge is a slot-leak in the in-flight chain's `finally` (slots never removed even after the flush's task settles); if `start_flush.calls` stops incrementing during the wedge then the cap-cutover loop in `AppendBatchAsync` is itself blocked and no new flush ever kicks off. |
| `orleans.lattice.wal.shard.pending_segments` | `Histogram<long>` | `{segment}` | Per-WAL-shard `_pendingSegments.Count` observed at every `StartFlush` entry, sampled *before* the pending list is captured into the new in-flight slot. Tagged `tree` and `shard`. Diagnostic intent: under the wedge, a growing distribution indicates callers are still arriving and enqueueing into `_pendingSegments` even though the chain cannot drain (back-pressure absorbing everything but never releasing). A stuck-at-zero distribution combined with a `start_flush.calls` trickle indicates the cap-cutover loop blocked itself; combined with a healthy `start_flush.calls` rate it indicates the wedge is downstream of the flush kick-off. |
| `orleans.lattice.shard_root.reshard.initiated` | `Counter<long>` | `{reshard}` | Count of `TreeReshardGrain.ReshardAsync` invocations that progressed past argument / interlock validation and started a reshard coordinator (or, for the empty-tree fast path, atomically updated the registry pin). Tagged `tree`. Diagnostic intent: the residual WAL wedge is correlated with the `reshard ... REJECTED (Forwarding failed)` log storm; pairing this counter with `reshard.completed` and `reshard.rejected` lets a dashboard correlate reshard activity with wedge onset directly without grepping a rotated silo log. Note: Orleans-side message-routing rejections ("Forwarding failed") are emitted by Orleans's own router and are not captured here - they remain log-only until a separate diagnostic source is added. |
| `orleans.lattice.shard_root.reshard.rejected` | `Counter<long>` | `{rejection}` | Count of `TreeReshardGrain.ReshardAsync` invocations rejected at the Lattice layer before starting a coordinator. Tagged `tree` and `reason`, where `reason` enumerates the rejection class (`argument_out_of_range_min`, `argument_out_of_range_max`, `already_in_progress`, `shrink_unsupported`, `resize_in_flight`, `state_write_failed`). Excludes Orleans-side message-routing rejections; see `reshard.initiated`. |
| `orleans.lattice.shard_root.reshard.completed` | `Counter<long>` | `{reshard}` | Count of `TreeReshardGrain` coordinator completions that reached the terminal phase successfully (and the empty-tree fast-path reshards, counted in lockstep with `reshard.initiated`). Tagged `tree`. The difference between this and `reshard.initiated` over a window is the number of reshards still in flight or that failed mid-coordinator. |
| `orleans.lattice.shard_root.reshard.in_flight` | `Histogram<long>` | `{reshard}` | Per-tree reshard in-flight state observation, emitted at every `ReshardAsync` entry as either `0` (idle) or `1` (a reshard is already in progress for this tree). Tagged `tree`. A non-zero observation immediately preceding wedge onset is the same signal a periodically-polled gauge would provide. |
| `orleans.lattice.wal.writer.append.dispatched` | `Counter<long>` | `{dispatch}` | Count of `WalCommitLogWriter` per-partition append dispatches that started (incremented at the `Enqueued` lifecycle stamp, before the shard RPC is invoked). Tagged `tree` and `partition`. Diagnostic intent: the writer-layer kick-off signal for the saturation-rung WAL wedge's dominant mode (5 of 7 wedged cohorts on 2026-06-03), where every shard's in-flight chain is empty yet hundreds of callers are parked in `WalCommitLogWriter.AppendForPartitionAsync`. A sustained dispatched rate combined with stale `wal.writer.partition.pending_appends` p99 readings localises the stall to the awaited shard-grain RPC; a collapse to zero ranges the wedge upstream of the writer itself. |
| `orleans.lattice.wal.writer.partition.pending_appends` | `Histogram<long>` | `{dispatch}` | Per-writer-partition pending-append-dispatch depth observed at every `WalCommitLogWriter` append entry, sampled *before* the new pending stamp is linked into the partition's tracker. Tagged `tree` and `partition`. Diagnostic intent: a growing distribution under the wedge confirms the writer is the choke (callers enqueuing into a tracker that cannot drain); a stuck-at-zero distribution combined with sustained `wal.writer.append.dispatched` rules out a writer-layer dispatch lifecycle stall and points the next bisect downstream of the `SentToShard` stage. Mirrors `wal.shard.pending_segments` one layer up. |
| `orleans.lattice.wal.writer.append.admission_timeouts` | `Counter<long>` | `{timeout}` | Count of `WalCommitLogWriter` append dispatches whose per-partition admission wait exceeded `WalAppendDispatchTimeout`. Tagged `tree` and `partition`. Reliability intent: the per-partition admission semaphore caps `PartitionTracker._inFlight` depth at `WalMaxPendingBatches`, mirroring the shard-side ceiling. When the shard cannot drain, callers awaiting an admission slot are released with a typed `TimeoutException` at the deadline rather than silently parking forever in an unbounded writer queue. A non-zero counter under steady-state operation is the signal that the offered rate exceeds the shard's drain rate - the saturation regime previously hidden as a silent wedge. Pair with `wal.writer.append.admission_wait` to distinguish back-pressured-cleanly (wait p99 elevated, zero timeouts) from back-pressure-exceeded-deadline (non-zero timeouts). |
| `orleans.lattice.wal.writer.append.admission_wait` | `Histogram<double>` | `ms` | Wall-clock ms a `WalCommitLogWriter` dispatch waited for a per-partition admission slot before linking a new `PendingAppend` stamp. Tagged `tree` and `partition`. Reliability intent: under healthy operation this histogram sits at the floor (a sub-microsecond uncontended semaphore acquire). A spreading distribution indicates the per-partition tracker is approaching its `WalMaxPendingBatches` ceiling, surfacing back-pressure as an honest tail-latency signal long before any caller hits the `wal.writer.append.admission_timeouts` deadline. Recorded for every dispatch that successfully acquired a slot (timed-out dispatches feed the counter only). |
| `orleans.lattice.wal.writer.append.drain.releases` | `Counter<long>` | `{release}` | Count of writer-side parked admission callers released by a silo-drain signal on host shutdown. Tagged `tree` and `partition`. One sample per parked caller faulted out of `PartitionTracker.AcquireAsync` when the owning `WalCommitLogWriter` drains on host shutdown; zero on a healthy shutdown that has no parked callers. Distinct from `wal.writer.append.admission_timeouts` (per-call deadline expiries during steady-state operation) and from `wal.shard.drain.budget.expirations` (shard-grain deactivation drains that had to force-fault). This counter names writer-side parked callers released by the silo's drain on shutdown - the surface that closes the writer-admission-semaphore-wedged-at-SIGTERM phenotype documented in `benchmark/azure-throughput/throughput.md` section 32.6. A non-zero rate on shutdown is normal when the silo was under storage saturation at drain entry; a non-zero rate during steady-state operation indicates the drain hook fired spuriously and is a regression signal. Per-silo: each silo process emits its own samples for the trackers its `WalCommitLogWriter` owns. |
| `orleans.lattice.wal.saturation.state` | `ObservableGauge<long>` | `{state}` | Current per-tree WAL saturation regime as an ordinal step function: `0` = Healthy, `1` = Throttled, `2` = Saturated. Tagged `tree` only - the ordinal value already encodes the regime, so the state is deliberately not also carried as a label (doing so fragmented the per-tree series on every transition and left the prior state's series lingering at its last elevated value under scrape staleness, making a recovered tree read as Saturated). Published by the silo-scoped `WalSaturationSampler`; a tree contributes a measurement only after the sampler has observed at least one signal for it, so an unwritten tree does not appear in the series. Pair with `wal.saturation.transitions` (which carries the per-state breakdown) to plot regime flips alongside the current regime. See [WAL Saturation Signal](wal-saturation-signal.md). |
| `orleans.lattice.wal.saturation.transitions` | `Counter<long>` | `{transition}` | Count of per-tree saturation-state transitions observed by the silo-scoped sampler. Tagged `tree`, `state` (new state, lowercased), `previous_state` (lowercased), and optionally `partition` (admission-depth-driven transitions) or `shard` (dispatch-timeout-driven or provider-failure-rate-driven transitions) when a single source dominated. A flat-zero series is the healthy steady state. A rising rate of `state=throttled` is the leading edge of a saturation episode; `state=saturated` is the regime itself. Flapping between Throttled and Saturated is a different operational signal from a sustained Saturated. |

### WAL append pipeline (sourced from `WalShardGrain` and `WalCommitLogWriter`)

The WAL partition grains expose per-flush instruments that distinguish
grain-side scheduling cost from storage-provider cost; every histogram
carries the Phase A attribution tags `wal_partitions` and
`wal_max_pending_batches` so cross-configuration comparisons are direct.

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.wal.shard.dispatch.duration` | `Histogram<double>` | `ms` | Caller-side wall-clock duration of the cross-grain `IWalShardGrain.AppendAsync` / `AppendBatchAsync` RPC, observed by `WalCommitLogWriter`. Tagged `tree`, `shard` (WAL partition index), `wal_partitions`, `wal_max_pending_batches`. Subtracting `wal.append.turn_wait` from this isolates the Orleans scheduling tax on the single WAL activation per partition. |
| `orleans.lattice.wal.shard.dispatch.entries` | `Histogram<int>` | `{entry}` | Per-dispatch entry count handed to `IWalShardGrain.AppendAsync` / `AppendBatchAsync` by `WalCommitLogWriter`. Tagged `tree`, `shard`, `wal_partitions`, `wal_max_pending_batches`. Single-key sends record `1`; batched sends record the per-partition slice size. |
| `orleans.lattice.wal.append.batch_entries` | `Histogram<int>` | `{entry}` | Per-flush packing inside the WAL grain: how many entries the grain's cutover loop accumulated into the batch that the storage provider ultimately sees. Tagged `tree`, `shard`, `wal_partitions`, `wal_max_pending_batches`. Pair with `wal.shard.dispatch.entries` to detect a missing cross-`AppendBatchAsync` coalescing window. |
| `orleans.lattice.wal.append.batch_bytes` | `Histogram<long>` | `By` | Per-flush size of the packed batch handed to the storage provider, in bytes. Tagged `tree`, `shard`, `wal_partitions`, `wal_max_pending_batches`. |
| `orleans.lattice.wal.append.in_flight` | `Histogram<int>` | `{flush}` | `_inFlight.Count` (in-flight flushes against the provider) sampled at the moment a new flush is admitted. Tagged `tree`, `shard`, `wal_partitions`, `wal_max_pending_batches`. p99 sitting at `wal_max_pending_batches - 1` indicates the cap is the binding constraint. |
| `orleans.lattice.wal.append.provider.duration` | `Histogram<double>` | `ms` | Wall-clock duration of one storage-provider flush, measured inside the WAL grain (`FlushAsync` body). Tagged `tree`, `shard`, `wal_partitions`, `wal_max_pending_batches`. This is the pure provider-RTT signal; the gap between `wal.shard.dispatch.duration` and this is everything Orleans + the grain do on top. |
| `orleans.lattice.wal.append.turn_wait` | `Histogram<double>` | `ms` | Wall-clock duration spent waiting for the WAL activation's turn token before the appending caller could enter the grain body, measured inside the WAL grain. Tagged `tree`, `shard`, `wal_partitions`, `wal_max_pending_batches`. |
| `orleans.lattice.wal.append.queue_depth` | `Histogram<int>` | `{entry}` | Number of callers parked on the WAL activation's turn-token queue sampled at the moment a new caller enters the grain body. Tagged `tree`, `shard`, `wal_partitions`, `wal_max_pending_batches`. |
| `orleans.lattice.wal.writer.append.admission_saturation_refusals` | `Counter<long>` | `{refusal}` | Writer-side admission dispatches refused with `LatticeSaturatedException` because the per-tree saturation signal stayed `Saturated` beyond `WalAdmissionSaturationWaitBudget`. Tagged `tree` and `partition` (WAL partition index). |

### Storage-provider commit pipeline

The Azure Table WAL provider (and any other WAL provider that opts in)
emits per-commit-phase histograms so the per-batch partition transaction
and the manifest partition transaction can be observed independently.
See [Write-Ahead Log](wal.md) and
[WAL Storage Providers](wal-storage-providers.md) for the underlying
two-phase commit shape.

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.provider.commit.duration` | `Histogram<double>` | `ms` | Wall-clock duration of one storage-provider commit transaction. Tagged `tree`, `shard`, `phase=phase1` (per-batch partition transaction) or `phase=phase2` (manifest partition transaction), and `pipeline_phase2` reflecting `AzureTableWalStorageOptions.PipelinePhaseTwoCommits`. The phase-2 measurement covers a single coalesced commit transaction, not the per-shard worker's whole drain loop. |
| `orleans.lattice.provider.phase2.batch_size` | `Histogram<int>` | `{commit}` | Number of coalesced phase-2 commits the per-shard provider worker bundled into a single transaction. Tagged `tree`, `shard`. A distribution concentrated near 1 means the worker is never catching up against backed-up arrivals; values closer to the per-transaction cap (~49 commits) indicate the worker is the shard's effective rate limiter. |
| `orleans.lattice.provider.retry.attempts` | `Counter<long>` | `{attempt}` | Individual retry attempts the storage-provider SDK performs on a WAL append or commit, incremented once per attempt regardless of whether the retry ultimately succeeds. Tagged `status` (the HTTP status of the response that triggered the retry, e.g. `503` / `429`, or `0` for a transport-level failure with no HTTP exchange); per-tree / per-shard correlation is intentionally omitted to keep cardinality low - use `provider.retry.exhausted` for shard-attributed failures. A non-zero rate is normal; sustained climbing rates suggest provider-side throttling that the local retry policy is masking. |
| `orleans.lattice.provider.retry.exhausted` | `Counter<long>` | `{call}` | Calls that exhausted the storage-provider retry budget and surfaced the underlying fault. Tagged `tree`, `shard`, `phase` (`phase1` per-batch partition transaction or `phase2` manifest partition transaction), and `status` (the HTTP status string the SDK observed, or `unknown`). **Any non-zero rate is alert-worthy** - the WAL append or commit failed past the SDK's retry envelope. |
| `orleans.lattice.provider.retry.short_circuited` | `Counter<long>` | `{attempt}` | Storage-provider SDK retry attempts abandoned early by the Azure Tables provider's saturation-aware retry policy (`AzureTableWalStorageOptions.HonorSaturationSignal = true`, the default). Tagged `status` (the synthetic-response HTTP status that signalled the SDK to exit the retry chain; `503` matches the policy's default short-circuit shape). Increments once per abandoned retry, including retries the policy short-circuits during the `AzureTableWalStorageOptions.SaturationShortCircuitCooldown` sticky window after the last observed `Saturated` tick. A non-zero rate confirms the policy is firing under storage-side back-pressure; a sustained climbing rate alongside `provider.retry.exhausted` indicates the storage account is shedding load faster than the SDK retry budget would otherwise surface to the writer. Always zero when `HonorSaturationSignal = false` (the historical unguarded SDK retry behaviour) or when the host supplies a pre-built `TableServiceClient` via `ServiceClient` (the host owns the pipeline in that mode). |
| `orleans.lattice.provider.idempotent_replays` | `Counter<long>` | `{call}` | Phase-one WAL batch commits whose `409 EntityAlreadyExists` conflict was proven to be an idempotent replay of an already-durable write and resolved as a success. Tagged `tree`, `shard`, and `phase` (always `phase1` - idempotent replays are only detected on the phase-one batch commit). Increments when the Azure Tables SDK retry pipeline resends a phase-one batch whose first attempt committed server-side but whose response was lost (a socket / read timeout under CPU or network pressure); the provider reads the resident rows back, confirms they are byte-identical to the batch it tried to write, and treats the conflict as success instead of failing the whole batch. A non-zero rate is **benign** (no data was lost) but is a leading indicator that the cluster is dropping phase-one responses under load - watch it alongside `provider.retry.exhausted`, not instead of it. A genuine offset collision (resident rows present but differing) is never counted here; it still surfaces as a hard failure on `provider.retry.exhausted`. |
| `orleans.lattice.provider.phase1.transient_retries` | `Counter<long>` | `{attempt}` | Phase-1 commit attempts the provider re-issued in place after a transient fault, resubmitting the byte-identical batch at the same offsets rather than faulting the calling WAL shard. Tagged `tree`, `shard`, and `phase` (always `phase1`). |
| `orleans.lattice.provider.phase2.commit.timeouts` | `Counter<long>` | `{commit}` | Phase-2 manifest commits abandoned by the per-shard worker after exceeding `AzureTableWalStorageOptions.PhaseTwoCommitTimeout`. Tagged `tree`, `shard`. Zero unless a commit's Azure Tables transaction stopped making progress (hung socket, server-side partition stall, or an SDK retry loop running past the deadline); **any non-zero rate is alert-worthy** - it is the direct signal that the per-shard phase-2 drain loop would otherwise have wedged. Never increments when `PhaseTwoCommitTimeout` is left unset (no deadline enforced). |

### Saga / coordinator / lifecycle

Long-running maintenance and atomicity primitives each emit a completion
signal so operators can alert on stalls, compensation spikes, or missing
coordinator progress independently of whether the event stream is enabled.

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.atomic_write.completed` | `Counter<long>` | `{saga}` | Terminal transition of a `SetManyAtomicAsync` saga. Tagged `outcome=committed` (all writes applied), `compensated` (rolled back via LWW), `failed` (post-compensation surrogate failure), or `shutdown_refused` (the saga's batched dispatch raised the writer-side `WalDrainBudget` refusal or the Orleans grain-rejection shape that indicates the host is shutting down; the saga short-circuited the retry loop and the compensate-broadcast pass rather than burning retry budget against a writer that is not coming back this lifetime - see [Atomic Writes](atomic-writes.md) and `LatticeShuttingDownException`). |
| `orleans.lattice.atomic_write.duration` | `Histogram<double>` | `ms` | End-to-end `SetManyAtomicAsync` saga duration, captured from the first `Prepare` to `Completed` and persisted across reminder-driven recovery so the recorded ms reflects true wall-clock cost (including any time the saga was suspended across silo restarts). Tagged with the same `outcome` values as `atomic_write.completed`; emitted alongside it on every terminal transition. Pair with `atomic_write.completed` to derive sustained atomic-write throughput and SLO percentiles. |
| `orleans.lattice.atomic_write.batch_size` | `Histogram<int>` | `{entry}` | Entry count of each `SetManyAtomicAsync` saga at terminal transition. Tagged with the same `outcome` values as `atomic_write.completed`; emitted alongside it. Lets operators correlate p99 saga duration with batch size and detect distribution shifts in caller batch sizing. |
| `orleans.lattice.atomic_write.cross_tree.completed` | `Counter<long>` | `{saga}` | Terminal transition of a cross-tree atomic-write saga (a `SetManyAtomicAsync` spanning more than one tree). Tagged `outcome` and `tree_count` (number of participating trees). |
| `orleans.lattice.atomic_write.cross_tree.duration` | `Histogram<double>` | `ms` | End-to-end cross-tree atomic-write coordinator duration. Tagged `outcome`. |
| `orleans.lattice.atomic_write.cross_tree.participants` | `Histogram<int>` | `{tree}` | Participating-tree count of each cross-tree atomic-write saga. Tagged `outcome`. |
| `orleans.lattice.saga.prepare.duration` | `Histogram<double>` | `ms` | Wall-clock ms inside one saga `Prepare` phase (the `lattice.SetManyAsync` fan-out that stages every per-key prepared mutation across all touched shards). Tagged `tree`. Pair with `set_many.duration` to confirm the saga prepare is dominated by the foreground multi-key write rather than saga-internal bookkeeping. |
| `orleans.lattice.saga.terminal_decision.duration` | `Histogram<double>` | `ms` | Wall-clock ms inside one saga `TerminalDecision` phase (decide whether to commit or compensate after every prepared mutation has acknowledged). Tagged `tree`. Sub-millisecond except under failure paths. |
| `orleans.lattice.saga.broadcast.duration` | `Histogram<double>` | `ms` | Wall-clock ms inside one saga `Broadcast` phase (the `Task.WhenAll` across affected shards that flips every prepared mutation to its terminal state). Tagged `tree`. The c2-xxiii batched-WAL terminal lift collapsed this phase from ~880ms p50 to ~96ms p50 at the `500:10` rung. |
| `orleans.lattice.saga.broadcast.shard.duration` | `Histogram<double>` | `ms` | Per-shard contribution inside the saga broadcast: wall-clock ms inside one `ShardRootGrain.AppendTxTerminalAsync` call. Tagged `tree` and `shard`. The gap between `saga.broadcast.duration` and this is the max-of-N parallel tail (Orleans scheduling + the slowest shard). |
| `orleans.lattice.saga.broadcast.shard.stage.duration` | `Histogram<double>` | `ms` | Per-sub-stage wall-clock ms inside one `ShardRootGrain.AppendTxTerminalAsync` call. Tagged `tree`, `shard`, and `stage=resolve` (affected-leaves resolution), `hlc` (terminal HLC compute via `GetClockAsync` fan-out), `wal` (per-shard WAL terminal append; collapsed to ~0ms after c2-xxiii batched the WAL terminal at the saga layer), or `fanout` (the per-leaf `ApplyTxTerminalAsync` fan-out). |
| `orleans.lattice.saga.broadcast.leaf.duration` | `Histogram<double>` | `ms` | Wall-clock ms inside a single per-leaf `IBPlusLeafGrain.ApplyTxTerminalAsync` RPC dispatched from the shard's terminal broadcast (step 4). Tagged `tree` and `shard`. |
| `orleans.lattice.saga.checkpoint.duration` | `Histogram<double>` | `ms` | Wall-clock ms inside one saga `Checkpoint` phase (the WriteStateAsync that persists the saga's terminal decision so recovery resumes after the decision rather than re-deciding). Tagged `tree`. |
| `orleans.lattice.saga.reminder.duration` | `Histogram<double>` | `ms` | Wall-clock ms spent inside reminder-driven saga progress callbacks (recovery after silo restart, fallback driver for stalled sagas). Tagged `tree`. Negligible (<0.1ms) on the happy path. |
| `orleans.lattice.saga.fanout.size` | `Histogram<int>` | `{entry}` | Entry count per atomic-write saga, observed at execute-phase entry. Tagged `tree`. The distribution tells operators whether the workload is dominated by small single-entry sagas or large multi-entry batches. |
| `orleans.lattice.saga.perkey.duration` | `Histogram<double>` | `ms` | Per-key duration inside a saga's prepare phase. Tagged `tree`. Pair with `atomic_write.batch_size` to derive amortised per-key atomic-write cost across the saga envelope. |
| `orleans.lattice.saga.wait.serial_gap` | `Histogram<double>` | `ms` | Serialisation gap (idle time between sequential sagas on the same coordinator activation) sampled per saga. Tagged `tree`. A wide tail under sustained offered load indicates the coordinator pool is the rate-limit. |
| `orleans.lattice.coordinator.completed` | `Counter<long>` | `{operation}` | Successful completion of a long-running coordinator. Tagged `kind=snapshot`, `resize`, `reshard`, `merge`, or `compaction`. |
| `orleans.lattice.tree.lifecycle` | `Counter<long>` | `{event}` | Tree-lifecycle transition from `TreeDeletionGrain`. Tagged `kind=deleted`, `recovered`, or `purged`. Emitted **unconditionally** - regardless of the tree's `PublishEvents` setting. |
| `orleans.lattice.warmup.invocations` | `Counter<long>` | `{call}` | One increment per successful `ILattice.WarmUpAsync` call. Tagged `tree`. Operators alerting on cold-start health expect to see exactly one increment per silo startup per warmed tree. |
| `orleans.lattice.warmup.duration` | `Histogram<double>` | `ms` | End-to-end duration of `ILattice.WarmUpAsync` - the wall-clock cost of pre-activating every physical shard root via a bounded-concurrency read-only probe. Tagged `tree` and `shard_count` (the per-tree physical-shard-root probe fan-out). The p99 is the primary warm-start latency signal; sustained increases are a leading indicator of placement-directory or grain-storage cold-touch cost growth. |
| `orleans.lattice.warmup.leaf_cache.prewarmed` | `Counter<long>` | `{leaf}` | Leaf caches successfully primed by a shard root's opt-in post-restart pre-warm. Tagged `tree`, `shard`, and the tenant label. Flat at zero unless `LatticeOptions.LeafCachePreWarmCount` is positive (the default is `0`). Individual priming failures are swallowed by design, so a value materially below the configured count is the only signal that they occurred. |
| `orleans.lattice.warmup.leaf_cache.duration` | `Histogram<double>` | `ms` | Wall-clock duration of one shard root's leaf-cache pre-warm fan-out. One observation per shard per warm-up when the feature is enabled and the access model ranked at least one leaf. Tagged `tree`, `shard`, and the tenant label. Read against `warmup.duration` to attribute how much of a tree's warm-start cost is leaf priming. |
| `orleans.lattice.leaf_access.model.leaves` | `Histogram<int>` | `{leaf}` | Leaves resident in a shard root's leaf-access frequency histogram, observed each time the model is persisted. Tagged `tree`, `shard`, and the tenant label. Bounded above by the model's tracked-leaf cap, so a distribution pinned at that cap means the shard's read set is wider than the model can represent and the pre-warm ranking is drawn from a pruned view. |

### Distributed lock (sourced from `LatticeLockGrain`)

The FIFO-fair distributed lock / lease grain behind `ILatticeLockGrain` (see
[Distributed lock](distributed-lock.md)). One activation per lock name serialises
all contending callers, so these instruments carry no per-key tag; scope them by
`cluster` (and `silo`) in the dashboards. Charted on the Overview dashboard's
"Distributed lock" row.

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.lock.acquired` | `Counter<long>` | `{acquire}` | One increment per acquire attempt that reached a terminal outcome. Tagged `outcome=granted` (the caller was handed the lease), `timeout` (a blocking `AcquireAsync` whose wait-timeout elapsed, or a non-blocking `MaxWait = 0` acquire on a held lock), or `unavailable` (a `TryAcquireAsync` that returned `null` because the lock was held). Pair `granted` with `released` to see live lock churn. |
| `orleans.lattice.lock.released` | `Counter<long>` | `{release}` | One increment per explicit `ReleaseAsync` by the current holder (a stale-token release is a silent no-op and is not counted). |
| `orleans.lattice.lock.lease_reclaimed` | `Counter<long>` | `{lease}` | One increment each time an expired lease was reclaimed because the holder neither renewed nor released before expiry. A sustained non-zero rate means holders are crashing or failing to renew within their lease duration; downstream resources should be relying on the fencing token to reject the reclaimed holder. |
| `orleans.lattice.lock.acquire.wait` | `Histogram<double>` | `ms` | Time a granted acquire spent parked in the FIFO queue before it was granted (recorded once per grant, so an uncontended acquire records ~0). The p95/p99 tail is the primary lock-contention signal. |

### Atomic action / saga (sourced from `AtomicActionGrain`)

The generic atomic-action (saga / TCC) coordinator behind `IAtomicActionGrain` (see
[Atomic action](atomic-action.md)). One activation per operation id, so these
instruments carry no per-key tag; scope them by `cluster` (and `silo`) in the
dashboards. Charted on the Overview dashboard's "Atomic action (saga / TCC)" row.

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.atomic_action.completed` | `Counter<long>` | `{saga}` | One increment per terminal transition of a saga. Tagged `outcome=committed` (every forward step committed), `compensated` (a forward step faulted and every committed step was rolled back in reverse order), or `compensation_failed` (a compensating effect itself faulted, so the saga parked for operator intervention). A rising `compensation_failed` rate means a caller's compensation contract is being violated. |
| `orleans.lattice.atomic_action.step` | `Counter<long>` | `{step}` | One increment per step effect the saga runs. Tagged `phase=forward` \| `compensate` and `outcome=ok` \| `fault`. The `compensate` series is non-zero only when a saga rolls back; a `compensate,fault` point is the leading indicator of a park. |
| `orleans.lattice.atomic_action.duration` | `Histogram<double>` | `ms` | End-to-end saga duration measured from saga start (persisted, so it includes time suspended across silo restarts) to the terminal transition. Tagged by `outcome` so rollback-path latency is separable from happy-path latency. |



These counters are emitted only when event publication is enabled on at least
one tree. They let operators detect a misconfigured stream provider or a
failing downstream queue before it starts consuming silo resources.

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.events.published` | `Counter<long>` | `{event}` | `LatticeTreeEvent` instances successfully dispatched to the configured stream provider. Tagged `kind` = the `LatticeTreeEventKind` name (e.g. `Set`, `SnapshotCompleted`). |
| `orleans.lattice.events.dropped` | `Counter<long>` | `{event}` | Events dropped by the publisher. Tagged `reason=missing_provider` (no stream provider by the configured name is registered on this silo) or `publish_error` (the stream provider threw during dispatch). A non-zero rate on `missing_provider` means [`LatticeOptions.PublishEvents`](configuration.md#publishevents) is `true` but the corresponding `AddMemoryStreams` / `AddEventHubStreams` call is missing on the silo. |

### Configuration

Runtime overrides applied through `ILattice` that mutate per-tree behaviour
emit a lightweight change counter so operators can audit policy changes on
the same pipeline as the traffic they affect.

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.config.changed` | `Counter<long>` | `{change}` | A per-tree configuration change was applied. Tagged `config` = the configuration dimension (currently `publish_events` from `ILattice.SetPublishEventsEnabledAsync`). |

### Materialised views

The view maintainer publishes on the core `orleans.lattice` meter (views need a
WAL-backed lattice, not the replication package), each instrument tagged with the
view name. See [Materialised views](materialised-views.md).

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.view.apply_lag` | `Histogram<long>` | `{entry}` | Apply lag (committed-but-unapplied source entries) sampled at the end of each drain pass. |
| `orleans.lattice.view.backlog_depth` | `Histogram<long>` | `{entry}` | WAL entries read in the drain pass. |
| `orleans.lattice.view.applied` | `Counter<long>` | `{write}` | View writes applied to the view tree. |
| `orleans.lattice.view.key_collisions` | `Counter<long>` | `{collision}` | Distinct source keys that re-mapped to one view key in a drain batch (injectivity violation); resolved last-writer-wins. |
| `orleans.lattice.view.aggregation_applied` | `Counter<long>` | `{contribution}` | Aggregation contributions folded into the view (count / sum / min / max / set-union / fold). |
| `orleans.lattice.view.aggregation_rejected` | `Counter<long>` | `{contribution}` | Aggregation contributions dropped because the group-key selector produced an empty key or one under the reserved NUL (`\u0000`) prefix; deterministic on the key, so clusters stay convergent. A non-zero value flags a selector emitting reserved keys. |
| `orleans.lattice.view.atomic_staging_backstop` | `Counter<long>` | `{rebuild}` | Times the bounded-buffer / retention backstop abandoned atomic staging and forced a rebuild. |
| `orleans.lattice.view.cross_tree_joint_violation` | `Counter<long>` | `{degradation}` | Cross-tree view batches that degraded to per-tree atomicity because a participant view did not become ready within `CrossTreeReadinessTimeout`. |
| `orleans.lattice.view.lag_budget_eviction` | `Counter<long>` | `{eviction}` | Views force-evicted (WAL unpinned and rebuilt) for exceeding their `MaxLagBudget`. |
| `orleans.lattice.view.source_backpressure` | `Counter<long>` | `{pass}` | Background drain passes that throttled themselves (scaled-down batch + deferred next tick) because the source tree was under WAL saturation back-pressure. Tagged `view` and `state` (the observed source regime, `throttled` / `saturated`); never recorded on a healthy source or when `ObeySourceBackpressure` is disabled. |

### Tag index reconciliation

Background tag-index reconciliation publishes on the core `orleans.lattice` meter,
each instrument tagged with the `index` name. See [Tag indexes](api.md#tag-indexes).

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.tag_index.reconcile.sweeps` | `Counter<long>` | `{sweep}` | Background tag-index reconciliation sweeps, tagged by outcome (`clean`, `repaired`, `probe_only`). |
| `orleans.lattice.tag_index.reconcile.trees.probed` | `Counter<long>` | `{tree}` | Covered trees whose digest fingerprint a reconciliation sweep probed. |
| `orleans.lattice.tag_index.reconcile.trees.mismatched` | `Counter<long>` | `{tree}` | Covered trees a reconciliation sweep found divergent from their digest baseline. |
| `orleans.lattice.tag_index.reconcile.orphan_rows.removed` | `Counter<long>` | `{row}` | Orphan membership rows removed by background tag-index reconciliation. |
| `orleans.lattice.tag_index.reconcile.duration` | `Histogram<double>` | `ms` | Wall-clock duration of a background tag-index reconciliation sweep. |

## Replication meter

The replication package (`Orleans.Lattice.Replication`) publishes its own
meter, `orleans.lattice.replication`, so an operator can subscribe to
cross-cluster telemetry independently of the core lattice surface. The meter
name is pinned by a regression test in the replication package.

| Member | Value |
|---|---|
| `LatticeReplicationMetrics.MeterName` | `orleans.lattice.replication` |
| `LatticeReplicationMetrics.Meter` | the `Meter` instance |

The full per-instrument catalogue - ship / apply / lag, causal-apply buffer,
dead-letter, per-peer health, and bootstrap instruments, plus the replication
tag conventions - lives in the replication package's
[Observability](../lattice.replication/observability.md) document, which is the
source of truth for every `orleans.lattice.replication.*` instrument.

To subscribe to both meters, register them by name in the OpenTelemetry pipeline:

```csharp
using OpenTelemetry.Metrics;

builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter("orleans.lattice")
        .AddMeter("orleans.lattice.replication")
        .AddPrometheusExporter());
```

## OpenTelemetry registration

Register the meter by name - this is the same pattern used for any other
`System.Diagnostics.Metrics` source:

```csharp
// In your silo host's Program.cs or similar composition root.
using OpenTelemetry.Metrics;

builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter("orleans.lattice")
        .AddPrometheusExporter()); // or AddOtlpExporter, AddAzureMonitorMetricExporter, etc.
```

The meter is created at assembly load time, so adding it before the silo starts
is sufficient - every subsequently-activated grain publishes into the already-
subscribed pipeline.

## Exposing metrics to an AI agent

Once the `orleans.lattice` meter is scraped into a Prometheus-compatible
backend, the opt-in `Orleans.Lattice.Api.Mcp.Telemetry` companion package can
expose that backend to an AI agent as read-only Model Context Protocol tools,
behind a dedicated cluster-wide `Telemetry` authorization grant and a backend
credential the agent never sees. See
[MCP Telemetry](../lattice.api.mcp.telemetry/README.md).

## Bundled Grafana dashboards

The companion `Orleans.Lattice.Dashboards` package ships ready-to-import
dashboards keyed by `LatticeDashboardKind`:

| Kind | Title | Focus |
|---|---|---|
| `Overview` | Orleans.Lattice - Overview | Per-tree throughput, leaf-write percentiles, cache hit-rate, tombstone churn, splits, atomic-write outcomes (rate), coordinator completions, tree-lifecycle, event publish/drop, runtime config changes. The dashboard also includes a horizontal row of three atomic-write panels (saga duration p50/p95/p99, batch-size p50/p95/p99, and a dedicated saga-failure-rate panel), and the top-of-stack read-path envelopes: `GetAsync` / `GetManyAsync` per-call latency at p50 / p95 / p99 (`get.duration`, `get_many.duration`), per-stage sub-attribution (`get.stage.duration` tagged `route|shard`, `get_many.stage.duration` tagged `route|bucket|fanout|merge`), and a low-traffic tile pairing `ExistsAsync` (`exists.duration`) with `GetWithVersionAsync` (`get_with_version.duration`) - the latter bypasses the `LeafCacheGrain` by design, so its envelope runs systematically higher than `GetAsync` on the same key distribution. |
| `CommitPath` | Orleans.Lattice - Commit Path | Per-step commit-pipeline latency (`wal` / `apply` / `observer` / `digest`), storage-provider write duration, compaction duration, activation-time replay duration / entries by recovery outcome, and tombstone churn (reaped / expired / created). Adds top-of-stack `SetAsync` / `SetManyAsync` envelope percentiles and per-stage sub-attribution (`set.stage.duration` tagged `stage`, `set_many.stage.duration` tagged `gate|route|bucket|fanout|events`), ShardRoot-side per-step (`shard_root.set_many.{local_apply, shadow_forward, leaf_rpc}.duration`), WAL append decomposition (`wal.append.turn_wait` / `provider.duration` / `wal.shard.dispatch.duration`) with batch shape (`batch_entries` / `batch_bytes` / `dispatch_entries`) and back-pressure (`wal.append.in_flight` / `queue_depth`), storage-provider phase-2 panels (`provider.commit.duration` + `provider.phase2.batch_size`, plus a retry-attempts vs retry-exhausted vs idempotent-replays panel), leaf commit-concurrency (`leaf.commit.in_flight` p95), `WarmUpAsync` invocations + duration, and a digest-coalescing efficacy panel (`leaf.digest.publishes` tagged `path`) that surfaces the c2-xxix regression signature directly. Strictly write-side; the read-path envelopes live on the `Overview` dashboard to keep the commit-pipeline focus uncluttered. |
| `Replication` | Orleans.Lattice.Replication - Operator | Cross-cluster ship/apply/lag, WAL append vs trim, dead-letter churn, FIFO violations, fall-off-log events, per-peer cursor lag. Sources the `orleans.lattice.replication` meter. |
| `AtomicWrites` | Orleans.Lattice - Atomic Writes | Dedicated `SetManyAtomicAsync` saga deep-dive: outcome rate (stacked area), saga duration p50/p95/p99 and p95 by outcome, batch size p50/p95/p99 and p95 by outcome, per-tree committed throughput, range-window non-committed saga count, and a separate saga-failure-rate panel with 1% / 5% threshold lines. Adds per-phase saga sub-attribution (`saga.{prepare, terminal_decision, broadcast, checkpoint, reminder}.duration` p95), broadcast deep-dive (`saga.broadcast.{shard, leaf, shard.stage}.duration`), per-key work vs serial-gap wait (`saga.perkey.duration` vs `saga.wait.serial_gap`), and saga fan-out size distribution (`saga.fanout.size` p50/p95/p99). |
| `MaterialisedViews` | Orleans.Lattice - Materialised Views | The `orleans.lattice.view.*` instruments above: apply-lag and drain-backlog-depth percentiles, filter / re-project (`view.applied`) and aggregation (`view.aggregation_applied`) apply throughput, and warning panels for lag-budget evictions, re-key collisions, atomic-staging backstop fall-backs, cross-tree joint-atomicity violations, and source back-pressure self-throttle (`view.source_backpressure`, split by observed source regime). Every panel is keyed by view name (and cluster); because a view's maintainer is a single grain activation that migrates between silos, the dashboard is a cluster-wide concern and deliberately offers no per-silo filter. Sources the core `orleans.lattice` meter only. |
| `Authorization` | Orleans.Lattice - Identity & Authorization | Enforcement-gate decision throughput (by `effect` and by `operation`), decision-latency percentiles, compiled-snapshot rebuild rate and the snapshot epoch / age gauges from the `orleans.lattice.auth` meter, alongside the subject-resolution cache hit-ratio and hit / miss throughput from the `orleans.lattice.membership` meter. Sources the `orleans.lattice.auth` and `orleans.lattice.membership` meters; useful only when the authentication / authorization packages are registered. Coverage of this meter is enforced from the owning packages' tests, not the dashboards package. |
| `Backup` | Orleans.Lattice - Backup & Restore | Capture / restore throughput and duration percentiles, per-backup size / artifact / entry distributions, cumulative processed throughput, retention reclaim and prune rates, incremental lag (entries and age behind the base cut), capture / restore failure rates by reason, scheduler skipped-run and overrun counters, the cross-tree-consistent fence selection / drain counters, and the inventory observable gauges (tracked count, max chain depth, catalog bytes, oldest / newest age, per-scope last-run status and last-success age). Sources the `orleans.lattice.backup` meter; useful only when the backup package is registered. Coverage of this meter is enforced from the backup package's own test, not the dashboards package. |

The `Overview` dashboard's atomic-write row is intentionally a teaser surface
suitable for at-a-glance operator scanning; the dedicated `AtomicWrites`
dashboard is the right home for incident triage and SLO drill-down.

Resolve the dashboard JSON at runtime:

```csharp
using Orleans.Lattice.Dashboards;

string atomicWritesJson = LatticeDashboards.GetGrafanaDashboardJson(
    LatticeDashboardKind.AtomicWrites);
```

The same dashboards are used by the in-repo benchmark cockpit
(`benchmark/grafana/dashboards/`) and by the persistent benchmark-history
cockpit (`benchmark/history/grafana/dashboards/`). The history cockpit
additionally ships a `Performance: Atomic Writes` view with headline KPI
tiles (ops/sec, p99 ns, mean ns, alloc/op B), trend timeseries, per-run
barcharts, and a stat + trend pair for the saga failure rate sourced from
the live cluster's Prometheus → VictoriaMetrics scrape.

## Performance

All instruments are zero-allocation on the hot path: counters use primitive
`Add`, and histograms use `Stopwatch.GetTimestamp()` deltas rather than
`Stopwatch` instances. When no listener is attached, the measurement callbacks
are elided by the runtime.

## Relationship to `DiagnoseAsync`

`ILattice.DiagnoseAsync` ([docs/lattice/diagnostics.md](diagnostics.md)) returns a
point-in-time snapshot intended for operator inspection and troubleshooting.
The metrics pipeline described here is the **continuous** telemetry feed for
dashboards and alerting. Both are sourced from the same underlying grain state
(shard hotness counters, leaf statistics) - they are complementary, not
redundant.