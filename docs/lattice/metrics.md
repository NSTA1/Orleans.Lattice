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
| `stage` | `orleans.lattice.set.stage.duration`, `orleans.lattice.set_many.stage.duration`, `orleans.lattice.saga.broadcast.shard.stage.duration` | Sub-stage name within an envelope - see each instrument below |
| `phase` | `orleans.lattice.provider.commit.duration` | `phase1` (per-batch partition txn) or `phase2` (manifest partition txn) |
| `outcome` | `orleans.lattice.atomic_write.completed`, `orleans.lattice.atomic_write.duration`, `orleans.lattice.atomic_write.batch_size`, `orleans.lattice.leaf.replay.duration`, `orleans.lattice.leaf.replay.entries`, `orleans.lattice.compaction.leaves.visited` | Discriminator - see each instrument below |
| `kind` | `orleans.lattice.coordinator.completed`, `orleans.lattice.tree.lifecycle`, `orleans.lattice.events.published` | Discriminator - see each instrument below |
| `trigger` | `orleans.lattice.compaction.pass.duration`, `orleans.lattice.compaction.leaves.visited` | `reminder`, `ratio`, `size`, or `operator` |
| `path` | `orleans.lattice.compaction.leaves.visited` | `walk` or `dirty-set` |
| `reason` | `orleans.lattice.events.dropped` | `missing_provider` or `publish_error` |
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
| `orleans.lattice.shard.reads` | `Counter<long>` | `{op}` | Read operations served by a shard root (`GetAsync`, `ExistsAsync`, scan, count, etc.). |
| `orleans.lattice.shard.writes` | `Counter<long>` | `{op}` | Write operations served by a shard root (`SetAsync`, `DeleteAsync`, `MergeManyAsync`, etc.). |
| `orleans.lattice.shard.digest_reads` | `Counter<long>` | `{op}` | Projection-digest reads served by a shard root - one increment per `IShardRootGrain.GetShardProjectionDigestAsync` call. A whole-tree poll of `ILattice.GetLeafProjectionDigestAsync` produces exactly `shardCount` increments; a higher rate signals a regression that fell back to walking every leaf. Tagged `tree` and `shard`. |
| `orleans.lattice.shard.splits_committed` | `Counter<long>` | `{split}` | Adaptive shard-split commits - fired once per successful `ShardMap` swap from `TreeShardSplitGrain.FinaliseAsync`. |
| `orleans.lattice.split.retroactive_forward.entries` | `Counter<long>` | `{entry}` | Pending prepared mutations retroactively shadow-forwarded from a source shard's leaf chain into the destination shard's `_pendingTx` buckets at the start of an adaptive split's `BeginShadowWrite` phase. Tagged `tree` and `shard` (source). |
| `orleans.lattice.split.retroactive_forward.duration` | `Histogram<double>` | `ms` | Wall-clock duration of the retroactive prepared-mutation sweep before the split coordinator transitions to the `Drain` phase. Tagged `tree` and `shard` (source). |

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
| `orleans.lattice.wal.append.queue_depth` | `Histogram<int>` | `{pending}` | Number of callers parked on the WAL activation's turn-token queue sampled at the moment a new caller enters the grain body. Tagged `tree`, `shard`, `wal_partitions`, `wal_max_pending_batches`. |

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
| `orleans.lattice.provider.phase2.batch_size` | `Histogram<int>` | `{commit}` | Number of coalesced phase-2 commits the per-shard provider worker bundled into a single transaction. Tagged `tree`, `shard`, `pipeline_phase2`. A distribution concentrated near 1 means the worker is never catching up against backed-up arrivals; values closer to the per-transaction cap (~49 commits) indicate the worker is the shard's effective rate limiter. |
| `orleans.lattice.provider.retry.attempts` | `Counter<long>` | `{retry}` | Per-call retry attempts incurred by the storage-provider SDK during a single WAL append or commit. Tagged `tree`, `shard`. A non-zero rate is normal; sustained climbing rates suggest provider-side throttling that the local retry policy is masking. |
| `orleans.lattice.provider.retry.exhausted` | `Counter<long>` | `{retry}` | Calls that exhausted the storage-provider retry budget and surfaced the underlying fault. Tagged `tree`, `shard`. **Any non-zero rate is alert-worthy** - the WAL append or commit failed past the SDK's retry envelope. |

### Saga / coordinator / lifecycle

Long-running maintenance and atomicity primitives each emit a completion
signal so operators can alert on stalls, compensation spikes, or missing
coordinator progress independently of whether the event stream is enabled.

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.atomic_write.completed` | `Counter<long>` | `{saga}` | Terminal transition of a `SetManyAtomicAsync` saga. Tagged `outcome=committed` (all writes applied), `compensated` (rolled back via LWW), or `failed` (post-compensation surrogate failure). |
| `orleans.lattice.atomic_write.duration` | `Histogram<double>` | `ms` | End-to-end `SetManyAtomicAsync` saga duration, captured from the first `Prepare` to `Completed` and persisted across reminder-driven recovery so the recorded ms reflects true wall-clock cost (including any time the saga was suspended across silo restarts). Tagged with the same `outcome` values as `atomic_write.completed`; emitted alongside it on every terminal transition. Pair with `atomic_write.completed` to derive sustained atomic-write throughput and SLO percentiles. |
| `orleans.lattice.atomic_write.batch_size` | `Histogram<int>` | `{entry}` | Entry count of each `SetManyAtomicAsync` saga at terminal transition. Tagged with the same `outcome` values as `atomic_write.completed`; emitted alongside it. Lets operators correlate p99 saga duration with batch size and detect distribution shifts in caller batch sizing. |
| `orleans.lattice.saga.prepare.duration` | `Histogram<double>` | `ms` | Wall-clock ms inside one saga `Prepare` phase (the `lattice.SetManyAsync` fan-out that stages every per-key prepared mutation across all touched shards). Tagged `tree`. Pair with `set_many.duration` to confirm the saga prepare is dominated by the foreground multi-key write rather than saga-internal bookkeeping. |
| `orleans.lattice.saga.terminal_decision.duration` | `Histogram<double>` | `ms` | Wall-clock ms inside one saga `TerminalDecision` phase (decide whether to commit or compensate after every prepared mutation has acknowledged). Tagged `tree`. Sub-millisecond except under failure paths. |
| `orleans.lattice.saga.broadcast.duration` | `Histogram<double>` | `ms` | Wall-clock ms inside one saga `Broadcast` phase (the `Task.WhenAll` across affected shards that flips every prepared mutation to its terminal state). Tagged `tree`. The c2-xxiii batched-WAL terminal lift collapsed this phase from ~880ms p50 to ~96ms p50 at the `500:10` rung. |
| `orleans.lattice.saga.broadcast.shard.duration` | `Histogram<double>` | `ms` | Per-shard contribution inside the saga broadcast: wall-clock ms inside one `ShardRootGrain.AppendTxTerminalAsync` call. Tagged `tree` and `shard`. The gap between `saga.broadcast.duration` and this is the max-of-N parallel tail (Orleans scheduling + the slowest shard). |
| `orleans.lattice.saga.broadcast.shard.stage.duration` | `Histogram<double>` | `ms` | Per-sub-stage wall-clock ms inside one `ShardRootGrain.AppendTxTerminalAsync` call. Tagged `tree`, `shard`, and `stage=resolve` (affected-leaves resolution), `hlc` (terminal HLC compute via `GetClockAsync` fan-out), `wal` (per-shard WAL terminal append; collapsed to ~0ms after c2-xxiii batched the WAL terminal at the saga layer), or `fanout` (the per-leaf `ApplyTxTerminalAsync` fan-out). |
| `orleans.lattice.saga.broadcast.leaf.duration` | `Histogram<double>` | `ms` | Wall-clock ms inside a single per-leaf `IBPlusLeafGrain.ApplyTxTerminalAsync` RPC dispatched from the shard's terminal broadcast (step 4). Tagged `tree` and `shard`. |
| `orleans.lattice.saga.checkpoint.duration` | `Histogram<double>` | `ms` | Wall-clock ms inside one saga `Checkpoint` phase (the WriteStateAsync that persists the saga's terminal decision so recovery resumes after the decision rather than re-deciding). Tagged `tree`. |
| `orleans.lattice.saga.reminder.duration` | `Histogram<double>` | `ms` | Wall-clock ms spent inside reminder-driven saga progress callbacks (recovery after silo restart, fallback driver for stalled sagas). Tagged `tree`. Negligible (<0.1ms) on the happy path. |
| `orleans.lattice.saga.fanout.size` | `Histogram<int>` | `{shard}` | Number of shards a saga touched in its prepare-phase fan-out. Tagged `tree`. The histogram's distribution tells operators whether the workload is dominated by single-shard sagas (cheap), evenly-distributed sagas (max fan-out), or hot-shard sagas. |
| `orleans.lattice.saga.perkey.duration` | `Histogram<double>` | `ms` | Per-key duration inside a saga's prepare phase. Tagged `tree`. Pair with `atomic_write.batch_size` to derive amortised per-key atomic-write cost across the saga envelope. |
| `orleans.lattice.saga.wait.serial_gap` | `Histogram<double>` | `ms` | Serialisation gap (idle time between sequential sagas on the same coordinator activation) sampled per saga. Tagged `tree`. A wide tail under sustained offered load indicates the coordinator pool is the rate-limit. |
| `orleans.lattice.coordinator.completed` | `Counter<long>` | `{operation}` | Successful completion of a long-running coordinator. Tagged `kind=snapshot`, `resize`, `reshard`, `merge`, or `compaction`. |
| `orleans.lattice.tree.lifecycle` | `Counter<long>` | `{event}` | Tree-lifecycle transition from `TreeDeletionGrain`. Tagged `kind=deleted`, `recovered`, or `purged`. Emitted **unconditionally** - regardless of the tree's `PublishEvents` setting. |
| `orleans.lattice.warmup.invocations` | `Counter<long>` | `{call}` | One increment per successful `ILattice.WarmUpAsync` call. Tagged `tree`. Operators alerting on cold-start health expect to see exactly one increment per silo startup per warmed tree. |
| `orleans.lattice.warmup.duration` | `Histogram<double>` | `ms` | End-to-end duration of `ILattice.WarmUpAsync` - the wall-clock cost of pre-activating every physical shard root via a bounded-concurrency read-only probe. Tagged `tree` and `shard_count` (the per-tree physical-shard-root probe fan-out). The p99 is the primary warm-start latency signal; sustained increases are a leading indicator of placement-directory or grain-storage cold-touch cost growth. |

### Event publisher health

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

## Replication meter

The replication package (`Orleans.Lattice.Replication`) publishes its own
meter so an operator can subscribe to cross-cluster telemetry independently
of the core lattice surface. The meter name is pinned by a regression test
in the replication package; subscribe to it the same way as the core meter.

| Member | Value |
|---|---|
| `LatticeReplicationMetrics.MeterName` | `orleans.lattice.replication` |
| `LatticeReplicationMetrics.Meter` | the `Meter` instance |

### Tag conventions (replication)

| Tag key | Applies to | Value |
|---|---|---|
| `tree` | every instrument | Logical tree id |
| `peer` | ship-side instruments | Configured peer cluster identifier |
| `origin` | apply-side instruments | Origin cluster id stamped on the inbound mutation |
| `shard` | causal-apply buffer gauges | Physical shard index |
| `outcome` | ship / bootstrap instruments | `success`, `transient_fault`, `permanent_fault`, etc. |
| `reason` | dead-letter counters | Discriminator describing why the entry was parked / removed |

### Outbound (ship) and replog throughput

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.replication.ship.duration` | `Histogram<double>` | `ms` | Wall-clock duration of one outbound ship-batch attempt. Tagged `tree`, `peer`, `outcome`. |
| `orleans.lattice.replication.wal.entries_appended` | `Counter<long>` | `{entry}` | Replog entries committed to the local WAL. Tagged `tree`. |
| `orleans.lattice.replication.wal.entries_shipped` | `Counter<long>` | `{entry}` | Replog entries acknowledged by a remote peer. Tagged `tree`, `peer`. |
| `orleans.lattice.replication.peer.fell_off_log` | `Counter<long>` | `{event}` | Receiver fall-off-the-log detection events. Tagged `tree`, `origin`. |
| `orleans.lattice.replication.peer.fell_off_log_suppressed` | `Counter<long>` | `{event}` | Receiver fall-off-the-log probes suppressed because the bootstrap coordinator is already draining from the same origin. Tagged `tree`, `origin`. |

### Inbound (apply) and causal-buffer

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.replication.apply.duration` | `Histogram<double>` | `ms` | Wall-clock duration of one inbound apply-batch attempt. Tagged `tree`, `peer`, `outcome`. |
| `orleans.lattice.replication.apply.lag` | `Histogram<double>` | `ms` | Receiver-side replication lag observed at successful apply. Tagged `tree`, `peer`. |
| `orleans.lattice.replication.apply.buffered_entries` | `ObservableGauge<long>` | `{entry}` | Replog entries currently parked in the causal-apply buffer. Tagged `tree`, `shard`. |
| `orleans.lattice.replication.apply.buffer_bytes` | `ObservableGauge<long>` | `By` | Cumulative serialised payload size parked in the causal-apply buffer. Tagged `tree`, `shard`. |
| `orleans.lattice.replication.apply.dependency_wait_ms` | `Histogram<double>` | `ms` | Wait time between park and drain for a buffered causal-apply entry. Tagged `tree`. |
| `orleans.lattice.replication.apply.causal_violations_blocked` | `Counter<long>` | `{entry}` | Replog entries blocked by an unsatisfied causal dependency at apply time. Tagged `tree`. |
| `orleans.lattice.replication.apply.fifo_violations` | `Counter<long>` | `{entry}` | Successful applies whose source HLC was strictly less than the previous apply for the same `(tree, origin)` pair. Tagged `tree`, `origin`. **Any non-zero rate is alert-worthy** - a FIFO violation breaks the per-origin causal guarantee. |

### Dead-letter queue

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.replication.dead_letter.enqueued` | `Counter<long>` | `{entry}` | Replog entries parked on the per-tree dead-letter queue. Tagged `tree`, `reason`. |
| `orleans.lattice.replication.dead_letter.removed` | `Counter<long>` | `{entry}` | Entries removed from the per-tree dead-letter queue. Tagged `tree`, `reason`. |

### Bootstrap (receiver-side coordinator)

| Name | Kind | Unit | Description |
|---|---|---|---|
| `orleans.lattice.replication.bootstrap.entries_received` | `Counter<long>` | `{entry}` | Snapshot entries applied by the bootstrap coordinator. Tagged `tree`, `origin`. |
| `orleans.lattice.replication.bootstrap.bytes_received` | `Counter<long>` | `By` | Bytes applied by the bootstrap coordinator. Tagged `tree`, `origin`. |
| `orleans.lattice.replication.bootstrap.duration` | `Histogram<double>` | `ms` | Bootstrap drain duration from `RequestingSnapshot` to the terminal phase. Tagged `tree`, `origin`, `outcome`. |
| `orleans.lattice.replication.bootstrap.transient_retries` | `Counter<long>` | `{retry}` | Number of transient-fault retries consumed by the receiver-side bootstrap drain. Tagged `tree`, `origin`. |

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

## Bundled Grafana dashboards

The companion `Orleans.Lattice.Dashboards` package ships ready-to-import
dashboards keyed by `LatticeDashboardKind`:

| Kind | Title | Focus |
|---|---|---|
| `Overview` | Orleans.Lattice - Overview | Per-tree throughput, leaf-write percentiles, cache hit-rate, tombstone churn, splits, atomic-write outcomes (rate), coordinator completions, tree-lifecycle, event publish/drop, runtime config changes. The dashboard now also includes a horizontal row of three atomic-write panels: saga duration p50/p95/p99, batch-size p50/p95/p99, and a dedicated saga-failure-rate panel. |
| `CommitPath` | Orleans.Lattice - Commit Path | Per-step commit-pipeline latency (`wal` / `apply` / `observer` / `digest`), storage-provider write duration, compaction duration, activation-time replay duration / entries by recovery outcome, and tombstone churn (reaped / expired / created). Adds top-of-stack `SetAsync` / `SetManyAsync` envelope percentiles and per-stage sub-attribution (`set.stage.duration` tagged `stage`, `set_many.stage.duration` tagged `gate|route|bucket|fanout|events`), ShardRoot-side per-step (`shard_root.set_many.{local_apply, shadow_forward, leaf_rpc}.duration`), WAL append decomposition (`wal.append.turn_wait` / `provider.duration` / `wal.shard.dispatch.duration`) with batch shape (`batch_entries` / `batch_bytes` / `dispatch_entries`) and back-pressure (`wal.append.in_flight` / `queue_depth`), storage-provider phase-2 panels (`provider.commit.duration` + `provider.phase2.batch_size`, plus a retry-attempts vs retry-exhausted panel), leaf commit-concurrency (`leaf.commit.in_flight` p95), `WarmUpAsync` invocations + duration, and a digest-coalescing efficacy panel (`leaf.digest.publishes` tagged `path`) that surfaces the c2-xxix regression signature directly. |
| `Replication` | Orleans.Lattice.Replication - Operator | Cross-cluster ship/apply/lag, WAL append vs trim, dead-letter churn, FIFO violations, fall-off-log events, per-peer cursor lag. Sources the `orleans.lattice.replication` meter. |
| `AtomicWrites` | Orleans.Lattice - Atomic Writes | Dedicated `SetManyAtomicAsync` saga deep-dive: outcome rate (stacked area), saga duration p50/p95/p99 and p95 by outcome, batch size p50/p95/p99 and p95 by outcome, per-tree committed throughput, range-window non-committed saga count, and a separate saga-failure-rate panel with 1% / 5% threshold lines. Adds per-phase saga sub-attribution (`saga.{prepare, terminal_decision, broadcast, checkpoint, reminder}.duration` p95), broadcast deep-dive (`saga.broadcast.{shard, leaf, shard.stage}.duration`), per-key work vs serial-gap wait (`saga.perkey.duration` vs `saga.wait.serial_gap`), and saga fan-out size distribution (`saga.fanout.size` p50/p95/p99). |

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