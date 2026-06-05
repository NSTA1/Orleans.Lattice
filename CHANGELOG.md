# Changelog

All notable changes to the Orleans.Lattice package family are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This changelog covers the **package family**: `Orleans.Lattice`, `Orleans.Lattice.Replication`, `Orleans.Lattice.Replication.Grpc`, `Orleans.Lattice.Storage.AzureTable`, and `Orleans.Lattice.Dashboards`. Packages ship in lockstep on the major and minor digits; patch digits may advance per-package.

## [Unreleased]

Items merged into `main` after the v6.2.0 cut accumulate here under the `### Added` / `### Changed` / `### Fixed` / `### Breaking` headings until the next ship cut.

Outstanding work is tracked on [GitHub Issues](https://github.com/NSTA1/Orleans.Lattice/issues), indexed in [`docs/lattice/features.md`](docs/lattice/features.md) and [`docs/lattice.replication/features.md`](docs/lattice.replication/features.md). See [`docs/RELEASING.md`](docs/RELEASING.md) for the per-package tag-and-publish protocol.

### Changed - configuration defaults

- **`LatticeOptions.WalMaxPendingBatches` default raised from 8 to 16.** Measured on Standard_D4as_v5 + Azure Tables Standard at 4,000 keys/s offered load, the new default delivers a +57% increase in steady-state silo throughput at the 4k:5 rung (mean of n=3 cohorts: 21,275 entries/s, range ~104 e/s, ~0.5% CoV) with no reliability regression (`failed=0`, no stall-watchdog firings, no admission timeouts). The change is a pure default flip; no public-API break and no behavioural change on hosts that already pin the value explicitly. Set to `1` to restore the historical single-in-flight-per-partition shape. At the canonical `WalPartitions = 8` the combined fan-out is `8 * 16 = 128` concurrent flushes against the provider, which sits at the edge of a single Azure Tables Standard storage account's sustained throughput budget; if you need more headroom, increase `WalPartitions` (fan-out across accounts) before lifting the per-partition cap further.

### Added - documentation

- **New `docs/lattice/wal-tuning.md`** covering how `WalMaxPendingBatches` and `WalPartitions` interact with a durable backend's throughput envelope, the SKU-sizing rules of thumb for Azure Tables Standard, and the storage-account-throughput ceiling (~2,500 ops/sec/account on Standard SKU) above which raising the cap further surfaces as 429 throttling rather than additional throughput. Includes the three instruments (`wal.writer.append.admission_wait`, `wal.append.provider.duration`, `wal.writer.partition.pending_appends`) that tell you which regime you are in.

### Changed - documentation

- **`docs/lattice/performance-single-silo.md`** Layer 2 `SetManyAsync` row updated to reflect the new shipping default (13,574 -> 21,275 entries/s; per-call p99 2.85 s -> ~1.4 s), with per-cell provenance disclosed for the cells that pre-date the WAL re-tune. The "How it was run" paragraph and the opening disclaimer no longer claim ACI or a single uniform operating point.
- **`docs/lattice/wal.md`**, **`docs/lattice/wal-storage-providers.md`**, **`docs/lattice/configuration.md`**: `WalMaxPendingBatches` default updated everywhere; cross-links to the new `wal-tuning.md` added.
- **`docs/lattice/benchmarks.md`** now documents the `azure-throughput` real-Azure-Tables tier alongside the docker-compose scenarios and the in-process `microbench`, with entry-point gestures and a link out to the harness's deep-detail README (no content duplication).
- **`benchmark/README.md`** `azure-throughput` section refreshed from the stale ACI narrative to the current single-VM + systemd + managed-identity reality.
- **`benchmark/azure-throughput/`** source and README scrubbed of stale ACI references: comments now describe the systemd / journald topology, with socket-hygiene knob attribution generalised to "cloud NAT" so the comments stay accurate while preserving the wedge investigation's historical attribution.

### Changed - benchmark harness

- **`benchmark/azure-throughput/Silo/Program.cs`** env-var defaults for `BENCH_WAL_PARTITIONS` and `BENCH_WAL_MAX_PENDING_BATCHES` now reference `LatticeOptions.DefaultWalPartitions` / `LatticeOptions.DefaultWalMaxPendingBatches` directly, so the harness automatically tracks future library re-tunes instead of drifting from the shipping default.

### Fixed - benchmark harness

- **`benchmark/azure-throughput/scripts/run-cohort.ps1`** runner is no longer hang-vulnerable on the post-FINAL artefact fetch path: every `scp` and `journalctl` pull is routed through a bounded job-wrapped wall-clock budget (`_ScpExec` / `_SshExec`), the silo log is fetched first (it carries the cohort sample), and producer-log / sampler-CSV failures are now soft-warned rather than aborting the cohort.

---

## [6.2.0] - 2026-06-04

Lockstep minor release across the package family (`Orleans.Lattice`, `Orleans.Lattice.Replication`, `Orleans.Lattice.Replication.Grpc`, `Orleans.Lattice.Storage.AzureTable`, `Orleans.Lattice.Dashboards`). Closes the wedge-investigation campaign that opened with G-019: every shard-side and writer-side activation / flush / dispatch / forward / publish path now carries an explicit deadline with per-`(tree, shard)` counter attribution, and every public `ILattice` operator transparently absorbs the typed `ShardActivationTimeoutException` during cold-start activation races so operators no longer see a bare `TimeoutException` leaking out of routine calls. Also lands byte-accurate per-tree storage usage with an advisory WAL byte-pressure policy, plus the fix that decouples the storage-usage poller from the foreground hot path so idle trees stay cold. Safe drop-in upgrade from v6.1.3; no public-API breaks.

### Added

- **Byte-accurate storage usage and advisory WAL byte-pressure retention.** You can now query the retained storage footprint of a tree (WAL, snapshot, and leaf-state bytes) via `ILattice.GetStorageUsageAsync`, and roll the figures up across every registered tree in the cluster via the new `ILatticeAdmin.GetTotalStorageUsageAsync`. The same totals are exposed as OpenTelemetry observable gauges (`orleans.lattice.storage.wal_bytes`, `storage.snapshot_bytes`, `storage.leaf_state_bytes`, `storage.total_bytes`) plus a `storage.policy.over_threshold` 0/1 gauge, with two new counters (`storage.policy.trim_triggered`, `storage.policy.bytes_reclaimed`) and four new panels on the bundled Grafana overview dashboard. A new advisory `LatticeOptions.WalMaxRetainedBytes` ceiling lets the WAL garbage collector prioritise byte-pressure trims without ever crossing the safe consumer frontier - an over-threshold tree held up by a lagging consumer is surfaced for diagnosis rather than silently growing unbounded. The storage gauges populate **automatically** on every silo via a background poller (`LatticeOptions.StorageUsagePollInterval`, default 15 s) - you no longer have to call the storage-usage API to make the dashboards light up - and stay correct across a multi-silo cluster: each tree is counted on exactly one silo and a series expires from a silo's sink when the tree's aggregator migrates away, so a cross-silo sum never double-counts. Set `StorageUsagePollInterval` to `TimeSpan.Zero` to opt out. The advisory policy uses a hysteresis band so a tree hovering near the ceiling is not trimmed on every garbage-collection pass: byte pressure arms when retained WAL crosses `WalMaxRetainedBytes` and disarms once a trim drives retained bytes at or below `WalMaxRetainedBytes * WalBytePressureReclaimTarget` (default `0.8`), and growth that stays inside that band does not re-trigger until the ceiling is crossed again.
- **WAL writer now back-pressures honestly instead of silently absorbing into an unbounded queue when the shard cannot drain (G-026).** The shard-side `WalMaxPendingBatches` ceiling (default 8) historically bounded `WalShardGrain._inFlight` only; the writer-side per-`(tree, partition)` `WalCommitLogWriter.PartitionTracker` was unbounded, so a saturating offered rate stacked up to 18+ pending dispatches per partition (observed in the 2026-06-03 wedge cohort) with no caller-visible signal until the silo wedged. A per-partition admission semaphore, sized at `WalMaxPendingBatches` and bounded by the existing `WalAppendDispatchTimeout`, now caps writer-side depth symmetrically. When the shard cannot drain, callers receive a typed `TimeoutException` naming the admission deadline in bounded time instead of parking forever. Two new instruments make the new regime observable: `orleans.lattice.wal.writer.append.admission_timeouts` (the counter that increments when a dispatch is refused at the deadline) and `orleans.lattice.wal.writer.append.admission_wait` (the histogram that surfaces back-pressure as honest tail-latency well before any hard timeout fires). Set `WalMaxPendingBatches=0` to opt out and restore the historical unbounded-writer shape for rollback / parity testing.
- **Writer-layer per-partition pending-append dispatch is now observable, so the upstream variant of the WAL wedge is attributable without source-walking (G-025).**
- **Per-shard WAL flush lifecycle and reshard activity are now observable, so the residual phase-1/activation WAL wedge can be attributed without source-walking (G-024).**

### Changed

- **`WalCommitLogWriter` internal awaits no longer silently drop the grain context (FX-024).** The singleton writer helper is invoked from grain turns (the leaf grain's foreground commit path, the shard-root saga terminal path); the historical `.ConfigureAwait(false)` on every internal await silently dropped the caller's grain scheduler on resume, making the resume-context of each await unclear and leaving the helper one bug-fix away from breaking the single-threaded-turn invariant for any state added later. Only the four deliberate outbound `IWalShardGrain` dispatch awaits keep `.ConfigureAwait(false)`, because their catch must land off a possibly-wedged grain context so the writer-side wedge-attribution counter and log line still fire. An audit test pins the count at exactly four going forward. No behavioural change for callers.

### Fixed

- **Every public `ILattice` operator that drives the shard-root activation-readiness seed now transparently absorbs cold-start seed timeouts, not just `ReshardAsync` (FX-027).** Extends the FX-026 typed-exception envelope across the rest of the operator surface: per-key read / write / version-check / CRDT-delta / get-or-set / delete paths (via the central `RetryOnStaleRoutingAsync` helper, which now catches `ShardActivationTimeoutException` alongside the existing stale-routing exceptions), multi-shard fan-outs (per-shard wraps inside `GetManyAsync`, `SetManyAsync`, `DeleteRangeAsync`, `CountAsync`, the scan first-page init in `KeysAsync` / `EntriesAsync`, the cursor WAL-head snapshot fan-out, and `GetMaterialiserLagAsync` so a single shard's seed-timeout retries only that shard and not every sibling), per-tree coordinator entry points (`DeleteTreeAsync` / `RecoverTreeAsync` / `PurgeTreeAsync` / `ResizeAsync` / `UndoResizeAsync` / `SnapshotAsync` / `MergeAsync` / `BulkLoadAsync` / `CompactShardAsync` / `RebuildLeafProjectionAsync`), the saga path (`SetManyAtomicAsync` both overloads via `IAtomicWriteGrain`), the digest fast path (`GetLeafProjectionDigestAsync`), and the warmup probe (per-shard wrap inside `WarmUpAsync`'s shard probe). Adds a generic `ShardActivationRetry.RunAsync<T>` overload for fan-outs that produce per-shard values. Replication-apply call sites remain out of scope because the receiver-side `ReplicationApplier` already has its own retry / dead-letter envelope.
- **`ILattice.ReshardAsync` transparently absorbs cold-start activation-seed timeouts instead of surfacing them to callers (FX-026).** The G-019 shard-root activation-readiness seed deadline (`LatticeOptions.ActivationReadyTimeout`, default 15 s) is by design retriable - every cross-grain step in the seed is idempotent on retry, with the timeout existing only to release the activation gate so a retry can take the seed - but `ReshardAsync` was surfacing the bare `TimeoutException` to the caller on the first parked seed, breaking any operator that calls reshard during host startup (e.g. a startup-reshard pattern that pins a tree at N shards from day one). The seed now throws a typed `ShardActivationTimeoutException` (publicly visible, derived from `TimeoutException`, with `TreeId` / `ShardIndex` / `TimeoutSeconds` slots for attribution), and `ReshardAsync` internally retries up to three times with linear 1 s / 2 s backoff before propagating; worst-case caller-visible wall is ~48 s on defaults, well under the Orleans response deadline. The retry envelope is exposed as a reusable internal helper (`ShardActivationRetry`) ready for the wider audit of other public operators that should adopt the same shape (sub-issue tracked under FX-026).
- **`docs/lattice/configuration.md` Options Reference table now lists every `LatticeOptions` property.**
- **`docs/lattice/tombstone-compaction.md` now describes the shard-root dirty-leaf flush coalescing knob.** `LatticeOptions.DirtyLeafFlushIntervalMs` (default 50 ms) was previously undocumented anywhere in the corpus despite being the coalescing window that removes the per-`Delete` shard-root storage write from the hot path. A new subsection inside "Dirty-Leaves Fast Path" covers the in-memory mark / timer-coalesced persist design, the loss bound under unclean silo shutdown (one missed leaf per crashed activation per window, re-discovered by the legacy chain-walk fallback), and the `0`-disables-coalescing fallback to the pre-coalescing per-mark-flush shape.
- **`docs/lattice/chaos-tests.md` no longer claims some chaos tests are `[Ignore]`'d.** A grep across every `[Category("Chaos")]` fixture in `test/` returns zero `[Ignore]` attributes - the stale prose from a previous campaign where multi-silo restart and OR-Map convergence tests were ignored was removed (two fragments, both saying the same thing in different sections).
- **`ITreeReshardGrain.ReshardAsync(n)` is now an idempotent no-op when `n` equals the tree's current shard count, instead of throwing `ArgumentOutOfRangeException` (FX-023).** Hosts that unconditionally pin a tree's configured shard count on every start-up previously crashed if the tree was already at that count (`A BackgroundService has thrown an unhandled exception, and the IHost instance is stopping.`). Equal-count requests now return success and leave the shard map / manifest version unchanged; only `n < currentCount` (a genuine shrink) continues to throw, since shrink is still unsupported.
- **Storage-usage gauges no longer activate idle leaves to compute their footprint (FX-025).** The cluster-wide storage-usage poller previously fanned out through the per-tree storage-usage aggregator, which in turn activated every leaf and snapshot grain in the tree to sample their byte sizes - defeating the cold-tree assumption that idle trees stay quiescent. The poller now drives `ILatticeAdmin.PollWalUsageAsync` against a new per-tree `ILatticeWalUsage` aggregator that touches only `IWalShardGrain` activations, never a leaf, internal node, snapshot storage, or shard-root grain. Leaf-state and snapshot bytes are now incrementally tracked at the source (`LeafEntryCache.StateBytes` maintained on every store / remove / clear; `LeafSnapshotBlob.SnapshotBytes` precomputed at save time) and pushed to the owning shard root on every digest-publish commit boundary, so `ShardRootGrain.GetStorageUsageAsync` becomes a constant-time read off activation-scoped running totals. Deep leaf-state and snapshot bytes remain available on demand via `ILattice.GetStorageUsageAsync` and the operator-driven `ILatticeAdmin.RefreshStorageUsageAsync`.
- **First-activation shard seed no longer wedges the write pipeline when a registry or root-leaf RPC parks (G-019).** The one-time activation-readiness seed a shard root runs the first time a brand-new or freshly-reactivated shard prepares for an operation (the defensive state re-read, the tree-registry registration, the deterministic root-leaf init pair, and the initial shard-state write) runs while the shard-root init gate is held. During a startup reshard or membership change Orleans can park one of those cross-grain awaits - the target registry or leaf activation is not yet visible - leaving the seed neither completing nor faulting, pinning the gate, stalling every interleaved read/write turn, and wedging the whole write pipeline with no fault and no activation recycle until the caller-side response deadline expires. The seed is now bounded by a new `LatticeOptions.ActivationReadyTimeout` deadline (default 15 s): a parked seed is abandoned and the operation retried against refreshed routing once the reshard settles, with every seed step idempotent on retry so no data is lost or double-registered. A new `orleans.lattice.shard_root.activation_ready.timeouts` counter surfaces the condition. Set `ActivationReadyTimeout` to `InfiniteTimeSpan` to restore the historical unbounded await.
- **Shard projection-digest entry counts stay exact across internal-node splits (FX-022).**
- **`GetManyAsync` now returns an all-or-nothing snapshot across a mid-saga reshard (G-020).**
- **WAL flush deadline now recovers even when a storage-provider hang ignores cancellation (G-019).** The `LatticeOptions.WalFlushTimeout` deadline previously only fired if the provider observed the deadline's cancellation token; a provider call that hangs in a non-cancellable wait - a half-activated partition from a placement/reshard race, or an SDK retry loop that swallows cancellation - left the grain awaiting forever even after the deadline elapsed, so the in-flight slot never drained and the shard's append pipeline stayed wedged at `WalMaxPendingBatches`. The grain now also bounds its own wait on the provider task, so the deadline recovers the shard regardless of whether the provider honours cancellation. This closes the residual real-Azure saturation-rung wedge that survived the initial v6.1.3 fix.
- **Reshard swap-phase writes no longer wedge the pipeline when a cross-shard forward parks (G-021).** During an online reshard the destination shard's ownership changes, and Orleans can reject an outbound shard-to-shard write forward and leave the caller-side await neither completing nor faulting; the forwarding turn then never returned, the per-shard write fan-out saturated at its in-flight limit, and the whole write pipeline stalled with no fault and no activation recycle. Outbound shard forwards (the online-resize shadow forward and the adaptive-split migration forward) are now bounded by a new `LatticeOptions.ShardForwardTimeout` deadline (default 15 s): a parked forward is abandoned and the write retried against refreshed routing, while data convergence stays guaranteed by the split coordinator's authoritative leaf-chain drain. A new `orleans.lattice.shard_root.forward.timeouts` counter and a CommitPath dashboard panel surface the condition. Set `ShardForwardTimeout` to `InfiniteTimeSpan` to restore the historical unbounded await.
- **Internal-node digest upward publishes no longer wedge a B+ tree branch when a parent parks mid-mutation (G-022).** When an internal node folded a child's projection digest and republished the aggregate to its own parent, that cross-grain publish ran under the node's non-reentrant split gate with no ceiling; a parent that was itself mid-mutation could leave the publish neither completing nor faulting, holding the split gate and wedging every subsequent mutating turn on that activation. The upward publish is now bounded by a new `LatticeOptions.DigestPublishTimeout` deadline (default 15 s): a parked publish is abandoned and the turn faults with a `TimeoutException` so the split gate releases, and the staleness-tolerant digest re-converges on the next mutation's publish. A new `orleans.lattice.internal.digest_publish.timeouts` counter surfaces the condition. Set `DigestPublishTimeout` to `InfiniteTimeSpan` to restore the historical unbounded await.
- **Residual phase-1/activation WAL wedge is now bounded and attributable per `(tree, shard)` (G-023).** Despite the activation-readiness seed deadline (G-019) and the WAL flush deadline (G-019), a residual bimodal wedge survived at the Azure-Tables WAL saturation rung where every shipped deadline read zero yet `inFlight` stayed pinned at `WalMaxPendingBatches`. The wedge mechanism lives upstream of the existing deadlines and could not be attributed without source-walked instrumentation. The change ships a coordinated diagnostic pack: the writer's outbound `IWalShardGrain.AppendBatchAsync` / `AppendAsync` RPC is now bounded by a new `LatticeOptions.WalAppendDispatchTimeout` deadline (default 30 s) - converting a 180-second blind hang on the Orleans response timeout into a structured `TimeoutException` with per-shard counter attribution via `orleans.lattice.wal.append_dispatch.timeouts` - while the per-shard `WalShardGrain.FlushAsync` preflight region (the synchronous setup and initial scheduler yield that precede the provider-call deadline) is now bounded by a new `LatticeOptions.WalFlushPreflightTimeout` deadline (default 5 s), so a paused activation scheduler that never resumes the post-yield continuation is caught and the slot drains, rather than pinning `_inFlight` with no deadline armed; the trip is attributed via `orleans.lattice.wal.flush.preflight.timeouts`. A new `orleans.lattice.wal.shard.deactivate.in_flight` histogram observes `_inFlight.Count` at every `OnDeactivateAsync`, so a deactivation with non-zero in-flight count immediately followed by a preflight timeout on a successor activation directly attributes the mid-call deactivation orphan hypothesis. Each deadline can be set to `InfiniteTimeSpan` to restore the historical unbounded await.

---

## [6.1.3] - 2026-06-02

Core-library patch release (`Orleans.Lattice` only). Fixes a severe WAL durability-path defect where a hung storage-provider call could permanently wedge a shard's append pipeline, plus stabilises the Azure Table durability integration tests under the pipelined phase-2 default. No public-API breaks; adds the opt-out `LatticeOptions.WalFlushTimeout` knob. Safe drop-in upgrade from v6.1.2.

### Fixed

- **Azure Table storage-provider durability integration tests under the pipelined phase-2 default.**
- **WAL flush no longer wedges a shard when a storage-provider call hangs (G-019).** A per-shard WAL flush now runs under a bounded `LatticeOptions.WalFlushTimeout` deadline (default 15 seconds): a provider append that hangs indefinitely - for example against a partition left half-activated by a placement/reshard race - is cancelled and surfaced as a `TimeoutException` routed through the normal failure handler, which resynchronises the dense-offset tail and drains the in-flight chain. Previously such a hang pinned its in-flight slot forever, saturating the chain at `WalMaxPendingBatches` and freezing every subsequent append on that shard with no fault and no activation recycle. Set `WalFlushTimeout` to `InfiniteTimeSpan` to restore the historical unbounded await.

---

## [6.1.2] - 2026-06-01

Core-library patch release (`Orleans.Lattice` only). Fixes two topology-integrity defects on the multi-silo restart surface - a silent topology-loss on shard-root reactivation and a `CountAsync` over-count when a restart interrupts a leaf split - plus two lower-allocation CRDT primitive optimizations. No public-API or behavioral changes; safe drop-in upgrade from v6.1.1.

### Changed

- **Lower-allocation `VersionVector.Merge` / `VersionVector.Clone`.** Both now seed their backing dictionary via the `Dictionary` copy constructor (exact-capacity bulk-copy) instead of filling an empty dictionary entry-by-entry, eliminating the incremental resize churn on this hot CRDT path. `Merge` allocates ~15% less and `Clone` ~31% less per call, with `Merge` also ~18% faster at steady state. No public-API or behavioral change.
- **Lower-allocation `OrMap.Clone` (and the `OrMap.Merge` / `OrMap.MergeFrom` paths that fold through it).** `Clone` now presizes its `Adds` / `Tombstones` backing dictionaries to the source key counts before the per-key list copy, eliminating the intermediate rehash-grow allocation. Each of clone, merge, and merge-from allocates ~224 bytes less per call. No public-API or behavioral change.

### Fixed

- **Shard-root reactivation no longer clobbers a live persisted topology after a secondary-silo restart.** When a `ShardRootGrain` reactivated against not-yet-visible (empty) in-memory state during a membership change or silo restart, the first-touch root materialisation could seed a fresh single-leaf root over a shard that already held a promoted internal root and a populated leaf chain in storage, silently dropping every key under the rest of the tree (observed as a universe collapsing from 50 keys to 29 in the multi-silo restart chaos surface). The lazy root-seed path now re-reads persistent state and adopts any already-persisted topology before seeding, and serialises the seed sequence behind a per-activation gate so two interleaved first-touch turns cannot both create a root. Steady-state operations keep the zero-I/O fast path. Internal fix - no public API change.
- **`CountAsync` / per-leaf stats no longer over-count after a silo restart interrupts a leaf split.** A donor leaf stuck mid-split (its right-half rows already wired onto the new sibling but not yet removed from its own cache) is now counted only for the keys it still owns, so `ILattice.CountAsync` returns the true live key count after the cluster converges instead of double-counting the split half (observed as a universe over-counting from 50 keys to 89 in the multi-silo restart chaos surface). Internal fix - no public API change.

---

## [6.1.1] - 2026-05-30

Core-library patch release (`Orleans.Lattice` only). Fixes a foreground-write throughput regression introduced by the v6.1.0 multi-partition WAL replay work. No public-API changes; safe drop-in upgrade from v6.1.0.

### Fixed

- **WAL hot-path throughput regression introduced by the v6.1.0 multi-partition WAL replay work.** The foreground commit-log writer (`WalCommitLogWriter.RouteAsync`) resolved `WalPartitions` through the full `LatticeOptionsResolver.ResolveAsync` on every `IWalShardGrain.AppendAsync` / `AppendBatchAsync`. The resolver had no per-tree result cache and issued an `ILatticeRegistry.GetEntryAsync` grain RPC each call, so every write serialised through the cluster-singleton registry activation before the writer could fan out across partitions. Sustained `set-many` throughput at the canonical c2-iii operating point collapsed from ~13,574 entries/s to ~1,000-2,000 entries/s on Azure Tables. `LatticeOptionsResolver` now exposes a `GetWalPartitionsAsync(treeId)` fast path that memoises the tree-immutable pin per-resolver-instance and returns an already-completed `ValueTask<int>` on a cache hit; `WalCommitLogWriter.RouteAsync` calls it instead of the full resolver. `ResolveAsync` populates the same cache as a side effect so any tree touched by any caller is warm for subsequent writer calls.

---

## [6.1.0] - 2026-05-29

Minor release. **Backwards-compatible at the compiled-API level**, with one observability shape change documented under `### Breaking` below. Adds bidirectional per-peer health observability and an opt-in idle-link liveness probe to the replication package; promotes the silo-wide WAL fan-out default from `1` to `8` partitions (per-tree-pinned so already-registered trees are unaffected); and expands the chaos-test suite across all four test projects with twelve new fixtures.

### Added

- **Bidirectional `peer.last_contact_seconds` and `peer.consecutive_errors`** (`Orleans.Lattice.Replication`). Both observable gauges now carry a `direction` tag (`outbound` / `inbound`). The receiver-side `ReplicationApplier` records an inbound success or failure per per-origin run keyed on `WalRecord.OriginClusterId`, so dashboards can finally answer "when did this silo last receive from peer X?" alongside the existing "when did this silo last ship to peer X?". `peer.entries_behind` and `peer.bytes_behind` remain outbound-only (the receiver does not track a per-peer backlog into itself).
- **Outbound liveness probe** (`Orleans.Lattice.Replication`). New `LatticeReplicationOptions.LivenessProbeInterval` (default `30 s`; `Timeout.InfiniteTimeSpan` disables). When the shipper's pump tick finds no entries to ship and the interval since the last successful outbound contact has elapsed, the shipper ships an empty `ReplicationBatch` so the outbound gauge resets on healthy idle links. Activation-anchored: the first idle tick is silent, the probe begins one interval after activation. Payload is the 16-byte framing header alone.
- **New public `ReplicationContactDirection` enum** (`Outbound = 0`, `Inbound = 1`) and **additive `Direction` init-property on `ReplicationPeerSnapshot`** (defaulted to `Outbound` - existing positional-constructor call sites are bit-identical). New public methods `ReplicationPeerStats.RecordInboundSuccess` / `RecordInboundError` for receiver-side recording.
- **Inbound health-check tier** (`Orleans.Lattice.Replication`). New `LatticeReplicationHealthCheckOptions.InboundDegradedAfter` / `InboundCriticalAfter` (both default `Timeout.InfiniteTimeSpan` - opt-in). Inbound-stale rows escalate on the same per-`(tree, peer)` first-degraded-since ladder the outbound thresholds use; inbound rows surface in the `degradedPeers` / `unhealthyPeers` arrays with a ` (inbound)` label suffix.
- **Bundled Grafana dashboard updates** (`Orleans.Lattice.Dashboards`). Panel #11 retitled to "Per-peer last outbound ship (seconds ago)" with `direction="outbound"` matcher; consecutive-errors panel breaks down by `direction`; new panel #20 "Per-peer last inbound apply (seconds ago)" mirrors the outbound view filtered by `direction="inbound"`.
- Multi-partition WAL replay on leaf activation. The silo-wide default for `LatticeOptions.WalPartitions` flips from `1` to `8`; existing trees pin the value in force at first WAL write into the tree registry, so the default change is non-breaking for already-registered trees. New trees pick up the new default and have every leaf's activation-time materialiser fan out across `[0, WalPartitions)` partitions with a two-pass replay (per-partition Set/Delete absorption with TxCommit / TxAbort / DeleteRange deferred until every partition's pending-tx record is populated, then drained) and a post-pass per-partition checkpoint reconciliation that advances each partition's `ProjectionCheckpointOffsetsByPartition` to the highest applied offset once the saga-prepare clamp lifts. The snapshot cursor and the operator projection-admin paths fan out alongside: `LatticeSnapshotCoordinate` gains an additive `[Id(3)] PerShardPerPartitionWalOffsets` slot (legacy scalar preserved via max-of-partitions), `ISnapshotLeafGrain.OpenAsync` takes per-partition captured offsets and the materialiser filters by shard index to prevent sibling-shard data absorption, and `IShardRootGrain.SnapshotWalHeadAsync` returns `long[]` (per-partition heads) with `GetShardMaterialiserLagAsync` summing per-partition lags. New wire-additive `LeafNodeState.ProjectionCheckpointOffsetsByPartition` slot persists per-partition checkpoints; per-partition cursor consumer ids (`_lattice_materialiser_{treeId}_{leafGrainId}_{partition}`) let the per-shard WAL GC trim each partition under its own slowest consumer. New `LatticeOptions.WalPartitions >= 1` option validator. **`LatticeReplicationOptions.DefaultReplogPartitions` flips from `1` to `8`** in lockstep so the replication shipper reads every partition the commit-log writer fanned across (without alignment the shipper would miss 7/8 of writes).
- Per-tree `WalPartitions` pin on `TreeRegistryEntry` (additive `[Id(9)] int?`). Stamped at first `ILatticeRegistry.RegisterAsync` from the silo's then-current `LatticeOptions.WalPartitions` value; never mutated thereafter. `LatticeOptionsResolver` reads the pin in preference to the live `IOptionsMonitor<LatticeOptions>` value, so the resolved `WalPartitions` seen by every grain is tree-immutable for the lifetime of the tree - the foreground commit-log writer and the activation-time materialiser always agree on the partition fan-out shape regardless of what the silo's currently-configured value is. System trees bypass the registry and hardcode `WalPartitions = LatticeConstants.DefaultSystemTreeWalPartitions` (`1`) so they stay on the single-partition shape regardless of the silo-wide user-tree default.
- **Expanded chaos-test coverage.** Twelve new chaos fixtures across the four test projects pin previously-unverified invariants: range-delete exclusivity under concurrent writers, `SetIfVersionAsync` (CAS) linearisable winners under contention, `ScanAsync` cooperative cancellation, two-silo restart under load (currently `[Ignore]`'d pending a grain-type-discrimination fix tracked on GitHub Issues), producer-side WAL trim cannot prune un-acked entries, per-peer liveness probe + receiver-side inbound-error counters under partition-and-heal, OR-Map convergence (currently `[Ignore]`'d pending a chaos-pump dedupe gap), tombstone-reap envelopes never crossing the producer boundary under compaction+shipping churn, gRPC transport convergence under transient channel faults, and Azurite-backed WAL append/trim under concurrent load with storage faults. New `ProductionShipperFixture` provides a real `AddLatticeReplication` + in-process loopback transport harness with partition / heal / batch-observation / receiver-fault-injection hooks. The `IsTransient` predicates used to classify retry-eligible exceptions on chaos hot paths now cover the documented exhaustion classes (CAS-budget exhaustion, stale routing) that production callers already absorb.

### Breaking

- **Doubled series on `peer.last_contact_seconds` and `peer.consecutive_errors`.** Hosts that opt into both directions see two series per `(tree, peer)` pair (one outbound, one inbound). Dashboards that previously matched these gauges without filtering by `direction` must add `direction="outbound"` to preserve the pre-bidirectional shape, or accept the doubled series. Metric names and units are unchanged. `peer.entries_behind` / `peer.bytes_behind` are unaffected.

---

## [6.0.1] - 2026-05-29

Minor release. **Backwards-compatible at the wire level.** Defaults flips on three configuration knobs lift the out-of-the-box throughput at the Azure-Tables operating point measured by the post-v6.0.0 throughput campaign; hosts that depend on the pre-v6.0.1 behaviour opt out explicitly per knob. One previously-deferred `Orleans.Lattice.Dashboards` rebuild surfaces 34 instruments that shipped without any operator-visible panel on v6.0.0.

### Changed - configuration defaults (opt-out)

- **`LatticeOptions.WalMaxPendingBatches`: `1` -> `8`.** The per-shard WAL grain's in-flight-batch cap. Raises the pipeline depth between the producer and the storage provider's flush envelope; pairs with the `PhaseTwoCoalescingWindow` flip so the worker's 49-row coalescing window has enough arrivals to engage. Hosts that depend on strict-serial-per-shard append shape opt out by setting `WalMaxPendingBatches = 1` explicitly.
- **`LatticeOptions.DigestCoalescingWindowMs`: `0` -> `5` ms.** The leaf-side projection-digest publish window. Defers the cross-grain `OnChildDigestPublishedAsync` hop behind a one-shot grain timer; mutations arriving within the window collapse into one publish. The running `ProjectionHash` on persisted state still advances per-mutation, so cold-reactivation replay is bit-identical to the synchronous shape. Hosts that depend on the read-after-write digest invariant opt out by setting `DigestCoalescingWindowMs = 0`.
- **`AzureTableWalStorageOptions.PhaseTwoCoalescingWindow`: `TimeSpan.Zero` -> `5 ms`.** The worker's first-arrival hold window before a phase-2 commit fires. Lets the worker coalesce multiple commits into one Azure-Tables transaction when arrivals overlap; pairs with `PipelinePhaseTwoCommits` and `WalMaxPendingBatches` so the upstream pipeline actually produces overlapping arrivals. Hosts that depend on commit-on-first-arrival opt out by setting `PhaseTwoCoalescingWindow = TimeSpan.Zero`.
- **`AzureTableWalStorageOptions.PipelinePhaseTwoCommits`: `false` -> `true`.** Returns the caller as soon as the *previous* batch's phase-2 commit lands; the current batch's phase-2 runs asynchronously through the same per-shard worker. Phases 0 and 1 remain synchronous and durable, so the activation-time reconciler contract is unchanged - the only observable change is that a phase-2 failure surfaces on the *next* `AppendBatchAsync` rather than the failing one. Hosts that need failure-on-the-failing-call semantics (or that run against a single-writer backend like Azurite where overlapping writes contend rather than overlap) opt out by setting `PipelinePhaseTwoCommits = false`.

**Multi-partition WAL replay shipped on `[Unreleased]`.** `LatticeOptions.WalPartitions` now defaults to `8` (the throughput-campaign sweet spot) and `LatticeReplicationOptions.DefaultReplogPartitions` follows in lockstep. The activation-time WAL replay loop on `BPlusLeafGrain` fans across every configured partition (two-pass replay with post-pass per-partition checkpoint reconciliation), the snapshot cursor and projection-admin paths fan out alongside, and the tree-registry pin keeps every grain agreeing on the partition fan-out shape for the lifetime of the tree.

### Added - operator-visible surface

- **34 new metrics surfaced in `Orleans.Lattice.Dashboards`.** The `CommitPath` dashboard gained 13 panels (SetAsync / SetManyAsync envelope p50/p95, per-stage sub-attribution for both, ShardRoot per-step, WAL append decomposition + batch shape + back-pressure, storage-provider phase-2 + retries, `leaf.commit.in_flight`, `WarmUpAsync`, digest-coalescing efficacy). The `AtomicWrites` dashboard gained 4 panels (per-phase saga durations, broadcast sub-attribution, per-key work vs serial-gap wait, fan-out size distribution). All 34 referenced metrics resolve via the existing drift-guard test.
- **`orleans.lattice.leaf.digest.publishes` counter** tagged `tree` + `path` (`coalesced_scheduled` / `coalesced_skipped` / `coalesced_fired` / `inline` / `deactivation_flush`). Closes the behavioural-attribution gap for the `DigestCoalescingWindowMs` flip - operators can now confirm the coalescing path is actually firing on Azure rather than silently regressing to the pre-c2-xxix resolver-drop shape.
- **`AzureTableWalStorageOptions.DefaultPhaseTwoCoalescingWindow`** and **`DefaultPipelinePhaseTwoCommits`** static-readonly fields so hosts can reference the shipping defaults symbolically. Each is pinned by a dedicated unit test (`PhaseTwoCoalescingWindow_defaults_to_five_ms`, `PipelinePhaseTwoCommits_default_value_is_true`).

### Added - WAL throughput

- **`AzureTableWalStorageOptions.EliminateCandidateRowOnHotPath`** - opt-in WAL throughput optimisation that skips the phase-0 candidate-row write on every `AppendBatchAsync` and shrinks the phase-2 transaction by one action. Off by default; orphan recovery is preserved by an additional batch-partition scan above `TAIL` in `ReconcileAsync`. See `docs/lattice/wal-storage-providers.md` for the downgrade-safety note.

### Added - benchmark harness

- **`benchmark/azure-throughput/`** - real-Azure Storage WAL throughput harness (two-container ACI deployment) brought into the benchmark family. Exposes `BENCH_WAL_ELIMINATE_CANDIDATE_ROW`, `BENCH_WAL_PARTITIONS`, `BENCH_WAL_MAX_PENDING_BATCHES`, `BENCH_WAL_PHASE2_COALESCING_WINDOW_MS`, `BENCH_FLUSH_CONCURRENCY`, `BENCH_SHARD_COUNT`, `BENCH_FLUSH_MS`, `BENCH_DIGEST_COALESCING_WINDOW_MS`, and `BENCH_RESPONSE_TIMEOUT_SEC` so the same single-silo harness can A/B every defaults-flip candidate against a real Azure Tables account. The bundled ladder driver and Phase-A attribution reporter were brought into this branch alongside the harness.

### Changed - documentation

- **`docs/lattice/wal-storage-providers.md`** updated for both Azure-Tables default flips: the `PipelinePhaseTwoCommits` section is now framed as "as of v6.0.1 the default is `true`" with the opt-out conditions enumerated, and the comparison table relabels "Default" / "Pipelined" as "Pre-v6.0.1 / opt-out (`= false`)" / "v6.0.1 default / pipelined (`= true`)".
- **`docs/lattice/metrics.md`** documents the new `orleans.lattice.leaf.digest.publishes` counter including the c2-xxix regression signature it catches, and the expanded scope of the `CommitPath` and `AtomicWrites` dashboards.
- **`docs/lattice/performance-single-silo.md`** notes the multi-partition WAL replay default flip - the Layer 2 write-path cells now reflect the shipping default-configured silo at `WalPartitions = 8`.
- **`docs/lattice/wal.md`** notes the `PhaseTwoCoalescingWindow` + `PipelinePhaseTwoCommits` flips as the new default operating point.
- Bundled Grafana dashboard panel **`Per-peer last contact (seconds ago)`** in `OrleansLatticeReplication.json` renamed to **`Per-peer last outbound ship (seconds ago)`** and gained an inline description explaining the outbound-only scope and the empty-tick climb behaviour, after operator-reported confusion on the MultiSiteManufacturing sample. The corresponding gauge docstring on `LatticeReplicationMetrics.LastContactSecondsName` was expanded with the same directional-scope explanation. A bidirectional rework (inbound twin + liveness probe) is tracked on the replication roadmap as **R-121**.

### Added - roadmap

- **`F-077 - Multi-partition WAL replay on leaf activation`** shipped on `[Unreleased]` (entry retained as the audit trail for the roadmap addition; the corresponding behavioural change is described in the `### Added` section above).
- **`R-121 - Bidirectional peer.last_contact_seconds (inbound twin + liveness probe)`** added to the replication roadmap with a full scope estimate covering both the outbound liveness probe and the receiver-side inbound recording.

### Fixed

- **CS9107 warning on `AtomicWriteGrain`.** The primary-constructor `context` parameter was being captured into the enclosing type for one call site while also being passed to the base `TtlGrain` constructor. The captured-and-base-passed double-use is replaced with a single `GrainContext.ActivationServices` call. No behavioural change; warning count drops from 1 to 0.

### Migration notes

- **No wire-format or persisted-state changes.** v6.0.1 is a drop-in upgrade from v6.0.0 with the four defaults-flip caveats above.
- **Hosts that depend on the pre-v6.0.1 defaults** explicitly opt out per knob - no other action required.
- **Hosts running against a single-writer WAL backend** (Azurite, single-shard configurations, or any WAL whose effective write width is one) should explicitly set `PipelinePhaseTwoCommits = false` because the table in `docs/lattice/wal-storage-providers.md` shows that mode is bounded by backend write concurrency.

---

## [6.0.0] - 2026-05-22

Major release. **Not backwards-compatible.** The on-wire WAL format, the persisted leaf state shape, the replication transport contract, and the public API surface have all changed since the v5.0.x line. There is no in-place upgrade from any v5.x deployment - tenants must drain and reseed Azure Table WAL storage, and every cross-cluster replication peer must be on v6.0.0 before traffic resumes. The historical v5.0.0 / v5.0.1 / v5.1.0 NuGet tags shipped only the F-052 operator-tooling slice on top of the v4.1.1 baseline; everything else listed below is new since that line.

### Breaking - on-wire and persisted format

- **`WalRecord.Value` dropped from CRDT-mode WAL and wire payloads (R-119).** CRDT entries now carry only the typed delta; the full-state serialisation is no longer emitted on the producer side. LWW entries are unaffected in shape. v5.x receivers do not understand the new shape - cross-cluster peers must be upgraded in lockstep.
- **Typed-delta wire collapse and OR-Map replication (R-035).** Replication envelopes carry the typed delta directly rather than the fold-then-reserialise round trip the v5.x line used.
- **Per-row WAL payload trims (R-114, R-115, R-116, R-117).** `TreeId`, `Mode`, `ShardIndex`, and the per-entry origin envelope are stripped from the on-wire bytes; the receiver reconstructs them from the envelope context. The on-disk WAL rows are not readable by a v5.x reader.
- **One-encode commit-to-wire end-to-end (R-114 stage 5, folding R-115).** The producer-side encode happens once and the same bytes flow from leaf-commit through WAL append to replication ship.
- **Azure Table WAL wire format harmonised with the in-memory baseline (R-079).** The on-disk row layout under `WalEntry` is the canonical shape; existing Azure Table data must be drained or migrated before upgrading.
- **Framing-tail compression for replication envelopes (R-043)** with a reusable `ILatticeCompressor` core seam. Compression negotiation is required between v6 peers.

### Breaking - persisted leaf state shape

- **Delta-only producer-side CRDT state model (R-120).** The leaf-as-`byte[]` model is replaced with a leaf-as-delta-journal-plus-cache model. The persisted `LeafNodeState.Entries` and `LeafNodeState.LiveCount` slots are gone; the runtime-owned `LeafEntryCache` is rebuilt from the WAL on every activation. Existing v5.x persisted leaf state is not loadable - tenants must reseed via WAL replay against a clean leaf state row.
- **Leaf snapshot safety net.** New `ILeafSnapshotStorageGrain` captures a point-in-time projection when a leaf's `ProjectionCheckpointOffset` approaches WAL retention; reactivation prefers the snapshot over from-scratch replay when the snapshot offset is strictly newer than the persisted checkpoint.

### Breaking - public API surface

- **`IReplicationTransport` typed-envelope shape (R-047).** The transport contract exchanges typed envelopes, not opaque `byte[]`. Hosts implementing a custom transport must migrate to the typed seam.
- **`ITypedReplicationTransport` is the canonical transport contract.** Receiver-side per-entry shard RPCs collapse into batched `ApplyMergeManyAsync`.
- **`IReplicationTopology` widened to govern doorbell and fall-off probes (R-113).** Topology providers now answer both routing and fall-off-probe authority questions.
- **Zero-copy WAL provider contract via `IWalMutationEncoder` (R-076).** `IWalStorageProvider` implementations receive a writer rather than an allocated byte array; existing providers must migrate.
- **`ILattice.ApplyCrdtDeltaAsync(string, LatticeMergeMode, byte[], CancellationToken)`** added for hosts that pre-compute typed deltas off-grain and want to commit them through the same producer-side path. The producer-side delta-apply seam is the new authoritative write path.
- **Per-tree OR-Map shape registration** via `ISiloBuilder.AddOrMapShape<TKey, TValue>(string)`; closed-shape modes resolve through the registry's global fallback.

### Added - CRDT primitives

- **MV-Register primitive and accessor (F-039)** with receiver-side replication dispatch (R-034).
- **OR-Map CRDT primitive and accessor (F-040)** - observed-remove map with recursive CRDT-value merge.
- **RGA sequence primitive and accessor (F-041)** - Replicated Growable Array for collaborative ordered lists and text.

### Added - replication and transport

- **Receiver-side flow control (R-062)** - back-pressure policy hook on the apply side.
- **Back-pressure `IHealthCheck` for the replication shipper (R-065)**.
- **Observable replication topology with runtime peer changes (R-066)** - topology can be updated without a restart.

### Added - storage and WAL

- **Pipelined phase-2 commit on the Azure Table WAL provider (F-070)**.
- **Batch WAL append on the leaf write path (F-069)**.
- **Pre-built `TableServiceClient` slot on `AzureTableWalStorageOptions` (R-112)** - hosts can hand a pre-configured client to the provider.

### Added - compaction

- **Compaction policy controls and telemetry (F-071)**.
- **Configurable compaction tick cadence (F-072)**.
- **Intra-shard leaf-walk batching for tombstone compaction (F-073)**.
- **Shard-root dirty-leaf tracking for compaction (F-074)**.

### Added - tuning and release infrastructure

- **`LatticeOptions.LeafSnapshotMargin`** (default `0.30`) and **`LatticeOptions.LeafSnapshotReClassifyEveryNCheckpoints`** (default `64`) to tune the snapshot-on-fall-off trigger.
- **`CHANGELOG.md`** - canonical per-version release notes for the package family, governed by [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) discipline going forward.
- **`docs/RELEASING.md`** - per-package tag-and-publish protocol documenting the release-engineering contract the publish workflow expects (tag shapes, per-package push order, accidental-bulk-push recovery, historical-tag backfill guidance).

### Changed

- **Exact byte accounting in WAL pending-batch sizing (R-075)**.
- **Trim-aware WAL live-entry accounting (R-077)**.
- **Multi-batch in-flight WAL flush concurrency (R-074)**.
- **Dead shipper-side encode eliminated on typed transports (R-078)** - the typed transport never re-encodes a payload the producer already emitted.
- **Package family realigned on a single major version.** `Orleans.Lattice.Replication`, `Orleans.Lattice.Replication.Grpc`, `Orleans.Lattice.Storage.AzureTable`, and `Orleans.Lattice.Dashboards` move from `5.0.1` to `6.0.0` in lockstep with the core package.

### Fixed

- **Post-restart projection drop.** The runtime-owned `LeafEntryCache` is rebuilt from the WAL on every activation. The persisted `ProjectionCheckpointOffset` survives across activations, so trusting it after a cold start would cause WAL replay to read only `(checkpoint, head]` and silently drop every offset `<= checkpoint` from the rebuilt cache. Activation now computes a local replay-start override when no snapshot rehydrated and the cache is empty, so the replay covers the entire readable window. The persisted slot is never mutated by activation; the override is local to the replay loop only, preserving the public empty-tree digest contract.
- **`LwwValue.Merge` tie-break instability on equal HLC.** Concurrent writes with identical HLC timestamps no longer hash-order non-deterministically; ties are broken by `(OriginClusterId, payload bytes)` lexicographic order for byte-exact convergence across replicas.
- **Bounded retry on `StaleShardRoutingException`** across the public `ILattice` surface. Reshards and merges no longer surface a transient routing exception to user code; the public surface retries with a small bounded budget and only surfaces if the budget is exhausted.

### Migration notes

- **There is no in-place upgrade from v5.x.** The persisted leaf state shape, the WAL on-disk format, the WAL on-wire format, and the replication transport envelope shape have all changed.
- **Coordinated upgrade required across replication peers.** A v6.0.0 producer cannot ship to a v5.x receiver and a v5.x producer cannot ship to a v6.0.0 receiver. Schedule a maintenance window, drain replication queues, upgrade all peers in lockstep, then resume.
- **Azure Table WAL tenants must drain or reseed.** Existing rows under the configured `WalEntry` table are not readable by the v6 reader. The recommended path is to quiesce writes, drain replication, drop the WAL table, and let v6 reseed from leaf snapshots.
- The migration guide (core roadmap **F-021**) tracks the full upgrade runbook.

---

## Prior to v6.0.0

> ## **Do not use**
>
> All pre-v6.0.0 releases (v0.x through v5.1.0) are superseded and unsupported. They predate the v6 wire, persisted-state, and public-API contracts and have no upgrade path forward. New consumers must start on v6.0.0 or later; existing v5.x deployments must drain and reseed per the v6.0.0 migration notes above.

The v3.x, v4.x, and v5.x release lines are best read from the per-tag GitHub Release pages

| Package | Last v5.x | Last v4.x |
|---|---|---|
| `Orleans.Lattice` | `5.1.0` | `4.1.1` |
| `Orleans.Lattice.Replication` | `5.0.1` | `4.0.0` |
| `Orleans.Lattice.Replication.Grpc` | `5.0.1` | `4.0.0` |
| `Orleans.Lattice.Storage.AzureTable` | `5.0.1` | `4.0.0` |
| `Orleans.Lattice.Dashboards` | `5.0.1` | `4.0.0` |

The v5.0.0 / v5.0.1 / v5.1.0 line shipped on top of `lattice-v4.1.1` and added only the F-052 operator-tooling slice (projection rebuild and materialiser lag). Every other entry in the v6.0.0 section above is genuinely new since v5.1.0 and was not previously published under any v5.x tag.

From v6.0.0 onward this file is the authoritative changelog, governed by [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) discipline.

---
[Unreleased]: https://github.com/NSTA1/Orleans.Lattice/compare/v6.2.0...HEAD
[6.2.0]: https://github.com/NSTA1/Orleans.Lattice/compare/v6.1.3...v6.2.0
[6.1.3]: https://github.com/NSTA1/Orleans.Lattice/compare/v6.1.2...v6.1.3
[6.1.2]: https://github.com/NSTA1/Orleans.Lattice/compare/v6.1.1...v6.1.2
[6.1.1]: https://github.com/NSTA1/Orleans.Lattice/compare/v6.1.0...v6.1.1
[6.0.1]: https://github.com/NSTA1/Orleans.Lattice/compare/v6.0.0...v6.0.1
[6.0.0]: https://github.com/NSTA1/Orleans.Lattice/compare/lattice-v5.1.0...v6.0.0