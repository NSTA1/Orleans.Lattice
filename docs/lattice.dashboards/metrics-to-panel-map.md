# Metric-to-panel coverage map

Every instrument on the `orleans.lattice` and `orleans.lattice.replication` meters is referenced by at least one panel in the bundled dashboards. The drift-guard test in `Orleans.Lattice.Dashboards.Tests` enforces the inverse direction: every metric name a dashboard references must resolve to a live instrument.

The add-on `orleans.lattice.auth` and `orleans.lattice.membership` meters are charted by the bundled Identity & Authorization dashboard. Their coverage is enforced from the owning packages: `Orleans.Lattice.Auth.Tests` and `Orleans.Lattice.Membership.Tests` each derive from the shared `MeterDashboardCoverageTestsBase`, which asserts every instrument on the meter is referenced by that dashboard (and that every token the dashboard references for the meter resolves to a live instrument).

The add-on `orleans.lattice.backup` meter is charted by the bundled Backup & Restore dashboard. Its coverage is enforced the same way, from `Orleans.Lattice.Backup.Tests` (deriving from `MeterDashboardCoverageTestsBase`).

The add-on `orleans.lattice.scaling` meter is charted by the bundled Autoscaling Signal dashboard. Its coverage is enforced the same way, from `Orleans.Lattice.Scaling.Tests` (deriving from `MeterDashboardCoverageTestsBase`).

The add-on `orleans.lattice.replication.grpc` meter is charted by the bundled Replication Transport (gRPC) dashboard. Its coverage is enforced the same way, from `Orleans.Lattice.Replication.Grpc.Tests` (deriving from `MeterDashboardCoverageTestsBase`).

## `orleans.lattice` meter

| Instrument | Type | Tags | Dashboard | Panel(s) |
|------------|------|------|-----------|----------|
| `orleans.lattice.shard.reads` | counter | `tree`, `shard` | Overview | Cluster throughput |
| `orleans.lattice.shard.writes` | counter | `tree`, `shard` | Overview | Cluster throughput, Per-tree write throughput |
| `orleans.lattice.shard.splits_committed` | counter | `tree`, `shard` | Overview | Splits committed |
| `orleans.lattice.leaf.write.duration` | histogram (ms) | `tree` | Overview, CommitPath | Leaf write duration percentiles |
| `orleans.lattice.leaf.scan.duration` | histogram (ms) | `tree`, `operation` | Overview | Leaf scan duration p95 by operation |
| `orleans.lattice.leaf.compaction.duration` | histogram (ms) | `tree` | Overview, CommitPath | Compaction duration p95 |
| `orleans.lattice.leaf.tombstones.created` | counter | `tree` | Overview, CommitPath | Tombstone churn |
| `orleans.lattice.leaf.tombstones.reaped` | counter | `tree` | Overview, CommitPath | Tombstone churn |
| `orleans.lattice.leaf.tombstones.expired` | counter | `tree` | Overview, CommitPath | Tombstone churn (TTL) |
| `orleans.lattice.compaction.pass.duration` | histogram (ms) | `tree`, `trigger` | Overview | Compaction pass duration p95 by trigger |
| `orleans.lattice.compaction.leaves.visited` | counter | `tree`, `outcome`, `trigger` | Overview | Compaction leaves visited (rate, by outcome) |
| `orleans.lattice.compaction.shard.retries` | counter | `tree` | Overview | Compaction shard retries / skips |
| `orleans.lattice.compaction.shard.skipped` | counter | `tree` | Overview | Compaction shard retries / skips |
| `orleans.lattice.leaf.tombstone.ratio` | histogram (`{ratio}`) | `tree` | Overview | Leaf tombstone ratio p95 |
| `orleans.lattice.leaf.splits` | counter | `tree` | Overview | Splits committed |
| `orleans.lattice.leaf.commit.duration` | histogram (ms) | `tree`, `step` | CommitPath | Commit-step latency p50/p95/p99 |
| `orleans.lattice.cache.hits` | counter | `tree` | Overview | Cache hit ratio |
| `orleans.lattice.cache.misses` | counter | `tree` | Overview | Cache hit ratio |
| `orleans.lattice.atomic_write.completed` | counter | `tree`, `outcome` | Overview, AtomicWrites | Atomic write outcomes (rate); per-tree committed throughput; saga failure rate (failed + compensated / total); range-window non-committed saga count |
| `orleans.lattice.atomic_write.duration` | histogram (ms) | `tree`, `outcome` | Overview, AtomicWrites | Saga duration p50/p95/p99; saga duration p95 by outcome |
| `orleans.lattice.atomic_write.batch_size` | histogram (`{entry}`) | `tree`, `outcome` | Overview, AtomicWrites | Batch size p50/p95/p99; batch size p95 by outcome |
| `orleans.lattice.coordinator.completed` | counter | `tree`, `kind` | Overview | Coordinator completions |
| `orleans.lattice.tree.lifecycle` | counter | `tree`, `kind` | Overview | Tree lifecycle events (annotation + stat) |
| `orleans.lattice.events.published` | counter | `tree`, `kind` | Overview | Events published |
| `orleans.lattice.events.dropped` | counter | `tree`, `reason` | Overview | Events dropped |
| `orleans.lattice.config.changed` | counter | `tree`, `config` | Overview | Runtime config changes |
| `orleans.lattice.storage.wal_bytes` | observable gauge (`By`) | `tree` | Overview | Storage footprint by tree |
| `orleans.lattice.storage.snapshot_bytes` | observable gauge (`By`) | `tree` | Overview | Storage footprint by tree |
| `orleans.lattice.storage.leaf_state_bytes` | observable gauge (`By`) | `tree` | Overview | Storage footprint by tree |
| `orleans.lattice.storage.total_bytes` | observable gauge (`By`) | `tree` | Overview | Cluster total retained bytes |
| `orleans.lattice.storage.policy.over_threshold` | observable gauge (0/1) | `tree` | Overview | Trees over advisory threshold |
| `orleans.lattice.storage.policy.trim_triggered` | counter | `tree`, `reason` | Overview | Byte-pressure trim activity |
| `orleans.lattice.storage.policy.bytes_reclaimed` | counter (`By`) | `tree` | Overview | Byte-pressure trim activity |
| `orleans.lattice.admission.live_keys` | observable gauge (`{key}`) | `tree` | Overview | Admission - live keys by tree |
| `orleans.lattice.admission.estimated_bytes` | observable gauge (`By`) | `tree` | Overview | Admission - estimated bytes by tree |
| `orleans.lattice.admission.over_advisory` | observable gauge (0/1) | `tree` | Overview | Admission - trees over advisory ceiling |
| `orleans.lattice.admission.would_reject` | counter (`{write}`) | `tree`, `dimension` | Overview | Admission - would-reject rate (advisory dry-run) |
| `orleans.lattice.admission.utilization` | observable gauge (ratio) | `tree`, `dimension` | Overview | Admission - utilization by dimension |
| `orleans.lattice.admission.rejected` | counter (`{write}`) | `tree`, `dimension` | Overview | Admission - rejected write rate (enforced) |
| `orleans.lattice.leaf.replay.duration` | histogram (ms) | `tree`, `outcome` | CommitPath | Activation replay duration by outcome |
| `orleans.lattice.leaf.replay.entries` | counter | `tree`, `outcome` | CommitPath | Replay entries (applied vs skipped) |
| `orleans.lattice.shard_root.forward.timeouts` | counter | `tree` | CommitPath | Shard-forward timeouts (reshard swap-phase wedge guard) |
| `orleans.lattice.wal.writer.append.admission_saturation_refusals` | counter | `tree` | CommitPath | WAL writer admission & dispatch (rate) |
| `orleans.lattice.wal.writer.append.admission_timeouts` | counter | `tree` | CommitPath | WAL writer admission & dispatch (rate) |
| `orleans.lattice.wal.writer.append.dispatched` | counter | `tree` | CommitPath | WAL writer admission & dispatch (rate) |
| `orleans.lattice.wal.writer.append.drain.releases` | counter | `tree` | CommitPath | WAL writer admission & dispatch (rate) |
| `orleans.lattice.wal.writer.append.admission_wait` | histogram (ms) | `tree` | CommitPath | WAL writer admission wait p50/p95/p99 |
| `orleans.lattice.wal.writer.partition.pending_appends` | histogram (`{dispatch}`) | `tree`, `partition` | CommitPath | WAL writer partition pending appends |
| `orleans.lattice.wal.shard.pending_segments` | histogram (`{segment}`) | `tree` | CommitPath | WAL shard backlog |
| `orleans.lattice.wal.shard.deactivate.in_flight` | histogram (`{slot}`) | `tree` | CommitPath | WAL shard backlog |
| `orleans.lattice.wal.shard.drain.budget.force_faulted_slots` | histogram (`{slot}`) | `tree` | CommitPath | WAL shard backlog |
| `orleans.lattice.wal.shard.drain.budget.expirations` | counter | `tree` | CommitPath | WAL shard drain budget & flush calls |
| `orleans.lattice.wal.shard.start_flush.calls` | counter | `tree` | CommitPath | WAL shard drain budget & flush calls |
| `orleans.lattice.wal.append_dispatch.timeouts` | counter | `tree` | CommitPath | WAL flush / dispatch timeouts |
| `orleans.lattice.wal.flush.preflight.timeouts` | counter | `tree` | CommitPath | WAL flush / dispatch timeouts |
| `orleans.lattice.provider.phase2.commit.timeouts` | counter | `tree` | CommitPath | Provider commit timeouts & retry short-circuits |
| `orleans.lattice.provider.retry.short_circuited` | counter | `tree` | CommitPath | Provider commit timeouts & retry short-circuits |
| `orleans.lattice.provider.phase1.transient_retries` | counter | - | CommitPath | Storage-provider retries (attempts vs exhausted vs idempotent-replays vs phase1-transient) |
| `orleans.lattice.shard.digest_reads` | counter | `tree`, `shard` | CommitPath | Digest reads & publish timeouts |
| `orleans.lattice.internal.digest_publish.timeouts` | counter | `tree` | CommitPath | Digest reads & publish timeouts |
| `orleans.lattice.shard_root.reshard.initiated` | counter | `tree` | CommitPath | Reshard activity |
| `orleans.lattice.shard_root.reshard.completed` | counter | `tree` | CommitPath | Reshard activity |
| `orleans.lattice.shard_root.reshard.rejected` | counter | `tree`, `reason` | CommitPath | Reshard activity |
| `orleans.lattice.shard_root.activation_ready.timeouts` | counter | `tree` | CommitPath | Reshard activity |
| `orleans.lattice.shard_root.reshard.in_flight` | histogram (`{reshard}`) | `tree` | CommitPath | Reshard runs in flight |
| `orleans.lattice.materialiser.pin.durable_writes` | counter | `tree`, `outcome` | CommitPath | Leaf-materialiser durable pin path (issue #1030) |
| `orleans.lattice.leaf.activation_replays` | counter | `tree` | CommitPath | Leaf-materialiser durable pin path (issue #1030) |
| `orleans.lattice.leaf.activation_cursor_publish_failures` | counter | `tree` | CommitPath | Leaf-materialiser durable pin path (issue #1030) |
| `orleans.lattice.materialiser.drain_lag` | histogram (ms) | `tree` | CommitPath | Leaf-materialiser drain lag p50/p95 (issue #1030 back-pressure) |
| `orleans.lattice.snapshot.replay.entries` | counter | `tree` | Overview | Snapshot replay throughput |
| `orleans.lattice.snapshot.replay.duration` | histogram (ms) | `tree` | Overview | Snapshot replay duration p50/p95/p99 |
| `orleans.lattice.snapshot.pins` | up/down counter | `tree` | Overview | Snapshot pins (current) |
| `orleans.lattice.split.retroactive_forward.entries` | counter | `tree` | Overview | Retroactive split-forward throughput |
| `orleans.lattice.split.retroactive_forward.duration` | histogram (ms) | `tree` | Overview | Retroactive split-forward duration p50/p95/p99 |
| `orleans.lattice.split.in_flight` | histogram (`{split}`) | `tree` | Overview | Autonomic split admission (cluster gate) |
| `orleans.lattice.split.candidates_suppressed` | counter | `tree` | Overview | Autonomic split admission (cluster gate) |
| `orleans.lattice.split.admission.deferred` | counter | `tree`, `reason` | Overview | Autonomic split admission (cluster gate) |
| `orleans.lattice.compaction.shard.dirty_leaves` | histogram (`{leaf}`) | `tree` | Overview | Compaction dirty leaves per pass |
| `orleans.lattice.compress.dictionary.training_runs` | counter | `outcome` | Overview | Auto-trained dictionary - training runs by outcome |
| `orleans.lattice.compress.dictionary.active_version` | observable gauge | - | Overview | Auto-trained dictionary - active version |
| `orleans.lattice.compress.dictionary.reservoir_fill` | observable gauge | `kind` | Overview | Auto-trained dictionary - reservoir fill |
| `orleans.lattice.compress.dictionary.trained_bytes_in` | counter (`By`) | - | Overview | Auto-trained dictionary - trained vs baseline compression ratio |
| `orleans.lattice.compress.dictionary.trained_bytes_out` | counter (`By`) | - | Overview | Auto-trained dictionary - trained vs baseline compression ratio |
| `orleans.lattice.view.apply_lag` | histogram (`{entry}`) | `view` | MaterialisedViews | Apply lag (entries) p50/p95/p99, Apply lag p95 by view |
| `orleans.lattice.view.backlog_depth` | histogram (`{entry}`) | `view` | MaterialisedViews | Drain backlog depth (entries) p50/p95/p99 |
| `orleans.lattice.view.applied` | counter | `view` | MaterialisedViews | View writes applied (rate) |
| `orleans.lattice.view.aggregation_applied` | counter | `view` | MaterialisedViews | Aggregation contributions applied (rate) |
| `orleans.lattice.view.aggregation_rejected` | counter | `view` | MaterialisedViews | Aggregation reserved-key rejections (rate) |
| `orleans.lattice.view.lag_budget_eviction` | counter | `view` | MaterialisedViews | Lag-budget evictions (rate) |
| `orleans.lattice.view.key_collisions` | counter | `view` | MaterialisedViews | Re-key collisions (rate) |
| `orleans.lattice.view.atomic_staging_backstop` | counter | `view` | MaterialisedViews | Atomic-staging backstop fall-backs (rate) |
| `orleans.lattice.view.cross_tree_joint_violation` | counter | `view` | MaterialisedViews | Cross-tree joint-atomicity violations (rate) |
| `orleans.lattice.view.source_backpressure` | counter | `view`, `state` | MaterialisedViews | Source back-pressure self-throttle (rate) |

## `orleans.lattice.replication` meter

| Instrument | Type | Tags | Panel(s) |
|------------|------|------|----------|
| `orleans.lattice.replication.ship.duration` | histogram (ms) | `tree` | Ship duration p50/p95/p99 |
| `orleans.lattice.replication.apply.duration` | histogram (ms) | `tree` | Apply duration p50/p95/p99 |
| `orleans.lattice.replication.apply.lag` | histogram (ms) | `tree` | Apply lag p50/p95/p99 |
| `orleans.lattice.replication.apply.dependency_wait_ms` | histogram (ms) | `tree` | Dependency wait p95 |
| `orleans.lattice.replication.wal.entries_shipped` | counter | `tree` | WAL throughput |
| `orleans.lattice.wal.entries_trimmed` | counter | `tree` | WAL throughput |
| `orleans.lattice.replication.dead_letter.enqueued` | counter | `tree` | Dead-letter churn |
| `orleans.lattice.replication.dead_letter.removed` | counter | `tree`, `reason` | Dead-letter churn |
| `orleans.lattice.replication.apply.fifo_violations` | counter | `tree` | Apply correctness violations |
| `orleans.lattice.replication.apply.causal_violations_blocked` | counter | `tree` | Apply correctness violations |
| `orleans.lattice.replication.peer.fell_off_log` | counter | `peer` | Peer fell-off-log events |
| `orleans.lattice.replication.apply.buffered_entries` | gauge | `tree` | Apply buffer (entries) |
| `orleans.lattice.replication.apply.buffer_bytes` | gauge | `tree` | Apply buffer (bytes) |
| `orleans.lattice.replication.peer.entries_behind` | gauge | `peer` | Per-peer entries behind |
| `orleans.lattice.replication.peer.bytes_behind` | gauge | `peer` | Per-peer bytes behind |
| `orleans.lattice.replication.peer.last_contact_seconds` | gauge | `peer` | Per-peer last contact |
| `orleans.lattice.replication.peer.consecutive_errors` | gauge | `peer` | Per-peer consecutive errors |
| `orleans.lattice.replication.wire_version.negotiated` | observable gauge (`{version}`) | `tree`, `peer` | Per-peer negotiated wire version |
| `orleans.lattice.replication.wire_version.downgrade_active` | observable gauge (`{bool}`) | `tree`, `peer` | Per-peer wire-version downgrade active (1 = mixed-version fleet) |
| `orleans.lattice.replication.digest_probe.compared` | counter | `tree`, `shard`, `peer`, `outcome` | Anti-entropy digest-probe comparisons (rate, by outcome) |
| `orleans.lattice.replication.digest_probe.mismatch` | counter | `tree`, `shard`, `peer` | Anti-entropy digest divergence (mismatch rate) |
| `orleans.lattice.replication.merkle_walk.localised` | counter | `tree`, `depth` | Merkle-walk leaves localised |
| `orleans.lattice.replication.merkle_walk.aborted` | counter | `reason` | Merkle-walk aborted |
| `orleans.lattice.replication.ship.redundant_payloads` | counter | `tree`, `peer` | Redundant payload re-send rate |
| `orleans.lattice.replication.ship.redundant_payload_bytes` | counter | `tree`, `peer` | Redundant payload re-send rate |
| `orleans.lattice.replication.ship.effective_batch_size` | histogram | `tree`, `peer` | Effective ship batch size (adaptive) |
| `orleans.lattice.replication.ship.ack_latency` | histogram (ms) | `tree`, `peer` | Ship ack latency p50/p95/p99 |
| `orleans.lattice.replication.coalesce.entries_elided` | counter | `tree`, `peer` | Pre-ship coalescing elided entries |
| `orleans.lattice.replication.coalesce.bytes_elided` | counter (`By`) | `tree`, `peer` | Pre-ship coalescing elided entries |
| `orleans.lattice.replication.coalesce.deltas_merged` | counter (`{delta}`) | `tree`, `peer` | Pre-ship CRDT deltas merged (rate) |
| `orleans.lattice.replication.doorbell.rung` | counter (`{ring}`) | `tree`, `peer` | Shipper doorbell coalescing (rate) |
| `orleans.lattice.replication.doorbell.coalesced` | counter (`{ring}`) | `tree`, `peer` | Shipper doorbell coalescing (rate) |
| `orleans.lattice.replication.leaf_rereplay.entries` | counter | `tree`, `peer` | Leaf re-replay entries re-shipped (rate) |
| `orleans.lattice.replication.leaf_rereplay.skipped` | counter | `tree`, `peer`, `reason` | Leaf re-replay skipped (rate by reason) |
| `orleans.lattice.replication.apply.parallel_runs` | histogram (`{run}`) | - | Apply parallelism - runs per batch |
| `orleans.lattice.replication.peer.ship_in_flight` | observable gauge | `peer` | Outbound batches in flight per peer |
| `orleans.lattice.replication.bootstrap.entries_received` | counter | `tree`, `origin` | Bootstrap throughput (rate) |
| `orleans.lattice.replication.bootstrap.bytes_received` | counter (`By`) | `tree`, `origin` | Bootstrap throughput (rate) |
| `orleans.lattice.replication.bootstrap.duration` | histogram (ms) | `tree`, `origin`, `outcome` | Bootstrap duration p50/p95/p99 |
| `orleans.lattice.replication.bootstrap.transient_retries` | counter | `tree`, `origin` | Bootstrap transient retries (rate) |
| `orleans.lattice.replication.peer.fell_off_log_suppressed` | counter | `tree`, `origin` | Fall-off-log suppressed events (rate) |
| `orleans.lattice.replication.bootstrap_fallback.triggered` | counter | `tree`, `peer` | Bootstrap fallback triggered (rate) |
| `orleans.lattice.replication.bootstrap_fallback.entries` | counter | `tree`, `peer` | Bootstrap fallback entries re-shipped (rate) |
| `orleans.lattice.replication.bootstrap_fallback.skipped` | counter | `tree`, `peer`, `reason` | Bootstrap fallback skipped (rate by reason) |
| `orleans.lattice.replication.digest_remediation.disabled` | observable gauge | `tree`, `peer`, `reason` | Remediation disabled (tree/peer by reason) |
| `orleans.lattice.replication.digest_remediation.skipped` | counter | `tree`, `peer`, `reason` | Remediation skipped (rate by reason) |
| `orleans.lattice.replication.compress.dictionary.bytes_in` | counter (`By`) | `tree` | Shared-dictionary compression ratio (before/after) |
| `orleans.lattice.replication.compress.dictionary.bytes_out` | counter (`By`) | `tree` | Shared-dictionary compression ratio (before/after) |
| `orleans.lattice.replication.ship.elided_payloads` | counter | `tree`, `peer` | Content-hash payload elision (rate) |
| `orleans.lattice.replication.ship.elided_payload_bytes` | counter (`By`) | `tree`, `peer` | Content-hash payload elision (rate) |
| `orleans.lattice.replication.ship.manifest_exchanges` | counter | `tree`, `peer` | Content-hash manifest exchanges (rate) |
| `orleans.lattice.replication.ship.dictionary_negotiation` | counter | `tree`, `peer`, `outcome` | Shared-dictionary negotiation outcomes (rate) |
| `orleans.lattice.replication.ship.dictionary_batches` | counter | `tree`, `peer`, `dictionary` | Batches shipped with vs without a shared dictionary (rate) |
| `orleans.lattice.replication.ship.dictionary_convergence` | counter | `tree`, `peer`, `outcome` | Shared-dictionary convergence pulls (rate) |
| `orleans.lattice.replication.ship.wire_version_down_stamp` | counter | `tree`, `peer`, `reason` | Wire-version down-stamp outcomes (rate by reason) |
| `orleans.lattice.replication.receiver.content_manifest_exchanges` | counter | `tree`, `peer` | Receiver content-hash exchanges handled (rate) |
| `orleans.lattice.replication.receiver.content_entries_elided` | counter | `tree`, `peer` | Receiver content entries elided (rate) |
| `orleans.lattice.replication.receiver.content_hwm_advances` | counter | `tree`, `peer` | Receiver metadata-only HWM advances (rate) |
| `orleans.lattice.replication.saga.phase.duration` | histogram (ms) | `phase` | Coordinated restore: saga phase and write-fence durations (p95, ms) |
| `orleans.lattice.replication.saga.fence.duration` | histogram (ms) | `tree` | Coordinated restore: saga phase and write-fence durations (p95, ms) |
| `orleans.lattice.replication.saga.participant.votes` | counter | `reason` | Coordinated restore: participant votes, commits and aborts (rate) |
| `orleans.lattice.replication.saga.participant.commits` | counter | - | Coordinated restore: participant votes, commits and aborts (rate) |
| `orleans.lattice.replication.saga.participant.aborts` | counter | - | Coordinated restore: participant votes, commits and aborts (rate) |
| `orleans.lattice.replication.saga.compensations` | counter | `cause` | Coordinated restore: saga compensations (rate by cause) |

## `orleans.lattice.auth` meter

Charted by the Identity & Authorization dashboard.

| Instrument | Type | Tags | Dashboard | Panel(s) |
|------------|------|------|-----------|----------|
| `orleans.lattice.auth.decisions` | counter | `operation`, `tree`, `effect` | Authorization | Authorization decisions (rate by effect); Decisions by operation (rate) |
| `orleans.lattice.auth.decision.duration` | histogram (ms) | `operation`, `tree`, `effect` | Authorization | Decision latency p50/p95/p99 (ms) |
| `orleans.lattice.auth.snapshot.rebuilds` | counter | (none) | Authorization | Compiled snapshot rebuilds (rate) |
| `orleans.lattice.auth.snapshot.epoch` | observable gauge | `instance` | Authorization | Compiled snapshot epoch |
| `orleans.lattice.auth.snapshot.age` | observable gauge | `instance` | Authorization | Compiled snapshot age |
| `orleans.lattice.auth.snapshot.subjects` | observable gauge | `instance` | Authorization | Members with policies configured |

## `orleans.lattice.membership` meter

Charted by the Identity & Authorization dashboard.

| Instrument | Type | Tags | Dashboard | Panel(s) |
|------------|------|------|-----------|----------|
| `orleans.lattice.membership.resolution_cache.hits` | counter | (none) | Authorization | Subject-resolution cache hit ratio; Subject-resolution cache hits vs misses (rate) |
| `orleans.lattice.membership.resolution_cache.misses` | counter | (none) | Authorization | Subject-resolution cache hit ratio; Subject-resolution cache hits vs misses (rate) |
| `orleans.lattice.membership.directory.search.duration` | histogram (ms) | (none) | Authorization | Identity-directory search latency p50/p95/p99 (ms) |
| `orleans.lattice.membership.directory.search.hits` | counter | (none) | Authorization | Identity-directory search hits vs misses (rate); Identity-directory search hit ratio |
| `orleans.lattice.membership.directory.search.misses` | counter | (none) | Authorization | Identity-directory search hits vs misses (rate); Identity-directory search hit ratio |

## `orleans.lattice.backup` meter

Charted by the Backup & Restore dashboard.

| Instrument | Type | Tags | Dashboard | Panel(s) |
|------------|------|------|-----------|----------|
| `orleans.lattice.backup.captures` | counter | `kind` | Backup | Captures (rate by kind) |
| `orleans.lattice.backup.capture.duration` | histogram (ms) | `kind` | Backup | Capture duration p50/p95/p99 |
| `orleans.lattice.backup.bytes` | histogram (`By`) | `kind` | Backup | Backup size p50/p95 (bytes) |
| `orleans.lattice.backup.artifacts` | histogram (`{artifact}`) | `kind` | Backup | Artifacts per backup p50/p95 |
| `orleans.lattice.backup.entries` | histogram (`{entry}`) | `kind` | Backup | Entries per backup p50/p95 |
| `orleans.lattice.backup.entries_processed` | counter | `kind` | Backup | Processing throughput |
| `orleans.lattice.backup.bytes_processed` | counter (`By`) | `kind` | Backup | Processing throughput |
| `orleans.lattice.backup.restore.duration` | histogram (ms) | (none) | Backup | Restore duration p50/p95/p99 |
| `orleans.lattice.backup.restore.entries` | counter | (none) | Backup | Restore entries (rate) |
| `orleans.lattice.backup.incremental.lag_entries` | histogram (`{entry}`) | (none) | Backup | Incremental lag entries p50/p95 |
| `orleans.lattice.backup.incremental.lag_age` | histogram (ms) | (none) | Backup | Incremental lag age p50/p95 |
| `orleans.lattice.backup.retention.bytes_reclaimed` | counter (`By`) | `scope` | Backup | Retention reclaimed |
| `orleans.lattice.backup.retention.pruned` | counter | `scope` | Backup | Retention pruned (rate) |
| `orleans.lattice.backup.capture.failures` | counter | `kind`, `phase`, `reason` | Backup | Capture failures (by reason) |
| `orleans.lattice.backup.restore.failures` | counter | `phase`, `reason` | Backup | Restore failures (by reason) |
| `orleans.lattice.backup.capture.retries` | counter | `reason` | Backup | Capture retries / fallbacks |
| `orleans.lattice.backup.scheduler.skipped` | counter | `scope` | Backup | Scheduler skipped vs overruns |
| `orleans.lattice.backup.scheduler.overruns` | counter | `scope` | Backup | Scheduler skipped vs overruns |
| `orleans.lattice.backup.cross_tree_fence.selections` | counter | `tree_count` | Backup | Cross-tree fence selections / drained |
| `orleans.lattice.backup.cross_tree_fence.drained_in_flight` | counter | (none) | Backup | Cross-tree fence selections / drained |
| `orleans.lattice.backup.cross_tree_fence.retries` | counter | (none) | Backup | Cross-tree fence retries |
| `orleans.lattice.backup.cross_tree_fence.drain_wait` | histogram (ms) | (none) | Backup | Cross-tree fence drain wait p50/p95 |
| `orleans.lattice.backup.inventory.count` | observable gauge (`{backup}`) | (none) | Backup | Tracked backups |
| `orleans.lattice.backup.inventory.chain_depth_max` | observable gauge (`{backup}`) | (none) | Backup | Max chain depth |
| `orleans.lattice.backup.catalog.bytes` | observable gauge (`By`) | (none) | Backup | Catalog size |
| `orleans.lattice.backup.inventory.oldest_age` | observable gauge (`s`) | (none) | Backup | Oldest backup age |
| `orleans.lattice.backup.inventory.newest_age` | observable gauge (`s`) | (none) | Backup | Newest backup age |
| `orleans.lattice.backup.scope.last_run_status` | observable gauge (`{status}`) | `scope` | Backup | Per-scope last-run status |
| `orleans.lattice.backup.scope.last_success_age` | observable gauge (`s`) | `scope` | Backup | Per-scope seconds since last success |

## `orleans.lattice.scaling` meter

All instruments are observable gauges published from the cached `ScalingSignal` on the silo's sampling timer. Charted by the Autoscaling Signal dashboard; coverage enforced from `Orleans.Lattice.Scaling.Tests`.

| Instrument | Type | Tags | Dashboard | Panel(s) |
|------------|------|------|-----------|----------|
| `orleans.lattice.scaling.scale_value` | observable gauge (`{replica}`) | (none) | Autoscaling Signal | Scale value (smoothed vs raw) |
| `orleans.lattice.scaling.raw_scale_value` | observable gauge (`{replica}`) | (none) | Autoscaling Signal | Scale value (smoothed vs raw) |
| `orleans.lattice.scaling.compute.activation_pressure` | observable gauge (`1`) | (none) | Autoscaling Signal | Compute pressure by dimension |
| `orleans.lattice.scaling.compute.resource_pressure` | observable gauge (`1`) | (none) | Autoscaling Signal | Compute pressure by dimension |
| `orleans.lattice.scaling.compute.wal_dispatch_pressure` | observable gauge (`1`) | (none) | Autoscaling Signal | Compute pressure by dimension |
| `orleans.lattice.scaling.compute.replicas` | observable gauge (`{replica}`) | (none) | Autoscaling Signal | Recommended replicas |
| `orleans.lattice.scaling.storage.accounts_over_threshold` | observable gauge (`{account}`) | (none) | Autoscaling Signal | WAL accounts over threshold |
| `orleans.lattice.scaling.storage.rebalance_recommendations` | observable gauge (`{recommendation}`) | (none) | Autoscaling Signal | WAL rebalance recommended |

## `orleans.lattice.replication.grpc` meter

The gRPC replication transport's telemetry. Charted by the Replication Transport (gRPC) dashboard; coverage enforced from `Orleans.Lattice.Replication.Grpc.Tests`.

| Instrument | Type | Tags | Dashboard | Panel(s) |
|------------|------|------|-----------|----------|
| `orleans.lattice.replication.grpc.insecure_channel` | counter (`{channel}`) | `peer`, `transport` | Replication Transport (gRPC) | Insecure (plaintext) channels constructed; Insecure channel construction rate by peer and transport |