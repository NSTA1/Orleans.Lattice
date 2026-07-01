# Metric-to-panel coverage map

Every instrument on the `orleans.lattice` and `orleans.lattice.replication` meters is referenced by at least one panel in the bundled dashboards. The drift-guard test in `Orleans.Lattice.Dashboards.Tests` enforces the inverse direction: every metric name a dashboard references must resolve to a live instrument.

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
