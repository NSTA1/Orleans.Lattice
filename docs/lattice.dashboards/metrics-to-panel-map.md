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

## `orleans.lattice.replication` meter

| Instrument | Type | Tags | Panel(s) |
|------------|------|------|----------|
| `orleans.lattice.replication.ship.duration` | histogram (ms) | `tree` | Ship duration p50/p95/p99 |
| `orleans.lattice.replication.apply.duration` | histogram (ms) | `tree` | Apply duration p50/p95/p99 |
| `orleans.lattice.replication.apply.lag` | histogram (ms) | `tree` | Apply lag p50/p95/p99 |
| `orleans.lattice.replication.apply.dependency_wait_ms` | histogram (ms) | `tree` | Dependency wait p95 |
| `orleans.lattice.replication.wal.entries_appended` | counter | `tree` | WAL throughput |
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
