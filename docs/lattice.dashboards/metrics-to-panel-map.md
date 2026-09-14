# Metric-to-panel coverage map

Almost every instrument on the `orleans.lattice` and `orleans.lattice.replication` meters is referenced by at least one panel in the bundled dashboards; the exceptions are listed as **not charted** in the tables below. The drift-guard test in `Orleans.Lattice.Dashboards.Tests` enforces the inverse direction unconditionally: every metric name a dashboard references must resolve to a live instrument.

> **The forward direction is enforced only for instruments the guard can observe.** The guard discovers live instruments by forcing the type initialisers of `LatticeMetrics` and `LatticeReplicationMetrics` and listening for what they publish, plus the instrument-name constants those two classes declare. An instrument created in a field initialiser on some *other* type - a grain, say - is never constructed at test time, so the guard neither sees it nor demands a panel for it. Such an instrument can ship unpaneled with a green build, which is exactly how the `orleans.lattice.tag_index.reconcile.*` family below came to be uncharted. When you add an instrument outside those two classes, add its row here by hand.

The add-on `orleans.lattice.auth` and `orleans.lattice.membership` meters are charted by the bundled Identity & Authorization dashboard. Their coverage is enforced from the owning packages: `Orleans.Lattice.Auth.Tests` and `Orleans.Lattice.Membership.Tests` each derive from the shared `MeterDashboardCoverageTestsBase`, which asserts every instrument on the meter is referenced by that dashboard (and that every token the dashboard references for the meter resolves to a live instrument).

The add-on `orleans.lattice.backup` meter is charted by the bundled Backup & Restore dashboard. Its coverage is enforced the same way, from `Orleans.Lattice.Backup.Tests` (deriving from `MeterDashboardCoverageTestsBase`).

The add-on `orleans.lattice.scaling` meter is charted by the bundled Autoscaling Signal dashboard. Its coverage is enforced the same way, from `Orleans.Lattice.Scaling.Tests` (deriving from `MeterDashboardCoverageTestsBase`).

The add-on `orleans.lattice.replication.grpc` meter is charted by the bundled Replication Transport (gRPC) dashboard. Its coverage is enforced the same way, from `Orleans.Lattice.Replication.Grpc.Tests` (deriving from `MeterDashboardCoverageTestsBase`).

The add-on `orleans.lattice.tenancy` meter is charted by the bundled Per-Tenant Observability dashboard. Its coverage is enforced the same way, from `Orleans.Lattice.Tenancy.Tests` (deriving from `MeterDashboardCoverageTestsBase`).

### How to read the Tags column

Each table's **Tags** column lists only the dimensions specific to that instrument. The derived `tenant` label is **not** repeated on every row: it is present on every instrument on every meter, and is described once in [The derived `tenant` label](#the-derived-tenant-label) below. A row that names no tag therefore still carries `tenant`.

### Per-operation vs per-record contract

A throughput-style counter measures either **operations** or **records**, and the two diverge sharply on batched and bulk paths. Every such instrument below declares which it is, so a panel title can never imply a semantic the instrument does not deliver:

- **Per-operation** (`{op}`) - one increment per grain call, whatever its payload. A batched or bulk call (`SetManyAsync`, `MergeManyAsync`, `DeleteRangeAsync`, `SetManyWherePredicateAsync`, `BulkLoadAsync`, `BulkLoadRawAsync`, `BulkAppendAsync`) counts **once regardless of entry count**, so a 5000-record import advances the counter by only the number of bulk operations.
- **Per-record** (`{record}`) - one increment per individual entry the operation carried. The same 5000-record import advances the counter by 5000.
- **Per-entry sample** - for a histogram, one observation per entry rather than per batch (for example `orleans.lattice.replication.apply.duration`, which contributes N samples for a batch of N).

`orleans.lattice.shard.writes` (operations) and `orleans.lattice.shard.records_written` (records) are the canonical pair: plot both, and their ratio is the effective batch size. Plotting the operation counter alone as "write throughput" under-represents bulk ingestion, which is the defect issue #1648 was raised for.

## `orleans.lattice` meter

| Instrument | Type | Tags | Dashboard | Panel(s) |
|------------|------|------|-----------|----------|
| `orleans.lattice.shard.reads` | counter (`{op}`, **per-operation**) | `tree`, `shard` | Overview | Cluster throughput (ops/s) |
| `orleans.lattice.shard.writes` | counter (`{op}`, **per-operation**) | `tree`, `shard` | Overview | Cluster throughput (ops/s), Per-tree write throughput (operations/s and records/s) |
| `orleans.lattice.shard.records_written` | counter (`{record}`, **per-record**) | `tree`, `shard` | Overview | Per-tree write throughput (operations/s and records/s) |
| `orleans.lattice.shard.splits_committed` | counter | `tree`, `shard` | Overview | Splits committed |
| `orleans.lattice.shard.consolidations_committed` | counter | `tree`, `shard` | Overview | Consolidations committed |
| `orleans.lattice.shard.healing.backlog` | histogram (`{shard}`) | `tree` | Overview | Healing backlog (shards above base) |
| `orleans.lattice.shard.healing.decisions` | counter | `tree`, `decision` | Overview | Healing decisions by outcome |
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
| `orleans.lattice.leaf.split.completion.in_flight` | gauge (`{completion}`) | `tree` | Overview | Leaf-split completions currently suspended inside `CompleteSplitAsync` (issue #2967). The divided/faulted outcome counters partition only TERMINATED divisions, so a division still in the completion body at scrape time belongs to none of them, and because the catch that records faulted is unconditional a division carrying neither divided nor faulted did not throw - it is genuinely in flight, a state nothing named before this gauge. Read beside the oldest-completion-age panel: a non-zero count is only actionable once that age is climbing. No per-tree pre-mint, so a tree with nothing in flight emits no series and No data is the healthy steady state |
| `orleans.lattice.leaf.split.completion.oldest_age` | gauge (`s`) | `tree` | Overview | Age in seconds of the oldest leaf-split completion suspended inside `CompleteSplitAsync` (issue #2967). Separates a division momentarily in flight (age near zero, falling) from one that is stuck (age climbing without bound), which the bare in-flight count cannot. A measurement, not a threshold: the code encodes no abandonment age, so alert on a sustained climb read against real completion durations. A registry-backed gauge rather than a completion-duration histogram on purpose - a histogram only records on completion, so a division that never completes would never appear in one. Emitted as the bare name with no unit suffix, matching `orleans.lattice.backup.inventory.oldest_age` |
| `orleans.lattice.leaf.bisect_refusals` | counter | `tree`, `reason`, `detach_seam` | Overview | Leaf division fast path forfeited (issue #2787). Read `reason` and `detach_seam` together: `no_snapshot_attached` with seam `none` is benign, the same reason with any other seam means an unrelated whole-leaf operation consumed the frame and the split must now materialise the entire leaf. Seam values `underlying_rows_accessor` and `range_hydration_completed` are never produced by a deployed process (issue #2865), so a missing line for either is expected and is not evidence about leaf behaviour |
| `orleans.lattice.leaf.byte.overflow` | counter | `tree`, `outcome` | Overview | Splits committed. Pre-minted at zero for both outcomes when a leaf reaches the byte-bound check during snapshot capture, with the tag set a real emission carries, so the panel omits `or vector(0)` and a missing line means the build did not land rather than that no leaf overflowed (issue #2756) |
| `orleans.lattice.leaf.split_attempts` | counter | `tree`, `outcome` | Overview | Leaf divisions sought. Read beside the bisect-refusal panel, which is uninterpretable alone: a zero refusal rate means either no division forfeited its fast path or none was ever attempted, and only this counter separates them. All four outcomes are pre-minted at zero when a leaf reaches the byte-bound check during snapshot capture, so the panel omits `or vector(0)` and a flat zero is a measured zero (issue #2756) |
| `orleans.lattice.leaf.commit.duration` | histogram (ms) | `tree`, `step` | CommitPath | Commit-step latency p50/p95/p99 |
| `orleans.lattice.cache.hits` | counter | `tree` | Overview | Cache hit ratio |
| `orleans.lattice.cache.misses` | counter | `tree` | Overview | Cache hit ratio |
| `orleans.lattice.atomic_write.completed` | counter | `tree`, `outcome` | Overview, AtomicWrites | Atomic write outcomes (rate); per-tree committed throughput; saga failure rate (failed + compensated / total); range-window non-committed saga count |
| `orleans.lattice.atomic_write.duration` | histogram (ms) | `tree`, `outcome` | Overview, AtomicWrites | Saga duration p50/p95/p99; saga duration p95 by outcome |
| `orleans.lattice.atomic_write.batch_size` | histogram (`{entry}`) | `tree`, `outcome` | Overview, AtomicWrites | Batch size p50/p95/p99; batch size p95 by outcome |
| `orleans.lattice.coordinator.completed` | counter | `tree`, `kind` | Overview | Coordinator completions |
| `orleans.lattice.coordinator.phase_tick.failures` | counter (`{failure}`) | `tree`, `kind`, tenant | Overview | Coordinator phase-tick failures (rate) - zero-primed per coordinator, so a flat zero is a reading that ticks are succeeding; any non-zero value is discarded phase-loop work and is operator-actionable |
| `orleans.lattice.coordinator.phase_tick.consecutive_failures` | gauge (`{failure}`) | `tree`, `kind`, tenant | Overview | Consecutive failed coordinator phase ticks, as the max over the activations sharing a tag set - every live coordinator reports, so `0` is a reading that the last tick succeeded; a value that keeps returning to zero is transient fault absorption, while one that only climbs is a wedged phase machine and is operator-actionable |
| `orleans.lattice.tree.lifecycle` | counter | `tree`, `kind` | Overview | Tree lifecycle events (annotation + stat) |
| `orleans.lattice.events.published` | counter | `tree`, `kind` | Overview | Events published |
| `orleans.lattice.events.dropped` | counter | `tree`, `reason` | Overview | Events dropped |
| `orleans.lattice.config.changed` | counter | `tree`, `config` | Overview | Runtime config changes |
| `orleans.lattice.observer.duration` | histogram (ms, **per-observer per-mutation sample**) | `tree`, `observer` | CommitPath | Mutation-observer inline latency p95 (ms) by observer |
| `orleans.lattice.storage.wal_bytes` | observable gauge (`By`) | `tree` | Overview | Storage footprint by tree |
| `orleans.lattice.storage.snapshot_bytes` | observable gauge (`By`) | `tree` | Overview | Storage footprint by tree |
| `orleans.lattice.storage.leaf_state_bytes` | observable gauge (`By`) | `tree` | Overview | Storage footprint by tree |
| `orleans.lattice.storage.total_bytes` | observable gauge (`By`) | `tree` | Overview | Cluster total retained bytes |
| `orleans.lattice.storage.usage_deep_published` | observable gauge (0/1) | `tree` | Overview | Storage usage measurement depth by tree |
| `orleans.lattice.storage.policy.over_threshold` | observable gauge (0/1) | `tree` | Overview | Trees over advisory threshold |
| `orleans.lattice.storage.policy.trim_triggered` | counter | `tree`, `reason` | Overview | Byte-pressure trim activity |
| `orleans.lattice.storage.policy.bytes_reclaimed` | counter (`By`) | `tree` | Overview | Byte-pressure trim activity |
| `orleans.lattice.wal.gc.passes` | counter (`{pass}`) | `tree`, `outcome` | Replication, CommitPath | WAL GC pass rate by outcome; WAL GC blocked passes by tree. Also charted on CommitPath alongside `leaf.snapshot.coverage_repairs`, where the `reclaimed` arm is the counterpart a zero-coverage repair is meant to unblock (issue #2692). All six outcome arms are primed at zero per tree (issues #2774, #2850), so the by-outcome panel carries no `or vector(0)` compensation: a missing line there means the build did not land, not that the system is healthy. Only `reclaimed` is affirmative; `idle` means a usable floor was evaluated and nothing was eligible, and `no_consumer` means there was no floor to evaluate |
| `orleans.lattice.wal.gc.blocked_leaf_reactivations` | counter (`{reactivation}`) | `tree`, `outcome` | Replication | Reactivations of a dormant leaf whose unusable durable pin was blocking its tree's cursor floor, by four lifecycle arms (`attempted`/`healed`/`abandoned`/`rearmed`) and four terminal outcome arms that partition every `attempted` (`completed`/`unresolvable`/`faulted`/`undelivered`), and five drive verdicts recording what the touch achieved (`drove_lifted`/`drove_no_advance`/`drove_memory_refused`/`drove_not_driven`/`drove_already_driving`, issue #2692) (issue #2710 Limitation 2, re-arm per issue #2783, `undelivered` per issue #2768, terminal arms armed exhaustively per issue #2938). Chart beside `wal.gc.passes{outcome="blocked"}`, which is the series this sweep exists to drive to zero. All 13 outcome arms share one instrument and every one is zero-primed per tree on every pass - the terminal arms and the drive verdicts by walking their respective outcome enums, so one added later cannot ship unarmed (issue #2938, issue #2692), and each terminal arm and each drive verdict additionally carries a positive control proving its recording path is reachable rather than frozen at zero (issue #2942) - so a zero on `healed` is a measured zero rather than an unpublished series: a sweep that touches leaves and achieves nothing reads identically to one that was never reached, and an absent series means the build carrying the sweep never landed. `abandoned` is the alarm arm - the leaf stayed blocked across every permitted attempt of a cycle, so activation alone cannot clear it and its snapshot capture is failing for a separate reason - and `rearmed` is its counterweight, marking each restoration of the budget after an escalating backoff. `abandoned` rising while `rearmed` stays flat is the signature of a sweep that has stopped. `undelivered` separates a leaf that was never reached, because the touch did not return within its budget, from one that was reached and stayed blocked, so an unreachable leaf no longer reads as an unhealable one. Deliberately not tagged with the leaf identity, which is unbounded; the blocking consumer is named on the paired warning log (issue #2464), which since issue #2815 emits one line per blocked episode rather than one per change of reported blocker and carries a distinct-blocker count, so a single named consumer is not evidence that only one was blocking |
| `orleans.lattice.wal.gc.interval` | histogram (`s`) | `tree` | Replication | WAL GC adaptive interval |
| `orleans.lattice.wal.gc.backlog_bytes` | histogram (`By`) | `tree` | Replication | WAL GC retained backlog after pass |
| `orleans.lattice.wal.gc.backlog_bytes_unavailable` | counter (`{pass}`) | `tree`, `reason` | Replication | WAL GC backlog bytes unavailable by reason |
| `orleans.lattice.wal.gc.offset_floor_unavailable` | counter (`{pass}`) | `tree` | Replication | WAL GC offset floor unavailable (pin store unreachable) |
| `orleans.lattice.admission.live_keys` | observable gauge (`{key}`) | `tree` | Overview | Admission - live keys by tree |
| `orleans.lattice.admission.estimated_bytes` | observable gauge (`By`) | `tree` | Overview | Admission - estimated bytes by tree |
| `orleans.lattice.admission.over_advisory` | observable gauge (0/1) | `tree` | Overview | Admission - trees over advisory ceiling |
| `orleans.lattice.admission.would_reject` | counter (`{write}`) | `tree`, `dimension` | Overview | Admission - would-reject rate (advisory dry-run) |
| `orleans.lattice.admission.utilization` | observable gauge (ratio) | `tree`, `dimension` | Overview | Admission - utilization by dimension |
| `orleans.lattice.admission.rejected` | counter (`{write}`) | `tree`, `dimension` | Overview | Admission - rejected write rate (enforced) |
| `orleans.lattice.lock.acquired` | counter (`{acquire}`) | `outcome` | Overview | Distributed lock - acquire / release / reclaim rate |
| `orleans.lattice.lock.released` | counter (`{release}`) | (none) | Overview | Distributed lock - acquire / release / reclaim rate |
| `orleans.lattice.lock.lease_reclaimed` | counter (`{lease}`) | (none) | Overview | Distributed lock - acquire / release / reclaim rate |
| `orleans.lattice.lock.acquire.wait` | histogram (ms) | (none) | Overview | Distributed lock - acquire wait latency |
| `orleans.lattice.atomic_action.completed` | counter (`{saga}`) | `outcome` | Overview | Atomic action - saga and step rate |
| `orleans.lattice.atomic_action.step` | counter (`{step}`) | `phase`, `outcome` | Overview | Atomic action - saga and step rate |
| `orleans.lattice.atomic_action.duration` | histogram (ms) | `outcome` | Overview | Atomic action - saga duration |
| `orleans.lattice.grain.call.outstanding_depth` | histogram (`{call}`) | `grain_type` | Overview | Outstanding calls per target activation, by grain type |
| `orleans.lattice.grain.call.duration` | histogram (ms) | `grain_type`, `outcome` | Overview | Grain-call duration by grain type and outcome |
| `orleans.lattice.shard_root.forward.timeouts` | counter | `tree` | CommitPath | Shard-root wedge guards (forward timeouts, scan-page stalls, scan resumptions, and flush suspensions) |
| `orleans.lattice.shard_root.scan_page.stalls` | counter | `tree`, `shard`, `phase` | CommitPath | Shard-root wedge guards (forward timeouts, scan-page stalls, scan resumptions, and flush suspensions) |
| `orleans.lattice.shard_root.scan_page.ceiling_outcomes` | counter | `tree`, `shard`, `outcome` | CommitPath | Shard-root wedge guards (forward timeouts, scan-page stalls, scan resumptions, and flush suspensions) |
| `orleans.lattice.shard_root.scan_page.leaf_read_outcomes` | counter | `tree`, `shard`, `outcome` | CommitPath | Scan-page leaf-read coalescing (issued, joined, served) |
| `orleans.lattice.scan.stall_resumptions` | counter | `tree`, `phase`, `outcome` | CommitPath | Shard-root wedge guards (forward timeouts, scan-page stalls, scan resumptions, and flush suspensions) |
| `orleans.lattice.scan.stall_futility_outcomes` | counter | `tree`, `phase`, `outcome` | CommitPath | Shard-root wedge guards (forward timeouts, scan-page stalls, scan resumptions, and flush suspensions) |
| `orleans.lattice.shard_root.flush.retries_suspended` | counter | `tree`, `shard`, `kind` | CommitPath | Shard-root wedge guards (forward timeouts, scan-page stalls, scan resumptions, and flush suspensions) |
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
| `orleans.lattice.materialiser.pin.advances` | counter (`{report}`) | `tree`, `outcome` | CommitPath | Leaf-materialiser pin advancement by outcome (issue #2694) |
| `orleans.lattice.leaf.activation_replays` | counter | `tree`, `activation_temperature` | CommitPath | Leaf-materialiser durable pin path (issue #1030); the `cold`/`warm` arms give the activation-temperature ratio (issue #2148) |
| `orleans.lattice.wal.replay.permit_adaptations` | counter (`{permit}`) | `outcome`, `trigger` (withheld arm only) | CommitPath | Memory-adaptive backpressure on the per-silo WAL replay concurrency gate, by `withheld`/`restored` (issues #2781, #2862). Withholding has two triggers - proactively when a replay finishes with managed heap occupancy at or above 75% of the GC hard limit, and reactively when a replay fails for memory pressure - while restoring requires a clean replay **and** occupancy back under 60%, so the arms trace a hysteresis band. **The withheld arm carries `trigger` (`occupancy`/`fault`) to separate those two producers** (issue #2883); before it did, `withheld` was a sum no scrape could attribute, and run 12's `withheld = 6` was published as evidence for the reactive trigger when it was equally consistent with that trigger firing zero times. Summing over `trigger` recovers the untagged total, so pre-tag comparisons stay valid. **The restored arm carries no `trigger` on purpose**: withheld permits are fungible, so `withheld{trigger=X} - restored` is meaningless and only the total `withheld - restored` is a valid level. Charted twice: as a rate, to show how hard the gate is oscillating, and as the level `withheld - restored`, which is the number of permits currently out of circulation and so the actual reduction in leaf-activation concurrency. Both arms share one instrument so a zero on `restored` is a measured zero rather than an unpublished series, and all three series - each trigger on the withheld arm, plus restored - are primed at zero when the gate is sized, so an absent series means the build did not land. Deliberately carries no `tree` tag - the gate is process-wide, and a per-tree tag would invite summing arms that share one resource. Chart beside `leaf.activation_failures{reason="canceled_awaiting_permit"}`, which is the population this reduction exists to stop starving |
| `orleans.lattice.wal.replay.permit_queue_wait` | histogram (`ms`) | `tree`, `tenant`, `outcome` | CommitPath | Time spent queued on the per-silo WAL replay concurrency gate, by `acquired`/`canceled` (issue #2873). The discriminator for `leaf.activation.failures{reason="canceled_awaiting_permit"}`, which counts where an activation was cancelled and cannot say why: a `canceled` arm whose wait approaches the request budget is genuine permit starvation, while a `canceled` arm at or near zero is upstream budget exhaustion arriving already doomed. The counter is identical in both cases, so the duration is the only separator. **Chart it as `rate(_sum) / rate(_count)`, never as a quantile**: this container's exposition renders a histogram as a Prometheus summary with `_sum` and `_count` and no `_bucket`, so `histogram_quantile` returns nothing and the `or vector(0)` repair would manufacture a literal zero indistinguishable from the genuine near-zero wait that is one of the two readings. The mean suffices because the regimes differ by orders of magnitude. Carries a `tree` tag although its `permit_adaptations` sibling deliberately does not: a ceiling is process-wide, a wait is one caller's own and is attributable. Not primed at zero, because a primed histogram sample is a `0 ms` datum reading as "instant" rather than a neutral marker, so an absent series is uninterpretable and must be corroborated against the primed `permit_adaptations` arms |
| `orleans.lattice.wal.replay.slice_narrowings` | counter (`{narrowing}`) | `tree`, `partition`, `tenant` | CommitPath | Activation-time replay slice-width narrowings forced by memory pressure on a commit-log read (issue #2867). **The second of the two factors that set peak replay memory, and the only one no operator can configure.** Peak memory is the *product* of how many replays run at once and how much each buffers: the first factor is the `WalMaterialiserMaxConcurrentReplays` ceiling, surfaced on the container overlay and already charted from both sides by `wal.replay.permit_adaptations` and `wal.replay.permit_queue_wait`; the second is the per-replay slice width, a private constant of 256 with no option behind it, so the reactive #2742 narrowing is the only thing that ever moves it. Chart beside `leaf.activation.failures` on the same tree, because together they separate two stories a failure count alone cannot: narrowings climbing with failures means the width is too coarse for the host and the narrowing is not keeping up, while failures climbing against a flat zero here means the allocation that failed was not the slice read at all and width is not the lever. Carries a `tree` tag for the same reason `permit_queue_wait` does and `permit_adaptations` does not - a ceiling is process-wide, but a buffer width is one replay's own and is attributable, which is what lets one tree be shown buffering harder than its siblings under identical cycling. Primed at zero per `(tree, partition)` when a partition replay begins: the healthy steady state is never to narrow, so without the prime the common case would be indistinguishable on the scrape from a build that cannot narrow, and an absent series here does mean the build did not land |
| `orleans.lattice.leaf.activation_replays_over_budget` | counter | `tree`, `partition` | CommitPath | Per-leaf post-filter replay cost over budget against an intact WAL (issues #1738, #2149) |
| `orleans.lattice.leaf.activation_stalled_replays` | counter | `tree`, `partition` | CommitPath | Leaf replay re-entered from a checkpoint that did not advance; fault arm, alert on persistence not appearance (issue #2285) |
| `orleans.lattice.leaf.activation_cursor_publish_failures` | counter | `tree` | CommitPath | Leaf-materialiser durable pin path (issue #1030) |
| `orleans.lattice.leaf.deactivation.checkpoint_delta` | histogram | `tree`, `deactivation_reason`, `activation_temperature` | CommitPath | Checkpoint offsets banked by an activation during graceful deactivation (issue #2280). LOWER BOUND, not a census: crash teardowns bypass the hook and a failed activation never reaches it. A zero on the `cold` arm is arithmetically forced, not symptomatic |
| `orleans.lattice.leaf.activation.failures` | counter | `tree`, `activation_temperature`, `reason` | CommitPath | Leaf activations that threw out of `OnActivateAsync`, by `canceled`/`canceled_awaiting_permit`/`faulted` (issue #2280). Counts the population the deactivation histogram is structurally blind to; read the two together |
| `orleans.lattice.leaf.activation.cold_replay_loop` | counter | `tree` | CommitPath | Cold activations cancelled at or past the consecutive-cancellation escalation threshold: the self-reinforcing cold WAL replay loop (issue #2280). A DEFECT signal, where `leaf.activation.failures` above is a COST signal - that counter is an aggregate and cannot separate one leaf cancelled five times from five leaves cancelled once. The count is consecutive and resets on any successful activation, so it measures leaf health rather than process age, and the threshold sits one above the highest value in the field measurement, so zero is the expected reading. Not tagged by leaf; identity, the consecutive count and the mid-replay/queued-for-permit split are on the paired warning |
| `orleans.lattice.leaf.replay_barrier_outcomes` | counter | `tree`, `outcome` | CommitPath | Terminal states of a leaf's deferred WAL replay, by `completed`/`faulted`/`canceled` (issue #2871). Since the replay no longer runs on the activation path, a failure here does NOT appear on `leaf.activation.failures` above - the activation succeeds and only its data operations fail, so read the two together and do not substitute one for the other. Zero-primed per tree at the arming site, so a flat zero on `faulted` is a measured zero rather than an absent series |
| `orleans.lattice.leaf.unresolved_prepare_ledger_beyond_cap` | counter | `tree`, `partition` | CommitPath | Resident unresolved prepares recorded beyond `MaxDurableUnresolvedReplayWork` (issue #2183); benign on the SQLite `local` profile, a persist hazard on Azure Table (1MB entity cap) - alert there |
| `orleans.lattice.leaf.deferred_terminals_dropped_at_cap` | counter (`{terminal}`) | `tree`, `partition` | CommitPath | Deferred terminals (`TxCommit`, `TxAbort`, `DeleteRange`) dropped by replay pass 1 because the durable `UnresolvedReplayWork` ledger was at `MaxDurableUnresolvedReplayWork`, falling back to the pre-#2165 in-memory clamp (issue #2756). Do NOT substitute `leaf.unresolved_prepare_ledger_beyond_cap` above: that is the uncapped prepare arm, this is the capped deferred arm sampled at the drop. On a tree running no sagas the prepare arm is structurally empty and reads zero forever while this one can still climb, because the deferred branch admits `DeleteRange` and not only transaction terminals. Pre-minted at zero per `(tree, partition)` on entry to replay, so the panel omits `or vector(0)` and a missing line means the build did not land |
| `orleans.lattice.leaf.snapshot.captures` | counter | `tree`, `outcome` | CommitPath | Leaf-snapshot capture attempts by `succeeded`/`failed`/`abandoned` (issue #2696). Before this the capture path carried no instrument at all, so "every capture is failing" and "no capture has ever run" were the same reading - both left `orleans.lattice.storage.snapshot_bytes` at zero. Chart attempts as the sum across `outcome`; keep `abandoned` separate, because a graceful fleet shutdown abandons captures by design and would otherwise read as a provider outage |
| `orleans.lattice.leaf.snapshot.capture.declines` | counter | `tree`, `reason` | CommitPath | Capture invocations that declined before the attempt boundary, by `no_tree_id`/`not_eligible`/`already_in_flight`/`no_coverage_claim` (issues #2696, #2725). Kept off the `outcome` tag of the attempt counter so attempts and durations stay exactly co-populated. Chart it beside the attempt counter: a declined capture is a third state, distinct from a failed one and from an idle deployment, and is the most likely way a self-heal silently never runs. `already_in_flight` dominating is contention against the shared snapshot provider; `not_eligible` dominating is the starved-leaf population; `no_tree_id` above zero is a bug and carries no `tree` tag; `no_coverage_claim` is the benign unclaimable-rows population - live rows, never checkpointed, so a blob would claim no coverage and be refused by the load gate |
| `orleans.lattice.leaf.snapshot.capture.duration` | histogram | `tree`, `outcome` | CommitPath | Duration of a capture attempt, same boundary and tags as `orleans.lattice.leaf.snapshot.captures` so the two share a population (issue #2696). Capture is awaited inline in `OnActivateAsync`, so this is activation latency paid by every caller of the leaf, not background work. Exported with explicit buckets like every other `ms` histogram on this meter, so chart `histogram_quantile` over `_bucket`; an interval mean over delta `_sum` / delta `_count` is a useful companion but is not the only available form |
| `orleans.lattice.leaf.snapshot.capture.concurrency_peak` | gauge | `tenant` (platform sentinel) | CommitPath | Monotone high-water mark of how many leaf-snapshot captures ran **concurrently on one silo** (issue #2696), the cross-leaf quantity PR #2723 deferred and that no per-leaf series can show: the single-flight guard is per activation, so a thousand different leaves each passing their own guard is invisible to every other capture instrument. Chart it as a plain `max` over the raw series and **not** as a rate or a quantile. It never falls, which is the point - a spike is reported by the scrape that sees it and by every scrape after, so an instantaneous gauge panel that would have missed the transient cannot miss this one. Do not expect it to return to zero when load subsides; it is a peak for the life of the process and a restart is what clears it. Carries no `tree` tag by design - the maximum spans every leaf on the silo and the contended snapshot provider is silo-wide, so a per-tree split would report several numbers none of which is the depth the provider saw |
| `orleans.lattice.leaf.snapshot.capture.concurrent_entries` | counter | `tree` | CommitPath | Captures admitted across the attempt boundary while another capture was already in flight (issue #2696). Chart it as a rate beside the peak gauge: the peak says how deep it got, this says how often it happens, and a peak alone cannot separate one transient spike at startup from sustained contention. Do not confuse it with the `already_in_flight` reason on `orleans.lattice.leaf.snapshot.capture.declines` - that counts captures the per-leaf guard **rejected**, this counts captures that were **admitted** alongside a different leaf, so the two populations are disjoint and only this one is real concurrent load. Zero-primed per tree by adding `0` on uncontended attempts, so a flat zero here means measured absence of contention rather than a tree that never captured |
| `orleans.lattice.leaf.snapshot.load_failures` | counter | `tree`, `reason` | CommitPath | Snapshot rehydrate attempts that failed to load, by `resource_exhausted`/`faulted`/`contiguity_exhausted` (issues #2364, #2844). A load failure and a leaf that has no snapshot both decline and both take the full-window cold replay, so without this counter the two are indistinguishable; `resource_exhausted` is the container-memory-limit arm, and since issue #2765 it declines the activation outright instead of falling through to the full-window cold replay, which allocated more than the load that had just failed; `contiguity_exhausted` is the arm where the claim FITTED the budget and the allocation failed anyway, which is a shortage of one unbroken run of memory and not of total memory, so it must not be answered with a larger memory grant |
| `orleans.lattice.leaf.snapshot.hydration_admissions` | counter (`{hydration}`) | `tree`, `outcome` | CommitPath | Snapshot hydrations passing the per-silo byte-budgeted admission gate, by `immediate`/`queued`/`sole_occupancy` (issues #2765, #2844). Leaf state is persisted as JSON text, so one hydration costs several multiples of the stored size in peak heap and an unbounded cold-start fan-out crossed the .NET heap hard limit the runtime sizes from the cgroup. A rising `queued` rate is the gate serialising oversized hydrations rather than letting them exhaust the heap together, and should decay as leaf division shrinks the corpus; `sole_occupancy` is a different predicate - a hydration serialised because its largest **contiguous** allocation was too big to attempt alongside anything else - and does NOT decay with a larger memory grant, which raises every byte-denominated bound and admits more of exactly these claims; all three outcome arms are pre-minted at zero per tree, so absent means the build did not land rather than nothing happening |
| `orleans.lattice.leaf.snapshot.segment_reads` | counter (`{segment}`) | `tree`, `outcome` | CommitPath | Individual snapshot segment frames read during a segmented hydration, by `loaded`/`missing`/`failed` (issue #2914). A snapshot above `LeafSnapshotSegmentBytes` is stored as row-aligned segments in separate grain-state rows and folded one at a time, because the contiguous allocation that failed in issue #2844 happens in the storage provider's column read before any lattice code runs, so the column is what has to be bounded. Chart `missing` and `failed` separately and alert on either: both fail the hydration closed and force a full-window WAL replay, but `missing` is a durability signal (the manifest references a segment that is not there) while `failed` is an I/O signal. All three outcome arms are pre-minted at zero per tree, so absent means the build did not land rather than nothing happening |
| `orleans.lattice.leaf.snapshot.segmented_hydrations` | counter (`{hydration}`) | `tree` | CommitPath | Activation-time hydrations that folded a segmented snapshot to completion (issue #2914). This is the population for which the contiguity bound was actually exercised - leaves whose snapshot fits inline are never counted - so chart it against the `sole_occupancy` arm of `orleans.lattice.leaf.snapshot.hydration_admissions`, which should fall as this rises, since a segmented snapshot no longer presents an oversized contiguous claim to the admission gate. Counted only on a complete fold, so it is not an attempt count; partial folds appear on the `missing`/`failed` arms of `orleans.lattice.leaf.snapshot.segment_reads` instead. Pre-minted at zero per tree |
| `orleans.lattice.leaf.snapshot.segment_peak_bytes` | gauge (`By`) | `tree` | CommitPath | High-water mark of the largest single contiguous segment frame this process has materialised while hydrating a segmented snapshot (issue #2914). This is the panel that answers whether the bound held: it must stay at or below the configured `LeafSnapshotSegmentBytes` window no matter how large the snapshot, so alert on it exceeding that window. Deliberately a monotonic high-water gauge and NOT a histogram, because histograms on this meter export as summaries with only `_sum` and `_count` - no buckets, no quantiles - so the only available reading would be a mean, which dilutes the one large allocation that matters into an average of many small ones. Read it as "the worst this host has seen since it started", not as a current level; it resets on restart by design. Pre-minted at zero per tree |
| `orleans.lattice.leaf.residency.sheds` | counter (`{activation}`) | `tree`, `kind` | CommitPath | Leaf activations deactivated by the per-silo resident leaf working set to hold the hydrated population under its derived byte budget, by `banked`/`unbanked` (issue #2767). The hydration admission gate bounds concurrency only, while a hydrated leaf's frame is retained for the activation's lifetime, so the steady state grows with the number of live activations and was unbounded. A steady `banked` rate is the bound working cheaply; a sustained `unbanked` rate means it has run out of snapshot-backed candidates and is buying headroom with whole-window replays, which points at snapshot coverage rather than at this bound. Both arms are pre-minted at zero per tree, so absent means the build did not land rather than nothing happening |
| `orleans.lattice.leaf.residency.budget_bytes` | gauge (`By`) | none | CommitPath | Resolved byte budget the per-silo resident leaf working set enforces (issue #2788). Read against the container memory grant: a budget of the same order as the whole grant means the process dies before the threshold can be crossed, so the bound never engages and the shed counter's zero says nothing. Originally derived from `GCMemoryInfo.TotalAvailableMemoryBytes` alone, which reports **host physical memory** rather than zero when no heap hard limit is set; now the smaller of the heap hard limit and the cgroup memory limit. A flat 1073741824 is the both-unknown fallback and is a detection defect, not a tuned value |
| `orleans.lattice.leaf.residency.resident_bytes` | gauge (`By`) | none | CommitPath | Bytes accounted to live, un-shed leaf registrations (issue #2788). Stack against the budget gauge; the ratio is the headroom. Resident near budget with a steady shed rate is the bound working; resident at a small fraction of budget while memory is exhausting means the budget is too large to bind, not that leaf residency is cheap |
| `orleans.lattice.leaf.residency.registrations` | gauge (`{registration}`) | none | CommitPath | Leaf registrations currently held by the per-silo resident leaf working set (issue #2788). The instrument that separates an empty ledger from a populated one correctly under budget, which the shed counter reads as zero for both because a counter reports events and both are the absence of one. Registered eagerly from `AddLattice`, so present-and-zero is a measured zero and absent means the build did not land |
| `orleans.lattice.leaf.snapshot.coverage_repairs` | counter | `tree`, `outcome` | CommitPath | Zero-coverage repair evaluations, by `repaired`/`unsatisfied`/`exhausted`/`capture_in_flight`/`no_checkpointed_uncovered_partition` (issues #2692, #2940). A leaf holding a checkpointed partition with no durable snapshot coverage publishes the Zero block pin, and one such leaf disables cursor-based WAL trimming for its whole tree. All five outcome arms are zero-primed per tree, so an absent series means the path never ran for that tree (or the build did not land) and the CommitPath panel carries no `or vector(0)` compensation. The two rejection arms are the diagnostic: `no_checkpointed_uncovered_partition` routes a remedy to the pin/guard seam, `unsatisfied` routes it to the capture seam, and before #2940 both exits were silent. Expect `no_checkpointed_uncovered_partition` to dominate the panel by rate on a healthy estate - it is the denominator, not a fault. Pair with `wal_gc_passes_total{outcome="reclaimed"}`: `repaired` falling to zero while reclaimed passes rise is the drain completing |
| `orleans.lattice.materialiser.drain_lag` | histogram (ms) | `tree` | CommitPath | Leaf-materialiser drain lag p50/p95 (issue #1030 back-pressure) |
| `orleans.lattice.materialiser.lagging_consumers` | histogram (`{consumer}`) | `tree` | CommitPath | Consumers individually past the drain-lag threshold on a tree already over it (issue #2444). Decomposes the `materialiser.drain_lag` minimum above, which reads identically for one dormant consumer and for many falling behind. Emitted only for over-threshold trees, so a healthy estate reports nothing and an absent series is the expected reading; the panel therefore omits the `or vector(0)` fallback its neighbours use, which would draw that healthy absence as a literal zero. Triageable, not diagnosable: it does not name the contributor (issue #2505) |
| `orleans.lattice.materialiser.pin.durable_write_latency` | histogram (ms) | `tree` | CommitPath | Durable pin-write latency: the only materialiser instrument that observes the retention floor rather than in-memory progress (issue #2015) |
| `orleans.lattice.materialiser.pin.reports_shed` | counter | `tree` | CommitPath | Steady-state pin reports dropped to protect a pin store that is not keeping up (issue #2014) |
| `orleans.lattice.snapshot.replay.entries` | counter | `tree` | Overview | Snapshot replay throughput |
| `orleans.lattice.snapshot.replay.duration` | histogram (ms) | `tree` | Overview | Snapshot replay duration p50/p95/p99 |
| `orleans.lattice.snapshot.pins` | observable gauge | `tree` | Overview | Snapshot pins (current) |
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
| `orleans.lattice.get.duration` | histogram (ms) | `tree` | Overview | GetAsync / GetManyAsync envelope p50 (ms); GetAsync / GetManyAsync envelope p95 / p99 (ms) |
| `orleans.lattice.get.stage.duration` | histogram (ms) | `tree`, `stage` | Overview | GetAsync stage breakdown p95 (ms) |
| `orleans.lattice.get_many.duration` | histogram (ms) | `tree` | Overview | GetAsync / GetManyAsync envelope p50 (ms); GetAsync / GetManyAsync envelope p95 / p99 (ms) |
| `orleans.lattice.get_many.stage.duration` | histogram (ms) | `tree`, `stage` | Overview | GetManyAsync stage breakdown p95 (ms) |
| `orleans.lattice.exists.duration` | histogram (ms) | `tree` | Overview | ExistsAsync / GetWithVersionAsync envelope p95 (ms) |
| `orleans.lattice.get_with_version.duration` | histogram (ms) | `tree` | Overview | ExistsAsync / GetWithVersionAsync envelope p95 (ms) |
| `orleans.lattice.set.duration` | histogram (ms) | `tree` | CommitPath | SetAsync / SetManyAsync envelope p50 (ms); SetAsync / SetManyAsync envelope p95 (ms) |
| `orleans.lattice.set.stage.duration` | histogram (ms) | `tree`, `stage` | CommitPath | SetAsync stage breakdown p95 (ms) |
| `orleans.lattice.set_many.duration` | histogram (ms) | `tree` | CommitPath | SetAsync / SetManyAsync envelope p50 (ms); SetAsync / SetManyAsync envelope p95 (ms) |
| `orleans.lattice.set_many.stage.duration` | histogram (ms) | `tree`, `stage` | CommitPath | SetManyAsync stage breakdown p95 (ms) |
| `orleans.lattice.shard_root.set_many.leaf_rpc.duration` | histogram (ms) | `tree` | CommitPath | ShardRoot.SetMany sub-attribution p95 (ms) |
| `orleans.lattice.shard_root.set_many.local_apply.duration` | histogram (ms) | `tree` | CommitPath | ShardRoot.SetMany sub-attribution p95 (ms) |
| `orleans.lattice.shard_root.set_many.shadow_forward.duration` | histogram (ms) | `tree` | CommitPath | ShardRoot.SetMany sub-attribution p95 (ms) |
| `orleans.lattice.warmup.invocations` | counter (`{call}`) | `tree` | CommitPath | WarmUpAsync - invocations and duration |
| `orleans.lattice.warmup.duration` | histogram (ms) | `tree` | CommitPath | WarmUpAsync - invocations and duration |
| `orleans.lattice.warmup.leaf_cache.prewarmed` | counter (`{leaf}`) | `tree`, `shard`, tenant | CommitPath | Leaf-cache pre-warm (on by default) - leaves primed, fan-out cost, model size |
| `orleans.lattice.warmup.leaf_cache.duration` | histogram (ms) | `tree`, `shard`, tenant | CommitPath | Leaf-cache pre-warm (on by default) - leaves primed, fan-out cost, model size |
| `orleans.lattice.leaf_access.model.leaves` | histogram (`{leaf}`) | `tree`, `shard`, tenant | CommitPath | Leaf-cache pre-warm (on by default) - leaves primed, fan-out cost, model size |
| `orleans.lattice.leaf.commit.in_flight` | histogram (`{commit}`) | `tree` | CommitPath | Leaf commit concurrency (in-flight) p95 |
| `orleans.lattice.leaf.digest.publishes` | counter (`{publish}`) | `tree`, `path` | CommitPath | Digest publish path attribution (ops/s) - coalescing efficacy |
| `orleans.lattice.provider.commit.duration` | histogram (ms) | `tree`, `shard`, `phase`, `pipeline_phase2` | CommitPath | Storage-provider phase-2 commit p95 (ms) + batch size |
| `orleans.lattice.provider.phase2.batch_size` | histogram (`{commit}`) | `tree`, `shard` | CommitPath | Storage-provider phase-2 commit p95 (ms) + batch size |
| `orleans.lattice.provider.retry.attempts` | counter (`{attempt}`) | `status` | CommitPath | Storage-provider retries (ops/s) - attempts vs exhausted vs idempotent-replays vs phase1-transient |
| `orleans.lattice.provider.retry.exhausted` | counter (`{call}`) | `tree`, `shard`, `phase`, `status` | CommitPath | Storage-provider retries (ops/s) - attempts vs exhausted vs idempotent-replays vs phase1-transient |
| `orleans.lattice.provider.idempotent_replays` | counter (`{call}`) | `tree`, `shard`, `phase` | CommitPath | Storage-provider retries (ops/s) - attempts vs exhausted vs idempotent-replays vs phase1-transient |
| `orleans.lattice.wal.append.turn_wait` | histogram (ms) | `tree`, `shard`, `wal_partitions`, `wal_max_pending_batches` | CommitPath | WAL append latency p95 (ms) - turn-wait / provider / dispatch |
| `orleans.lattice.wal.append.provider.duration` | histogram (ms) | `tree`, `shard`, `wal_partitions`, `wal_max_pending_batches` | CommitPath | WAL append latency p95 (ms) - turn-wait / provider / dispatch |
| `orleans.lattice.wal.append.in_flight` | histogram (`{flush}`) | `tree`, `shard`, `wal_partitions`, `wal_max_pending_batches` | CommitPath | WAL pipeline depth p95 - in-flight flushes / queue depth |
| `orleans.lattice.wal.append.queue_depth` | histogram (`{entry}`) | `tree`, `shard`, `wal_partitions`, `wal_max_pending_batches` | CommitPath | WAL pipeline depth p95 - in-flight flushes / queue depth |
| `orleans.lattice.wal.append.batch_entries` | histogram (`{entry}`) | `tree`, `shard`, `wal_partitions`, `wal_max_pending_batches` | CommitPath | WAL batch shape p95 - entries / bytes / dispatch-entries |
| `orleans.lattice.wal.append.batch_bytes` | histogram (`By`) | `tree`, `shard`, `wal_partitions`, `wal_max_pending_batches` | CommitPath | WAL batch shape p95 - entries / bytes / dispatch-entries |
| `orleans.lattice.wal.shard.dispatch.duration` | histogram (ms) | `tree`, `shard`, `wal_partitions`, `wal_max_pending_batches` | CommitPath | WAL append latency p95 (ms) - turn-wait / provider / dispatch |
| `orleans.lattice.wal.shard.dispatch.entries` | histogram (`{entry}`) | `tree`, `shard`, `wal_partitions`, `wal_max_pending_batches` | CommitPath, Replication | WAL batch shape p95 - entries / bytes / dispatch-entries; Log-tailing producer: leaf WAL append vs ship rate (ops/s) |
| `orleans.lattice.wal.saturation.state` | observable gauge (0/1/2) | `tree` | Overview | WAL saturation regime - % time non-Healthy (1h); WAL saturation regime - current state per tree |
| `orleans.lattice.wal.saturation.transitions` | counter (`{transition}`) | `tree`, `shard`, `partition`, `state`, `previous_state` | Overview | WAL saturation regime - per-partition attribution (heat-map); WAL saturation regime - transition rate by direction (ops/s) |
| `orleans.lattice.storage.wal.uncompressed_bytes` | counter (`By`) | `tree` | Overview | WAL compression savings ratio by tree |
| `orleans.lattice.storage.wal.stored_bytes` | counter (`By`) | `tree` | Overview | WAL compression savings ratio by tree |
| `orleans.lattice.storage.wal.compression_skipped` | counter (`{row}`) | `tree`, `reason` | Overview | WAL compression skips by reason |
| `orleans.lattice.saga.prepare.duration` | histogram (ms) | `tree`, `wal_partitions` | AtomicWrites | Saga phase durations p95 (ms) - prepare / decision / broadcast / checkpoint / reminder |
| `orleans.lattice.saga.terminal_decision.duration` | histogram (ms) | `tree`, `wal_partitions` | AtomicWrites | Saga phase durations p95 (ms) - prepare / decision / broadcast / checkpoint / reminder |
| `orleans.lattice.saga.broadcast.duration` | histogram (ms) | `tree`, `wal_partitions` | AtomicWrites | Saga phase durations p95 (ms) - prepare / decision / broadcast / checkpoint / reminder |
| `orleans.lattice.saga.broadcast.shard.duration` | histogram (ms) | `tree`, `shard` | AtomicWrites | Saga broadcast sub-attribution p95 (ms) - per-shard / per-leaf / per-shard-stage |
| `orleans.lattice.saga.broadcast.leaf.duration` | histogram (ms) | `tree`, `shard` | AtomicWrites | Saga broadcast sub-attribution p95 (ms) - per-shard / per-leaf / per-shard-stage |
| `orleans.lattice.saga.broadcast.shard.stage.duration` | histogram (ms) | `tree`, `shard`, `stage` | AtomicWrites | Saga broadcast sub-attribution p95 (ms) - per-shard / per-leaf / per-shard-stage |
| `orleans.lattice.saga.checkpoint.duration` | histogram (ms) | `tree`, `phase` | AtomicWrites | Saga phase durations p95 (ms) - prepare / decision / broadcast / checkpoint / reminder |
| `orleans.lattice.saga.reminder.duration` | histogram (ms) | `tree`, `phase` | AtomicWrites | Saga phase durations p95 (ms) - prepare / decision / broadcast / checkpoint / reminder |
| `orleans.lattice.saga.perkey.duration` | histogram (ms) | `tree`, `wal_partitions` | AtomicWrites | Per-key saga work - p95 per-key duration (ms) |
| `orleans.lattice.saga.fanout.size` | histogram (`{entry}`) | `tree`, `wal_partitions` | AtomicWrites | Saga fan-out size (entries per saga) |
| `orleans.lattice.atomic_write.cross_tree.completed` | counter (`{saga}`) | `outcome`, `tree_count` | AtomicWrites | Cross-tree atomic write outcomes (rate); Cross-tree failure rate (%) |
| `orleans.lattice.atomic_write.cross_tree.duration` | histogram (ms) | `outcome` | AtomicWrites | Cross-tree coordinator duration (p50/p95/p99 ms) |
| `orleans.lattice.atomic_write.cross_tree.participants` | histogram (`{tree}`) | `outcome` | AtomicWrites | Cross-tree participant fan-out (trees per saga) |
| `orleans.lattice.grainindex.grains_enrolled` | counter (`{grain}`) | `index`, `path` | GrainIndex | Grains enrolled per second, by route |
| `orleans.lattice.grainindex.entries` | up-down counter (`{entry}`) | `index` | GrainIndex | Index entries held |
| `orleans.lattice.grainindex.write_failures` | counter (`{failure}`) | `index`, `path` | GrainIndex | Index write failures per second, by route |
| `orleans.lattice.grainindex.projection.duration` | histogram (ms) | `index` | GrainIndex | Projection latency percentiles |
| `orleans.lattice.grainindex.backfill.processed` | observable gauge (`{grain}`) | `index` | GrainIndex | Backfill progress (processed vs total) |
| `orleans.lattice.grainindex.backfill.total` | observable gauge (`{grain}`) | `index` | GrainIndex | Backfill progress (processed vs total) |
| `orleans.lattice.grainindex.backfill.percent_complete` | observable gauge (`%`) | `index` | GrainIndex | Backfill percent complete |
| `orleans.lattice.grainindex.backfill.state` | observable gauge (`{state}`) | `index` | GrainIndex | Backfill state |
| `orleans.lattice.tag_index.reconcile.sweeps` | counter (`{sweep}`) | `index`, `outcome` | *(not charted)* | Background tag-index reconciliation sweeps, by outcome (`clean`, `repaired`, `probe_only`) |
| `orleans.lattice.tag_index.reconcile.trees.probed` | counter (`{tree}`) | `index` | *(not charted)* | Covered trees whose digest fingerprint a sweep probed |
| `orleans.lattice.tag_index.reconcile.trees.mismatched` | counter (`{tree}`) | `index` | *(not charted)* | Covered trees a sweep found divergent from their digest baseline |
| `orleans.lattice.tag_index.reconcile.orphan_rows.removed` | counter (`{row}`) | `index` | *(not charted)* | Orphan membership rows removed by background reconciliation |
| `orleans.lattice.tag_index.reconcile.duration` | histogram (ms) | `index` | *(not charted)* | Wall-clock duration of a background reconciliation sweep |

The `orleans.lattice.tag_index.reconcile.*` family is emitted by the background tag-index reconciliation sweep and is documented in [Metrics](../lattice/metrics.md). No bundled dashboard charts it yet; scrape it directly, or add panels and update the rows above.

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
| `orleans.lattice.backup.scheduler.failures` | counter | `scope`, `reason` | Backup | Scheduler capture failures by reason |
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

## `orleans.lattice.tenancy` meter

Per-tenant usage, quota, burst, and metered-overage telemetry published by the opt-in `lattice.tenancy` add-on. Charted by the Per-Tenant Observability dashboard; coverage enforced from `Orleans.Lattice.Tenancy.Tests`.

Every instrument is an **observable gauge** published on a fixed cadence (`TenantObservabilityOptions.PublishInterval`, default 30 seconds) from the last landed metering sample, so these are periodic samples rather than live readings. Every series carries a `tenant` tag except the cluster-aggregate tenant count. A `quota.*` gauge emits a measurement only for a **bounded** dimension - an unbounded ceiling contributes no series at all, so "no series" reads as "unlimited", not "zero". The `overage.*` gauges are grow-only converged sums, not instantaneous readings.

`MaxOpsPerSecond` has no gauge: the rate budget is enforced from silo-local token buckets rather than a published aggregate, so a breach surfaces as an `ops-per-second` `LatticeQuotaExceededException` rather than a series.

| Instrument | Type | Tags | Dashboard | Panel(s) |
|------------|------|------|-----------|----------|
| `orleans.lattice.tenancy.tenants` | observable gauge (`{tenant}`) | `tenant` = `_platform_` (cluster aggregate) | Per-Tenant Observability | Registered tenants |
| `orleans.lattice.tenancy.usage.bytes` | observable gauge (`By`) | `tenant` | Per-Tenant Observability | Stored bytes by tenant |
| `orleans.lattice.tenancy.quota.bytes` | observable gauge (`By`) | `tenant` | Per-Tenant Observability | Stored bytes by tenant (quota overlay) |
| `orleans.lattice.tenancy.usage.keys` | observable gauge (`{key}`) | `tenant` | Per-Tenant Observability | Live keys by tenant |
| `orleans.lattice.tenancy.quota.keys` | observable gauge (`{key}`) | `tenant` | Per-Tenant Observability | Live keys by tenant (quota overlay) |
| `orleans.lattice.tenancy.usage.memory_bytes` | observable gauge (`By`) | `tenant` | Per-Tenant Observability | Resident memory by tenant |
| `orleans.lattice.tenancy.quota.memory_bytes` | observable gauge (`By`) | `tenant` | Per-Tenant Observability | Resident memory by tenant (quota overlay) |
| `orleans.lattice.tenancy.usage.trees` | observable gauge (`{tree}`) | `tenant` | Per-Tenant Observability | Owned trees by tenant |
| `orleans.lattice.tenancy.quota.trees` | observable gauge (`{tree}`) | `tenant` | Per-Tenant Observability | Owned trees by tenant (quota overlay) |
| `orleans.lattice.tenancy.quota.burst_percent` | observable gauge (`%`) | `tenant` | Per-Tenant Observability | Burst headroom by tenant |
| `orleans.lattice.tenancy.overage.bytes` | observable gauge (`By`) | `tenant` | Per-Tenant Observability | Metered byte overage by tenant |
| `orleans.lattice.tenancy.overage.keys` | observable gauge (`{key}`) | `tenant` | Per-Tenant Observability | Metered overage by tenant (keys) |
| `orleans.lattice.tenancy.overage.memory_bytes` | observable gauge (`By`) | `tenant` | Per-Tenant Observability | Metered overage by tenant (memory) |
| `orleans.lattice.tenancy.overage.trees` | observable gauge (`{tree}`) | `tenant` | Per-Tenant Observability | Metered overage by tenant (trees) |

## `Orleans.Lattice.Api.Mcp.RepoContext` meter

The repository-context MCP surface's telemetry, published by the opt-in `lattice.api.mcp.repocontext` add-on. This meter is named unlike every other meter on this page: it carries the assembly-style name `Orleans.Lattice.Api.Mcp.RepoContext` rather than a dotted-lowercase `orleans.lattice.*` name, and its instruments carry a bare `repocontext.` prefix. The two halves of that mismatch behave differently, and the difference is load-bearing. The container's Prometheus exposition subscribes by **meter** name and compares case-insensitively, so `Orleans.Lattice.Api.Mcp.RepoContext` does match an `orleans.lattice` prefix and these series are collected; the collector pins that exact meter name in a test. A selector or a doc guard written against the **instrument** names does not match, because those begin `repocontext.` rather than `orleans.lattice.`. That is why this section exists as its own table rather than as rows under an existing meter, and why a doc-coverage fixture for this package needs its own instrument-name prefix.

**No bundled dashboard charts these instruments, so the Panel(s) column reads "not charted" throughout.** That is a real gap recorded here rather than an omission in this table, and the record is executable rather than prose: `RepoContextMetricsToPanelMapTests` in `test/lattice.dashboards` asserts that every `repocontext.*` instrument declared in source has a row here, that every row here still names a declared instrument, that each row still reads "not charted", and that no bundled dashboard references a `repocontext_` token. The dashboard half carries a positive control over `orleans_lattice_` tokens, so "no repocontext token found" cannot be produced by broken token extraction. The container serves a Prometheus text exposition on `/metrics`, on the same listener as MCP and the health probes, so these series are scrapeable today. That inverts the earlier position: while the endpoint returned 404 nothing could scrape them, so an unmapped instrument had no observable consequence; now it is a live gap. One caveat when scoping a scraper: the exposition subscribes by meter-name prefix, so it covers this meter and the core `orleans.lattice` meter alike, but a selector narrowed to `Orleans.Lattice.Api.Mcp.RepoContext` alone measures a false absence for core series such as `orleans.lattice.leaf.deactivation.checkpoint_delta` and `orleans.lattice.leaf.activation.failures`, which are published on `LatticeMetrics` rather than here.

A second cause is structural and is not removed by that fix. A `Histogram<T>` renders on this endpoint as a Prometheus `summary` carrying `_sum` and `_count` and **no `_bucket` series**, so a `histogram_quantile` panel over it returns nothing, and the common dashboard idiom of appending `or vector(0)` then substitutes a literal zero that is indistinguishable from a genuine sustained-zero fault. That applies to `repocontext.retrieval.ready_seconds`, the one histogram in the table below: chart it as `_sum` and `_count`, and treat any quantile panel against this endpoint as unavailable rather than as measured. The full account is in [the container guide](../lattice.api.mcp.repocontext/container.md). Author the panels and update this column when they land.

Every instrument here carries the derived `tenant` label with the reserved `_platform_` value described in [The derived `tenant` label](#the-derived-tenant-label): the repository-context surface is platform-owned and has no owning tree.

| Instrument | Type | Tags | Dashboard | Panel(s) |
|------------|------|------|-----------|----------|
| `repocontext.calls` | counter (`{call}`, **per-operation**) | `command` | (none) | **not charted** |
| `repocontext.response_tokens` | counter (`{token}`) | `command` | (none) | **not charted** |
| `repocontext.reads_replaced_tokens` | counter (`{token}`) | `command` | (none) | **not charted** |
| `repocontext.ann.sweep` | counter (`{sweep}`) | `outcome` = `armed`, `empty`, `faulted` | (none) | **not charted** |
| `repocontext.ann.sweep.arming` | counter (`{repository}`) | `result` = `armed`, `deferred`, `faulted` | (none) | **not charted** - denominated by repository visits rather than by sweeps, so it must not be ratioed against `repocontext.ann.sweep` |
| `repocontext.ann.build.corpus` | counter (`{build}`) | `coverage` = `nonempty`, `unrestricted`, `filtered`, `denied`, `unknown` | (none) | **not charted** |
| `repocontext.ann.build.denial_terminal` | counter (`{coordinator}`) | (none) | (none) | **not charted** |
| `repocontext.ann.build.slice` | counter (`{step}`) | `repository` = the onboarded repository id; `space` = `{model-id}/{dimension}` or `unspecified`; `phase` = `coordinating`, `opening`, `ingesting`, `training`, `persisting`, `reconciling`; `progress` = `advanced`, `churned`, `starved`, `idle`, `faulted`; `cause` = `scan-page-stalled`, `projection-stale`, `dependency-unavailable`, `plane-rejected`, `unexpected` (on the `faulted` arm only) | (none) | **not charted** - twenty-two `(phase, progress)` arms are zero-primed per `(repository, space)` plane when that plane is first armed, the five `cause` values deliberately are not, so a cause reading zero is uninterpretable rather than innocent: read it only once `faulted` itself is non-zero, against which the causes sum exactly. `coordinating` and `opening` precede any build step, so they are primed on the `faulted` arm alone and the remaining eight combinations are deliberately never minted. `phase` on a fault is read at the fault site rather than snapshotted on entry, because a step entered in `training` also runs the persist within the same step; without that, a failed persist and a failed corpus read are the same series (issue #2855). Cardinality is repositories x embedding spaces, one durable index and one coordinator apiece, so it is operator-chosen and small |
| `repocontext.ann.partitioning` | counter (`{observation}`) | `state` = `partitioned`, `unpartitioned-small`, `unpartitioned-large` | (none) | **not charted** |
| `repocontext.ann.repartition` | counter (`{training}`) | `outcome` = `partitioned`, `declined` | (none) | **not charted** |
| `repocontext.ann.index.load` | counter (`{attempt}`) | `outcome` = `fresh`, `resumed`, `faulted` | (none) | **not charted** - a two-sided discriminator read as a pair, not as a rate: `faulted` rising with `resumed` flat is a durable-index load that is restarting rather than resuming (#2953), and no faults at all is health. All three outcome arms are pre-minted at zero, so an absent arm means the build did not ship. |
| `repocontext.retrieval.ann.search` | counter (`{query}`) | `state` = `bootstrapping`, `exhaustive`, `approximate` | (none) | **not charted** |
| `repocontext.retrieval.exact_gather.faults` | counter (`{fault}`) | `fault` = `stalled`, `timed_out`, `exhausted`, `abandoned`, `propagated`, `deterministic` | (none) | **not charted** - all six fault arms are zero-primed. The first four are absorbed as capacity and backed off; `propagated` is an index-integrity fault. Do **not** read a flat `propagated` count beside a climbing absorbed one as load - that reading ran issue #2948's six-hour total retrieval outage as capacity pressure. What separates a deterministic defect from load is the fault rate, which load cannot hold at one hundred percent, and `deterministic` is the arm that reports it. Either non-absorbed arm warrants an operator |
| `repocontext.retrieval.ready_seconds` | histogram (`s`) | `phase` = `serving`, `keyword_only`, `nothing_registered` | (none) | **not charted** |
| `repocontext.retrieval.unavailable` | counter (`{event}`) | `cause` | (none) | **not charted** |
| `repocontext.vectorplane.rederive` | counter (`{event}`) | `tree`, `outcome` | (none) | **not charted** |
| `repocontext.retrieval.duration` | histogram (`s`) | `tool` = `search`, `context`, `outline`, `related`; `path` = the resolved retrieval path, `not_applicable` (graph read), or `unresolved` (call ended before a path was settled) | (none) | **not charted** |
| `repocontext.retrieval.stage.duration` | histogram (`s`) | `stage` = `embed`, `vector_search`, `hydrate`, `keyword_scan`; `path` as above | (none) | **not charted** |
| `repocontext.bootstrap.pass_arm_faults` | counter (`{fault}`) | `arm` = `retire`, `ingest-files`, `ingest-symbols`, `ingest-memory`; `kind` = `scan-page-stalled` or an exception type name | (none) | **not charted** |
| `repocontext.bootstrap.phase_cancelled` | counter (`{cancellation}`) | `phase` = `Walking`, `Reconciling`, `Applying`, `Vectorising` | (none) | **not charted** - zero-primed for all four phases, so any non-zero value is an indexing run whose in-flight work was discarded |
| `repocontext.bootstrap.phase_cancelled.discarded_time` | counter (`ms`) | `phase` = `Walking`, `Reconciling`, `Applying`, `Vectorising` | (none) | **not charted** - running total of run time thrown away by the cancellations above; zero-primed for the same four phases |
| `repocontext.bootstrap.memory_marker_scan` | counter (`{walk}`) | `outcome` = `complete`, `resumed`, `banked` | (none) | **not charted** - read as a three-state pair, not a rate: `banked` rising with both completion arms flat is a marker scan that banks forever without converging (#2071), a non-zero `resumed` is the resumable cursor proven working end to end, and all three at their primed zero means the scan was never reached at all. A `complete` reading is not evidence the bootstrap as a whole is healthy (#2964). |
| `repocontext.bootstrap.coverage_probe` | counter (`{probe}`) | `arm` = `file`, `symbol`, `sweep`; `outcome` = `conclusive`, `gate_pruned`, `probe_failed` | (none) | **not charted** - read as a four-state grid, not a rate. `gate_pruned` > 0 on any arm is a standing misconfiguration that never clears by waiting: the ingestor cannot read its own membership keys, so it stands the back-fill sweep down and reports a converged bootstrap forever (#2964). `probe_failed` > 0 with `gate_pruned` == 0 in the same arm means that zero proves nothing, because the gate check sits below the probe-failure branch at every site. `conclusive` only is healthy. All nine at their primed zero means no coverage resolution was reached - which is also the normal steady state of a keyword-only deployment with no embedder bound, and this instrument alone cannot separate those two; check whether an embedding provider is registered. The `arm` tag localises which consumer stood down, not which grant is missing: there is one membership grant behind all three. |

The two retrieval-latency histograms are a matched pair and neither is readable alone. `repocontext.retrieval.duration` is recorded from a `finally` on **every** call, including a cancelled or faulted one (tagged `path="unresolved"` when it ended before a path was settled), so its `_count` is a true call total. `repocontext.retrieval.stage.duration` is recorded **only for stages that actually ran**, so it is deliberately sparse and its zero does not describe itself: it is the call total that turns the absence into a measurement. No `embed` beside a rising call total on `repocontext.retrieval.duration` is an intended keyword-only host; both at zero means no retrieval ran at all. Both are subject to the summary rendering described above, so read them as `rate(..._sum[5m]) / rate(..._count[5m])` - a mean - and treat `histogram_quantile` against this endpoint as unavailable rather than as returning a wrong answer.

Several of these are partitions of a total rather than free-standing counts, and reading them as free-standing counts inverts their meaning. `repocontext.ann.sweep` counts **every** sweep including the faulting one, and a faulting sweep is re-run on a retry backoff rather than at the sweep interval, so `outcome="faulted"` must not be denominated by the sweep interval. `repocontext.retrieval.ann.search` counts every query including the ones the approximate plane could not answer. `repocontext.ann.build.corpus` counts every approximate-index build that reached `Ready`, including the ordinary `coverage="nonempty"` ones, which is what makes `coverage="denied"` reading zero beside a rising total a measured absence of authorization denial rather than an absent measurement - a denied range read returns a clean empty result rather than throwing, so without that denominator an authorization failure and an empty repository are the same observation. In each case a zero on one tag value beside a non-zero total is a **measured** absence. Every arm of these partitions is pre-minted at zero when its reporter is constructed, so an arm reading zero and an arm being absent are different observations rather than the same one: a series whose first occurrence falls after the collector reaches a ceiling is refused at creation and never appears, so an **absent** arm is a collector fault to be read from `lattice_metrics_series` and `lattice_metrics_dropped_measurements_by_family_total`, not a statement about the subsystem. All arms reading zero means no iteration has completed yet, which does not on its own establish that the loop is not running; the service's startup line reports that directly.

### The derived `tenant` label

**Every instrument on every meter carries a `tenant` tag.** It is derived from the tree id rather than measured, and it is emitted on tenancy-on and tenancy-off clusters alike, so a panel or a named query is byte-identical in both deployment modes - there are no tenancy-on and tenancy-off variants of a query.

The value is one of three kinds:

| Value | Means |
|---|---|
| a tenant id | The series belongs to that tenant. Tenancy composes tree ids as `t/{tenantId}/{name}` and ownership is re-derived from that prefix. |
| `default` | The reserved legacy-adoption tenant, which owns every bare unsegmented tree id - and therefore every series on a cluster with tenancy off. It is a real, queryable tenant. |
| `_platform_` | A reserved sentinel for series that belong to the platform and to no tenant: the `_lattice_` and `sys-` tree namespaces, and every instrument carrying no tree dimension at all. |

`_platform_` is a **sentinel rather than an absent label**, deliberately. If platform-owned series were simply untagged, a tenant-scoped matcher would exclude them only by accident of absence, and any later change that started tagging them would silently widen every existing query. Naming the platform explicitly means `{tenant="acme"}` excludes it by stating so. The value opens with an underscore, which the tenant-id grammar forbids, so it can never collide with a real tenant.

**Do not derive a tenant by regex over the `tree` label.** Tree ownership is a genuine three-way classification and a single regex cannot reproduce it: tenant `acme` maps cleanly to `tree=~"^t/acme/.*"`, but the default tenant's adopted legacy ids are bare, so its matcher becomes `tree!~"^t/.*"` - which also matches the `_lattice_` and `sys-` platform namespaces and leaks platform-internal series into a tenant's view. An instrument with no `tree` tag cannot be scoped that way at all.

The label is cardinality-neutral. `tree -> tenant` is a function, so it attaches to series that already exist rather than multiplying them: two measurements that shared a series before still share one after, because equal tree ids always derive equal tenant labels.

A small number of instruments are documented as **unscopable** - cluster-level and per-peer telemetry that has no owning tree by construction. Those carry `_platform_` rather than being left untagged, for the reason above.

`orleans.lattice.tenancy` remains the only meter whose instruments are *about* tenancy (quota, usage, enforcement). The `tenant` label described here is a dimension on everything else, which is a different thing: it says whose the series is, not what it measures.
