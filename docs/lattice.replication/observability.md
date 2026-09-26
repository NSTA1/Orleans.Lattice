# Observability

`Orleans.Lattice.Replication` publishes every replication-side instrument on a single meter, `orleans.lattice.replication`. An OpenTelemetry pipeline (or any `MeterListener`) subscribes once and receives every replication metric. The instruments fall into four shapes:

- **Per-peer gauges** - `entries_behind`, `bytes_behind`, `ship_in_flight`, `consecutive_errors`, `last_contact_seconds`. Owned by `ReplicationPeerStats`. Tagged `tree` + `peer`. The `consecutive_errors` and `last_contact_seconds` gauges are **bidirectional** and additionally carry a `direction` tag (`outbound` from the local sender's ship loop, `inbound` from the local receiver's apply loop). `entries_behind`, `bytes_behind`, and `ship_in_flight` remain outbound-only (the receiver does not track a per-peer backlog into itself, nor does it pipeline into itself).
- **Per-operation histograms** - `ship.duration`, `apply.duration`, `apply.lag`, `apply.parallel_runs`, `ship.effective_batch_size`, `ship.ack_latency`. Reported in milliseconds except `apply.parallel_runs` (unit `{run}`) and `ship.effective_batch_size` (unit `{entry}`).
- **Throughput counters** - `wal.entries_shipped`. Counts entries the producer durably ships to each peer; correlate it against WAL retention / GC to confirm the sender keeps pace with the log. The companion `wal.entries_trimmed` counter belongs to the core library and is published on the `orleans.lattice` meter (`LatticeMetrics.WalEntriesTrimmed`); subscribe to both meters when correlating ship-rate against trim-rate. The `ship.redundant_payloads` / `ship.redundant_payload_bytes` counters (see below) ride on the same meter and emit in a default build because content-hash dedup measurement is on by default; the `coalesce.entries_elided` / `coalesce.bytes_elided` / `coalesce.deltas_merged` counters likewise emit by default because pre-ship coalescing is on by default. Each set falls silent only when its option (`ContentHashDedupEnabled` / `PreShipCoalescingEnabled`) is explicitly set to `false`.
- **DLQ counters** - `dead_letter.enqueued`, `dead_letter.removed`. Tagged `tree` + `reason`.

These four shapes are the headline families, not the full set: the [instrument index](#instrument-index) at the end of this page lists every instrument on the meter with its kind, unit, and tags, and points at the page that documents it.

Every instrument also carries the repository-wide derived `tenant` tag (`LatticeTenantLabel.TagTenant`), computed from the `tree` value, or fixed to the platform sentinel `_platform_` on the instruments that carry no `tree` tag. The two `wire_version.*` gauges are the exception: they carry only `tree` and `peer`. The Tags columns on this page list the replication-specific dimensions and leave `tenant` implicit.

## Replication-lag histogram (`apply.lag`)

`orleans.lattice.replication.apply.lag` is recorded by the canonical applier immediately after a successful point apply (`Set` / `Delete`). The sample is `now - entry.Timestamp.WallClockTicks` in milliseconds, **clamped to a non-negative value** so a future-dated source HLC (e.g. a faster-moving peer's wall clock) reports as `0` rather than corrupting the histogram with a negative sample.

| Property | Value |
|---|---|
| Name | `orleans.lattice.replication.apply.lag` |
| Unit | `ms` |
| Tags | `tree`, `peer` |

The `peer` tag carries the entry's `OriginClusterId` - i.e. the **authoring** cluster of the replicated mutation, not the immediate transport hop the receiver pulled it from. The tag is read from the producer-side `WalRecord.OriginClusterId` slot. The shipper - and the leaf re-replay repair that re-ships from the WAL - only ever sends entries its own cluster authored, so on those paths the authoring cluster and the delivering peer coincide and an entry a cluster applied on another origin's behalf is never re-shipped onward. The two snapshot-based paths are the exception, because a snapshot row carries no origin: a snapshot bootstrap stamps the source cluster it bootstraps from as the origin of every drained entry, and the anti-entropy bootstrap fallback stamps the sending cluster's id on every row it re-ships. On both, a row the sender holds on another origin's behalf reaches the receiver attributed to the delivering cluster, and a point write that reaches the merge step records a sample measuring the row's age (now minus its stored HLC) rather than a replication delay. Operators filtering inbound apply lag by the source-of-truth replica use this tag value directly; queries that need transport-hop attribution join the `tree` + `peer` pair against the cluster's known replication topology.

The histogram is intentionally not recorded for:

- **`MutationKind.DeleteRange`** - range deletes carry `HybridLogicalClock.Zero` by design (a range walk produces many per-leaf HLCs that cannot be faithfully collapsed into one), so the lag would be a meaningless multi-decade value.
- **Deduplicated or deferred deliveries** - an entry dropped at or below the snapshot-pinned causal floor, suppressed by the shadow-forward identity cache, or deferred by a restore saga's receive fence never reaches the merge step, so reporting lag would conflate "applied" and "filtered" samples.
- **Local-origin entries** - the apply path short-circuits at the local-origin no-op gate before touching the receiver-side merge.
- **Source HLC equal to `Zero`** - protects against a malformed entry that would otherwise publish a garbage "now - 0" sample.

A receiver whose every delivery is dropped by the pinned floor or the identity cache reports an empty `apply.lag` distribution. That is the correct signal: there is no replication progress to measure. A re-delivery that survives both - its cache entry already evicted - re-applies idempotently under per-key last-writer-wins and does record a sample.

## Apply-duration histogram (`apply.duration`)

`orleans.lattice.replication.apply.duration` records the wall-clock time the canonical applier spends inside `ApplyAsync`, from entry through every terminal return path. The body is wrapped in a `try { ... } finally { Record(...); }` so an uncaught exception still records a sample tagged with the failure outcome before unwinding. The duration is read via `Stopwatch.GetElapsedTime(long)`, which is allocation-free.

| Property | Value |
|---|---|
| Name | `orleans.lattice.replication.apply.duration` |
| Unit | `ms` |
| Tags | `tree`, `peer`, `outcome` |

The `peer` tag carries the same value as `apply.lag`'s `peer` tag - the entry's `OriginClusterId`, identifying the authoring cluster rather than the transport hop. The batch path's `ApplyOriginRunAsync` groups entries into contiguous same-`(treeId, originClusterId)` runs and records each per-entry duration with the run's shared `peer` value, so multi-origin batches surface as one `peer` per run rather than collapsing into a single dominant value.

The `outcome` tag partitions the histogram into ten mutually-exclusive buckets:

| Value | Constant | When |
|---|---|---|
| `success` | `LatticeReplicationMetrics.OutcomeSuccess` | The entry was applied successfully - directly applied point operations (`Set` / `Delete`), range deletes, and saga terminal marks (`TxCommit` / `TxAbort`) all contribute. Each `ApplyAsync` invocation records exactly one `apply.duration` sample regardless of how many entries the call drains from the causal-apply buffer: a drain cascade triggered by an arriving satisfier contributes its drained-entry work to the satisfier's own `success` sample, and the originally parked entries do not generate additional samples on drain. |
| `dedup` | `LatticeReplicationMetrics.OutcomeDedup` | The entry was short-circuited before merge: its `Timestamp` is at or below the origin's snapshot-pinned causal floor, it is a tombstone-reap envelope (local structural cleanup that is never meant to ship), a restore saga's durable receive fence deferred it (the result carries `Deferred = true` and the sender re-ships it once the fence lifts), or its origin is the local cluster - that check is the receiver's only enforcement that an entry is never applied back onto its authoring cluster, not defence-in-depth behind the sender's outbound origin filter. |
| `failure` | `LatticeReplicationMetrics.OutcomeFailure` | The apply attempt threw. Recorded in the `finally` path before the exception unwinds. Includes payload-shape faults (`ArgumentException`, `InvalidOperationException`), `OperationCanceledException` from a cancelled `cancellationToken` (graceful shutdown traffic appears here), transport / IO failures, and any other unhandled exception out of the apply pipeline. |
| `parked-causal-buffer` | `LatticeReplicationMetrics.OutcomeParkedCausalBuffer` | The entry parked on the causal-apply buffer because its declared `VectorClock` was not yet dominated by the local vector clock. The original delivery did not advance the high-water-mark; the entry re-enters the apply pipeline through the buffer drain when its dependencies arrive. |
| `shadow-forward-dedup` | `LatticeReplicationMetrics.OutcomeShadowForwardDedup` | The entry was suppressed by the per-tree shadow-forward dedupe cache because a matching identity tuple (`(originClusterId, timestamp, key, op)`) was already applied since the last cache eviction. The duplicate arises when a structural rewrite - a shard split, or a shard consolidation (merge) - shadow-forwards a user write into a different shard, so both emits ride the WAL with identical identity tuples. |
| `rejected-not-replicated` | `LatticeReplicationMetrics.OutcomeRejectedNotReplicated` | The inbound entry was rejected by the receiver-side enrollment gate because its `TreeId` is not enrolled for replication on this receiver (the local per-tree resolver returns no merge mode for it). The entry is dropped without applying and without dead-lettering - a non-enrolled tree id is peer-controlled, so parking it would let a peer spawn unbounded dead-letter-queue activations. |
| `rejected-mode-mismatch` | `LatticeReplicationMetrics.OutcomeRejectedModeMismatch` | The inbound entry was rejected by the receiver-side merge-mode gate because its peer-supplied `Mode` disagrees with the merge mode the receiver resolves locally for the entry's `TreeId`. The entry is not applied; because the tree is enrolled (and therefore bounded) the entry is dead-lettered with the `mode_mismatch` reason rather than silently dropped. |
| `rejected-foreign-tenant` | `LatticeReplicationMetrics.OutcomeRejectedForeignTenant` | The receiver-side tenant-isolation gate (live only when tenancy is on) refused the entry because its `TreeId` names a tenant that does not exist on this receiver. The owning tenant is derived from the tree id alone, never from a wire field. The entry is dead-lettered with the `foreign_tenant` reason and the high-water-mark is left unchanged. The rejection is not a deferral, so the batch is still acknowledged and the sender advances past the entry rather than re-shipping it; replay it from the [dead-letter queue](dead-letter-queue.md) once the tenant exists. |
| `rejected-tenant-offline` | `LatticeReplicationMetrics.OutcomeRejectedTenantOffline` | The tenant-isolation gate refused the entry because its tenant exists but is not resident in the region serving this receiver. Dead-lettered with `tenant_offline` and the high-water-mark left unchanged; the batch is still acknowledged, so the sender does not re-ship the entry - replay it from the dead-letter queue once the tenant becomes resident here. |
| `rejected-tenant-suspended` | `LatticeReplicationMetrics.OutcomeRejectedSuspendedTenant` | The tenant-isolation gate refused the entry because its tenant exists but has been suspended or disabled by an operator. Dead-lettered with `tenant_suspended` and the high-water-mark left unchanged; the batch is still acknowledged, so the sender does not re-ship the entry - replay it from the dead-letter queue if the tenant is reinstated. |

A receiver with a single overwhelmed subscriber surfaces as a rising `failure` bucket; a receiver with persistent causal skew surfaces as a rising `parked-causal-buffer` bucket. Both are independent of `apply.lag`, which only samples successful merges.

## Parallel-apply degree (`apply.parallel_runs`)

`orleans.lattice.replication.apply.parallel_runs` records the effective degree of parallelism the receiver-side batch-apply path used for a single inbound batch - the number of independent `(treeId, originClusterId)` run-groups applied concurrently. One sample is recorded per multi-entry batch.

| Property | Value |
|---|---|
| Name | `orleans.lattice.replication.apply.parallel_runs` |
| Unit | `{run}` |
| Tags | _(none beyond `tenant`, fixed to `_platform_`)_ |

The histogram carries no `tree` tag: the measurement describes the batch as a whole, which may span multiple trees. A value of `1` denotes fully-sequential apply - either the default posture (`ApplyMaxParallelRuns = 1`) or a single-tree batch where cross-tree parallelism is moot. A value greater than `1` reports the achieved concurrency, which is the host-configured `LatticeReplicationOptions.ApplyMaxParallelRuns` clamped to the number of distinct trees present in the batch.

Operators use the distribution to confirm parallel apply is actually engaging under multi-tree load (the `p50` rising above `1` after raising `ApplyMaxParallelRuns`) and to correlate the achieved parallelism against `apply.lag` and `apply.duration`. Independence is enforced at the tree granularity: distinct trees apply concurrently, while runs that share a tree stay sequential so the per-tree causal-apply buffer, shadow-forward dedupe cache, per-origin FIFO, and per-origin high-water-mark monotonicity hold exactly as in the sequential path. See [the batch-apply section of replication-apply.md](replication-apply.md) for the full independence model.

## Ship-rate (`wal.entries_shipped`)

The producer no longer emits a commit-time append counter: a commit reaches the per-shard write-ahead log exactly once, via the leaf commit-log writer, and the per-`(tree, peer)` shipper tails that log in the background. Ship progress is therefore observed directly through the ship counter and correlated against WAL retention / GC.

| Counter | Tags | Recorded |
|---|---|---|
| `orleans.lattice.replication.wal.entries_shipped` | `tree`, `peer` | When a `Push` call on the gRPC transport returns an acknowledgement - including one with `Accepted = false` (a receive-fence deferral), so a deferred batch is counted again when it is re-shipped. Incremented by the count of entries in the shipped envelope; a heartbeat / keep-alive (zero-entry) batch contributes zero. |

Operators monitor `rate(wal_entries_shipped)` per tree-peer pair against the WAL's growth and trim signals (`wal.entries_trimmed`, plus the configured retention window). A ship rate that persistently lags the WAL's growth means the local log is accumulating faster than the sender can drain it: the shipper's unacknowledged cursor then holds back the min-acked-cursor WAL GC, and the growing backlog surfaces on the per-peer `peer.entries_behind` gauge that the [back-pressure health check](health-check.md) evaluates.

## Ship duration (`ship.duration`)

`orleans.lattice.replication.ship.duration` is recorded by the gRPC push transport around every `Push` unary call it issues - entry-carrying batches and empty liveness probes alike - in a `finally`, so a failed call still records a sample.

| Property | Value |
|---|---|
| Name | `orleans.lattice.replication.ship.duration` |
| Unit | `ms` |
| Tags | `tree`, `peer`, `outcome` |

`outcome` is `ok` when the call returned an ack and `error` when it threw; the values are string literals at the emission site rather than published constants. Because the sample is taken inside the transport, a host that ships through a custom `IReplicationTransport` does not record it, and neither does it record `wal.entries_shipped`.

## DLQ enqueue-reason classification

`orleans.lattice.replication.dead_letter.enqueued` is tagged with one of these reason values ([Dead-Letter Queue](dead-letter-queue.md) carries the operator-side detail):

| Value | When |
|---|---|
| `schema` | A terminal apply failure was an `ArgumentException` (malformed entry, missing field, range delete with no end key) or an `InvalidOperationException` (unrecognised `LatticeMergeMode`, state-merge CAS budget exhausted) - raised after the dead-letter-tracking decorator exhausted `MaxApplyRetries`, or by a drained causal-buffer entry. The sender also parks a batch it cannot encode as `schema`, and an entry with an empty tree id, which cannot be parked per tree, is dropped but still counted as `schema` with an empty `tree` tag. |
| `unknown` | Catch-all for every other terminal failure shape (e.g. transport / IO / `TimeoutException`). |
| `hlc_skew` | A blocked entry evicted from a full causal-apply buffer (`CausalBufferMaxEntries` / `CausalBufferMaxBytes`) to make room for a newer park. |
| `mode_mismatch` | The receiver-side merge-mode gate rejected an entry whose wire `Mode` disagrees with the merge mode the receiver resolves locally for the tree. |
| `foreign_tenant` / `tenant_offline` / `tenant_suspended` | The tenant-isolation gate refused the write: unknown tenant / tenant not resident in this region / tenant suspended or disabled (the matching `apply.duration` outcomes are `rejected-foreign-tenant`, `rejected-tenant-offline`, and `rejected-tenant-suspended` above). |
| `oversized` | Reserved. Nothing emits it today; it is published for host decorators that wrap the canonical applier with a per-entry size check. |

`orleans.lattice.replication.dead_letter.removed` is tagged `discarded` (explicit operator discard), `replayed` (removed after a successful replay), or `evicted` (FIFO capacity eviction during a later enqueue).

The failure-to-reason mapping is intentionally conservative: only failure shapes whose source is under the package's control are matched explicitly, so the `reason` dimension stays stable across publishers and operators can alert on `unknown` rising without false positives from future schema-shape additions.

## Sender-side pipelining depth (`peer.ship_in_flight`)

`orleans.lattice.replication.peer.ship_in_flight` (`LatticeReplicationMetrics.ShipInFlightName`) reports the number of outbound replication batches the local sender currently has shipped-but-unacknowledged to the named peer - the live depth of the sender-side pipelining window bounded by `LatticeReplicationOptions.ShipMaxInFlight` (see [Sender-side pipelining](receiver-flow-control.md#sender-side-pipelining)).

| Property | Value |
|---|---|
| Name | `orleans.lattice.replication.peer.ship_in_flight` |
| Unit | `{batch}` |
| Tags | `tree`, `peer` |

Outbound-only (the receiver does not pipeline into itself), so the gauge emits a single series per `(tree, peer)` pair without the `direction` tag, matching `entries_behind` and `bytes_behind`. The shipper records the depth through `ReplicationPeerStats.RecordInFlight(tree, peer, depth)` each time the window grows (a batch is launched) or shrinks (a batch is acknowledged, or the window is drained / collapsed), and the depth is also visible on the `ReplicationPeerSnapshot.InFlight` snapshot field.

Operators read the gauge against the configured window: a value at or near `ShipMaxInFlight` signals the sender is keeping the pipeline saturated (the link is the bottleneck, as intended); a value pinned at `0` on a peer that is also reporting nonzero `entries_behind` signals the window collapsed under receiver flow-control back-pressure (a `SuggestedBatchSize` hint forced it back to a single serial batch). On a serial (default `ShipMaxInFlight = 1`) sender at rest the gauge sits at `0` between ticks.

## Content-hash payload re-send rate (`ship.redundant_payloads` / `ship.redundant_payload_bytes`)

These counters fire by default: `LatticeReplicationOptions.ContentHashDedupEnabled` defaults to `true`, so a stock build records the payload re-send rate out of the box. Setting `ContentHashDedupEnabled = false` opts out - the shipper then does no extra work and never records them. They measure how often the sender ships a `Set` whose value bytes are byte-identical to the value most recently shipped for the same key - the idempotent-re-write rate that decides whether a sender-manifest / receiver-pull-missing dedup round trip would pay for its extra latency.

| Counter | Constant | Unit | Tags | Recorded |
|---|---|---|---|---|
| `orleans.lattice.replication.ship.redundant_payloads` | `LatticeReplicationMetrics.ShipRedundantPayloadsName` | `{entry}` | `tree`, `peer` | Once per shipped `Set` whose value hashes equal to the last value shipped for that key. |
| `orleans.lattice.replication.ship.redundant_payload_bytes` | `LatticeReplicationMetrics.ShipRedundantPayloadBytesName` | `By` | `tree`, `peer` | The summed value-byte length of the entries counted above. |

The shipper keeps a per-activation, per-key bounded LRU of the last-shipped content hash (FNV-1a 64-bit over the op, key, range end-key, and value bytes), sized by `LatticeReplicationOptions.ContentHashDedupCacheSize` (default `4096`, validated `>= 64`). Read the redundant fraction as `rate(ship_redundant_payloads) / rate(wal_entries_shipped)` per `(tree, peer)`: a high ratio signals idempotent upstream retry logic re-sending the same value, which is exactly the signal that justifies opting into a dedup round trip. `ship.redundant_payload_bytes` quantifies the bandwidth that round trip could reclaim, not just the entry count.

The measurement is **observability-only**: it never elides, reorders, or alters the bytes the sender ships, so the wire output is unchanged whether or not dedup measurement is enabled. (Actually skipping a byte-identical re-set carrying a newer HLC would strand the receiver's per-origin high-water mark and change LWW/HLC convergence; the receiver must consent through a content-manifest exchange, which is the separate opt-in `ContentHashDedupElisionEnabled` - see [Content-manifest payload elision](#content-manifest-payload-elision).) Because the counters fire as entries are framed onto the wire, a batch re-shipped after a transient transport failure counts its entries again - which is correct, since a re-ship is itself a redundant wire payload.

## Content-manifest payload elision

These counters are **opt-in**: they fire only when `LatticeReplicationOptions.ContentHashDedupElisionEnabled` is set (which requires `ContentHashDedupEnabled`) and the peer implements the content-manifest exchange. Before shipping a drained batch, the sender advertises a per-entry content-hash manifest, the receiver answers with the entries it does not already hold, and only those payloads ship. An identical-content entry carrying a newer HLC advances the receiver's per-origin high-water-mark through a metadata-only update during the exchange.

| Counter | Constant | Unit | Tags | Recorded |
|---|---|---|---|---|
| `orleans.lattice.replication.ship.manifest_exchanges` | `LatticeReplicationMetrics.ManifestExchangesName` | `{exchange}` | `tree`, `peer` | Sender side, once per completed exchange with a peer that supports it. A failed exchange ships the full batch and records nothing. |
| `orleans.lattice.replication.ship.elided_payloads` | `LatticeReplicationMetrics.ShipElidedPayloadsName` | `{entry}` | `tree`, `peer` | Sender side, the number of entries the exchange removed from the outbound batch. |
| `orleans.lattice.replication.ship.elided_payload_bytes` | `LatticeReplicationMetrics.ShipElidedPayloadBytesName` | `By` | `tree`, `peer` | Sender side, the summed pre-encoded wire-segment length of the entries counted above. |
| `orleans.lattice.replication.receiver.content_manifest_exchanges` | `LatticeReplicationMetrics.ReceiverContentManifestExchangesName` | `{exchange}` | `tree`, `peer` | Receiver side, once per manifest exchange the gRPC endpoint serves. `peer` is the requesting origin cluster. |
| `orleans.lattice.replication.receiver.content_entries_elided` | `LatticeReplicationMetrics.ReceiverContentEntriesElidedName` | `{entry}` | `tree`, `peer` | Receiver side, the number of manifest entries it already held and therefore did not request. |
| `orleans.lattice.replication.receiver.content_hwm_advances` | `LatticeReplicationMetrics.ReceiverContentHwmAdvancesName` | `{advance}` | `tree`, `peer` | Receiver side, once per exchange whose metadata-only update actually advanced the per-origin high-water-mark. |

A peer whose exchange seam reports it cannot perform the exchange makes the sender fall back to shipping full batches for the rest of the activation, so a mixed fleet shows these counters only on the links where both ends support elision.

## Sender-side adaptive batch sizing (`ship.effective_batch_size` / `ship.ack_latency`)

These histograms instrument the sender-side AIMD batch-size controller behind `LatticeReplicationOptions.AdaptiveBatchSizingEnabled` (see [Sender-side adaptive batch sizing](receiver-flow-control.md#sender-side-adaptive-batch-sizing)). **Both emit once per acknowledged batch regardless of the flag** - they are pure observability and are useful even with static sizing, where `ship.effective_batch_size` collapses onto the configured `ShipBatchSize` (modulated only by any active receiver hint).

| Property | `ship.effective_batch_size` | `ship.ack_latency` |
|---|---|---|
| Name | `orleans.lattice.replication.ship.effective_batch_size` | `orleans.lattice.replication.ship.ack_latency` |
| Constant | `LatticeReplicationMetrics.ShipEffectiveBatchSizeName` | `LatticeReplicationMetrics.ShipAckLatencyName` |
| Unit | `{entry}` | `ms` |
| Tags | `tree`, `peer` | `tree`, `peer` |

`ship.effective_batch_size` records the entry cap the sender actually applied for the batch - the result of `min(adaptive size, receiver-suggested size, ShipBatchSize)` floored at `1`. With adaptive sizing off the distribution tracks the static cap; with it on the distribution tracks the controller's AIMD output as it grows on fast acks and backs off on rising latency or errors.

`ship.ack_latency` records the wall-clock interval between the sender launching a batch's `IReplicationTransport.SendAsync` and that batch's ack returning, measured with `Stopwatch.GetElapsedTime(long)` (allocation-free, monotonic). On the bounded-pipelining path the interval includes the time the batch spent queued behind lower-HLC batches in the FIFO window, so it reflects the effective per-batch round-trip the sender observes - which is exactly the signal the controller's sliding window averages against `AdaptiveBatchLatencyThreshold`. Neither histogram samples liveness probes.

Operators correlate the two: a rising `ship.ack_latency` p50 followed by a falling `ship.effective_batch_size` is the controller backing off ahead of the receiver's WAL-saturation hint; a flat `ship.effective_batch_size` pinned at `ShipBatchSize` with low `ship.ack_latency` is a healthy link running at the configured ceiling.

## Pre-ship coalescing (`coalesce.entries_elided` / `coalesce.bytes_elided` / `coalesce.deltas_merged`)

These counters fire by default: `LatticeReplicationOptions.PreShipCoalescingEnabled` defaults to `true`, so a stock build records the coalescing win out of the box. Setting `PreShipCoalescingEnabled = false` opts out - the shipper then does no extra work and never records them. They measure how many redundant per-key versions the sender dropped from a drained batch before it crossed the wire - the win pre-ship coalescing reclaims on a hot key rewritten several times within one ship window (see [Pre-ship coalescing](replication-drivers.md#pre-ship-coalescing)). Distinct from the content-hash counters above, which only measure and never alter the bytes shipped: these record entries that were actually elided.

| Counter | Constant | Unit | Tags | Recorded |
|---|---|---|---|---|
| `orleans.lattice.replication.coalesce.entries_elided` | `LatticeReplicationMetrics.CoalesceEntriesElidedName` | `{entry}` | `tree`, `peer` | Once per WAL entry dropped from an outbound batch by the coalescing pass. |
| `orleans.lattice.replication.coalesce.bytes_elided` | `LatticeReplicationMetrics.CoalesceBytesElidedName` | `By` | `tree`, `peer` | The summed pre-encoded wire-segment length of the entries counted above. |
| `orleans.lattice.replication.coalesce.deltas_merged` | `LatticeReplicationMetrics.CoalesceDeltasMergedName` | `{delta}` | `tree`, `peer` | On a CRDT tree, once per source delta folded into a combined delta (the CRDT-specific dimension; the `entries_elided` / `bytes_elided` counters still record the source entries dropped on this path too). |

Coalescing runs on both last-writer-wins and recognised CRDT trees, but by different mechanics. On a `LwwRegister` tree the pass keeps only the latest same-key version and drops the earlier ones outright. On a recognised CRDT tree, dropping earlier versions would lose each entry's delta contribution, so the pass instead **folds** the same-key deltas into one combined delta - a join over the primitive's semilattice whose receiver-side apply effect is identical to applying the source deltas in sequence - re-encodes it onto the kept entry, and elides the rest. A registered `OrMap` tree folds by unioning the dot-tagged adds and tombstones and lattice-merging any same-dot value snapshots through the value CRDT's own join, so it coalesces like the closed shapes. `coalesce.deltas_merged` counts the source deltas folded on this CRDT path; `entries_elided` / `bytes_elided` count the source entries it dropped, exactly as on the LWW path. An `OrMap` tree whose `(TKey, TValue)` shape is unregistered (no shape descriptor resolves) and any CRDT entry carrying no typed delta fall back to shipping individually (loss-free). Only plain point `Set` / `Delete` writes with a real (non-`Zero`) HLC that are neither prepared nor part of an atomic batch are eligible; range deletes, saga terminal marks, and zero-HLC entries are never coalesced. Read the elided fraction as `rate(coalesce_entries_elided) / rate(wal_entries_shipped)` per `(tree, peer)`: a high ratio signals a hot rewrite pattern that coalescing is collapsing, and `coalesce.bytes_elided` quantifies the cross-cluster bandwidth reclaimed. The coalesced output converges identically on an unmodified receiver - a strict subset on LWW trees, an effect-equivalent merge on CRDT trees.

## Doorbell coalescing (`doorbell.rung` / `doorbell.coalesced`)

Separate from the *pre-ship* coalescing above (which reduces the bytes a batch carries), these counters measure how the commit-time nudge that *wakes* the shipper is coalesced at the source. A doorbell is an idempotent, edge-triggered "there is work" signal, so the commit-time doorbell sink collapses a burst of per-commit ring requests for the same `(tree, peer)` into at most one in-flight ring plus one pending follow-up, rather than dispatching one `OnDoorbellAsync` grain call per commit onto the non-reentrant shipper activation (see [Writer-side coalescing](replication-drivers.md#writer-side-coalescing)). Both counters fire whenever `LatticeReplicationOptions.ShipDoorbellEnabled` is `true` (the default).

| Counter | Constant | Unit | Tags | Recorded |
|---|---|---|---|---|
| `orleans.lattice.replication.doorbell.rung` | `LatticeReplicationMetrics.DoorbellRungName` | `{ring}` | `tree`, `peer` | Once per doorbell ring actually dispatched to the shipper (the in-flight ring plus any trailing follow-up). |
| `orleans.lattice.replication.doorbell.coalesced` | `LatticeReplicationMetrics.DoorbellCoalescedName` | `{ring}` | `tree`, `peer` | Once per ring request elided because a ring for the same `(tree, peer)` was already in flight (folded into the single pending follow-up). |

Read the coalescing win as `rate(doorbell_coalesced) / (rate(doorbell_coalesced) + rate(doorbell_rung))` per `(tree, peer)`: under a sustained write burst this ratio approaches 1 (nearly every per-commit request is absorbed) while `doorbell.rung` stays near the small constant the shipper actually needs to stay awake. A `doorbell.rung` rate that tracks the raw commit rate with a near-zero `doorbell.coalesced` rate signals writes arriving slower than the shipper drains them - no storm to absorb. Because the base phase-timer and keepalive-reminder still drive shipping independently of doorbells, a coalesced (elided) ring never delays delivery beyond one timer tick.

## Shared-dictionary compression ratio (`compress.dictionary.bytes_in` / `compress.dictionary.bytes_out`)

These counters are **opt-in** and fire only when a batch is actually framed with shared-dictionary compression - either statically (`LatticeReplicationOptions.FramingCompression = LatticeCompression.ZstdDictionary` with a non-zero `FramingCompressionDictionaryId`) or through the auto-shared dictionary (`AutoSharedDictionaryEnabled`) once a trained dictionary is active - and the requested dictionary resolves on the sending silo; the default build never records them. They quantify the before/after win of compressing the batch tail against a shared Zstandard dictionary (see [Shared-dictionary Zstandard compression](../lattice/compression.md#shared-dictionary-zstandard-compression)).

| Counter | Constant | Unit | Tags | Recorded |
|---|---|---|---|---|
| `orleans.lattice.replication.compress.dictionary.bytes_in` | `LatticeReplicationMetrics.CompressDictionaryBytesInName` | `By` | `tree` | The uncompressed tail length each time a batch is framed with the `ZstdDictionary` tag (the "before"). |
| `orleans.lattice.replication.compress.dictionary.bytes_out` | `LatticeReplicationMetrics.CompressDictionaryBytesOutName` | `By` | `tree` | The compressed tail length emitted for that same batch (the "after"). |

Read the achieved ratio as `rate(compress_dictionary_bytes_out) / rate(compress_dictionary_bytes_in)` per `tree`: a value well below `1.0` is the dictionary saving (lower is better; `1.0` means no saving). Compare it against the dictionary-less `Zstd` baseline on the same workload to decide whether a given dictionary id is worth shipping. The counters are emitted on the framing encode path only, so a frame that gracefully degrades to plain `Zstd` (because the dictionary could not be resolved locally) does not contribute - which keeps the ratio honest about the dictionary path specifically. The `peer` tag is not available at the encode seam, so these counters are tagged by `tree` only.

## Shared-dictionary convergence (`ship.dictionary_convergence`)

This counter is **opt-in** and fires only when the auto-distributing shared dictionary is enabled: it is incremented once per shared-dictionary pull attempt the shipper makes against a peer-advertised dictionary id it does not yet hold, so an operator can watch how a fleet converges onto a shared trained dictionary and spot fingerprint rejections.

| Counter | Constant | Unit | Tags | Recorded |
|---|---|---|---|---|
| `orleans.lattice.replication.ship.dictionary_convergence` | `LatticeReplicationMetrics.DictionaryConvergenceName` | `{pull}` | `tree`, `peer`, `outcome` | Once per shared-dictionary pull attempt against a peer-advertised id the shipper does not yet hold. `outcome` is one of `installed` (the peer served bytes whose fingerprint matched the advertised fingerprint and they were installed locally), `rejected` (the served bytes' fingerprint did not match, or a local id collision rejected the install, so they were discarded), or `unavailable` (the peer or transport did not serve the pull - an un-upgraded peer, a momentarily unreachable hop, or the peer no longer holds the id - so the shipper leaves it uninstalled and retries on a later tick). |

Read the convergence health as `rate(ship_dictionary_convergence{outcome="installed"}) / rate(ship_dictionary_convergence)` per `(tree, peer)`: a ratio climbing toward `1` as a newly trained dictionary propagates confirms the fleet is converging, while a sustained `rejected` fraction flags a fingerprint mismatch worth investigating and a sustained `unavailable` fraction flags peers that have not yet upgraded or cannot serve the id.

> Note: the dashboards package (`docs/lattice.dashboards`) owns the metrics-to-panel map; a corresponding panel + map row for this counter is maintained there, not in this package.

## Subscribing

Wire `LatticeReplicationMetrics.MeterName` into an OpenTelemetry `MeterProviderBuilder.AddMeter(...)` call, or attach a `MeterListener` directly:

```csharp verify
using System.Diagnostics.Metrics;

using var listener = new MeterListener
{
    InstrumentPublished = (instrument, l) =>
    {
        if (instrument.Meter.Name == LatticeReplicationMetrics.MeterName)
        {
            l.EnableMeasurementEvents(instrument);
        }
    },
};
listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => { /* ... */ });
listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => { /* ... */ });
listener.Start();
```

## Causal+ instruments

The following instruments surface the receiver-side causal-apply buffer used by the causal-plus dependency check. They share the meter and tag conventions of the rest of the package - `tree` always identifies the logical tree, and `shard` is reserved as a second tag dimension on the buffer-state instruments so a future per-shard buffer partitioning can populate it without a wire-format break. The current implementation is one-buffer-per-tree, so `shard` is always `"0"`.

| Instrument | Kind | Tags | Recorded when |
|---|---|---|---|
| `orleans.lattice.replication.apply.buffered_entries` | `UpDownCounter<long>` | `tree`, `shard` | Increments by 1 on every successful park (including overflow-evicting parks); decrements by 1 per evicted entry inside the same park; decrements by the count of drained entries on each successful drain pass. |
| `orleans.lattice.replication.apply.buffer_bytes` | `UpDownCounter<long>` | `tree`, `shard` | Tracks the same lifecycle as `buffered_entries` but in cumulative serialised bytes (key length × 2 + end-key length × 2 + value length + 128 envelope overhead). |
| `orleans.lattice.replication.apply.dependency_wait` | `Histogram<double>` (ms) | `tree` | One sample per drained entry: `now - parked_at`, clamped non-negative. Evicted entries do not contribute - only successful waits are observed. |
| `orleans.lattice.replication.apply.causal_violations_blocked` | `Counter<long>` | `tree` | Incremented once per successful park. Duplicate-tuple parks do not count. An alert on `rate > 0` flags causal-skew health regardless of whether buffered entries eventually drain or evict. |

Operators monitor them together:

- A steady-state replicating peer keeps `buffered_entries` near zero and emits `dependency_wait` samples close to the round-trip-time of a single ack cycle.
- A persistent rise in `buffered_entries` or `buffer_bytes` paired with a low or zero `causal_violations_blocked` rate is the classic "bounded buffer absorbing transient skew, then draining" pattern - healthy.
- A sustained nonzero `causal_violations_blocked` rate paired with `apply.dependency_wait` distributions in the seconds-to-minutes range indicates structural causal skew the bounded buffer is masking; pair with the DLQ enqueue rate (`dead_letter.enqueued{reason="hlc_skew"}`) to detect overflow.
- A sudden buffer drain that does not advance the local high-water-mark (visible as a spike in `dependency_wait` with no matching `apply.lag` improvement) suggests a CRDT-merge regression rather than a transport-side issue.

## Per-origin FIFO invariant (`apply.fifo_violations`)

`apply.fifo_violations` counts successful applies whose source HLC is lower than the highest source HLC already applied for the same `(tree, origin)`. It is an ordering diagnostic, not a correctness alarm. Source HLCs are stamped per leaf, each leaf ticking its own clock, and the write-ahead log partitions by key hash, so one origin's HLC stream is not monotonic in delivery order: the shipper merges each drain by HLC, but a write committed on a lagging leaf can reach the log after a higher-HLC write has already shipped, and it then arrives below the running maximum as a genuine new write. That is why the receiver drops point writes only at the snapshot-pinned causal floor rather than at the per-origin high-water-mark (see [the pinned causal floor](replication-apply.md#2-snapshot-pinned-causal-floor-and-per-origin-high-water-mark)).

| Property | Value |
|---|---|
| Name | `orleans.lattice.replication.apply.fifo_violations` |
| Unit | `{entry}` |
| Tags | `tree`, `origin` |

The canonical applier records the most recently applied source HLC per `(treeId, originClusterId)` in process-local memory and increments `apply.fifo_violations` when a successfully applied entry's HLC is **strictly less** than the prior recorded value for the same pair. The counter is recorded:

- **After a successful apply** (direct or drained from the causal-apply buffer) - never on park. The invariant tracks "what has been merged" rather than "what has been observed", so a transient park of a higher-HLC entry that drains after a lower-HLC arrival does not falsely register a violation.
- **For point operations only** (`Set` / `Delete`). `DeleteRange` carries `HybridLogicalClock.Zero` by design and is excluded - it neither records a violation nor overwrites the recorded HLC.

A violation **does not change apply behaviour**: the entry is still applied, and the high-water-mark advance is a monotonic no-op for it because the running maximum is already higher. Read the counter as a rate against `wal.entries_shipped` rather than alerting on `rate > 0`: a steady low rate reflects ordinary per-leaf interleaving, while a step change on one `(tree, origin)` pair points at a sender or transport path that has started reordering deliveries. Operators triage by joining the `tree` and `origin` tags against the producer-side topology.

Genuine causal dependencies are not enforced through this counter: an entry that depends on another origin's write carries the dependency in its `VectorClock` and routes through the causal-apply buffer's dependency check instead. Bootstrap-drain entries are not recorded, because a snapshot export visits shards and leaves in arbitrary order. The tracker holds one value per `(tree, origin)` in process-local memory, so a silo restart resets it.

## Fall-off-the-log detection (`peer.fell_off_log` / `peer.fell_off_log_suppressed`)

The fall-off detector compares a peer's per-origin high-water-mark with an oldest-available HLC for that peer and treats a high-water-mark strictly below it as a gap incremental replication cannot bridge. The per-tree maintenance grain supplies that HLC from the local write-ahead log - the oldest retained entry the peer authored - once per `MaintenanceFallOffCheckInterval` (see [Replication Drivers](replication-drivers.md#independent-cadences)).

| Counter | Constant | Unit | Tags | Recorded |
|---|---|---|---|---|
| `orleans.lattice.replication.peer.fell_off_log` | `LatticeReplicationMetrics.PeerFellOffLogName` | `{event}` | `tree`, `origin` | Once per fresh fall-off detection. Emitted whether or not `AutoBootstrapOnFallOffLog` then starts a bootstrap, so disabling auto-bootstrap does not silence the alert. |
| `orleans.lattice.replication.peer.fell_off_log_suppressed` | `LatticeReplicationMetrics.PeerFellOffLogSuppressedName` | `{event}` | `tree`, `origin` | Once per detection absorbed because a bootstrap from the same source cluster is already in flight (requesting, applying, or handing off), so repeated probes during a long drain do not inflate the fresh-detection count. |

`origin` is the source cluster the detector probed, the same key the bootstrap instruments below use.

## Bootstrap instruments

The receiver-side bootstrap coordinator emits the following instruments tracking the cross-cluster snapshot-drain pipeline plus a structured phase-transition log line. Together they let an operator dashboard the lifecycle of an in-flight bootstrap and tail a single run end-to-end through the silo log.

| Instrument | Kind | Tags | Recorded when |
|---|---|---|---|
| `orleans.lattice.replication.bootstrap.entries_received` | `Counter<long>` | `tree`, `origin` | Incremented by 1 per snapshot entry handed to the local replication applier (post-decorator chain) once its apply call returns without throwing - whether or not the entry was newly merged. Committed rows with no value and prepared rows with no transaction id are skipped and not counted. |
| `orleans.lattice.replication.bootstrap.bytes_received` | `Counter<long>` (`By`) | `tree`, `origin` | Incremented by `entry.Value.Length` per applied entry. Mirrors the lifecycle of `entries_received`. |
| `orleans.lattice.replication.bootstrap.duration` | `Histogram<double>` (`ms`) | `tree`, `origin`, `outcome` | Recorded once per terminal phase transition. `outcome` is `live` or `failed`; `timed_out` is published as a constant but not emitted today (see below). |
| `orleans.lattice.replication.bootstrap.transient_retries` | `Counter<long>` | `tree`, `origin` | Incremented by 1 each time the bootstrap drain catches a classified-transient transport fault and consumes one slot of the configured `LatticeReplicationOptions.BootstrapTransientRetry` budget. A bootstrap that completes on its first drain attempt records zero on this counter; a bootstrap that exhausts the budget and pivots to `Failed` records `MaxAttempts - 1` (one per consumed retry slot). |

The `origin` tag carries the source cluster id supplied at kickoff (`BootstrapAsync(treeName, sourceClusterId, ...)`), matching the tag dimensionality used by the per-origin fall-off-the-log counters so dashboards can join the two without a separate keying.

The histogram's `outcome` values are exposed as `LatticeReplicationMetrics.BootstrapOutcomeLive`, `BootstrapOutcomeFailed`, and `BootstrapOutcomeTimedOut` constants. The `timed_out` value is reserved for a future transport-timeout policy; the in-tree coordinator emits only `live` and `failed` today, but the constant is published so dashboard rules referencing it remain valid across future releases.

The duration timer is anchored on a per-activation in-memory stopwatch captured at kickoff (or lazy-initialised on the first drain pass after a silo failover). It records `Stopwatch.GetElapsedTime` from that anchor to the terminal transition; a silo failover between kickoff and completion therefore truncates the measured interval to the span since the most recent reactivation. Operators monitoring cross-failover total durations should pair the histogram with the per-entry counters, which are restartable across reactivations.

Phase-transition structured logs are emitted at `LogLevel.Information` from the bootstrap coordinator. Every transition log has the same shape and carries the `TreeName`, `SourceClusterId`, and `LastAppliedHlc` properties; the two phase names are literal text in the message, except that the failure transition carries the phase it failed from as a `PreviousPhase` property:

```
Bootstrap phase transition for tree '{TreeName}' from source '{SourceClusterId}': <previous> -> <next> (LastAppliedHlc={LastAppliedHlc})
Bootstrap phase transition for tree '{TreeName}' from source '{SourceClusterId}': {PreviousPhase} -> Failed (LastAppliedHlc={LastAppliedHlc})
```

covering the five transitions:

- `Idle -> RequestingSnapshot` (kickoff persist).
- `RequestingSnapshot -> ApplyingSnapshot` (snapshot stream opened, pivot persisted). Suppressed on crash-resume when the persisted phase is already `ApplyingSnapshot`.
- `ApplyingSnapshot -> IncrementalHandoff` (end-of-stream, cursor persisted).
- `IncrementalHandoff -> LiveIncremental` (HWM pinned, coordinator torn down).
- `{previous} -> Failed` (catch-and-persist path; the previous phase is included so an operator can see where the drain aborted).

Tailing the silo log for a single bootstrap run is `(treeName, sourceClusterId)` keyed: every transition log carries both. Pair with the metric tags above to correlate log-line timestamps against per-entry throughput.

## Bidirectional `peer.last_contact_seconds` and the liveness probe

`peer.last_contact_seconds` and `peer.consecutive_errors` carry a `direction` tag with two values:

- `direction="outbound"` - recorded by the per-peer shipper after a peer accepts a shipped batch. Includes the periodic empty **liveness probe** the shipper fires when the drain buffer is empty and the wall-clock interval since the last successful outbound contact has elapsed. The probe is configured by `LatticeReplicationOptions.LivenessProbeInterval` (default `30 s`; set to `Timeout.InfiniteTimeSpan` to disable). The probe interval timer is anchored on the first idle pump tick after activation, so the first idle tick is silent and the probe begins one interval after activation. The payload is the fixed 32-byte framing header plus the length-prefixed tree name and origin cluster id; no entries are shipped.
- `direction="inbound"` - recorded by the canonical applier's batch path after a per-origin run of inbound entries applies (or fails) on the local receiver. Keyed by the entries' `WalRecord.OriginClusterId`. Entries with no origin or tree id, and local-origin entries, skip the recording.

The two directions are independent: a peer that this silo only ships to never produces an inbound row; a peer that this silo only receives from never produces an outbound row. `Snapshot()` returns one row per `(tree, peer, direction)` triple, each carrying a `Direction` property of type `ReplicationContactDirection`.

**Migration note.** Dashboards that previously matched `peer.last_contact_seconds` or `peer.consecutive_errors` without filtering on `direction` will see two series per `(tree, peer)` pair on hosts that opt into both directions. Add `direction="outbound"` to the matcher to preserve the pre-bidirectional shape, or accept the doubled series. Metric names and units are otherwise unchanged. `peer.entries_behind` and `peer.bytes_behind` remain outbound-only and emit a single series per pair without the `direction` tag.

## Coordinated-restore saga

The [coordinated multi-cluster restore](coordinated-restore.md) saga emits its
own instruments on the same `orleans.lattice.replication` meter, so an
OpenTelemetry pipeline already subscribed to replication receives them without
additional wiring. All durations are milliseconds as `double`.

| Instrument | Kind | Tags | Recorded when |
|---|---|---|---|
| `orleans.lattice.replication.saga.phase.duration` | `Histogram<double>` (`ms`) | `phase` | Recorded by the cross-cluster coordinator after each phase transition. `phase` is `prepare` (the fan-out prepare / vote-collection window), `commit` (the commit fan-out), or `abort` (the compensation fan-out). Separates the long unfenced build window from the short cutover. |
| `orleans.lattice.replication.saga.fence.duration` | `Histogram<double>` (`ms`) | `tree` | Recorded by the durable write-fence grain when the write fence is lifted (on the local cutover flip or on the self-lifting deadline), once per fenced tree. Measures only the write-blocking cutover window (engage to lift), NOT the longer globally-gated shipping pause, so operators can confirm the fence stays bounded to the cutover. |
| `orleans.lattice.replication.saga.participant.votes` | `Counter<long>` (`{vote}`) | `reason` | Incremented once per participant prepare with the vote outcome. `reason` is `commit`, or one of the abort reasons `infeasible` (admission refused a target that cannot fit), `precondition` (a missing backup or base in the manifest chain), `build-failed` (the bounded build-retry budget was exhausted), `engine-unavailable` (the backup package is not wired on this cluster), or `not-replicated` (the saga named a target tree this cluster does not replicate, so the participant refused it without building anything). Lets operators watch the commit-vote fraction and the distribution of abort refusals. A rising `not-replicated` count is a security signal rather than an operational one: the target tree arrives on a channel that authorizes the origin cluster and not the tree, so it means a peer is asking for a tree outside its enrollment. |
| `orleans.lattice.replication.saga.participant.commits` | `Counter<long>` (`{commit}`) | `reason` | Incremented once per committed participant. `reason` is `single` (single-tree restore), `set` (backup-set group-atomic restore), or `not-replicated` (a commit naming a target tree this cluster does not replicate, refused before the alias swap). |
| `orleans.lattice.replication.saga.participant.aborts` | `Counter<long>` (`{abort}`) | `reason` | Incremented once per aborted participant. `reason` is `single`, `set`, `engine-unavailable`, or `not-replicated`. |
| `orleans.lattice.replication.saga.compensations` | `Counter<long>` (`{compensation}`) | `cause` | Incremented once per participant grain that rolls back a prepared saga. `cause` is `vote-abort` (a coordinator-driven rollback after at least one participant voted abort) or `coordinator-loss` (a participant's own cutover-fence expiry auto-compensation after the coordinator decision never arrived). |

Every instrument name, tag key, and tag value is exposed as a `const` on
`LatticeReplicationMetrics` (for example `SagaPhaseDurationName`, `TagPhase`,
`SagaPhasePrepare`, `TagCause`, `SagaCauseCoordinatorLoss`), so dashboard rules
and external subscribers reference the constants rather than hard-coding the
strings. The `saga.phase.duration` histogram uses a monotonic stopwatch anchored
at each phase entry; a silo failover mid-phase truncates the measured interval to
the span since the most recent reactivation, matching the bootstrap-duration
behaviour described above.

## Instrument index

Every instrument on the `orleans.lattice.replication` meter. Kind and unit come from the instrument declarations in `LatticeReplicationMetrics` and `ReplicationPeerStats`; Tags lists the replication-specific dimensions each emission site attaches (the derived `tenant` tag is implicit - see the note at the top of this page).

| Instrument | Kind | Unit | Tags | Details |
|---|---|---|---|---|
| `orleans.lattice.replication.peer.entries_behind` | `ObservableGauge<long>` | `{entry}` | `tree`, `peer` | [Health check](health-check.md#related-metrics) |
| `orleans.lattice.replication.peer.bytes_behind` | `ObservableGauge<long>` | `By` | `tree`, `peer` | [Bidirectional contact gauges](#bidirectional-peerlast_contact_seconds-and-the-liveness-probe) |
| `orleans.lattice.replication.peer.ship_in_flight` | `ObservableGauge<long>` | `{batch}` | `tree`, `peer` | [Pipelining depth](#sender-side-pipelining-depth-peership_in_flight) |
| `orleans.lattice.replication.peer.consecutive_errors` | `ObservableGauge<long>` | `{error}` | `tree`, `peer`, `direction` | [Bidirectional contact gauges](#bidirectional-peerlast_contact_seconds-and-the-liveness-probe) |
| `orleans.lattice.replication.peer.last_contact_seconds` | `ObservableGauge<double>` | `s` | `tree`, `peer`, `direction` | [Bidirectional contact gauges](#bidirectional-peerlast_contact_seconds-and-the-liveness-probe) |
| `orleans.lattice.replication.wire_version.negotiated` | `ObservableGauge<long>` | `{version}` | `tree`, `peer` (no `tenant`) | [Wire-version negotiation](wire-format.md#wire-version-capability-negotiation) |
| `orleans.lattice.replication.wire_version.downgrade_active` | `ObservableGauge<long>` | `{bool}` | `tree`, `peer` (no `tenant`) | [Wire-version negotiation](wire-format.md#wire-version-capability-negotiation) |
| `orleans.lattice.replication.digest_remediation.disabled` | `ObservableGauge<long>` | `{state}` | `tree`, `peer`, `reason` | [Remediation guards](anti-entropy-remediation-guards.md#observability) |
| `orleans.lattice.replication.ship.duration` | `Histogram<double>` | `ms` | `tree`, `peer`, `outcome` | [Ship duration](#ship-duration-shipduration) |
| `orleans.lattice.replication.wal.entries_shipped` | `Counter<long>` | `{entry}` | `tree`, `peer` | [Ship-rate](#ship-rate-walentries_shipped) |
| `orleans.lattice.replication.ship.effective_batch_size` | `Histogram<int>` | `{entry}` | `tree`, `peer` | [Adaptive batch sizing](#sender-side-adaptive-batch-sizing-shipeffective_batch_size--shipack_latency) |
| `orleans.lattice.replication.ship.ack_latency` | `Histogram<double>` | `ms` | `tree`, `peer` | [Adaptive batch sizing](#sender-side-adaptive-batch-sizing-shipeffective_batch_size--shipack_latency) |
| `orleans.lattice.replication.ship.redundant_payloads` | `Counter<long>` | `{entry}` | `tree`, `peer` | [Re-send rate](#content-hash-payload-re-send-rate-shipredundant_payloads--shipredundant_payload_bytes) |
| `orleans.lattice.replication.ship.redundant_payload_bytes` | `Counter<long>` | `By` | `tree`, `peer` | [Re-send rate](#content-hash-payload-re-send-rate-shipredundant_payloads--shipredundant_payload_bytes) |
| `orleans.lattice.replication.ship.manifest_exchanges` | `Counter<long>` | `{exchange}` | `tree`, `peer` | [Content-manifest elision](#content-manifest-payload-elision) |
| `orleans.lattice.replication.ship.elided_payloads` | `Counter<long>` | `{entry}` | `tree`, `peer` | [Content-manifest elision](#content-manifest-payload-elision) |
| `orleans.lattice.replication.ship.elided_payload_bytes` | `Counter<long>` | `By` | `tree`, `peer` | [Content-manifest elision](#content-manifest-payload-elision) |
| `orleans.lattice.replication.receiver.content_manifest_exchanges` | `Counter<long>` | `{exchange}` | `tree`, `peer` | [Content-manifest elision](#content-manifest-payload-elision) |
| `orleans.lattice.replication.receiver.content_entries_elided` | `Counter<long>` | `{entry}` | `tree`, `peer` | [Content-manifest elision](#content-manifest-payload-elision) |
| `orleans.lattice.replication.receiver.content_hwm_advances` | `Counter<long>` | `{advance}` | `tree`, `peer` | [Content-manifest elision](#content-manifest-payload-elision) |
| `orleans.lattice.replication.coalesce.entries_elided` | `Counter<long>` | `{entry}` | `tree`, `peer` | [Pre-ship coalescing](#pre-ship-coalescing-coalesceentries_elided--coalescebytes_elided--coalescedeltas_merged) |
| `orleans.lattice.replication.coalesce.bytes_elided` | `Counter<long>` | `By` | `tree`, `peer` | [Pre-ship coalescing](#pre-ship-coalescing-coalesceentries_elided--coalescebytes_elided--coalescedeltas_merged) |
| `orleans.lattice.replication.coalesce.deltas_merged` | `Counter<long>` | `{delta}` | `tree`, `peer` | [Pre-ship coalescing](#pre-ship-coalescing-coalesceentries_elided--coalescebytes_elided--coalescedeltas_merged) |
| `orleans.lattice.replication.doorbell.rung` | `Counter<long>` | `{ring}` | `tree`, `peer` | [Doorbell coalescing](#doorbell-coalescing-doorbellrung--doorbellcoalesced) |
| `orleans.lattice.replication.doorbell.coalesced` | `Counter<long>` | `{ring}` | `tree`, `peer` | [Doorbell coalescing](#doorbell-coalescing-doorbellrung--doorbellcoalesced) |
| `orleans.lattice.replication.compress.dictionary.bytes_in` | `Counter<long>` | `By` | `tree` | [Dictionary compression ratio](#shared-dictionary-compression-ratio-compressdictionarybytes_in--compressdictionarybytes_out) |
| `orleans.lattice.replication.compress.dictionary.bytes_out` | `Counter<long>` | `By` | `tree` | [Dictionary compression ratio](#shared-dictionary-compression-ratio-compressdictionarybytes_in--compressdictionarybytes_out) |
| `orleans.lattice.replication.ship.dictionary_negotiation` | `Counter<long>` | `{negotiation}` | `tree`, `peer`, `outcome` | [Dictionary negotiation](wire-format.md#per-peer-shared-dictionary-capability-negotiation) |
| `orleans.lattice.replication.ship.dictionary_batches` | `Counter<long>` | `{batch}` | `tree`, `peer`, `dictionary` | [Dictionary negotiation](wire-format.md#per-peer-shared-dictionary-capability-negotiation) |
| `orleans.lattice.replication.ship.dictionary_convergence` | `Counter<long>` | `{pull}` | `tree`, `peer`, `outcome` | [Dictionary convergence](#shared-dictionary-convergence-shipdictionary_convergence) |
| `orleans.lattice.replication.ship.wire_version_down_stamp` | `Counter<long>` | `{batch}` | `tree`, `peer`, `reason` | [Down-stamping](wire-format.md#version-adaptive-down-stamping-wireversiondownencoder) |
| `orleans.lattice.replication.apply.duration` | `Histogram<double>` | `ms` | `tree`, `peer`, `outcome` | [Apply duration](#apply-duration-histogram-applyduration) |
| `orleans.lattice.replication.apply.lag` | `Histogram<double>` | `ms` | `tree`, `peer` | [Replication lag](#replication-lag-histogram-applylag) |
| `orleans.lattice.replication.apply.parallel_runs` | `Histogram<int>` | `{run}` | _(none)_ | [Parallel-apply degree](#parallel-apply-degree-applyparallel_runs) |
| `orleans.lattice.replication.apply.fifo_violations` | `Counter<long>` | `{entry}` | `tree`, `origin` | [FIFO diagnostic](#per-origin-fifo-invariant-applyfifo_violations) |
| `orleans.lattice.replication.apply.buffered_entries` | `UpDownCounter<long>` | `{entry}` | `tree`, `shard` | [Causal+ instruments](#causal-instruments) |
| `orleans.lattice.replication.apply.buffer_bytes` | `UpDownCounter<long>` | `By` | `tree`, `shard` | [Causal+ instruments](#causal-instruments) |
| `orleans.lattice.replication.apply.dependency_wait` | `Histogram<double>` | `ms` | `tree` | [Causal+ instruments](#causal-instruments) |
| `orleans.lattice.replication.apply.causal_violations_blocked` | `Counter<long>` | `{entry}` | `tree` | [Causal+ instruments](#causal-instruments) |
| `orleans.lattice.replication.dead_letter.enqueued` | `Counter<long>` | `{entry}` | `tree`, `reason` | [DLQ reasons](#dlq-enqueue-reason-classification) |
| `orleans.lattice.replication.dead_letter.removed` | `Counter<long>` | `{entry}` | `tree`, `reason` | [DLQ reasons](#dlq-enqueue-reason-classification) |
| `orleans.lattice.replication.peer.fell_off_log` | `Counter<long>` | `{event}` | `tree`, `origin` | [Fall-off detection](#fall-off-the-log-detection-peerfell_off_log--peerfell_off_log_suppressed) |
| `orleans.lattice.replication.peer.fell_off_log_suppressed` | `Counter<long>` | `{event}` | `tree`, `origin` | [Fall-off detection](#fall-off-the-log-detection-peerfell_off_log--peerfell_off_log_suppressed) |
| `orleans.lattice.replication.bootstrap.entries_received` | `Counter<long>` | `{entry}` | `tree`, `origin` | [Bootstrap instruments](#bootstrap-instruments) |
| `orleans.lattice.replication.bootstrap.bytes_received` | `Counter<long>` | `By` | `tree`, `origin` | [Bootstrap instruments](#bootstrap-instruments) |
| `orleans.lattice.replication.bootstrap.duration` | `Histogram<double>` | `ms` | `tree`, `origin`, `outcome` | [Bootstrap instruments](#bootstrap-instruments) |
| `orleans.lattice.replication.bootstrap.transient_retries` | `Counter<long>` | `{retry}` | `tree`, `origin` | [Bootstrap instruments](#bootstrap-instruments) |
| `orleans.lattice.replication.digest_probe.compared` | `Counter<long>` | `{comparison}` | `tree`, `shard`, `peer`, `outcome` | [Digest probe](anti-entropy-digest-probe.md#observability) |
| `orleans.lattice.replication.digest_probe.mismatch` | `Counter<long>` | `{comparison}` | `tree`, `shard`, `peer` | [Digest probe](anti-entropy-digest-probe.md#observability) |
| `orleans.lattice.replication.merkle_walk.localised` | `Counter<long>` | `{leaf}` | `tree`, `depth` | [Merkle walk](anti-entropy-merkle-walk.md#observability) |
| `orleans.lattice.replication.merkle_walk.aborted` | `Counter<long>` | `{walk}` | `reason` | [Merkle walk](anti-entropy-merkle-walk.md#observability) |
| `orleans.lattice.replication.leaf_rereplay.entries` | `Counter<long>` | `{entry}` | `tree`, `peer` | [Leaf re-replay](anti-entropy-leaf-rereplay.md#observability) |
| `orleans.lattice.replication.leaf_rereplay.skipped` | `Counter<long>` | `{skip}` | `tree`, `peer`, `reason` | [Leaf re-replay](anti-entropy-leaf-rereplay.md#observability) |
| `orleans.lattice.replication.bootstrap_fallback.triggered` | `Counter<long>` | `{fallback}` | `tree`, `peer` | [Bootstrap fallback](anti-entropy-bootstrap-fallback.md#observability) |
| `orleans.lattice.replication.bootstrap_fallback.entries` | `Counter<long>` | `{entry}` | `tree`, `peer` | [Bootstrap fallback](anti-entropy-bootstrap-fallback.md#observability) |
| `orleans.lattice.replication.bootstrap_fallback.skipped` | `Counter<long>` | `{skip}` | `tree`, `peer`, `reason` | [Bootstrap fallback](anti-entropy-bootstrap-fallback.md#observability) |
| `orleans.lattice.replication.digest_remediation.skipped` | `Counter<long>` | `{skip}` | `tree`, `peer`, `reason` | [Remediation guards](anti-entropy-remediation-guards.md#observability) |
| `orleans.lattice.replication.saga.phase.duration` | `Histogram<double>` | `ms` | `phase` | [Coordinated-restore saga](#coordinated-restore-saga) |
| `orleans.lattice.replication.saga.fence.duration` | `Histogram<double>` | `ms` | `tree` | [Coordinated-restore saga](#coordinated-restore-saga) |
| `orleans.lattice.replication.saga.participant.votes` | `Counter<long>` | `{vote}` | `reason` | [Coordinated-restore saga](#coordinated-restore-saga) |
| `orleans.lattice.replication.saga.participant.commits` | `Counter<long>` | `{commit}` | `reason` | [Coordinated-restore saga](#coordinated-restore-saga) |
| `orleans.lattice.replication.saga.participant.aborts` | `Counter<long>` | `{abort}` | `reason` | [Coordinated-restore saga](#coordinated-restore-saga) |
| `orleans.lattice.replication.saga.compensations` | `Counter<long>` | `{compensation}` | `cause` | [Coordinated-restore saga](#coordinated-restore-saga) |
