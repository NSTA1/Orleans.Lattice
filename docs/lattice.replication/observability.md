# Observability

`Orleans.Lattice.Replication` publishes every replication-side instrument on a single meter, `orleans.lattice.replication`. An OpenTelemetry pipeline (or any `MeterListener`) subscribes once and receives every replication metric. The instruments fall into four shapes:

- **Per-peer gauges** - `entries_behind`, `bytes_behind`, `ship_in_flight`, `consecutive_errors`, `last_contact_seconds`. Owned by `ReplicationPeerStats`. Tagged `tree` + `peer`. The `consecutive_errors` and `last_contact_seconds` gauges are **bidirectional** and additionally carry a `direction` tag (`outbound` from the local sender's ship loop, `inbound` from the local receiver's apply loop). `entries_behind`, `bytes_behind`, and `ship_in_flight` remain outbound-only (the receiver does not track a per-peer backlog into itself, nor does it pipeline into itself).
- **Per-operation histograms** - `ship.duration`, `apply.duration`, `apply.lag`, `apply.parallel_runs`, `ship.effective_batch_size`, `ship.ack_latency`. Reported in milliseconds except `apply.parallel_runs` (unit `{run}`) and `ship.effective_batch_size` (unit `{entry}`).
- **Throughput counters** - `wal.entries_shipped`. Counts entries the producer durably ships to each peer; correlate it against WAL retention / GC to confirm the sender keeps pace with the log. The companion `wal.entries_trimmed` counter belongs to the core library and is published on the `orleans.lattice` meter (`LatticeMetrics.WalEntriesTrimmed`); subscribe to both meters when correlating ship-rate against trim-rate. The `ship.redundant_payloads` / `ship.redundant_payload_bytes` counters (see below) ride on the same meter and emit in a default build because content-hash dedup measurement is on by default; the `coalesce.entries_elided` / `coalesce.bytes_elided` / `coalesce.deltas_merged` counters likewise emit by default because pre-ship coalescing is on by default. Each set falls silent only when its option (`ContentHashDedupEnabled` / `PreShipCoalescingEnabled`) is explicitly set to `false`.
- **DLQ counters** - `dead_letter.enqueued`, `dead_letter.removed`. Tagged `tree` + `reason`.

## Replication-lag histogram (`apply.lag`)

`orleans.lattice.replication.apply.lag` is recorded by the canonical applier immediately after a successful point apply (`Set` / `Delete`). The sample is `now - entry.Timestamp.WallClockTicks` in milliseconds, **clamped to a non-negative value** so a future-dated source HLC (e.g. a faster-moving peer's wall clock) reports as `0` rather than corrupting the histogram with a negative sample.

| Property | Value |
|---|---|
| Name | `orleans.lattice.replication.apply.lag` |
| Unit | `ms` |
| Tags | `tree`, `peer` |

The `peer` tag carries the entry's `OriginClusterId` - i.e. the **authoring** cluster of the replicated mutation, not the immediate transport hop the receiver pulled it from. Under transitive replication (A &#8594; B &#8594; C) an entry shipped from B to C still records `peer=A`, mirroring the producer-side `WalRecord.OriginClusterId` slot. Operators filtering inbound apply lag by the source-of-truth replica use this tag value directly; queries that need transport-hop attribution join the `tree` + `peer` pair against the cluster's known replication topology.

The histogram is intentionally not recorded for:

- **`MutationKind.DeleteRange`** - range deletes carry `HybridLogicalClock.Zero` by design (a range walk produces many per-leaf HLCs that cannot be faithfully collapsed into one), so the lag would be a meaningless multi-decade value.
- **HWM-deduped re-deliveries** - the entry never reached the merge step, so reporting lag would conflate "applied" and "filtered" samples.
- **Local-origin entries** - the apply path short-circuits at the local-origin no-op gate before touching the receiver-side merge.
- **Source HLC equal to `Zero`** - protects against a malformed entry that would otherwise publish a garbage "now - 0" sample.

A receiver that operates entirely under HWM dedupe (i.e. every entry it sees has already been applied locally) reports an empty `apply.lag` distribution. That is the correct signal: there is no replication progress to measure.

## Apply-duration histogram (`apply.duration`)

`orleans.lattice.replication.apply.duration` records the wall-clock time the canonical applier spends inside `ApplyAsync`, from entry through every terminal return path. The body is wrapped in a `try { ... } finally { Record(...); }` so an uncaught exception still records a sample tagged with the failure outcome before unwinding. The duration is read via `Stopwatch.GetElapsedTime(long)`, which is allocation-free.

| Property | Value |
|---|---|
| Name | `orleans.lattice.replication.apply.duration` |
| Unit | `ms` |
| Tags | `tree`, `peer`, `outcome` |

The `peer` tag carries the same value as `apply.lag`'s `peer` tag - the entry's `OriginClusterId`, identifying the authoring cluster rather than the transport hop. The batch path's `ApplyOriginRunAsync` groups entries into contiguous same-`(treeId, originClusterId)` runs and records each per-entry duration with the run's shared `peer` value, so multi-origin batches surface as one `peer` per run rather than collapsing into a single dominant value.

The `outcome` tag partitions the histogram into seven mutually-exclusive buckets:

| Value | Constant | When |
|---|---|---|
| `success` | `LatticeReplicationMetrics.OutcomeSuccess` | The entry was applied successfully - both directly applied point operations (`Set` / `Delete`) and range deletes contribute. Each `ApplyAsync` invocation records exactly one `apply.duration` sample regardless of how many entries the call drains from the causal-apply buffer: a drain cascade triggered by an arriving satisfier contributes its drained-entry work to the satisfier's own `success` sample, and the originally parked entries do not generate additional samples on drain. |
| `dedup` | `LatticeReplicationMetrics.OutcomeDedup` | The entry was short-circuited before merge - either the per-origin high-water-mark already covers `entry.Timestamp`, or the local-origin defence-in-depth gate detected an entry that must not loop back onto its authoring cluster. |
| `failure` | `LatticeReplicationMetrics.OutcomeFailure` | The apply attempt threw. Recorded in the `finally` path before the exception unwinds. Includes payload-shape faults (`ArgumentException`, `InvalidOperationException`), `OperationCanceledException` from a cancelled `cancellationToken` (graceful shutdown traffic appears here), transport / IO failures, and any other unhandled exception out of the apply pipeline. |
| `parked-causal-buffer` | `LatticeReplicationMetrics.OutcomeParkedCausalBuffer` | The entry parked on the causal-apply buffer because its declared `VectorClock` was not yet dominated by the local vector clock. The original delivery did not advance the high-water-mark; the entry re-enters the apply pipeline through the buffer drain when its dependencies arrive. |
| `shadow-forward-dedup` | `LatticeReplicationMetrics.OutcomeShadowForwardDedup` | The entry was suppressed by the per-tree shadow-forward dedupe cache because a matching identity tuple (`(originClusterId, timestamp, key, op)`) was already applied since the last cache eviction. The duplicate arises when a structural rewrite (shard split / merge / saga compensate) shadow-forwards a user write into a different shard, so both emits ride the WAL with identical identity tuples. |
| `rejected-not-replicated` | `LatticeReplicationMetrics.OutcomeRejectedNotReplicated` | The inbound entry was rejected by the receiver-side enrollment gate because its `TreeId` is not enrolled for replication on this receiver (the local per-tree resolver returns no merge mode for it). The entry is dropped without applying and without dead-lettering - a non-enrolled tree id is peer-controlled, so parking it would let a peer spawn unbounded dead-letter-queue activations. |
| `rejected-mode-mismatch` | `LatticeReplicationMetrics.OutcomeRejectedModeMismatch` | The inbound entry was rejected by the receiver-side merge-mode gate because its peer-supplied `Mode` disagrees with the merge mode the receiver resolves locally for the entry's `TreeId`. The entry is not applied; because the tree is enrolled (and therefore bounded) the entry is dead-lettered with the `mode_mismatch` reason rather than silently dropped. |

A receiver with a single overwhelmed subscriber surfaces as a rising `failure` bucket; a receiver with persistent causal skew surfaces as a rising `parked-causal-buffer` bucket. Both are independent of `apply.lag`, which only samples successful merges.

## Parallel-apply degree (`apply.parallel_runs`)

`orleans.lattice.replication.apply.parallel_runs` records the effective degree of parallelism the receiver-side batch-apply path used for a single inbound batch - the number of independent `(treeId, originClusterId)` run-groups applied concurrently. One sample is recorded per multi-entry batch.

| Property | Value |
|---|---|
| Name | `orleans.lattice.replication.apply.parallel_runs` |
| Unit | `{run}` |
| Tags | _(none)_ |

The histogram is untagged: the measurement describes the batch as a whole, which may span multiple trees. A value of `1` denotes fully-sequential apply - either the default posture (`ApplyMaxParallelRuns = 1`) or a single-tree batch where cross-tree parallelism is moot. A value greater than `1` reports the achieved concurrency, which is the host-configured `LatticeReplicationOptions.ApplyMaxParallelRuns` clamped to the number of distinct trees present in the batch.

Operators use the distribution to confirm parallel apply is actually engaging under multi-tree load (the `p50` rising above `1` after raising `ApplyMaxParallelRuns`) and to correlate the achieved parallelism against `apply.lag` and `apply.duration`. Independence is enforced at the tree granularity: distinct trees apply concurrently, while runs that share a tree stay sequential so the per-tree causal-apply buffer, shadow-forward dedupe cache, per-origin FIFO, and per-origin high-water-mark monotonicity hold exactly as in the sequential path. See [the batch-apply section of replication-apply.md](replication-apply.md) for the full independence model.

## Ship-rate (`wal.entries_shipped`)

The producer no longer emits a commit-time append counter: a commit reaches the per-shard write-ahead log exactly once, via the leaf commit-log writer, and the per-`(tree, peer)` shipper tails that log in the background. Ship progress is therefore observed directly through the ship counter and correlated against WAL retention / GC.

| Counter | Tags | Recorded |
|---|---|---|
| `orleans.lattice.replication.wal.entries_shipped` | `tree`, `peer` | After a successful Push acknowledgement at the gRPC transport. Incremented by the count of entries inside the acknowledged envelope; a heartbeat / keep-alive (zero-entry) batch contributes zero. |

Operators monitor `rate(wal_entries_shipped)` per tree-peer pair against the WAL's growth and trim signals (`wal.entries_trimmed`, plus the configured retention window). A ship rate that persistently lags the WAL's growth means the local log is accumulating faster than the sender can drain it, which is the signal the min-acked-cursor WAL GC predicate and a future health check both consume.

## DLQ enqueue-reason classification

`orleans.lattice.replication.dead_letter.enqueued` is tagged with one of four canonical reason values:

| Value | When |
|---|---|
| `schema` | The terminal failure was an `ArgumentException` (malformed entry, missing field, range delete with no end key) or an `InvalidOperationException` (unrecognised `LatticeMergeMode`, state-merge CAS budget exhausted). The receiver classifies these as payload-shape faults. |
| `hlc_skew` | Reserved. Future receiver decorators that surface implausible HLC skew between the receiver's wall clock and the entry's `Timestamp` as a classified exception will tag this value. |
| `oversized` | Reserved. Future receiver decorators that wrap the canonical applier with a size-validating check will tag this value when a single entry exceeds the configured per-entry size ceiling. |
| `unknown` | Catch-all for terminal failure shapes the canonical decorator could not classify (e.g. transport / IO / `TimeoutException`). |

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

The measurement is **observability-only**: it never elides, reorders, or alters the bytes the sender ships, so the wire output is unchanged whether or not dedup measurement is enabled. (Actually skipping a byte-identical re-set carrying a newer HLC would strand the receiver's per-origin high-water mark and change LWW/HLC convergence; the receiver must consent through a manifest/pull exchange, which is deferred until wire-version capability negotiation lands.) Because the counters fire as entries are framed onto the wire, a batch re-shipped after a transient transport failure counts its entries again - which is correct, since a re-ship is itself a redundant wire payload.

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

Coalescing runs on both last-writer-wins and recognised CRDT trees, but by different mechanics. On a `LwwRegister` tree the pass keeps only the latest same-key version and drops the earlier ones outright. On a recognised CRDT tree, dropping earlier versions would lose each entry's delta contribution, so the pass instead **folds** the same-key deltas into one combined delta - a join over the primitive's semilattice whose receiver-side apply effect is identical to applying the source deltas in sequence - re-encodes it onto the kept entry, and elides the rest. A registered `OrMap` tree folds by unioning the dot-tagged adds and tombstones and lattice-merging any same-dot value snapshots through the value CRDT's own join, so it coalesces like the closed shapes. `coalesce.deltas_merged` counts the source deltas folded on this CRDT path; `entries_elided` / `bytes_elided` count the source entries it dropped, exactly as on the LWW path. An `OrMap` tree whose `(TKey, TValue)` shape is unregistered (no shape descriptor resolves) and any CRDT entry carrying no typed delta fall back to shipping individually (loss-free). Only plain point `Set` / `Delete` writes with a real (non-`Zero`) HLC that are not prepared atomic-batch entries are eligible; range deletes, saga terminal marks, and zero-HLC entries are never coalesced. Read the elided fraction as `rate(coalesce_entries_elided) / rate(wal_entries_shipped)` per `(tree, peer)`: a high ratio signals a hot rewrite pattern that coalescing is collapsing, and `coalesce.bytes_elided` quantifies the cross-cluster bandwidth reclaimed. The coalesced output converges identically on an unmodified receiver - a strict subset on LWW trees, an effect-equivalent merge on CRDT trees.

## Doorbell coalescing (`doorbell.rung` / `doorbell.coalesced`)

Separate from the *pre-ship* coalescing above (which reduces the bytes a batch carries), these counters measure how the commit-time nudge that *wakes* the shipper is coalesced at the source. A doorbell is an idempotent, edge-triggered "there is work" signal, so the commit-time doorbell sink collapses a burst of per-commit ring requests for the same `(tree, peer)` into at most one in-flight ring plus one pending follow-up, rather than dispatching one `OnDoorbellAsync` grain call per commit onto the non-reentrant shipper activation (see [Writer-side coalescing](replication-drivers.md#writer-side-coalescing)). Both counters fire whenever `LatticeReplicationOptions.ShipDoorbellEnabled` is `true` (the default).

| Counter | Constant | Unit | Tags | Recorded |
|---|---|---|---|---|
| `orleans.lattice.replication.doorbell.rung` | `LatticeReplicationMetrics.DoorbellRungName` | `{ring}` | `tree`, `peer` | Once per doorbell ring actually dispatched to the shipper (the in-flight ring plus any trailing follow-up). |
| `orleans.lattice.replication.doorbell.coalesced` | `LatticeReplicationMetrics.DoorbellCoalescedName` | `{ring}` | `tree`, `peer` | Once per ring request elided because a ring for the same `(tree, peer)` was already in flight (folded into the single pending follow-up). |

Read the coalescing win as `rate(doorbell_coalesced) / (rate(doorbell_coalesced) + rate(doorbell_rung))` per `(tree, peer)`: under a sustained write burst this ratio approaches 1 (nearly every per-commit request is absorbed) while `doorbell.rung` stays near the small constant the shipper actually needs to stay awake. A `doorbell.rung` rate that tracks the raw commit rate with a near-zero `doorbell.coalesced` rate signals writes arriving slower than the shipper drains them - no storm to absorb. Because the base phase-timer and keepalive-reminder still drive shipping independently of doorbells, a coalesced (elided) ring never delays delivery beyond one timer tick.

## Shared-dictionary compression ratio (`compress.dictionary.bytes_in` / `compress.dictionary.bytes_out`)

These counters are **opt-in** and fire only when shared-dictionary compression is selected (`LatticeReplicationOptions.FramingCompression = LatticeCompression.ZstdDictionary` with a non-zero `FramingCompressionDictionaryId`, and the requested dictionary resolves on the sending silo); the default build never records them. They quantify the before/after win of compressing the batch tail against a shared Zstandard dictionary (see [Shared-dictionary Zstandard compression](../lattice/compression.md#shared-dictionary-zstandard-compression)).

| Counter | Constant | Unit | Tags | Recorded |
|---|---|---|---|---|
| `orleans.lattice.replication.compress.dictionary.bytes_in` | `LatticeReplicationMetrics.CompressDictionaryBytesInName` | `By` | `tree` | The uncompressed tail length each time a batch is framed with the `ZstdDictionary` tag (the "before"). |
| `orleans.lattice.replication.compress.dictionary.bytes_out` | `LatticeReplicationMetrics.CompressDictionaryBytesOutName` | `By` | `tree` | The compressed tail length emitted for that same batch (the "after"). |

Read the achieved ratio as `rate(compress_dictionary_bytes_out) / rate(compress_dictionary_bytes_in)` per `tree`: a value well below `1.0` is the dictionary saving (lower is better; `1.0` means no saving). Compare it against the dictionary-less `Zstd` baseline on the same workload to decide whether a given dictionary id is worth shipping. The counters are emitted on the framing encode path only, so a frame that gracefully degrades to plain `Zstd` (because the dictionary could not be resolved locally) does not contribute - which keeps the ratio honest about the dictionary path specifically. The `peer` tag is not available at the encode seam, so these counters are tagged by `tree` only.

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
| `orleans.lattice.replication.apply.dependency_wait_ms` | `Histogram<double>` (ms) | `tree` | One sample per drained entry: `now - parked_at`, clamped non-negative. Evicted entries do not contribute - only successful waits are observed. |
| `orleans.lattice.replication.apply.causal_violations_blocked` | `Counter<long>` | `tree` | Incremented once per successful park. Duplicate-tuple parks do not count. An alert on `rate > 0` flags causal-skew health regardless of whether buffered entries eventually drain or evict. |

Operators monitor them together:

- A steady-state replicating peer keeps `buffered_entries` near zero and emits `dependency_wait_ms` samples close to the round-trip-time of a single ack cycle.
- A persistent rise in `buffered_entries` or `buffer_bytes` paired with a low or zero `causal_violations_blocked` rate is the classic "bounded buffer absorbing transient skew, then draining" pattern - healthy.
- A sustained nonzero `causal_violations_blocked` rate paired with `apply.dependency_wait_ms` distributions in the seconds-to-minutes range indicates structural causal skew the bounded buffer is masking; pair with the DLQ enqueue rate (`dead_letter.enqueued{reason="hlc_skew"}`) to detect overflow.
- A sudden buffer drain that does not advance the local high-water-mark (visible as a spike in `dependency_wait_ms` with no matching `apply.lag` improvement) suggests a CRDT-merge regression rather than a transport-side issue.

## Per-origin FIFO invariant (`apply.fifo_violations`)

The receiver-side apply pipeline relies on a per-origin FIFO contract for its causal-apply buffer's occupancy bounds: under correct sender + transport behaviour, the producer's partitioned change feed yields per-shard in WAL-offset order and each shard's WAL is HLC-monotonic per origin, so per-`(origin, shard)` FIFO is preserved end-to-end with **no cross-shard sender serialisation** (which would defeat the whole point of partitioned replog scaling).

| Property | Value |
|---|---|
| Name | `orleans.lattice.replication.apply.fifo_violations` |
| Unit | `{entry}` |
| Tags | `tree`, `origin` |

The canonical applier records the most recently applied source HLC per `(treeId, originClusterId)` in process-local memory and increments `apply.fifo_violations` when a successfully applied entry's HLC is **strictly less** than the prior recorded value for the same pair. The counter is recorded:

- **After a successful apply** (direct or drained from the causal-apply buffer) - never on park. The invariant tracks "what has been merged" rather than "what has been observed", so a transient park of a higher-HLC entry that drains after a lower-HLC arrival does not falsely register a violation.
- **For point operations only** (`Set` / `Delete`). `DeleteRange` carries `HybridLogicalClock.Zero` by design and is excluded - it neither records a violation nor overwrites the recorded HLC.

A violation **does not change apply behaviour**: the entry is still applied, the HWM is still advanced. This is purely an observability surface - an alert on `rate > 0` flags a transport-side regression that broke the per-origin order, not a correctness defect on the receiver. Operators triage by joining the `tree` and `origin` tags against the producer-side topology to identify which sender path regressed.

Cross-shard interleaving for the same origin is permitted by design and is **not** a FIFO violation under this contract: entries that have a genuine cross-shard causal dependency carry it in their `VectorClock` and route through the causal-apply buffer's dependency-check path instead. The current implementation tracks one entry per `(tree, origin)` because the canonical applier is one-instance-per-tree; a future per-shard applier partitioning will key the tracker by `(tree, shard, origin)` without changing the metric's tag dimensionality.

## Bootstrap instruments

The receiver-side bootstrap coordinator emits the following instruments tracking the cross-cluster snapshot-drain pipeline plus a structured phase-transition log line. Together they let an operator dashboard the lifecycle of an in-flight bootstrap and tail a single run end-to-end through the silo log.

| Instrument | Kind | Tags | Recorded when |
|---|---|---|---|
| `orleans.lattice.replication.bootstrap.entries_received` | `Counter<long>` | `tree`, `origin` | Incremented by 1 per snapshot entry successfully applied through the local replication applier (post-decorator chain). |
| `orleans.lattice.replication.bootstrap.bytes_received` | `Counter<long>` (`By`) | `tree`, `origin` | Incremented by `entry.Value.Length` per applied entry. Mirrors the lifecycle of `entries_received`. |
| `orleans.lattice.replication.bootstrap.duration` | `Histogram<double>` (`ms`) | `tree`, `origin`, `outcome` | Recorded once per terminal phase transition. `outcome` is one of `live`, `failed`, or `timed_out`. |
| `orleans.lattice.replication.bootstrap.transient_retries` | `Counter<long>` | `tree`, `origin` | Incremented by 1 each time the bootstrap drain catches a classified-transient transport fault and consumes one slot of the configured `LatticeReplicationOptions.BootstrapTransientRetry` budget. A bootstrap that completes on its first drain attempt records zero on this counter; a bootstrap that exhausts the budget and pivots to `Failed` records `MaxAttempts - 1` (one per consumed retry slot). |

The `origin` tag carries the source cluster id supplied at kickoff (`BootstrapAsync(sourceClusterId, ...)`), matching the tag dimensionality used by the per-origin fall-off-the-log counters so dashboards can join the two without a separate keying.

The histogram's `outcome` values are exposed as `LatticeReplicationMetrics.BootstrapOutcomeLive`, `BootstrapOutcomeFailed`, and `BootstrapOutcomeTimedOut` constants. The `timed_out` value is reserved for a future transport-timeout policy; the in-tree coordinator emits only `live` and `failed` today, but the constant is published so dashboard rules referencing it remain valid across future releases.

The duration timer is anchored on a per-activation in-memory stopwatch captured at kickoff (or lazy-initialised on the first drain pass after a silo failover). It records `Stopwatch.GetElapsedTime` from that anchor to the terminal transition; a silo failover between kickoff and completion therefore truncates the measured interval to the span since the most recent reactivation. Operators monitoring cross-failover total durations should pair the histogram with the per-entry counters, which are restartable across reactivations.

Phase-transition structured logs are emitted at `LogLevel.Information` from the bootstrap coordinator with the message template

```
Bootstrap phase transition for tree '{TreeName}' from source '{SourceClusterId}': {Previous} -> {Next} (LastAppliedHlc={LastAppliedHlc})
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

- `direction="outbound"` - recorded by the per-peer shipper after a peer accepts a shipped batch. Includes the periodic empty **liveness probe** the shipper fires when the drain buffer is empty and the wall-clock interval since the last successful outbound contact has elapsed. The probe is configured by `LatticeReplicationOptions.LivenessProbeInterval` (default `30 s`; set to `Timeout.InfiniteTimeSpan` to disable). The probe interval timer is anchored on the first idle pump tick after activation, so the first idle tick is silent and the probe begins one interval after activation. The payload is the 16-byte framing header alone; no entries are shipped.
- `direction="inbound"` - recorded by the canonical applier's batch path after a per-origin run of inbound entries applies (or fails) on the local receiver. Keyed by the entries' `WalRecord.OriginClusterId`. Range-delete and local-origin entries skip the recording.

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
| `orleans.lattice.replication.saga.participant.votes` | `Counter<long>` (`{vote}`) | `reason` | Incremented once per participant prepare with the vote outcome. `reason` is `commit`, or one of the abort reasons `infeasible` (admission refused a target that cannot fit), `precondition` (a missing backup or base in the manifest chain), `build-failed` (the bounded build-retry budget was exhausted), or `engine-unavailable` (the backup package is not wired on this cluster). Lets operators watch the commit-vote fraction and the distribution of abort refusals. |
| `orleans.lattice.replication.saga.participant.commits` | `Counter<long>` (`{commit}`) | `reason` | Incremented once per committed participant. `reason` is `single` (single-tree restore) or `set` (backup-set group-atomic restore). |
| `orleans.lattice.replication.saga.participant.aborts` | `Counter<long>` (`{abort}`) | `reason` | Incremented once per aborted participant. `reason` is `single`, `set`, or `engine-unavailable`. |
| `orleans.lattice.replication.saga.compensations` | `Counter<long>` (`{compensation}`) | `cause` | Incremented once per participant grain that rolls back a prepared saga. `cause` is `vote-abort` (a coordinator-driven rollback after at least one participant voted abort) or `coordinator-loss` (a participant's own cutover-fence expiry auto-compensation after the coordinator decision never arrived). |

Every instrument name, tag key, and tag value is exposed as a `const` on
`LatticeReplicationMetrics` (for example `SagaPhaseDurationName`, `TagPhase`,
`SagaPhasePrepare`, `TagCause`, `SagaCauseCoordinatorLoss`), so dashboard rules
and external subscribers reference the constants rather than hard-coding the
strings. The `saga.phase.duration` histogram uses a monotonic stopwatch anchored
at each phase entry; a silo failover mid-phase truncates the measured interval to
the span since the most recent reactivation, matching the bootstrap-duration
behaviour described above.
