# Observability

`Orleans.Lattice.Replication` publishes every instrument on a single meter, `orleans.lattice.replication`. An OpenTelemetry pipeline (or any `MeterListener`) subscribes once and receives every replication metric. The instruments fall into four shapes:

- **Per-peer gauges** — `entries_behind`, `bytes_behind`, `consecutive_errors`, `last_contact_seconds`. Owned by `ReplicationPeerStats`. Tagged `tree` + `peer`.
- **Per-operation histograms** — `ship.duration`, `apply.duration`, `apply.lag`. Reported in milliseconds.
- **Throughput counters** — `wal.entries_appended`, `wal.entries_shipped`, `wal.entries_trimmed`. Used to compute growth-rate vs. ship-rate ratios.
- **DLQ counters** — `dead_letter.enqueued`, `dead_letter.removed`. Tagged `tree` + `reason`.

## Replication-lag histogram (`apply.lag`)

`orleans.lattice.replication.apply.lag` is recorded by the canonical `ReplicationApplier` immediately after a successful point apply (`Set` / `Delete`). The sample is `now - entry.Timestamp.WallClockTicks` in milliseconds, **clamped to a non-negative value** so a future-dated source HLC (e.g. a faster-moving peer's wall clock) reports as `0` rather than corrupting the histogram with a negative sample.

| Property | Value |
|---|---|
| Name | `orleans.lattice.replication.apply.lag` |
| Unit | `ms` |
| Tags | `tree` |

The histogram is intentionally not recorded for:

- **`ReplogOp.DeleteRange`** — range deletes carry `HybridLogicalClock.Zero` by design (a range walk produces many per-leaf HLCs that cannot be faithfully collapsed into one), so the lag would be a meaningless multi-decade value.
- **HWM-deduped re-deliveries** — the entry never reached the merge step, so reporting lag would conflate "applied" and "filtered" samples.
- **Local-origin entries** — the apply path short-circuits at the local-origin no-op gate before touching the receiver-side merge.
- **Source HLC equal to `Zero`** — protects against a malformed entry that would otherwise publish a garbage "now - 0" sample.

A receiver that operates entirely under HWM dedupe (i.e. every entry it sees has already been applied locally) reports an empty `apply.lag` distribution. That is the correct signal: there is no replication progress to measure.

## Apply-duration histogram (`apply.duration`)

`orleans.lattice.replication.apply.duration` records the wall-clock time the canonical `ReplicationApplier` spends inside `ApplyAsync`, from entry through every terminal return path. The body is wrapped in a `try { ... } finally { Record(...); }` so an uncaught exception still records a sample tagged with the failure outcome before unwinding. The duration is read via `Stopwatch.GetElapsedTime(long)`, which is allocation-free.

| Property | Value |
|---|---|
| Name | `orleans.lattice.replication.apply.duration` |
| Unit | `ms` |
| Tags | `tree`, `outcome` |

The `outcome` tag partitions the histogram into four mutually-exclusive buckets:

| Value | Constant | When |
|---|---|---|
| `success` | `LatticeReplicationMetrics.OutcomeSuccess` | The entry was applied successfully — both directly applied point operations (`Set` / `Delete`) and range deletes contribute. Each `ApplyAsync` invocation records exactly one `apply.duration` sample regardless of how many entries the call drains from the causal-apply buffer: a drain cascade triggered by an arriving satisfier contributes its drained-entry work to the satisfier's own `success` sample, and the originally parked entries do not generate additional samples on drain. |
| `dedup` | `LatticeReplicationMetrics.OutcomeDedup` | The entry was short-circuited before merge — either the per-origin high-water-mark already covers `entry.Timestamp`, or the local-origin defence-in-depth gate detected an entry that must not loop back onto its authoring cluster. |
| `failure` | `LatticeReplicationMetrics.OutcomeFailure` | The apply attempt threw. Recorded in the `finally` path before the exception unwinds. Includes payload-shape faults (`ArgumentException`, `InvalidOperationException`), `OperationCanceledException` from a cancelled `cancellationToken` (graceful shutdown traffic appears here), transport / IO failures, and any other unhandled exception out of the apply pipeline. |
| `parked-causal-buffer` | `LatticeReplicationMetrics.OutcomeParkedCausalBuffer` | The entry parked on the causal-apply buffer because its declared `VectorClock` was not yet dominated by the local vector clock. The original delivery did not advance the high-water-mark; the entry re-enters the apply pipeline through the buffer drain when its dependencies arrive. |

A receiver with a single overwhelmed subscriber surfaces as a rising `failure` bucket; a receiver with persistent causal skew surfaces as a rising `parked-causal-buffer` bucket. Both are independent of `apply.lag`, which only samples successful merges.

## Growth-rate vs. ship-rate (`wal.entries_appended` / `wal.entries_shipped`)

The two counters are deliberately a pair:

| Counter | Tags | Recorded |
|---|---|---|
| `orleans.lattice.replication.wal.entries_appended` | `tree` | After a successful WAL append at the `ShardedReplogSink` seam — counts entries the producer durably committed to the local WAL. A throwing append does **not** contribute. |
| `orleans.lattice.replication.wal.entries_shipped` | `tree`, `peer` | After a successful Push acknowledgement at the gRPC transport. Incremented by the count of entries inside the acknowledged envelope; a heartbeat / keep-alive (zero-entry) batch contributes zero. |

Operators monitor `rate(wal_entries_appended) / rate(wal_entries_shipped)` per tree-peer pair. Steady-state replication keeps the ratio close to `1`. A persistently rising ratio indicates the local WAL is growing faster than the sender can ship, which is the signal the min-acked-cursor WAL GC predicate and a future health check both consume.

## DLQ enqueue-reason classification

`orleans.lattice.replication.dead_letter.enqueued` is tagged with one of four canonical reason values:

| Value | When |
|---|---|
| `schema` | The terminal failure was an `ArgumentException` (malformed entry, missing field, range delete with no end key) or an `InvalidOperationException` (unrecognised `ReplicationMode`, state-merge CAS budget exhausted). The receiver classifies these as payload-shape faults. |
| `hlc_skew` | Reserved. Future receiver decorators that surface implausible HLC skew between the receiver's wall clock and the entry's `Timestamp` as a classified exception will tag this value. |
| `oversized` | Reserved. Future receiver decorators that wrap the canonical applier with a size-validating check will tag this value when a single entry exceeds the configured per-entry size ceiling. |
| `unknown` | Catch-all for terminal failure shapes the canonical decorator could not classify (e.g. transport / IO / `TimeoutException`). |

The mapping lives in `DeadLetterTrackingReplicationApplier.ClassifyFailure` and is intentionally conservative: only failure shapes whose source is under the package's control are matched explicitly, so the `reason` dimension stays stable across publishers and operators can alert on `unknown` rising without false positives from future schema-shape additions.

## Atomic-batch instruments

Four instruments cover the receiver-side cross-cluster atomic-batch staging buffer and saga lifecycle. Every instrument is per-tree, gated on the `LatticeReplicationOptions.AtomicBatchDelivery` opt-in: a tree with the option `false` (the default) never admits an entry to the buffer and therefore never emits any of these signals. A tree opting in surfaces every transaction's lifecycle from first-staged-entry through terminal disposition.

### Buffered-transaction gauge (`apply.tx_buffered`)

| Property | Value |
|---|---|
| Name | `orleans.lattice.replication.apply.tx_buffered` |
| Kind | UpDownCounter |
| Unit | `{transaction}` |
| Tags | `tree` |

Tracks the count of distinct `(originClusterId, transactionId)` keys currently staged on the buffer. Incremented by `+1` when the first entry of a new transaction is admitted; subsequent admits within the same transaction are a no-op on the gauge (the buffer-bytes gauge tracks per-entry growth instead). Decremented by `-1` when the transaction is removed for any reason (apply completion, capacity eviction, orphan eviction, manual discard). Activation rehydration of staged entries does **not** contribute — the gauge is session-scoped and tracks live admission lifecycle, not durable buffer occupancy.

### Buffered-bytes gauge (`apply.tx_buffer_bytes`)

| Property | Value |
|---|---|
| Name | `orleans.lattice.replication.apply.tx_buffer_bytes` |
| Kind | UpDownCounter |
| Unit | `By` |
| Tags | `tree` |

Tracks cumulative serialised payload bytes parked on the buffer at per-entry granularity. Every staged entry contributes its estimated serialised size on admission and reverses that contribution on removal. Drives a future health-probe integration so operators can alert on buffer pressure before `AtomicBatchBufferMaxBytes` triggers capacity eviction. Like `apply.tx_buffered`, activation rehydration does not contribute.

### Atomic-apply duration histogram (`apply.tx_apply_duration_ms`)

| Property | Value |
|---|---|
| Name | `orleans.lattice.replication.apply.tx_apply_duration_ms` |
| Kind | Histogram |
| Unit | `ms` (encoded in the instrument name) |
| Tags | `tree`, `outcome` |

Wall-clock interval, in milliseconds, between the first staged entry of an atomic batch landing on the buffer and the saga that applies the completed batch returning a terminal outcome. The sample is `now - min(staged.EnqueuedAtTicks across all entries in the completed batch)`, clamped to a non-negative value so cross-activation wall-clock skew (a rehydrated `EnqueuedAtTicks` carried forward from a prior silo whose clock was ahead of the current silo's, or an in-flight NTP correction) never produces a negative sample. Recorded **once** per terminal apply outcome — every entry inside the batch shares the same sample, so a 5-key batch with a 200 ms saga records one 200 ms sample, not five.

This is the single most operationally-important instrument on the atomic-batch surface: a host configuring `AtomicBatchDelivery = true` is explicitly trading per-transaction latency for cross-cluster atomic visibility, and this histogram is how that trade-off is verified in production. Pair it with `apply.lag` (per-entry, point-write granularity) to compare the producer-emit-to-receiver-apply lag of point writes vs atomic batches on the same tree.

The `outcome` tag partitions samples by the saga's terminal disposition:

| Value | Constant | When |
|---|---|---|
| `success` | `LatticeReplicationMetrics.OutcomeTxSuccess` | The saga committed every entry in the batch and the per-origin high-water-mark advanced to the batch's max HLC. |
| `dlq_apply_failure` | `LatticeReplicationMetrics.OutcomeTxDlqApplyFailure` | The saga returned `Compensated`, or threw any non-cancellation exception. The receiver routes every entry in the batch to the dead-letter queue tagged `atomic-apply-failure` and holds the high-water-mark unchanged so the producer re-ships on the next pump cycle. |

The histogram is intentionally **not** recorded for the two non-apply terminal paths (`dlq_orphan` and `evicted_capacity`): both reach a terminal disposition without invoking the saga, so a duration sample would conflate "time the buffer held the entries" with "time the saga spent applying them" and corrupt latency dashboards. Both paths still emit a `tx_completed` counter sample tagged with the matching outcome, so terminal accounting stays balanced.

`OperationCanceledException` rethrown from the saga (graceful shutdown traffic) does **not** record a sample — cancellation is not a terminal disposition; the transaction remains in the buffer for the next pump tick to pick up.

### Atomic-batch terminal-outcome counter (`apply.tx_completed`)

| Property | Value |
|---|---|
| Name | `orleans.lattice.replication.apply.tx_completed` |
| Kind | Counter |
| Unit | `{transaction}` |
| Tags | `tree`, `outcome` |

Increments by `1` on every terminal disposition of a buffered transaction. The `outcome` tag partitions the counter into four mutually-exclusive buckets so the sum across outcomes equals the total number of transactions that reached a terminal state on this tree:

| Value | Constant | When |
|---|---|---|
| `success` | `LatticeReplicationMetrics.OutcomeTxSuccess` | The saga committed every entry. Pairs with the `apply.tx_apply_duration_ms{outcome=success}` sample. |
| `dlq_apply_failure` | `LatticeReplicationMetrics.OutcomeTxDlqApplyFailure` | The saga returned `Compensated` or threw a non-cancellation exception. Pairs with the `apply.tx_apply_duration_ms{outcome=dlq_apply_failure}` sample. Every entry in the batch is parked on the dead-letter queue tagged `atomic-apply-failure`. |
| `dlq_orphan` | `LatticeReplicationMetrics.OutcomeTxDlqOrphan` | The orphan-sweep maintenance pass evicted a transaction whose admission age exceeded `TxBufferOrphanTimeout` because at least one sibling never arrived. Every staged entry of the orphan is parked on the dead-letter queue tagged `orphan-transaction`. |
| `evicted_capacity` | `LatticeReplicationMetrics.OutcomeTxEvictedCapacity` | A new admission would have exceeded `AtomicBatchBufferMaxTransactions` or `AtomicBatchBufferMaxBytes`, so the buffer evicted the FIFO-oldest transaction to admit the new one. The displaced transaction's staged entries are parked on the dead-letter queue. |

Two carve-outs preserve the counter's "every admitted transaction reaches exactly one terminal outcome" contract:

- **Cancellation does not increment.** A saga that throws `OperationCanceledException` (host shutdown, transport cancellation, explicit operator stop) leaves the transaction staged for the next pump tick. The counter is only stamped on a genuine terminal disposition.
- **Partial admission does not increment.** A transaction whose `BatchSize` is `5` but only `3` entries have arrived stays in the buffer without contributing to the counter. Only the entry that completes the batch (or the eviction that displaces it before completion) emits the sample.

Activation rehydration walks the durable system tree and reconstructs the in-memory index without emitting any signal — the rehydrated transactions resume their pre-restart admission lifecycle and only emit `tx_completed` when they reach their next terminal disposition.

A receiver running healthy steady-state atomic-batch traffic shows `success` dominating; a sustained `dlq_apply_failure` rise indicates a deterministic saga-side fault (operators inspect the `atomic-apply-failure` DLQ entries); a sustained `dlq_orphan` rise indicates the producer is dropping siblings mid-batch (operators inspect the producer-side ship loop and partition routing); a sustained `evicted_capacity` rise indicates the buffer is undersized for the workload (operators tune `AtomicBatchBufferMaxTransactions` / `AtomicBatchBufferMaxBytes`).

## Subscribing

Wire `LatticeReplicationMetrics.MeterName` into an OpenTelemetry `MeterProviderBuilder.AddMeter(...)` call, or attach a `MeterListener` directly:

```text
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

Four instruments surface the receiver-side causal-apply buffer (`CausalApplyBuffer`) used by the causal-plus dependency check. They share the meter and tag conventions of the rest of the package — `tree` always identifies the logical tree, and `shard` is reserved as a second tag dimension on the buffer-state instruments so a future per-shard buffer partitioning can populate it without a wire-format break. The current implementation is one-buffer-per-tree, so `shard` is always `"0"`.

| Instrument | Kind | Tags | Recorded when |
|---|---|---|---|
| `orleans.lattice.replication.apply.buffered_entries` | `UpDownCounter<long>` | `tree`, `shard` | Increments by 1 on every successful park (including overflow-evicting parks); decrements by 1 per evicted entry inside the same park; decrements by the count of drained entries on each successful drain pass. |
| `orleans.lattice.replication.apply.buffer_bytes` | `UpDownCounter<long>` | `tree`, `shard` | Tracks the same lifecycle as `buffered_entries` but in cumulative serialised bytes (key length × 2 + end-key length × 2 + value length + 128 envelope overhead). |
| `orleans.lattice.replication.apply.dependency_wait_ms` | `Histogram<double>` (ms) | `tree` | One sample per drained entry: `now - parked_at`, clamped non-negative. Evicted entries do not contribute — only successful waits are observed. |
| `orleans.lattice.replication.apply.causal_violations_blocked` | `Counter<long>` | `tree` | Incremented once per successful park. Duplicate-tuple parks do not count. An alert on `rate > 0` flags causal-skew health regardless of whether buffered entries eventually drain or evict. |

Operators monitor the four together:

- A steady-state replicating peer keeps `buffered_entries` near zero and emits `dependency_wait_ms` samples close to the round-trip-time of a single ack cycle.
- A persistent rise in `buffered_entries` or `buffer_bytes` paired with a low or zero `causal_violations_blocked` rate is the classic "bounded buffer absorbing transient skew, then draining" pattern — healthy.
- A sustained nonzero `causal_violations_blocked` rate paired with `apply.dependency_wait_ms` distributions in the seconds-to-minutes range indicates structural causal skew the bounded buffer is masking; pair with the DLQ enqueue rate (`dead_letter.enqueued{reason="hlc_skew"}`) to detect overflow.
- A sudden buffer drain that does not advance the local high-water-mark (visible as a spike in `dependency_wait_ms` with no matching `apply.lag` improvement) suggests a CRDT-merge regression rather than a transport-side issue.

## Per-origin FIFO invariant (`apply.fifo_violations`)

The receiver-side apply pipeline relies on a per-origin FIFO contract for its causal-apply buffer's occupancy bounds: under correct sender + transport behaviour, the producer's partitioned change feed yields per-shard in WAL-offset order and each shard's WAL is HLC-monotonic per origin, so per-`(origin, shard)` FIFO is preserved end-to-end with **no cross-shard sender serialisation** (which would defeat the whole point of partitioned replog scaling).

| Property | Value |
|---|---|
| Name | `orleans.lattice.replication.apply.fifo_violations` |
| Unit | `{entry}` |
| Tags | `tree`, `origin` |

The canonical `ReplicationApplier` records the most recently applied source HLC per `(treeId, originClusterId)` in process-local memory and increments `apply.fifo_violations` when a successfully applied entry's HLC is **strictly less** than the prior recorded value for the same pair. The counter is recorded:

- **After a successful apply** (direct or drained from the causal-apply buffer) — never on park. The invariant tracks "what has been merged" rather than "what has been observed", so a transient park of a higher-HLC entry that drains after a lower-HLC arrival does not falsely register a violation.
- **For point operations only** (`Set` / `Delete`). `DeleteRange` carries `HybridLogicalClock.Zero` by design and is excluded — it neither records a violation nor overwrites the recorded HLC.

A violation **does not change apply behaviour**: the entry is still applied, the HWM is still advanced. This is purely an observability surface — an alert on `rate > 0` flags a transport-side regression that broke the per-origin order, not a correctness defect on the receiver. Operators triage by joining the `tree` and `origin` tags against the producer-side topology to identify which sender path regressed.

Cross-shard interleaving for the same origin is permitted by design and is **not** a FIFO violation under this contract: entries that have a genuine cross-shard causal dependency carry it in their `VectorClock` and route through the causal-apply buffer's dependency-check path instead. The current implementation tracks one entry per `(tree, origin)` because the canonical applier is one-instance-per-tree; a future per-shard applier partitioning will key the tracker by `(tree, shard, origin)` without changing the metric's tag dimensionality.