# Observability

`Orleans.Lattice.Replication` publishes every replication-side instrument on a single meter, `orleans.lattice.replication`. An OpenTelemetry pipeline (or any `MeterListener`) subscribes once and receives every replication metric. The instruments fall into four shapes:

- **Per-peer gauges** - `entries_behind`, `bytes_behind`, `consecutive_errors`, `last_contact_seconds`. Owned by `ReplicationPeerStats`. Tagged `tree` + `peer`.
- **Per-operation histograms** - `ship.duration`, `apply.duration`, `apply.lag`. Reported in milliseconds.
- **Throughput counters** - `wal.entries_appended`, `wal.entries_shipped`. Used to compute growth-rate vs. ship-rate ratios. The companion `wal.entries_trimmed` counter belongs to the core library and is published on the `orleans.lattice` meter (`LatticeMetrics.WalEntriesTrimmed`); subscribe to both meters when correlating ship-rate against trim-rate.
- **DLQ counters** - `dead_letter.enqueued`, `dead_letter.removed`. Tagged `tree` + `reason`.

## Replication-lag histogram (`apply.lag`)

`orleans.lattice.replication.apply.lag` is recorded by the canonical `ReplicationApplier` immediately after a successful point apply (`Set` / `Delete`). The sample is `now - entry.Timestamp.WallClockTicks` in milliseconds, **clamped to a non-negative value** so a future-dated source HLC (e.g. a faster-moving peer's wall clock) reports as `0` rather than corrupting the histogram with a negative sample.

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

`orleans.lattice.replication.apply.duration` records the wall-clock time the canonical `ReplicationApplier` spends inside `ApplyAsync`, from entry through every terminal return path. The body is wrapped in a `try { ... } finally { Record(...); }` so an uncaught exception still records a sample tagged with the failure outcome before unwinding. The duration is read via `Stopwatch.GetElapsedTime(long)`, which is allocation-free.

| Property | Value |
|---|---|
| Name | `orleans.lattice.replication.apply.duration` |
| Unit | `ms` |
| Tags | `tree`, `peer`, `outcome` |

The `peer` tag carries the same value as `apply.lag`'s `peer` tag - the entry's `OriginClusterId`, identifying the authoring cluster rather than the transport hop. The batch path's `ApplyOriginRunAsync` groups entries into contiguous same-`(treeId, originClusterId)` runs and records each per-entry duration with the run's shared `peer` value, so multi-origin batches surface as one `peer` per run rather than collapsing into a single dominant value.

The `outcome` tag partitions the histogram into four mutually-exclusive buckets:

| Value | Constant | When |
|---|---|---|
| `success` | `LatticeReplicationMetrics.OutcomeSuccess` | The entry was applied successfully - both directly applied point operations (`Set` / `Delete`) and range deletes contribute. Each `ApplyAsync` invocation records exactly one `apply.duration` sample regardless of how many entries the call drains from the causal-apply buffer: a drain cascade triggered by an arriving satisfier contributes its drained-entry work to the satisfier's own `success` sample, and the originally parked entries do not generate additional samples on drain. |
| `dedup` | `LatticeReplicationMetrics.OutcomeDedup` | The entry was short-circuited before merge - either the per-origin high-water-mark already covers `entry.Timestamp`, or the local-origin defence-in-depth gate detected an entry that must not loop back onto its authoring cluster. |
| `failure` | `LatticeReplicationMetrics.OutcomeFailure` | The apply attempt threw. Recorded in the `finally` path before the exception unwinds. Includes payload-shape faults (`ArgumentException`, `InvalidOperationException`), `OperationCanceledException` from a cancelled `cancellationToken` (graceful shutdown traffic appears here), transport / IO failures, and any other unhandled exception out of the apply pipeline. |
| `parked-causal-buffer` | `LatticeReplicationMetrics.OutcomeParkedCausalBuffer` | The entry parked on the causal-apply buffer because its declared `VectorClock` was not yet dominated by the local vector clock. The original delivery did not advance the high-water-mark; the entry re-enters the apply pipeline through the buffer drain when its dependencies arrive. |

A receiver with a single overwhelmed subscriber surfaces as a rising `failure` bucket; a receiver with persistent causal skew surfaces as a rising `parked-causal-buffer` bucket. Both are independent of `apply.lag`, which only samples successful merges.

## Growth-rate vs. ship-rate (`wal.entries_appended` / `wal.entries_shipped`)

The two counters are deliberately a pair:

| Counter | Tags | Recorded |
|---|---|---|
| `orleans.lattice.replication.wal.entries_appended` | `tree` | After a successful WAL append at the `ShardedReplogSink` seam - counts entries the producer durably committed to the local WAL. A throwing append does **not** contribute. |
| `orleans.lattice.replication.wal.entries_shipped` | `tree`, `peer` | After a successful Push acknowledgement at the gRPC transport. Incremented by the count of entries inside the acknowledged envelope; a heartbeat / keep-alive (zero-entry) batch contributes zero. |

Operators monitor `rate(wal_entries_appended) / rate(wal_entries_shipped)` per tree-peer pair. Steady-state replication keeps the ratio close to `1`. A persistently rising ratio indicates the local WAL is growing faster than the sender can ship, which is the signal the min-acked-cursor WAL GC predicate and a future health check both consume.

## DLQ enqueue-reason classification

`orleans.lattice.replication.dead_letter.enqueued` is tagged with one of four canonical reason values:

| Value | When |
|---|---|
| `schema` | The terminal failure was an `ArgumentException` (malformed entry, missing field, range delete with no end key) or an `InvalidOperationException` (unrecognised `LatticeMergeMode`, state-merge CAS budget exhausted). The receiver classifies these as payload-shape faults. |
| `hlc_skew` | Reserved. Future receiver decorators that surface implausible HLC skew between the receiver's wall clock and the entry's `Timestamp` as a classified exception will tag this value. |
| `oversized` | Reserved. Future receiver decorators that wrap the canonical applier with a size-validating check will tag this value when a single entry exceeds the configured per-entry size ceiling. |
| `unknown` | Catch-all for terminal failure shapes the canonical decorator could not classify (e.g. transport / IO / `TimeoutException`). |

The mapping lives in `DeadLetterTrackingReplicationApplier.ClassifyFailure` and is intentionally conservative: only failure shapes whose source is under the package's control are matched explicitly, so the `reason` dimension stays stable across publishers and operators can alert on `unknown` rising without false positives from future schema-shape additions.

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

Four instruments surface the receiver-side causal-apply buffer (`CausalApplyBuffer`) used by the causal-plus dependency check. They share the meter and tag conventions of the rest of the package - `tree` always identifies the logical tree, and `shard` is reserved as a second tag dimension on the buffer-state instruments so a future per-shard buffer partitioning can populate it without a wire-format break. The current implementation is one-buffer-per-tree, so `shard` is always `"0"`.

| Instrument | Kind | Tags | Recorded when |
|---|---|---|---|
| `orleans.lattice.replication.apply.buffered_entries` | `UpDownCounter<long>` | `tree`, `shard` | Increments by 1 on every successful park (including overflow-evicting parks); decrements by 1 per evicted entry inside the same park; decrements by the count of drained entries on each successful drain pass. |
| `orleans.lattice.replication.apply.buffer_bytes` | `UpDownCounter<long>` | `tree`, `shard` | Tracks the same lifecycle as `buffered_entries` but in cumulative serialised bytes (key length × 2 + end-key length × 2 + value length + 128 envelope overhead). |
| `orleans.lattice.replication.apply.dependency_wait_ms` | `Histogram<double>` (ms) | `tree` | One sample per drained entry: `now - parked_at`, clamped non-negative. Evicted entries do not contribute - only successful waits are observed. |
| `orleans.lattice.replication.apply.causal_violations_blocked` | `Counter<long>` | `tree` | Incremented once per successful park. Duplicate-tuple parks do not count. An alert on `rate > 0` flags causal-skew health regardless of whether buffered entries eventually drain or evict. |

Operators monitor the four together:

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

The canonical `ReplicationApplier` records the most recently applied source HLC per `(treeId, originClusterId)` in process-local memory and increments `apply.fifo_violations` when a successfully applied entry's HLC is **strictly less** than the prior recorded value for the same pair. The counter is recorded:

- **After a successful apply** (direct or drained from the causal-apply buffer) - never on park. The invariant tracks "what has been merged" rather than "what has been observed", so a transient park of a higher-HLC entry that drains after a lower-HLC arrival does not falsely register a violation.
- **For point operations only** (`Set` / `Delete`). `DeleteRange` carries `HybridLogicalClock.Zero` by design and is excluded - it neither records a violation nor overwrites the recorded HLC.

A violation **does not change apply behaviour**: the entry is still applied, the HWM is still advanced. This is purely an observability surface - an alert on `rate > 0` flags a transport-side regression that broke the per-origin order, not a correctness defect on the receiver. Operators triage by joining the `tree` and `origin` tags against the producer-side topology to identify which sender path regressed.

Cross-shard interleaving for the same origin is permitted by design and is **not** a FIFO violation under this contract: entries that have a genuine cross-shard causal dependency carry it in their `VectorClock` and route through the causal-apply buffer's dependency-check path instead. The current implementation tracks one entry per `(tree, origin)` because the canonical applier is one-instance-per-tree; a future per-shard applier partitioning will key the tracker by `(tree, shard, origin)` without changing the metric's tag dimensionality.

## Bootstrap instruments

The receiver-side bootstrap coordinator (`LatticeBootstrapCoordinatorGrain`) emits three instruments tracking the cross-cluster snapshot-drain pipeline plus a structured phase-transition log line. Together they let an operator dashboard the lifecycle of an in-flight bootstrap and tail a single run end-to-end through the silo log.

| Instrument | Kind | Tags | Recorded when |
|---|---|---|---|
| `orleans.lattice.replication.bootstrap.entries_received` | `Counter<long>` | `tree`, `origin` | Incremented by 1 per snapshot entry successfully applied through the local replication applier (post-decorator chain). |
| `orleans.lattice.replication.bootstrap.bytes_received` | `Counter<long>` (`By`) | `tree`, `origin` | Incremented by `entry.Value.Length` per applied entry. Mirrors the lifecycle of `entries_received`. |
| `orleans.lattice.replication.bootstrap.duration` | `Histogram<double>` (`ms`) | `tree`, `origin`, `outcome` | Recorded once per terminal phase transition. `outcome` is one of `live`, `failed`, or `timed_out`. |

The `origin` tag carries the source cluster id supplied at kickoff (`BootstrapAsync(sourceClusterId, ...)`), matching the tag dimensionality used by the per-origin fall-off-the-log counters so dashboards can join the two without a separate keying.

The histogram's `outcome` values are exposed as `LatticeReplicationMetrics.BootstrapOutcomeLive`, `BootstrapOutcomeFailed`, and `BootstrapOutcomeTimedOut` constants. The `timed_out` value is reserved for a future transport-timeout policy; the in-tree coordinator emits only `live` and `failed` today, but the constant is published so dashboard rules referencing it remain valid across future releases.

The duration timer is anchored on a per-activation in-memory stopwatch captured at kickoff (or lazy-initialised on the first drain pass after a silo failover). It records `Stopwatch.GetElapsedTime` from that anchor to the terminal transition; a silo failover between kickoff and completion therefore truncates the measured interval to the span since the most recent reactivation. Operators monitoring cross-failover total durations should pair the histogram with the per-entry counters, which are restartable across reactivations.

Phase-transition structured logs are emitted at `LogLevel.Information` from `LatticeBootstrapCoordinatorGrain` with the message template

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
