# Observability

`Orleans.Lattice.Replication` publishes every instrument on a single meter, `orleans.lattice.replication`. An OpenTelemetry pipeline (or any `MeterListener`) subscribes once and receives every replication metric. The instruments fall into four shapes:

- **Per-peer gauges** — `entries_behind`, `bytes_behind`, `consecutive_errors`, `last_contact_seconds`. Owned by `ReplicationPeerStats`. Tagged `tree` + `peer`.
- **Per-operation histograms** — `ship.duration`, `apply.duration`, `apply.lag`. Reported in milliseconds.
- **Throughput counters** — `wal.entries_appended`, `wal.entries_shipped`, `wal.entries_trimmed`. Used to compute growth-rate vs. ship-rate ratios.
- **DLQ counters** — `dead_letter.enqueued`, `dead_letter.removed`. Tagged `tree` + `reason`.

## Replication-lag histogram (`apply.lag`)

`orleans.lattice.replication.apply.lag` is recorded by the canonical `ReplicationApplier` immediately after a successful point apply (`Set` / `Delete`). The sample is `now - entry.Timestamp.WallClockTicks` in milliseconds, **clamped to a non-negative value** so a future-dated source HLC (e.g. a faster-moving peer''s wall clock) reports as `0` rather than corrupting the histogram with a negative sample.

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

## Growth-rate vs. ship-rate (`wal.entries_appended` / `wal.entries_shipped`)

The two counters are deliberately a pair:

| Counter | Tags | Recorded |
|---|---|---|
| `orleans.lattice.replication.wal.entries_appended` | `tree` | After a successful WAL append at the `ShardedReplogSink` seam — counts entries the producer durably committed to the local WAL. A throwing append does **not** contribute. |
| `orleans.lattice.replication.wal.entries_shipped` | `tree`, `peer` | After a successful Push acknowledgement at the gRPC transport. Incremented by the count of entries inside the acknowledged envelope; a heartbeat / keep-alive (zero-entry) batch contributes zero. |

Operators monitor `rate(wal_entries_appended) / rate(wal_entries_shipped)` per tree-peer pair. Steady-state replication keeps the ratio close to `1`. A persistently rising ratio indicates the local WAL is growing faster than the sender can ship, which is the signal R-061''s GC predicate and a future health check both consume.

## DLQ enqueue-reason classification

`orleans.lattice.replication.dead_letter.enqueued` is tagged with one of four canonical reason values:

| Value | When |
|---|---|
| `schema` | The terminal failure was an `ArgumentException` (malformed entry, missing field, range delete with no end key) or an `InvalidOperationException` (unrecognised `ReplicationMode`, state-merge CAS budget exhausted). The receiver classifies these as payload-shape faults. |
| `hlc_skew` | Reserved. Future receiver decorators that surface implausible HLC skew between the receiver''s wall clock and the entry''s `Timestamp` as a classified exception will tag this value. |
| `oversized` | Reserved. Future receiver decorators that wrap the canonical applier with a size-validating check will tag this value when a single entry exceeds the configured per-entry size ceiling. |
| `unknown` | Catch-all for terminal failure shapes the canonical decorator could not classify (e.g. transport / IO / `TimeoutException`). |

The mapping lives in `DeadLetterTrackingReplicationApplier.ClassifyFailure` and is intentionally conservative: only failure shapes whose source is under the package''s control are matched explicitly, so the `reason` dimension stays stable across publishers and operators can alert on `unknown` rising without false positives from future schema-shape additions.

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