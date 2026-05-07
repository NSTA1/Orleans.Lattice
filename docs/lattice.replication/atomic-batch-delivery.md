# Cross-cluster atomic batch delivery

`Orleans.Lattice.Replication` ships an **opt-in receiver-side staging pipeline** that delivers
multi-key writes authored via `SetManyAtomicAsync` as a unit on every cluster. With the opt-in
on, a remote reader concurrent with replication never observes a state where some keys of an
atomic batch have arrived and others have not — the receiver buffers every entry until the
whole batch is in hand and then applies it under one saga, advancing the per-origin
high-water-mark **once** at completion.

The opt-in is per-tree, default `false`. With it off the receiver applies each entry as a
point write and concurrent readers may observe a partial view until the batch finishes
converging — the causal+ default contract. This document covers the opt-in surface, the
operator-facing knobs and observability, and the recovery playbook for the four terminal
disposition paths a buffered batch can reach.

For the architectural design see [`wal-causal-plus.md`](wal-causal-plus.md) §7.3
(blocked-floor producer-side GC pin) and §12.1 (completeness coverage matrix). For the
end-to-end consistency contract across local + replicated paths see
[`../lattice/consistency.md`](../lattice/consistency.md).

---

## Contract

### Per-tree opt-in

```text
siloBuilder.AddLatticeReplication(opts =>
{
    opts.ClusterId = "site-a";
    opts.ReplicatedTrees = new Dictionary<string, ReplicationMode>
    {
        ["orders"] = ReplicationMode.LwwRegister,
    };
    opts.AtomicBatchDelivery = true;        // opt the default tree in
});
```

Per-tree overrides follow the same `IOptionsMonitor<LatticeReplicationOptions>.Get(treeName)`
pattern as every other replication option:

```text
siloBuilder.Services.Configure<LatticeReplicationOptions>("orders", o =>
{
    o.AtomicBatchDelivery = true;
    o.AtomicBatchBufferMaxTransactions = 256;
    o.TxBufferOrphanTimeout = TimeSpan.FromMinutes(2);
});
```

Producer-side stamping of `AtomicBatchSize` / `AtomicBatchIndex` on every emitted entry is
**unconditional** — a producer never needs to restart to roll the opt-in forward to a peer,
and a peer rolling the opt-in backward immediately stops staging without losing in-flight
state (entries already buffered drain through the existing terminal paths described below).

### Atomic visibility guarantee

| Property | With `AtomicBatchDelivery = false` (default) | With `AtomicBatchDelivery = true` |
|---|---|---|
| Per-key apply | Each sibling applies independently as it arrives. | Every sibling buffers; the batch applies under one saga on completion. |
| Reader concurrent with replication | May observe `K of N` siblings applied for any `K < N`. | Never observes partial state — every sibling becomes visible on the same saga commit boundary. |
| Per-origin high-water-mark advance | Per-entry, in HLC order. | **Once at batch completion**, to `max(entry.Timestamp)` across the batch. |
| Worst-case batch latency | Sum of per-entry replication-window RTTs. | `max` per-entry replication-window RTT plus saga commit time. |

The guarantee is **scoped to a single tree**. A `SetManyAtomicAsync` call against tree `T`
becomes one transaction id; cross-tree atomic visibility (one user-authored batch touching
`T1` and `T2`) is out of scope and is not delivered by enabling this option.

### Latency vs. visibility trade-off

The opt-in trades two things for the stronger guarantee:

1. **Tail latency on the receiver.** A batch's terminal disposition is bounded by its slowest
   sibling's arrival time, not its first sibling's. Healthy steady-state replication keeps
   this at the per-entry RTT plus the saga commit overhead; a slow re-delivery on one
   sibling stalls the whole batch's apply until the orphan timeout expires.
2. **Producer-side WAL-GC pin.** While a batch is partially staged on any receiver, the
   producer's WAL trim frontier is held at the lowest staged HLC across consumers (see
   `wal-causal-plus.md` §7.3). A receiver buffer that holds a batch for 30 s pins the
   producer's WAL through that 30 s window even though every other downstream consumer has
   acknowledged past it.

### Carve-out: real-time receiver-side reader isolation

Cross-cluster atomic-batch delivery pins the **inbound apply boundary**: every key of an
incoming atomic batch becomes visible together. Real-time reader isolation **inside** the
receiver-side saga commit window — a reader concurrent with the per-key sequential commit
that the saga issues to its leaf grains — remains an open carve-out, identical in shape to
the equivalent local-saga reader-isolation carve-out documented in [`../lattice/consistency.md`](../lattice/consistency.md).
Until that primitive ships, applications that need strict reader isolation under concurrent
atomic-batch apply should layer `GetWithVersionAsync` + `SetIfVersionAsync` on top,
exactly as for local sagas.

---

## Knobs

| Option | Default | Validator | Meaning |
|---|---|---|---|
| `AtomicBatchDelivery` | `false` | none | Per-tree opt-in. `true` enables receiver-side buffering and atomic apply on completion; `false` applies each entry as a point write. |
| `AtomicBatchBufferMaxTransactions` | `512` | `>= 1` | Maximum distinct in-flight `(originClusterId, transactionId)` keys the per-tree staging buffer admits before evicting the oldest partially-buffered transaction (FIFO). Bounds working-set cardinality, not entry count. Eviction routes the displaced batch to the per-tree DLQ tagged `evicted`. |
| `AtomicBatchBufferMaxBytes` | 64 MB | `>= 1 MB` | Soft cap on cumulative buffered payload bytes. Approximate (read from `entry.Value.Length` plus a small overhead). A single entry larger than the cap is admitted as-is rather than evicting the buffer. |
| `TxBufferOrphanTimeout` | 5 min | `> TimeSpan.Zero` | Maximum residency time of a partially-buffered transaction before the maintenance grain sweeps it. Sweep cadence is half the WAL `MaintenanceGcInterval`. Eviction routes every staged entry to the DLQ tagged `orphan-transaction`, advances the per-origin HWM past the orphan's max HLC, and clears the producer-side blocked-floor pin. |
| `SnapshotSagaQuiesceTimeout` | 30 s | `> TimeSpan.Zero` | Producer-side wall-clock window during which `ISnapshotProvider.ExportAsync` waits for in-flight atomic sagas to finish emitting before reading tree state. Sagas exceeding the timeout are stamped on `SnapshotStream.SagaBlacklist` and degraded to causal+ on the bootstrapping receiver (atomic visibility carve-out for those specific sagas only). |

The four staging knobs are independent of each other; the snapshot quiesce window only
applies during a producer-driven snapshot export.

---

## Observability

Every receiver-side instrument lives on the `orleans.lattice.replication` meter and is
fully documented under [`observability.md`](observability.md). Briefly:

| Instrument | Kind | When |
|---|---|---|
| `apply.tx_buffered` | UpDownCounter | `+1` when the first entry of a new `(originClusterId, transactionId)` is staged; `-1` on any terminal removal. Tracks live admission lifecycle, not durable rehydrated occupancy. |
| `apply.tx_buffer_bytes` | UpDownCounter | Cumulative staged-payload bytes; per-entry granularity. Drives a future health-probe integration. |
| `apply.tx_apply_duration_ms` | Histogram | Sampled `now - min(staged.EnqueuedAtTicks across the completed batch)`, recorded **once** per terminal apply outcome (every entry shares the same sample). The single most operationally-important atomic-batch instrument. Tagged `tree` + `outcome`. |
| `apply.tx_completed` | Counter | One increment per terminal disposition. Tagged `tree` + `outcome`. |

The `outcome` tag partitions both the histogram and the counter into mutually-exclusive
terminal buckets; the per-tree sum across buckets equals the total transactions reaching a
terminal state. The histogram is intentionally not recorded for the two non-apply paths
(`dlq_orphan`, `evicted_capacity`); the counter still emits, so terminal accounting stays
balanced.

Two reason-tag constants on `LatticeReplicationMetrics` partition DLQ enqueues for atomic
batches: `ReasonOrphanTransaction = "orphan-transaction"` and
`ReasonAtomicApplyFailure = "atomic-apply-failure"`. Both are described in
[`dead-letter-queue.md`](dead-letter-queue.md).

---

## Operator playbook

A buffered batch reaches one of four terminal dispositions. Each has its own recovery shape.

### `success` — atomic apply committed

The saga committed every entry; the per-origin HWM advanced once to the batch's max HLC.
No operator action required. This is the steady-state happy path and accounts for every
atomic-batch sample in healthy production traffic.

### `dlq_apply_failure` — saga returned `Compensated` or threw

The receiver-side saga (`IReplicationApplyGrain.ApplyManyAtomicAsync`) returned
`AtomicApplyOutcome.Compensated` or threw a non-cancellation exception. Every staged entry
is parked on the per-tree DLQ tagged `atomic-apply-failure`; the per-origin HWM is held
unchanged so the producer continues to re-ship the batch on its next pump cycle.

**Recovery**: inspect the parked rows via `ILatticeReplicationDeadLetters.ListAsync`,
filter for entries whose `Entry.TransactionId` matches the failing batch, and either:

1. **Discard** every row of the transaction if the failure is deterministic and the data
   is recoverable from upstream — replay would re-park indefinitely.
2. **Replay** every row of the transaction in `Entry.AtomicBatchIndex` order. Replay
   bypasses the staging buffer (entries route through the canonical applier as point
   writes), so atomic visibility is **degraded to causal+** for the recovered batch.
   Acceptable when the alternative is permanent data loss.

```text
ILatticeReplicationDeadLetters dlq =
    serviceProvider.GetRequiredService<ILatticeReplicationDeadLetters>();

IReadOnlyList<DeadLetterEntry> parked = await dlq.ListAsync("orders", cancellationToken);

Guid txid = parked[0].Entry.TransactionId;
IEnumerable<DeadLetterEntry> batch = parked
    .Where(p => p.Entry.TransactionId == txid)
    .OrderBy(p => p.Entry.AtomicBatchIndex);

foreach (DeadLetterEntry row in batch)
{
    await dlq.ReplayAsync("orders", row.EntryId, cancellationToken);
}
```

A persistently-failing replay leaves the row parked. Inspect the saga's logged exception
on the receiver to determine whether the payload is malformed (DLQ tag would be
`schema`) or whether the failure is transport-shaped.

### `dlq_orphan` — partial batch exceeded `TxBufferOrphanTimeout`

A partially-staged transaction held the buffer past the orphan timeout. The per-tree
maintenance grain swept it: every staged entry is parked on the DLQ tagged
`orphan-transaction`; the per-origin HWM is **advanced** past the orphan's max HLC; the
producer-side blocked-floor pin is cleared.

This is the recovery path for transport-side data loss (a sibling never arrived) or
producer-side mid-saga crash (the producer never emitted every sibling because it died
between the persisted intent and the final commit). Recovery shape is identical to
`dlq_apply_failure`'s replay sequence — but because the HWM has already advanced past the
orphan, replay through the canonical applier is filtered as a re-delivery
(`Applied = false`) and the parked rows are removed without touching downstream state.

A persistently-occurring `dlq_orphan` outcome means transport-side losses are routine —
investigate the gRPC transport's reconnect / retry posture before raising
`TxBufferOrphanTimeout`. Raising the timeout prolongs the producer-side WAL-GC pin and
buffer occupancy without addressing the root cause.

### `evicted_capacity` — buffer cap exceeded

Either `AtomicBatchBufferMaxTransactions` or `AtomicBatchBufferMaxBytes` was reached and
the oldest partially-buffered transaction was evicted to make room for a new admission.
Every staged entry of the displaced batch is parked tagged `evicted`; the per-origin HWM is
held unchanged so the producer re-ships the original entries on the next pump cycle.

This outcome is a **capacity-tuning signal**, not a transport fault. Two responses:

1. **Raise the cap** when the operator-facing dashboards show steady-state buffer
   pressure under expected load (a misconfigured cap relative to the in-flight working
   set). `apply.tx_buffered` and `apply.tx_buffer_bytes` are the gauges to watch.
2. **Lower the inbound batch fan-in** when the cap is sized correctly but a producer
   burst exceeded the buffer's working set. Producer-side back-pressure is the right
   long-term fix; raising the cap on the receiver only delays the same problem.

### Snapshot-during-saga troubleshooting

`ISnapshotProvider.ExportAsync` quiesces in-flight atomic sagas before scanning. Any saga
that fails to drain within `SnapshotSagaQuiesceTimeout` is stamped on
`SnapshotStream.SagaBlacklist` and the receiver bypasses the staging buffer for entries
carrying a blacklisted transaction id — applying them as point writes instead. **Atomic
visibility is degraded to causal+ for those specific sagas only.**

A non-empty `SagaBlacklist` on a steady-state snapshot stream means routine snapshot
exports are racing routine atomic-batch sagas. Two responses:

1. **Raise `SnapshotSagaQuiesceTimeout`** to give the in-flight sagas more time to
   complete. Costs more wall-clock time per export.
2. **Reduce snapshot concurrency or schedule snapshots during low-write windows** so the
   quiesce window naturally drains.

Inspect the receiver's `BootstrapCoordinatorState.SagaBlacklist` row to see which sagas
were degraded; combine with producer-side saga-completion logs to confirm the racing
window.

---

## Capacity planning

Size `AtomicBatchBufferMaxBytes` against the worst-case in-flight working set the
receiver must hold, computed as:

```text
in-flight working set = concurrent in-flight batches
                      × average serialised payload per batch
                      × replication-window RTT in seconds
                      × producer batch-emit rate per second
```

A 1000-batches-per-second producer pumping 16 KB average batches with a 200 ms
end-to-end replication-window RTT against 4 concurrent in-flight batches works out to:

```text
4 × 16 KB × 0.2 × 1000 = 12.8 MB of typical buffered footprint
```

Size `AtomicBatchBufferMaxBytes` at 4–8× the typical footprint to absorb a burst without
escalating to `evicted_capacity`. The default `64 MB` is sized for the high end of this
range under typical workloads; lower it on memory-constrained silos with steady-state
low concurrency, raise it for fan-in workloads with many concurrent producers per
receiver.

`AtomicBatchBufferMaxTransactions` sizes the **cardinality** of in-flight batches, not the
byte footprint. Default `512` accommodates a wide range of producer concurrencies. Raise
when producers run thousands of concurrent atomic sagas; lower only on highly
memory-constrained silos and only after confirming the per-batch byte size is small.

`TxBufferOrphanTimeout` sizes the **patience window** for slow-arriving siblings. Default
`5 min` absorbs routine transport hiccups (silo restart, transient gRPC reconnect, brief
network partition heal). Set it longer than the worst-case re-delivery latency in your
deployment, but **not** longer than is acceptable for the producer-side WAL-GC pin a
held buffer creates — a silent producer-side disk pressure incident is a worse outcome
than a `dlq_orphan` recovery path.
