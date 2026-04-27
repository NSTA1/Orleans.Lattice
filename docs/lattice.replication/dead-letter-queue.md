# Dead-letter queue (DLQ)

When the inbound apply pipeline cannot install a `ReplogEntry` after exhausting `LatticeReplicationOptions.MaxApplyRetries` consecutive attempts, the entry is *parked* on a per-tree dead-letter queue. Parking unblocks the apply stream so a single poison entry cannot stall the pipeline forever, while preserving the failed entry plus diagnostic context for an operator to triage, replay, or discard.

## Topology

```text
   transport.PushAsync(batch)
            |
            v
   IReplicationApplier -- decorator: DeadLetterTrackingReplicationApplier
            |              |-- inner.ApplyAsync (canonical ReplicationApplier)
            |              |-- on success -> clear failure counter
            |              \-- on failure -> increment counter
            |                                 |-- < MaxApplyRetries -> re-throw
            |                                 \-- >= MaxApplyRetries -> park + advance HWM + return Applied=false
            v
   IReplicationDeadLetterGrain "{treeId}"
            |
            v
   ISystemLattice "_lattice_replog_dlq_{treeId}"  (system tree, e/{19-padded-id} rows)
```

The decorator is registered as the silo-side `IReplicationApplier` singleton. Apply paths inside the cluster therefore go through the decorator transparently. Operator inspection and replay use the public `ILatticeReplicationDeadLetters` seam, which routes through the **canonical** `ReplicationApplier` so a deterministically-failing parked entry does not re-park itself on every replay.

## Storage

Parked entries live in a reserved system tree named `_lattice_replog_dlq_{treeId}` accessed through the internal `ISystemLattice` surface. Each row is keyed `e/{19-padded-id}` and holds an Orleans-binary-serialised `DeadLetterEntry`. The DLQ inherits the scaling, sharding, and persistence of the core B+ tree rather than living inside one grains persistent-state row, which would hit the storage row-size ceiling under sustained apply failure.

On activation the grain bulk-loads every parked row into an in-memory cache; subsequent reads (`List` / `Count` / `TryGet`) are served from memory and writes (`Enqueue` / `Discard` / `RemoveReplayed`) are applied to the cache and written through to the system tree. Cache size is bounded by `DeadLetterQueueCapacity` (validator pins to >= 1).

## FIFO eviction

When the queue is at capacity, a new enqueue evicts the oldest entry first (FIFO) and emits `dead_letter.removed{reason=evicted}` per evicted row before parking the new one.

## Configuration

```text
siloBuilder.AddLatticeReplication(opts =>
{
    opts.ClusterId = "site-a";
    opts.MaxApplyRetries = 5;            // default 5; >= 1
    opts.DeadLetterQueueCapacity = 1000; // default 1000; >= 1
});
```

| Option | Default | Meaning |
|---|---|---|
| `MaxApplyRetries` | `5` | Consecutive failed apply attempts on the same `(treeId, originClusterId, timestamp, key, op)` tuple before parking. |
| `DeadLetterQueueCapacity` | `1000` | Maximum parked entries per tree before FIFO eviction kicks in. |

## Inspection seam — `ILatticeReplicationDeadLetters`

Resolve the seam from DI and call per-tree:

| Method | Returns | Notes |
|---|---|---|
| `ListAsync(treeId, ct)` | `IReadOnlyList<DeadLetterEntry>` | Ascending entry-id order. Pure read. |
| `CountAsync(treeId, ct)` | `int` | Cached count, served from memory. |
| `DiscardAsync(treeId, entryId, ct)` | `bool` | `true` when removed; `false` when the id was unknown. Emits `reason=discarded`. |
| `ReplayAsync(treeId, entryId, ct)` | `ApplyResult?` | `null` when the id is unknown. Routes through the canonical applier (bypasses the decorators failure tracker). On any non-throwing return — including HWM-filtered re-delivery — the entry is removed with `reason=replayed`. A thrown exception leaves the entry parked. |

```text
var dlq = serviceProvider.GetRequiredService<ILatticeReplicationDeadLetters>();
var parked = await dlq.ListAsync("orders", cancellationToken);
foreach (var parkedEntry in parked)
{
    Console.WriteLine(
        $"entry={parkedEntry.EntryId} key={parkedEntry.Entry.Key} reason={parkedEntry.FailureReason} retries={parkedEntry.RetryCount}");
}

if (parked.Count > 0)
{
    var result = await dlq.ReplayAsync("orders", parked[0].EntryId, cancellationToken);
    // result is null when the id is unknown; otherwise the replay routed
    // through the canonical applier and the entry is removed.
}
```

## High-water-mark interaction

Parking advances the per-origin HWM (`{treeId}/{originClusterId}`) past the parked entrys HLC for *point* operations (`Set` / `Delete`). The canonical appliers HWM filter then dedupes future re-deliveries from the transport, so a transport that re-ships the parked entry observes `Applied=false` at the `ReplicationApplier` layer without re-engaging the failure tracker.

`DeleteRange` entries skip HWM advance because the canonical applier does not consult the HWM for range deletes (range applies are naturally idempotent at the leaf layer). The entry is still parked.

## Replay semantics

`ReplayAsync` deliberately routes through the **canonical** `ReplicationApplier`, not the decorator. Three reasons:

1. A parked entry that failed deterministically would re-park itself on every replay if routed through the decorator, which would produce an infinite re-park loop and corrupt the failure-counter state for that tuple.
2. Operators are explicitly opting into a "this entry might still apply" attempt; the failure budget is logically a transport-level concern, not an operator-replay concern.
3. The HWM is already at or past the parked entrys HLC (the parking step advanced it), so the canonical applier reports a filtered re-delivery (`Applied=false`) without touching downstream state. The seam treats that as terminal-for-cleanup and removes the parked row.

A throwing replay leaves the entry parked. The operator can re-attempt or `Discard`.

## Metrics

Two counters on the `orleans.lattice.replication` meter, both tagged with `tree` and `reason`:

| Instrument | Tags | Meaning |
|---|---|---|
| `orleans.lattice.replication.dead_letter.enqueued` | `tree`, `reason=unknown` | Replog entry parked. The `unknown` bucket is a placeholder pending later enqueue-cause classification. |
| `orleans.lattice.replication.dead_letter.removed` | `tree`, `reason in { discarded, replayed, evicted }` | Entry removed. `discarded` = explicit operator call; `replayed` = removed after `ReplayAsync` completed; `evicted` = FIFO capacity eviction during a later enqueue. |

## Persistence and rehydration

The grain bulk-loads its parked rows from the system tree on every activation. Operators can therefore deactivate or restart the silo and parked entries reappear with their original `EntryId` values intact — `_nextEntryId` is recomputed as `max(stored entry-id) + 1` so subsequent enqueues are still monotonic.

## When to discard vs. replay

- **Discard** when you have validated the underlying data fault and deliberately want to drop the entry (e.g. it carries a key your tree no longer participates in). Emits `reason=discarded`.
- **Replay** when you have fixed the upstream cause of the apply failure (config drift, schema mismatch, transient infra fault) and want the entry back in the apply path. Emits `reason=replayed`. Note that for point operations the HWM has already advanced past the entry; the replay surfaces this as `Applied=false`, which is terminal for inspection — the entry is still removed.

