# Dead-letter queue (DLQ)

When the inbound apply pipeline cannot install a `WalRecord` after exhausting `LatticeReplicationOptions.MaxApplyRetries` consecutive attempts, the entry is *parked* on a per-tree dead-letter queue. Parking unblocks the apply stream so a single poison entry cannot stall the pipeline forever, while preserving the failed entry plus diagnostic context for an operator to triage, replay, or discard.

## Topology

```text
   transport.PushAsync(batch)
            |
            v
   IReplicationApplier -- dead-letter-tracking decorator
            |              |-- inner.ApplyAsync (canonical applier)
            |              |-- on success -> clear failure counter
            |              \-- on failure -> increment counter
            |                                 |-- < MaxApplyRetries -> re-throw
            |                                 \-- >= MaxApplyRetries -> park + advance HWM + return Applied=false
            v
   per-tree dead-letter store "{treeId}"
            |
            v
   ISystemLattice "_lattice_replog_dlq_{treeId}"  (system tree, e/{19-padded-id} rows)
```

The decorator is registered as the silo-side `IReplicationApplier` singleton. Apply paths inside the cluster therefore go through the decorator transparently. Operator inspection and replay use the public `ILatticeReplicationDeadLetters` seam, which routes through the **canonical** applier so a deterministically-failing parked entry does not re-park itself on every replay.

## Storage

Parked entries live in a reserved system tree named `_lattice_replog_dlq_{treeId}` accessed through the internal `ISystemLattice` surface. Each row is keyed `e/{19-padded-id}` and holds an Orleans-binary-serialised `DeadLetterEntry`. The DLQ inherits the scaling, sharding, and persistence of the core B+ tree rather than living inside one grains persistent-state row, which would hit the storage row-size ceiling under sustained apply failure.

On activation the grain bulk-loads every parked row into an in-memory cache; subsequent reads (`List` / `Count` / `TryGet`) are served from memory and writes (`Enqueue` / `Discard` / `RemoveReplayed`) are applied to the cache and written through to the system tree. Cache size is bounded by `DeadLetterQueueCapacity` (validator pins to >= 1).

## FIFO eviction

When the queue is at capacity, a new enqueue evicts the oldest entry first (FIFO) and emits `dead_letter.removed{reason=evicted}` per evicted row before parking the new one.

## Configuration

```csharp verify
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

## Inspection seam - `ILatticeReplicationDeadLetters`

Resolve the seam from DI and call per-tree:

| Method | Returns | Notes |
|---|---|---|
| `ListAsync(treeId, ct)` | `IReadOnlyList<DeadLetterEntry>` | Ascending entry-id order. Pure read. |
| `CountAsync(treeId, ct)` | `int` | Cached count, served from memory. |
| `DiscardAsync(treeId, entryId, ct)` | `bool` | `true` when removed; `false` when the id was unknown. Emits `reason=discarded`. |
| `ReplayAsync(treeId, entryId, ct)` | `ApplyResult?` | `null` when the id is unknown. Routes through the canonical applier (bypasses the decorators failure tracker). On any non-throwing return - including HWM-filtered re-delivery - the entry is removed with `reason=replayed`. A thrown exception leaves the entry parked. |

```csharp verify
var dlq = client.ServiceProvider.GetRequiredService<ILatticeReplicationDeadLetters>();
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

Parking advances the per-origin HWM (`{treeId}/{originClusterId}`) past the parked entrys HLC for *point* operations (`Set` / `Delete`). The canonical appliers HWM filter then dedupes future re-deliveries from the transport, so a transport that re-ships the parked entry observes `Applied=false` at the canonical applier layer without re-engaging the failure tracker.

`DeleteRange` entries skip HWM advance because the canonical applier does not consult the HWM for range deletes (range applies are naturally idempotent at the leaf layer). The entry is still parked.

## Replay semantics

`ReplayAsync` deliberately routes through the **canonical** applier, not the decorator. Three reasons:

1. A parked entry that failed deterministically would re-park itself on every replay if routed through the decorator, which would produce an infinite re-park loop and corrupt the failure-counter state for that tuple.
2. Operators are explicitly opting into a "this entry might still apply" attempt; the failure budget is logically a transport-level concern, not an operator-replay concern.
3. The HWM is already at or past the parked entrys HLC (the parking step advanced it), so the canonical applier reports a filtered re-delivery (`Applied=false`) without touching downstream state. The seam treats that as terminal-for-cleanup and removes the parked row.

A throwing replay leaves the entry parked. The operator can re-attempt or `Discard`.

## Metrics

Counters on the `orleans.lattice.replication` meter, both tagged with `tree` and `reason`:

| Instrument | Tags | Meaning |
|---|---|---|
| `orleans.lattice.replication.dead_letter.enqueued` | `tree`, `reason in { schema, hlc_skew, oversized, unknown }` | Replog entry parked. The dead-letter-tracking decorator classifies the terminal failure exception: `ArgumentException` and `InvalidOperationException` are tagged `schema` (malformed entry, missing field, unrecognised `LatticeMergeMode`, CAS-budget exhaustion); every other exception type lands on `unknown`. The `hlc_skew` and `oversized` reason values are reserved for future receiver decorators that surface size / clock-skew violations as classified exceptions. |
| `orleans.lattice.replication.dead_letter.removed` | `tree`, `reason in { discarded, replayed, evicted }` | Entry removed. `discarded` = explicit operator call; `replayed` = removed after `ReplayAsync` completed; `evicted` = FIFO capacity eviction during a later enqueue. |

## Persistence and rehydration

The grain bulk-loads its parked rows from the system tree on every activation. Operators can therefore deactivate or restart the silo and parked entries reappear with their original `EntryId` values intact - `_nextEntryId` is recomputed as `max(stored entry-id) + 1` so subsequent enqueues are still monotonic.

## When to discard vs. replay

- **Discard** when you have validated the underlying data fault and deliberately want to drop the entry (e.g. it carries a key your tree no longer participates in). Emits `reason=discarded`.
- **Replay** when you have fixed the upstream cause of the apply failure (config drift, schema mismatch, transient infra fault) and want the entry back in the apply path. Emits `reason=replayed`. Note that for point operations the HWM has already advanced past the entry; the replay surfaces this as `Applied=false`, which is terminal for inspection - the entry is still removed.

## Bootstrap under concurrent load

When a peer bootstraps from a snapshot while the rest of the topology is still authoring at full rate, the receiver completes the snapshot drain, pins the snapshot's `(asOfHlc, causalStableFrontier)` on its per-tree high-water-mark grain, and switches to incremental delivery. The very next batch of incremental entries can carry vector-clock dependencies on origins whose diagonal advanced *after* the snapshot was captured. The receiver-side causal-apply pipeline handles that transient catch-up window:

| Incoming entry | Receiver behaviour |
|---|---|
| `entry.VectorClock` is dominated by the pinned frontier | Per-origin HWM dedupes the entry as already-applied-via-snapshot. No buffering, no re-merge, no DLQ. |
| `entry.VectorClock` is above the frontier on at least one origin AND every dep is satisfied by the local vector clock | Applies directly. HWM advances to the entry's HLC. |
| `entry.VectorClock` is above the frontier on at least one origin AND a dep is not yet satisfied | Parks in the per-tree bounded causal-apply buffer (`CausalBufferMaxEntries` / `CausalBufferMaxBytes`). Drains and applies in FIFO order as soon as the missing predecessor lands and advances the local vector clock. |
| Buffer is at capacity when the next park request arrives | Oldest parked entry is evicted to the DLQ with `reason=hlc_skew`. The newer entry takes its slot. The producer-side WAL still holds the evicted entry, so a peer that later closes its catch-up gap can replay it from the dead-letter store. |

The window during which the third and fourth rows are reachable is bounded: it lasts only until every origin's local diagonal climbs to the frontier the producer pinned at snapshot time. Under steady-state load the window closes within seconds; under sustained heavy concurrent writes against the same origin set, it can extend long enough to fill the buffer.

### Operator playbook for `reason=hlc_skew` after a bootstrap

1. **Wait for the catch-up window to close.** Watch `apply.buffered_entries{tree}` - once it returns to zero (or near zero), every origin's diagonal has caught up to the snapshot frontier and the steady-state apply path is back in control. Replaying DLQ entries before this point is safe but pointless: the missing predecessors might still be in flight.
2. **List parked entries.** `await dlq.ListAsync(treeName, ct)` enumerates every entry the receiver parked since the bootstrap. Filter by `EnqueuedAtTicks` to scope to the bootstrap window if other DLQ traffic is mixed in.
3. **Replay each entry.** `await dlq.ReplayAsync(treeName, entryId, ct)` routes the entry through the canonical applier (which bypasses the failure-tracking decorator). Two terminal outcomes:
   - `ApplyResult.Applied = true` - the entry's deps are now satisfied, the apply landed, and the entry is removed from the DLQ with `reason=replayed`.
   - `ApplyResult.Applied = false` - the entry's deps were satisfied via in-flight transport delivery while it sat in the DLQ; the canonical applier short-circuits as a re-delivery and the entry is still removed with `reason=replayed`. **This is the expected case** for any entry that landed in the DLQ purely because it lost the FIFO eviction race.
4. **Discard only after validation.** If `ReplayAsync` throws repeatedly (e.g. the entry references a tree configuration that no longer exists), fall back to `DiscardAsync`. Replication continues regardless - the dead-letter store never blocks the apply stream.

A persistent rate of `reason=hlc_skew` long after every bootstrap completes signals a structural problem (sustained authoring load above the receiver's apply throughput, transport reordering breaking per-origin FIFO, an undersized `CausalBufferMaxEntries` for the tree's fan-in). Treat it as the cue to raise `CausalBufferMaxEntries` / `CausalBufferMaxBytes` for the affected tree, or to investigate the producer-side write rate.

