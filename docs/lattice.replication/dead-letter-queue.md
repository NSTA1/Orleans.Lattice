# Dead-letter queue (DLQ)

When the inbound apply pipeline cannot install a `WalRecord` after exhausting `LatticeReplicationOptions.MaxApplyRetries` consecutive attempts, the entry is *parked* on a per-tree dead-letter queue. Parking unblocks the apply stream so a single poison entry cannot stall the pipeline forever, while preserving the failed entry plus diagnostic context for an operator to triage, replay, or discard. Several other paths park entries on the same per-tree queue with no retry budget (`RetryCount = 0`): a blocked entry evicted from a full causal-apply buffer, a buffered entry whose drained apply throws, an inbound entry rejected by the receiver's merge-mode or tenant-isolation gate, and a batch the sender cannot encode. The `reason` tag on `dead_letter.enqueued` tells them apart - see [Metrics](#metrics).

## Topology

```text
   inbound batch (gRPC Push RPC)
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

Parked entries live in a reserved system tree named `_lattice_replog_dlq_{treeId}` accessed through the internal `ISystemLattice` surface. Each row is keyed `e/{19-padded-id}` and holds an Orleans-binary-serialised `DeadLetterEntry`. The DLQ inherits the scaling, sharding, and persistence of the core B+ tree rather than living inside one grain's persistent-state row, which would hit the storage row-size ceiling under sustained apply failure.

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
| `ReplayAsync(treeId, entryId, ct)` | `ApplyResult?` | `null` when the id is unknown. Routes through the canonical applier (bypasses the decorator's failure tracker). On any non-throwing return - including a result the canonical applier filtered or diverted (`Applied = false`) - the entry is removed with `reason=replayed`. A thrown exception leaves the entry parked. |

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

Parking advances the tree's per-origin HWM (the entry for the parked entry's `OriginClusterId`) to at least the parked entry's HLC for every operation except `DeleteRange` and the saga terminal records (`TxCommit` / `TxAbort`). The advance does not make a later re-delivery a no-op: the canonical applier does not drop a point write at or below the per-origin HWM (its only point-write drop threshold is the snapshot-pinned causal floor, which parking does not move), so a re-delivered copy of the parked entry is applied afresh and, if it fails again, re-enters the failure tracker. The transport does not normally re-deliver it: parking returns a non-deferred `Applied=false`, so the receive path acknowledges the batch and the sender advances past the entry.

`DeleteRange` entries skip HWM advance because the canonical applier does not consult the HWM for range deletes (range applies are naturally idempotent at the leaf layer). `TxCommit` / `TxAbort` skip it too: a saga terminal's HLC is a saga linearization point, not a per-origin frontier, and terminals are deduplicated through the per-tree transaction registry instead. The entry is still parked.

## Replay semantics

`ReplayAsync` deliberately routes through the **canonical** applier, not the decorator. Two reasons:

1. A parked entry that failed deterministically would re-park itself on every replay if routed through the decorator, which would produce an infinite re-park loop and corrupt the failure-counter state for that tuple.
2. Operators are explicitly opting into a "this entry might still apply" attempt; the failure budget is logically a transport-level concern, not an operator-replay concern.

The replay is a genuine apply attempt: parking advances the per-origin HWM but not the snapshot-pinned causal floor - the canonical applier's only point-write drop threshold - so a replayed point entry runs the full apply pipeline. The seam treats any non-throwing return as terminal for cleanup and removes the parked row, whatever the resulting `Applied` flag. `Applied = true` means the write landed. `Applied = false` means the canonical applier filtered or diverted it: its HLC is at or below the pinned floor, its identity is still held in the shadow-forward dedupe cache, a receiver-side gate rejected it (the enrollment gate drops it; the merge-mode and tenant-isolation gates dead-letter it again under a new id), or a dependency is still missing and it was re-parked in the causal-apply buffer.

A throwing replay leaves the entry parked. The operator can re-attempt or `Discard`.

## Metrics

Counters on the `orleans.lattice.replication` meter, both tagged with `tree`, `reason`, and `tenant`:

| Instrument | Tags | Meaning |
|---|---|---|
| `orleans.lattice.replication.dead_letter.enqueued` | `tree`, `tenant`, `reason in { schema, unknown, hlc_skew, mode_mismatch, foreign_tenant, tenant_offline, tenant_suspended, oversized }` | Replog entry parked. `schema` / `unknown`: the dead-letter-tracking decorator (and the causal-buffer drain) classify a terminal apply exception - `ArgumentException` and `InvalidOperationException` are `schema` (malformed entry, missing field, unrecognised `LatticeMergeMode`, CAS-budget exhaustion), every other exception type is `unknown`; the sender also parks a batch it cannot encode as `schema`. `hlc_skew`: a blocked entry evicted from a full causal-apply buffer (see [Bootstrap under concurrent load](#bootstrap-under-concurrent-load)). `mode_mismatch`: the entry's wire merge mode disagrees with the receiver's resolved mode for the tree. `foreign_tenant` / `tenant_offline` / `tenant_suspended`: the tenant-isolation gate refused the write (unknown tenant / tenant not resident in this region / tenant not active). `oversized` is reserved and has no emitter today. An entry with an empty tree id cannot be parked per tree: it is dropped and still counted as `schema` with an empty `tree` tag. |
| `orleans.lattice.replication.dead_letter.removed` | `tree`, `tenant`, `reason in { discarded, replayed, evicted }` | Entry removed. `discarded` = explicit operator call; `replayed` = removed after `ReplayAsync` completed; `evicted` = FIFO capacity eviction during a later enqueue. |

## Persistence and rehydration

The grain bulk-loads its parked rows from the system tree on every activation. Operators can therefore deactivate or restart the silo and parked entries reappear with their original `EntryId` values intact - `_nextEntryId` is recomputed as `max(stored entry-id) + 1` so subsequent enqueues are still monotonic.

## When to discard vs. replay

- **Discard** when you have validated the underlying data fault and deliberately want to drop the entry (e.g. it carries a key your tree no longer participates in). Emits `reason=discarded`.
- **Replay** when you have fixed the upstream cause of the apply failure (config drift, schema mismatch, transient infra fault) and want the entry back in the apply path. Emits `reason=replayed`. Check the returned `ApplyResult`: `Applied = true` confirms the write landed, while `Applied = false` means the canonical applier filtered or diverted it (see [Replay semantics](#replay-semantics)) - the entry is removed either way.

## Bootstrap under concurrent load

When a peer bootstraps from a snapshot while the rest of the topology is still authoring at full rate, the receiver completes the snapshot drain, pins the snapshot's `(asOfHlc, causalStableFrontier)` on its per-tree high-water-mark grain, and switches to incremental delivery. The very next batch of incremental entries can carry vector-clock dependencies on origins whose diagonal advanced *after* the snapshot was captured. The receiver-side causal-apply pipeline handles that transient catch-up window:

| Incoming entry | Receiver behaviour |
|---|---|
| `entry.Timestamp` is at or below its origin's coordinate in the pinned frontier | The snapshot-pinned causal floor dedupes the entry as already-applied-via-snapshot. No buffering, no re-merge, no DLQ. |
| `entry.Timestamp` is above the pinned floor AND every dependency in `entry.VectorClock` is satisfied by the local vector clock | Applies directly. The per-origin HWM advances monotonically to the entry's HLC. |
| `entry.Timestamp` is above the pinned floor AND a dependency in `entry.VectorClock` is not yet satisfied | Parks in the per-tree bounded causal-apply buffer (`CausalBufferMaxEntries` / `CausalBufferMaxBytes`). Drains and applies in FIFO order as soon as the missing predecessor lands and advances the local vector clock. |
| Buffer is at capacity when the next park request arrives | Oldest parked entry is evicted to the DLQ with `reason=hlc_skew`. The newer entry takes its slot. The evicted entry is kept in the dead-letter store, so an operator can replay it once its dependencies have landed. |

The window during which the third and fourth rows are reachable is bounded: it lasts only until every origin's local diagonal climbs to the frontier the producer pinned at snapshot time. Under steady-state load the window closes within seconds; under sustained heavy concurrent writes against the same origin set, it can extend long enough to fill the buffer.

### Operator playbook for `reason=hlc_skew` after a bootstrap

1. **Wait for the catch-up window to close.** Watch `apply.buffered_entries{tree}` - once it returns to zero (or near zero), every origin's diagonal has caught up to the snapshot frontier and the steady-state apply path is back in control. Replaying DLQ entries before this point is safe but pointless: the missing predecessors might still be in flight.
2. **List parked entries.** `await dlq.ListAsync(treeName, ct)` enumerates every entry the receiver parked since the bootstrap. Filter by `EnqueuedAtTicks` to scope to the bootstrap window if other DLQ traffic is mixed in.
3. **Replay each entry.** `await dlq.ReplayAsync(treeName, entryId, ct)` routes the entry through the canonical applier (which bypasses the failure-tracking decorator). Two terminal outcomes:
   - `ApplyResult.Applied = true` - the entry's deps are now satisfied, the apply landed, and the entry is removed from the DLQ with `reason=replayed`.
   - `ApplyResult.Applied = false` - the canonical applier did not install the entry on this attempt, and the entry is still removed with `reason=replayed`. It does not mean a later copy already landed: the transport never re-delivers an evicted entry (its original delivery was acknowledged when it was parked). Either a dependency is still missing and the entry was re-parked in the causal-apply buffer, or its identity is still held in the shadow-forward dedupe cache from that original delivery and the apply was suppressed. Verify the key's state rather than treating this outcome as confirmation.
4. **Discard only after validation.** If `ReplayAsync` throws repeatedly (e.g. the entry references a tree configuration that no longer exists), fall back to `DiscardAsync`. Replication continues regardless - the dead-letter store never blocks the apply stream.

A persistent rate of `reason=hlc_skew` long after every bootstrap completes signals a structural problem (sustained authoring load above the receiver's apply throughput, transport reordering breaking per-origin FIFO, an undersized `CausalBufferMaxEntries` for the tree's fan-in). Treat it as the cue to raise `CausalBufferMaxEntries` / `CausalBufferMaxBytes` for the affected tree, or to investigate the producer-side write rate.

