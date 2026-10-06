# Dead-letter queue (DLQ)

When the inbound apply pipeline cannot install a `WalRecord` after exhausting `LatticeReplicationOptions.MaxApplyRetries` consecutive attempts, the entry is *parked* on a per-tree dead-letter queue. A saga record is the exception: a prepare or a `TxCommit` / `TxAbort` terminal is never parked alone, because parking acknowledges it and the sender would then release the saga's terminal, so the receiver would commit the saga without that key, or a parked terminal would strand the saga's staged writes (#4591). It is *deferred* instead: the receiver answers with a not-accepted ack, the sender keeps and re-ships the record, no later record of the same saga in that batch is applied, and the stream from that origin for that tree waits until the failure clears (counted on `apply.saga_deferred`; see [Observability](observability.md#dlq-enqueue-reason-classification)). Parking unblocks the apply stream so a single poison entry cannot stall the pipeline forever, while preserving the failed entry plus diagnostic context for an operator to triage, replay, or discard. Several other paths park entries on the same per-tree queue with no retry budget (`RetryCount = 0`): a blocked entry evicted from a full causal-apply buffer, a buffered entry whose drained apply throws, and an inbound entry rejected by the receiver's merge-mode or tenant-isolation gate. A batch the sender cannot encode is not parked: the sender quarantines it and has the peer re-seeded (see [Unencodable batches](replication-drivers.md#unencodable-batches)). The `reason` tag on `dead_letter.enqueued` tells them apart - see [Metrics](#metrics).

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
   reserved system tree "_lattice_replog_dlq_{treeId}"  (e/{19-padded-id} rows)
```

The decorator is registered as the silo-side `IReplicationApplier` singleton. Apply paths inside the cluster therefore go through the decorator transparently. When it applies entries one at a time - every single-entry batch, and the per-entry fallback it takes for a batch with retry history or one the canonical applier's batch call threw on - it also records the inbound per-peer contact the canonical applier's batch path would otherwise record (the `direction="inbound"` series of `peer.last_contact_seconds` and `peer.consecutive_errors`): an error when the apply fails, whether the entry is then retried or parked, and a success otherwise (a cancelled apply records nothing). Operator inspection and replay use the public `ILatticeReplicationDeadLetters` seam, which routes through the **canonical** applier so a deterministically-failing parked entry does not re-park itself on every replay.

## Storage

Parked entries live in a reserved system tree named `_lattice_replog_dlq_{treeId}` accessed through the core library's internal system-tree surface. Each row is keyed `e/{19-padded-id}` and holds an Orleans-binary-serialised `DeadLetterEntry`. The DLQ inherits the scaling, sharding, and persistence of the core B+ tree rather than living inside one grain's persistent-state row, which would hit the storage row-size ceiling under sustained apply failure.

On activation the grain bulk-loads every parked row into an in-memory cache; subsequent reads (`List` / `Count` / `TryGet`) are served from memory and writes (`Enqueue` / `Discard` / `RemoveReplayed`) are applied to the cache and written through to the system tree. Cache size is bounded by `DeadLetterQueueCapacity` (validator pins to >= 1).

Enqueue is idempotent by the parked entry's identity (origin, HLC, key, range end, operation and transaction id): a re-shipped copy of an entry that is already parked returns the existing entry id and takes no second slot. The identity map is rebuilt from the stored rows on activation.

## Capacity and backpressure

The queue never evicts. Every parked entry was acknowledged to its sender without being applied, so evicting one would lose the write for good and leave the receiver diverged with no signal (issue #4603). When the queue already holds `DeadLetterQueueCapacity` entries, an enqueue is **refused**: the queue emits `dead_letter.refused` and throws, and the caller keeps the entry unacknowledged instead of parking it:

| Parking path | When the queue is full |
|---|---|
| Retry-budget exhaustion (dead-letter-tracking decorator) | The apply returns `Deferred = true`. The failure count is kept, the high-water mark does not advance, and the sender re-ships the entry. |
| Merge-mode or tenant-isolation gate rejection, lost dependency | The apply (or the batch run it belongs to) returns `Deferred = true`, so the sender keeps its cursor and re-ships. |
| Causal-apply buffer overflow | The park fails, so the receiver does not acknowledge and the sender re-ships, as for any other DLQ enqueue failure. |
| Causal-apply buffer drain (apply failure, lost dependency) | The entry stays parked in the buffer and the drain stops; the next drain retries it. |

The replication link that is being held back reports **Stalled** on the peer-status path (`ReplicationPeerStatusRow.DeadLetterFullSeconds` is non-null - `direction="outbound"` on the sender, `direction="inbound"` on the receiver) until a park succeeds again. Free capacity by replaying or discarding parked entries, or raise `DeadLetterQueueCapacity` for the tree.

### Alarm and operator escape for a stalled link

A full queue is a deliberate stall, not a silent one. Alarm on either signal:

- `orleans.lattice.replication.dead_letter.refused` - any sustained non-zero rate for a tree means parks are being refused and an entry is being held back. A useful rule is `sum by (tree) (rate(orleans_lattice_replication_dead_letter_refused_total[5m])) > 0` for 5 minutes. The series is emitted only on a refusal, so it is absent rather than zero on a healthy tree.
- the replication link health reported through peer status - a link held back by a full queue classifies as **Stalled**, and its `ReplicationPeerStatusRow.DeadLetterFullSeconds` is how long it has been held.

The escape is to make room in the tree's queue; nothing has to be restarted, and the held-back entries are re-shipped and parked (or applied) on the next attempt once a slot is free:

1. **Triage.** `ListAsync(treeId)` and group the parked entries by `FailureReason` and the `dead_letter.enqueued` reason. Fix the cause the reasons point at (merge-mode or tenant configuration, schema, a missing dependency).
2. **Replay** every entry that can now apply with `ReplayAsync(treeId, entryId)`. A non-deferred replay removes the entry and frees its slot.
3. **Discard** (the per-entry purge - there is no bulk purge) with `DiscardAsync(treeId, entryId)` each entry you have validated should never apply. A discard of a foreign-origin entry records it as a lost write, so its dependents are dead-lettered with `reason=dependency_lost` rather than applied; budget for those when you purge.
4. **Or raise `DeadLetterQueueCapacity`** for the tree. The queue reads the capacity on every enqueue, so a raised limit is honoured by the next park once the options change is visible to the silo.

The link leaves Stalled - `DeadLetterFullSeconds` returns to null and `dead_letter.refused` stops rising - as soon as a park succeeds again.

## Lost writes and their dependents

Discarding a parked entry whose origin is another cluster gives that write up: it was acknowledged and will never be applied here. Before the row is removed, the queue records a durable **lost mark** for the write's identity `(origin, HLC)` on the origin's causal frontier (`IReplicationOriginFrontierGrain`, shared by every tree because a dependency names an origin's write, not a tree - issue #4586); if that write fails, the discard fails and the entry stays parked. A later entry that names a lost write as a causal dependency is never released - neither by the applier nor by the causal-apply buffer drain - and is dead-lettered with `reason=dependency_lost` (apply outcome `rejected-dependency-lost`) instead, so an operator sees exactly which writes were affected. Lost marks are never pruned; their population is bounded by operator discards. Discarding a local-origin entry (one this cluster's sender parked because it could not encode it) records no lost mark, because the receiver's dependency check never names the local cluster. While a foreign-origin entry is parked here, the queue lists it as held on the same frontier - before the enqueue returns - so a dependent of it stays parked until it is replayed and applied.

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
| `SagaDeferralTimeout` | `15 minutes` | Wall-clock bound from first receiver deferral of a saga prepare or terminal to receiver-side poison. |
| `DeadLetterQueueCapacity` | `1000` | Maximum parked entries per tree. A full queue refuses further parks and holds the affected replication link back (see [Capacity and backpressure](#capacity-and-backpressure)); it never evicts. |

## Inspection seam - `ILatticeReplicationDeadLetters`

Resolve the seam from DI and call per-tree:

| Method | Returns | Notes |
|---|---|---|
| `ListAsync(treeId, ct)` | `IReadOnlyList<DeadLetterEntry>` | Ascending entry-id order. Pure read. |
| `CountAsync(treeId, ct)` | `int` | Cached count, served from memory. |
| `DiscardAsync(treeId, entryId, ct)` | `bool` | `true` when removed; `false` when the id was unknown. Records a lost mark for a foreign-origin entry first (see [Lost writes and their dependents](#lost-writes-and-their-dependents)). Emits `reason=discarded`. |
| `ReplayAsync(treeId, entryId, ct)` | `ApplyResult?` | `null` when the id is unknown. Routes through the canonical applier (bypasses the decorator's failure tracker). On any non-throwing, non-deferred return - including a result the canonical applier filtered or diverted (`Applied = false`) - the entry is removed with `reason=replayed`. A result deferred by a coordinated restore's receive fence (`Deferred = true`), a result refused for its source lineage (`SourceLineageRefused = true`), or a thrown exception leaves the entry parked. |
| `ReleaseQuarantinedSagaAsync(treeId, originClusterId, transactionId, ct)` | `bool` | Host-trusted. Releases a quarantined receiver saga once its cause is fixed (see [Receiver-side poisoned sagas](#receiver-side-poisoned-sagas)); `false` when it was not quarantined. Parked records are untouched. Emits `apply.saga_poisoned{outcome="quarantine_released"}`. |

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
    // through the canonical applier and the entry is removed - unless a
    // coordinated restore deferred it (result.Value.Deferred), which leaves
    // it parked for a later replay, or it was read under a source lineage
    // this tree no longer holds (result.Value.SourceLineageRefused), which
    // leaves it parked for the operator to discard.
}
```

## High-water-mark interaction

Parking an entry that exhausted its retry budget advances the tree's per-origin HWM (the entry for the parked entry's `OriginClusterId`) to at least the parked entry's HLC for every operation except `DeleteRange` and the saga terminal records (`TxCommit` / `TxAbort`); the other park paths - a gate rejection, a lost dependency, a causal-apply-buffer eviction or drain failure, and a sender-side encode failure - leave the high-water mark unchanged. The advance does not make a later re-delivery a no-op: the canonical applier does not drop a point write at or below the per-origin HWM, and it no longer has any snapshot-pinned floor threshold, so a re-delivered copy of the parked entry enters the apply pipeline and, if it fails again, re-enters the failure tracker. The transport does not normally re-deliver it: parking returns a non-deferred `Applied=false`, so the receive path acknowledges the batch and the sender advances past the entry.

`DeleteRange` entries skip HWM advance because the canonical applier does not consult the HWM for range deletes (range applies are naturally idempotent at the leaf layer). `TxCommit` / `TxAbort` skip it too: a saga terminal's HLC is a saga linearization point, not a per-origin frontier, and terminals are deduplicated through the per-tree transaction registry instead. The entry is still parked.

## Replay semantics

`ReplayAsync` deliberately routes through the **canonical** applier, not the decorator. Two reasons:

1. A parked entry that failed deterministically would re-park itself on every replay if routed through the decorator, which would produce an infinite re-park loop and corrupt the failure-counter state for that tuple.
2. Operators are explicitly opting into a "this entry might still apply" attempt; the failure budget is logically a transport-level concern, not an operator-replay concern.

The replay is a genuine apply attempt: a replayed point entry runs the full apply pipeline. The canonical applier's only per-origin HLC drop threshold is the bootstrap drop floor (#4549), which a replay meets like any other delivery. Every park path also releases the entry's own shadow-forward dedupe reservation (or never took one), so the replay is not suppressed as a duplicate of its original delivery. The seam treats any non-throwing, non-deferred return as terminal for cleanup and removes the parked row, whatever the resulting `Applied` flag. `Applied = true` means the write landed. `Applied = false` means the canonical applier filtered or diverted it: its identity is held in the shadow-forward dedupe cache by a re-delivered copy of the same record that has since been applied or parked; its origin is the local cluster (an entry the sender parked because it could not encode the batch), which the canonical applier never applies back onto its authoring cluster; a receiver-side gate rejected it (the enrollment gate drops it; the merge-mode and tenant-isolation gates dead-letter it again under a new id); a dependency is still missing and it was re-parked in the causal-apply buffer; or it is a write of another origin than the tree's last bootstrap source, stamped below that bootstrap's drop floor and not held there, so the bootstrap's export already reflects it (outcome `bootstrap-floor-dropped`). The one non-terminal outcome is a deferral: while a coordinated restore holds the tree's inbound receive fence, or while the entry is below a drop floor whose bootstrap has not yet closed stable (`bootstrap-floor-deferred`), the applier returns `Deferred = true` without applying anything, and the seam leaves the parked row in place (no `dead_letter.removed` is emitted), because nothing re-ships a parked entry once the fence lifts or the floor settles. Replay it again once the restore or the bootstrap completes.

A parked entry keeps the source lineage its sender stamped on the batch it arrived in (`DeadLetterEntry.SourceLineage`, with the stamping sender in `DeadLetterEntry.SourceLineageClusterId`; both are `null` for an unstamped entry). The replay runs under that stamp, so the canonical applier checks it against the lineage the tree has drained from that sender since, exactly as it checks a push ([#4707](https://github.com/NSTA1/Orleans.Lattice/issues/4707); see [Source lineage gate](snapshot-bootstrap.md#source-lineage-gate)). An entry read under a lineage the tree no longer holds - before a source restore, purge and recreate, or alias move the tree has since drained - is not applied: the replay returns `SourceLineageRefused = true` and leaves the row parked. It will never apply, so discard it.

## Receiver-side poisoned sagas

Receiver deferral is bounded for prepares and terminals. If a deferred prepare stays deferred past `SagaDeferralTimeout`, the receiver first checks its own transaction registry with `GetRecordedStatusAsync`; any status other than `InFlight` refuses poison and keeps the record deferred. A successful poison records the txid in a durable per-tree poison set keyed by origin plus transaction id, parks the deferred prepare with `reason=poisoned_saga` and acknowledges it so the link resumes, and starts a full bootstrap from that origin when `AutoBootstrapOnFallOffLog` allows it. Until that re-seed retires the poison, every terminal of the saga is withheld: deferred with a not-accepted ack and counted on `apply.saga_deferred`, never applied (it would commit the saga without the poisoned prepare) and never parked (the terminal of a saga still in flight at the re-seed's export must still arrive). A later prepare of the saga applies as usual, since it only stages; one that fails is parked at once, without waiting out the bound again. If the kickoff is disabled or fails, a durable re-seed owed marker remains on the poison grain and the maintenance tick retries it.

A deferred `TxCommit` or `TxAbort` terminal gets the same bound (#4692). A terminal that fails on every retry - a malformed record, a decision that conflicts with one the receiver registry already holds, a cross-tree barrier whose participating trees resolve different cluster ids, or any failure that outlasts the bound - would otherwise hold its tree's stream from that origin for ever, because the not-accepted ack keeps the sender's cursor. Past `SagaDeferralTimeout` the receiver poisons the saga without the registry check a prepare gets (a recorded opposite decision is one of the failures the bound exists for), counts `apply.saga_poisoned{outcome="terminal_timeout"}`, and starts or records an owed full re-seed from the origin. The terminal itself is withheld like every other terminal of a poisoned saga, never parked and never dropped. The re-seed settles the saga from the export, as for a poisoned prepare: a saga still in flight at the source keeps its staged buckets, and a decided one takes its committed rows and decision row from the export. A cross-tree wait-set change is not one of these failures: the receiver barrier keeps the wait set it froze on the first terminal (see [Cross-tree terminals](replication-apply.md#cross-tree-terminals-receiver-barrier)).

**Quarantine.** A record whose failure survives the re-seed would otherwise repeat that cycle for ever: the re-seed settles the saga and retires the poison, the sender re-ships the record, and it fails again for the same cause - a malformed record, a decision that contradicts the one the re-seed recorded, a misconfigured per-tree `ClusterId`. So the poison set remembers the sagas a completed re-seed retired. When a record of such a saga keeps failing past `SagaDeferralTimeout` again, the receiver **quarantines** the saga instead of poisoning it (#4692). It records the saga in a durable, bounded quarantine set on the same grain, counts `apply.saga_poisoned{outcome="quarantined"}`, logs an input-integrity fault naming the saga, origin, tree and cross-tree operation, and parks the record with `reason=poisoned_saga`. Parking acknowledges it, so the stream from that origin moves past it and every other saga proceeds; the saga is never re-seeded again for that cause. A later terminal of the quarantined saga is parked at once, without being applied, and a later record that fails is parked without waiting out the bound. The saga stays as the re-seed left it, so all-or-nothing visibility holds; its liveness is given up, as an input-integrity fault. Any cause that outlasts the bound counts, a transient one included: a storage or transport outage longer than `SagaDeferralTimeout` that hits a record of a saga a re-seed already retired quarantines that saga too, exactly as an input-integrity fault would. That costs no visibility - the re-seed already settled the saga from the export, so its parked records are redundant - but the saga stays quarantined until an operator releases it. To resolve a quarantine, fix the cause, inspect the parked records with `ILatticeReplicationDeadLetters.ListAsync` and remove them with `DiscardAsync`, then release the saga with `ReleaseQuarantinedSagaAsync(treeId, originClusterId, transactionId, cancellationToken)`. Discarding the parked records alone does not release it. Release removes the saga from the quarantine set, so its records are applied again; the saga stays recorded as retired, so a record of it that still fails is quarantined again after the bound, never poisoned and re-seeded. A full queue keeps a quarantined record unacknowledged (#4603), as it does any record it cannot park.

The quarantine set is bounded per tree. When a saga must be quarantined and the set is full, the receiver does not fall back to poison and re-seed, which would restart the cycle quarantine exists to end: it holds the record unacknowledged, fail-closed, so the stream from that origin for that tree waits while every other tree and origin proceeds, and it counts `apply.saga_poisoned{outcome="quarantine_full"}` and logs an error on every attempt. This is a liveness carve-out wider than the quarantine itself: while the set is full, every later write of that origin to that tree waits behind the held record, not only the saga's, and nothing but an operator ends the wait. Releasing resolved quarantines with `ReleaseQuarantinedSagaAsync` frees capacity; the next attempt then quarantines the saga, parks its record, and moves the stream on. Watch `quarantine_full` as a stalled link, not as noise. The set is held bounded rather than grown without limit because it is one grain's state, rewritten whole on every change, and it grows only from records a peer supplies.

The full re-seed settles poison at the drain boundary. Before the export rows are applied, the bootstrap coordinator captures the poisoned txids for that origin; the drain records which of them the export shipped as prepared rows. Those sagas were still in flight at the source: their staged buckets are kept, and their terminals arrive once the poison retires and commit them whole. Every other captured saga is decided at the source (the export shipped its outcome as committed rows and its decision row) or gone from it, so after the drain the coordinator discards the receiver's pending buckets for it from every leaf, without writing an abort decision into the receiver registry. That discard is durable: each leaf that held a matching bucket records the txid in its own persisted state, so a later activation skips any replayed prepare for the poisoned saga instead of rebuilding `_pendingTx`. When replay observes the discarded prepare's WAL offset and the leaf later persists a projection checkpoint at or beyond that offset, the leaf prunes the marker in the same checkpoint write. After the drain reaches live incremental, the poison entries are retired, so the withheld terminals the sender re-ships apply normally.

Operators can request the same escape with `ILatticeReplicationDeadLetters.PoisonSagaAsync(treeId, originClusterId, transactionId, cancellationToken)`. The API validates arguments, applies the same receiver-registry decision guard, logs the outcome, records `apply.saga_poisoned{outcome="operator"}` on success, and does not expose a gRPC or control-API verb. `ReleaseQuarantinedSagaAsync` sits on the same host-trusted seam with the same argument validation: it returns `false` and changes nothing when the saga is not quarantined, records `apply.saga_poisoned{outcome="quarantine_released"}` on success, and has no gRPC or control-API verb either.

A throwing replay leaves the entry parked. The operator can re-attempt or `Discard`.

## Metrics

Counters on the `orleans.lattice.replication` meter, each tagged with `tree`, `reason`, and `tenant`:

| Instrument | Tags | Meaning |
|---|---|---|
| `orleans.lattice.replication.dead_letter.enqueued` | `tree`, `tenant`, `reason in { schema, unknown, hlc_skew, mode_mismatch, foreign_tenant, tenant_offline, tenant_suspended, poisoned_saga, dependency_lost, oversized }` | Replog entry parked. `schema` / `unknown`: the dead-letter-tracking decorator (and the causal-buffer drain) classify a terminal apply exception - `ArgumentException` and `InvalidOperationException` are `schema` (malformed entry, missing field, unrecognised `LatticeMergeMode`, CAS-budget exhaustion), every other exception type is `unknown`; the sender also parks a batch it cannot encode as `schema`. `hlc_skew`: a blocked entry evicted from a full causal-apply buffer (see [Bootstrap under concurrent load](#bootstrap-under-concurrent-load)). `mode_mismatch`: the entry's wire merge mode disagrees with the receiver's resolved mode for the tree. `foreign_tenant` / `tenant_offline` / `tenant_suspended`: the tenant-isolation gate refused the write (unknown tenant / tenant not resident in this region / tenant not active). `poisoned_saga`: the sender withheld a later prepare or a terminal of a saga whose prepare it parked as `schema`, so the peer never commits the saga torn; the peer serves the saga as never written until it is re-bootstrapped. `dependency_lost`: the entry depends on a write an operator discarded from this queue (see [Lost writes and their dependents](#lost-writes-and-their-dependents)). `oversized` is reserved and has no emitter today. An entry with an empty tree id cannot be parked per tree: it is dropped and still counted as `schema` with an empty `tree` tag. |
| `orleans.lattice.replication.dead_letter.removed` | `tree`, `tenant`, `reason in { discarded, replayed }` | Entry removed. `discarded` = explicit operator call; `replayed` = removed after `ReplayAsync` completed. The `evicted` reason is no longer emitted: the queue refuses rather than evicts. |
| `orleans.lattice.replication.dead_letter.refused` | `tree`, `tenant`, `reason` (the reason the entry would have been parked under) | An enqueue was refused because the queue was full; the entry was kept unacknowledged (see [Capacity and backpressure](#capacity-and-backpressure)). A sustained non-zero rate means a replication link is stalled until parked entries are replayed or discarded. |
| `orleans.lattice.replication.apply.saga_poisoned` | `tree`, `tenant`, `origin`, `outcome in { timeout, terminal_timeout, quarantined, quarantine_full, quarantine_released, operator, refused_decided, refused_full }` | Receiver-side saga poison outcomes. `timeout` (a prepare) / `terminal_timeout` (a terminal, #4692) / `operator` record successful poison and a required full re-seed from the origin. `quarantined` records a saga quarantined because its record failed again after a re-seed settled it (#4692): an input-integrity fault, with no further re-seed. `quarantine_full` means a saga had to be quarantined but the bounded quarantine set is full, so its record is held unacknowledged and the stream from that origin for that tree waits. `quarantine_released` records an operator release through `ReleaseQuarantinedSagaAsync`. `refused_decided` means the receiver registry already recorded a terminal decision. `refused_full` means the bounded poison set is full and deferral remains fail-closed. |

## Persistence and rehydration

The per-tree queue bulk-loads its parked rows from the system tree on every activation. Operators can therefore deactivate or restart the silo and parked entries reappear with their original `EntryId` values intact. The next id is recomputed as the highest stored entry id plus one, so new ids stay above every surviving entry - but an id removed from the top of the queue before a reactivation (by a discard or replay of the newest entries, or by draining the queue to empty) can be issued again to a later entry.

## When to discard vs. replay

- **Discard** when you have validated the underlying data fault and deliberately want to drop the entry (e.g. it carries a key your tree no longer participates in). Emits `reason=discarded`.
- **Replay** when you have fixed the upstream cause of the apply failure (config drift, schema mismatch, transient infra fault) and want the entry back in the apply path. Emits `reason=replayed`. Check the returned `ApplyResult`: `Applied = true` confirms the write landed, while `Applied = false` means the canonical applier filtered or diverted it (see [Replay semantics](#replay-semantics)) - the entry is removed either way, unless `Deferred = true`, which means a coordinated restore's receive fence held it back and it is still parked.

## Bootstrap under concurrent load

When a peer bootstraps from a snapshot while the rest of the topology is still authoring at full rate, the receiver completes the snapshot drain, merges the snapshot's `(asOfHlc, causalStableFrontier)` into its per-tree high-water-mark grain by pointwise maximum, drains the durable causal-apply buffer, and switches to incremental delivery. The very next batch of incremental entries can carry vector-clock dependencies on origins whose diagonal advanced *after* the snapshot was captured. The receiver-side causal-apply pipeline handles that transient catch-up window:

| Incoming entry | Receiver behaviour |
|---|---|
| `entry.Timestamp` is at or below its origin's coordinate in the pinned frontier | No HLC-threshold dedup runs. The entry applies, parks on missing dependencies, or is suppressed only if its exact identity is already in the shadow-forward cache. |
| Every dependency in `entry.VectorClock` is satisfied by the local vector clock | Applies directly. The per-origin HWM advances monotonically to the entry's HLC when that HLC is above the current diagonal. |
| A dependency in `entry.VectorClock` is not yet satisfied | Parks durably in the per-tree causal-apply buffer (`CausalBufferMaxEntries` / `CausalBufferMaxBytes`) before the receiver acknowledges the delivery. The buffer drains in FIFO fixed-point order after the park, after HWM advances that observe it non-empty, on first touch after restart, after bootstrap handoff, and on every replication maintenance tick. |
| Buffer is at capacity when the next park request arrives | Oldest parked entry is enqueued to the DLQ with `reason=hlc_skew` before the durable buffer write removes it. The newer entry takes its slot only after that write succeeds; if the DLQ enqueue or buffer write fails, the park fails and the sender re-ships. The evicted entry is kept in the dead-letter store, so an operator can replay it once its dependencies have landed; its shadow-forward dedupe reservation is released as it is evicted, so that replay (or a re-delivered copy) is applied rather than dropped as a duplicate. |

The window during which the third and fourth rows are reachable is bounded: it lasts only until every origin's local diagonal climbs to the frontier the producer pinned at snapshot time. Under steady-state load the window closes within seconds; under sustained heavy concurrent writes against the same origin set, it can extend long enough to fill the buffer.

### Operator playbook for `reason=hlc_skew` after a bootstrap

1. **Wait for the catch-up window to close.** Watch `apply.buffered_entries{tree}` - once it returns to zero (or near zero), every origin's diagonal has caught up to the snapshot frontier and the steady-state apply path is back in control. Replaying DLQ entries before this point is safe but pointless: the missing predecessors might still be in flight.
2. **List parked entries.** `await dlq.ListAsync(treeName, ct)` enumerates every entry the receiver parked since the bootstrap. Filter by `EnqueuedAtTicks` to scope to the bootstrap window if other DLQ traffic is mixed in.
3. **Replay each entry.** `await dlq.ReplayAsync(treeName, entryId, ct)` routes the entry through the canonical applier (which bypasses the failure-tracking decorator). Two terminal outcomes, and one that is not:
   - `ApplyResult.Applied = true` - the entry's deps are now satisfied, the apply landed, and the entry is removed from the DLQ with `reason=replayed`.
   - `ApplyResult.Applied = false` - the canonical applier did not install the entry on this attempt, and the entry is still removed with `reason=replayed`. Eviction released the entry's shadow-forward dedupe reservation, so its original delivery cannot suppress the replay; the usual cause is a dependency that is still missing, in which case the entry was re-parked in the causal-apply buffer. The transport does not normally re-deliver an evicted entry (its original delivery was acknowledged when it was parked), but a copy that does arrive again - for example in a batch re-shipped after a lost acknowledgement - is applied or parked in its own right and then holds the identity, so the replay is suppressed as its duplicate. See [Replay semantics](#replay-semantics) for the other `Applied = false` outcomes. Verify the key's state rather than treating this outcome as confirmation.
   - `ApplyResult.Deferred = true` - a coordinated restore holds the tree's inbound receive fence, so nothing was applied and the entry stays parked. Replay it again once the restore completes.
4. **Discard only after validation.** If `ReplayAsync` throws repeatedly (e.g. the entry references a tree configuration that no longer exists), fall back to `DiscardAsync`. A discard records the write as lost, so any later entry that depends on it is dead-lettered with `reason=dependency_lost` rather than applied. Replication continues past parked entries while the queue has room; a full queue holds the link back until entries are replayed or discarded.

A persistent rate of `reason=hlc_skew` long after every bootstrap completes signals a structural problem (sustained authoring load above the receiver's apply throughput, transport reordering breaking per-origin FIFO, an undersized `CausalBufferMaxEntries` for the tree's fan-in). Treat it as the cue to raise `CausalBufferMaxEntries` / `CausalBufferMaxBytes` for the affected tree, or to investigate the producer-side write rate.
