# Atomic Multi-Key Writes

`ILattice.SetManyAtomicAsync(entries)` commits a batch of key-value pairs with
all-or-nothing semantics: either every entry in the batch is durably written,
or - on any failure - every already-committed entry in that batch is rolled
back to its pre-saga value. The feature is implemented as a saga coordinator
grain that wraps the existing per-key `SetAsync` path.

The non-atomic `SetManyAsync` remains available for throughput-oriented use
cases where partial application on failure is acceptable.

If you need to commit a tree write all-or-nothing *together with* a non-tree
effect - a payment, an email, a call to another grain - reach for the
[Atomic Action](atomic-action.md) coordinator instead. It generalizes this
key-only atomic write to an ordered plan of arbitrary caller-defined
forward/compensate steps, and ships a built-in tree-write step that delegates
back to this machinery, so a `SetManyAtomicAsync`-equivalent mutation can be one
step of a larger saga without giving up the atomicity guarantees described here.

## Atomicity Guarantees

This section describes the **atomicity** contract of the saga. For the
reader-visibility model - the per-tree `ITxRegistryGrain` linearization
point and how each read path consults it - see
[Consistency: Atomic visibility](consistency.md#atomic-visibility).
The atomicity guarantee is **universal**: it holds within a single
tree on the local cluster *and* across every cluster the tree
replicates to. Receiver-side replication routes prepared writes and
the per-shard terminal mark through the same `ITxRegistryGrain`
linearization point used locally, so a remote reader concurrent with
replication of a `SetManyAtomicAsync` observes either zero or all of
the saga's keys - never a partial view (see Consistency for details).

Given a batch `[(k0, v0), (k1, v1), ..., (kN-1, vN-1)]`, a successful
`SetManyAtomicAsync` call guarantees:

1. **All-or-nothing commit.** On successful return every `ki` holds `vi` as
   its last-writer-wins (LWW) value, or - if the saga failed - every `ki`
   holds the value it had before the saga started (its pre-saga value; for
   keys that did not exist before the saga, the key is tombstoned).
2. **Sequential per-key ordering.** Each key is written at most once by the
   saga, with a monotonically-increasing [Hybrid Logical Clock](state-primitives.md)
   timestamp. Compensation writes use a fresh HLC tick, so LWW merge resolves
   the rollback as the winner even if an external writer raced the saga.
3. **Crash recovery.** If the silo hosting the saga grain crashes mid-flight,
   a keepalive reminder resumes the saga on reactivation and drives it to a
   terminal state (either Completed or Compensated + Completed).
4. **Input validation.** Duplicate keys and null keys fail fast with
   `ArgumentException` before any write is attempted. A null value fails fast
   for an upsert entry, but is permitted for a delete entry in a mixed
   set+delete batch (a delete carries no value). An empty batch returns
   immediately without contacting any leaf grain.
5. **Idempotent client retry.** A re-invocation of the same saga grain (same
   `treeId` + `operationId`) after successful completion returns success
   without re-executing. A re-invocation after a compensated-failure replays
   the original `InvalidOperationException` with the preserved failure
   message.

### What `SetManyAtomicAsync` does **not** guarantee

- **Cross-tree atomicity (this call).** A `SetManyAtomicAsync` call is
  bound to a single tree (`ILattice`) and the per-tree
  `ITxRegistryGrain` is its unit of atomic visibility. To commit a batch
  spanning two or more distinct trees all-or-nothing, use
  `IGrainFactory.SetManyAtomicAsync` (or the
  `BeginAtomicWrite` fluent builder) - a two-level saga that layers a
  single global decision over each tree's per-tree saga. See
  [Cross-tree (multi-tree) atomic writes](#cross-tree-multi-tree-atomic-writes)
  below.
- **Ordering across distinct sagas.** Two concurrent `SetManyAtomicAsync`
  calls touching overlapping keys are resolved pairwise by LWW - the later
  HLC tick wins per key. There is no global transaction order.
- **Compensation durability in every failure mode.** If compensation itself
  fails persistently (every retry attempt throws), the saga is marked
  *poisoned* and the keepalive reminder continues firing; operators can
  inspect `AtomicWriteState.FailureMessage` via persistent state. This is
  rare in practice and limited to total-storage-outage scenarios.

## Usage

```csharp verify
var tree = grainFactory.GetGrain<ILattice>("orders");

// Byte-array surface
var batch = new List<KeyValuePair<string, byte[]>>
{
    new("order:42/status", Encoding.UTF8.GetBytes("shipped")),
    new("order:42/tracking", Encoding.UTF8.GetBytes("1Z999")),
    new("customer:alice/last-order", Encoding.UTF8.GetBytes("42")),
};
await tree.SetManyAtomicAsync(batch);

// Typed extension (System.Text.Json by default)
var typedBatch = new List<KeyValuePair<string, Order>>
{
    new("order:42", new Order("42", 99.95m)),
    new("order:43", new Order("43", 12.50m)),
};
await tree.SetManyAtomicAsync(typedBatch);
```

## Mixed upsert + delete batches

`SetManyAtomicAsync(upserts, deletes, operationId)` commits a batch that mixes
**upserts and deletes** in a single all-or-nothing visibility flip. Every key
in `upserts` is written and every key in `deletes` is tombstoned as one atomic
unit: a reader observes either the whole batch applied or none of it, never a
partial state where some upserts are visible but the deletes are not (or vice
versa). Each delete becomes visible on commit and is dropped on abort, riding
the **same saga terminal** as the upserts.

This is the primitive behind atomic re-key retraction: when a logical row moves
from key `A` to key `B`, the upsert at `B` and the delete at `A` flip together,
so a reader never sees both keys at once or neither. The materialised-view
maintainer uses it to flush a completed atomic batch's upserts and retraction
deletes inside one mixed op (see [Materialised views](materialised-views.md)).

A stable `operationId` is required for this overload so a client retry
re-attaches to the original saga. The delete-key channel is optional on the
wire: a batch with no deletes is byte-identical to the value-only overload.

```csharp verify
var tree = grainFactory.GetGrain<ILattice>("orders");

// Move order:42's row from the "pending" index key to the "shipped" index key
// atomically: the new key appears and the old key disappears in one flip.
var upserts = new List<KeyValuePair<string, byte[]>>
{
    new("index/shipped/order:42", Encoding.UTF8.GetBytes("42")),
};
var deletes = new List<string> { "index/pending/order:42" };

await tree.SetManyAtomicAsync(upserts, deletes, "rekey:order-42");
```

On the cross-tree fluent builder, stage a retraction with `.Delete(key)`
alongside `.Set(...)` calls under the same `ForTree(...)`; the deletes ride the
cross-tree saga and flip jointly with the upserts (see
[Cross-tree atomic writes](#cross-tree-multi-tree-atomic-writes)).

## Guarded atomic writes

`SetManyAtomicAsync<T>(entries, predicate)` adds an all-or-nothing
precondition: the batch commits only if **every** targeted key's pre-saga
value satisfies the predicate. The predicate is compiled to the serializable
predicate IR and evaluated once, server-side, against each key's captured
pre-saga document during the saga's prepare phase - a key with no live
pre-saga value counts as a non-match. When any key fails the saga aborts
before any write and the call returns `AtomicWriteOutcome.PreconditionFailed`
with nothing committed; when all match it returns
`AtomicWriteOutcome.Committed`. A precondition miss is reported as a value,
not an exception, so a guarded conflict is ordinary control flow. Genuine
write failures still throw and compensate exactly as the unguarded overload.

The idempotency-key overload re-attaches to the original saga and returns its
memoized outcome without re-evaluating the (pure) predicate against
possibly-moved data.

```csharp verify
var tree = grainFactory.GetGrain<ILattice>("orders");
var guardedBatch = new List<KeyValuePair<string, Order>>
{
    new("order:42", new Order("42", 99.95m)),
    new("order:43", new Order("43", 12.50m)),
};

// Only commit the whole batch if every order's current Total is below 1000.
AtomicWriteOutcome outcome =
    await tree.SetManyAtomicAsync<Order>(guardedBatch, o => o.Total < 1000m);

if (outcome == AtomicWriteOutcome.PreconditionFailed)
{
    // At least one order's pre-saga value failed the guard; nothing was written.
}
```

## How It Works

### Saga Coordinator Grain

Each call to `SetManyAtomicAsync` routes through `LatticeGrain` to a freshly
created `AtomicWriteGrain` activation keyed by `{treeId}/{operationId}`,
where `operationId` is a GUID generated per call. The grain persists a single
`AtomicWriteState` POCO whose phase transitions drive the saga lifecycle:

```
NotStarted --> Prepare --> Execute --> registry.MarkCommitted --> fan-out --> Completed   (success)
                              |
                              +--> Compensate --> registry.MarkAborted --> fan-out --> Completed   (failure)
```

The saga's persisted phase advances through the states shown above - not
started, prepare, execute, compensate, and completed - plus two further
states that the happy-path diagram omits. The first is a terminal
**precondition-failed** state: a guard predicate evaluated against the
pre-saga snapshot failed for at least one key, so the saga aborted before any
write and committed nothing; it is memoized distinctly from ordinary
completion so a guarded caller re-reads the precondition-miss outcome on
re-attach. The second is a **prepared / paused** state used by cross-tree
sagas: every write has been staged into the leaf pending buckets (hidden from
readers) and the per-tree registry has delegated this saga's txid to the
cross-tree coordinator, and the saga waits there until the coordinator
finalizes it into commit (execute-tail) or abort (compensate). The
`registry.Mark*` and `fan-out` steps run synchronously inside `RunSagaAsync`
between the execute / compensate phase exit and the final completed-phase
write. **The registry write is the single tree-wide visibility flip**
(see [Consistency: Atomic
visibility](consistency.md#atomic-visibility));
the terminal fan-out is best-effort lazy GC of each touched leaf's
pending-tx bucket.

### Phase 1 - Prepare

The coordinator registers a **keepalive reminder** (1-minute period) so that a
silo crash during any subsequent phase triggers reactivation and resumption
on the next reminder tick. It then issues `GetAsync(key)` for every key in
the batch and records each pre-saga value (including absence) in
`AtomicWriteState.PreValues`. Persisting the full pre-saga snapshot before
any write is the prerequisite for bounded-time compensation. The phase ends
with a `WriteStateAsync` that flips the persisted phase to `Execute`.

### Phase 2 - Execute

The coordinator dispatches the saga's unwritten remainder in a single
`ILattice.SetManyAsync` call under an ambient
`LatticePreparedContext.BeginScope()`. The shard-bucketing fan-out
inside `LatticeGrain.SetManyAsync` runs cross-leaf calls in parallel
via `Task.WhenAll`, giving the saga concurrent per-shard dispatch.
The leaf grain's commit pipeline observes the prepared-context flag
and routes each per-key write into its in-memory `_pendingTx[txid]`
bucket rather than into the visible `Entries` projection - so prepared
writes are **invisible to concurrent readers** for the duration of
the prepare window. `NextIndex` is incremented and persisted after
each successful batch, so crash-resume can pick up exactly where it
left off without replaying committed prepares. Retry semantics are
**per-batch**: `MaxRetriesPerStep = 1` is the per-batch retry budget;
on any task's failure the whole unwritten remainder is re-attempted,
and on budget exhaustion the saga pivots to `Compensate` without
re-throwing. The shutdown-refused fast-path (writer-side drain refusal
or post-deactivation leaf rejection) bypasses the retry budget
entirely - see [Shutdown back-pressure](#shutdown-back-pressure)
below.

### Phase 3 - Compensate (failure path only)

The coordinator walks already-committed entries in **reverse** order. For
each previously-existing key it calls `SetAsync(key, preValue)`; for each
previously-absent key it calls `DeleteAsync(key)`. Every compensation write
receives a fresh HLC tick from the target leaf grain, so LWW merge resolves
the compensating write as the winner even if an external writer modified the
key during the saga's execution window.

On reminder-driven re-entry, the per-step retry counter is reset so a
transient fault that outlived the previous activation can be retried
freshly.

### Tree-wide visibility flip

After Execute completes (success path) or after Compensate completes
(failure path), the saga records its terminal outcome on the per-tree
`ITxRegistryGrain` via `MarkCommittedAsync(txid)` or
`MarkAbortedAsync(txid)`. **This single registry write is the moment
of tree-wide visibility flip.** It is the only point in the saga's
lifecycle at which any reader, anywhere in the tree, can observe the
saga's effect: every read path that finds a key in a leaf's pending-tx
bucket dials back through the registry and resolves the read against
the recorded outcome. There is no inter-leaf coordination beyond the
registry write - the post-decision / pre-fan-out window is invisible
to readers because the registry is the single tree-wide linearization
point.

After the registry write the saga broadcasts `MutationKind.TxCommit`
(success) or `MutationKind.TxAbort` (failure) terminal marks to every
shard root the saga touched, which fans each mark out to the affected
leaves. The terminals drain
each leaf's pending-tx bucket into visible entries (commit) or drop
the prepared values (abort) - this is **best-effort lazy GC of the
pending bucket**, not the visibility primitive. Readers that race the
fan-out continue to observe the registry-recorded outcome via
dial-back until their pending entries are drained.

### Phase 4 - Complete

Whether the saga succeeds or rolls back, it ends by writing `Phase =
Completed`, unregistering the keepalive reminder, arming the retention
reminder (`atomic-write-retention`, default 48 h via
`LatticeOptions.AtomicWriteRetention`) for delayed state cleanup,
calling `registry.ForgetAsync(txid)` so the registry transitions the
decision into its tombstone retention window, and calling
`DeactivateOnIdle`. Failed sagas preserve `FailureMessage` so a client
that re-invokes the same grain key receives the original failure via
`InvalidOperationException`.

The terminal write also **releases the staged batch payload** from the
persisted state. A completed saga is retained only so an idempotent
re-entry (or a reminder-driven reactivation) can re-derive its
lightweight outcome - `Phase`, `FailureMessage`, `KeyFingerprint`,
`TransactionId` - none of which need the value-bearing staged batch. The
byte-array fields (`Entries`, `PreValues`, and the per-entry delta/delete
carries) are emptied in the same checkpoint that flips `Phase` to
`Completed`, so a completed saga's persisted row is bounded to its outcome
fields rather than pinning the full batch value payload for the whole
retention window. The small scalar/metadata fields (`TouchedShards`,
`Guard`, `VectorClock`) carry no value bytes and are kept. The release is
crash-safe: if the terminal persist fails, the released fields are restored
verbatim so the same activation can retry and a reactivation re-reads the
intact pre-terminal record from disk.

`ForgetAsync` does **not** evict the decision record immediately - it
stamps a tombstone in the per-tree `ITxRegistryGrain` with a `ForgottenAt`
timestamp and retains the committed/aborted verdict for
`LatticeOptions.TxDecisionRetention` (default 60 s). During this window
`GetStatusAsync` / `GetStatusManyAsync` / `SnapshotAsync` continue to
surface the saga's outcome, so any process that races the saga's
terminal fan-out and installs a *new* pending bucket on the saga's txid
can still resolve the verdict and apply the terminal directly. The
primary race this guards is the retroactive shadow-forward sweep at
the start of an adaptive shard split: the split coordinator walks the
source shard's leaves at `BeginShadowWrite` entry, replays every
in-flight prepared mutation into the destination shard's
`_pendingTx` buckets, and then runs a post-sweep cleanup pass that
resolves any saga whose terminal already completed via the retained
verdict. Without the retention window, a saga that completed
microseconds before the sweep installed its pending bucket on the
destination would leave an orphan bucket whose verdict the registry
had already forgotten. Tombstones expire and are physically purged on
the next `ForgetAsync` / `MarkCommittedAsync` / `MarkAbortedAsync`
call. Setting `TxDecisionRetention = TimeSpan.Zero` restores the
pre-tombstone immediate-evict behaviour and reintroduces the orphan
risk - reserved for unit tests or environments that disable adaptive
splitting.

## Crash-Recovery Timeline

| Crash point | State on reactivation | Recovery path |
|---|---|---|
| Before `Prepare` persists | `Phase = NotStarted` | Reminder tick unregisters itself and deactivates. Client's pending call returns a transport error; client retries with a fresh `operationId`. |
| During `Execute`, after *k* writes committed | `Phase = Execute`, `NextIndex = k` | Reminder tick calls `RunSagaAsync`; the saga resumes at entry *k* and drives to completion. |
| During `Compensate`, after *m* rollbacks | `Phase = Compensate`, `NextIndex = N - m` | Reminder tick resets `RetriesOnCurrentStep`, continues compensation, then completes. |
| After `Completed` persists | `Phase = Completed` | Reminder tick unregisters itself and deactivates. |

## Performance Notes

- A saga of size *N* issues approximately *2N* + 3 Orleans calls: *N*
  pre-saga reads, *N* writes, and 3 `WriteStateAsync` calls on the saga's own
  state - plus one registry write (`MarkCommittedAsync` /
  `MarkAbortedAsync`), one per-shard terminal fan-out RPC, and one
  registry `ForgetAsync` cleanup. For large batches where atomicity is
  not required, prefer the parallel `SetManyAsync`.
- The saga's persisted state is stored under the Lattice storage provider
  (`"OrleansLattice"`). The saga grain deactivates on completion, so the
  storage row is typically read exactly once (on activation) and written
  four times (Prepare -> Execute -> ... -> Completed). The final
  (`Completed`) write also releases the staged batch payload, so a
  completed saga's persisted footprint is bounded to its lightweight
  outcome fields for the retention window rather than the full batch value
  payload - which matters most for high-cardinality bulk workloads that
  produce one saga per touched key.
- Readers observing a saga in flight pay one extra registry RPC per
  pending key: a per-leaf pending-status resolve for direct
  single-key reads, a single batched pending-status resolve per leaf for
  scans, or a single `SnapshotAsync` per multi-shard fan-out (stamped
  onto an ambient context so every leaf in the scan reuses it). The
  coordinator does not block, lock, or serialise reads - the registry
  RPC is the entire visibility cost.
- Multi-page enumerations (streaming `ScanKeysAsync` /
  `ScanEntriesAsync` and point-in-time durable cursors opened with
  `pointInTime: true`) take the same single `SnapshotAsync` once and
  reuse it across every page, so the per-page registry cost is zero
  after the initial capture. Point-in-time durable cursors additionally
  pin the captured decision set on the registry so a saga that
  completes mid-enumeration still has its verdict resolvable for the
  cursor's lifetime; see [Durable Cursors - Point-in-time cursors](durable-cursors.md#point-in-time-cursors).

## Shutdown back-pressure

A saga that is dispatched while the silo is shutting down (the host
has received SIGTERM and the WAL writer is draining) cannot reach the
storage layer - the writer-side drain refuses new appends with the
typed `LatticeShuttingDownException`. The saga coordinator detects
this regime (and the related Orleans grain-rejection shape that fires
when a leaf grain has been deactivated as part of the same shutdown)
and **fast-fails without consuming retry budget**: the next retry
would route through the same drained writer and fail identically, so
the saga short-circuits both the per-batch retry loop and the per-
shard compensate-broadcast pass and surfaces the failure to the
caller as `LatticeShuttingDownException`.

The terminal outcome is recorded on the
`orleans.lattice.atomic_write.completed` counter as
`outcome=shutdown_refused` so operators can distinguish saga failures
caused by shutdown coincidence from saga failures caused by genuine
commit conflicts on the same operator dashboard.

Caller contract: treat the `LatticeShuttingDownException` as back-
pressure. The entries the saga carried were never durably committed,
but the silo refused to accept them because the host is going away
rather than because the storage layer rejected them. Long-lived
clients should either fail over to a peer silo (if the cluster is
multi-node) or surface the back-pressure to upstream callers (drop
the request, queue it to a side outbox, or rate-limit). Re-issuing
the same `operationId` after the host restarts is the normal recovery
path: the saga's persisted state was never committed past the
prepare phase, so the re-issued saga runs against a fresh silo
activation as a brand-new saga. See [API Reference - Shutdown back-
pressure](api.md#shutdown-back-pressure---latticeshuttingdownexception)
for the cross-feature contract.

The saga also opportunistically quiesces on the per-tree
`IWalSaturationSignal` before each batched dispatch when the signal
reports `Saturated`. The wait is capped at a small saga-local budget
so the saga's own per-attempt deadline always wins on a tree that
never recovers, and is silently skipped when no
`IWalSaturationSignal` is registered in DI. The gate prevents the
saga from burning retry budget against a writer-side admission
semaphore that is provably parked, in addition to the terminal
shutdown-refused fast-fail above.

## Caller-supplied idempotency keys

The default `SetManyAtomicAsync(entries)` overload generates a fresh
`Guid` per call as the saga's `operationId`. On a transport-level failure
(silo restart mid-call, client-side timeout, transient network error) the
client has no way to re-attach to the in-flight saga and must treat the
call as failed - the original saga may still commit server-side.

The `SetManyAtomicAsync(entries, operationId)` overload takes a stable
caller-supplied idempotency key, which maps directly to the saga grain
identity (`{treeId}/{operationId}`). Re-submitting the same
`operationId` re-attaches to the original saga:

- If it has already reached a terminal state, the second call returns
  immediately (or rethrows the original terminal failure) - the client
  observes the original outcome.
- If it is still in flight, the second call awaits the saga's terminal
  state.

This turns a transport-level failure into a recoverable client-side retry
- the caller simply calls again with the same `operationId`.

```csharp verify
string orderId = "42";
string customerId = "7";

// Stable per-business-operation idempotency key.
var operationId = $"order-{orderId}";
var entries = new List<KeyValuePair<string, byte[]>>
{
    new($"order:{orderId}:state",    Encoding.UTF8.GetBytes("paid")),
    new($"customer:{customerId}:last-order", Encoding.UTF8.GetBytes(orderId)),
};

// Safe to retry on timeout: the saga is bound to operationId, not to
// this RPC attempt.
await tree.SetManyAtomicAsync(entries, operationId, cancellationToken);
```

### Key-set stability

An `operationId` is bound to the exact **set of keys** submitted on its
first call. The saga persists a SHA-256 fingerprint of the sorted key
set during `Prepare`; a subsequent call reusing the same `operationId`
with a different key set (added, removed, or renamed keys) throws
`LatticeIdempotencyKeyMismatchException` (a subclass of
`InvalidOperationException`). Reordering keys or changing their values
is allowed - the fingerprint hashes the sorted key list only, so the
same logical retry with a slightly-different serialized payload is
accepted as idempotent.

### Validation

- `operationId` must be non-null, non-empty, and non-whitespace.
- `operationId` must not contain `'/'` - reserved as the grain-key
  separator between tree ID and operation ID.

Both constraints throw `ArgumentException` at submission time.

### Retention window

Completed saga state is retained for
`LatticeOptions.AtomicWriteRetention` (default 48 hours) so delayed
retries within the window still observe the original outcome. Only the
lightweight outcome fields are retained; the staged batch payload is
released on the terminal checkpoint (see [Phase 4 - Complete](#phase-4---complete)),
so the retained footprint per completed saga is bounded to its outcome and
does not scale with the batch's value size. After
the window, the saga grain's state is cleared and its activation
deactivates - the same `operationId` then becomes eligible for a fresh
saga. Set the option to `Timeout.InfiniteTimeSpan` to disable
retention cleanup (completed saga state then lives forever, at the
cost of unbounded growth in the number of retained outcome rows).

## Ambient context capture-once

A caller that wraps `SetManyAtomicAsync` in
`LatticeVectorClockContext.With(...)` (or `LatticeOriginContext.With(...)`)
has the ambient frontier captured **once** on the saga's first `Prepare`
and re-stamped onto every per-key write the saga issues during `Execute`
- including any compensation rewrites, which restore each key's
pre-saga origin and frontier captured alongside its pre-saga value. The
saga guarantees that every emit in the batch carries the **identical**
`VectorClock` (and identical `OriginClusterId`), closing per-key drift a
remote replication consumer would otherwise see as a partial-set state
where the writer's frontier said all N keys should be visible together.

The captured frontier is durable: a silo crash mid-saga resumes from
persisted state and re-stamps the persisted ambient on every remaining
emit, so observers see the same VC across the original commits and the
post-recovery emits.

```csharp verify
var vc = new Orleans.Lattice.VersionVector();
vc.Tick("origin-peer");

using (LatticeVectorClockContext.With(vc))
{
    await tree.SetManyAtomicAsync(new List<KeyValuePair<string, byte[]>>
    {
        new("k1", Encoding.UTF8.GetBytes("v1")),
        new("k2", Encoding.UTF8.GetBytes("v2")),
        new("k3", Encoding.UTF8.GetBytes("v3")),
    });
}
// Every per-key mutation observed downstream carries the identical
// VectorClock, regardless of how the saga's writes were interleaved.
```

## Atomic-batch metadata on emitted mutations

In addition to the saga-wide `VectorClock` and `OriginClusterId`, every
per-key `LatticeMutation` the saga emits also carries
`AtomicBatchSize` (the total entry count of the enclosing transaction)
and `AtomicBatchIndex` (the zero-based per-key position within the
batch). The size is captured once on the first `Prepare` from
`Entries.Count`, persisted on the saga grain's state alongside the
existing capture-once slots, and re-stamped onto Orleans
`RequestContext` via the ambient `LatticeAtomicBatchContext` helper at
the head of every per-key call the saga issues - including
compensation rolls, which inherit the original prepare's index for
each key. Single-key writes outside a saga emit `0` / `0` (the
"not-in-a-saga" sentinel).

These two slots are **observability metadata about the saga's shape**
- they let a downstream observer (a change-feed consumer, a
mutation-observer pipeline, an audit log) recognise that several
mutations belong to the same enclosing batch and reason about
batch-level invariants without coordinating with the producer. They
are *not* the atomicity primitive: tree-wide atomic visibility is
delivered by the per-tree `ITxRegistryGrain` + per-leaf pending-tx
mechanism described above. The `AtomicBatchSize` and
`AtomicBatchIndex` slots remain reserved for future receiver-side
batch optimisations but are not consumed by the current apply path.
See [Consistency: Atomic visibility](consistency.md#atomic-visibility).

## Cross-cluster atomic visibility

**Cross-cluster atomic visibility ships universally** through the
same per-tree transaction-registry linearization point used locally.
Prepared writes ride the standard per-key WAL and replication transport
carrying an additive `IsPrepared` slot and the saga's
`TransactionId`; every per-shard `TxCommit` / `TxAbort` terminal mark
ships as a single record exempt from the producer-side per-key
filter. On the receiver, prepared records install each entry into the
destination leaf's per-tx pending bucket. Terminal records gate the
per-tree linearization flip
on a per-source-shard arrival tally before fanning the terminal out
to the receiver's leaves.

### Multi-shard receiver gate

A saga that touched **N** source shards emits **N** independent
`TxCommit` (or `TxAbort`) WAL records, one per source shard. Each one
ships through the change feed under its own backpressure / batching
cadence, so the receiver can observe them in any order and arbitrarily
spaced in wall-clock time. The receiver must therefore hold back the
per-tree visibility flip until it has seen every per-source-shard
terminal; otherwise a remote reader concurrent with replication can
observe post-saga values on the drained-shard keys and pre-saga
values on the still-pending-shard keys - a partial-batch visibility
split the atomicity contract makes impossible.

The producer stamps the saga's authoritative touched-shard count on
each terminal record via the additive
`LatticeMutation.AtomicShardCount` slot. The slot is published by
`AtomicWriteGrain.MarkOneShardAsync` through the ambient
`LatticeAtomicShardCountContext` and read inside
`ShardRootGrain.AppendTxTerminalAsync` at the moment the terminal
mutation is assembled - so a producer-side mid-saga shadow-forward
split that grows the touched-shard set between successive per-shard
terminals stamps the post-growth count on the later terminals and the
receiver adopts the larger value (`max(seen, incoming)`) without ever
under-counting.

The receiver-side `TxRegistryGrain.RecordTerminalArrivalAsync(txid,
sourceShardIndex, committed, atomicShardCount)` deduplicates arrivals
per `(txid, sourceShardIndex)` into a persistent
`TerminalArrivals: Dictionary<Guid, HashSet<int>>` slot and tracks
the expected total in `ExpectedTerminals: Dictionary<Guid, int>`. The
call returns a `TerminalTallyResult` carrying:

| Field | Meaning |
|---|---|
| `IsFinal` | `true` once `arrivals.Count >= max(expected, atomicShardCount)` for the txid. |
| `Committed` | The saga's outcome (commit or abort), latched on the first arrival. A mismatched-outcome arrival preserves the earlier abort. |
| `ObservedSourceShards` | The full union of per-source-shard indices seen so far. Populated only on the final arrival; empty otherwise so the non-final path allocates no array. |

`LatticeGrain.ApplyTxTerminalAsync` consults the tally first. A
non-final tally leaves the per-tree linearization mark unset and the
receiver leaves' pending buckets undrained - readers dialling back
through `ITxRegistryGrain.GetStatusAsync` observe `InFlight` for the
whole saga and fall through to the pre-saga value. Only on the final
arrival does the receiver:

1. Mark the registry's decision (`MarkCommittedAsync` /
   `MarkAbortedAsync`) - the single per-tree visibility flip.
2. Resolve the transitive split-forward closure of every observed
   source-shard via `TerminalFanOutResolver` and dispatch the
   per-shard terminal mark to each destination in a single parallel
   hop, under the saga's source HLC and origin-cluster id.

The gate handles producer/receiver shard-count divergence naturally
because the tally key is the **producer**-stamped source-side shard
index, not the receiver's shard layout. A receiver whose adaptive
splits or operator resize have produced a different shard count than
the source still observes exactly `atomicShardCount` distinct source
terminals (one per source shard the saga touched).

The slots are forward- and backward-compatible across mixed-version
deployments. A legacy persisted `TxRegistryState` with no
`TerminalArrivals` / `ExpectedTerminals` decodes to empty
dictionaries (the correct "no terminals tallied yet" default). A
receiver consuming records from a legacy producer that never stamps
`AtomicShardCount` sees `atomicShardCount == 0` on every arrival,
which the gate treats as "no expected-total information" and falls
back to first-terminal-wins semantics - equivalent to the
pre-gate behaviour. `ForgetAsync` clears both slots alongside the
decision entry, so the persisted footprint stays bounded by the
in-flight + recently-completed saga set.

### Prepared writes never enter the receiver's batched merge path

The receiver's `ReplicationApplier.ApplyBatchAsync` classifies
inbound `WalRecord` entries into a fast-path batched LWW merge
(`ApplyMergeManyAsync`) and a per-entry fallback (`ApplyPointAsync`).
The classifier predicate explicitly excludes any entry with
`IsPrepared == true`: prepared `Set` / `Delete` records are forced
onto the per-entry path, where they reach
`ApplyPreparedSetAsync` / `ApplyPreparedDeleteAsync` and land in the
destination leaf's `_pendingTx` bucket. Unprepared writes continue to
consume the batched merge path. Without this exclusion, prepared
writes would commit directly into the receiver leaf's visible
`Entries` and the saga's terminal mark would find no matching pending
entries to flip - so the cross-cluster reader would observe the
prepared write as visible *before* the registry gate ever flipped,
purely as a function of whether the inbound run happened to be
batched (steady-state load) or single-entry (cold start). The
exclusion makes the strict-isolation invariant independent of the
receiver's batching cadence.

### Read cache: cursor-based delivery, not HLC-based

A cross-cluster apply on the receiver preserves the **source**
cluster's HLC verbatim on every committed entry - the destination
leaf does not bump the source HLC into its local clock, because LWW
convergence across clusters with skewed wall clocks requires the
authoring cluster's HLC to remain the comparator. Whenever the
source HLC is below the destination leaf's already-published
`Version[ReplicaId]`, an HLC-based delta filter on the read cache
silently drops the entry from the per-key delta and continues
serving the stale local value indefinitely.

To decouple cache delivery from LWW HLC ordering,
`LeafCacheGrain.RefreshAsync` pulls from the primary leaf via the
internal `IBPlusLeafGrain.GetDeltaSinceCursorAsync(LeafDeliveryCursor)`
seam rather than the legacy HLC-keyed delta. The cursor is an
activation-scoped `(Epoch, Sequence)` pair: `Sequence` is bumped
once per `StoreEntry` / `RemoveEntry` on the leaf - regardless of the
write's LWW HLC - so the cache pulls every write strictly newer than
its last delivered sequence even when the underlying HLC has rewound.
`Epoch` is bumped on every leaf activation, so a cache holding a
stale cursor across a leaf re-activation falls back to a full-
snapshot delivery on its next refresh. The cursor is intentionally
non-persistent: the WAL replay path remains the sole projection
source-of-truth, and the cursor adds zero per-write durable I/O. The
cache also delegates reads back to the primary leaf for any cached
row carrying `IsMigrated = true`, so the leaf-side shadow guard
protecting an in-flight cross-shard migration is never bypassed by
the cache fast path.

## Cross-tree (multi-tree) atomic writes

`SetManyAtomicAsync` is bound to a single tree. To commit a batch that
spans **two or more distinct `ILattice` trees** all-or-nothing, use the
`IGrainFactory.SetManyAtomicAsync` extension (or the
`BeginAtomicWrite` fluent builder). The cross-tree primitive extends the
same atomic-visibility guarantee the single-tree saga gives *within* a
tree to a set of trees: either every targeted key across every
participating tree becomes visible, or none of them do - observed
atomically by readers on the local cluster and on every cluster the
trees replicate to.

### Public surface

| Member | Purpose |
|---|---|
| `IGrainFactory.SetManyAtomicAsync(IReadOnlyList<LatticeTreeBatch>, operationId, ct)` | Commit per-tree slices atomically; returns a `CrossTreeAtomicWriteOutcome`. |
| `IGrainFactory.BeginAtomicWrite(operationId)` | Open a `LatticeAtomicWriteBuilder` for fluent, per-tree staging. |
| `LatticeAtomicWriteBuilder.Delete(key)` | Stage a retraction (tombstone) for `key` under the current `ForTree(...)`; rides the saga and flips jointly with sibling `Set` calls. |
| `LatticeTreeBatch(TreeId, Entries, Predicate = null, EntryDeltas = null, EntryDeletes = null)` | One tree's slice: tree id, key/value entries, optional server-side guard predicate, optional per-entry CRDT deltas, and an optional parallel per-entry is-delete channel (a `true` slot tombstones its key). |
| `CrossTreeAtomicWriteOutcome` | `Committed` (all trees committed) or `PreconditionFailed` (a guard failed; nothing committed anywhere). |

A stable `operationId` is **required** (there is no auto-generated
overload): a cross-tree saga touches multiple registries, so a stable
idempotency key is mandatory for safe retry. The `operationId` must not
contain `/` (reserved as the grain-key separator). Tree ids in the batch
must be distinct and non-empty.

### Usage

```csharp verify
// Fluent builder: atomically move an order between two trees, only if
// the source order's total still clears a threshold.
var outcome = await grainFactory
    .BeginAtomicWrite("xfer:txn-1001")
    .ForTree("orders-east")
        .SetWhere("order:42", new Order("42", 0m), o => o.Total >= 100m)
    .ForTree("orders-west")
        .Set("order:42", new Order("42", 250m))
    .CommitAsync(cancellationToken);

if (outcome == CrossTreeAtomicWriteOutcome.PreconditionFailed)
{
    // The guard failed; nothing was committed on either tree.
}
```

```csharp verify
// Mixed set + delete across trees: move a row from one tree's key to
// another's, retracting the old key inside the same cross-tree saga.
await grainFactory
    .BeginAtomicWrite("rekey:order-42")
    .ForTree("orders-east")
        .Delete("order:42")
    .ForTree("orders-west")
        .Set("order:42", new Order("42", 250m))
    .CommitAsync(cancellationToken);
```

```csharp verify
// Explicit batch list form.
var batches = new List<LatticeTreeBatch>
{
    new("orders", new List<KeyValuePair<string, byte[]>>
    {
        new("order:42/status", Encoding.UTF8.GetBytes("shipped")),
    }),
    new("inventory", new List<KeyValuePair<string, byte[]>>
    {
        new("sku:99/reserved", Encoding.UTF8.GetBytes("0")),
    }),
};

CrossTreeAtomicWriteOutcome result =
    await grainFactory.SetManyAtomicAsync(batches, "fulfil:order-42", cancellationToken);
```

### Coupling a CRDT mutation into an atomic write

A staged CRDT write lets a typed CRDT mutation ride a cross-tree atomic
write so it commits all-or-nothing alongside sibling last-writer-wins
(LWW) writes on other trees.

Each CRDT accessor exposes a `Stage*` counterpart for every live
mutator. A `Stage*` call:

1. reads the key's current snapshot once,
2. mints the typed CRDT delta **once** (the same dot-minting logic the
   live mutator uses),
3. folds the delta into the snapshot to produce the merged state, and
4. returns a `LatticeStagedCrdtWrite` carrying the key, the serialized
   merged state (the **value**), and the serialized typed delta.

It performs **no** durable write - the saga owns the commit. Hand the
token to the builder's `Set(LatticeStagedCrdtWrite)` overload under the
`ForTree(...)` whose configured merge mode matches the accessor's
primitive (the accessor was obtained from that same tree).

```csharp verify
// Stage a PnCounter increment from a CRDT-mode tree's accessor. Staging
// mints the typed delta once and folds it into a merged snapshot; it does
// not write durably - the saga owns the commit.
var metrics = grainFactory.GetGrain<ILattice>("metrics");
LatticeStagedCrdtWrite staged =
    await metrics.PnCounter("orders:placed")
                 .StageIncrementAsync("replica-east", 1, cancellationToken);

// Couple the staged CRDT mutation with a sibling LWW Set on another tree.
// Either both land or neither does.
var outcome = await grainFactory
    .BeginAtomicWrite("place-order:1001")
    .ForTree("metrics").Set(staged)
    .ForTree("orders").Set("order:1001", new Order("1001", 250m))
    .CommitAsync(cancellationToken);

if (outcome == CrossTreeAtomicWriteOutcome.Committed)
{
    // The merged counter value is readable locally right away.
}
```

**Convergence and consistency contract.** The merged value stored by the
saga is computed from a stage-time snapshot and stored LWW by HLC. Reads
on the authoring cluster see that merged value immediately, and it
replicates to every peer through the cross-tree saga's prepared /
terminal path. A single authoring cluster's staged CRDT write therefore
converges everywhere on its merged value.

Two concurrent staged CRDT writes to the **same key** - whether in the
same cluster or on different clusters - converge by the **per-replica
typed-delta union**, identical to the live (non-atomic) accessor path.
The cross-tree atomic write ships the staged typed delta and merge mode
alongside the merged-state value on the two-phase prepared / terminal
replication path, and the receiver **folds the typed delta into its
current visible state** (the primitive's `MergeDelta`) on the saga's
terminal commit rather than installing the prepared value
last-writer-wins. So a counter written `+5` on one cluster and `+3` on
another converges to `8` on **both** clusters, exactly as the live
`IncrementAsync` accessor would. The same holds for the internal
tag-index flag-membership rows, which use the same per-entry carry: an
active-active membership add on each cluster converges to the union
through the atomic (prepared) path, not only the eventual accessor path.

Value-only sagas - a plain `Set(key, bytes)` slice with no staged CRDT
delta - stay on the last-writer-wins prepared path unchanged: the highest
HLC wins, because there is no typed delta to fold.

**Compensation.** An aborting saga drops the staged value **and** the
staged delta on every cluster (the prepare-phase write never became
visible), so there is no byte-inverse compensation: abort means the
mutation never happened.

### How it works - two-level saga

A single **coordinator grain** keyed by `operationId` is the global
decision authority. The flow is:

1. **Prepare-and-pause.** Each participating tree runs the existing
   single-tree saga in a new prepare-and-pause mode: it stages the
   prepared writes into the per-leaf pending-tx buckets, registers a
   *delegation* mapping its local txid to the coordinator, then pauses
   without broadcasting any terminal. Each tree votes `Prepared` (or
   `PreconditionFailed` if its guard missed).
2. **Single global decision.** Once every tree has voted, the
   coordinator writes **one** decision - `Committed` or `Aborted` - to
   its own persistent state. This single write is the cross-tree
   linearization point.
3. **Finalize.** The coordinator fans out a `Finalize` call to every
   tree's saga, which marks its per-tree registry and broadcasts the
   per-shard terminals exactly as the single-tree saga does.

Between prepare and the coordinator's decision, every participating
tree's registry *delegates* the status of its prepared txid to the
coordinator. A reader dialling a per-tree registry
(`GetStatusAsync` / `GetStatusManyAsync` / `SnapshotAsync`) for a
delegated txid resolves it against the coordinator and caches the
terminal verdict locally. Before the coordinator decides, every
delegated read returns `InFlight`, so the prepared keys are invisible
(indistinguishable from pre-saga); after it decides, every tree returns
the **same** global verdict. The global visibility flip is therefore the
coordinator's single decision write, applied uniformly across all trees.

### Cross-cluster cross-tree visibility (receiver barrier)

The authoring-cluster flip above is a *single* write, but each
participating tree's terminals replicate to remote clusters on their
**own** per-tree WAL feed. A receiver can therefore apply tree A's
terminal long before tree B's terminal arrives. Without a receiver-side
barrier a remote reader could observe tree A committed while tree B is
still pre-saga - a partial cross-tree view the authoring cluster never
exposes.

The receiver closes this gap with an internal **receiver coordinator
grain** (`ILatticeCrossTreeReceiverGrain`), distinct from the
authoring-side coordinator and keyed by the compound
`(originClusterId, operationId)`. Each tree's cross-tree terminal carries
the operation id and the participant tree-set (`WalRecord`'s
`CrossTreeOperationId` / `CrossTreeParticipants`). When a tree's per-shard
gate completes on the receiver, instead of flipping that tree's registry
immediately the receiver:

1. durably registers the tree's local txid as **delegated to the receiver
   coordinator** in the per-tree registry, then
2. notifies the receiver coordinator that this tree's terminal has
   arrived (with its commit/abort vote).

The receiver coordinator holds a frozen **wait set** of the trees it must
hear from and decides only once a terminal has arrived for every tree in
the set; the global verdict is `Committed` iff every arrived terminal
voted commit. Before the coordinator decides, a delegated read on any
participating tree's registry dials the receiver coordinator and resolves
`InFlight` - so every tree stays invisible. After it decides, every tree
resolves the same verdict and the receiver flips them **together**,
mirroring the authoring cluster's single-write flip. The coordinator only
ever *returns* the decision (it never calls back into a tree grain), so
the calling `LatticeGrain` performs the per-tree finalizes - itself inline
and siblings via their apply grains - without any circular wait.

**Partial replication is valid.** The wait set is scoped to the trees
actually replicated on the receiver: it is the intersection of the
batch's participant set with the receiver's configured replicated trees
(`LatticeReplicationOptions.ReplicatedTrees`). The tree that received the
terminal is always in the set. A participating tree that is **not**
replicated on this receiver is simply excluded, so a cross-tree batch
spanning a mix of replicated and non-replicated trees completes its
barrier on the present subset rather than blocking forever on a tree that
will never arrive. The receiver thus preserves cross-tree atomic
visibility across exactly the trees it hosts, whatever subset of the
batch that is.

### Guarantees and non-guarantees

- **All-or-nothing commit across trees.** On a `Committed` outcome every
  key on every participating tree holds its target value; on
  `PreconditionFailed` nothing is committed on any tree. A mid-flight
  write failure compensates every tree and throws
  `InvalidOperationException`.
- **Crash recovery.** The coordinator grain drives the saga to a
  terminal state via a keepalive reminder if its silo crashes mid-flight,
  exactly as the single-tree saga does.
- **Idempotent retry.** Re-submitting the same `operationId` with the
  same tree-set and key-set re-attaches to the in-flight (or completed)
  saga and returns its memoized outcome. Re-submitting the same
  `operationId` with a *different* tree-set or key-set throws
  `LatticeIdempotencyKeyMismatchException`.
- **Atomicity, not cross-tree read isolation.** The guarantee is
  all-or-nothing *commit* of the write, anchored to the coordinator's single
  decision write as one global linearization point: at any single instant the
  saga is either undecided (every tree returns `InFlight`) or decided (every
  tree returns the same verdict), never durably half-applied. It is **not** a
  cross-tree read snapshot: Lattice has no read operation spanning trees, so a
  reader issuing *separate* per-tree reads at *different* instants that
  straddle the linearization point may legitimately compose a mixed view (one
  tree's pre-saga value, another tree's post-saga value), exactly as two
  independent `SELECT`s under read-committed isolation can. What it can never
  observe is a single tree showing a *partial* slice of one cross-tree saga:
  within any one read, every saga key on that tree resolves against one global
  verdict.

The cross-tree workload is exercised under concurrent shard splits in
the chaos suite - see
[Chaos Tests: Test 11](chaos-tests.md#test-11---cross-tree-atomic-write-under-shard-churn-chaoscrosstreeatomicwriteintegrationtests).

## Related

- [API Reference](api.md) - full `SetManyAtomicAsync` signature and typed
  extensions.
- [State Primitives](state-primitives.md) - HLC and LWW semantics the saga
  relies on for compensation correctness.
- [Chaos Tests](chaos-tests.md) - atomic-write workload exercised under
  concurrent splits and fault injection.
- [Atomic Action](atomic-action.md) - the generic saga / TCC coordinator that
  layers arbitrary forward/compensate steps over this atomic-write machinery.
