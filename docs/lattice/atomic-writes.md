# Atomic Multi-Key Writes

`ILattice.SetManyAtomicAsync(entries)` commits a batch of key-value pairs with
all-or-nothing semantics: either every entry in the batch is durably written,
or - on any failure - every already-committed entry in that batch is rolled
back to its pre-saga value. The feature is implemented as a saga coordinator
grain that wraps the existing per-key `SetAsync` path.

The non-atomic `SetManyAsync` remains available for throughput-oriented use
cases where partial application on failure is acceptable.

## Atomicity Guarantees

This section describes the **atomicity** contract of the saga. For the
reader-visibility model - the per-tree `ITxRegistryGrain` linearization
point and how each read path consults it - see
[Consistency: Atomic visibility](consistency.md#atomic-visibility-single-tree-foreground-and-cross-cluster).
The atomicity guarantee is **universal**: it holds within a single
tree on the local cluster *and* across every cluster the tree
replicates to. Receiver-side replication routes prepared writes and
the per-shard terminal mark through the same `ITxRegistryGrain`
linearization point used locally, so a remote reader concurrent with
replication of a `SetManyAtomicAsync` observes either zero or all of
the saga's keys - never a partial view (see Consistency for details).

Given a batch `[(k₀, v₀), (k₁, v₁), …, (kₙ₋₁, vₙ₋₁)]`, a successful
`SetManyAtomicAsync` call guarantees:

1. **All-or-nothing commit.** On successful return every `kᵢ` holds `vᵢ` as
   its last-writer-wins (LWW) value, or - if the saga failed - every `kᵢ`
   holds the value it had before the saga started (its pre-saga value; for
   keys that did not exist before the saga, the key is tombstoned).
2. **Sequential per-key ordering.** Each key is written at most once by the
   saga, with a monotonically-increasing [Hybrid Logical Clock](state-primitives.md)
   timestamp. Compensation writes use a fresh HLC tick, so LWW merge resolves
   the rollback as the winner even if an external writer raced the saga.
3. **Crash recovery.** If the silo hosting the saga grain crashes mid-flight,
   a keepalive reminder resumes the saga on reactivation and drives it to a
   terminal state (either Completed or Compensated + Completed).
4. **Input validation.** Duplicate keys and null values fail fast with
   `ArgumentException` before any write is attempted. An empty batch returns
   immediately without contacting any leaf grain.
5. **Idempotent client retry.** A re-invocation of the same saga grain (same
   `treeId` + `operationId`) after successful completion returns success
   without re-executing. A re-invocation after a compensated-failure replays
   the original `InvalidOperationException` with the preserved failure
   message.

### What `SetManyAtomicAsync` does **not** guarantee

- **Cross-tree atomicity.** The per-tree `ITxRegistryGrain` is the unit
  of atomic visibility. A `SetManyAtomicAsync` call is bound to a single
  tree (`ILattice`) and gives strict atomic visibility *within* that
  tree only. Operations that span multiple trees (separate `ILattice`
  instances) require application-level coordination - Lattice does not
  offer a cross-tree saga primitive.
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

## How It Works

### Saga Coordinator Grain

Each call to `SetManyAtomicAsync` routes through `LatticeGrain` to a freshly
created `AtomicWriteGrain` activation keyed by `{treeId}/{operationId}`,
where `operationId` is a GUID generated per call. The grain persists a single
`AtomicWriteState` POCO whose phase transitions drive the saga lifecycle:

```
NotStarted ──► Prepare ──► Execute ──► registry.MarkCommitted ──► fan-out ──► Completed   (success)
                              │
                              └──► Compensate ──► registry.MarkAborted ──► fan-out ──► Completed   (failure)
```

The persisted `AtomicWritePhase` enum tracks `NotStarted, Prepare,
Execute, Compensate, Completed`; the `registry.Mark*` and `fan-out`
steps run synchronously inside `RunSagaAsync` between the
Execute / Compensate phase exit and the final `Phase = Completed`
write. **The registry write is the single tree-wide visibility flip**
(see [Consistency: Atomic
visibility](consistency.md#atomic-visibility-single-tree-foreground-and-cross-cluster));
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

The coordinator walks the batch in order, calling `ILattice.SetAsync(key,
value)` for each entry under an ambient `LatticePreparedContext.BeginScope()`.
The leaf grain's commit pipeline observes the prepared-context flag and
routes each per-key write into its in-memory `_pendingTx[txid]` bucket
rather than into the visible `Entries` projection - so prepared writes
are **invisible to concurrent readers** for the duration of the
prepare window. `NextIndex` is incremented and persisted after every
successful step, so crash-resume can pick up exactly where it left off
without replaying committed prepares. Each step has a bounded retry
budget (`MaxRetriesPerStep = 1`); a persistent fault pivots the saga
into `Compensate` without re-throwing.

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
shard root the saga touched (`ShardRootGrain.AppendTxTerminalAsync`
→ per-leaf `ApplyTxCommit` / `ApplyTxAbort`). The terminals drain
each leaf's pending-tx bucket into visible `Entries` (commit) or drop
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
| During `Compensate`, after *m* rollbacks | `Phase = Compensate`, `NextIndex = N − m` | Reminder tick resets `RetriesOnCurrentStep`, continues compensation, then completes. |
| After `Completed` persists | `Phase = Completed` | Reminder tick unregisters itself and deactivates. |

## Performance Notes

- A saga of size *N* issues approximately *2N* + 3 Orleans calls: *N*
  pre-saga reads, *N* writes, and 3 `WriteStateAsync` calls on the saga's own
  state - plus one registry write (`MarkCommittedAsync` /
  `MarkAbortedAsync`), one per-shard terminal fan-out RPC, and one
  registry `ForgetAsync` cleanup. For large batches where atomicity is
  not required, prefer the parallel `SetManyAsync`.
- `AtomicWriteState` is stored under the Lattice storage provider
  (`"OrleansLattice"`). The saga grain deactivates on completion, so the
  storage row is typically read exactly once (on activation) and written
  four times (Prepare → Execute → … → Completed).
- Readers observing a saga in flight pay one extra registry RPC per
  pending key: `BPlusLeafGrain.ResolvePendingStatusAsync` for direct
  single-key reads, a single batched `GetStatusManyAsync` per leaf for
  scans, or a single `SnapshotAsync` per multi-shard fan-out (stamped
  onto an ambient context so every leaf in the scan reuses it). The
  coordinator does not block, lock, or serialise reads - the registry
  RPC is the entire visibility cost.

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
`InvalidOperationException`. Reordering keys or changing their values
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
retries within the window still observe the original outcome. After
the window, the saga grain's state is cleared and its activation
deactivates - the same `operationId` then becomes eligible for a fresh
saga. Set the option to `Timeout.InfiniteTimeSpan` to disable
retention cleanup (completed saga state then lives forever, at the
cost of unbounded storage growth).

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
var vc = new Orleans.Lattice.Primitives.VersionVector();
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
`Operations.Count`, persisted on the saga grain's state alongside the
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
See [Consistency: Atomic visibility](consistency.md#atomic-visibility-single-tree-foreground-and-cross-cluster).

## Cross-cluster atomic visibility

**Cross-cluster atomic visibility ships universally** through the
same per-tree `ITxRegistryGrain` linearization point used locally.
Prepared writes ride the standard per-key WAL → replication transport
carrying an additive `IsPrepared` slot and the saga's
`TransactionId`; every per-shard `TxCommit` / `TxAbort` terminal mark
ships as a single record exempt from the producer-side per-key
filter. On the receiver, prepared records route to
`IReplicationApplyGrain.ApplyPreparedSetAsync` /
`ApplyPreparedDeleteAsync`, which install each entry into the
destination leaf's per-tx pending bucket. Terminal records route to
`ApplyTxTerminalAsync`, which gates the per-tree linearization flip
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

## Related

- [API Reference](api.md) - full `SetManyAtomicAsync` signature and typed
  extensions.
- [State Primitives](state-primitives.md) - HLC and LWW semantics the saga
  relies on for compensation correctness.
- [Chaos Tests](chaos-tests.md) - atomic-write workload exercised under
  concurrent splits and fault injection.
