# Replication apply seam (`IReplicationApplier`)

`IReplicationApplier` is the public, in-process inbound seam over the per-tree apply pipeline. It installs a single `WalRecord` authored on a remote cluster onto the local tree while preserving the remote cluster's `HybridLogicalClock` and origin id end-to-end, and it filters re-delivery via a per-origin high-water-mark so at-least-once transports become at-most-once apply.

The contract is deliberately neutral: there is no transport binding, no per-peer state, no ack envelope. It is the seam custom transports and integration tests plug into.

## API

The interface and result type live in `Orleans.Lattice.Replication`:

```text
public interface IReplicationApplier
{
    Task<ApplyResult> ApplyAsync(
        WalRecord entry,
        CancellationToken cancellationToken = default);

    Task<ApplyResult> ApplyBatchAsync(
        IReadOnlyList<WalRecord> entries,
        CancellationToken cancellationToken = default);
}

public readonly record struct ApplyResult
{
    public bool Applied { get; init; }
    public HybridLogicalClock HighWaterMark { get; init; }
}
```

| `ApplyResult` member | Semantics |
|---|---|
| `Applied` | `true` when the entry was merged onto the local tree; `false` when the entry was filtered out as a re-delivery (its `Timestamp` was at or below the per-origin high-water-mark) or rejected as inapplicable (its `OriginClusterId` matched the local cluster id and would have looped). For batch calls, `true` if **any** entry in the batch was newly merged. |
| `HighWaterMark` | For point applies (`Set` / `Delete`) this is the per-origin HWM after the call - equal to `entry.Timestamp` when `Applied` is `true`, or the HWM that suppressed the apply when `Applied` is `false`. For range deletes and local-origin no-op rejections - neither of which consults the HWM - this is `HybridLogicalClock.Zero`. For batch calls, the pointwise maximum HWM across every distinct origin in the batch. |

## Apply semantics

Three concerns the applier composes for every call:

### 1. Source-HLC and origin preservation

For `LwwRegister` mode point applies route through the core library's apply seam, which persists the entry's `LwwValue<byte[]>` with the supplied `Timestamp` and `OriginClusterId` **verbatim** - no fresh local HLC is stamped. This is what unlocks transitive replication (A → B → C with A's HLC intact) and deterministic LWW resolution against concurrent local writes.

For typed CRDT modes (`OrSet`, `PnCounter`, `VersionVector`, `MvRegister`, `OrMap`) the applier reads the typed delta DTO from `WalRecord.Delta` (the producer authored it via the accessor at commit time), reads the locally-stored primitive under optimistic concurrency, calls the primitive's instance `MergeDelta(delta)` operation, and writes the merged state back. `WalRecord.Value` is preserved alongside `Delta` for change-feed back-compat but is not consulted on the CRDT apply path. The merge is wrapped in a `LatticeOriginContext.With(originClusterId)` scope so the receiver's commit-time observer publishes the foreign origin and the producer-side ship loop filters the resulting entry out. The persisted `HybridLogicalClock` is a fresh local tick (representing the merge point) - this is correct for typed-delta CRDTs, where the delta itself carries the per-replica causal context. For `OrMap` mode the receiver resolves the concrete `(TKey, TValue)` shape through `OrMapShapeRegistry`; an `OrMap`-mode apply against an unregistered tree faults with a clear configuration-error message.

Range deletes carry `HybridLogicalClock.Zero` by design (a range walk produces many per-leaf HLCs that cannot be faithfully collapsed into a single timestamp). The receiver walks the leaf chain locally and stamps each tombstone with a freshly-ticked local HLC; the remote `OriginClusterId` rides through an ambient `LatticeOriginContext` scope so the receiver-side change-feed observer publishes it on every emitted `LatticeMutation`.

### 2. Per-origin high-water-mark dedupe

The applier resolves a per-origin high-water-mark for every `(TreeId, OriginClusterId)` pair it sees. Before applying a point entry it reads the current HWM; if `entry.Timestamp <= hwm` the call is a no-op (`Applied = false`). After a successful point apply it advances the HWM monotonically - concurrent appliers that race ahead leave the HWM higher and the laggard's advance becomes a no-op, exactly the semantics at-most-once apply requires. Typed CRDT modes consult and advance the HWM the same way, even though state-based merge is naturally idempotent - the dedupe just short-circuits redundant grain calls.

Range deletes bypass the HWM by design. Range applies are naturally idempotent at the leaf layer: re-running a range delete on already-tombstoned keys merges to the same state, so dedupe is unnecessary.

### 3. Local-origin defence-in-depth

A `WalRecord` whose `OriginClusterId` matches the local cluster id is rejected as a no-op (`Applied = false`). The outbound ship loop's origin filter already prevents this in steady state, but hand-built apply pipelines and tests can still hand the applier such an entry - surfacing it as an explicit rejection rather than silently merging it into the same cluster's state is the safer default.

### 4. Shadow-forward dedupe cache

A structural rewrite (shard split, shard merge, saga compensate) that shadow-forwards a user write into a different shard generates a duplicate-emit pair: one entry from the originating shard's commit, one from the shadow-forwarded shard's commit, both carrying identical `(originClusterId, timestamp, key, op)` identity tuples. The per-origin HWM check catches the second delivery when it is sequential (the first has already advanced the HWM), but a concurrent inbound delivery can otherwise observe the same pre-advance HWM on both deliveries and both pass before either advances it.

The applier holds a per-tree bounded FIFO cache of recently-applied identity tuples (`LatticeReplicationOptions.ShadowForwardDedupeCacheSize`, default `4096`, validator floor `64`). The cache is consulted *after* the per-origin HWM dedupe so HWM-deduped entries do not pollute it (which preserves operator-driven re-pin semantics where lowering the per-origin frontier must re-admit previously-deduped identity tuples). On cache hit the apply is suppressed with `Applied = false` and the apply-duration histogram is tagged `outcome=shadow-forward-dedup`. Range deletes bypass the cache because they carry `HybridLogicalClock.Zero` (ambiguous identity); the leaf layer is naturally idempotent for range applies.

Correctness is still bounded by the HWM. The cache is a fast-path optimisation: it suppresses the duplicate-emit pair before the apply grain hop even when the HWM round-trip would otherwise admit both. Cache eviction under sustained churn cannot cause a re-merge - the HWM remains the authoritative dedupe key for any entry the cache has evicted.

## Validation

`ApplyAsync` throws `ArgumentException` when:

- `entry.TreeId` is null or empty.
- `entry.OriginClusterId` is null or empty.
- `entry.Op == Set` and `entry.Value` is null (for any mode).
- `entry.Op == DeleteRange` and `entry.EndExclusiveKey` is null.

`InvalidOperationException` is thrown when:

- `entry.Mode` is an undefined integer value (no apply rule registered).
- A typed CRDT state-merge exhausts its CAS retry budget under sustained contention on the target key.

`OperationCanceledException` is thrown when the supplied `CancellationToken` is already cancelled or fires during a grain call.

## Registration

`AddLatticeReplication` registers the default `IReplicationApplier` implementation as a silo-side singleton:

```csharp verify
siloBuilder.AddLatticeReplication(o => o.ClusterId = "site-a");
```

Resolve it from inside a silo-side service (typically a transport adapter or a hosted-service inbound pipeline) via constructor injection on `IReplicationApplier`. The applier is not exposed on the cluster client - it is a silo-local seam by design, because the apply path must run inside the cluster that owns the receiving tree.

## Threading and concurrency

The applier is a stateless singleton that holds no per-call state; all coordination flows through the per-origin high-water-mark grain (single-threaded under Orleans turn semantics) and the per-tree apply grain (`StatelessWorker`). Concurrent `ApplyAsync` calls for the same `(tree, origin)` pair are serialised by the HWM grain; concurrent calls for different pairs are independent.

## Bootstrap handoff

The per-origin high-water-mark is the explicit handoff contract for the bootstrap protocol introduced in a later phase: a newly-bootstrapped peer pins the HWM to the snapshot's authoring HLC, then resumes incremental replication from that pinned frontier with exactly-once apply guarantees across the snapshot / incremental boundary. The pinned value may be lower than the receiver's prior HWM (a peer that rewinds to an older snapshot must accept the snapshot's frontier as the apply point); the underlying grain therefore exposes an unconditional pin alongside the monotonic advance.

## Caveats

- **Range deletes do not preserve a single source HLC.** The wire format does not carry per-leaf HLCs, so the receiver tombstones with fresh local HLCs. LWW resolution against a concurrent local write therefore depends on the local clock at apply time, not the remote walk's clock. Idempotence at the leaf layer is the primary correctness guarantee.
- **The HWM is per-origin, not per-shard.** A receiver applying entries from origin `X` against a tree split into N shards advances a single HWM row keyed `(tree, X)` regardless of which shard the entry targets. This is intentional: the HWM contract is the bootstrap-handoff seam, and bootstrap operates per-origin not per-shard.

## Batch apply path

Inbound transports deliver batches of `WalRecord` records, not single entries: a 256-entry gRPC push from a single producer is one network round-trip carrying 256 mutations. `ApplyBatchAsync` is the seam that lets the receiver process such a batch as one logical operation rather than 256 independent `ApplyAsync` calls - it collapses the per-entry per-origin HWM grain RPCs to one `GetAsync` + one `TryAdvanceAsync` per distinct origin per batch and drains the causal-apply buffer once at the end of the batch instead of after every successful apply.

The default-interface-method body provides backward-compatible semantics: it loops over `ApplyAsync` and aggregates the per-entry results, so any custom `IReplicationApplier` written before the batch seam existed continues to work without changes. The shipped `ReplicationApplier` overrides the batch path with the optimised implementation described below.

### Run grouping

The optimised batch path walks the inbound list and identifies maximal contiguous runs of entries that share the same `(TreeId, OriginClusterId)` tuple. For a 256-entry batch shipped by a single producer the entire batch is one run; for an interleaved batch (e.g. a snapshot recovery merge that intersperses entries from two origins) the path emits one run per contiguous group, each amortised independently. Within a run:

- A single `GetAsync` reads the persisted per-origin HWM at the start of the run.
- An in-memory `runningHwm` tracks the highest applied HLC for the rest of the run, so subsequent entries dedupe without a fresh round trip. The producer's per-origin HLC monotonicity guarantee makes this strictly equivalent to per-entry `GetAsync` + dedupe.
- Causal-dependency entries fetch the local vector clock lazily on first use and reuse it until an apply has occurred, at which point a `localVcDirty` flag forces a re-fetch on the next causal-dep check.
- A single `TryAdvanceAsync` advances the persisted HWM to the highest applied HLC at the end of the run.
- A single `DrainBufferAsync` drains the causal-apply buffer once if the run advanced the persisted HWM.

For a 256-entry single-origin batch this collapses ~3·256 = 768 grain round-trips (per-entry `GetAsync` + `ApplyPointAsync` + `TryAdvanceAsync`) to 256 + 2 = 258 - the dominant receiver-side cost on every inbound push.

### Preserved per-entry semantics

Every classification the per-entry path produces survives the batch path:

- **Range-delete entries** bypass HWM dedup and apply unconditionally (they carry `HybridLogicalClock.Zero` by design).
- **Local-origin runs** classify every entry as `Dedup` with `HighWaterMark = HybridLogicalClock.Zero` and emit no grain calls.
- **In-batch dedup** against the in-memory `runningHwm` is bit-equivalent to per-entry dedup against a freshly-read HWM, because the producer guarantees per-origin HLC monotonicity within the batch.
- **Causal-park** is exercised per-entry; only the local-vector-clock fetch is lazy.
- **Per-entry instrumentation** (`ApplyDuration`, `ApplyLag`, `ApplyFifoViolations`) is recorded inside the per-entry loop so observability is preserved verbatim.
- **Single-entry batches** defer to `ApplyAsync` so behaviour is bit-identical with the legacy receiver for the trivial case.

### Failure model

Per-entry failures inside the batch surface as `ApplyAsync`-equivalent exceptions. The `LatticeReplicationGrpcService.Push` receiver wraps the batch call in a transport-level exception so the sender's backoff/retry loop kicks in for the whole batch - partial-batch acceptance is not a guarantee the seam offers. A `DeadLetterTrackingReplicationApplier` decorator detects retry history on any entry in the batch and falls back to per-entry routing so its DLQ accounting is exact.

## Cross-cluster atomic visibility - receiver seam

`SetManyAtomicAsync` sagas authored on the source cluster ride the
standard WAL replication transport: every prepared per-key write
emits a `Set` / `Delete` `WalRecord` with `IsPrepared = true` and a
non-empty `TransactionId`, and the saga's terminal phase emits one
`TxCommit` (or `TxAbort`) `WalRecord` per touched shard. The shipper
preserves these records verbatim; the receiver seam interprets them
through three additional internal hops on `IReplicationApplyGrain`:

| Method | Wire trigger | Receiver behaviour |
|---|---|---|
| `ApplyPreparedSetAsync` | `Op == Set` && `IsPrepared == true` | Stages the write under the saga's `TransactionId` in the destination leaf's per-tx pending bucket. The visible projection is unchanged - public readers (`GetAsync`, `KeysAsync`, etc.) do not observe the prepared entry. |
| `ApplyPreparedDeleteAsync` | `Op == Delete` && `IsPrepared == true` | Stages a tombstone under the saga's `TransactionId` in the same pending bucket. The pre-saga value remains visible to public readers until the terminal arrives. |
| `ApplyTxTerminalAsync` | `Op == TxCommit` or `Op == TxAbort` | Calls `ITxRegistryGrain.RecordTerminalArrivalAsync(txid, sourceShardIndex, committed, atomicShardCount)` to tally this per-source-shard arrival. While the tally is not final the registry mark stays unset and the receiver leaves' pending buckets stay in place so reads remain all-or-nothing. Only on the final arrival does the receiver mark the per-tree `ITxRegistry` entry and pre-fan the terminal across the transitive split-forward closure of every observed source-shard in a single parallel hop. On commit every pending entry under the `TransactionId` flips into the visible projection; on abort the pending entries are dropped. |

The `ReplicationApplier.ApplyBatchAsync` classifier excludes any
entry with `IsPrepared == true` from the batched LWW fast-path so
prepared `Set` / `Delete` records are always routed through the
per-entry `ApplyPreparedSetAsync` / `ApplyPreparedDeleteAsync` seam.
Without this exclusion the prepared writes would commit directly
into the receiver leaf's visible `Entries` and the saga's terminal
mark would find no matching pending entries to flip - so the
cross-cluster reader would observe the prepared write as visible
before the registry gate flipped, purely as a function of whether
the inbound run happened to be batched or single-entry. Unprepared
writes continue to consume the batched merge path.

The per-source-shard arrival tally is the receiver-side
multi-shard atomic-visibility gate. A saga that touched **N** source
shards emits **N** independent terminal records, one per source
shard, that ship through the change feed under independent
backpressure / batching cadences. Each terminal carries the saga's
authoritative touched-shard count in the additive
`WalRecord.AtomicShardCount` slot, which the receiver feeds into
`RecordTerminalArrivalAsync` to compute `IsFinal`. A receiver
running a pre-gate producer sees `atomicShardCount == 0` on every
terminal, which the gate treats as "no expected-total information"
and falls back to first-terminal-wins semantics - equivalent to the
pre-gate behaviour and wire-compatible across mixed-version
deployments.

The transport-level filter (`ShouldShip`) explicitly bypasses
per-tree `KeyFilter` and `KeyPrefixes` for `TxCommit` and `TxAbort`
records: a saga whose prepared keys passed the filter must have its
terminal delivered or the receiver-side pending bucket leaks. The
empty-origin guard and the cycle-break filter still run before the
bypass, so a malformed or self-loopback terminal is still rejected.

### Cross-tree terminals (receiver barrier)

A terminal that belongs to a **cross-tree** atomic write
(`IGrainFactory.SetManyAtomicAsync`) carries two additional slots -
`WalRecord.CrossTreeOperationId` and `WalRecord.CrossTreeParticipants`
(the canonical participant tree-id set). The applier threads these into
`ApplyTxTerminalAsync` as a `crossTreeOperationId` plus a receiver-scoped
**wait set**. The wait set is the participant set intersected with the
trees this receiver actually replicates
(`LatticeReplicationOptions.ReplicatedTrees`); the tree that received the
terminal is always included. A participant tree not replicated here is
excluded, so a cross-tree batch spanning a mix of replicated and
non-replicated trees stays valid - the barrier completes on the present
subset rather than waiting forever on a tree that never ships here.

Once a tree's per-source-shard gate is final, a cross-tree terminal does
**not** flip that tree's registry directly. Instead the receiver durably
registers the tree's local txid as delegated to a **receiver coordinator
grain** (`ILatticeCrossTreeReceiverGrain`, keyed by
`(originClusterId, operationId)`) and notifies it of this tree's arrival
and commit/abort vote. The coordinator decides only once a terminal has
arrived for every tree in the wait set, committing iff every arrived tree
voted commit. Before the decision, a delegated read on any participating
tree's registry resolves `InFlight` against the coordinator, so every
tree stays invisible; after it, the receiver flips every participating
tree together. The coordinator only ever returns the decision (it never
calls back into a tree grain); the calling `LatticeGrain` performs the
per-tree finalizes - itself inline, siblings via their apply grains - so
there is no circular wait. A null/empty `crossTreeOperationId` routes the
terminal through the legacy single-tree gate unchanged.

Public readers therefore observe the receiver-side same-cluster
atomic-visibility property end-to-end: at every point in time,
either every key the saga prepared on the receiver is at its
post-saga value (after the commit terminal applies) or none of them
is (during the prepare window or after an abort). The HLC the
visible value carries is the source cluster's HLC verbatim - the
receiver's wall-clock progression does not bump it - so transitive
LWW resolution (A -> B -> C with A's HLC intact) holds across saga
output identically to single-key cross-cluster writes.

What ships today:

- `WalRecord.AtomicBatchSize`, `AtomicBatchIndex`, `AtomicShardCount`,
  `TransactionId`, and `IsPrepared` are preserved on the wire end-to-end.
  The receiver consumes `TransactionId` and `IsPrepared` to drive the
  prepared / terminal staging path, and `AtomicShardCount` to drive
  the per-source-shard arrival tally on terminal records.
  `AtomicBatchSize` and `AtomicBatchIndex` remain reserved for future
  receiver-side batch optimisations.
- The receiver-side multi-key atomic apply seam (and its associated
  `Atomic` + `Apply` value types) was deleted by the universal-
  visibility ship. Cross-cluster atomic visibility is provided
  exclusively by the per-key prepared / per-shard terminal-mark apply
  hops described above; the local `SetManyAtomicAsync` saga inside
  `Orleans.Lattice` uses the same point-apply seam as a non-saga
  write, with the `IsPrepared` flag selecting the staging behaviour.
- Local single-tree atomic visibility (within one cluster) is
  shipped end-to-end via the per-tree `ITxRegistryGrain`
  linearization point; see
  [Atomic Writes](../lattice/atomic-writes.md) for the protocol
  and [Consistency](../lattice/consistency.md#atomic-visibility-single-tree-foreground-and-cross-cluster)
  for the read-path dial-back. The cross-cluster receiver seam
  reuses the same registry grain.
- The producer-side per-key WAL filter shipped earlier. Hosts that
  need to bound the change feed at commit time configure
  `ReplicatedTrees`, `KeyFilter`, or `KeyPrefixes` on
  `LatticeReplicationOptions` - see [`wal.md`](wal.md). `TxCommit`
  and `TxAbort` records are exempt from the per-key filter as
  described above.
