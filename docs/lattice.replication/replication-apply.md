# Replication apply seam (`IReplicationApplier`)

`IReplicationApplier` is the public, in-process inbound seam over the per-tree apply pipeline. It installs a single `WalRecord` authored on a remote cluster onto the local tree while preserving the remote cluster's `HybridLogicalClock` and origin id end-to-end, and it filters re-delivery via a snapshot-pinned causal floor plus a shadow-forward identity cache and per-key last-writer-wins idempotence, so at-least-once transports become at-most-once apply.

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
| `Applied` | `true` when the entry was merged onto the local tree; `false` when the entry was filtered out as a re-delivery (its `Timestamp` was at or below the origin's pinned snapshot floor, or its identity tuple hit the shadow-forward cache) or rejected as inapplicable (its `OriginClusterId` matched the local cluster id and would have looped). For batch calls, `true` if **any** entry in the batch was newly merged. |
| `HighWaterMark` | For point applies (`Set` / `Delete`) this is the per-origin HWM after the call - equal to `entry.Timestamp` when `entry.Timestamp` advanced the frontier, or the current HWM otherwise (including when `Applied` is `false`). For range deletes and local-origin no-op rejections - neither of which consults the HWM - this is `HybridLogicalClock.Zero`. For batch calls, the pointwise maximum HWM across every distinct origin in the batch. |

## Apply semantics

Three concerns the applier composes for every call:

### 1. Source-HLC and origin preservation

For `LwwRegister` mode point applies route through the core library's apply seam, which persists the entry's `LwwValue<byte[]>` with the supplied `Timestamp` and `OriginClusterId` **verbatim** - no fresh local HLC is stamped. This is what unlocks transitive replication (A → B → C with A's HLC intact) and deterministic LWW resolution against concurrent local writes.

For typed CRDT modes (`OrSet`, `PnCounter`, `VersionVector`, `MvRegister`, `OrMap`) a steady-state entry carries the producer's typed delta in `WalRecord.Delta` (authored via the accessor at commit time). The applier forwards that delta verbatim through the same `CrdtDelta`-recording grain seam used by both the batch path and a locally-authored CRDT write, preserving the source `Timestamp` and `OriginClusterId` and folding the delta into the visible state in one grain turn. The receiver therefore records a `CrdtDelta` revision with per-member ADDED/REMOVED changes - identical history fidelity to a local write - rather than a flattened full-value `Set`. The fold is wrapped in a `LatticeOriginContext.With(originClusterId)` scope so the receiver's commit-time observer publishes the foreign origin and the producer-side ship loop filters the resulting entry out. A bootstrap committed-projection row carries the full state in `WalRecord.Value` with no delta; it has no per-delta shape, so it folds via state-based merge under optimistic concurrency and stays a full-state set. For `OrMap` mode the receiver resolves the concrete `(TKey, TValue)` shape through `OrMapShapeRegistry`; an `OrMap`-mode apply against an unregistered tree faults with a clear configuration-error message.

Range deletes carry `HybridLogicalClock.Zero` by design (a range walk produces many per-leaf HLCs that cannot be faithfully collapsed into a single timestamp). The receiver walks the leaf chain locally and stamps each tombstone with a freshly-ticked local HLC; the remote `OriginClusterId` rides through an ambient `LatticeOriginContext` scope so the receiver-side change-feed observer publishes it on every emitted `LatticeMutation`.

### 2. Snapshot-pinned causal floor (and per-origin high-water-mark)

The applier tracks two distinct per-`(TreeId, OriginClusterId)` quantities on the high-water-mark grain:

- The **per-origin high-water-mark** is the max-applied source HLC. It advances monotonically after every successful point apply and drives FIFO / causal ordering, observability, and the bootstrap handoff. It is **not** a drop criterion for steady-state point writes.
- The **pinned causal floor** is written only by `PinSnapshotAsync` (the bootstrap handoff) and records the snapshot frontier below which every mutation is already contained in the pinned snapshot. When no snapshot has been pinned the floor is `HybridLogicalClock.Zero`.

Before applying a point entry the applier reads the pinned floor; if `entry.Timestamp <= floor` the call is a no-op (`Applied = false`), because everything at or below a pinned snapshot frontier is already durably present. Otherwise the entry is admitted and its at-most-once guarantee rests on per-key last-writer-wins idempotence at the leaf (a superseded or duplicate write merges to the same state) plus the shadow-forward identity cache below. After a successful point apply the HWM advances monotonically; a laggard's lower advance becomes a no-op.

This is deliberately **not** a `entry.Timestamp <= hwm` drop. The per-origin HLC stream is not monotonic in write-ahead-log / ship order: HLCs are stamped per leaf (each leaf carries its own clock) and the write-ahead-log partitions by key hash, so many leaves interleave in one partition and a genuinely-new point write can arrive with a source HLC below the running max-applied HLC. Dropping such an entry on a scalar `hwm` comparison silently strands it - the cross-cluster data-loss regime this seam is designed to avoid. A legitimately out-of-order-but-new entry that lands below the current HWM is applied and increments `apply.fifo_violations` (observability only). Typed CRDT modes consult the floor and advance the HWM the same way; state-based merge is naturally idempotent, so the floor gate just short-circuits redundant grain calls for the below-snapshot backlog.

Range deletes bypass the floor by design. Range applies are naturally idempotent at the leaf layer: re-running a range delete on already-tombstoned keys merges to the same state, so dedupe is unnecessary.

### 3. Local-origin defence-in-depth

A `WalRecord` whose `OriginClusterId` matches the local cluster id is rejected as a no-op (`Applied = false`). The outbound ship loop's origin filter already prevents this in steady state, but hand-built apply pipelines and tests can still hand the applier such an entry - surfacing it as an explicit rejection rather than silently merging it into the same cluster's state is the safer default.

### 4. Shadow-forward dedupe cache

A structural rewrite (shard split, shard merge, saga compensate) that shadow-forwards a user write into a different shard generates a duplicate-emit pair: one entry from the originating shard's commit, one from the shadow-forwarded shard's commit, both carrying identical `(originClusterId, timestamp, key, op)` identity tuples. Because the pinned-floor gate does not drop above-floor point writes, both deliveries reach the apply path; the identity cache is what collapses the redundant second grain hop before it happens.

The applier holds a per-tree bounded FIFO cache of recently-applied identity tuples (`LatticeReplicationOptions.ShadowForwardDedupeCacheSize`, default `4096`, validator floor `64`). The cache is consulted *after* the pinned-floor gate so floor-deduped entries do not pollute it (which preserves operator-driven re-pin semantics where lowering the pinned floor must re-admit previously-deduped identity tuples). On cache hit the apply is suppressed with `Applied = false` and the apply-duration histogram is tagged `outcome=shadow-forward-dedup`. Range deletes bypass the cache because they carry `HybridLogicalClock.Zero` (ambiguous identity); the leaf layer is naturally idempotent for range applies.

The cache is a fast-path optimisation, not the correctness backstop. It suppresses the duplicate-emit pair before the apply grain hop; if an entry it would have caught has been evicted under sustained churn, the duplicate still re-applies to the same state under per-key last-writer-wins idempotence at the leaf, so an eviction can never cause a divergent re-merge - it only costs one redundant grain hop.

### 5. Causal-dependency gate

Entries authored with causal-plus tracking carry a `VectorClock` frontier. Before applying such an entry the receiver fetches its local vector clock and checks that every component of the entry's frontier is dominated-or-equal locally; an entry with an unsatisfied dependency is parked in the per-tree bounded causal-apply buffer and retried each time a later apply advances the local clock. Two frontier components are exempt from the check:

- **The entry's own origin diagonal.** The per-origin high-water-mark tracks that origin's own FIFO progression, so requiring the local clock to dominate the diagonal would deadlock the very entry being applied.
- **The receiver's own cluster id.** The receiver-side local vector clock tracks only *foreign*-applied frontiers - it never advances its own diagonal - but the receiver durably holds every write it authored itself, so any dependency on one of the receiver's own writes is trivially satisfied. Without this exemption a peer entry whose frontier references a write the receiver originated (for example, site C's post-partition write that causally follows site A's pre-partition write, once an A-C partition heals) would park forever against a perpetually-zero self-component and stall convergence.

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

The pinned causal floor is the explicit handoff contract for the bootstrap protocol: a newly-bootstrapped peer calls `PinSnapshotAsync`, which atomically installs both the per-origin HWM and the pinned floor at the snapshot's authoring frontier, then resumes incremental replication from that pinned frontier with exactly-once apply guarantees across the snapshot / incremental boundary. `PinSnapshotAsync` *replaces* the floor rather than max-merging it, so a peer that rewinds to an older snapshot (a pinned value lower than the receiver's prior frontier) correctly lowers the floor and re-admits entries above the new, lower cut. The grain therefore exposes this unconditional pin alongside the monotonic HWM advance and a `GetPinnedFloorAsync` read.

## Caveats

- **Range deletes do not preserve a single source HLC.** The wire format does not carry per-leaf HLCs, so the receiver tombstones with fresh local HLCs. LWW resolution against a concurrent local write therefore depends on the local clock at apply time, not the remote walk's clock. Idempotence at the leaf layer is the primary correctness guarantee.
- **The HWM is per-origin, not per-shard.** A receiver applying entries from origin `X` against a tree split into N shards advances a single HWM row keyed `(tree, X)` regardless of which shard the entry targets. This is intentional: the HWM contract is the bootstrap-handoff seam, and bootstrap operates per-origin not per-shard.

## Batch apply path

Inbound transports deliver batches of `WalRecord` records, not single entries: a 256-entry gRPC push from a single producer is one network round-trip carrying 256 mutations. `ApplyBatchAsync` is the seam that lets the receiver process such a batch as one logical operation rather than 256 independent `ApplyAsync` calls - it collapses the per-entry per-origin HWM grain RPCs to one `GetAsync` + one `GetPinnedFloorAsync` + one `TryAdvanceAsync` per distinct origin per batch and drains the causal-apply buffer once at the end of the batch instead of after every successful apply.

The default-interface-method body provides backward-compatible semantics: it loops over `ApplyAsync` and aggregates the per-entry results, so any custom `IReplicationApplier` written before the batch seam existed continues to work without changes. The shipped applier overrides the batch path with the optimised implementation described below.

### Run grouping

The optimised batch path walks the inbound list and identifies maximal contiguous runs of entries that share the same `(TreeId, OriginClusterId)` tuple. For a 256-entry batch shipped by a single producer the entire batch is one run; for an interleaved batch (e.g. a snapshot recovery merge that intersperses entries from two origins) the path emits one run per contiguous group, each amortised independently. Within a run:

- A single `GetAsync` reads the persisted per-origin HWM and a single `GetPinnedFloorAsync` reads the pinned causal floor at the start of the run.
- The pinned floor is constant for the run, so every entry in the run is dedup-tested against the same floor with no further round trips; there is no in-batch running-HWM accumulator (a below-max-applied-HLC entry is a genuine write under non-monotonic per-origin HLC, not a duplicate, so it must not be dropped mid-run).
- Causal-dependency entries fetch the local vector clock lazily on first use and reuse it until an apply has occurred, at which point a `localVcDirty` flag forces a re-fetch on the next causal-dep check.
- A single `TryAdvanceAsync` advances the persisted HWM to the highest applied HLC at the end of the run.
- A single `DrainBufferAsync` drains the causal-apply buffer once if the run advanced the persisted HWM.

For a 256-entry single-origin batch this collapses ~3·256 = 768 grain round-trips (per-entry `GetAsync` + `ApplyPointAsync` + `TryAdvanceAsync`) to 256 + 3 = 259 (the batched `GetAsync` + `GetPinnedFloorAsync` + `TryAdvanceAsync`) - the dominant receiver-side cost on every inbound push.

### Preserved per-entry semantics

Every classification the per-entry path produces survives the batch path:

- **Range-delete entries** bypass the pinned-floor gate and apply unconditionally (they carry `HybridLogicalClock.Zero` by design).
- **Local-origin runs** classify every entry as `Dedup` with `HighWaterMark = HybridLogicalClock.Zero` and emit no grain calls.
- **Below-floor dedup** against the run's pinned causal floor is bit-equivalent to per-entry dedup against a freshly-read floor, because the floor is written only by `PinSnapshotAsync` and is therefore constant across a run.
- **Causal-park** is exercised per-entry; only the local-vector-clock fetch is lazy.
- **Per-entry instrumentation** (`ApplyDuration`, `ApplyLag`, `ApplyFifoViolations`) is recorded inside the per-entry loop so observability is preserved verbatim.
- **Single-entry batches** defer to `ApplyAsync` so behaviour is bit-identical with the legacy receiver for the trivial case.

### Failure model

Per-entry failures inside the batch surface as `ApplyAsync`-equivalent exceptions. The gRPC receiver endpoint wraps the batch call in a transport-level exception so the sender's backoff/retry loop kicks in for the whole batch - partial-batch acceptance is not a guarantee the seam offers. The dead-letter-tracking applier decorator detects retry history on any entry in the batch and falls back to per-entry routing so its DLQ accounting is exact.

### Parallel apply across independent runs

Under multi-tree load the per-run walk can serialise otherwise-independent work: a batch that interleaves runs from several trees applies them one after another even though they share no per-tree state, inflating apply latency and `apply.lag` (which now also drives receiver back-pressure, so slow applies translate directly into sender throttling).

`LatticeReplicationOptions.ApplyMaxParallelRuns` bounds how many **independent** runs the batch path may apply concurrently. Independence is defined at the **tree** granularity:

- Runs targeting **distinct trees** may apply in parallel. Distinct trees share no per-tree state - separate causal-apply buffers, shadow-forward dedupe caches, high-water-mark grains, and apply grains - so concurrent apply cannot reorder or interleave their work.
- Runs that **share a tree** (different origins of the same tree) stay in one ordered group and apply strictly sequentially in write-ahead-log order. The per-tree causal-apply buffer and shadow-forward dedupe cache are shared across a tree's origins, so keeping same-tree runs serialised guarantees those structures observe the exact access order the fully-sequential path produces.

Parallelism is therefore only ever introduced **across** independent runs, never **within** one. Every within-run ordering invariant holds unchanged regardless of the configured degree of parallelism: per-origin FIFO, the causal dependency gate and its bounded buffer, per-origin high-water-mark monotonicity, and atomic-batch (saga) apply boundaries. A multi-entry run still collapses to a single batched merge; an atomic batch still applies as a unit on its owning run.

The effective degree of parallelism for a given batch is the host-configured `ApplyMaxParallelRuns` clamped to the number of distinct trees present in that batch, and is bounded by a per-batch semaphore so concurrency can never amplify local WAL saturation beyond the configured cap. It is surfaced on the `apply.parallel_runs` histogram (see [observability](observability.md)).

**Default posture: fully sequential.** `ApplyMaxParallelRuns` defaults to `1`, which is exactly the historical behaviour - the batch path walks every run in order and awaits each before the next. The single-tree batch (the overwhelmingly common inbound shape, since the transport ships per-`(tree, peer)`) always takes the allocation-free sequential walk regardless of the configured value, because cross-tree parallelism is moot when there is only one tree. Raise the value conservatively, per workload, only after validating parallel apply for that topology.

## Cross-cluster atomic visibility - receiver seam

`SetManyAtomicAsync` sagas authored on the source cluster ride the
standard WAL replication transport: every prepared per-key write
emits a `Set` / `Delete` `WalRecord` with `IsPrepared = true` and a
non-empty `TransactionId`, and the saga's terminal phase emits one
`TxCommit` (or `TxAbort`) `WalRecord` per touched shard. The shipper
preserves these records verbatim; the receiver seam interprets them
through three additional internal apply hops:

| Apply hop | Wire trigger | Receiver behaviour |
|---|---|---|
| Prepared set | `Op == Set` && `IsPrepared == true` | Stages the write under the saga's `TransactionId` in the destination leaf's per-tx pending bucket. The visible projection is unchanged - public readers (`GetAsync`, `KeysAsync`, etc.) do not observe the prepared entry. |
| Prepared delete | `Op == Delete` && `IsPrepared == true` | Stages a tombstone under the saga's `TransactionId` in the same pending bucket. The pre-saga value remains visible to public readers until the terminal arrives. |
| Transaction terminal | `Op == TxCommit` or `Op == TxAbort` | Records this per-source-shard terminal arrival in the per-tree transaction registry (keyed by txid, source shard index, commit/abort outcome, and atomic shard count) to tally arrivals. While the tally is not final the registry mark stays unset and the receiver leaves' pending buckets stay in place so reads remain all-or-nothing. Only on the final arrival does the receiver mark the per-tree transaction-registry entry and pre-fan the terminal across the transitive split-forward closure of every observed source-shard in a single parallel hop. On commit every pending entry under the `TransactionId` flips into the visible projection; on abort the pending entries are dropped. |

The batch-apply classifier excludes any
entry with `IsPrepared == true` from the batched LWW fast-path so
prepared `Set` / `Delete` records are always routed through the
per-entry prepared-set / prepared-delete apply hops.
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
the terminal-arrival tally to compute finality. A receiver
running a pre-gate producer sees `atomicShardCount == 0` on every
terminal, which the gate treats as "no expected-total information"
and falls back to first-terminal-wins semantics - equivalent to the
pre-gate behaviour and wire-compatible across mixed-version
deployments.

The producer-side ship filter explicitly bypasses
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
the transaction-terminal apply hop as a `crossTreeOperationId` plus a receiver-scoped
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
grain** (keyed by
`(originClusterId, operationId)`) and notifies it of this tree's arrival
and commit/abort vote. The coordinator decides only once a terminal has
arrived for every tree in the wait set, committing iff every arrived tree
voted commit. Before the decision, a delegated read on any participating
tree's registry resolves `InFlight` against the coordinator, so every
tree stays invisible; after it, the receiver flips every participating
tree together. The coordinator only ever returns the decision (it never
calls back into a tree grain); the calling tree grain performs the
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
  shipped end-to-end via the per-tree transaction registry
  linearization point; see
  [Atomic Writes](../lattice/atomic-writes.md) for the protocol
  and [Consistency](../lattice/consistency.md#atomic-visibility)
  for the read-path dial-back. The cross-cluster receiver seam
  reuses the same registry grain.
- The producer-side per-key WAL filter shipped earlier. Hosts that
  need to bound the change feed at commit time configure
  `ReplicatedTrees`, `KeyFilter`, or `KeyPrefixes` on
  `LatticeReplicationOptions` - see [`wal.md`](wal.md). `TxCommit`
  and `TxAbort` records are exempt from the per-key filter as
  described above.
