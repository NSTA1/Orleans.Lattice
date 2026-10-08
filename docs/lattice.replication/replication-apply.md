# Replication apply seam (`IReplicationApplier`)

`IReplicationApplier` is the public, in-process inbound seam over the per-tree apply pipeline. It installs a single `WalRecord` authored on a remote cluster onto the local tree while preserving the remote cluster's origin id end-to-end and, except where [Source-HLC and origin preservation](#1-source-hlc-and-origin-preservation) notes otherwise, its `HybridLogicalClock`, and it absorbs re-delivery through a shadow-forward identity cache plus per-key last-writer-wins idempotence, so at-least-once transports converge without a per-origin HLC drop threshold.

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
    public bool Deferred { get; init; }
}
```

| `ApplyResult` member | Semantics |
|---|---|
| `Applied` | `true` when the entry was merged onto the local tree; `false` when the entry was filtered out as a re-delivery because its identity tuple hit the shadow-forward cache, parked on the causal-apply buffer to await its dependencies, deferred by a restore saga's receive fence, acknowledged as a no-op because it is a tombstone-reap envelope, or rejected as inapplicable (its `OriginClusterId` matched the local cluster id and would have looped, its tree is not enrolled for replication on this receiver, its wire merge mode disagreed with the locally-resolved mode, or the tenant-isolation gate refused it). For batch calls, `true` if **any** entry in the batch was newly merged. |
| `HighWaterMark` | For point applies (`Set` / `Delete`) this is the per-origin HWM after the call - equal to `entry.Timestamp` when `entry.Timestamp` advanced the frontier, or the current HWM otherwise (including when `Applied` is `false`). For range deletes, saga terminal marks, tombstone-reap envelopes, local-origin no-op rejections, receive-fence deferrals, and receiver-side enrollment / merge-mode / tenant-isolation rejections - none of which reads or advances the HWM - this is `HybridLogicalClock.Zero`. For batch calls, the pointwise maximum HWM across every distinct origin in the batch. |
| `Deferred` | `true` when the entry (or run) was **not** applied by this delivery and must be re-shipped: either a cross-cluster restore saga's durable receive fence has paused inbound apply for the tree, or the entry routed to a restored copy that saga has fenced against it - still closed, or admitted before the saga paused receiving ([#4593](coordinated-restore.md#restored-copies-are-born-receive-closed)) - (re-ship once the fence lifts), or the entry duplicates an identity whose first delivery is still in flight on this receiver (see [Shadow-forward dedupe cache](#5-shadow-forward-dedupe-cache)). A third deferral cause is a full dead-letter queue: an entry a receiver-side gate would dead-letter is deferred rather than acknowledged when the queue refuses the park (see [Capacity and backpressure](dead-letter-queue.md#capacity-and-backpressure)). For batch calls, `true` if **any** entry or run in the batch was deferred. Receive paths turn a deferred result into a not-accepted, cursor-preserving ack; every other `Applied == false` outcome is terminal and lets the sender advance past the entry. |

## Apply semantics

The applier composes these concerns for every call:

### 1. Source-HLC and origin preservation

For `LwwRegister` mode point applies route through the core library's apply seam, which persists the entry's value with the supplied `Timestamp` and `OriginClusterId` **verbatim** - no fresh local HLC is stamped. This is what unlocks transitive replication (A -> B -> C with A's HLC intact) and deterministic LWW resolution against concurrent local writes.

For typed CRDT modes, a steady-state entry carries the producer's typed delta in `WalRecord.Delta` (authored via the accessor at commit time). The applier forwards that delta verbatim through the same `CrdtDelta`-recording grain seam used by both the batch path and a locally-authored CRDT write, preserving the source `OriginClusterId` and folding the delta into the visible state in one grain turn. The source `Timestamp` is preserved only when the entry is folded as part of a multi-entry batched run, where the batch path re-stamps each item with its source HLC; a per-entry apply - a single-entry batch, an `OrMap` entry, a causal-buffer drain, or a dead-letter replay - takes a fresh local HLC at the merge point. The receiver therefore records a `CrdtDelta` revision with per-member ADDED/REMOVED changes - identical history fidelity to a local write - rather than a flattened full-value `Set`. The fold is wrapped in a `LatticeOriginContext.With(originClusterId)` scope so the receiver's commit-time observer publishes the foreign origin and the producer-side ship loop filters the resulting entry out. A bootstrap committed-projection row carries the full state in `WalRecord.Value` with no delta; it has no per-delta shape, so it folds via state-based merge under optimistic concurrency and stays a full-state set, written at a fresh local HLC and without the row's `ExpiresAtTicks`, so the key is stored as a durable entry. For `OrMap` mode the receiver resolves the concrete `(TKey, TValue)` shape through `CrdtShapeRegistry` (populated by `ISiloBuilder.AddOrMapShape<TKey, TValue>`); an `OrMap`-mode apply against an unregistered tree faults with a clear configuration-error message.

Range deletes carry the producer's issue HLC: the authoring cluster pins one HLC for each uninterrupted run of the range-delete fan-out (a WAL clock-floor refusal mid-fan-out re-issues a fresh dominating stamp for the remainder, [#4586](https://github.com/NSTA1/Orleans.Lattice/issues/4586)), stamps every tombstone it writes in that run with it, and publishes it on the `WalRecord`. The producer ticks that HLC past the highest clock of every leaf the range covers, so it sorts above every row and prepared write already on them ([#4530](https://github.com/NSTA1/Orleans.Lattice/issues/4530)). The receiver walks the leaf chain locally and pins every tombstone it writes to that same HLC, so a range delete authored at `T` cannot overwrite a foreign-origin write whose HLC is strictly greater than `T`; the remote `OriginClusterId` rides through an ambient `LatticeOriginContext` scope so the receiver-side change-feed observer publishes it on every emitted `LatticeMutation`. A predicate-filtered range delete also ships the explicit set of keys it matched, and the receiver tombstones exactly those keys instead of re-deriving membership from the range bounds. Only an entry persisted by an older producer carries `HybridLogicalClock.Zero`; for such an entry the receiver falls back to stamping each tombstone with a freshly-ticked local HLC.

### 2. Per-origin high-water-mark (no drop threshold)

The applier tracks a per-tree local vector clock on the high-water-mark grain. Its diagonal entry for each `OriginClusterId` is the max-applied source HLC for that origin, advanced monotonically after every successful point apply and raised by the cross-cluster bootstrap handoff through `MergeBootstrapFrontierAsync` (pointwise maximum with the vector already held). `PinSnapshotAsync` keeps replace semantics only for the intra-cluster restore re-seed, where rollback is deliberate. The vector drives FIFO diagnostics, causal-plus dependency checks, WAL fall-off detection, and the bootstrap handoff. It is **not** a drop criterion for point writes.

Before applying a point entry the applier reads the diagonal HWM so it can return the current frontier and later advance it after a successful merge. It does not drop `entry.Timestamp <= hwm`, and this build also installs no snapshot-pinned drop floor. A point write is admitted unless another gate rejects, parks, defers, or exact-identity-dedupes it. Re-delivery is absorbed by the shadow-forward identity cache below and by idempotent per-key last-writer-wins or CRDT merge at the leaf. After a successful point apply the HWM advances monotonically; a laggard's lower advance becomes a no-op.

This is deliberately **not** a per-origin HLC threshold. The per-origin HLC stream is not monotonic in write-ahead-log / ship order: HLCs are stamped per leaf (each leaf carries its own clock) and the write-ahead-log partitions by key hash, so many leaves interleave in one partition and a genuinely-new point write can arrive with a source HLC below the running max-applied HLC. Dropping such an entry on a scalar `hwm` comparison silently strands it - the cross-cluster data-loss regime this seam avoids. A legitimately out-of-order-but-new entry that lands below the current HWM is applied and increments `apply.fifo_violations` (observability only). No single snapshot-pinned HLC per origin is downward-closed over what a snapshot contains either, so `PinSnapshotAsync` clears any legacy persisted floor and the applier never reads it. Typed CRDT modes follow the same rule: correctness rests on commutative/idempotent merge plus the exact identity cache, not an HLC floor.

Range deletes bypass point-write identity dedup and HWM advance by design. Range applies are naturally idempotent at the leaf layer: re-running a range delete on already-tombstoned keys merges to the same state, so a threshold is unnecessary.

### 3. Local-origin rejection

A `WalRecord` whose `OriginClusterId` matches the local cluster id is rejected as a no-op (`Applied = false`). This check is the receiving cluster's only enforcement that a local-origin entry is never applied back onto its authoring cluster - it is not defence-in-depth behind the sender's outbound origin filter. That filter runs on the sending cluster and decides what the sender ships, not what this receiver accepts, and a hand-built apply pipeline or a test can hand the applier such an entry directly.

### 4. Receiver-side enrollment and merge-mode gate

Before the concerns above run, the applier gates every inbound entry against this receiver's own per-tree replication configuration, re-resolving the tree's enrollment and merge mode locally instead of trusting the wire. The peer-supplied `OriginClusterId` is unverified and `WalRecord.Mode` is a peer-controlled header field, so neither is taken on faith. The gate yields two rejections:

- **Not enrolled here (dropped).** A tree that is not enrolled for replication on this receiver is dropped: the call returns `Applied = false` with `HighWaterMark = HybridLogicalClock.Zero`, records the apply-duration outcome `rejected-not-replicated`, and is **not** dead-lettered. A non-enrolled tree id is peer-controlled, so parking it in a dead-letter queue would let a hostile peer spawn unbounded dead-letter-queue activations; dropping keeps the rejection cheap and bounded. This closes the gap where a peer that holds the mesh secret could otherwise write a tree the cluster deliberately kept cluster-local by not enrolling it - the reserved-prefix core-tree guard covers only the `_lattice_` core trees, not the `sys-`-prefixed authorization and identity trees.
- **Enrolled but wire mode mismatched (dead-lettered).** A tree that *is* enrolled but whose peer-supplied wire mode disagrees with the locally resolved merge mode is dead-lettered: the call returns `Applied = false` with `HighWaterMark = HybridLogicalClock.Zero`, the entry is enqueued to the tree's dead-letter queue tagged `mode_mismatch`, and the apply-duration outcome `rejected-mode-mismatch` is recorded (or, when the dead-letter queue is full, the entry is deferred and re-shipped instead). The tree is enrolled and therefore a bounded id, so parking the entry cannot be abused to spawn unbounded activations. Re-resolving the mode locally rather than trusting the wire field stops a peer from overriding the local merge algebra by shipping a different mode.

The merge mode is always re-resolved locally through the receiver's per-tree resolver (`ILatticeReplicationContext.ResolveMergeMode`, falling back to the raw `LatticeReplicationOptions.ReplicatedTrees` map); the wire `Mode` field is only ever compared against that resolution, never adopted. An applier with neither an injected replication context nor a `ReplicatedTrees` map has no enrollment signal, so the gate fails closed: every inbound entry is dropped as `rejected-not-replicated` (not dead-lettered) and a one-time warning is logged. Production registers the replication context, so the gate is always evaluable there.

A run the gate rejects - either way, or for want of an enrollment source - is also never recorded as inbound contact in `ReplicationPeerStats`, on any receive path, so a peer cannot plant a tree id of its choosing in the peer statistics or the peer-status report. The inbound half of that state is additionally capped, because the origin id of an admitted run is still the peer's own claim; see [observability](observability.md#bidirectional-peerlast_contact_seconds-and-the-liveness-probe).

Two further receiver-side gates run after an entry clears enrollment, before any high-water-mark read:

- **Tenant isolation (dead-lettered).** When tenancy is on, the owning tenant is
  derived from the tree id alone - never from a wire field. An entry is refused
  when the tenant does not exist, is inactive, or is not resident at the
  destination; the destination may receive tenant data while `Backfilling` or
  `Online`. When residency is configured, the authenticated direct sender must
  also be resident for that tenant (`Provisioning`, `Backfilling`, `Online`, or
  `Draining`). A draining region may finish shipping writes accepted while it was
  online, but it is not admitted as a destination or client-serving region.
  Authorization uses the direct sender authenticated by the transport, not
  `WalRecord.OriginClusterId` or its source-lineage stamp, so a relay must itself
  be resident even when the original writer was resident. A missing sender is
  refused only when source residency is configured and active; inactive or
  unconfigured residency preserves the legacy admit-all behavior. Refused entries
  are dead-lettered with `foreign_tenant`, `tenant_offline`, `tenant_suspended`,
  `tenant_source_not_resident`, or `missing_source_identity` as appropriate and
  leave the high-water mark unchanged. Legacy causal-buffer and dead-letter
  entries without a stored sender therefore replay when source residency is
  unconfigured, and fail closed when it is configured; optional lineage is never
  treated as sender identity. The receive path acknowledges a refusal only after
  the entry is durably parked; a full dead-letter queue defers and re-ships it
  instead. With tenancy off the gate is inactive and costs nothing. Custom `IReplicationTenantIsolationGate` implementations must override the sender-aware overload to enforce source residency; the default preserves legacy tenant-only evaluation.
- **Restore receive fence (deferred).** While a cross-cluster restore saga has paused inbound apply for the tree, the entry is not applied: the call returns `Applied = false` with `Deferred = true`, and the sender keeps its cursor and re-ships once the fence lifts. The tree's apply seam refuses an entry that routes to a restored copy the saga still holds closed, or that was admitted before the saga paused receiving, and the applier defers it the same way ([#4593](coordinated-restore.md#restored-copies-are-born-receive-closed)). A single-entry apply records the deferral under the `dedup` apply-duration outcome; the batch path defers a multi-entry run whole and records no apply-duration sample for it.

The batch path applies the same classification once per run, except that it checks the restore receive fence first: a run for a fenced tree is deferred whole before it is classified. The wire mode is part of the run key - a run is a contiguous `(TreeId, OriginClusterId, Mode)` segment, so a mode change starts a new run that is classified on its own - and the representative first entry therefore classifies the whole run. A rejected run neither merges nor advances the per-origin high-water-mark; every entry still records its matching apply-duration outcome so per-entry receiver observability is preserved, while a single warning is logged per run rather than per entry to avoid a log-flood amplification from a hostile peer.

### 5. Shadow-forward dedupe cache

A structural rewrite that shadow-forwards a user write into a different shard - a shard split, or a shard consolidation (merge), which reuses the split's shadow-write window - generates a duplicate-emit pair: one entry from the originating shard's commit, one from the destination shard's commit, both carrying identical `(originClusterId, timestamp, key, op)` identity tuples, because the forward carries the original last-writer-wins value and its HLC. An atomic-write abort is not a source of such pairs: it issues no per-key rollback writes, only an abort recorded in the tree's transaction registry and `TxAbort` terminals that discard the prepared writes. With no HLC drop threshold ahead of it, the identity cache is what collapses the redundant second grain hop before it happens.

The applier holds a per-tree bounded FIFO cache of recently-applied identity tuples (`LatticeReplicationOptions.ShadowForwardDedupeCacheSize`, default `4096`, validator floor `64`). The cache is consulted before point apply and is the only receiver-side exact-identity short-circuit for point writes. On cache hit the apply is suppressed with `Applied = false` and the apply-duration histogram is tagged `outcome=shadow-forward-dedup`. Range deletes bypass the cache - they are applied before it is consulted - and the leaf layer is naturally idempotent for range applies.

A reservation is **in flight** from the moment a delivery takes it until that delivery has applied the entry, and is then marked completed; a delivery that parks the entry releases the reservation once the durable causal-apply buffer holds it, because the buffer dedups re-deliveries of a parked entry itself. A duplicate that finds a **completed** reservation is a genuine re-delivery and is acknowledged (`Applied = false`, `outcome=shadow-forward-dedup`). A duplicate that finds an **in-flight** reservation is not: the first delivery can still be aborted - the receiver restarts mid-call, or the apply throws and the sender has already moved past that failure - and acknowledging the duplicate would let the sender's cursor pass an entry that is then neither applied nor dead-lettered. The applier instead returns `Deferred = true` (tagged `outcome=dedup`, the receive fence's tag), so the receive path answers with a not-accepted, cursor-preserving ack and the sender re-ships; by then the first delivery has completed (and the re-delivery is acknowledged as a duplicate) or rolled back its reservation (and the re-delivery applies). On the batch path a run carrying such a duplicate still applies its other entries and reports `Deferred`; a duplicate whose in-flight reservation is held by an entry the same run has deferred into its pending batch is an ordinary duplicate, because that run's own outcome settles both copies. A run that defers a saga's prepared entry this way also defers every later terminal (`TxCommit` / `TxAbort`) of the same saga in the run, so the receiver never applies a terminal ahead of a prepare the sender delivered before it ([#4499](https://github.com/NSTA1/Orleans.Lattice/issues/4499)); the sender re-ships both behind the not-accepted ack.

The cache is a fast-path optimisation, not the correctness backstop. It suppresses the duplicate-emit pair before the apply grain hop; if an entry it would have caught has been evicted under sustained churn, the duplicate still re-applies to the same state under per-key last-writer-wins idempotence at the leaf, so an eviction can never cause a divergent re-merge - it only costs one redundant grain hop.

### 6. Causal-dependency gate

Entries authored with causal-plus tracking carry a `VectorClock` frontier. Each component `(o, t)` it requires is a **dependency**, and a dependency names exactly one write: origin `o`'s write at HLC `t` ([#4586](https://github.com/NSTA1/Orleans.Lattice/issues/4586)). The per-origin high-water mark is never consulted for this. It is the maximum HLC applied, and with per-leaf clocks and interleaved WAL partitions a later write of `o` routinely arrives first ([#1060](https://github.com/NSTA1/Orleans.Lattice/issues/1060)), so a mark above `t` does not show that the write at `t` arrived.

The tree's high-water-mark grain decides each dependency in one call:

1. **Fast path.** It is **Met** when the tree recorded that exact write as applied. Every apply records its write's identity in the same grain call that advances the high-water mark, so this adds no grain call per write. The record is in memory and bounded per origin by `LatticeReplicationOptions.CausalAppliedIdentityCapacity` (16,384 by default). A saga prepare is not recorded, because it is not visible until its terminal. The record is forgotten whenever the tree's contents change lineage, because a write recorded before the change may no longer be in the tree: on every alias swap of the tree (a restore, revert, resize, remediation or operator rebind) and on a bootstrap or restore re-seed of its high-water mark. A forgotten identity is then decided by step 2, so the reset can only delay a dependent, never release one.
2. **Otherwise the origin's frontier decides.** That is the per-origin replication frontier, shared by every tree, because a dependency names an origin's write and not a tree. It holds:
   - the low watermark `S` the origin ships: every write of `o` stamped strictly below `S` was acknowledged here;
   - the writes of `o` this receiver acknowledged without applying: those parked in any tree's causal-apply buffer or dead-letter queue, which publish them before they acknowledge;
   - the writes marked **lost**: discarded from a dead-letter queue (issue #4603), or dropped because their tree is not enrolled here (recorded only for a configured `ReplicationPeers` member, since the origin is wire-supplied).

   A dependency is **Lost** when its write is marked lost. It is **Met** when `t < S` and the write is not held, because acknowledged and not held means applied. Otherwise it is **Unmet**. A held listing that a crash left behind is confirmed with its source before it blocks anything.

**How the origin's low watermark reaches the receiver.** An origin's shipper derives `S` from its WAL partitions' clock floors (the producer clock floor refuses a fresh local stamp below the floor, so once the peer has acknowledged every offset before the floor took effect, every write stamped below it has been acknowledged). It sends `S` beside each push as the `x-lattice-replication-source-frontier` call header, like the re-seed epoch, so the batch framing is unchanged and a receiver that predates it ignores it. The header carries the low watermark over the batch's tree, the low watermark over every tree the origin replicates to this receiver, an aggregate generation, and the receiver lineage the covering acknowledgements were taken under. The receiver reads it only after the caller's origin is authenticated, only for a tree enrolled here and an origin in the configured replication topology, and parses it strictly and bounded; a missing or malformed header vouches for nothing. Beside a frontier the shipper vouches, it also sends its acknowledged read positions - per WAL partition of its bound log, the lowest offset the peer has not acknowledged, capped at held terminals - as the `x-lattice-replication-acked-positions` header ([#4684](https://github.com/NSTA1/Orleans.Lattice/issues/4684)). It is read only with a valid frontier, under the same checks, parsed strictly and bounded, and recorded on the tree's frontier for the origin; a malformed value vouches no positions but keeps the watermark. A receiver that predates it ignores it. An import of another tree reads these positions to decide whether this tree has passed the sibling boundary its export captured (see [Snapshot bootstrap](snapshot-bootstrap.md#snapshot-and-in-flight-atomic-visibility)).

**The receiver's tree frontier.** Each tree has a durable per-tree replication frontier that owns a frontier epoch and records each origin's watermark for the tree. Every acknowledgement reports the epoch as `ReplicationAck.ReceiverLineage`, and a watermark is accepted only if it is tagged with the current epoch. Whenever the tree's contents may be replaced, the frontier re-mints its epoch. The tree registry tells it before it persists any change to the tree's lineage (the tree's lineage token, [#4537](https://github.com/NSTA1/Orleans.Lattice/issues/4537)) - a registration, an unregistration or purge, a shadow-cutover restore or its revert, an alias move - and a failure to force the gap fails the change; an alias swap re-stamps it too, and an activation that finds a registry lineage it never observed re-stamps as well. In-place restore, resize, reshard and remediation keep the lineage, because they lose no applied write. On a re-stamp the frontier zeroes every origin's watermark, caps each origin's aggregate at zero on its frontier, and forgets the tree's applied identities, before the replacement proceeds. A sender that sees the epoch change treats it as a forced gap and re-seeds this receiver, because the new contents may lack writes it already shipped. The guarantee covers the writes the sender's current contents still hold. If a cluster replaces its own tree's contents outside a coordinated restore, its peers keep the writes it discarded, and a peer write that depends on one of them is released on that cluster's watermark once it re-covers the tree, without the discarded write; the replacing cluster logs a warning and counts it on `orleans.lattice.replication.source_restore.uncoordinated`. The tree accepts no watermark until a full bootstrap installs its export; the cap holds until the origin ships a watermark tagged with the new epoch, and from then on the origin's frontier ignores any aggregate of an older generation, which may still count the tree's lost coverage.

The tree's frontier is in one of four modes per origin, counted by `orleans.lattice.replication.causal.frontier_origins`: `exact` (a watermark was accepted), `pending` (no watermark yet: an older sender, or its clock floor is not enabled), `awaiting_reseed` (contents replaced, re-seed outstanding) or `degraded` (the tree registry tracks no lineage for the tree). Each mode is sound; only `exact` lets the low watermark meet a dependency. A tree leaves the non-exact modes by itself once its senders run floor-capable builds and its registry tracks a lineage.

Until an origin ships a low watermark, only the fast path can meet a dependency. That is safe: an entry whose dependency's identity was forgotten, or lives in another tree, waits, and overflows to the dead-letter queue as `hlc_skew` if it waits past the buffer bound. A Lost entry is never applied or buffered: it is dead-lettered with reason `dependency_lost` (`outcome=rejected-dependency-lost`), and the causal-apply buffer drain does the same for a parked entry whose dependency is later marked lost. An entry with an unsatisfied dependency is durably parked in the per-tree causal-apply buffer grain before the delivery is acknowledged (`Applied = false`, `outcome=parked-causal-buffer`). The grain serializes all parks and drains for that tree across silos, immediately re-checks and drains after a park to close lost wakeups, and re-arms drains after every apply that records a write identity (a write below the high-water mark is still a new identity), on a silo's first touch of the tree, after the bootstrap handoff, and on every replication maintenance tick. The maintenance tick bounds how long a satisfied entry can wait after restart, under quiescence, or when another silo applied the dependency.

The buffer is bounded by `CausalBufferMaxEntries` (default `1024`) and `CausalBufferMaxBytes` (default 16 MiB). Overflow is deliberate and observable: an evicted entry was already acknowledged to its sender, so the grain enqueues the oldest parked entry to the tree's dead-letter queue with reason `hlc_skew` before durably removing it from the buffer. If the DLQ enqueue or buffer write fails - including a full dead-letter queue, which refuses rather than evicts - the park fails and the sender re-ships. A drain that cannot dead-letter an entry because the queue is full keeps it parked and stops, so nothing leaves the buffer without being applied or parked. Parked or evicted entries release their shadow-forward identity reservation, so a dead-letter replay or peer re-delivery is applied or parked in its own right rather than suppressed as a duplicate. Two frontier components are exempt from the check (the causal buffer's required dependency frontier drops them):

- **The entry's own origin diagonal.** The per-origin high-water-mark tracks that origin's own FIFO progression, so requiring the local clock to dominate the diagonal would deadlock the very entry being applied.
- **The receiver's own cluster id.** The receiver-side local vector clock tracks only *foreign*-applied frontiers - it never advances its own diagonal - but the receiver durably holds every write it authored itself, so any dependency on one of the receiver's own writes is trivially satisfied. Without this exemption a peer entry whose frontier references a write the receiver originated (for example, site C's post-partition write that causally follows site A's pre-partition write, once an A-C partition heals) would park forever against a perpetually-zero self-component and stall convergence.

### 7. Bootstrap drop floor

After a full bootstrap the high-water-mark grain may hold a durable drop floor: per origin, the source's applied low watermark when the export opened and the writes below it the source held without applying ([snapshot bootstrap](snapshot-bootstrap.md), issue #4549). A point write or prepare of that origin stamped below the watermark and not held is not merged. While the bootstrap's import is still open the floor is provisional and the delivery is deferred (`Deferred = true`, `outcome=bootstrap-floor-deferred`); once the import closes stable it is acknowledged without being merged (`Applied = false`, `outcome=bootstrap-floor-dropped`), because the export already reflected it. The check runs after the terminal branch, so saga terminals are never held back, and it is skipped inside a bootstrap drain, so the drain's own rows apply. The batch path applies the same per-entry check and defers its run when any entry is below a provisional floor.

Every other replicated write is stamped with the tree's floor epoch from the same admission read. A shard root armed by a later floor install refuses a write stamped with an older epoch with a stale replication-floor admission fault, and the applier maps it to a deferral (`outcome=bootstrap-floor-deferred`), so the re-shipped delivery is admitted against the floor. The floor is cleared whenever the tree's applied identities are reset, and a later bootstrap replaces it.

## Validation

`ApplyAsync` throws `ArgumentException` when:

- `entry.TreeId` is null or empty.
- `entry.OriginClusterId` is null or empty.
- `entry.Op == Set` on an `LwwRegister` entry and `entry.Value` is null, or `entry.Op == Set` on a CRDT-mode entry that carries neither a typed `Delta` nor a full-state `Value`.
- `entry.Op == DeleteRange` and `entry.EndExclusiveKey` is null, or the range delete carries atomic-batch metadata (`AtomicBatchSize > 0`).
- A prepared entry (`IsPrepared == true`) carries an empty `TransactionId`, or a prepared `Set` carries a null `Value`.
- A saga terminal mark (`TxCommit` / `TxAbort`) carries no usable shard index or an empty `TransactionId`.

`InvalidOperationException` is thrown when:

- `entry.Mode` has no apply rule, or `entry.Op` is not a point-apply kind. The merge-mode gate normally intercepts an unknown wire mode first, dead-lettering it as `mode_mismatch`, because it cannot match the locally resolved mode.
- An `OrMap` tree has no registered `(TKey, TValue)` shape.
- A typed CRDT state-merge exhausts its CAS retry budget under sustained contention on the target key.

`OperationCanceledException` is thrown when the supplied `CancellationToken` is already cancelled or fires during a grain call.

## Registration

`AddLatticeReplication` registers the default `IReplicationApplier` implementation as a silo-side singleton:

```csharp verify
siloBuilder.AddLatticeReplication(o => o.ClusterId = "site-a");
```

Resolve it from inside a silo-side service (typically a transport adapter or a hosted-service inbound pipeline) via constructor injection on `IReplicationApplier`. The applier is not exposed on the cluster client - it is a silo-local seam by design, because the apply path must run inside the cluster that owns the receiving tree.

## Threading and concurrency

The applier is a silo-wide singleton. It holds no per-call state, only process-local per-tree structures - the shadow-forward dedupe cache, a per-silo hint that the durable causal-apply buffer may be non-empty, and the FIFO-diagnostic tracker. Durable coordination flows through the per-origin high-water-mark grain, the per-tree causal-apply buffer grain, and the per-tree apply grain (`StatelessWorker`). The HWM and buffer grains serialize their own turns, but they do not serialize whole `ApplyAsync` calls: two concurrent deliveries for the same `(tree, origin)` pair can both reserve or apply before either advances the HWM, which is why the shadow-forward identity cache and per-key last-writer-wins idempotence at the leaf, not the HWM, absorb a concurrent duplicate. Calls for different pairs are independent.

## Bootstrap handoff

The per-origin vector is the explicit handoff contract for the bootstrap protocol: a newly-bootstrapped peer calls `MergeBootstrapFrontierAsync`, which atomically raises the receiver's local vector clock to the pointwise maximum of the held vector and the snapshot's causal-stable frontier, then clears any legacy pinned-floor state an earlier build persisted. The coordinator immediately drains the durable causal-apply buffer because the merge can satisfy dependencies without a later HWM advance. The receiver then resumes incremental replication from that vector with idempotent apply across the snapshot / incremental boundary: duplicates are absorbed by the shadow-forward identity cache and per-key merge, while new point writes at or below one origin coordinate are still admitted. `PinSnapshotAsync` keeps replace semantics only for `IReplicationLocalVcSeeder` after an intra-cluster restore; `GetPinnedFloorAsync` remains only for rolling-upgrade compatibility with older silos that still read the slot, and after this build pins or merges, those silos see an empty floor and drop nothing.

## Caveats

- **Range deletes preserve the producer's issue HLC, not per-leaf HLCs.** The wire carries the HLC the producer pinned for that run of the range delete and the receiver stamps every tombstone with it, so LWW resolution against a concurrent write compares against the producer's authoring HLC rather than the receiver's local clock. An entry persisted by an older producer carries `HybridLogicalClock.Zero` and falls back to fresh local HLCs, where LWW resolution depends on the local clock at apply time. Idempotence at the leaf layer is what makes a re-applied range delete safe.
- **The HWM is per-origin, not per-shard.** A receiver applying entries from origin `X` against a tree split into N shards advances a single HWM row keyed `(tree, X)` regardless of which shard the entry targets. This is intentional: the HWM contract is the bootstrap-handoff seam, and bootstrap operates per-origin not per-shard.

## Batch apply path

Inbound transports deliver batches of `WalRecord` records, not single entries: a 256-entry gRPC push from a single producer is one network round-trip carrying 256 mutations. `ApplyBatchAsync` is the seam that lets the receiver process such a batch as one logical operation rather than 256 independent `ApplyAsync` calls - for each contiguous run of entries sharing a tree, origin, and merge mode it reads the high-water-mark once, merges the run's plain point writes in one batched grain call instead of one apply call per entry, advances the high-water-mark once, and drains the causal-apply buffer once at the end of the run instead of after every successful apply.

The default-interface-method body provides backward-compatible semantics: it loops over `ApplyAsync` and aggregates the per-entry results - `Applied` if any entry was newly merged, the pointwise-maximum `HighWaterMark`, and `Deferred` if any entry was deferred - so any custom `IReplicationApplier` written before the batch seam existed continues to work without changes, and a restore saga's receive-fence deferral returned by its `ApplyAsync` still reaches the receive path as a deferred, cursor-preserving result. The shipped applier overrides the batch path with the optimised implementation described below.

### Run grouping

The optimised batch path walks the inbound list and identifies maximal contiguous runs of entries that share the same `(TreeId, OriginClusterId, Mode)` tuple (a well-formed batch carries one merge mode per run, so including the mode never splits a legitimate run). For a 256-entry batch shipped by a single producer the entire batch is one run; for an interleaved batch (e.g. a snapshot recovery merge that intersperses entries from two origins) the path emits one run per contiguous group, each amortised independently. Within a run:

- A single `GetAsync` reads the persisted per-origin HWM at the start of the run. There is no `GetPinnedFloorAsync` read.
- There is no in-batch HLC threshold or running-HWM dedupe accumulator: a below-max-applied-HLC entry is a genuine write under non-monotonic per-origin HLC unless the exact identity cache or a later idempotent merge proves it redundant.
- Causal-dependency entries fetch the local vector clock lazily on first use and reuse it until an apply has occurred, at which point a `localVcDirty` flag forces a re-fetch on the next causal-dep check.
- A single `TryAdvanceAsync` advances the persisted HWM to the highest applied HLC at the end of the run.
- The causal-apply buffer is drained once, if the run advanced the persisted HWM.
- Non-prepared `LwwRegister` `Set` / `Delete` entries that pass classification are deferred and merged in one batched grain call per run, and non-prepared typed-CRDT `Set` entries that carry a delta (every CRDT mode except `OrMap`) fold in one batched delta call. Range deletes, saga terminal marks, prepared entries, CRDT-mode deletes, and `OrMap` or delta-less CRDT entries flush the pending batch first and take their own per-entry apply hop.

For a 256-entry single-origin `LwwRegister` batch this collapses roughly 3 x 256 = 768 grain round-trips on the per-entry path (a high-water-mark read, a point apply, and a high-water-mark advance per entry) to about three (one HWM read, one batched merge, and one HWM advance) - the dominant receiver-side cost on every inbound push.

### Preserved per-entry semantics

Every classification the per-entry path produces survives the batch path, except the tombstone-reap no-op (the last bullet):

- **Range-delete entries** bypass point-write identity dedup and HWM advance, and apply unconditionally (a range apply is naturally idempotent at the leaf layer).
- **Local-origin runs** classify every entry as `Dedup` with `HighWaterMark = HybridLogicalClock.Zero` and emit no grain calls.
- **No HLC-threshold dedup** runs in either path: point writes below the current HWM, or below a legacy snapshot coordinate, still enter the apply pipeline unless another gate filters them.
- **Causal-park** is exercised per-entry; only the local-vector-clock fetch is lazy.
- **Per-entry instrumentation** (`ApplyDuration`, `ApplyLag`, `ApplyFifoViolations`) is recorded inside the per-entry loop so observability is preserved verbatim.
- **Single-entry batches** defer to `ApplyAsync` so behaviour is bit-identical with the legacy receiver for the trivial case.
- **Tombstone-reap envelopes** are not acknowledged as `dedup` on the batch path: it has no no-op branch for them, so a multi-entry batch carrying one faults on that entry, because the point-apply step has no rule for it. The dead-letter-tracking decorator then falls back to per-entry apply, which does acknowledge the envelope as a no-op. The sender never ships these envelopes, so only an older shipper or a hand-built caller can deliver one.

### Failure model

Per-entry failures inside the batch surface as `ApplyAsync`-equivalent exceptions. The gRPC receiver endpoint wraps the batch call in a transport-level exception so the sender's backoff/retry loop kicks in for the whole batch - partial-batch acceptance is not a guarantee the seam offers. The dead-letter-tracking applier decorator falls back to per-entry routing when any entry in the batch already has retry history, or when the batch call throws part-way, so its DLQ accounting is exact. On that per-entry fallback (and for a single-entry batch) the decorator itself records the inbound per-peer contact, which the batch path otherwise records once per run; entries from runs the failed batch attempt already recorded can therefore record contact a second time for the same push (see [Observability](observability.md#bidirectional-peerlast_contact_seconds-and-the-liveness-probe)).

### Parallel apply across independent runs

Under multi-tree load the per-run walk can serialise otherwise-independent work: a batch that interleaves runs from several trees applies them one after another even though they share no per-tree state, inflating apply latency and `apply.lag` (which now also drives receiver back-pressure, so slow applies translate directly into sender throttling).

`LatticeReplicationOptions.ApplyMaxParallelRuns` bounds how many **independent** runs the batch path may apply concurrently. Independence is defined at the **tree** granularity:

- Runs targeting **distinct trees** may apply in parallel. Distinct trees share no per-tree state - separate causal-apply buffers, shadow-forward dedupe caches, high-water-mark grains, and apply grains - so concurrent apply cannot reorder or interleave their work.
- Runs that **share a tree** (different origins of the same tree) stay in one ordered group and apply strictly sequentially in write-ahead-log order. The per-tree causal-apply buffer and shadow-forward dedupe cache are shared across a tree's origins, so keeping same-tree runs serialised guarantees those structures observe the exact access order the fully-sequential path produces.

Parallelism is therefore only ever introduced **across** independent runs, never **within** one. Every within-run ordering invariant holds unchanged regardless of the configured degree of parallelism: per-origin FIFO, the causal dependency gate and its bounded buffer, per-origin high-water-mark monotonicity, and atomic-batch (saga) apply boundaries. A multi-entry run still collapses to a single batched merge; an atomic batch still applies as a unit on its owning run.

The effective degree of parallelism for a given batch is the largest `ApplyMaxParallelRuns` configured for any tree in that batch (the option resolves per tree), clamped to the number of distinct trees present in it, and is bounded by a per-batch semaphore so concurrency can never amplify local WAL saturation beyond the configured cap. It is surfaced on the `apply.parallel_runs` histogram (see [observability](observability.md)).

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
into the receiver leaf's visible projection and the saga's terminal
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
trees this receiver actually replicates (its per-tree enrollment - the
`LatticeReplicationOptions.ReplicatedTrees` declaration or a runtime-enabled
tree); the tree that received the
terminal is always included. A participant tree not replicated here is
excluded, so a cross-tree batch spanning a mix of replicated and
non-replicated trees stays valid - the barrier completes on the present
subset rather than waiting forever on a tree that never ships here.

The wait set is fixed when the barrier opens. The first terminal freezes it
and the coordinator persists it; it is never recomputed from the
receiver's live configuration (#4692). The applier still computes each
terminal's wait set from the trees replicated here at that moment, so a
configuration change between two terminals of one operation can hand a
later terminal a different set. The coordinator logs that and keeps the
frozen one. A tree that was not replicated here when the set froze, and
whose terminal arrives later, joins the set itself (it is arriving, so it
adds nothing to wait for) after the same cluster-identity check the freeze
makes; once the barrier has decided, it is finalized with the decided
verdict.

A participant that stops being replicated here before its own terminal
arrives would otherwise be waited for for ever: its terminal is dropped at
the enrollment gate. That drop - and only that one, the gate's
not-replicated arm - tells the barrier the tree is absent. An undecided
barrier that still waits for it removes it from the wait set and, if every
remaining tree has arrived, decides by the usual rule; the applier then
finalizes the remaining trees. The dropped tree's pending bucket of the
sub-saga is discarded, because the tree is no longer a replica of the
origin. A terminal dropped or deferred for any other reason (a merge-mode
mismatch, an apply failure) never decides the barrier. A drop that would
leave the wait set empty never decides it either (#4741): with no tree left
to vote, the commit-iff-every-arrival rule would hold vacuously, and a later
arrival of the dropped tree would be finalized with a verdict nothing
reported. The barrier instead withdraws its index entry and clears to
unopened, so a later arrival opens a fresh barrier and decides on its own
verdict. A barrier that has
not opened is left untouched and persists nothing, since the tree and
operation ids are peer-supplied.

Once a tree's per-source-shard gate is final, a cross-tree terminal does
**not** flip that tree's registry directly. Instead the receiver durably
registers the tree's local txid as delegated to a **receiver coordinator
grain** (keyed by
`(originClusterId, operationId)`) and notifies it of this tree's arrival
and commit/abort vote. The coordinator decides only once a terminal has
arrived for every tree in the wait set, committing iff every arrived tree
voted commit. Before the decision, a delegated read on any participating
tree's registry resolves `InFlight` against the coordinator, so every
tree stays invisible (an unreachable coordinator resolves `Indeterminate`,
which likewise keeps the keys invisible); after it, the receiver flips every
participating tree together. The coordinator only ever returns the decision (it never
calls back into a tree grain); the calling tree grain performs the
per-tree finalizes - itself inline, siblings via their apply grains - so
there is no circular wait. A null/empty `crossTreeOperationId` routes the
terminal through the legacy single-tree gate unchanged.

A bootstrap or re-seed of one participating tree arrives at the barrier
too. Its export carries a decision row that names the sub-saga's
cross-tree operation, and the drain records the tree's arrival with that
verdict as the tree's terminal would. A fresh bootstrap holds its imported
shadow copy unpublished until the barrier decides; a persisted legacy
in-place import keeps its read fence. Neither path serves the imported
post-saga tree beside a sibling that is still pre-saga
([#4683](https://github.com/NSTA1/Orleans.Lattice/issues/4683); see
[Snapshot bootstrap](snapshot-bootstrap.md#snapshot-and-in-flight-atomic-visibility)).

The receiver acknowledges a cross-tree terminal only once the barrier has
recorded it: the apply hop returns after the coordinator persisted the
arrival, and a notify that fails fails the apply, so the batch is not
accepted. The origin relies on this when it purges a cross-tree decision
([#4684](https://github.com/NSTA1/Orleans.Lattice/issues/4684); see
[Cross-tree decision purge hold](replication-drivers.md#cross-tree-decision-purge-hold)).
Saga terminals are never parked in the causal apply buffer or dead-lettered
(a terminal that exhausts its retries is deferred instead), and a
multi-shard sub-saga notifies on its final source shard's terminal. The one
terminal acknowledged without reaching the barrier is the enrollment gate's
drop of a tree no longer replicated here, which removes the tree from the
barrier instead (above). A barrier also registers itself, before it persists
its wait set, under every tree it waits for (the cross-tree barrier index,
keyed by the receiver tree), and withdraws once decided, so an import of one
of those trees can find it.

An index entry never outlives the barrier state it points to
([#4730](https://github.com/NSTA1/Orleans.Lattice/issues/4730)). The
withdrawal on decision is best effort, so an import does not trust an entry:
it asks the barrier, serialized with whatever opens or decides it, whether it
still holds the tree. A barrier holds the tree only while it has opened,
waits for the tree, and has no durable decision. Otherwise it withdraws the
entry, and it never holds the tree's fence. That covers a barrier that never
opened, because its open write failed after it indexed itself: its sibling's
terminal is not acknowledged until the notify succeeds, so the sibling
boundary keeps the tree fenced until it is redelivered. When a decided
barrier's retention runs, it withdraws itself from every index and keeps a
**decided tombstone** (identity and verdict) instead of clearing its state.
The origin can ship the operation again for as long as it stores the
decision: a rewind re-ships its terminals, and an export carries its decision
row while the cross-tree decision purge hold is unreleased. A cleared barrier
would reopen on such an arrival and wait for ever for a sibling whose terminal
was acknowledged long ago. The tombstone finalizes the arriving tree with the
verdict instead.

A tombstone is dropped once no arrival of its operation can reach this
receiver any more
([#4733](https://github.com/NSTA1/Orleans.Lattice/issues/4733)). The origin
advertises a **cross-tree purge frontier** per tree: a decision sequence at or
below which it stores no cross-tree decision of the tree and never will again
(see [Cross-tree decision purge hold](replication-drivers.md#cross-tree-decision-purge-hold)).
It sends a chunk of it beside every push, as the
`x-lattice-replication-cross-tree-purge-frontier` header. That header is read
only on a push whose origin is authenticated and a configured peer, and parsed
strictly and bounded. The receiver keeps the highest value per (origin, tree)
(the cross-tree purge frontier). A tombstone records the operation's decision
sequences and **every** participant the operation named, not only the trees
replicated here: a participant that becomes replicated here later can still
import the operation's decision row while the origin stores it. The tombstone
is listed under each participant, and is dropped once every participant's
frontier has reached its sequence:
- when a frontier advance sweeps the listing; or
- when the retention that creates it reads the frontier itself, after listing
  it, so neither can miss the other.

An operation decided before sequencing counts as sequence 0 on every
participant: its tombstone drops once the origin stores no such decision. A
tombstone with no recorded participants (an older build's) is kept. Both kept
sets are finite - only operations decided before the upgrade - and each is
logged when its retention runs. Every sweep logs, per tree, how many
tombstones it dropped and how many are still held, so the held count can be
seen draining to that fixed residue. Dropping
relies on the purge hold: the origin purges a cross-tree decision only after
every peer acknowledged the operation's terminals, so no terminal can be
re-shipped after the frontier passes it.

A shipped cross-tree terminal carries the operation's **decision stamps**:
per participating tree, the export epoch the origin read after the decision
was durable (`WalRecord.CrossTreeDecisionStamps`, on the wire only). The
applier records them on the barrier before it applies the terminal. A
barrier compares each tree that has not arrived with the tree's latest
snapshot import, which the same index records, and records the tree's
arrival with the operation's verdict when that import named the operation
nowhere and its export opened after the decision
([#4684](https://github.com/NSTA1/Orleans.Lattice/issues/4684); see
[Snapshot bootstrap](snapshot-bootstrap.md#snapshot-and-in-flight-atomic-visibility)).
Such an arrival finalizes nothing, because the import already settled the
tree; a real terminal of the tree that arrives later replaces it, so its
pending bucket is still finalized.

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
  the per-source-shard arrival tally on terminal records. It also reads
  `AtomicBatchSize` to recognise saga prepare-phase entries (which
  bypass the causal-dependency gate) and to reject a
  range delete that carries atomic-batch metadata, and it forwards
  `AtomicBatchSize` and `AtomicBatchIndex` with every prepared write
  into the per-transaction staging hop.
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
