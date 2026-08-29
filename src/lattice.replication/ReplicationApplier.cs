using Orleans.Lattice.BPlusTree.Grains;
using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="IReplicationApplier"/> implementation. Resolves
/// the per-origin high-water-mark for the entry's
/// <c>(treeId, originClusterId)</c> pair, filters re-delivery, runs
/// the causal-plus dependency check - parking entries whose
/// declared <see cref="WalRecord.VectorClock"/> is not yet
/// dominated by the local vector clock - and routes the entry through
/// the core library's <see cref="IReplicationApplyGrain"/> seam so
/// the persisted <c>LwwValue&lt;byte[]&gt;</c> carries the remote
/// cluster's HLC and origin id verbatim. The HWM is advanced only
/// after the apply returns successfully; every advance triggers a
/// drain of the per-tree causal-apply buffer so blocked entries
/// whose deps are now satisfied wake up and apply in FIFO order.
/// </summary>
internal sealed partial class ReplicationApplier(
    IGrainFactory grainFactory,
    IOptionsMonitor<LatticeReplicationOptions> options,
    CrdtShapeRegistry? crdtShapes = null,
    ILogger<ReplicationApplier>? logger = null,
    ReplicationPeerStats? peerStats = null,
    ReceiverAppliedContentIndex? appliedContentIndex = null,
    ILatticeReplicationContext? replicationContext = null,
    IReplicationReceiveGate? receiveGate = null,
    IReplicationTenantIsolationGate? tenantIsolationGate = null) : IReplicationApplier
{
    /// <summary>
    /// Optional tenant-isolation gate (issue #1633). When non-<see langword="null"/>
    /// and <see cref="IReplicationTenantIsolationGate.IsActive"/> is
    /// <see langword="true"/> (the tenancy add-on wired a real gate), every inbound
    /// run is classified against the tenant namespace its tree id names: a write for
    /// a non-existent tenant, or for a tenant not resident in this serving region, is
    /// dead-lettered rather than applied, and never auto-creates a tenant. The gate
    /// enforces the isolation boundary only - it never gates on quota, because a
    /// replicated apply converges a write that already happened on the origin.
    /// Optional so existing call sites that construct the applier without a gate
    /// continue to compile and behave exactly as before (isolation not enforced);
    /// the null default's <see cref="IReplicationTenantIsolationGate.IsActive"/> is
    /// <see langword="false"/>, so replication is byte-for-byte unchanged when
    /// tenancy is off.
    /// </summary>
    private readonly IReplicationTenantIsolationGate? _tenantIsolationGate = tenantIsolationGate;

    /// <summary>
    /// Optional inbound receive fence. When non-<see langword="null"/> and a
    /// tree's receive fence is engaged by an in-flight restore saga, peer
    /// entries for that tree are deferred (returned as an un-applied no-op with
    /// the HWM unchanged) rather than union-merged, so a laggard's post-cut
    /// entries cannot re-advance a tree that has already flipped. Optional so
    /// existing call sites that construct the applier without a gate continue to
    /// compile and behave as before (never paused).
    /// </summary>
    private readonly IReplicationReceiveGate? _receiveGate = receiveGate;

    private readonly ILogger<ReplicationApplier> _logger =
        logger ?? NullLogger<ReplicationApplier>.Instance;

    /// <summary>
    /// One-shot guard for the "no enrollment source wired" misconfiguration
    /// warning (issue #1398). Flipped to 1 the first time the fail-closed
    /// <see cref="InboundTreeAdmission.RejectNoEnrollmentSource"/> arm drops an
    /// entry, so the diagnostic - and its per-entry string interpolation - fires
    /// once per applier rather than on every dropped inbound entry. Never
    /// touched on the steady-state (enrolled) apply path.
    /// </summary>
    private int _noEnrollmentSourceWarned;

    /// <summary>
    /// Optional receiver-side applied-content index. When non-<see langword="null"/>
    /// and the entry's tree has
    /// <see cref="LatticeReplicationOptions.ContentHashDedupEnabled"/>
    /// set, a successfully-applied point-<see cref="MutationKind.Set"/>
    /// records its key-to-content-hash mapping so a subsequent inbound
    /// content-manifest exchange can report the receiver already holds
    /// the content; an applied <see cref="MutationKind.Delete"/> removes
    /// the key and a <see cref="MutationKind.DeleteRange"/> invalidates
    /// the whole tree's index. Maintained off-path-free when the
    /// content-hash dedup master switch is off (the index stays empty).
    /// </summary>
    private readonly ReceiverAppliedContentIndex? _appliedContentIndex = appliedContentIndex;

    /// <summary>
    /// Optional injectable view of the host's replication configuration. When
    /// non-<see langword="null"/> it is the canonical signal for whether a
    /// participant tree is replicated on this receiver (see
    /// <see cref="IsTreeReplicatedHere"/>): the production
    /// <see cref="ConfiguredLatticeReplicationContext"/> reports a non-null
    /// <see cref="ILatticeReplicationContext.ResolveMergeMode"/> for exactly
    /// the trees that are replicated here (it delegates to the same per-tree
    /// resolver the shipper, change feed, and bootstrap path consult), and a
    /// host that opts trees in through a custom resolver is honoured too.
    /// Optional so existing call sites that construct the applier without a
    /// context continue to compile, falling back to the raw
    /// <see cref="LatticeReplicationOptions.ReplicatedTrees"/> map.
    /// </summary>
    private readonly ILatticeReplicationContext? _replicationContext = replicationContext;

    /// <summary>
    /// Optional per-peer telemetry sink. When non-<see langword="null"/>
    /// the batch-apply path records an inbound success
    /// (<see cref="ReplicationPeerStats.RecordInboundSuccess(string, string)"/>)
    /// or failure
    /// (<see cref="ReplicationPeerStats.RecordInboundError(string, string)"/>)
    /// per per-origin run keyed by the entries'
    /// <see cref="WalRecord.OriginClusterId"/>, so the bidirectional
    /// <c>peer.last_contact_seconds{direction="inbound"}</c> and
    /// <c>peer.consecutive_errors{direction="inbound"}</c> observable
    /// gauges surface receiver-side liveness. Optional so existing
    /// call sites (and tests) that construct the applier without a
    /// stats sink continue to compile.
    /// </summary>
    private readonly ReplicationPeerStats? _peerStats = peerStats;

    /// <summary>
    /// Per-tree causal-apply buffers, lazily created on first park.
    /// Each tree's buffer is independent - there is no cross-tree
    /// coordination. The map itself is concurrent because Orleans
    /// grain calls into a singleton applier may interleave across
    /// trees; per-buffer concurrency is enforced by the buffer's
    /// internal lock.
    /// </summary>
    private readonly ConcurrentDictionary<string, CausalApplyBuffer> _buffers = new(StringComparer.Ordinal);

    /// <summary>
    /// Per-tree shadow-forward dedupe caches, lazily created on first
    /// non-range-delete entry per tree. Each cache holds a bounded
    /// FIFO of recently-applied
    /// <c>(originClusterId, timestamp, key, op)</c> identity tuples
    /// and rejects the duplicate-emit pair structural rewrites
    /// (shard split / merge / saga compensate) generate when they
    /// shadow-forward a user write into a different shard. The cache
    /// is a fast-path race-killer: under concurrent inbound
    /// delivery, both duplicate emits can otherwise observe the same
    /// pre-advance per-origin high-water-mark and both pass the HWM
    /// check before either advances it. Correctness is still bounded
    /// by the HWM - cache eviction under sustained churn cannot
    /// cause a re-merge.
    /// </summary>
    private readonly ConcurrentDictionary<string, RecentApplyCache> _dedupeCaches =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Per-<c>(treeId, originClusterId)</c> last-applied source HLC. Used
    /// to surface a transport-side regression that breaks the per-origin
    /// FIFO invariant the causal-apply pipeline relies on for occupancy
    /// bounds: under correct sender + transport behaviour every
    /// successful apply for a given origin has an HLC strictly greater
    /// than the previous successful apply for that origin (the producer's
    /// partitioned change feed yields per-shard in WAL-offset order and
    /// each shard's WAL is HLC-monotonic per origin). A violation does
    /// not change apply behaviour - the entry is still applied and the
    /// HWM is still advanced - it only increments the
    /// <see cref="LatticeReplicationMetrics.ApplyFifoViolations"/>
    /// counter so an alert on <c>rate &gt; 0</c> flags the regression.
    /// Updated on successful apply (not on park) so the invariant tracks
    /// "what has been merged" rather than "what has been observed".
    /// </summary>
    private readonly ConcurrentDictionary<(string TreeId, string Origin), HybridLogicalClock> _lastAppliedSourceHlc =
        new();

    /// <inheritdoc />
    public async Task<ApplyResult> ApplyAsync(WalRecord entry, CancellationToken cancellationToken = default)
    {
        // SYSTEM-ORIGIN APPLY BYPASS (issue #982). Replication-applied writes to
        // the reserved membership/auth system trees (and every other replicated
        // tree) are receiver-side convergence, not user writes: the remote
        // cluster already authorized the originating write, and the "caller" on
        // this side has no user identity. Enter the system-origin scope for the
        // whole apply so the core access gate is bypassed on every sub-path this
        // method drives - both the LWW apply seam (which writes below the gate)
        // and the CRDT/prepared paths that route through the gated public
        // ILattice methods (ApplyCrdtDeltaAsync / SetAsync). Without this a
        // replicated policy revoke would be rejected by the receiver's own gate
        // because the apply has no subject. The scope flows on RequestContext to
        // every outgoing grain call and is nest-safe/idempotent on dispose; it
        // does not affect WAL capture, re-shipping, or the change feed.
        using var systemOrigin = LatticeAccessGateContext.EnterSystemOrigin();

        // Apply-duration instrumentation: record the wall-clock duration
        // of every terminal apply outcome (success / dedup / failure /
        // parked-causal-buffer) into the apply.duration histogram. The
        // stopwatch is allocation-free (a long timestamp captured via
        // Stopwatch.GetTimestamp); the outcome local is updated before
        // each return so the finally records the matching tag value.
        // A throw out of the body unwinds with the default outcome
        // (failure), which is correct because every uncaught exception
        // here represents a failed apply attempt.
        var startTimestamp = Stopwatch.GetTimestamp();
        var outcome = LatticeReplicationMetrics.OutcomeFailure;
        try
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (string.IsNullOrEmpty(entry.TreeId))
            {
                throw new ArgumentException("WalRecord.TreeId must be non-empty.", nameof(entry));
            }

            if (string.IsNullOrEmpty(entry.OriginClusterId))
            {
                throw new ArgumentException(
                    "WalRecord.OriginClusterId must be non-empty for replication apply.",
                    nameof(entry));
            }

            // RECEIVER-SIDE ENROLLMENT / MERGE-MODE GATE (issue #1267). The
            // inbound apply path must not write a tree this cluster kept
            // cluster-local by not enrolling it: the core ThrowIfSystemTree
            // reserved-prefix check covers only the `_lattice_` core trees, not
            // the `sys-`-prefixed authorization / identity trees, and the
            // peer-supplied OriginClusterId is unverified. Reject an entry whose
            // tree is not enrolled here (drop it - a non-enrolled tree id is
            // peer-controlled, so dead-lettering it would let a peer spawn
            // unbounded DLQ activations) and dead-letter an entry whose
            // peer-supplied wire mode disagrees with the locally resolved mode
            // for an enrolled tree (re-resolving the mode locally rather than
            // trusting the wire field). If the receiver has no enrollment source
            // wired at all the gate cannot be evaluated, so it fails closed and
            // drops the entry too (issue #1398). Enforced before any grain call
            // so the rejection is cheap and covers every transport that funnels
            // through the applier.
            switch (ClassifyInboundTree(in entry))
            {
                case InboundTreeAdmission.RejectNotReplicated:
                    _logger.LogWarning(
                        "Rejected inbound replication entry for tree '{Tree}' from origin '{Origin}': "
                        + "the tree is not enrolled for replication on this receiver.",
                        entry.TreeId, entry.OriginClusterId);
                    outcome = LatticeReplicationMetrics.OutcomeRejectedNotReplicated;
                    return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };

                case InboundTreeAdmission.RejectNoEnrollmentSource:
                    // Fail closed on ambiguity (issue #1398): with no enrollment
                    // source wired at all the gate cannot be evaluated, so the
                    // entry is dropped like a non-enrolled tree (no dead-letter -
                    // the tree id is peer-controlled). Unreachable in production
                    // (the context is always registered); reachable only by a
                    // mis-wired hand-built applier, so warn once - the interpolated
                    // message must not allocate per inbound entry on this arm.
                    if (Interlocked.Exchange(ref _noEnrollmentSourceWarned, 1) == 0)
                    {
                        _logger.LogWarning(
                            "Dropping inbound replication entry for tree '{Tree}' from origin '{Origin}': "
                            + "no replication enrollment source is configured on this receiver "
                            + "(no ILatticeReplicationContext and no ReplicatedTrees map), so the "
                            + "enrollment gate cannot be evaluated. All inbound entries are dropped "
                            + "until a replication context is wired. This warning is logged once.",
                            entry.TreeId, entry.OriginClusterId);
                    }
                    outcome = LatticeReplicationMetrics.OutcomeRejectedNotReplicated;
                    return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };

                case InboundTreeAdmission.RejectModeMismatch:
                    var expectedMode = ResolveLocalMergeMode(entry.TreeId, out _)!.Value;
                    _logger.LogWarning(
                        "Rejected inbound replication entry for tree '{Tree}' from origin '{Origin}': "
                        + "wire merge mode '{WireMode}' disagrees with the locally resolved mode '{LocalMode}'.",
                        entry.TreeId, entry.OriginClusterId, entry.Mode, expectedMode);
                    await DeadLetterModeMismatchAsync(entry, expectedMode, cancellationToken).ConfigureAwait(false);
                    outcome = LatticeReplicationMetrics.OutcomeRejectedModeMismatch;
                    return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
            }

            // RECEIVER-SIDE TENANT-ISOLATION GATE (issue #1633). After the tree is
            // confirmed enrolled here, keep the write inside its correct tenant
            // namespace. The owning tenant is derived from the tree id alone (never
            // from a wire-supplied field), so a peer cannot redirect a write into a
            // foreign tenant. A write whose tree names a non-existent tenant, or a
            // tenant not resident in this serving region, is refused and
            // dead-lettered (the tree is enrolled and therefore bounded) with the
            // HWM left unchanged so the sender re-ships and convergence recovers once
            // the tenant exists / becomes resident. This is the isolation boundary
            // only: it never gates on quota (a replicated apply converges a write
            // that already happened on the origin), and it is bypassed entirely when
            // tenancy is off - the null gate's IsActive is false, so this is a single
            // bool read that leaves replication byte-for-byte unchanged. Platform /
            // definition trees and bare legacy trees always admit, so definitions
            // converge everywhere.
            if (_tenantIsolationGate is not null && _tenantIsolationGate.IsActive)
            {
                var decision = await _tenantIsolationGate
                    .EvaluateAsync(entry.TreeId, cancellationToken).ConfigureAwait(false);
                if (decision != ReplicationTenantIsolationDecision.Admit)
                {
                    await DeadLetterTenantIsolationAsync(entry, decision, cancellationToken)
                        .ConfigureAwait(false);
                    outcome = decision == ReplicationTenantIsolationDecision.RejectOutOfRegion
                        ? LatticeReplicationMetrics.OutcomeRejectedTenantOffline
                        : LatticeReplicationMetrics.OutcomeRejectedForeignTenant;
                    return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
                }
            }

            // DURABLE RECEIVE FENCE (issue #1173). While a cross-cluster restore
            // saga has paused inbound apply for this tree, peer entries must not
            // be admitted: an early-flipping cluster that applied a laggard's
            // still-advanced post-cut entries would union-merge and re-advance
            // itself. Defer the entry with an explicit Deferred=true signal (and
            // HWM unchanged) so the receive path returns a not-accepted,
            // cursor-preserving ack; the sender keeps its cursor and re-ships the
            // same entry once the fence lifts on global completion. The gate is
            // fronted by a short in-memory cache so this is not a per-entry grain
            // call.
            if (_receiveGate is not null
                && await _receiveGate.IsReceivePausedAsync(entry.TreeId, cancellationToken).ConfigureAwait(false))
            {
                outcome = LatticeReplicationMetrics.OutcomeDedup;
                return new ApplyResult
                {
                    Applied = false,
                    HighWaterMark = HybridLogicalClock.Zero,
                    Deferred = true,
                };
            }

            // Defence-in-depth: tombstone-reap envelopes
            // (`MutationKind.Tombstone`) are local structural cleanup
            // records and are filtered out at the producer boundary by
            // `ReplicationShipperGrain.ShouldShip` / `ChangeFeed.Subscribe`.
            // A receiver should therefore never see one in the steady
            // state; an older shipper or a hand-built apply call site
            // could still deliver one. Surface it as an explicit
            // dedup-shaped no-op (Applied=false, HWM unchanged) so the
            // entry is acknowledged without faulting the apply loop.
            // The category signal is not preserved through `WalRecord`
            // (no Category slot), so the guard keys on `Op` directly.
            if (entry.Op == MutationKind.Tombstone)
            {
                outcome = LatticeReplicationMetrics.OutcomeDedup;
                return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
            }

            var resolved = options.Get(entry.TreeId);
            if (string.Equals(entry.OriginClusterId, resolved.ClusterId, StringComparison.Ordinal))
            {
                // Defence-in-depth: a local-origin entry must never be applied
                // back onto its authoring cluster. The outbound ship loop's
                // origin filter already prevents this in the steady state, but
                // hand-built apply pipelines and tests can still hand us such
                // an entry - surface it as an explicit no-op rather than
                // silently merging into the same cluster's state.
                outcome = LatticeReplicationMetrics.OutcomeDedup;
                return new ApplyResult { Applied = false, HighWaterMark = HybridLogicalClock.Zero };
            }

            // Range deletes carry HybridLogicalClock.Zero by design (the walk
            // produces many per-leaf HLCs that cannot be faithfully collapsed),
            // so per-origin HWM dedupe does not apply to them. Range applies
            // are naturally idempotent at the leaf layer.
            if (entry.Op == MutationKind.DeleteRange)
            {
                // Defence-in-depth: a DeleteRange entry tagged with
                // atomic-batch metadata (AtomicBatchSize > 0) is a
                // producer-contract violation - the producer's
                // SetManyAtomicAsync surface emits only Set/Delete, never
                // DeleteRange, so atomic-batch stamps on a range op are
                // intrinsic ambiguity (no consistent saga key, no
                // single-HLC commit point). Surface it as an explicit
                // ArgumentException so the producer fails fast rather
                // than the receiver silently applying a non-atomic range
                // delete that carries an unfulfilled atomic-batch
                // promise. The violation is producer-shaped, not
                // receiver-shaped - the wire slot remains additive on
                // every WalRecord so the receiver-side prepared-Set /
                // prepared-Delete primitive can route Set/Delete entries
                // through the per-tx pending bucket while DeleteRange
                // entries are explicitly rejected here.
                if (entry.AtomicBatchSize > 0)
                {
                    throw new ArgumentException(
                        "WalRecord.Op=DeleteRange must not carry atomic-batch metadata "
                        + "(AtomicBatchSize > 0). Atomic batches must contain only Set / Delete entries; "
                        + "range deletes are emitted by the producer's DeleteRangeAsync surface, not "
                        + "SetManyAtomicAsync.",
                        nameof(entry));
                }

                await ApplyRangeAsync(entry, cancellationToken);
                InvalidateAppliedContentIndexForRange(in entry, resolved);
                outcome = LatticeReplicationMetrics.OutcomeSuccess;
                return new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Zero };
            }

            // Saga terminal-mark records (TxCommit / TxAbort) are
            // saga-id-keyed and idempotent on the receiver
            // (per-tree TxRegistry repeat-same-outcome no-op + per-leaf
            // _recentlyTerminal HashSet dedup). They bypass the
            // per-origin HWM check, the shadow-forward dedup cache, and
            // the causal-buffer parking path: those primitives are
            // per-key data-flow dedup primitives and have no defined
            // semantics on saga linearization records. The receiver-side
            // ApplyTxTerminalAsync routes the mark through the per-tree
            // ITxRegistryGrain (the linearization point readers dial
            // back through) and the addressed shard's
            // AppendTxTerminalAsync under a LatticeHlcOverrideContext
            // so the receiver's local WAL append re-stamps the source
            // cluster's terminal HLC verbatim - preserving the
            // cross-cluster ordering invariant on receiver replays.
            if (entry.Op is MutationKind.TxCommit or MutationKind.TxAbort)
            {
                await ApplyTxTerminalCoreAsync(entry, cancellationToken);
                outcome = LatticeReplicationMetrics.OutcomeSuccess;
                return new ApplyResult { Applied = true, HighWaterMark = HybridLogicalClock.Zero };
            }

            var hwmGrain = GetHwmGrain(entry.TreeId);
            var hwm = await hwmGrain.GetAsync(entry.OriginClusterId!, cancellationToken);

            // Point-write dedup gate: the SNAPSHOT-PINNED CAUSAL FLOOR,
            // not the incrementally-advanced per-origin diagonal.
            //
            // The source HLC is stamped per leaf (BPlusLeafGrain's own
            // clock) and WAL/replog partitions are keyed by
            // WalPartitionHash(key), so many independent per-leaf HLC
            // streams interleave within one origin cluster. In WAL-append
            // (delivery) order the per-origin HLC is therefore NOT
            // monotonic: a genuinely-new point write to a distinct key
            // routinely arrives with a source HLC below the highest
            // already-applied source HLC for the same origin. Gating on
            // the incremental diagonal (`entry.Timestamp <= hwm`) treated
            // every such entry as a duplicate and silently discarded it -
            // the receiver half of the #1060 replication-gap (US
            // shipped == EU applied == 1041 of 3967). Correctness for the
            // incremental stream is instead upheld by the shadow-forward
            // identity cache below (exact (origin, hlc, key, op) tuple)
            // plus the leaf-level per-key LWW guard (re-applying an
            // already-present (key, source-HLC) is a no-op). A below-
            // diagonal entry that survives both is a new write; its
            // out-of-order arrival is surfaced (observability-only) by the
            // FIFO-violation counter in RecordFifoState.
            //
            // The pinned floor IS a valid drop threshold: it is written
            // only by PinSnapshotAsync (bootstrap-snapshot handoff or
            // operator rollback re-pin), never by incremental
            // TryAdvanceAsync, so every origin entry at or below it is
            // provably contained in the pinned snapshot. Dropping those is
            // the exactly-once optimisation for the snapshot -> incremental
            // handoff: the peer may re-deliver a large below-snapshot
            // backlog that is already captured by the restore, and the
            // floor short-circuits it without a leaf round-trip. When no
            // snapshot has been pinned the floor is HybridLogicalClock.Zero
            // for every origin, so nothing is dropped.
            //
            // Bypasses (unchanged): bootstrap-drain delivery runs before
            // the post-drain pin, so its floor is still Zero and the
            // bypass is belt-and-braces; saga prepare-phase entries
            // (IsPrepared && AtomicBatchSize > 0) carry non-monotonic
            // per-leaf HLCs across the saga's touched leaves and are
            // deduped by the per-leaf AddPreparedMutation LWW merge + the
            // per-tx terminal-mark idempotency instead.
            var isBootstrapDrain = LatticeBootstrapApplyContext.IsActive;
            var isPreparedAtomicBatch = entry.IsPrepared && entry.AtomicBatchSize > 0;
            if (!isBootstrapDrain && !isPreparedAtomicBatch)
            {
                var pinnedFloor = await hwmGrain.GetPinnedFloorAsync(entry.OriginClusterId!, cancellationToken);
                if (entry.Timestamp <= pinnedFloor)
                {
                    outcome = LatticeReplicationMetrics.OutcomeDedup;
                    return new ApplyResult { Applied = false, HighWaterMark = hwm };
                }
            }

            // Shadow-forward dedupe cache: a structural rewrite (shard
            // split / merge / saga compensate) that shadow-forwards a
            // user write into a different shard generates a duplicate
            // emit pair with identical (origin, hlc, key, op). This cache
            // is the primary exact-identity dedup for incremental point
            // writes (the pinned-floor gate above only covers entries
            // provably inside a snapshot); it catches recent re-deliveries
            // without a leaf hop, and any re-delivery evicted from the
            // bounded cache falls through to the idempotent leaf-level LWW
            // apply (a no-op for identical bytes). Range deletes bypass it
            // entirely because they carry HLC.Zero (ambiguous identity).
            var cache = _dedupeCaches.GetOrAdd(
                entry.TreeId,
                static (_, capacity) => new RecentApplyCache(capacity),
                resolved.ShadowForwardDedupeCacheSize);
            if (!cache.TryAdd(entry))
            {
                outcome = LatticeReplicationMetrics.OutcomeShadowForwardDedup;
                return new ApplyResult { Applied = false, HighWaterMark = hwm };
            }

            // Roll back the cache reservation if the apply pipeline
            // throws (or is cancelled) after TryAdd succeeded. Without
            // rollback, a transient apply failure would leave a phantom
            // cache entry that suppresses the transport's retry path:
            // the retry would observe TryAdd=false (cache hit), classify
            // the call as Applied=false (shadow-forward-dedup), and the
            // dead-letter decorator's retry-counter contract would clear
            // the failure counter on what looks like a filtered call -,
            // silently dropping the entry until FIFO eviction admits a
            // future retry. The park branch returns normally inside the
            // try, so its cache reservation is correctly retained: the
            // drained entry routes through ApplyPointAsync directly,
            // bypassing the cache, and the retained reservation
            // continues to suppress duplicate-emit pairs of the parked
            // entry that arrive while it is buffered.
            try
            {
                // Causal-plus dependency check. Skip the fetch entirely
                // when the entry carries no declared dependencies - legacy
                // peers and pre-causal-plus entries decode VectorClock as null
                // and must continue to apply unconditionally on the existing
                // HWM-only path so this code is wire-compatible with the
                // additive vector-clock schema slot.
                //
                // Phase D1c: saga prepare-phase entries
                // (IsPrepared && AtomicBatchSize > 0) bypass the
                // causal-park gate for the same reason they bypass
                // the HWM gate (see the HWM dedup comment above):
                // parallel cross-leaf saga writes carry VectorClock
                // frontiers whose entries point at sibling per-leaf
                // clocks, and parking them would produce a
                // chicken-and-egg deadlock with their not-yet-arrived
                // siblings. The per-leaf AddPreparedMutation routes
                // the entry into the pending-tx bucket where causal
                // ordering across the saga's keys is irrelevant -
                // the terminal flip is the single atomic-visibility
                // transition.
                if (!isPreparedAtomicBatch && HasCausalDependencies(entry))
                {
                    var localVc = await hwmGrain.GetVectorAsync(cancellationToken);
                    if (!CausalApplyBuffer.DependenciesSatisfied(entry, localVc, resolved.ClusterId))
                    {
                        await ParkAsync(entry, resolved, cancellationToken);
                        outcome = LatticeReplicationMetrics.OutcomeParkedCausalBuffer;
                        return new ApplyResult { Applied = false, HighWaterMark = hwm };
                    }
                }

                await ApplyPointAsync(entry);
                RecordApplyLag(entry);
                RecordAppliedContentForIndex(in entry, resolved);

                // Bootstrap-drain bypass: skip the per-origin HWM
                // advance and the per-(tree, origin) FIFO-violation
                // tracker for entries delivered through a
                // <see cref="LatticeBootstrapApplyContext"/> scope.
                // The drain produces entries whose per-origin HLCs
                // are not globally ordered (the snapshot exporter
                // visits shards/leaves in arbitrary order), so a
                // mid-drain advance can later suppress a still-pending
                // saga key with a strictly-earlier HLC and break
                // per-saga all-or-nothing visibility, and recording
                // those non-monotonic HLCs on
                // <c>_lastAppliedSourceHlc</c> would surface every
                // out-of-order shard arrival as a spurious
                // <see cref="LatticeReplicationMetrics.ApplyFifoViolations"/>
                // increment (the counter is documented as a
                // transport-side FIFO-regression signal, which is
                // expressly not what bootstrap delivery is). The
                // post-drain
                // <see cref="Grains.IReplicationHighWaterMarkGrain.PinSnapshotAsync"/>
                // call installs the per-origin HWM at the snapshot
                // cut atomically; the first live-incremental entry
                // delivered after the pin seeds
                // <c>_lastAppliedSourceHlc</c> at the producer's
                // first post-pin HLC, which is the canonical FIFO
                // anchor for steady-state replication.
                if (isBootstrapDrain)
                {
                    outcome = LatticeReplicationMetrics.OutcomeSuccess;
                    return new ApplyResult { Applied = true, HighWaterMark = hwm };
                }

                RecordFifoState(entry);

                // Advance the HWM only after the apply commits.
                var advanced = await hwmGrain.TryAdvanceAsync(entry.OriginClusterId!, entry.Timestamp, cancellationToken);
                var newHwm = advanced
                    ? entry.Timestamp
                    : await hwmGrain.GetAsync(entry.OriginClusterId!, cancellationToken);

                // The advance may have unblocked entries parked by an earlier
                // delivery whose deps included this origin's diagonal. Drain
                // FIFO until the buffer reaches a fixed point - each drained
                // apply may itself advance the local vector clock, so re-fetch
                // before each pass.
                if (advanced)
                {
                    await DrainBufferAsync(entry.TreeId, hwmGrain, resolved, cancellationToken);
                }

                outcome = LatticeReplicationMetrics.OutcomeSuccess;
                return new ApplyResult { Applied = true, HighWaterMark = newHwm };
            }
            catch
            {
                cache.Remove(entry);
                throw;
            }
        }
        finally
        {
            RecordApplyDuration(entry.TreeId, entry.OriginClusterId ?? string.Empty, startTimestamp, outcome);
        }
    }

    /// <summary>
    /// Records a single sample on the <see cref="LatticeReplicationMetrics.ApplyDuration"/>
    /// histogram. Skipped when <paramref name="treeId"/> is empty so a
    /// validation throw on the tree-id guard does not publish a histogram
    /// sample with an empty <c>tree</c> tag (which would be unusable for
    /// per-tree alerting). The <paramref name="peerOriginClusterId"/> is
    /// emitted on the <see cref="LatticeReplicationMetrics.TagPeer"/>
    /// dimension so per-source-peer break-down is honoured by the
    /// instrument's documented schema; an empty value (e.g. when the
    /// origin guard threw before any apply work happened) is emitted
    /// verbatim rather than skipped so the histogram still records the
    /// failure outcome and operators get a stable cardinality. The
    /// duration is read via <see cref="Stopwatch.GetElapsedTime(long)"/>,
    /// which is allocation-free.
    /// </summary>
    private static void RecordApplyDuration(string treeId, string peerOriginClusterId, long startTimestamp, string outcome)
    {
        if (string.IsNullOrEmpty(treeId))
        {
            return;
        }

        var elapsedMs = Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds;
        LatticeReplicationMetrics.ApplyDuration.Record(
            elapsedMs,
            new System.Diagnostics.TagList
            {
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeId),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, peerOriginClusterId ?? string.Empty),
                new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOutcome, outcome),
                LatticeTenantLabel.ForTree(treeId),
            });
    }

    private static bool HasCausalDependencies(WalRecord entry) =>
        entry.VectorClock is { Entries.Count: > 0 };

    /// <summary>
    /// Records a successfully-applied point mutation into the
    /// receiver-side applied-content index so a subsequent inbound
    /// content-manifest exchange can answer "do I already hold
    /// byte-identical content for this key?". No-op when no index is
    /// registered or the tree's content-hash dedup master switch is off,
    /// so the index is maintained off-path-free under the default
    /// behaviour. Only last-writer-wins point Set / Delete entries that
    /// are not part of a not-yet-visible atomic-batch prepare phase are
    /// recorded: a Set stamps the key's content hash (computed with the
    /// same FNV-1a digest the sender manifests), a Delete removes the
    /// key. CRDT-mode entries are skipped because the receiver merges
    /// rather than overwrites them, so they are never elision-eligible
    /// and must always ship.
    /// </summary>
    private void RecordAppliedContentForIndex(in WalRecord entry, LatticeReplicationOptions resolved)
    {
        if (_appliedContentIndex is null
            || !resolved.ContentHashDedupEnabled
            || entry.Mode != LatticeMergeMode.LwwRegister
            || entry.IsPrepared
            || entry.AtomicBatchSize > 0
            || string.IsNullOrEmpty(entry.TreeId))
        {
            return;
        }

        if (entry.Op == MutationKind.Set)
        {
            _appliedContentIndex.RecordSet(
                entry.TreeId!,
                entry.Key ?? string.Empty,
                ReplicationContentHash.Compute(in entry),
                resolved.ContentHashDedupCacheSize);
        }
        else if (entry.Op == MutationKind.Delete)
        {
            _appliedContentIndex.RecordDelete(entry.TreeId!, entry.Key ?? string.Empty);
        }
    }

    /// <summary>
    /// Invalidates the receiver-side applied-content index for a tree
    /// after a range delete removed an arbitrary key span from the
    /// visible projection. The index has no range query, so the whole
    /// tree's entries are cleared rather than risk a stale "already
    /// holds" answer for a key the range delete removed (which would
    /// otherwise let the sender elide a payload the receiver no longer
    /// holds). Range deletes are rare, so the coarse clear is cheap; a
    /// cleared index simply reports subsequent manifest entries as
    /// missing until it re-populates. No-op when no index is registered
    /// or the tree's content-hash dedup master switch is off.
    /// </summary>
    private void InvalidateAppliedContentIndexForRange(in WalRecord entry, LatticeReplicationOptions resolved)
    {
        if (_appliedContentIndex is null
            || !resolved.ContentHashDedupEnabled
            || string.IsNullOrEmpty(entry.TreeId))
        {
            return;
        }

        _appliedContentIndex.InvalidateTree(entry.TreeId!);
    }

    private async Task ParkAsync(
        WalRecord entry,
        LatticeReplicationOptions resolved,
        CancellationToken cancellationToken)
    {
        var buffer = _buffers.GetOrAdd(entry.TreeId, static treeId => new CausalApplyBuffer(treeId));
        var outcome = buffer.TryAdd(
            entry,
            resolved.CausalBufferMaxEntries,
            resolved.CausalBufferMaxBytes,
            out var evicted);

        if (outcome == AddOutcome.AddedWithEviction && evicted.Count > 0)
        {
            var dlq = grainFactory.GetGrain<IReplicationDeadLetterGrain>(entry.TreeId);
            foreach (var displaced in evicted)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await dlq.EnqueueAsync(
                    displaced,
                    failureReason: "Causal-apply buffer full; evicted blocked entry to make room.",
                    retryCount: 0,
                    reasonTag: LatticeReplicationMetrics.ReasonHlcSkew,
                    cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task DrainBufferAsync(
        string treeId,
        IReplicationHighWaterMarkGrain hwmGrain,
        LatticeReplicationOptions resolved,
        CancellationToken cancellationToken)
    {
        if (!_buffers.TryGetValue(treeId, out var buffer) || buffer.Count == 0)
        {
            return;
        }

        // Iterate to a fixed point: each drained apply may advance the
        // local vector clock and unblock further entries on the next
        // pass. Bounded by the buffer's current size + any retried
        // entries that fail and re-park; in practice convergence is
        // O(n) where n is the gap between received and applied.
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var localVc = await hwmGrain.GetVectorAsync(cancellationToken);
            var ready = buffer.DrainSatisfied(localVc, resolved.ClusterId);
            if (ready.Count == 0)
            {
                return;
            }

            foreach (var ent in ready)
            {
                cancellationToken.ThrowIfCancellationRequested();
                try
                {
                    await ApplyPointAsync(ent);
                    RecordApplyLag(ent);
                    RecordFifoState(ent);
                    RecordAppliedContentForIndex(in ent, resolved);
                    await hwmGrain
                        .TryAdvanceAsync(ent.OriginClusterId!, ent.Timestamp, cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    // A drained apply has no transport-level retry
                    // path: the original delivery was already ack'd
                    // when ApplyAsync returned for the entry that
                    // unblocked this one. Route the failed drained
                    // entry to the dead-letter queue rather than
                    // dropping it silently. ArgumentException /
                    // InvalidOperationException are schema-shaped
                    // faults; everything else is unknown.
                    var reasonTag = ex is ArgumentException or InvalidOperationException
                        ? LatticeReplicationMetrics.ReasonSchema
                        : LatticeReplicationMetrics.ReasonUnknown;
                    var dlq = grainFactory.GetGrain<IReplicationDeadLetterGrain>(ent.TreeId);
                    await dlq.EnqueueAsync(
                        ent,
                        failureReason: ex.Message ?? "<no message>",
                        retryCount: 0,
                        reasonTag: reasonTag,
                        cancellationToken).ConfigureAwait(false);

                    // Roll back the shadow-forward cache reservation
                    // that was made when ApplyAsync originally parked
                    // this entry. Without rollback, an operator-driven
                    // retry from the DLQ would observe TryAdd=false on
                    // the cache and be classified as Applied=false
                    // (shadow-forward-dedup); the dead-letter
                    // decorator's "Applied=false clears the counter"
                    // contract would then silently drop the entry
                    // until FIFO eviction. The HWM was never advanced
                    // for this entry (apply threw before TryAdvance),
                    // so HWM dedupe will not suppress the retry - the
                    // cache rollback is the only step required.
                    if (_dedupeCaches.TryGetValue(ent.TreeId, out var cache))
                    {
                        cache.Remove(ent);
                    }
                }
            }
        }
    }

    private Task ApplyPointAsync(WalRecord entry)
    {
        var apply = grainFactory.GetGrain<IReplicationApplyGrain>(entry.TreeId);

        // Saga prepare-phase entries (IsPrepared==true) route through
        // the prepared-apply seam so the receiver leaf parks them in
        // its per-tx pending bucket rather than flipping the visible
        // projection. The terminal record arriving subsequently via
        // ApplyTxTerminalAsync is the per-shard linearization point
        // that flips pending into visible (or drops on abort). Both
        // plain LWW saga writes (the value-only SetManyAtomicAsync
        // surface) AND staged-CRDT writes ride this seam: a CRDT-mode
        // prepared entry carries its typed Delta + Mode through
        // ApplyPreparedSetAsync so the receiver folds the per-replica
        // delta into its current visible state on the terminal commit
        // (the per-replica union) rather than installing the prepared
        // merged-state value last-writer-wins. LWW-mode prepared entries
        // leave Delta null and stay on the byte-for-byte unchanged path.
        if (entry.IsPrepared && entry.Op is MutationKind.Set or MutationKind.Delete)
        {
            return ApplyPreparedPointAsync(apply, entry);
        }

        return entry.Op switch
        {
            // The null-Value guard applies only to LwwRegister, whose
            // Value is the canonical payload the receiver writes
            // verbatim. A CRDT-mode Set carries its contribution either
            // as a typed Delta (the steady-state incremental path - the
            // encoder strips Value when a typed Delta is present) or as
            // a full-state Value with no Delta (the bootstrap
            // committed-projection path - a snapshot compacts the WAL
            // into committed state, so the individual deltas are gone
            // and only the merged full state remains). Each
            // ApplyTypedDeltaAsync overload routes on Delta presence:
            // present -> typed-delta fold; absent -> state-based merge
            // of the full Value. A CRDT-mode entry that arrives with
            // both Value and Delta null is still a hard error and is
            // surfaced inside the dispatch.
            MutationKind.Set when entry.Value is null
                && entry.Mode == LatticeMergeMode.LwwRegister
                => throw new ArgumentException(
                    "WalRecord.Value must be non-null for MutationKind.Set on LwwRegister entries.",
                    nameof(entry)),
            MutationKind.Set => entry.Mode switch
            {
                LatticeMergeMode.LwwRegister => apply.ApplySetAsync(
                    entry.Key,
                    entry.Value!,
                    entry.Timestamp,
                    entry.OriginClusterId!,
                    sourceVectorClock: null,
                    entry.ExpiresAtTicks),
                LatticeMergeMode.OrSet => ApplyTypedDeltaAsync<OrSet>(
                    entry,
                    static (state, other) => state.MergeFrom(other)),
                LatticeMergeMode.PnCounter => ApplyTypedDeltaAsync<PnCounter>(
                    entry,
                    static (state, other) => state.MergeFrom(other)),
                LatticeMergeMode.VersionVector => ApplyTypedDeltaAsync<VersionVector>(
                    entry,
                    static (state, other) => state.MergeFrom(other)),
                LatticeMergeMode.MvRegister => ApplyTypedDeltaAsync<MvRegister>(
                    entry,
                    static (state, other) => state.MergeFrom(other)),
                LatticeMergeMode.OrMap => ApplyOrMapDeltaAsync(entry),
                LatticeMergeMode.Sequence => ApplyTypedDeltaAsync<Rga>(
                    entry,
                    static (state, other) => state.MergeFrom(other)),
                LatticeMergeMode.OrFlag => ApplyTypedDeltaAsync<OrFlag>(
                    entry,
                    static (state, other) => state.MergeFrom(other)),
                LatticeMergeMode.RwFlag => ApplyTypedDeltaAsync<RwFlag>(
                    entry,
                    static (state, other) => state.MergeFrom(other)),
                LatticeMergeMode.GCounter => ApplyTypedDeltaAsync<GCounter>(
                    entry,
                    static (state, other) => state.MergeFrom(other)),
                LatticeMergeMode.GSet => ApplyTypedDeltaAsync<GSet>(
                    entry,
                    static (state, other) => state.MergeFrom(other)),
                LatticeMergeMode.RwSet => ApplyTypedDeltaAsync<RwSet>(
                    entry,
                    static (state, other) => state.MergeFrom(other)),
                LatticeMergeMode.MaxRegister => ApplyTypedDeltaAsync<BoundedRegister>(
                    entry,
                    static (state, other) => state.MergeFrom(other)),
                LatticeMergeMode.MinRegister => ApplyTypedDeltaAsync<BoundedRegister>(
                    entry,
                    static (state, other) => state.MergeFrom(other)),
                _ => throw new InvalidOperationException(
                    $"WalRecord on tree '{entry.TreeId}' carries unrecognised replication mode '{entry.Mode}' "
                    + "(value="
                    + ((int)entry.Mode).ToString(System.Globalization.CultureInfo.InvariantCulture)
                    + "). The receiver has no apply rule registered for this mode; in a future release such "
                    + "entries will be routed to a dead-letter queue."),
            },
            MutationKind.Delete => apply.ApplyDeleteAsync(
                entry.Key,
                entry.Timestamp,
                entry.OriginClusterId!,
                sourceVectorClock: null),
            _ => throw new InvalidOperationException(
                $"Unsupported point-apply op {entry.Op} for entry on tree '{entry.TreeId}'."),
        };
    }

    /// <summary>
    /// Routes an inbound saga prepare-phase entry through the
    /// receiver-side <see cref="IReplicationApplyGrain.ApplyPreparedSetAsync"/>
    /// or <see cref="IReplicationApplyGrain.ApplyPreparedDeleteAsync"/>
    /// seam. The seam wraps the per-key write in the same ambient
    /// context stack the source saga's prepare step would have produced
    /// (<see cref="LatticePreparedContext"/>, 
    /// <see cref="LatticeOriginContext"/>, <see cref="LatticeVectorClockContext"/>, 
    /// <see cref="LatticeHlcOverrideContext"/>, 
    /// <see cref="LatticeAtomicBatchContext"/>, and the request-scope
    /// transaction id) so the receiver leaf routes the entry into its
    /// per-tx pending bucket instead of the visible projection.
    /// </summary>
    private static Task ApplyPreparedPointAsync(IReplicationApplyGrain apply, WalRecord entry)
    {
        if (entry.TransactionId == Guid.Empty)
        {
            throw new ArgumentException(
                $"WalRecord on tree '{entry.TreeId}' carries IsPrepared=true but TransactionId=Guid.Empty; "
                + "saga prepare-phase entries must stamp the saga's transaction id so the receiver can "
                + "route the entry into the correct per-leaf pending-tx bucket.",
                nameof(entry));
        }

        if (entry.Op == MutationKind.Set)
        {
            if (entry.Value is null)
            {
                throw new ArgumentException(
                    "WalRecord.Value must be non-null for prepared MutationKind.Set.",
                    nameof(entry));
            }

            return apply.ApplyPreparedSetAsync(
                entry.Key,
                entry.Value,
                entry.Timestamp,
                entry.OriginClusterId!,
                sourceVectorClock: null,
                entry.ExpiresAtTicks,
                entry.TransactionId,
                entry.AtomicBatchSize,
                entry.AtomicBatchIndex,
                // Carry the typed CRDT delta + merge mode so the receiver
                // folds the per-replica delta into its current visible state
                // on the saga's terminal commit instead of installing the
                // prepared LWW value verbatim. A null delta / LwwRegister mode
                // (the common case) keeps the byte-for-byte unchanged LWW path.
                entry.Delta,
                entry.Mode);
        }

        return apply.ApplyPreparedDeleteAsync(
            entry.Key,
            entry.Timestamp,
            entry.OriginClusterId!,
            sourceVectorClock: null,
            entry.TransactionId,
            entry.AtomicBatchSize,
            entry.AtomicBatchIndex);
    }

    /// <summary>
    /// Routes an inbound saga terminal-mark record (TxCommit / TxAbort)
    /// through the receiver-side
    /// <see cref="IReplicationApplyGrain.ApplyTxTerminalAsync"/> seam.
    /// Resolves the source-side shard index from the typed
    /// <see cref="WalRecord.ShardIndex"/> slot, falling back to parsing
    /// <see cref="WalRecord.Key"/> for back-compat with pre-Option A WAL
    /// records authored before the typed slot was introduced.
    /// </summary>
    private Task ApplyTxTerminalCoreAsync(WalRecord entry, CancellationToken cancellationToken)
    {
        var apply = grainFactory.GetGrain<IReplicationApplyGrain>(entry.TreeId);
        var committed = entry.Op == MutationKind.TxCommit;

        int shardIndex;
        if (entry.ShardIndex > 0)
        {
            shardIndex = entry.ShardIndex;
        }
        else if (!string.IsNullOrEmpty(entry.Key)
            && int.TryParse(entry.Key, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var parsed)
            && parsed >= 0)
        {
            shardIndex = parsed;
        }
        else
        {
            throw new ArgumentException(
                $"WalRecord.Op={entry.Op} on tree '{entry.TreeId}' must carry a positive ShardIndex slot or a "
                + $"non-negative numeric Key (got Key='{entry.Key}', ShardIndex={entry.ShardIndex}).",
                nameof(entry));
        }

        if (entry.TransactionId == Guid.Empty)
        {
            throw new ArgumentException(
                $"WalRecord.Op={entry.Op} on tree '{entry.TreeId}' must carry a non-empty TransactionId so the "
                + "receiver can address the saga's per-tree TxRegistry mark.",
                nameof(entry));
        }

        // Cross-tree atomic write: scope the receiver barrier's wait set to the
        // participant trees that are actually replicated on THIS receiver. A
        // participant tree not replicated here is excluded, so the barrier
        // completes on the present subset (partial-replication batches are
        // valid). The current tree always received this terminal, so it is
        // replicated here and is always in the wait set.
        var crossTreeOperationId = string.IsNullOrEmpty(entry.CrossTreeOperationId)
            ? null
            : entry.CrossTreeOperationId;
        IReadOnlyList<string>? crossTreeWaitSet = null;
        if (crossTreeOperationId is not null)
        {
            var waitSet = new List<string> { entry.TreeId };
            if (entry.CrossTreeParticipants is { Count: > 0 } participants)
            {
                foreach (var participant in participants)
                {
                    if (string.Equals(participant, entry.TreeId, StringComparison.Ordinal)) continue;
                    if (IsTreeReplicatedHere(participant)) waitSet.Add(participant);
                }
            }
            crossTreeWaitSet = waitSet;
        }

        return apply.ApplyTxTerminalAsync(
            entry.TransactionId,
            committed,
            shardIndex,
            entry.Timestamp,
            entry.OriginClusterId!,
            entry.AtomicShardCount,
            crossTreeOperationId,
            crossTreeWaitSet,
            cancellationToken);
    }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="treeId"/> is opted into
    /// replication on this receiver. The canonical signal is the injected
    /// <see cref="ILatticeReplicationContext"/> (a non-null
    /// <see cref="ILatticeReplicationContext.ResolveMergeMode"/> result): the
    /// production context reports a merge mode for exactly the trees that are
    /// replicated here - delegating to the same per-tree resolver the shipper
    /// and change feed consult - so this agrees with the data plane, while a
    /// host that opts trees in through a custom resolver is honoured too.
    /// Falls back to the raw <see cref="LatticeReplicationOptions.ReplicatedTrees"/>
    /// map when no context was injected. Used to scope a cross-tree atomic
    /// write's receiver barrier to the participant trees present on this
    /// cluster.
    /// </summary>
    private bool IsTreeReplicatedHere(string treeId) =>
        _replicationContext?.ResolveMergeMode(treeId) is not null
        || options.Get(treeId).ReplicatedTrees?.ContainsKey(treeId) == true;

    /// <summary>
    /// Receiver-side inbound admission outcomes for the enrollment / merge-mode
    /// gate (issue #1267). The gate is the sole tree-scope authorization the
    /// data-apply path enforces beyond the core <c>ThrowIfSystemTree</c>
    /// reserved-prefix check, which does not cover the <c>sys-</c>-prefixed
    /// authorization / identity trees a cluster may deliberately keep
    /// cluster-local by not enrolling them.
    /// </summary>
    private enum InboundTreeAdmission
    {
        /// <summary>The entry's tree is enrolled here and its wire mode matches the local mode; apply it.</summary>
        Admit,

        /// <summary>The entry's tree is not enrolled for replication on this receiver; drop it.</summary>
        RejectNotReplicated,

        /// <summary>The entry's tree is enrolled but its peer-supplied wire mode disagrees with the local mode; dead-letter it.</summary>
        RejectModeMismatch,

        /// <summary>
        /// The receiver has no replication enrollment source wired at all (neither an
        /// injected <see cref="ILatticeReplicationContext"/> nor a
        /// <see cref="LatticeReplicationOptions.ReplicatedTrees"/> map), so the enrollment
        /// gate cannot be evaluated. Fail closed and drop the entry (like
        /// <see cref="RejectNotReplicated"/>, no dead-letter - the tree id is
        /// peer-controlled). Distinguished from <see cref="RejectNotReplicated"/> only so
        /// the drop can carry a one-time misconfiguration warning.
        /// </summary>
        RejectNoEnrollmentSource,
    }

    /// <summary>
    /// Classifies an inbound entry against the receiver's per-tree replication
    /// enrollment and merge-mode configuration. The check is the receiver-side
    /// hardening for issue #1267: a peer holding the mesh secret must not be
    /// able to write a tree this cluster kept cluster-local by not enrolling it
    /// (the peer-supplied <see cref="WalRecord.OriginClusterId"/> is
    /// unverified), nor override the merge algebra by supplying a different
    /// wire <see cref="WalRecord.Mode"/>.
    /// <para>
    /// The gate fails closed on ambiguity (issue #1398). A hand-built applier
    /// with neither an injected <see cref="ILatticeReplicationContext"/> nor a
    /// per-tree <see cref="LatticeReplicationOptions.ReplicatedTrees"/> map has
    /// no enrollment signal, so the gate cannot be evaluated and the entry is
    /// dropped (<see cref="InboundTreeAdmission.RejectNoEnrollmentSource"/>)
    /// rather than admitted. Production always registers
    /// <c>ConfiguredLatticeReplicationContext</c>, so <c>hasEnrollmentSource</c>
    /// is always <see langword="true"/> there and this arm is unreachable; a
    /// tree enabled at runtime via replication control resolves through the same
    /// context (its resolver reports the live mode) and is admitted on the
    /// normal enrolled path. The lookup is a single cached dictionary read (no
    /// per-entry allocation): <see cref="ILatticeReplicationContext.ResolveMergeMode"/>
    /// is cache-backed and <see cref="IOptionsMonitor{TOptions}.Get"/> returns
    /// a cached options instance.
    /// </para>
    /// </summary>
    private InboundTreeAdmission ClassifyInboundTree(in WalRecord entry)
    {
        var localMode = ResolveLocalMergeMode(entry.TreeId, out var hasEnrollmentSource);
        if (!hasEnrollmentSource)
        {
            return InboundTreeAdmission.RejectNoEnrollmentSource;
        }

        if (localMode is null)
        {
            return InboundTreeAdmission.RejectNotReplicated;
        }

        return entry.Mode == localMode.Value
            ? InboundTreeAdmission.Admit
            : InboundTreeAdmission.RejectModeMismatch;
    }

    /// <summary>
    /// Resolves the merge mode this receiver applies to <paramref name="treeId"/>,
    /// preferring the injected <see cref="ILatticeReplicationContext"/> (the
    /// same per-tree resolver the shipper, change feed, and bootstrap path
    /// consult) and falling back to the raw
    /// <see cref="LatticeReplicationOptions.ReplicatedTrees"/> map when no
    /// context was injected. <paramref name="hasEnrollmentSource"/> reports
    /// whether any enrollment configuration was available at all: when both the
    /// context and the map are absent the applier cannot enforce the gate and
    /// the caller fails closed, dropping the entry (issue #1398). Returns
    /// <c>null</c> for a tree that has an enrollment source but is not enrolled.
    /// </summary>
    private LatticeMergeMode? ResolveLocalMergeMode(string treeId, out bool hasEnrollmentSource)
    {
        if (_replicationContext is not null)
        {
            hasEnrollmentSource = true;
            return _replicationContext.ResolveMergeMode(treeId);
        }

        var trees = options.Get(treeId).ReplicatedTrees;
        if (trees is not null)
        {
            hasEnrollmentSource = true;
            return trees.TryGetValue(treeId, out var mode) ? mode : null;
        }

        hasEnrollmentSource = false;
        return null;
    }

    /// <summary>
    /// Dead-letters an inbound entry the receiver-side merge-mode gate rejected
    /// because its peer-supplied <see cref="WalRecord.Mode"/> disagreed with
    /// the locally resolved mode for its tree. Only reached on the rejection
    /// path (an enrolled tree shipped a mismatched mode), so the interpolated
    /// diagnostic string is not on any steady-state hot path. The tree is
    /// enrolled and therefore bounded, so parking the entry cannot be used to
    /// spawn unbounded dead-letter-queue activations.
    /// </summary>
    private Task DeadLetterModeMismatchAsync(
        WalRecord entry,
        LatticeMergeMode expected,
        CancellationToken cancellationToken)
    {
        var dlq = grainFactory.GetGrain<IReplicationDeadLetterGrain>(entry.TreeId);
        return dlq.EnqueueAsync(
            entry,
            failureReason: $"Inbound replication entry for tree '{entry.TreeId}' declared merge mode "
                + $"'{entry.Mode}' but the receiver resolves this tree to '{expected}'; the entry was "
                + "rejected so a peer cannot override the local merge algebra via the wire mode field.",
            retryCount: 0,
            reasonTag: LatticeReplicationMetrics.ReasonModeMismatch,
            cancellationToken);
    }

    /// <summary>
    /// Dead-letters an inbound entry the receiver-side tenant-isolation gate
    /// (issue #1633) refused because its tree id names a non-existent tenant, or a
    /// tenant not resident in this serving region. Only reached on the rejection
    /// path, so the interpolated diagnostic string is not on any steady-state hot
    /// path. The tree is enrolled and therefore bounded, so parking the entry cannot
    /// be used to spawn unbounded dead-letter-queue activations. The owning tenant is
    /// derived from the tree id alone, never from a wire-supplied field.
    /// </summary>
    private Task DeadLetterTenantIsolationAsync(
        WalRecord entry,
        ReplicationTenantIsolationDecision decision,
        CancellationToken cancellationToken)
    {
        var (reasonTag, failureReason) = decision == ReplicationTenantIsolationDecision.RejectOutOfRegion
            ? (LatticeReplicationMetrics.ReasonTenantOffline,
                $"Inbound replicated write for tree '{entry.TreeId}' targets a tenant that is not "
                + "resident in the region serving this receiver; the write was refused so it cannot "
                + "land in a region outside the tenant's residency set.")
            : (LatticeReplicationMetrics.ReasonForeignTenant,
                $"Inbound replicated write for tree '{entry.TreeId}' targets a tenant that does not "
                + "exist on this receiver; the write was refused so a peer cannot create or smuggle "
                + "into a foreign or non-existent tenant.");

        var dlq = grainFactory.GetGrain<IReplicationDeadLetterGrain>(entry.TreeId);
        return dlq.EnqueueAsync(
            entry,
            failureReason: failureReason,
            retryCount: 0,
            reasonTag: reasonTag,
            cancellationToken);
    }


    /// <summary>
    /// CAS retry budget for the read-merge-write loop used by typed CRDT
    /// delta applies (<see cref="LatticeMergeMode.OrSet"/>,
    /// <see cref="LatticeMergeMode.PnCounter"/>, <see cref="LatticeMergeMode.VersionVector"/>,
    /// <see cref="LatticeMergeMode.MvRegister"/>).
    /// Mirrors the budget the typed accessors (<see cref="OrSetAccessor.DefaultMaxAttempts"/>,
    /// <see cref="PnCounterAccessor.DefaultMaxAttempts"/>,
    /// <see cref="VersionVectorAccessor.DefaultMaxAttempts"/>,
    /// <see cref="MvRegisterAccessor{T}.DefaultMaxAttempts"/>) use for the
    /// authoring side, so a typical fan-in matches.
    /// </summary>
    private const int StateMergeMaxAttempts = 16;

    /// <summary>
    /// Routes an inbound CRDT-mode <see cref="MutationKind.Set"/> entry
    /// through the appropriate receive path based on whether the entry
    /// carries a typed <see cref="WalRecord.Delta"/>.
    /// <para>
    /// <b>Steady-state incremental path (Delta present).</b> The producer
    /// authored a typed delta into <see cref="WalRecord.Delta"/>. The
    /// receiver forwards the delta verbatim through the same
    /// <see cref="IReplicationApplyGrain.ApplyCrdtDeltaManyAsync"/> grain
    /// seam the batch path uses (<see cref="ApplyCrdtDeltaThroughGrainAsync"/>),
    /// so the merged write is recorded as a <c>CrdtDelta</c> with member
    /// changes plus the source <see cref="WalRecord.OriginClusterId"/> -
    /// identical history fidelity to a locally-authored CRDT write -
    /// rather than the full-value <see cref="MutationKind.Set"/> the old
    /// read-merge-write fold produced. The state-merge
    /// <paramref name="mergeState"/> is unused on this path.
    /// </para>
    /// <para>
    /// <b>Bootstrap committed-projection path (Delta absent).</b> A
    /// bootstrap snapshot compacts the sender's WAL into committed state,
    /// so the per-delta causal records the steady-state path relies on
    /// are gone; each committed row carries the full CRDT state in
    /// <see cref="WalRecord.Value"/> with <see cref="WalRecord.Delta"/>
    /// null. The receiver deserialises <c>Value</c> as the full state and
    /// folds it into its existing state via the state-based CRDT merge
    /// <paramref name="mergeState"/> (a commutative, associative,
    /// idempotent join that preserves both the receiver's concurrent
    /// local contributions and the sender's observed-remove history). See
    /// <see cref="ApplyFullStateMergeAsync{TState}"/>. Bootstrap rows have
    /// no per-delta shape, so they stay full-state <c>Set</c>.
    /// </para>
    /// </summary>
    private Task ApplyTypedDeltaAsync<TState>(
        WalRecord entry,
        Action<TState, TState> mergeState)
        where TState : class
    {
        if (entry.Delta is null)
        {
            return ApplyFullStateMergeAsync(entry, mergeState);
        }

        return ApplyCrdtDeltaThroughGrainAsync(entry);
    }

    /// <summary>
    /// Forwards a steady-state delta-carrying CRDT entry through the public
    /// <see cref="ILattice.ApplyCrdtDeltaAsync"/> seam - the same path a
    /// locally-authored CRDT write takes - so the receiver records a
    /// <c>CrdtDelta</c> revision (member changes + origin) instead of
    /// flattening the merge to a full-value <see cref="MutationKind.Set"/>.
    /// The remote origin is stamped via an ambient
    /// <see cref="LatticeOriginContext"/> scope so the receiver's commit-time
    /// observer publishes the foreign origin and the producer's ship loop
    /// filters the entry back out; the merge point gets a fresh local HLC,
    /// matching the legacy read-merge-write semantics, so an older-clocked
    /// remote delta still folds (CRDT joins are commutative) rather than
    /// being dropped by a stamp gate. Allocations are one origin-scope token;
    /// this per-entry path is the off-batch fallback so the steady-state hot
    /// path stays on the folded batch seam.
    /// </summary>
    private async Task ApplyCrdtDeltaThroughGrainAsync(WalRecord entry)
    {
        var apply = grainFactory.GetGrain<IReplicationApplyGrain>(entry.TreeId);
        using var scope = LatticeOriginContext.With(entry.OriginClusterId);
        // Carry the record's absolute per-entry expiry so a TTL'd CRDT write
        // expires on this replica too. The expiry rides the same origin-scoped,
        // receiver-advances-clock semantics as the non-expiry per-entry path;
        // it is applied verbatim (never re-resolved from a relative TTL) under
        // the max-absolute-ticks join. Durable entries carry ExpiresAtTicks == 0.
        await apply.ApplyCrdtDeltaWithExpiryAsync(entry.Key, entry.Mode, entry.Delta!, entry.ExpiresAtTicks).ConfigureAwait(true);
    }

    /// <summary>
    /// Applies a bootstrap committed-projection CRDT row that carries the
    /// full CRDT state in <see cref="WalRecord.Value"/> and no typed
    /// <see cref="WalRecord.Delta"/>. The full state is deserialised once
    /// and folded into the receiver's existing state via the state-based
    /// CRDT merge <paramref name="mergeState"/> under the same optimistic-
    /// concurrency CAS loop as the typed-delta path. When the key is
    /// absent on the receiver - the common case after a fall-off-WAL
    /// resync wiped the projection - the incoming <c>Value</c> bytes are
    /// installed verbatim (they are already the canonical serialisation of
    /// the full state), avoiding a needless deserialise/merge/serialise
    /// round-trip. A CRDT-mode <see cref="MutationKind.Set"/> with both
    /// <c>Delta</c> and <c>Value</c> null is malformed and faults.
    /// </summary>
    private async Task ApplyFullStateMergeAsync<TState>(
        WalRecord entry,
        Action<TState, TState> mergeState)
        where TState : class
    {
        if (entry.Value is null)
        {
            throw new ArgumentException(
                $"WalRecord for {entry.Mode} apply on tree '{entry.TreeId}', key '{entry.Key}' carries "
                + "neither a typed Delta nor a full-state Value. A CRDT-mode Set must carry a typed Delta "
                + "(steady-state incremental path) or a full-state Value (bootstrap committed-projection "
                + "path); an entry with both absent is malformed.",
                nameof(entry));
        }

        var lattice = grainFactory.GetGrain<ILattice>(entry.TreeId);
        var stateSerializer = JsonLatticeSerializer<TState>.Default;
        var incoming = stateSerializer.Deserialize(entry.Value);

        using var scope = LatticeOriginContext.With(entry.OriginClusterId);

        for (var attempt = 0; attempt < StateMergeMaxAttempts; attempt++)
        {
            var versioned = await lattice.GetWithVersionAsync(entry.Key);
            if (versioned.Value is null)
            {
                var installed = await lattice.SetIfVersionAsync(entry.Key, entry.Value, versioned.Version);
                if (installed)
                {
                    return;
                }

                continue;
            }

            var existing = stateSerializer.Deserialize(versioned.Value);
            mergeState(existing, incoming);
            var bytes = stateSerializer.Serialize(existing);
            var ok = await lattice.SetIfVersionAsync(entry.Key, bytes, versioned.Version);
            if (ok)
            {
                return;
            }
        }

        throw new InvalidOperationException(
            $"Replication full-state-merge CAS budget exhausted after {StateMergeMaxAttempts} attempts on "
            + $"tree '{entry.TreeId}', key '{entry.Key}', mode '{entry.Mode}'. The receiver could not install "
            + "the merged state under optimistic concurrency; reduce contention on this key or increase the "
            + "budget in a future configuration knob.");
    }

    /// <summary>
    /// Routes an inbound <see cref="LatticeMergeMode.OrMap"/> entry through
    /// the registered <see cref="CrdtShape"/> for the entry's
    /// tree. The wire shape is generic over <c>(TKey, TValue)</c>, so the
    /// receiver cannot statically pick a deserialiser; the host registers
    /// the concrete pair via
    /// <see cref="LatticeServiceCollectionExtensions.AddOrMapShape{TKey, TValue}(ISiloBuilder, string)"/>
    /// before silo start, and this method looks the descriptor up by tree
    /// id and delegates to it. A tree configured for
    /// <see cref="LatticeMergeMode.OrMap"/> with no registered shape faults
    /// the apply so the misconfiguration is surfaced rather than silently
    /// dropping the entry.
    /// </summary>
    private async Task ApplyOrMapDeltaAsync(WalRecord entry)
    {
        var shape = crdtShapes?.TryGet(entry.TreeId, LatticeMergeMode.OrMap)
            ?? throw new InvalidOperationException(
                $"Tree '{entry.TreeId}' is configured for LatticeMergeMode.OrMap but no "
                + "CrdtShape is registered with the receiver. Call "
                + "siloBuilder.AddOrMapShape<TKey, TValue>(\"" + entry.TreeId + "\") on the "
                + "service collection before silo start so the receiver can deserialise the generic delta.");

        // Bootstrap committed-projection rows carry the full OrMap state in
        // Value with no typed Delta. Fold the full state into the
        // receiver's existing state via the shape's state-based merge,
        // mirroring the typed-delta path's CAS loop. The steady-state
        // incremental path (Delta present) is unchanged.
        if (entry.Delta is null)
        {
            await ApplyOrMapFullStateMergeAsync(entry, shape).ConfigureAwait(true);
            return;
        }

        // Steady-state delta-present OrMap entry: forward the delta verbatim
        // through the CrdtDelta grain seam so the receiver records member
        // changes + origin, identical to the typed-delta modes and to a
        // locally-authored OrMap write. The shape lookup above still runs so
        // an OrMap tree with no registered (TKey, TValue) shape faults here
        // rather than silently deferring the misconfiguration to the grain.
        await ApplyCrdtDeltaThroughGrainAsync(entry).ConfigureAwait(true);
    }

    /// <summary>
    /// Applies a bootstrap committed-projection <see cref="LatticeMergeMode.OrMap"/>
    /// row whose <see cref="WalRecord.Value"/> carries the full OrMap
    /// state and whose <see cref="WalRecord.Delta"/> is null. Mirrors
    /// <see cref="ApplyFullStateMergeAsync{TState}"/> but goes through the
    /// registered generic <see cref="CrdtShape"/> because the receiver
    /// cannot statically pick a (TKey, TValue) deserialiser.
    /// </summary>
    private async Task ApplyOrMapFullStateMergeAsync(WalRecord entry, CrdtShape shape)
    {
        if (entry.Value is null)
        {
            throw new ArgumentException(
                $"WalRecord for OrMap apply on tree '{entry.TreeId}', key '{entry.Key}' carries neither a "
                + "typed Delta nor a full-state Value. A CRDT-mode Set must carry a typed Delta (steady-state "
                + "incremental path) or a full-state Value (bootstrap committed-projection path); an entry "
                + "with both absent is malformed.",
                nameof(entry));
        }

        var lattice = grainFactory.GetGrain<ILattice>(entry.TreeId);
        var incoming = shape.DeserializeState(entry.Value);

        using var scope = LatticeOriginContext.With(entry.OriginClusterId);

        for (var attempt = 0; attempt < StateMergeMaxAttempts; attempt++)
        {
            var versioned = await lattice.GetWithVersionAsync(entry.Key);
            if (versioned.Value is null)
            {
                var installed = await lattice.SetIfVersionAsync(entry.Key, entry.Value, versioned.Version);
                if (installed)
                {
                    return;
                }

                continue;
            }

            var existing = shape.DeserializeState(versioned.Value);
            shape.MergeStates(existing, incoming);
            var bytes = shape.SerializeState(existing);
            var ok = await lattice.SetIfVersionAsync(entry.Key, bytes, versioned.Version);
            if (ok)
            {
                return;
            }
        }

        throw new InvalidOperationException(
            $"Replication OrMap full-state-merge CAS budget exhausted after {StateMergeMaxAttempts} attempts "
            + $"on tree '{entry.TreeId}', key '{entry.Key}'. The receiver could not install the merged state "
            + "under optimistic concurrency; reduce contention on this key or increase the budget in a future "
            + "configuration knob.");
    }

    private Task ApplyRangeAsync(WalRecord entry, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (entry.EndExclusiveKey is null)
        {
            throw new ArgumentException(
                "WalRecord.EndExclusiveKey must be non-null for MutationKind.DeleteRange.",
                nameof(entry));
        }

        var apply = grainFactory.GetGrain<IReplicationApplyGrain>(entry.TreeId);
        // Pass entry.Timestamp through verbatim: producers stamp the
        // range-delete issue HLC into this slot via
        // ShardRootGrain.PublishDeleteRangeAsync; receivers pin every
        // per-leaf tombstone to this HLC on the apply seam so the
        // cross-origin LWW invariant is preserved. Legacy entries
        // persisted before this invariant was enforced carry
        // HybridLogicalClock.Zero; the apply seam detects the sentinel
        // and falls back to fresh-local-HLC stamping for back-compat.
        return apply.ApplyDeleteRangeAsync(
            entry.Key,
            entry.EndExclusiveKey,
            entry.Timestamp,
            entry.OriginClusterId!,
            sourceVectorClock: null,
            // A predicate-filtered range delete ships the explicit matched key
            // set; the receiver tombstones exactly those keys rather than
            // re-deriving membership from the range bounds. Null for an
            // unconditional range delete.
            explicitMatchedKeys: entry.MatchedKeys);
    }

    private IReplicationHighWaterMarkGrain GetHwmGrain(string treeId) =>
        grainFactory.GetGrain<IReplicationHighWaterMarkGrain>(treeId);

    /// <summary>
    /// Records the receiver-side replication-lag sample for a successfully
    /// applied point operation. Lag is computed as
    /// <c>now - entry.Timestamp.WallClockTicks</c> in milliseconds and
    /// clamped at zero - a future-dated source HLC (the producing cluster's
    /// wall clock leads the receiver's) reports as <c>0</c> rather than a
    /// negative sample, which would corrupt downstream histograms.
    /// </summary>
    private static void RecordApplyLag(WalRecord entry)
    {
        var sourceTicks = entry.Timestamp.WallClockTicks;
        if (sourceTicks <= 0)
        {
            // Source HLC was never stamped (HybridLogicalClock.Zero or a
            // pathological negative). Reporting "now - 0" would publish
            // a garbage multi-decade lag value; skip the sample instead.
            return;
        }

        var deltaTicks = DateTime.UtcNow.Ticks - sourceTicks;
        if (deltaTicks < 0)
        {
            deltaTicks = 0;
        }

        var ms = deltaTicks / (double)TimeSpan.TicksPerMillisecond;
        LatticeReplicationMetrics.ApplyLag.Record(
            ms,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, entry.TreeId),
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagPeer, entry.OriginClusterId ?? string.Empty),
            LatticeTenantLabel.ForTree(entry.TreeId));
    }

    /// <summary>
    /// Updates the per-<c>(treeId, originClusterId)</c> last-applied
    /// source-HLC tracker for a successfully applied point operation
    /// and increments
    /// <see cref="LatticeReplicationMetrics.ApplyFifoViolations"/> when
    /// the entry's HLC is strictly less than the previously recorded
    /// value - surfacing a transport-side regression that broke the
    /// per-origin FIFO invariant. The recorded value is the pointwise
    /// max so a benign re-delivery (which the HWM check already filters
    /// upstream) does not silently downgrade the tracker on the rare
    /// path where it slipped through.
    /// </summary>
    private void RecordFifoState(WalRecord entry)
    {
        // Skip range deletes (carry HLC.Zero) and entries with a missing
        // origin (defensive - the caller-side guard rejects empty origins
        // before we get here).
        if (entry.Op == MutationKind.DeleteRange || string.IsNullOrEmpty(entry.OriginClusterId))
        {
            return;
        }

        var key = (entry.TreeId, entry.OriginClusterId!);
        var ts = entry.Timestamp;

        // Allocation-free CAS loop: avoids the closure that an
        // AddOrUpdate factory would capture per call. Under steady-state
        // monotonic delivery the loop runs exactly once.
        while (true)
        {
            if (!_lastAppliedSourceHlc.TryGetValue(key, out var existing))
            {
                if (_lastAppliedSourceHlc.TryAdd(key, ts))
                {
                    return;
                }
                continue;
            }

            if (ts < existing)
            {
                // Strictly out-of-order: surface the regression and
                // keep the existing (higher) recorded HLC so a future
                // monotonic delivery is compared against the true
                // pointwise max.
                LatticeReplicationMetrics.ApplyFifoViolations.Add(
                    1,
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, entry.TreeId),
                    new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagOrigin, entry.OriginClusterId!),
                    LatticeTenantLabel.ForTree(entry.TreeId));
                return;
            }

            if (ts == existing || _lastAppliedSourceHlc.TryUpdate(key, ts, existing))
            {
                return;
            }
        }
    }
}
