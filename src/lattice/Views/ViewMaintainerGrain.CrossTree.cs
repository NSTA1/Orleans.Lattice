using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Views;

/// <summary>
/// Cross-tree atomic-visibility path of the view maintainer. When a source-side
/// cross-tree atomic write commits across multiple source trees, the views
/// derived from those source trees must flip jointly, all-or-nothing: a reader
/// must never observe one participating view's slice committed while another
/// participating view's slice is still pre-commit. This is the view-layer
/// analogue of the receiver-side cross-tree barrier.
/// <para>
/// <b>Interception.</b> When a staged atomic batch completes (terminals +
/// prepares) and its terminal carried a non-null
/// <see cref="LatticeMutation.CrossTreeOperationId"/>, the maintainer does
/// <b>not</b> flip its slice into its own view tree immediately. Instead it
/// computes its slice (the same coalesced upserts / retractions the single-tree
/// path produces), resolves its own active view tree id locally, and registers
/// its readiness with the view-side coordinator
/// (<see cref="IViewCrossTreeCoordinatorGrain"/>) keyed by the cross-tree
/// operation id. The coordinator freezes the view wait set on the first
/// registration - the participant <i>source</i> trees that actually have a
/// configured view (<c>participants ∩ present</c>) - and, once every present
/// participant view has registered, issues one cross-tree atomic write across the
/// participant view trees so readers observe them flip together.
/// </para>
/// <para>
/// <b>Liveness / degrade.</b> While the joint decision is pending the staged
/// batch is kept (holding the checkpoint back, exactly as the single-tree staging
/// path does), and the maintainer re-registers on each drain. If the joint flip
/// has not committed within
/// <see cref="LatticeViewOptions.CrossTreeReadinessTimeout"/> the maintainer
/// degrades to per-tree-slice atomicity: it notifies the coordinator (which
/// terminally records the degrade so it never issues a late joint flip that could
/// clobber a degraded participant's local flip - unless the joint flip already
/// committed, in which case the late-degrading maintainer applies that result
/// instead), flips its own slice atomically into its own view tree, emits the
/// <see cref="LatticeMetrics.ViewCrossTreeJointViolation"/> metric, and schedules
/// a reconcile - choosing liveness over an indefinite WAL-pinning stall when a
/// participant view is permanently unavailable.
/// </para>
/// <para>
/// <b>Deletes.</b> A slice's retraction deletes ride <b>inside</b> the joint
/// cross-tree flip alongside its upserts (each delete staged as a tombstone
/// in the same mixed atomic op), so a re-key projection's old-key delete and
/// new-key upsert flip as a single visibility change - no reader observes the
/// old and new view keys simultaneously. The degenerate single-view and the
/// degrade-to-local paths flip the same mixed upsert+delete slice through the
/// view tree's single-tree mixed atomic primitive.
/// </para>
/// </summary>
internal sealed partial class ViewMaintainerGrain
{
    private static readonly Counter<long> ViewCrossTreeJointViolation = LatticeMetrics.ViewCrossTreeJointViolation;

    // Set when a cross-tree batch degrades to per-tree-slice atomicity this pass;
    // the drain schedules a single reconcile at the end of the pass rather than
    // recursing into a rebuild mid-flush (which would clear the staging buffer
    // under the flush loop).
    private bool _pendingCrossTreeReconcile;

    /// <summary>
    /// The configured bounded interval a cross-tree batch waits for every other
    /// participant view to become ready before degrading to per-tree-slice
    /// atomicity, falling back to the default when unset.
    /// </summary>
    private TimeSpan CrossTreeReadinessTimeout
    {
        get
        {
            var timeout = Options.CrossTreeReadinessTimeout;
            return timeout > TimeSpan.Zero ? timeout : LatticeViewOptions.DefaultCrossTreeReadinessTimeout;
        }
    }

    /// <summary>
    /// The view wait set for a cross-tree batch: the ordinal-sorted names of every
    /// view whose source tree is one of the batch's <paramref name="participants"/>
    /// and that has a configured view. Mirrors the receiver's
    /// <c>participants ∩ present</c> handling - a participant source tree with no
    /// view contributes nothing, and a cross-tree batch flips jointly across the
    /// subset of participant views that are present.
    /// </summary>
    private List<string> ComputeViewWaitSet(IReadOnlyList<string> participants)
    {
        var participantSet = new HashSet<string>(participants, StringComparer.Ordinal);
        var waitSet = new List<string>();
        foreach (var registration in catalog.All())
        {
            if (participantSet.Contains(registration.SourceTreeId))
            {
                waitSet.Add(registration.ViewName);
            }
        }

        waitSet.Sort(StringComparer.Ordinal);
        return waitSet;
    }

    /// <summary>
    /// Handles one completed cross-tree atomic batch: registers this view's slice
    /// with the coordinator and, once the joint flip has committed, applies this
    /// view's retraction deletes. Returns <see langword="true"/> when the batch is
    /// resolved this pass (joint flip observed, degenerate single-view flip, or
    /// degrade-on-timeout) and the caller should evict it from the staging buffer,
    /// or <see langword="false"/> while the joint decision is still pending and the
    /// batch must stay staged for a later drain.
    /// </summary>
    private async Task<bool> HandleCrossTreeBatchAsync(
        ILattice viewTree,
        Guid txId,
        StagedTransaction tx,
        List<KeyValuePair<string, byte[]>> upserts,
        List<string> deletes,
        CancellationToken cancellationToken)
    {
        var participants = tx.CrossTreeParticipants ?? [];
        var waitSet = ComputeViewWaitSet(participants);

        // Degenerate / partial-replication: only this view participates (no other
        // present participant view to rendezvous with), so a joint flip is exactly
        // a single-tree flip. Apply locally.
        if (waitSet.Count <= 1)
        {
            await FlipLocalSliceAsync(viewTree, txId, upserts, deletes, cancellationToken);
            return true;
        }

        var coordinator = grainFactory.GetGrain<IViewCrossTreeCoordinatorGrain>(tx.CrossTreeOperationId!);
        var decision = await coordinator.RegisterReadyAsync(new ViewCrossTreeReadiness
        {
            OperationId = tx.CrossTreeOperationId!,
            ViewName = ViewName,
            WaitSet = waitSet,
            ViewTreeId = ViewTreeId,
            Upserts = upserts,
            Deletes = deletes,
        });

        if (decision.Applied)
        {
            // The coordinator flipped this view's upserts AND retraction deletes
            // jointly across every participant view tree inside one mixed atomic
            // op, so nothing is left to apply out-of-band.
            return true;
        }

        if (decision.Degraded)
        {
            // Another participant already terminally degraded this operation: flip
            // our own slice locally too (the coordinator will never issue a joint
            // flip), so every participant converges on per-tree-slice atomicity.
            await DegradeLocallyAsync(viewTree, txId, tx, upserts, deletes, cancellationToken);
            return true;
        }

        // Not yet ready: record the deadline on the first wait and keep the batch
        // staged (holding the checkpoint back) for a later drain.
        if (tx.CrossTreeFirstSeenTicks == 0)
        {
            tx.CrossTreeFirstSeenTicks = DateTime.UtcNow.Ticks;
        }

        var timeout = CrossTreeReadinessTimeout;
        var waited = DateTime.UtcNow.Ticks - tx.CrossTreeFirstSeenTicks;
        if (waited <= timeout.Ticks)
        {
            return false;
        }

        // Bounded wait elapsed: a participant view is permanently unavailable.
        // Tell the coordinator we are degrading so it terminally records the
        // degrade and never issues a late joint flip that could clobber our local
        // flip. If the joint flip raced in just before our timeout, the coordinator
        // reports it committed and we apply the joint result instead.
        var degradeDecision = await coordinator.RegisterDegradedAsync(ViewName);
        if (degradeDecision.Applied)
        {
            // The joint flip raced in just before our timeout and already carried
            // this view's upserts AND retraction deletes in one mixed atomic op,
            // so nothing is left to apply out-of-band.
            return true;
        }

        await DegradeLocallyAsync(viewTree, txId, tx, upserts, deletes, cancellationToken);
        return true;
    }

    /// <summary>
    /// Degrades a cross-tree batch to per-tree-slice atomicity: flips this view's
    /// own slice atomically into its own tree, emits the joint-atomicity-violation
    /// metric, and schedules a reconcile - choosing liveness over an indefinite
    /// WAL-pinning stall when a participant view is permanently unavailable.
    /// </summary>
    private async Task DegradeLocallyAsync(
        ILattice viewTree,
        Guid txId,
        StagedTransaction tx,
        List<KeyValuePair<string, byte[]>> upserts,
        List<string> deletes,
        CancellationToken cancellationToken)
    {
        await FlipLocalSliceAsync(viewTree, txId, upserts, deletes, cancellationToken);
        ViewCrossTreeJointViolation.Add(1, ViewTag);
        _pendingCrossTreeReconcile = true;
        logger.LogWarning(
            "View '{ViewName}' degraded cross-tree operation '{OperationId}' to per-tree-slice atomicity after waiting {Timeout} for participant readiness; scheduling a reconcile.",
            ViewName, tx.CrossTreeOperationId, CrossTreeReadinessTimeout);
    }

    /// <summary>
    /// Flips a slice into this view's own tree atomically: the upserts and the
    /// retraction deletes ride a SINGLE mixed atomic op keyed by the
    /// deterministic view-saga operation id (so a replay re-attaches), so a
    /// re-key projection flips the upsert at the new view key and the delete at
    /// the old view key as one visibility change. The same shape the
    /// single-tree atomic flush uses.
    /// </summary>
    private static async Task FlipLocalSliceAsync(
        ILattice viewTree,
        Guid txId,
        List<KeyValuePair<string, byte[]>> upserts,
        List<string> deletes,
        CancellationToken cancellationToken)
    {
        if (upserts.Count > 0 || deletes.Count > 0)
        {
            await viewTree.SetManyAtomicAsync(upserts, deletes, ViewSagaOperationId(txId), cancellationToken);
        }
    }

    /// <summary>
    /// Runs the deferred reconcile scheduled by a cross-tree degrade this pass, if
    /// any. Called at the end of a drain pass (after the checkpoint is persisted)
    /// so the rebuild does not clear the staging buffer under the flush loop.
    /// </summary>
    private async Task RunPendingCrossTreeReconcileAsync(CancellationToken cancellationToken)
    {
        if (!_pendingCrossTreeReconcile)
        {
            return;
        }

        _pendingCrossTreeReconcile = false;
        try
        {
            await ReconcileAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex,
                "View '{ViewName}' reconcile after a cross-tree degrade failed; will retry on a later pass.",
                ViewName);
        }
    }
}
