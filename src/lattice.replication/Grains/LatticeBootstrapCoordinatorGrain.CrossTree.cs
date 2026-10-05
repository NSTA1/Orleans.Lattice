using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// A per-tree import of one cross-tree participant (issue #4683). A cross-tree
/// atomic write replicates as one sub-saga per participating tree, and the
/// receiver's cross-tree barrier (<see cref="ILatticeCrossTreeReceiverGrain"/>)
/// flips every replicated participant visible together once each tree's
/// terminal has arrived. A bootstrap or re-seed of one participant settles that
/// tree's sub-saga from the export - committed rows plus a decision row - in
/// place of its terminal, which may never be shipped again. Without telling the
/// barrier, a sibling that already delegated to it waits for ever, and is served
/// pre-saga while the imported tree is served post-saga.
/// <para>
/// So a decision row that names its cross-tree operation records the tree's
/// arrival at the barrier exactly as a shipped terminal does, and the imported
/// tree stays read-fenced until every barrier it arrived at has decided: a
/// fenced tree is consistent with either side, while lifting the fence at the
/// end of the drain would serve the saga split until the sibling's own terminal
/// arrives. Both steps are idempotent: a re-driven drain re-records the same
/// arrival, and the tree's real terminal, if it is ever shipped, overwrites it.
/// </para>
/// </summary>
internal sealed partial class LatticeBootstrapCoordinatorGrain
{
    /// <summary>
    /// Records an imported cross-tree sub-saga's arrival at the receiver's
    /// barrier for its operation, and materializes every participant when the
    /// arrival decides it. Returns the barrier's key, or <see langword="null"/>
    /// for a row that names no operation.
    /// </summary>
    private async Task<string?> NotifyImportedCrossTreeArrivalAsync(
        string treeName,
        string sourceClusterId,
        SnapshotEntry entry,
        CancellationToken cancellationToken)
    {
        if (entry.SettledDecision is not { } committed
            || entry.TransactionId == Guid.Empty
            || string.IsNullOrEmpty(entry.CrossTreeOperationId)
            || string.IsNullOrEmpty(sourceClusterId))
        {
            return null;
        }

        // The wait set the shipped terminal would have carried: the
        // participants replicated here, and always this tree.
        var waitSet = new List<string> { treeName };
        if (!entry.CrossTreeParticipants.IsDefaultOrEmpty)
        {
            foreach (var participant in entry.CrossTreeParticipants)
            {
                if (!string.Equals(participant, treeName, StringComparison.Ordinal) && IsTreeReplicatedHere(participant))
                {
                    waitSet.Add(participant);
                }
            }
        }

        var key = LatticeCrossTreeReceiverGrain.ComputeKey(sourceClusterId, entry.CrossTreeOperationId);
        var decision = await _grainFactory.GetGrain<ILatticeCrossTreeReceiverGrain>(key)
            .NotifyTerminalAsync(new CrossTreeReceiverTerminal
            {
                OriginClusterId = sourceClusterId,
                OperationId = entry.CrossTreeOperationId,
                TreeId = treeName,
                TransactionId = entry.TransactionId,
                Committed = committed,
                WaitSet = waitSet,
                // The import already settled this tree's keys; there is no
                // terminal to fan out to its leaves.
                ObservedSourceShards = [],
                TerminalHlc = HybridLogicalClock.Zero,
            })
            .ConfigureAwait(true);

        if (decision.Decided)
        {
            await FinalizeCrossTreeTreesAsync(decision, cancellationToken).ConfigureAwait(true);
        }

        return key;
    }

    /// <summary>
    /// The barriers that may wait for this tree's arrival through an operation
    /// the export does not name (issue #4684): opened by
    /// <paramref name="sourceClusterId"/>, undecided, waiting for
    /// <paramref name="treeName"/>, and already holding a sibling's arrival.
    /// Read before the export is requested, so every arrival they hold was
    /// recorded before the export opened. Keyed by barrier, valued by
    /// operation id.
    /// </summary>
    private async Task<Dictionary<string, string>> CaptureCrossTreeImportCandidatesAsync(string treeName, string sourceClusterId)
    {
        var candidates = new Dictionary<string, string>(StringComparer.Ordinal);
        if (string.IsNullOrEmpty(sourceClusterId))
        {
            return candidates;
        }

        var keys = await _grainFactory.GetGrain<ICrossTreeBarrierIndexGrain>(treeName).GetAsync().ConfigureAwait(true);
        foreach (var key in keys)
        {
            var status = await _grainFactory.GetGrain<ILatticeCrossTreeReceiverGrain>(key).GetStatusAsync().ConfigureAwait(true);
            if (status.Opened
                && !status.Decided
                && string.Equals(status.OriginClusterId, sourceClusterId, StringComparison.Ordinal)
                && status.WaitSet.Contains(treeName, StringComparer.Ordinal)
                && !status.ArrivedTrees.Contains(treeName, StringComparer.Ordinal)
                && status.ArrivedTrees.Count > 0)
            {
                candidates[key] = status.OperationId;
            }
        }

        return candidates;
    }

    /// <summary>
    /// Records this tree's arrival at every captured barrier whose operation
    /// the export named in no row (issue #4684). The export opened after a
    /// sibling's terminal was recorded here, so after the operation decided at
    /// the origin and so after this tree's sub-saga prepared there: an export
    /// that names it nowhere carried the sub-saga's outcome as plain rows,
    /// because the origin had purged it. The tree arrives with its siblings'
    /// verdict and stays read-fenced until the barrier decides.
    /// </summary>
    private async Task RecordUnnamedCrossTreeArrivalsAsync(
        string treeName,
        Dictionary<string, string> candidates,
        HashSet<string> namedOperations,
        HashSet<string> crossTreeBarriers,
        CancellationToken cancellationToken)
    {
        foreach (var (key, operationId) in candidates)
        {
            if (namedOperations.Contains(operationId))
            {
                continue;
            }

            var decision = await _grainFactory.GetGrain<ILatticeCrossTreeReceiverGrain>(key)
                .RecordImportedArrivalAsync(treeName)
                .ConfigureAwait(true);
            if (decision.Decided)
            {
                await FinalizeCrossTreeTreesAsync(decision, cancellationToken).ConfigureAwait(true);
            }

            crossTreeBarriers.Add(key);
        }
    }

    /// <summary>
    /// Materializes every participant of a decided barrier, as the terminal
    /// that completes a barrier does. Idempotent.
    /// </summary>
    private async Task FinalizeCrossTreeTreesAsync(CrossTreeReceiverDecision decision, CancellationToken cancellationToken)
    {
        foreach (var finalize in decision.TreesToFinalize)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await _grainFactory.GetGrain<IReplicationApplyGrain>(finalize.TreeId)
                .FinalizeCrossTreeTerminalAsync(
                    finalize.TransactionId,
                    decision.Committed,
                    finalize.ObservedSourceShards,
                    finalize.TerminalHlc,
                    finalize.OriginClusterId,
                    cancellationToken)
                .ConfigureAwait(true);
        }
    }

    /// <summary>The barriers among <paramref name="keys"/> that have not decided yet.</summary>
    private async Task<List<string>> UndecidedBarriersAsync(IEnumerable<string> keys)
    {
        var undecided = new List<string>();
        foreach (var key in keys)
        {
            var status = await _grainFactory.GetGrain<ILatticeCrossTreeReceiverGrain>(key)
                .GetDecisionAsync()
                .ConfigureAwait(true);
            if (status == TxStatus.InFlight)
            {
                undecided.Add(key);
            }
        }

        undecided.Sort(StringComparer.Ordinal);
        return undecided;
    }

    /// <summary>
    /// Re-checks the barriers holding the imported tree's read fence. Returns
    /// <see langword="true"/> once none is undecided, having lifted the fence
    /// (the caller persists); <see langword="false"/> while one still waits for a
    /// sibling's terminal, leaving the tree fenced for the next tick.
    /// </summary>
    private async Task<bool> ReleaseCrossTreeHoldAsync()
    {
        if (state.State.PendingCrossTreeBarriers.Count == 0)
        {
            return true;
        }

        var undecided = await UndecidedBarriersAsync(state.State.PendingCrossTreeBarriers).ConfigureAwait(true);
        if (undecided.Count > 0)
        {
            if (undecided.Count != state.State.PendingCrossTreeBarriers.Count)
            {
                state.State.PendingCrossTreeBarriers = undecided;
                await state.WriteStateAsync().ConfigureAwait(true);
            }

            Logger.LogDebug(
                "Tree '{TreeName}' stays read-fenced: {Count} cross-tree barrier(s) its import arrived at await a sibling tree's terminal",
                TreeName, undecided.Count);
            return false;
        }

        if (state.State.ReadFenceArmed)
        {
            await LiftReadFenceAsync().ConfigureAwait(true);
        }

        state.State.PendingCrossTreeBarriers = [];
        Logger.LogInformation(
            "Every cross-tree barrier the import of tree '{TreeName}' arrived at has decided; its read fence is lifted",
            TreeName);
        return true;
    }

    /// <summary>
    /// Whether <paramref name="treeId"/> is replicated on this receiver, by the
    /// rule the replication applier scopes a shipped terminal's wait set with.
    /// </summary>
    private bool IsTreeReplicatedHere(string treeId) =>
        Context.ActivationServices?.GetService<ILatticeReplicationContext>()?.ResolveMergeMode(treeId) is not null
        || _optionsMonitor.Get(treeId).ReplicatedTrees?.ContainsKey(treeId) == true;
}
