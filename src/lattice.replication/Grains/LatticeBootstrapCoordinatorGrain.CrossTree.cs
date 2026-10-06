using System.Collections.Immutable;
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
        var barrier = _grainFactory.GetGrain<ILatticeCrossTreeReceiverGrain>(key);
        if (entry.CrossTreeDecisionStamps is { Count: > 0 } stamps)
        {
            // Before the arrival, so the barrier judges its other trees' imports
            // against them from the moment this row opens it (#4684).
            await barrier.RecordDecisionStampsAsync(stamps).ConfigureAwait(true);
        }

        var decision = await barrier
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
    /// Records the import in the tree's barrier index (issue #4684) - the
    /// export's epoch and every cross-tree operation one of its rows named -
    /// then has every barrier indexed under the tree re-evaluate, materializing
    /// any that decides. Recorded only for an export the source served under
    /// the cross-tree hold and decision stamping, the premise a barrier's
    /// judgement of the import rests on.
    /// </summary>
    private async Task RecordCrossTreeImportAsync(
        string treeName,
        string sourceClusterId,
        SnapshotStream snapshot,
        HashSet<string> namedOperations,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(sourceClusterId) || !snapshot.CrossTreeHoldHonoured || snapshot.ExportEpoch <= 0)
        {
            return;
        }

        var index = _grainFactory.GetGrain<ICrossTreeBarrierIndexGrain>(treeName);
        await index.RecordImportAsync(sourceClusterId, new CrossTreeImportRecord
            {
                ExportEpoch = snapshot.ExportEpoch,
                NamedOperations = namedOperations.ToImmutableHashSet(StringComparer.Ordinal),
            })
            .ConfigureAwait(true);

        foreach (var key in await index.GetAsync().ConfigureAwait(true))
        {
            var decision = await _grainFactory.GetGrain<ILatticeCrossTreeReceiverGrain>(key)
                .ReevaluateAsync()
                .ConfigureAwait(true);
            if (decision.Decided)
            {
                await FinalizeCrossTreeTreesAsync(decision, cancellationToken).ConfigureAwait(true);
            }
        }
    }

    /// <summary>Every barrier indexed under <paramref name="treeName"/>, decided or not.</summary>
    private async Task<IReadOnlyCollection<string>> IndexedBarriersAsync(string treeName) =>
        await _grainFactory.GetGrain<ICrossTreeBarrierIndexGrain>(treeName).GetAsync().ConfigureAwait(true);

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
            // The drain lifted the fence: nothing was waiting.
            return true;
        }

        // While the fence is held, every barrier indexed under the tree counts,
        // including one that opened after the drain: the tree is served only
        // once none waits (#4684).
        var undecided = await UndecidedBarriersAsync(
                state.State.PendingCrossTreeBarriers.Union(await IndexedBarriersAsync(TreeName).ConfigureAwait(true), StringComparer.Ordinal))
            .ConfigureAwait(true);
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
