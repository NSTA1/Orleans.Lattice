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

    /// <summary>
    /// The barriers among <paramref name="keys"/> that still hold this tree's
    /// read fence: opened, waiting for the tree and not durably decided. Each
    /// is asked itself, serialized with whatever opens or decides it, and
    /// withdraws its entry from the tree's index when it holds nothing, so an
    /// entry left by a failed withdrawal - or by a barrier whose open write
    /// failed, or that its retention cleared after it decided - never pins the
    /// fence (#4730). A barrier that cannot be asked holds (fail closed).
    /// </summary>
    private async Task<List<string>> UndecidedBarriersAsync(IEnumerable<string> keys)
    {
        var undecided = new List<string>();
        foreach (var key in keys)
        {
            bool holds;
            try
            {
                holds = await _grainFactory.GetGrain<ILatticeCrossTreeReceiverGrain>(key)
                    .SettleIndexEntryAsync(TreeName)
                    .ConfigureAwait(true);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                Logger.LogDebug(ex, "Settling cross-tree barrier {Key} for tree '{TreeName}' failed; it keeps holding the fence", key, TreeName);
                holds = true;
            }

            if (holds)
            {
                undecided.Add(key);
            }
        }

        undecided.Sort(StringComparer.Ordinal);
        return undecided;
    }

    /// <summary>
    /// Re-checks what holds the imported tree's read fence: the cross-tree
    /// barriers indexed under it (#4683, #4684) and the sibling boundaries its
    /// import captured (#4684). Returns <see langword="true"/> once nothing
    /// holds it, having lifted the fence (the caller persists);
    /// <see langword="false"/> while something still does, leaving the tree
    /// fenced for the next tick.
    /// </summary>
    private async Task<bool> ReleaseCrossTreeHoldAsync()
    {
        if (state.State.PendingCrossTreeBarriers.Count == 0 && state.State.PendingSiblingBoundaries.Count == 0)
        {
            // The drain lifted the fence: nothing was waiting.
            return true;
        }

        // While the fence is held, every barrier indexed under the tree counts,
        // including one that opened after the drain: the tree is served only
        // once none waits (#4684).
        var undecided = state.State.PendingCrossTreeBarriers.Count == 0
            ? []
            : await UndecidedBarriersAsync(
                    state.State.PendingCrossTreeBarriers.Union(await IndexedBarriersAsync(TreeName).ConfigureAwait(true), StringComparer.Ordinal))
                .ConfigureAwait(true);
        var siblingsBefore = state.State.PendingSiblingBoundaries.Count;
        await PruneSiblingBoundariesAsync(state.State.SourceClusterId).ConfigureAwait(true);
        if (state.State.PendingSiblingBoundaries.Count > 0)
        {
            await RequestStuckSiblingReseedsAsync(state.State.SourceClusterId).ConfigureAwait(true);
        }

        if (undecided.Count > 0 || state.State.PendingSiblingBoundaries.Count > 0)
        {
            if (undecided.Count != state.State.PendingCrossTreeBarriers.Count
                || siblingsBefore != state.State.PendingSiblingBoundaries.Count)
            {
                state.State.PendingCrossTreeBarriers = undecided;
                await state.WriteStateAsync().ConfigureAwait(true);
            }

            Logger.LogDebug(
                "Tree '{TreeName}' stays read-fenced: {Barriers} cross-tree barrier(s) await a sibling tree's terminal and {Siblings} sibling tree(s) have not passed their boundary",
                TreeName, undecided.Count, state.State.PendingSiblingBoundaries.Count);
            return false;
        }

        if (state.State.ReadFenceArmed)
        {
            await LiftReadFenceAsync().ConfigureAwait(true);
        }

        state.State.PendingCrossTreeBarriers = [];
        state.State.SiblingReseedsRequested.Clear();
        Logger.LogInformation(
            "Every cross-tree barrier and sibling boundary holding the import of tree '{TreeName}' is settled; its read fence is lifted",
            TreeName);
        return true;
    }

    /// <summary>How long a sibling may fail to pass its boundary before this coordinator asks to re-seed it.</summary>
    internal static TimeSpan SiblingBoundaryReseedAfter { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Records the sibling boundaries the drained export captured (issue #4684),
    /// scoped to the trees replicated here: a sibling this cluster does not
    /// replicate never vouches to it and has no barrier here to complete. An
    /// export that carried none - from a source that did not serve it under the
    /// cross-tree hold - holds nothing.
    /// </summary>
    private void RecordSiblingBoundaries(string treeName, SnapshotStream snapshot)
    {
        state.State.PendingSiblingBoundaries = new Dictionary<string, CrossTreeSiblingBoundary>(StringComparer.Ordinal);
        state.State.SiblingReseedsRequested.Clear();
        state.State.SiblingBoundariesSinceUtcTicks = DateTime.UtcNow.Ticks;
        if (snapshot.SiblingBoundaries is not { } boundaries)
        {
            return;
        }

        foreach (var (sibling, boundary) in boundaries)
        {
            if (!string.Equals(sibling, treeName, StringComparison.Ordinal)
                && !boundary.IsEmpty
                && IsTreeReplicatedHere(sibling))
            {
                state.State.PendingSiblingBoundaries[sibling] = boundary;
            }
        }
    }

    /// <summary>
    /// Drops every sibling that has passed its boundary (issue #4684): its
    /// shipper vouched acknowledged positions on the captured log at or past
    /// every captured tail - an acknowledged cross-tree terminal has reached its
    /// barrier here - or this cluster drained an import of it from an export
    /// numbered above the captured epoch, which opened after the capture. In
    /// memory; the caller persists. A sibling's import counts once its drain
    /// ends, before its own fence lifts, so two imports that wait on each
    /// other both pass.
    /// </summary>
    private async Task PruneSiblingBoundariesAsync(string? sourceClusterId)
    {
        if (state.State.PendingSiblingBoundaries.Count == 0 || string.IsNullOrEmpty(sourceClusterId))
        {
            return;
        }

        foreach (var (sibling, boundary) in state.State.PendingSiblingBoundaries.ToList())
        {
            var frontier = await _grainFactory.GetGrain<IReplicationTreeFrontierGrain>(sibling).GetAsync().ConfigureAwait(true);
            if (frontier.AckedPositions.TryGetValue(sourceClusterId, out var acked)
                && acked.CoversTails(boundary.PhysicalTreeId, boundary.Tails))
            {
                state.State.PendingSiblingBoundaries.Remove(sibling);
                continue;
            }

            var imported = await _grainFactory.GetGrain<ILatticeBootstrapCoordinatorGrain>(sibling)
                .GetDrainedExportEpochAsync(sourceClusterId)
                .ConfigureAwait(true);
            if (imported is { } epoch && epoch > boundary.ExportEpoch)
            {
                state.State.PendingSiblingBoundaries.Remove(sibling);
            }
        }
    }

    /// <summary>
    /// Asks to re-seed every pending sibling that has not passed its boundary
    /// within <see cref="SiblingBoundaryReseedAfter"/> (issue #4684): a sibling
    /// whose shipper never vouches positions here (it is off the log, or its
    /// replication is key-filtered) would otherwise pin the fence until an
    /// operator acts. Its import from an export opened after the capture passes
    /// the boundary. Honours <see cref="LatticeReplicationOptions.AutoBootstrapOnFallOffLog"/>
    /// as a fall-off does; asks at most once per sibling per import.
    /// </summary>
    private async Task RequestStuckSiblingReseedsAsync(string? sourceClusterId)
    {
        if (string.IsNullOrEmpty(sourceClusterId)
            || DateTime.UtcNow.Ticks - state.State.SiblingBoundariesSinceUtcTicks < SiblingBoundaryReseedAfter.Ticks)
        {
            return;
        }

        foreach (var sibling in state.State.PendingSiblingBoundaries.Keys.ToList())
        {
            if (!state.State.SiblingReseedsRequested.Add(sibling))
            {
                continue;
            }

            if (!_optionsMonitor.Get(sibling).AutoBootstrapOnFallOffLog)
            {
                Logger.LogWarning(
                    "Tree '{TreeName}' stays read-fenced: sibling tree '{Sibling}' has not passed its boundary from {Source} and automatic bootstrap is disabled; re-seed it to serve '{TreeName}'",
                    TreeName, sibling, sourceClusterId, TreeName);
                continue;
            }

            try
            {
                await _grainFactory.GetGrain<ILatticeBootstrapCoordinatorGrain>(sibling)
                    .BootstrapAsync(sourceClusterId, CancellationToken.None)
                    .ConfigureAwait(true);
                Logger.LogInformation(
                    "Sibling tree '{Sibling}' has not passed its boundary from {Source} within {Bound}; re-seeding it so tree '{TreeName}' can be served",
                    sibling, sourceClusterId, SiblingBoundaryReseedAfter, TreeName);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                state.State.SiblingReseedsRequested.Remove(sibling);
                Logger.LogDebug(ex, "Re-seeding sibling tree '{Sibling}' from {Source} was not accepted; retried on a later tick", sibling, sourceClusterId);
            }
        }
    }

    /// <summary>
    /// Whether <paramref name="treeId"/> is replicated on this receiver, by the
    /// rule the replication applier scopes a shipped terminal's wait set with.
    /// </summary>
    private bool IsTreeReplicatedHere(string treeId) =>
        Context.ActivationServices?.GetService<ILatticeReplicationContext>()?.ResolveMergeMode(treeId) is not null
        || _optionsMonitor.Get(treeId).ReplicatedTrees?.ContainsKey(treeId) == true;
}
