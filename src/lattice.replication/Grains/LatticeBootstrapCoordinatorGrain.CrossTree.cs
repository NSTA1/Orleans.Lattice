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
        // Before the arrival, so the barrier judges its other trees' imports
        // against them from the moment this row opens it (#4684), and keeps what
        // its tombstone is dropped against (#4733).
        await barrier.RecordDecisionStampsAsync(
                (IReadOnlyDictionary<string, long>?)entry.CrossTreeDecisionStamps ?? System.Collections.Immutable.ImmutableDictionary<string, long>.Empty,
                entry.CrossTreeDecisionSequences,
                entry.CrossTreeParticipants.IsDefaultOrEmpty ? null : entry.CrossTreeParticipants)
            .ConfigureAwait(true);

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
    /// Whether the fence is held for a decommissioned source (#4742): the
    /// latch is set, or the cluster-wide decommissioned-peer registry lists the
    /// source, in which case the latch is set and persisted so a re-add, not
    /// the registry clearing, releases it. A registry that cannot be read holds
    /// (fail closed).
    /// </summary>
    private async Task<bool> FenceHeldForDecommissionedSourceAsync()
    {
        if (state.State.FenceHeldForDecommissionedSource)
        {
            return true;
        }

        bool decommissioned;
        try
        {
            decommissioned = await _grainFactory
                .GetGrain<IReplicationDecommissionedPeerRegistryGrain>(IReplicationDecommissionedPeerRegistryGrain.SingletonKey)
                .IsDecommissionedAsync(state.State.SourceClusterId)
                .ConfigureAwait(true);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Logger.LogDebug(ex, "Reading the decommissioned-peer registry for tree '{TreeName}' failed; its fence stays up", TreeName);
            return true;
        }

        if (!decommissioned)
        {
            return false;
        }

        state.State.FenceHeldForDecommissionedSource = true;
        await state.WriteStateAsync().ConfigureAwait(true);
        return true;
    }

    /// <inheritdoc />
    public async Task<bool> HoldFenceForDecommissionedSourceAsync(string sourceClusterId)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceClusterId);
        if (!state.State.InProgress
            || !string.Equals(state.State.SourceClusterId, sourceClusterId, StringComparison.Ordinal)
            || (!state.State.ReadFenceArmed
                && state.State.PendingCrossTreeBarriers.Count == 0
                && state.State.PendingSiblingBoundaries.Count == 0))
        {
            return false;
        }

        if (!state.State.FenceHeldForDecommissionedSource)
        {
            state.State.FenceHeldForDecommissionedSource = true;
            try
            {
                await state.WriteStateAsync().ConfigureAwait(true);
            }
            catch
            {
                state.State.FenceHeldForDecommissionedSource = false;
                throw;
            }

            Logger.LogWarning(
                "Source '{SourceClusterId}' of tree '{TreeName}' was decommissioned while its import held the read fence; the fence stays up until the source is re-added",
                sourceClusterId, TreeName);
        }

        return true;
    }

    /// <inheritdoc />
    public async Task<bool> RedriveForReAddedSourceAsync(string sourceClusterId)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceClusterId);
        if (!state.State.FenceHeldForDecommissionedSource
            || !string.Equals(state.State.SourceClusterId, sourceClusterId, StringComparison.Ordinal))
        {
            return false;
        }

        // The fence slots are carried over, as a failed bootstrap's re-drive
        // carries them (#4526): the fresh drain decides whether it lifts.
        var prevPhase = state.State.Phase;
        state.State.FenceHeldForDecommissionedSource = false;
        state.State.Phase = LatticeBootstrapState.RequestingSnapshot;
        try
        {
            await state.WriteStateAsync().ConfigureAwait(true);
        }
        catch
        {
            state.State.FenceHeldForDecommissionedSource = true;
            state.State.Phase = prevPhase;
            throw;
        }

        Logger.LogWarning(
            "Source '{SourceClusterId}' of tree '{TreeName}' was re-added after a decommission; re-driving the read-fenced import from a fresh export",
            sourceClusterId, TreeName);
        return true;
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

        // An abandon is not a decision (#4742): a decommission of the source
        // clears its barriers without deciding them, so they stop counting
        // below. Until a re-add re-drives the import, the fence stays up.
        if (await FenceHeldForDecommissionedSourceAsync().ConfigureAwait(true))
        {
            Logger.LogDebug(
                "Tree '{TreeName}' stays read-fenced: its source '{SourceClusterId}' was decommissioned and has not been re-added",
                TreeName, state.State.SourceClusterId);
            return false;
        }

        // While any barrier the drain recorded still waits, every barrier indexed
        // under the tree counts too, including one that opened after the drain
        // (#4684). Once the drain's own barriers have decided, a barrier that
        // opens under the tree later does not hold the fence: the import did not
        // arrive at it, so its import either opened before that operation's
        // decision or named it, and the tree's keys of it are still pending or
        // pre-saga, which no sibling contradicts. The sibling boundaries below
        // hold the fence in their own right.
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
        state.State.SiblingRefreshesRequested.Clear();
        Logger.LogInformation(
            "Every cross-tree barrier and sibling boundary holding the import of tree '{TreeName}' is settled; its read fence is lifted",
            TreeName);
        return true;
    }

    /// <inheritdoc />
    public Task<string[]> GetPendingCrossTreeHoldsForTestingAsync()
    {
        var holds = new List<string>(
            state.State.PendingCrossTreeBarriers.Count + state.State.PendingSiblingBoundaries.Count);
        holds.AddRange(state.State.PendingCrossTreeBarriers
            .Order(StringComparer.Ordinal)
            .Select(static barrier => $"barrier:{barrier}"));
        holds.AddRange(state.State.PendingSiblingBoundaries
            .OrderBy(static entry => entry.Key, StringComparer.Ordinal)
            .Select(entry =>
            {
                var boundary = entry.Value;
                var tails = string.Join(",", boundary.Tails);
                var reseedRequested = state.State.SiblingReseedsRequested.Contains(entry.Key);
                var refreshRequested = state.State.SiblingRefreshesRequested.Contains(entry.Key);
                return $"sibling:{entry.Key};exportEpoch={boundary.ExportEpoch};physical={boundary.PhysicalTreeId};tails=[{tails}];reseedRequested={reseedRequested};refreshRequested={refreshRequested}";
            }));
        return Task.FromResult(holds.ToArray());
    }

    /// <inheritdoc />
    public async Task<bool> ReleaseCrossTreeHoldForTestingAsync(bool ageSiblingBoundaries)
    {
        if (ageSiblingBoundaries && state.State.PendingSiblingBoundaries.Count > 0)
        {
            state.State.SiblingBoundariesSinceUtcTicks =
                DateTime.UtcNow.Ticks - SiblingBoundaryReseedAfter.Ticks - 1;
        }

        return await ReleaseCrossTreeHoldAsync().ConfigureAwait(true);
    }

    /// <inheritdoc />
    public async Task MarkSiblingReseedRequestedForTestingAsync(string siblingTreeName)
    {
        ArgumentException.ThrowIfNullOrEmpty(siblingTreeName);
        if (!state.State.PendingSiblingBoundaries.ContainsKey(siblingTreeName))
        {
            throw new InvalidOperationException($"Sibling '{siblingTreeName}' has no pending boundary for tree '{TreeName}'.");
        }

        state.State.SiblingReseedsRequested.Add(siblingTreeName);
        await state.WriteStateAsync().ConfigureAwait(true);
    }

    /// <inheritdoc />
    public async Task<bool> RefreshStuckSiblingImportAsync(string sourceClusterId, string requestingTreeName)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceClusterId);
        ArgumentException.ThrowIfNullOrEmpty(requestingTreeName);

        // Only the lower-named tree may refresh the higher-named side of a
        // mutual hold. This deterministic choice prevents both coordinators
        // from repeatedly reopening snapshots at the same time.
        if (string.CompareOrdinal(requestingTreeName, TreeName) >= 0
            || !state.State.InProgress
            || state.State.Phase != LatticeBootstrapState.IncrementalHandoff
            || !string.Equals(state.State.SourceClusterId, sourceClusterId, StringComparison.Ordinal)
            || state.State.PendingSiblingBoundaries.Count == 0
            || DateTime.UtcNow.Ticks - state.State.SiblingBoundariesSinceUtcTicks < SiblingBoundaryReseedAfter.Ticks)
        {
            return false;
        }

        var previousPhase = state.State.Phase;
        state.State.Phase = LatticeBootstrapState.RequestingSnapshot;
        try
        {
            await state.WriteStateAsync().ConfigureAwait(true);
        }
        catch
        {
            state.State.Phase = previousPhase;
            throw;
        }

        await StartCoordinatorAsync().ConfigureAwait(true);

        Logger.LogWarning(
            "Tree '{TreeName}' is re-opening its snapshot from '{SourceClusterId}' to break an aged mutual sibling-boundary hold requested by '{RequestingTreeName}'",
            TreeName, sourceClusterId, requestingTreeName);
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
        state.State.SiblingRefreshesRequested.Clear();
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
    /// as a fall-off does; starts an idle sibling's bootstrap once, or asks one
    /// side of an aged mutual handoff to refresh its snapshot without starting a
    /// competing bootstrap on an active coordinator.
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
            var coordinator = _grainFactory.GetGrain<ILatticeBootstrapCoordinatorGrain>(sibling);
            BootstrapCoordinatorStatus status;
            try
            {
                status = await coordinator.GetStatusAsync(CancellationToken.None).ConfigureAwait(true);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                Logger.LogDebug(ex, "Reading the bootstrap status of sibling tree '{Sibling}' failed; its re-seed will be retried on a later tick", sibling);
                continue;
            }

            if (status.SourceClusterId is null)
            {
                var reseedFinished = state.State.SiblingReseedsRequested.Remove(sibling);
                var refreshFinished = state.State.SiblingRefreshesRequested.Remove(sibling);
                if (reseedFinished || refreshFinished)
                {
                    // The requested drain finished but did not pass this captured
                    // boundary. Back off before asking for another export, which
                    // must open after the boundary rather than merely repeat it.
                    state.State.SiblingBoundariesSinceUtcTicks = DateTime.UtcNow.Ticks;
                    await state.WriteStateAsync().ConfigureAwait(true);
                    continue;
                }
            }

            if (!_optionsMonitor.Get(sibling).AutoBootstrapOnFallOffLog)
            {
                Logger.LogWarning(
                    "Tree '{TreeName}' stays read-fenced: sibling tree '{Sibling}' has not passed its boundary from {Source} and automatic bootstrap is disabled; re-seed it to serve '{TreeName}'",
                    TreeName, sibling, sourceClusterId, TreeName);
                continue;
            }

            // An active drain should normally finish on its own. If both
            // imports have reached handoff and each is waiting for the other's
            // captured epoch, let only the lower-named tree request one fresh
            // snapshot from the higher-named sibling. This advances a boundary
            // without dropping the strict epoch comparison or calling
            // BootstrapAsync on a coordinator that is already active.
            if (status.SourceClusterId is not null)
            {
                if (status.Phase != LatticeBootstrapState.IncrementalHandoff
                    || !string.Equals(status.SourceClusterId, sourceClusterId, StringComparison.Ordinal)
                    || string.CompareOrdinal(TreeName, sibling) >= 0
                    || !state.State.SiblingRefreshesRequested.Add(sibling))
                {
                    continue;
                }

                try
                {
                    await state.WriteStateAsync().ConfigureAwait(true);
                }
                catch
                {
                    state.State.SiblingRefreshesRequested.Remove(sibling);
                    throw;
                }

                try
                {
                    if (!await coordinator.RefreshStuckSiblingImportAsync(sourceClusterId, TreeName).ConfigureAwait(true))
                    {
                        state.State.SiblingRefreshesRequested.Remove(sibling);
                        await state.WriteStateAsync().ConfigureAwait(true);
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    state.State.SiblingRefreshesRequested.Remove(sibling);
                    await state.WriteStateAsync().ConfigureAwait(true);
                    Logger.LogDebug(ex, "The request to refresh stuck sibling import '{Sibling}' failed; retried on a later tick", sibling);
                }

                continue;
            }

            if (state.State.SiblingReseedsRequested.Contains(sibling))
            {
                continue;
            }

            if (!state.State.SiblingReseedsRequested.Add(sibling))
            {
                continue;
            }

            try
            {
                // Do not await a sibling coordinator here: it may be making the
                // same request while its own timer turn is waiting on this one.
                _ = ObserveSiblingReseedAsync(coordinator.BootstrapAsync(sourceClusterId, CancellationToken.None), sibling, sourceClusterId);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                state.State.SiblingReseedsRequested.Remove(sibling);
                Logger.LogDebug(ex, "The request to re-seed sibling tree '{Sibling}' from {Source} failed; retried on a later tick", sibling, sourceClusterId);
            }
        }

    }

    private async Task ObserveSiblingReseedAsync(Task request, string sibling, string sourceClusterId)
    {
        try
        {
            await request.ConfigureAwait(true);
            Logger.LogInformation(
                "Sibling tree '{Sibling}' has not passed its boundary from {Source} within {Bound}; its re-seed request completed so tree '{TreeName}' can be served",
                sibling, sourceClusterId, SiblingBoundaryReseedAfter, TreeName);
        }
        catch (Exception ex)
        {
            state.State.SiblingReseedsRequested.Remove(sibling);
            try
            {
                await state.WriteStateAsync().ConfigureAwait(true);
            }
            catch (Exception writeException)
            {
                Logger.LogWarning(writeException,
                    "Could not persist retry state after the re-seed request for sibling tree '{Sibling}' from {Source} failed",
                    sibling, sourceClusterId);
            }

            Logger.LogDebug(ex, "The request to re-seed sibling tree '{Sibling}' from {Source} failed; retried on a later tick", sibling, sourceClusterId);
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
