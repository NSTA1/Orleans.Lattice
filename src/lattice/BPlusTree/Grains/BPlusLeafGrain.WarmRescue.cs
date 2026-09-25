using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

internal sealed partial class BPlusLeafGrain
{
    private long[]? _warmCacheProvenOffsets;
    private long[]? _warmCacheOriginOffsets;
    private long _warmCacheOriginScalar = -1;
    private int _warmCacheHydrations;
    private bool _warmCacheTopologyChanged;
    private bool _warmCacheReplayFailed;
    private int _warmCacheReplaysInFlight;
    private bool _warmRescueInFlight;
    private long _snapshotKeptRevision;
    private ulong _warmRescueLoggedReasons;

    internal WarmRescueDeclineReason? LastWarmRescueDecline { get; private set; }

    private long WarmCacheOrigin(int partition) =>
        _warmCacheOriginOffsets is { } offsets && partition < offsets.Length
            ? offsets[partition]
            : partition == 0 ? _warmCacheOriginScalar : -1;

    private bool CanProveWarmReplayStart(int partition, long checkpoint) =>
        _warmCacheHydrations == 1
        && !_warmCacheTopologyChanged
        && !_warmCacheReplayFailed
        && _warmCacheReplaysInFlight == 1
        && WarmCacheOrigin(partition) >= 0
        && checkpoint >= WarmCacheOrigin(partition)
        && checkpoint <= (_warmCacheProvenOffsets is { } proven && partition < proven.Length
            ? proven[partition] : WarmCacheOrigin(partition));

    private WarmRescueDeclineReason? CheckWarmRescue(int partitionCount, bool captureOwned = false)
    {
        if (_warmCacheHydrations > 1 || _replayBarrierRetired)
            return WarmRescueDeclineReason.CacheRehydratedOrReset;
        if (_warmCacheProvenOffsets is not { } proven || proven.Length != partitionCount)
            return WarmRescueDeclineReason.UnprovenBaseline;
        if (_warmCacheTopologyChanged)
            return WarmRescueDeclineReason.TopologyChanged;
        if (_warmCacheReplayFailed || _warmCacheReplaysInFlight != 0 || !_replayBarrierSatisfied)
            return WarmRescueDeclineReason.ReplayIncomplete;
        if (_lastStaleReplayPartition < 0 || _lastStaleReplayPartition >= partitionCount
            || ResolveCoveragePartitionCount(partitionCount) != partitionCount)
            return WarmRescueDeclineReason.UnknownPartition;
        if (_reclaimRetired != 0 || state.State.MovedAwaySlots is { Length: > 0 })
            return WarmRescueDeclineReason.RetiredOrSealed;
        if (HasInterruptedSplit)
            return WarmRescueDeclineReason.SplitInFlight;
        if (_pendingTx is { Count: > 0 } || _pendingTxDeltas is { Count: > 0 }
            || _shadowedSagas is { Count: > 0 } || state.State.UnresolvedReplayWork is { Count: > 0 })
            return WarmRescueDeclineReason.PendingTransactions;
        if (_mutationsInFlight != 0)
            return WarmRescueDeclineReason.MutationInFlight;
        if (_snapshotCaptureInFlight && !captureOwned)
            return WarmRescueDeclineReason.CaptureInFlight;
        for (var partition = 0; partition < partitionCount; partition++)
        {
            if (WarmCacheOrigin(partition) > GetPersistedCheckpointForPartition(partition))
                return WarmRescueDeclineReason.ActivationAnchorAhead;
            if (GetCurrentCheckpointForPartition(partition) > proven[partition])
                return WarmRescueDeclineReason.CheckpointUnproven;
        }
        return null;
    }

    private bool DeclineWarmRescue(WarmRescueDeclineReason reason, Exception? fault = null)
    {
        LastWarmRescueDecline = reason;
        var bit = 1UL << (int)reason;
        if ((_warmRescueLoggedReasons & bit) == 0)
        {
            _warmRescueLoggedReasons |= bit;
            ResolveLogger()?.LogWarning(fault,
                "Warm stale-leaf rescue declined for leaf {LeafId} of tree {TreeId}: {DeclineReason}. No rescue checkpoint or pin was published.",
                context.GrainId, state.State.TreeId, reason);
        }
        return false;
    }

    private async Task<bool> TryRescueWarmStaleLeafAsync(int partitionCount, CancellationToken cancellationToken)
    {
        if (CheckWarmRescue(partitionCount) is { } reason)
            return DeclineWarmRescue(reason);
        if (!_splitGate.Wait(0))
            return DeclineWarmRescue(WarmRescueDeclineReason.SplitInFlight);

        _warmRescueInFlight = true;
        LastWarmRescueDecline = null;
        var keptRevision = _snapshotKeptRevision;
        try
        {
            // Only the activation's bound identity addresses storage. Neither a
            // replayed mutation nor a peer-supplied identifier selects this tree.
            var treeId = state.State.TreeId!;
            var claim = new long[partitionCount];
            for (var partition = 0; partition < partitionCount; partition++)
                claim[partition] = GetCurrentCheckpointForPartition(partition);
            for (var partition = 0; partition < partitionCount; partition++)
            {
                var coordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>($"{treeId}/{partition}");
                var tail = await coordinator.GetTailOffsetAsync(cancellationToken);
                if (tail > claim[partition] && tail - claim[partition] > 1)
                    return DeclineWarmRescue(WarmRescueDeclineReason.WalGapBeyondCache);
            }
            if (CheckWarmRescue(partitionCount) is { } afterProbe)
                return DeclineWarmRescue(afterProbe);

            cancellationToken.ThrowIfCancellationRequested();
            await CaptureSnapshotCoreAsync(cancellationToken, claim);
            if (_snapshotKeptRevision == keptRevision)
                return DeclineWarmRescue(LastWarmRescueDecline ?? WarmRescueDeclineReason.CaptureDeclined);

            cancellationToken.ThrowIfCancellationRequested();
            await FlushPendingCheckpointAsync(persistEvenWithoutPendingAdvance: true);
            await FlushDurableMaterialiserFrontierAsync(cancellationToken);
            _projectionStaleDriveLatch = null;
            LastWarmRescueDecline = null;
            ResolveLogger()?.LogInformation(
                "Warm stale-leaf rescue kept a covering snapshot for leaf {LeafId} of tree {TreeId}; durable checkpoint publication resumed.",
                context.GrainId, treeId);
            return true;
        }
        catch (Exception fault) when (fault is not OperationCanceledException
            && _snapshotKeptRevision == keptRevision)
        {
            return DeclineWarmRescue(WarmRescueDeclineReason.StorageFailure, fault);
        }
        finally
        {
            _warmRescueInFlight = false;
            _splitGate.Release();
        }
    }
}
