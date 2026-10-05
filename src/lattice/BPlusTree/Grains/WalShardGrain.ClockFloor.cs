namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The partition's clock floor (issue #4586). A replication shipper reading the
/// partition raises the floor to trail the wall clock by
/// <see cref="LatticeOptions.ReplicationClockFloorLag"/>, persists it, and
/// receives it paired with the next offset. Every append from then on is checked
/// against the floor under the same state gate that assigns its offset, so a
/// freshly authored local write stamped below a published floor never lands at
/// or above the offset it was published with.
/// </summary>
internal sealed partial class WalShardGrain
{
    /// <summary>
    /// The floor in force: the highest persisted floor. Read and raised under
    /// <see cref="_stateGate"/>; never lowered.
    /// </summary>
    private HybridLogicalClock _floor;

    /// <summary>This cluster's origin id for the partition's tree, resolved at activation.</summary>
    private string? _localClusterId;

    /// <summary>The in-flight floor persist, so concurrent shipping reads share one storage write.</summary>
    private Task? _floorAdvance;

    /// <summary>Loads the persisted floor and the local origin id. Called once per activation.</summary>
    private void InitializeClockFloor()
    {
        var persisted = floorState.State?.Floor ?? HybridLogicalClock.Zero;
        lock (_stateGate)
        {
            if (persisted > _floor)
            {
                _floor = persisted;
            }
        }

        _localClusterId = clusterIdResolver.Resolve(_treeId);
    }

    /// <summary>
    /// Whether every entry from <paramref name="start"/> on is admitted by the
    /// floor in force. Called under <see cref="_stateGate"/>.
    /// </summary>
    private bool IsBatchRemainderAdmitted(IReadOnlyList<WalRecord> entries, int start)
    {
        for (var j = start; j < entries.Count; j++)
        {
            var record = entries[j];
            if (!WalClockFloorCore.IsAdmitted(in record, _floor, _localClusterId))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>The stamp of the first entry from <paramref name="start"/> on that the floor in force refuses.</summary>
    private HybridLogicalClock FirstRefusedStamp(IReadOnlyList<WalRecord> entries, int start)
    {
        HybridLogicalClock floor;
        lock (_stateGate)
        {
            floor = _floor;
        }

        for (var j = start; j < entries.Count; j++)
        {
            var record = entries[j];
            if (!WalClockFloorCore.IsAdmitted(in record, floor, _localClusterId))
            {
                return record.Timestamp;
            }
        }

        return HybridLogicalClock.Zero;
    }

    /// <summary>Counts a refusal of <paramref name="refused"/> entries and builds its typed exception.</summary>
    private WalStampBelowFloorException RefuseBelowFloor(HybridLogicalClock stamp, HybridLogicalClock floor, int refused)
    {
        LatticeMetrics.WalAppendFloorRefusals.Add(refused, _treeTag, _shardTag, _tenantTag);
        return new WalStampBelowFloorException(_treeId, _shardIndex, stamp, floor);
    }

    /// <summary>
    /// Advances the floor when the capability gate is open and the floor has
    /// fallen half a lag behind its target, then returns the floor in force
    /// paired with the next offset, captured together under the gate. A floor
    /// already published is returned even while the gate is closed: it stays
    /// enforced, so the pair stays true.
    /// </summary>
    private ValueTask<(HybridLogicalClock Floor, long Offset)> PublishClockFloorAsync(CancellationToken cancellationToken)
    {
        if (floorGate.IsOpen)
        {
            var lag = Options.ReplicationClockFloorLag;
            var target = WalClockFloorCore.Target(TimeProvider.System.GetUtcNow().UtcTicks, lag);
            HybridLogicalClock current;
            lock (_stateGate)
            {
                current = _floor;
            }

            if (WalClockFloorCore.ShouldAdvance(current, target, lag))
            {
                return AdvanceThenCaptureAsync(target, cancellationToken);
            }
        }

        return ValueTask.FromResult(CaptureClockFloor());
    }

    private async ValueTask<(HybridLogicalClock Floor, long Offset)> AdvanceThenCaptureAsync(
        HybridLogicalClock target,
        CancellationToken cancellationToken)
    {
        Task advance;
        lock (_stateGate)
        {
            if (_floorAdvance is not { IsCompleted: false } running)
            {
                running = PersistClockFloorAsync(target);
                _floorAdvance = running;
            }

            advance = running;
        }

        await advance.WaitAsync(cancellationToken).ConfigureAwait(true);
        return CaptureClockFloor();
    }

    /// <summary>
    /// Persists <paramref name="target"/> and only then raises the floor in force,
    /// so a published floor is always durable. A failed write leaves the floor
    /// where it was; the next shipping read retries.
    /// </summary>
    private async Task PersistClockFloorAsync(HybridLogicalClock target)
    {
        var previous = floorState.State.Floor;
        if (target <= previous)
        {
            return;
        }

        floorState.State.Floor = target;
        try
        {
            await floorState.WriteStateAsync().ConfigureAwait(true);
        }
        catch (Exception ex)
        {
            floorState.State.Floor = previous;
            Trace($"clock-floor.persist-failed tree={_treeId} shard={_shardIndex} error={ex.GetType().Name}: {ex.Message}");
            return;
        }

        lock (_stateGate)
        {
            if (target > _floor)
            {
                _floor = target;
            }
        }
    }

    private (HybridLogicalClock Floor, long Offset) CaptureClockFloor()
    {
        lock (_stateGate)
        {
            return _floor == HybridLogicalClock.Zero ? (HybridLogicalClock.Zero, 0L) : (_floor, _nextOffset);
        }
    }
}
