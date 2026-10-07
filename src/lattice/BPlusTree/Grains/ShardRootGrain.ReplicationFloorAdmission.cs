using Orleans.Lattice.BPlusTree.State;
using Orleans.Serialization.Invocation;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The bootstrap drop-floor admission gate (issue #4549).
/// <para>
/// A full bootstrap installs a drop floor on the tree's replication
/// high-water-mark grain, then scans the tree for rows of other origins the
/// source had applied and deleted. The replication applier checks each delivery
/// against the floor when it admits it, but a write admitted just before the
/// install reaches the tree some time later, so without a barrier it could land
/// after the scan and resurrect a key the source deleted. Installing a floor
/// therefore raises a tree-level floor epoch (<see cref="TreeRegistryEntry.ReplicationFloorEpoch"/>),
/// the applier stamps every replicated write with the epoch it was admitted
/// under (<see cref="ReplicationFloorAdmission"/>), and every shard root - the
/// one seam every write of the tree passes through on its way to a leaf -
/// refuses a write carrying an older epoch with
/// <see cref="ReplicationFloorAdmissionStaleException"/>. The applier maps that
/// to a deferral, so the sender re-ships the write against the floor.
/// </para>
/// <para>
/// <see cref="ArmReplicationFloorEpochAsync"/> is the barrier. It is a serial
/// turn, so the point-write quiesce guard drains every admitted point write
/// before it runs, and no serial write can overlap it; it then raises the epoch
/// and waits for the interleaved writes admitted under an older epoch to finish
/// their leaf merges. A shard root that activates later - a split or reshard
/// target included - reads the epoch from the registry before it admits its
/// first stamped write. Writes that carry no stamp are never refused.
/// </para>
/// </summary>
internal sealed partial class ShardRootGrain
{
    /// <summary>The highest floor epoch this activation knows of.</summary>
    private long _replicationFloorEpoch;

    /// <summary>Whether <see cref="_replicationFloorEpoch"/> has been reconciled with the registry.</summary>
    private bool _replicationFloorEpochLoaded;

    /// <summary>Interleaved stamped writes in flight, by the epoch they were admitted under.</summary>
    private Dictionary<long, int>? _floorStampedWritesInFlight;

    /// <summary>Completed whenever an interleaved stamped write finishes, while an arm waits.</summary>
    private TaskCompletionSource? _floorStampedWriteFinished;

    /// <summary>The floor epoch this activation enforces. Exposed for unit tests.</summary>
    internal long ReplicationFloorEpoch => _replicationFloorEpoch;

    /// <inheritdoc />
    public async Task ArmReplicationFloorEpochAsync(long epoch)
    {
        EnsureInternalOrigin(LatticeOperation.Replication);
        if (epoch > _replicationFloorEpoch)
        {
            _replicationFloorEpoch = epoch;
        }

        while (AnyStampedWriteInFlightBelow(epoch))
        {
            _floorStampedWriteFinished ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await _floorStampedWriteFinished.Task;
        }
    }

    /// <summary>
    /// The <see cref="IShardRootGrain"/> methods a replicated write enters a
    /// shard through. Leaf-to-shard callbacks are deliberately absent: they
    /// belong to a write already admitted, which the arm waits for rather than
    /// refuses.
    /// </summary>
    internal static bool IsFloorGatedMethod(string? methodName) => methodName switch
    {
        nameof(IShardRootGrain.SetAsync) => true,
        nameof(IShardRootGrain.SetManyAsync) => true,
        nameof(IShardRootGrain.DeleteAsync) => true,
        nameof(IShardRootGrain.MergeManyAsync) => true,
        nameof(IShardRootGrain.ApplyCrdtDeltaAsync) => true,
        nameof(IShardRootGrain.ApplyCrdtDeltaManyAsync) => true,
        _ => false,
    };

    /// <summary>
    /// The gated dispatch for a stamped replicated write, or <see langword="null"/>
    /// when the call is not one. A single request-context read on the unstamped path.
    /// </summary>
    private Task? InvokeIfFloorStamped(IIncomingGrainCallContext context)
    {
        if (!ReplicationFloorAdmission.TryGet(out var epoch)
            || context.Request.GetInterfaceType() != typeof(IShardRootGrain)
            || !IsFloorGatedMethod(context.Request.GetMethodName()))
        {
            return null;
        }

        return InvokeFloorStampedAsync(context, epoch);
    }

    private async Task InvokeFloorStampedAsync(IIncomingGrainCallContext context, long epoch)
    {
        await EnsureReplicationFloorEpochLoadedAsync();
        ThrowIfFloorAdmissionStale(epoch);

        switch (ClassifyIncomingTurn(context.Request))
        {
            case IncomingTurnKind.PointWrite:
                // Re-checked after any serial turn it waits out (an arm included).
                await InvokePointWriteAsync(context);
                return;

            case IncomingTurnKind.Serial:
                // No arm can overlap a serial turn.
                await InvokeSerialTurnAsync(context);
                return;

            default:
                var inFlight = _floorStampedWritesInFlight ??= new Dictionary<long, int>();
                inFlight[epoch] = inFlight.GetValueOrDefault(epoch) + 1;
                try
                {
                    await InvokeRoutingFiltered(context);
                }
                finally
                {
                    if (--inFlight[epoch] == 0)
                    {
                        inFlight.Remove(epoch);
                    }

                    var finished = _floorStampedWriteFinished;
                    _floorStampedWriteFinished = null;
                    finished?.TrySetResult();
                }

                return;
        }
    }

    /// <summary>
    /// Refuses the current flow's write if it carries a floor epoch older than
    /// the one this shard enforces. Called again by a point write once it has
    /// waited out a serial turn, which may have been an arm.
    /// </summary>
    private void ThrowIfFloorAdmissionStale()
    {
        if (ReplicationFloorAdmission.TryGet(out var epoch))
        {
            ThrowIfFloorAdmissionStale(epoch);
        }
    }

    private void ThrowIfFloorAdmissionStale(long epoch)
    {
        if (epoch < _replicationFloorEpoch)
        {
            throw new ReplicationFloorAdmissionStaleException(TreeId, epoch, _replicationFloorEpoch);
        }
    }

    private async Task EnsureReplicationFloorEpochLoadedAsync()
    {
        if (_replicationFloorEpochLoaded)
        {
            return;
        }

        var entry = await grainFactory.GetLatticeRegistry().GetEntryAsync(TreeId);
        if (entry?.ReplicationFloorEpoch is { } registered && registered > _replicationFloorEpoch)
        {
            _replicationFloorEpoch = registered;
        }

        _replicationFloorEpochLoaded = true;
    }

    private bool AnyStampedWriteInFlightBelow(long epoch)
    {
        if (_floorStampedWritesInFlight is not { Count: > 0 } inFlight)
        {
            return false;
        }

        foreach (var admitted in inFlight.Keys)
        {
            if (admitted < epoch)
            {
                return true;
            }
        }

        return false;
    }
}
