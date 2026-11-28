namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Write-class gate (issue #5699). A tree disabled at runtime, or whose merge
/// mode became ambiguous, resolves no write class, and every peer drops its
/// records. A shipper that kept shipping would advance its cursor past those
/// records and lose them. So the shipper re-resolves the tree's write class on
/// every tick rather than caching it for its lifetime, and while none resolves
/// it applies the class's <see cref="ReplicationWriteClassRetention"/>: today
/// it stops shipping and detaches from the log (<see cref="DetachCoreAsync"/>),
/// marking the peer for re-seed. When a write class resolves again it
/// re-attaches (<see cref="ReattachToLogAsync"/>) and the peer is re-seeded.
/// <para>
/// A shipper detached because its peer was removed is not re-attached here;
/// only the driver's <see cref="EnsureActiveAsync"/> does that.
/// </para>
/// </summary>
internal sealed partial class ReplicationShipperGrain
{
    private LatticeMergeMode? _lastResolvedWriteClass;

    /// <summary>
    /// Applies the write-class gate. Returns <see langword="true"/> when the
    /// tick must not read or ship. Without a replication context (the
    /// direct-construction unit-test path) the gate is open.
    /// </summary>
    private async Task<bool> SuspendIfWriteClassUnresolvedAsync()
    {
        if (_replicationContext is null)
        {
            return false;
        }

        var writeClass = _replicationContext.ResolveMergeMode(_treeName);
        if (writeClass is not null)
        {
            _lastResolvedWriteClass = writeClass;
            if (state.State.SuspendedForWriteClass)
            {
                await ReattachToLogAsync();
            }

            return false;
        }

        if (state.State.DetachedFromLog)
        {
            // Suspended already, or detached because the peer was removed.
            return true;
        }

        switch (ReplicationWriteClassRetentionPolicy.For(_lastResolvedWriteClass))
        {
            case ReplicationWriteClassRetention.DetachAndReseed:
            default:
                await DetachCoreAsync(writeClassUnresolved: true);
                return true;
        }
    }

    /// <summary>
    /// Whether the driver's <see cref="EnsureActiveAsync"/> may re-attach this
    /// shipper now. While the tree resolves no write class a detached shipper
    /// stays detached and is handed to the write-class gate, which re-attaches
    /// it once a write class resolves.
    /// </summary>
    private async Task<bool> MayReattachOnEnsureAsync()
    {
        if (_replicationContext is null || _replicationContext.ResolveMergeMode(_treeName) is not null)
        {
            return true;
        }

        if (state.State.DetachedFromLog && !state.State.SuspendedForWriteClass)
        {
            state.State.SuspendedForWriteClass = true;
            await state.WriteStateAsync();
        }

        return false;
    }
}
