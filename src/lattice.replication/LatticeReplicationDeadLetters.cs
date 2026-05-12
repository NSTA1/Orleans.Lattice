using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Default <see cref="ILatticeReplicationDeadLetters"/> implementation.
/// Routes inspection / discard calls straight to the per-tree
/// <see cref="IReplicationDeadLetterGrain"/> activation, and replays
/// through the canonical concrete <see cref="ReplicationApplier"/> so
/// the in-memory failure tracker on
/// <see cref="DeadLetterTrackingReplicationApplier"/> is not engaged
/// for replay attempts (otherwise a deterministically-failing parked
/// entry would re-enqueue itself on every replay).
/// </summary>
internal sealed class LatticeReplicationDeadLetters(
    IGrainFactory grainFactory,
    ReplicationApplier inner) : ILatticeReplicationDeadLetters
{
    /// <inheritdoc />
    public Task<IReadOnlyList<DeadLetterEntry>> ListAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return Grain(treeId).ListAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task<int> CountAsync(string treeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return Grain(treeId).CountAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task<bool> DiscardAsync(string treeId, long entryId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        return Grain(treeId).DiscardAsync(entryId, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<ApplyResult?> ReplayAsync(string treeId, long entryId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);

        var grain = Grain(treeId);
        var parked = await grain.TryGetAsync(entryId, cancellationToken).ConfigureAwait(false);
        if (parked is null)
        {
            return null;
        }

        // Replay routes through the canonical applier, bypassing the
        // failure-tracking decorator. A successful return removes the
        // entry from the queue with reason=replayed; a thrown exception
        // leaves the entry parked for the operator to decide.
        var result = await inner.ApplyAsync(parked.Value.Entry, cancellationToken).ConfigureAwait(false);

        // Successful apply (or filtered re-delivery) is terminal for
        // inspection - remove the entry and tag the metric with
        // reason=replayed so dashboards can distinguish operator replay
        // from explicit discard.
        await grain.RemoveReplayedAsync(entryId, cancellationToken).ConfigureAwait(false);
        return result;
    }

    private IReplicationDeadLetterGrain Grain(string treeId) =>
        grainFactory.GetGrain<IReplicationDeadLetterGrain>(treeId);
}

