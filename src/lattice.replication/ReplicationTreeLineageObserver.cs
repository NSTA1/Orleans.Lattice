using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Replication-side <see cref="ITreeLineageObserver"/> (issue #4586 part 2b):
/// before the tree registry persists a change to a tree's lineage - a new
/// registration, an unregistration or purge, a restore, revert or alias move -
/// it forces a gap on the tree's receiver frontier, so no applied low watermark
/// accepted under the old contents outlives them. A failure propagates and the
/// registry does not persist the change. A tree not enrolled for replication
/// here holds no frontier claims, so it is skipped; when no enrollment source is
/// wired, every tree is notified.
/// </summary>
internal sealed class ReplicationTreeLineageObserver(
    IGrainFactory grainFactory,
    ILatticeReplicationContext? replicationContext = null) : ITreeLineageObserver
{
    private readonly IGrainFactory _grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));

    /// <inheritdoc />
    public Task OnLineageChangingAsync(string treeId, Guid? currentLineage, Guid? nextLineage, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();
        if (replicationContext is not null && replicationContext.ResolveMergeMode(treeId) is null)
        {
            return Task.CompletedTask;
        }

        return _grainFactory.GetGrain<IReplicationTreeFrontierGrain>(treeId)
            .OnLineageChangingAsync(nextLineage, cancellationToken);
    }
}