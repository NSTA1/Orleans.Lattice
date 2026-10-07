using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
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
    ILatticeReplicationContext? replicationContext = null,
    ILogger<ReplicationTreeLineageObserver>? logger = null) : ITreeLineageObserver
{
    private readonly IGrainFactory _grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));
    private readonly ILogger _logger = logger ?? NullLogger<ReplicationTreeLineageObserver>.Instance;

    /// <inheritdoc />
    public async Task OnLineageChangingAsync(string treeId, Guid? currentLineage, Guid? nextLineage, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();
        if (replicationContext is not null && replicationContext.ResolveMergeMode(treeId) is null)
        {
            return;
        }

        await _grainFactory.GetGrain<IReplicationTreeFrontierGrain>(treeId)
            .OnLineageChangingAsync(nextLineage, cancellationToken)
            .ConfigureAwait(false);

        if (currentLineage is not null && nextLineage is not null)
        {
            await WarnIfUncoordinatedAsync(treeId).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// A replacement of an existing replicated tree's contents that no
    /// coordinated restore holds the tree's receive fence for is a unilateral
    /// source restore, revert or rebind (issue #4586): the peers keep the writes
    /// it discarded, so they may diverge from this cluster, and a peer's write
    /// that depends on a discarded write can be made visible there without it,
    /// because this cluster's shipped watermark passes the discarded write once
    /// it re-covers the tree. A coordinated restore converges them. Observability
    /// only: it never fails the change.
    /// </summary>
    private async Task WarnIfUncoordinatedAsync(string treeId)
    {
        try
        {
            var fence = await _grainFactory.GetGrain<ITreeReceiveFenceGrain>(treeId).ObserveAsync().ConfigureAwait(false);
            if (fence.Paused)
            {
                return;
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogDebug(ex, "Could not read the receive fence of tree {Tree}; treating its lineage change as uncoordinated", treeId);
        }

        LatticeReplicationMetrics.SourceRestoreUncoordinated.Add(
            1,
            new KeyValuePair<string, object?>(LatticeReplicationMetrics.TagTree, treeId),
            LatticeTenantLabel.ForTree(treeId));
        _logger.LogWarning(
            "The contents of replicated tree {Tree} were replaced outside a coordinated restore. Peers keep the writes the replacement discarded and may diverge from this cluster, and a peer write that depends on a discarded write may become visible there without it. Use a coordinated restore to converge them.",
            treeId);
    }
}
