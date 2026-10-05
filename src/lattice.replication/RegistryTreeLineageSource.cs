using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Reads a tree's lineage from the tree registry entry
/// (<c>TreeRegistryEntry.Lineage</c>, issue #4537). A legacy row that predates
/// lineage stamping, or a tree with no row, reports none, which keeps the
/// receiver tree frontier in degraded mode for it.
/// </summary>
internal sealed class RegistryTreeLineageSource(IGrainFactory grainFactory) : ITreeLineageSource
{
    private readonly IGrainFactory _grainFactory = grainFactory ?? throw new ArgumentNullException(nameof(grainFactory));

    /// <inheritdoc />
    public async Task<Guid?> GetLineageAsync(string treeId, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        cancellationToken.ThrowIfCancellationRequested();
        var entry = await _grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId)
            .GetEntryAsync(treeId)
            .ConfigureAwait(false);
        return entry?.Lineage;
    }
}