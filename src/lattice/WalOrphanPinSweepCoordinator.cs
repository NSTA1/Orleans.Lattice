namespace Orleans.Lattice;

/// <summary>
/// Default <see cref="IWalOrphanPinSweepCoordinator"/>: forwards demand to the
/// tree's <see cref="BPlusTree.Grains.IWalOrphanPinSweepGrain"/>.
/// </summary>
internal sealed class WalOrphanPinSweepCoordinator(IGrainFactory grainFactory) : IWalOrphanPinSweepCoordinator
{
    /// <inheritdoc/>
    public Task<WalOrphanPinSweepOutcome?> RequestSweepAsync(
        string treeId,
        bool classifyFloorHolders,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();
        return grainFactory
            .GetGrain<BPlusTree.Grains.IWalOrphanPinSweepGrain>(treeId)
            .RequestSweepAsync(classifyFloorHolders)
            .WaitAsync(cancellationToken);
    }
}
