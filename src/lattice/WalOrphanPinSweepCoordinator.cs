namespace Orleans.Lattice;

/// <summary>
/// Default <see cref="IWalOrphanPinSweepCoordinator"/>: forwards demand to the
/// tree's <see cref="BPlusTree.Grains.IWalOrphanPinSweepGrain"/>.
/// </summary>
internal sealed class WalOrphanPinSweepCoordinator(IGrainFactory grainFactory, TimeProvider? timeProvider = null)
    : IWalOrphanPinSweepCoordinator
{
    /// <inheritdoc/>
    public Task<WalOrphanPinSweepOutcome?> RequestSweepAsync(
        string treeId,
        bool classifyFloorHolders,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        cancellationToken.ThrowIfCancellationRequested();
        var request = grainFactory
            .GetGrain<BPlusTree.Grains.IWalOrphanPinSweepGrain>(treeId)
            .RequestSweepAsync(classifyFloorHolders);
        request.Ignore();
        return request.WaitAsync(
            LatticeWalGcScheduler.OrphanSweepOperationBudget, timeProvider ?? TimeProvider.System, cancellationToken);
    }
}
