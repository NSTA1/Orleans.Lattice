namespace Orleans.Lattice;

/// <summary>
/// Routes a silo's WAL GC orphan-pin sweep demand to the tree's cluster-wide
/// <see cref="BPlusTree.Grains.IWalOrphanPinSweepGrain"/> (issue #5097).
/// </summary>
internal interface IWalOrphanPinSweepCoordinator
{
    /// <summary>
    /// Records sweep demand for <paramref name="treeId"/> and returns the latest
    /// completed outcome, or <see langword="null"/> when none has completed yet.
    /// </summary>
    /// <param name="treeId">The tree to sweep.</param>
    /// <param name="classifyFloorHolders">Whether the caller needs the floor holders classified.</param>
    /// <param name="cancellationToken">Abandons the request.</param>
    Task<WalOrphanPinSweepOutcome?> RequestSweepAsync(
        string treeId,
        bool classifyFloorHolders,
        CancellationToken cancellationToken);
}
