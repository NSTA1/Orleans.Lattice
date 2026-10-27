namespace Orleans.Lattice;

/// <summary>
/// Runs one WAL GC orphan-pin sweep of a tree on behalf of
/// <see cref="BPlusTree.Grains.IWalOrphanPinSweepGrain"/> (issue #5097).
/// Implemented by <see cref="LatticeWalGcScheduler"/>, which owns the sweep.
/// </summary>
internal interface IWalOrphanPinSweepRunner
{
    /// <summary>
    /// Sweeps <paramref name="treeId"/> once, without any rate limit, and
    /// returns its outcome, or <see langword="null"/> when this silo cannot
    /// sweep (no leaf-state storage provider).
    /// </summary>
    /// <param name="treeId">The tree to sweep.</param>
    /// <param name="classifyFloorHolders">Whether to classify the floor holders.</param>
    /// <param name="cancellationToken">Abandons the sweep.</param>
    Task<WalOrphanPinSweepOutcome?> RunAsync(
        string treeId,
        bool classifyFloorHolders,
        CancellationToken cancellationToken);
}
