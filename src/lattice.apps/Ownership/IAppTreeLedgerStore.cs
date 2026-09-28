using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Apps;

/// <summary>
/// The storage seam of the tree ownership ledger. The production implementation is
/// <see cref="LatticeAppTreeLedgerStore"/> over the reserved <c>sys-app-trees</c> tree; the seam
/// exists so claim, conflict and release logic can be driven deterministically in unit tests.
/// Every write is a compare-and-set, so two concurrent claimants of one tree cannot both win.
/// </summary>
internal interface IAppTreeLedgerStore
{
    /// <summary>Reads a claim and the version to write it back conditionally on.</summary>
    /// <param name="treeId">The effective tree id (the ledger key).</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The claim (or <c>null</c>) and its version (<see cref="HybridLogicalClock.Zero"/> when absent).</returns>
    Task<AppTreeLedgerRead> GetAsync(string treeId, CancellationToken cancellationToken);

    /// <summary>
    /// Writes <paramref name="claim"/> only if the stored version still equals
    /// <paramref name="expectedVersion"/> (<see cref="HybridLogicalClock.Zero"/> meaning create only
    /// if still absent).
    /// </summary>
    /// <param name="treeId">The effective tree id (the ledger key).</param>
    /// <param name="claim">The claim to write.</param>
    /// <param name="expectedVersion">The version read before deciding the write.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns><c>true</c> when the write applied; <c>false</c> when a competing writer won.</returns>
    Task<bool> TrySetAsync(string treeId, AppTreeClaim claim, HybridLogicalClock expectedVersion, CancellationToken cancellationToken);

    /// <summary>Enumerates every ledger entry in ascending key order.</summary>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <returns>Each effective tree id with its claim.</returns>
    IAsyncEnumerable<KeyValuePair<string, AppTreeClaim>> ScanAsync(CancellationToken cancellationToken);
}
