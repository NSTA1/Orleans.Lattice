using Orleans.Lattice.Operations;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The tracked (operation-relayed) surface of the cluster's admin grain, keyed by
/// <see cref="LatticeConstants.AdminGrainKey"/> and implemented by the same
/// activation as <see cref="ILatticeAdmin"/>. Kept off the public
/// <see cref="ILatticeAdmin"/> so the released interface does not grow: these are
/// the long admin calls a coordinated operation drives, which report progress to
/// the operation and stop when it is cancelled.
/// </summary>
[Alias(TypeAliases.ILatticeAdminTrackedGrain)]
internal interface ILatticeAdminTrackedGrain : IGrainWithStringKey
{
    /// <summary>
    /// <see cref="ILatticeAdmin.ExecuteWalMoveAsync(string, int, string, WalMoveOptions?, CancellationToken)"/>
    /// as a tracked grain call: reports the tail copy (entries copied of the live
    /// tail), the verification and the pin flip to the coordinated operation
    /// <paramref name="ticket"/> names, and stops when that operation is cancelled.
    /// A move stopped before the flip leaves the source live, exactly as a failed
    /// move does.
    /// </summary>
    /// <param name="treeId">The tree whose WAL partition to move.</param>
    /// <param name="partition">The WAL partition to move.</param>
    /// <param name="targetProviderKey">The catalog key to move the partition to.</param>
    /// <param name="options">The move options, or <see langword="null"/> for the defaults.</param>
    /// <param name="ticket">The operation to report to. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the move before its flip.</param>
    /// <returns>The move receipt.</returns>
    [ResponseTimeout(LatticeMaintenanceProgress.TrackedCallResponseTimeout)]
    Task<WalMoveReceipt> ExecuteWalMoveTrackedAsync(
        string treeId,
        int partition,
        string targetProviderKey,
        WalMoveOptions? options,
        LatticeOperationTicket ticket,
        CancellationToken cancellationToken = default);
}
