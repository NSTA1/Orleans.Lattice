namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Internal per-tree WAL-only storage-usage aggregator. One activation per
/// tree, keyed by <c>treeId</c>. Fans out only to this tree's WAL partition
/// grains (never to shard roots, leaves, internal nodes, or snapshot
/// storage grains), so the byte-pressure WAL retention policy and the
/// <c>storage.wal_bytes</c> / <c>storage.policy.over_threshold</c> gauges
/// can be refreshed by the cluster-wide poller without pinning an idle
/// tree's full leaf chain into memory. Not part of the public API -
/// callers use <see cref="ILattice.GetStorageUsageAsync"/> for the deep
/// surface or <see cref="ILatticeAdmin.RefreshStorageUsageAsync"/> for an
/// operator-driven deep refresh.
/// </summary>
[Alias(TypeAliases.ILatticeWalUsage)]
internal interface ILatticeWalUsage : IGrainWithStringKey
{
    /// <summary>
    /// Returns the current WAL-only byte-accurate
    /// <see cref="TreeWalUsageReport"/> for this tree. Resolves the tree's
    /// WAL partition count and sums <see cref="Orleans.Lattice.BPlusTree.Grains.IWalShardGrain.GetRetainedByteSizeAsync"/>
    /// across every partition. Touches no leaf, internal-node, snapshot, or
    /// shard-root grain.
    /// </summary>
    /// <param name="cancellationToken">Cancels the WAL fan-out.</param>
    Task<TreeWalUsageReport> GetWalUsageAsync(CancellationToken cancellationToken);
}
