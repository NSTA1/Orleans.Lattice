namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Internal per-tree storage-usage aggregator. One activation per tree,
/// keyed by <c>treeId</c>. Fans out to every physical shard and every WAL
/// partition on cache-miss to assemble a byte-accurate
/// <see cref="TreeStorageUsageReport"/>, caching the result for
/// <see cref="LatticeOptions.StorageUsageCacheTtl"/>. Not part of the public
/// API - callers use <see cref="ILattice.GetStorageUsageAsync"/>.
/// </summary>
[Alias(TypeAliases.ILatticeStorageUsage)]
internal interface ILatticeStorageUsage : IGrainWithStringKey
{
    /// <summary>
    /// Returns the current byte-accurate <see cref="TreeStorageUsageReport"/>
    /// for this tree. Concurrent callers within the configured cache TTL
    /// receive the cached report; a cache-miss fans out to every shard root
    /// (for leaf-state and snapshot bytes) and every WAL partition (for
    /// retained WAL bytes). A WAL provider that does not support byte
    /// accounting sets <see cref="TreeStorageUsageReport.Partial"/>.
    /// </summary>
    /// <param name="forceRefresh">When <see langword="true"/>, the cached report is
    /// discarded and the fan-out is re-run unconditionally. Operator-driven deep
    /// refresh (<see cref="ILatticeAdmin.RefreshStorageUsageAsync"/>) sets this so
    /// the returned figures reflect the current tree state, not a TTL-aged sample.</param>
    /// <param name="cancellationToken">Cancels the storage-usage fan-out.</param>
    Task<TreeStorageUsageReport> GetReportAsync(bool forceRefresh, CancellationToken cancellationToken);
}
