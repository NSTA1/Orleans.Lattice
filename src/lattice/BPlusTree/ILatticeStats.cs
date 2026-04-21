using System.ComponentModel;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Internal per-tree diagnostics aggregator. One activation per tree, keyed
/// by <c>treeId</c>. Fans out to every physical shard on cache-miss and
/// caches the result for <see cref="LatticeOptions.DiagnosticsCacheTtl"/>.
/// Not part of the public API — callers use <see cref="ILattice.DiagnoseAsync"/>.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
[Alias(TypeAliases.ILatticeStats)]
public interface ILatticeStats : IGrainWithStringKey
{
    /// <summary>
    /// Returns the current <see cref="TreeDiagnosticReport"/>. Concurrent callers
    /// share the same in-flight fan-out; repeated callers within the configured
    /// cache TTL receive the cached report. When <paramref name="deep"/> is
    /// <c>true</c> the shard fan-out aggregates tombstone counts (walks the
    /// leaf chain of each shard).
    /// </summary>
    Task<TreeDiagnosticReport> GetReportAsync(bool deep, CancellationToken cancellationToken);

    /// <summary>
    /// Records a successful adaptive-split commit in the recent-splits ring buffer.
    /// Fire-and-forget hook invoked by the split coordinator; failures are ignored
    /// so diagnostics never block the split hot path.
    /// </summary>
    Task RecordSplitAsync(int shardIndex, DateTime atUtc);
}
