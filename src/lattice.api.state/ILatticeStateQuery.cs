namespace Orleans.Lattice.Api.State;

/// <summary>
/// Transport-agnostic read facade over a cluster's lattice state. Every
/// transport binding (the gRPC service now, a future MCP surface) is a thin
/// adapter over this single surface, so the read semantics are written and
/// tested once and no transport concern leaks into the query logic.
/// </summary>
/// <remarks>
/// The facade is read-only: it never mutates tree data or configuration. It
/// aggregates the metadata the core library already exposes (the tree
/// registry, <c>DiagnoseAsync</c>, per-shard digests) and performs a bounded
/// number of grain calls per request - O(1) for a tree-level summary.
/// </remarks>
internal interface ILatticeStateQuery
{
    /// <summary>
    /// Returns a point-in-time summary of the given tree, or a typed
    /// not-found result when the tree does not exist.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="deep">
    /// When <see langword="true"/>, includes tombstone counts (a more
    /// expensive read); when <see langword="false"/>, tombstone counts are
    /// reported as <c>0</c>.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<TreeSummaryResult> GetTreeSummaryAsync(
        string treeId,
        bool deep = true,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the per-shard summaries for the given tree, ordered by shard
    /// index, or a typed not-found result when the tree does not exist.
    /// </summary>
    /// <param name="treeId">Logical tree identifier.</param>
    /// <param name="deep">Whether to include tombstone counts.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<ShardSummariesResult> GetShardSummariesAsync(
        string treeId,
        bool deep = true,
        CancellationToken cancellationToken = default);
}
