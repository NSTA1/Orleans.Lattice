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

    /// <summary>
    /// Enumerates the trees registered in the cluster as a paged, deterministic
    /// catalog ordered by tree id. Each entry carries the tree's lifecycle
    /// state, shard count, alias transparency, and effective configuration.
    /// Reserved internal system trees are hidden unless
    /// <see cref="CatalogRequest.IncludeSystemTrees"/> is set.
    /// </summary>
    /// <param name="request">Paging and filtering options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<TreeCatalogPage> ListTreesAsync(
        CatalogRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Enumerates the materialised views known to the cluster as a paged,
    /// deterministic catalog ordered by view name. Each entry carries the
    /// view's source tree and (when
    /// <see cref="CatalogRequest.IncludeViewStats"/> is set) its apply lag and
    /// materialised entry count.
    /// </summary>
    /// <param name="request">Paging and filtering options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<ViewCatalogPage> ListViewsAsync(
        CatalogRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the structural node graph of the given tree - shard roots,
    /// internal nodes, and leaves - each annotated with node kind, key-range
    /// bounds, live/tombstone counts, fan-out, and depth, or a typed
    /// not-found result when the tree does not exist. Built from the pushed-up
    /// per-shard topology snapshots: a whole-tree read issues one structural
    /// read per shard root (O(shards)) and never fans out to leaves. The
    /// response is bounded by the request's depth and node-count budget, with
    /// <see cref="NodeStateSummary.HasMoreChildren"/> markers so a client can
    /// lazily expand truncated subtrees via
    /// <see cref="StructureRequest.SubPathNodeId"/>.
    /// </summary>
    /// <param name="request">Scope (tree, optional shard / sub-path) and depth / node budget.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<TreeStructureResult> GetTreeStructureAsync(
        StructureRequest request,
        CancellationToken cancellationToken = default);
}
