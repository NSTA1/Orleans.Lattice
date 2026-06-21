namespace Orleans.Lattice.Api.State;

/// <summary>
/// Outcome of a state-query lookup that can fail to resolve its target.
/// </summary>
public enum StateQueryStatus
{
    /// <summary>The target was found and the payload is populated.</summary>
    Found = 0,

    /// <summary>The requested tree does not exist.</summary>
    TreeNotFound = 1,
}

/// <summary>
/// Result of <see cref="ILatticeStateQuery.GetTreeSummaryAsync"/>: either a
/// populated <see cref="TreeStateSummary"/> or a typed not-found, so an
/// unknown tree never surfaces an Orleans-internal exception to callers.
/// </summary>
public sealed record TreeSummaryResult
{
    /// <summary>Lookup outcome.</summary>
    public StateQueryStatus Status { get; init; }

    /// <summary>The tree id that was queried.</summary>
    public required string TreeId { get; init; }

    /// <summary>The summary when <see cref="Status"/> is <see cref="StateQueryStatus.Found"/>.</summary>
    public TreeStateSummary? Summary { get; init; }

    /// <summary>Builds a found result.</summary>
    public static TreeSummaryResult Found(TreeStateSummary summary)
    {
        ArgumentNullException.ThrowIfNull(summary);
        return new TreeSummaryResult { Status = StateQueryStatus.Found, TreeId = summary.TreeId, Summary = summary };
    }

    /// <summary>Builds a not-found result for the given tree id.</summary>
    public static TreeSummaryResult NotFound(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return new TreeSummaryResult { Status = StateQueryStatus.TreeNotFound, TreeId = treeId };
    }
}

/// <summary>
/// Result of <see cref="ILatticeStateQuery.GetShardSummariesAsync"/>: either
/// the per-shard summaries or a typed not-found.
/// </summary>
public sealed record ShardSummariesResult
{
    /// <summary>Lookup outcome.</summary>
    public StateQueryStatus Status { get; init; }

    /// <summary>The tree id that was queried.</summary>
    public required string TreeId { get; init; }

    /// <summary>The per-shard summaries (empty when not found), ordered by shard index.</summary>
    public IReadOnlyList<ShardStateSummary> Shards { get; init; } = Array.Empty<ShardStateSummary>();

    /// <summary>Builds a found result.</summary>
    public static ShardSummariesResult Found(string treeId, IReadOnlyList<ShardStateSummary> shards)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(shards);
        return new ShardSummariesResult { Status = StateQueryStatus.Found, TreeId = treeId, Shards = shards };
    }

    /// <summary>Builds a not-found result for the given tree id.</summary>
    public static ShardSummariesResult NotFound(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return new ShardSummariesResult { Status = StateQueryStatus.TreeNotFound, TreeId = treeId };
    }
}

/// <summary>
/// Result of <see cref="ILatticeStateQuery.GetTreeStructureAsync"/>: either a
/// bounded, depth-limited node graph (one entry per shard root for a
/// whole-tree read, or a single subtree for a sub-path descent) or a typed
/// not-found.
/// </summary>
public sealed record TreeStructureResult
{
    /// <summary>Lookup outcome.</summary>
    public StateQueryStatus Status { get; init; }

    /// <summary>The tree id that was queried.</summary>
    public required string TreeId { get; init; }

    /// <summary>
    /// The root nodes of the response, in deterministic key-range order. For a
    /// whole-tree read this is the per-shard root nodes; for a sub-path descent
    /// it is the single requested subtree. Empty when not found.
    /// </summary>
    public IReadOnlyList<NodeStateSummary> Roots { get; init; } = Array.Empty<NodeStateSummary>();

    /// <summary>
    /// Whether the node-count budget was exhausted, so some subtrees were
    /// truncated and can be re-read with a sub-path descent. The per-node
    /// <see cref="NodeStateSummary.HasMoreChildren"/> flags identify exactly
    /// which nodes were truncated.
    /// </summary>
    public bool Truncated { get; init; }

    /// <summary>Builds a found result.</summary>
    public static TreeStructureResult Found(string treeId, IReadOnlyList<NodeStateSummary> roots, bool truncated)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        ArgumentNullException.ThrowIfNull(roots);
        return new TreeStructureResult
        {
            Status = StateQueryStatus.Found,
            TreeId = treeId,
            Roots = roots,
            Truncated = truncated,
        };
    }

    /// <summary>Builds a not-found result for the given tree id.</summary>
    public static TreeStructureResult NotFound(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        return new TreeStructureResult { Status = StateQueryStatus.TreeNotFound, TreeId = treeId };
    }
}
