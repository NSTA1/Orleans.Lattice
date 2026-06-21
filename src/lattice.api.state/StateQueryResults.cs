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
