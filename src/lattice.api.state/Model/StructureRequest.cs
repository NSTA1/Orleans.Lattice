namespace Orleans.Lattice.Api.State;

/// <summary>
/// Request for the tree-structure query endpoint
/// (<see cref="ILatticeStateQuery.GetTreeStructureAsync"/>). Selects a tree
/// (optionally scoped to a single shard or descended into a named internal
/// node) and bounds the returned node graph by a depth and node-count budget
/// so a structure read of a large tree never returns an unbounded response.
/// </summary>
/// <remarks>
/// Whole-tree assembly issues one structural read per shard root
/// (O(shards)); descending into a sub-path costs O(visited internal nodes)
/// and never reads unrelated shards or fans out to leaves.
/// </remarks>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.StructureRequest)]
[Immutable]
public sealed record StructureRequest
{
    /// <summary>Default depth budget used when <see cref="DepthLimit"/> is unset.</summary>
    public const int DefaultDepthLimit = 4;

    /// <summary>Largest depth budget honoured; larger values are clamped down.</summary>
    public const int MaxDepthLimit = 64;

    /// <summary>Default node budget used when <see cref="MaxNodes"/> is unset.</summary>
    public const int DefaultMaxNodes = 1000;

    /// <summary>Largest node budget honoured; larger values are clamped down.</summary>
    public const int MaxNodeBudget = 100_000;

    /// <summary>Logical tree identifier to read the structure of.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// When set, restricts the response to the single shard with this index
    /// instead of assembling every shard root.
    /// </summary>
    [Id(1)] public int? ShardIndex { get; init; }

    /// <summary>
    /// When set, descends into the internal node with this id (the
    /// <see cref="NodeStateSummary.NodeId"/> returned by a prior, depth-limited
    /// response) and returns only that subtree, without re-reading unrelated
    /// shards.
    /// </summary>
    [Id(2)] public string? SubPathNodeId { get; init; }

    /// <summary>
    /// Maximum depth of internal-node expansion. Values below <c>0</c> fall
    /// back to <see cref="DefaultDepthLimit"/>; values above
    /// <see cref="MaxDepthLimit"/> are clamped to it.
    /// </summary>
    [Id(3)] public int DepthLimit { get; init; } = DefaultDepthLimit;

    /// <summary>
    /// Maximum number of nodes materialised across the whole response. Values
    /// below <c>1</c> fall back to <see cref="DefaultMaxNodes"/>; values above
    /// <see cref="MaxNodeBudget"/> are clamped to it. When the budget is
    /// exhausted, deeper nodes are omitted and their parent is flagged
    /// <see cref="NodeStateSummary.HasMoreChildren"/>.
    /// </summary>
    [Id(4)] public int MaxNodes { get; init; } = DefaultMaxNodes;

    /// <summary>The effective, clamped depth limit derived from <see cref="DepthLimit"/>.</summary>
    public int EffectiveDepthLimit => DepthLimit switch
    {
        < 0 => DefaultDepthLimit,
        > MaxDepthLimit => MaxDepthLimit,
        _ => DepthLimit,
    };

    /// <summary>The effective, clamped node budget derived from <see cref="MaxNodes"/>.</summary>
    public int EffectiveMaxNodes => MaxNodes switch
    {
        < 1 => DefaultMaxNodes,
        > MaxNodeBudget => MaxNodeBudget,
        _ => MaxNodes,
    };
}
