namespace Orleans.Lattice.Api.State;

/// <summary>
/// Read-only summary of a single B+ tree node, used by the tree-structure
/// query endpoint to render the node graph. The structural fields
/// (<see cref="KeyRangeLow"/>, <see cref="KeyRangeHigh"/>,
/// <see cref="ChildCount"/>, <see cref="SubtreeKeyCount"/>) are populated
/// from the pushed-up topology snapshot.
/// </summary>
[GenerateSerializer]
[Alias(ApiStateTypeAliases.NodeStateSummary)]
[Immutable]
public sealed record NodeStateSummary
{
    /// <summary>Kind of node (shard root, internal, or leaf).</summary>
    [Id(0)] public NodeKind Kind { get; init; }

    /// <summary>
    /// Stable identity of the node within its tree (e.g. the grain key), used
    /// to descend into a sub-path on a follow-up request.
    /// </summary>
    [Id(1)] public required string NodeId { get; init; }

    /// <summary>Physical shard index this node belongs to.</summary>
    [Id(2)] public int ShardIndex { get; init; }

    /// <summary>Depth of this node within its shard subtree (shard root is depth 0).</summary>
    [Id(3)] public int Depth { get; init; }

    /// <summary>
    /// Inclusive low key-range bound owned by this node's subtree, when known.
    /// </summary>
    [Id(4)] public string? KeyRangeLow { get; init; }

    /// <summary>
    /// Exclusive high key-range bound owned by this node's subtree, when known.
    /// </summary>
    [Id(5)] public string? KeyRangeHigh { get; init; }

    /// <summary>Number of immediate children (0 for leaves).</summary>
    [Id(6)] public int ChildCount { get; init; }

    /// <summary>Live keys held in this node's subtree.</summary>
    [Id(7)] public long SubtreeKeyCount { get; init; }

    /// <summary>Tombstoned / expired entries held in this node's subtree.</summary>
    [Id(8)] public long SubtreeTombstoneCount { get; init; }

    /// <summary>Whether this node is currently involved in a split.</summary>
    [Id(9)] public bool SplitInProgress { get; init; }

    /// <summary>
    /// Whether this node has children that were not materialised in the
    /// current (depth- or budget-limited) response, so the client can lazily
    /// expand them with a follow-up request.
    /// </summary>
    [Id(10)] public bool HasMoreChildren { get; init; }

    /// <summary>Immediate children included in this response (may be empty).</summary>
    [Id(11)] public IReadOnlyList<NodeStateSummary> Children { get; init; } = Array.Empty<NodeStateSummary>();
}
