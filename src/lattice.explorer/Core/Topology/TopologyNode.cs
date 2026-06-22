using Orleans.Lattice.Api.State;

namespace Orleans.Lattice.Explorer.Core.Topology;

/// <summary>
/// A node in the explorer's view of a tree's structure, projected from the
/// state-API <see cref="NodeStateSummary"/>. Children are carried as a nested
/// list so layout and leaf-collapsing operate on a plain tree.
/// </summary>
public sealed record TopologyNode
{
    /// <summary>The node's opaque id, stable for lazy expansion.</summary>
    public required string NodeId { get; init; }

    /// <summary>Whether this is a shard root, an internal node, or a leaf.</summary>
    public NodeKind Kind { get; init; }

    /// <summary>The shard this node belongs to.</summary>
    public int ShardIndex { get; init; }

    /// <summary>The node's depth within its shard.</summary>
    public int Depth { get; init; }

    /// <summary>The number of children the node has in the tree.</summary>
    public int ChildCount { get; init; }

    /// <summary>The number of live keys beneath this node. Drives load colouring.</summary>
    public long SubtreeKeyCount { get; init; }

    /// <summary>The number of tombstones beneath this node.</summary>
    public long SubtreeTombstoneCount { get; init; }

    /// <summary><see langword="true"/> when the node is mid-split.</summary>
    public bool SplitInProgress { get; init; }

    /// <summary>
    /// <see langword="true"/> when the node has children that were not included
    /// in this response and can be lazily expanded.
    /// </summary>
    public bool HasMoreChildren { get; init; }

    /// <summary>The node's fetched children.</summary>
    public IReadOnlyList<TopologyNode> Children { get; init; } = Array.Empty<TopologyNode>();

    /// <summary>Projects a state-API <see cref="NodeStateSummary"/> tree into <see cref="TopologyNode"/>s.</summary>
    public static TopologyNode From(NodeStateSummary summary)
    {
        ArgumentNullException.ThrowIfNull(summary);

        var children = summary.Children.Count == 0
            ? Array.Empty<TopologyNode>()
            : summary.Children.Select(From).ToArray();

        return new TopologyNode
        {
            NodeId = summary.NodeId,
            Kind = summary.Kind,
            ShardIndex = summary.ShardIndex,
            Depth = summary.Depth,
            ChildCount = summary.ChildCount,
            SubtreeKeyCount = summary.SubtreeKeyCount,
            SubtreeTombstoneCount = summary.SubtreeTombstoneCount,
            SplitInProgress = summary.SplitInProgress,
            HasMoreChildren = summary.HasMoreChildren,
            Children = children,
        };
    }
}
