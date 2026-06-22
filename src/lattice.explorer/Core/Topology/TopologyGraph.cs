namespace Orleans.Lattice.Explorer.Core.Topology;

/// <summary>
/// A laid-out topology graph: positioned nodes, the edges between them, and the
/// maximum subtree key count used to normalise the load colour scale.
/// </summary>
public sealed record TopologyGraph
{
    /// <summary>The positioned, visible nodes.</summary>
    public IReadOnlyList<PositionedNode> Nodes { get; init; } = Array.Empty<PositionedNode>();

    /// <summary>The parent-to-child edges among visible nodes.</summary>
    public IReadOnlyList<GraphEdge> Edges { get; init; } = Array.Empty<GraphEdge>();

    /// <summary>The number of grid columns the layout spans.</summary>
    public int ColumnCount { get; init; }

    /// <summary>The number of grid levels the layout spans.</summary>
    public int LevelCount { get; init; }

    /// <summary>The busiest visible node's subtree key count, the load-scale ceiling.</summary>
    public long MaxLoad { get; init; }

    /// <summary>An empty graph.</summary>
    public static TopologyGraph Empty { get; } = new();
}
