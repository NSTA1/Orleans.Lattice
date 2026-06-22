namespace Orleans.Lattice.Explorer.Core.Topology;

/// <summary>An edge between a parent and child in the positioned graph.</summary>
public sealed record GraphEdge
{
    /// <summary>The parent node id.</summary>
    public required string FromId { get; init; }

    /// <summary>The child node id.</summary>
    public required string ToId { get; init; }

    /// <summary>The parent's column.</summary>
    public double FromColumn { get; init; }

    /// <summary>The parent's level.</summary>
    public int FromLevel { get; init; }

    /// <summary>The child's column.</summary>
    public double ToColumn { get; init; }

    /// <summary>The child's level.</summary>
    public int ToLevel { get; init; }
}
