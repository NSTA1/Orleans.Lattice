namespace Orleans.Lattice.Explorer.Core.Topology;

/// <summary>
/// The result of a structure fetch: the root nodes returned for a tree (or for a
/// lazily-expanded subtree) and whether the response was budget-truncated.
/// </summary>
public sealed record TopologyFetch
{
    /// <summary>The roots returned by the state API for this request.</summary>
    public IReadOnlyList<TopologyNode> Roots { get; init; } = Array.Empty<TopologyNode>();

    /// <summary>
    /// <see langword="true"/> when the depth or node budget truncated the
    /// response, so deeper nodes exist than were returned.
    /// </summary>
    public bool Truncated { get; init; }
}
