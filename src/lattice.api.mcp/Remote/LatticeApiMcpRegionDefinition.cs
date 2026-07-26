namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The immutable definition of one region the server can route to, as assembled
/// at startup from the topology's configuration. It is the single input both the
/// <see cref="ILatticeApiMcpRegionRouter"/> (for validation and discovery) and the
/// per-group routing gRPC invokers are derived from, so discovery and routing can
/// never disagree.
/// </summary>
internal sealed record LatticeApiMcpRegionDefinition
{
    /// <summary>The stable region id a caller targets this region by.</summary>
    public required string RegionId { get; init; }

    /// <summary>The Orleans cluster id the region belongs to, when known; otherwise empty.</summary>
    public string ClusterId { get; init; } = string.Empty;

    /// <summary>Whether this is the default (current) region a call targets when no <c>region</c> is supplied.</summary>
    public required bool IsCurrent { get; init; }

    /// <summary>
    /// The facade groups this region serves, mapped to the endpoint each is
    /// reached at (<see langword="null"/> for a co-hosted, in-silo group). A group
    /// absent from the map is not served by the region.
    /// </summary>
    public required IReadOnlyDictionary<LatticeApiMcpGroup, string?> Groups { get; init; }
}
