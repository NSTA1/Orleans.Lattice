namespace Orleans.Lattice.Api.Region;

/// <summary>
/// The reachability of a single Lattice API facade group within one region, as
/// reported by <see cref="ILatticeRegionCatalog"/>. A group is
/// <see cref="Available"/> in a region when the server holds a route (and, for a
/// remote peer, the credentials) to reach that group there, so a caller can tell
/// which groups a region actually serves before targeting it.
/// </summary>
/// <remarks>
/// This is a transport-agnostic contract type shared by every binding (the MCP
/// <c>lattice_list_regions</c> tool today, a facade / gRPC surface for the
/// explorer later), so <see cref="Group"/> is the stable lower-case group name
/// (<c>state</c>, <c>data</c>, <c>backup</c>, <c>auth</c>, <c>telemetry</c>,
/// <c>replication</c>) rather than a binding-specific enum.
/// </remarks>
[GenerateSerializer]
[Alias(ApiRegionTypeAliases.LatticeRegionGroupReachability)]
[Immutable]
public sealed record LatticeRegionGroupReachability
{
    /// <summary>
    /// The stable lower-case facade group name this entry describes (for example
    /// <c>state</c> or <c>data</c>).
    /// </summary>
    [Id(0)] public required string Group { get; init; }

    /// <summary>
    /// <see langword="true"/> when the region serves the group (the server has a
    /// route to it there); <see langword="false"/> when the region does not
    /// expose the group, in which case a call targeting it fails closed.
    /// </summary>
    [Id(1)] public required bool Available { get; init; }

    /// <summary>
    /// The endpoint the group is served from in this region, when the region is a
    /// remote peer reached over a named endpoint; <see langword="null"/> for a
    /// co-hosted (in-silo) region or a group the region does not serve.
    /// </summary>
    [Id(2)] public string? Endpoint { get; init; }
}
