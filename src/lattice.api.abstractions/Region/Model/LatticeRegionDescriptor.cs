namespace Orleans.Lattice.Api.Region;

/// <summary>
/// A region the server can route API calls to: the current cluster the binding
/// is co-hosted with (or primarily wired to), plus any reachable, credentialed
/// peer. Returned by <see cref="ILatticeRegionCatalog.ListRegionsAsync"/> so a
/// caller can discover the regions it may target and, per region, which facade
/// groups are reachable there.
/// </summary>
/// <remarks>
/// Discovery is fail-closed: a region is listed only when the server actually
/// has a route to it, and <see cref="Groups"/> reports per-group reachability so
/// a caller never targets a group a region does not serve. The contract is
/// transport-agnostic and shared by every binding, so the same descriptor backs
/// the MCP <c>lattice_list_regions</c> tool today and a facade / gRPC surface for
/// the explorer later.
/// </remarks>
[GenerateSerializer]
[Alias(ApiRegionTypeAliases.LatticeRegionDescriptor)]
[Immutable]
public sealed record LatticeRegionDescriptor
{
    /// <summary>
    /// The stable region id a caller targets this region by (the value passed as
    /// the optional per-call <c>region</c> selector). Unique within the catalog.
    /// </summary>
    [Id(0)] public required string RegionId { get; init; }

    /// <summary>
    /// The Orleans cluster id the region's silo belongs to, when known; the empty
    /// string when the server did not resolve one.
    /// </summary>
    [Id(1)] public string ClusterId { get; init; } = string.Empty;

    /// <summary>
    /// <see langword="true"/> for the current (default) region a call targets
    /// when no <c>region</c> is supplied; <see langword="false"/> for a peer.
    /// </summary>
    [Id(2)] public required bool IsCurrent { get; init; }

    /// <summary>
    /// The per-group reachability within this region, one entry per facade group,
    /// so a caller can tell which groups the region serves before targeting it.
    /// </summary>
    [Id(3)] public IReadOnlyList<LatticeRegionGroupReachability> Groups { get; init; }
        = Array.Empty<LatticeRegionGroupReachability>();
}
