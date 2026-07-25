namespace Orleans.Lattice.Api.Region;

/// <summary>
/// The transport-agnostic region discovery contract: enumerates the regions the
/// server can route API calls to (the current cluster plus any reachable,
/// credentialed peer), each with its per-group reachability. It is the single
/// source of truth a binding advertises regions from, so discovery and routing
/// can never disagree - a region is listed only when the server actually has a
/// route to it.
/// </summary>
/// <remarks>
/// Implemented by each binding over its own topology (the MCP server derives the
/// catalog from its configured remote endpoints; a co-hosted binding reports the
/// single current region). The MCP <c>lattice_list_regions</c> tool and any
/// future facade / gRPC surface for the explorer are thin adapters over this
/// contract rather than owning their own region model.
/// </remarks>
public interface ILatticeRegionCatalog
{
    /// <summary>
    /// Enumerates the regions the server can route to, each with per-group
    /// reachability. Fail-closed: a region with no route is omitted. The current
    /// region (<see cref="LatticeRegionDescriptor.IsCurrent"/>) is always present.
    /// </summary>
    /// <param name="cancellationToken">Cancels the enumeration.</param>
    /// <returns>The routable regions, current region first.</returns>
    Task<IReadOnlyList<LatticeRegionDescriptor>> ListRegionsAsync(
        CancellationToken cancellationToken = default);
}
