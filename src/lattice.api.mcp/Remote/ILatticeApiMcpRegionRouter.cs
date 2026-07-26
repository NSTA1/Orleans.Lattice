using Orleans.Lattice.Api.Region;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Resolves the optional per-call <c>region</c> selector to a validated route for
/// a specific facade group, and is the single source of truth the region catalog
/// and the routing gRPC invokers are both derived from, so discovery and routing
/// can never disagree. Registered in both the in-silo topology (a single current
/// region) and the remote-host topology (the current region plus each configured
/// peer).
/// </summary>
/// <remarks>
/// Resolution is fail-closed: an unknown region, or a region that does not serve
/// the requested group, yields a rejected <see cref="LatticeApiMcpRegionRoute"/>
/// the tool seam surfaces as a clean typed fault - never a route to the wrong
/// region and never a leaked exception.
/// </remarks>
internal interface ILatticeApiMcpRegionRouter
{
    /// <summary>The id of the default (current) region a call targets when no <c>region</c> is supplied.</summary>
    string DefaultRegionId { get; }

    /// <summary>
    /// Resolves <paramref name="requestedRegionId"/> for <paramref name="group"/>.
    /// A <see langword="null"/> or empty request routes to the default region; a
    /// value naming a known region that serves the group routes there; anything
    /// else is rejected fail-closed with an actionable fault message.
    /// </summary>
    /// <param name="requestedRegionId">The caller-supplied region selector, or <see langword="null"/>.</param>
    /// <param name="group">The facade group the call belongs to.</param>
    /// <returns>The resolved route, valid or rejected.</returns>
    LatticeApiMcpRegionRoute Resolve(string? requestedRegionId, LatticeApiMcpGroup group);

    /// <summary>
    /// The static per-region descriptors backing region discovery, current region
    /// first. Cluster ids known only at runtime (the current in-silo region) may
    /// be empty and are enriched by <see cref="ILatticeRegionCatalog"/>.
    /// </summary>
    IReadOnlyList<LatticeRegionDescriptor> Snapshot();
}
