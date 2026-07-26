namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The outcome of asserting that a configured region's endpoint actually reaches
/// that region's own cluster, produced by
/// <see cref="ILatticeApiMcpRegionIdentityVerifier"/>. Guards against a region
/// that is (mis)configured to a shared or anycast endpoint - for example an Azure
/// Front Door endpoint that latency-routes a call to the nearest region rather
/// than the one the caller targeted - which would silently answer a targeted call
/// from the wrong cluster.
/// </summary>
internal enum RegionIdentityVerdict
{
    /// <summary>
    /// Verification does not apply: the current (local) region, a region with no
    /// advertised cluster id to assert against, or one that serves no state facade
    /// to probe. Treated as routable - there is nothing to contradict.
    /// </summary>
    Skipped,

    /// <summary>
    /// The region's state facade reported a cluster id matching the one advertised
    /// for the region, so the endpoint provably reaches the intended cluster.
    /// </summary>
    Verified,

    /// <summary>
    /// The region's state facade reported a cluster id different from the one
    /// advertised for the region: the endpoint reaches the wrong cluster (a
    /// mis-pointed or anycast endpoint). Fail-closed - the region is not routable.
    /// </summary>
    Mismatch,

    /// <summary>
    /// The region's state facade could not be probed (unreachable or timed out), so
    /// its identity could not be proven. Fail-closed for this attempt, but never
    /// cached, so a later call re-probes once the region recovers.
    /// </summary>
    Unreachable,
}
