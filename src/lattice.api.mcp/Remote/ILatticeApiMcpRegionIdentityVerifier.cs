namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Asserts that a configured peer region's endpoint actually reaches that region's
/// own cluster before a call is routed to it, so a targeted call can never be
/// silently answered by the wrong cluster (the failure mode when a region is
/// pointed at a shared or anycast endpoint - for example an Azure Front Door
/// endpoint that latency-routes to the nearest region rather than the targeted
/// one). Opt-in: registered only when
/// <see cref="LatticeApiMcpRemoteOptions.VerifyRegionIdentity"/> is set, so a
/// deployment that does not enable it keeps the byte-for-byte-unchanged default
/// routing path with no verification cost.
/// </summary>
/// <remarks>
/// Verification is fail-closed and cheap after the first probe: each region is
/// probed at most once (a <see cref="RegionIdentityVerdict.Verified"/> or
/// <see cref="RegionIdentityVerdict.Mismatch"/> verdict is a stable property of the
/// configured endpoint and is cached for the process lifetime), while an
/// <see cref="RegionIdentityVerdict.Unreachable"/> verdict is never cached so a
/// transient outage does not permanently demote a region.
/// </remarks>
internal interface ILatticeApiMcpRegionIdentityVerifier
{
    /// <summary>
    /// Resolves the identity verdict for <paramref name="regionId"/>, probing the
    /// region's state facade (under its region scope and the ambient forwarded
    /// caller credential) once and caching the stable outcome.
    /// </summary>
    /// <param name="regionId">The peer region id to verify.</param>
    /// <param name="cancellationToken">
    /// Cancels the caller's wait for the verdict. It does not cancel the shared
    /// underlying probe, so one caller cancelling never poisons the cached result
    /// another caller is awaiting.
    /// </param>
    /// <returns>The region's identity verdict.</returns>
    ValueTask<RegionIdentityVerdict> VerifyAsync(
        string regionId, CancellationToken cancellationToken = default);
}
