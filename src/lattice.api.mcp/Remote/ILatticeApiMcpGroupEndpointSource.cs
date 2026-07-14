namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Optional seam that supplies the served endpoint for a
/// <see cref="LatticeApiMcpGroup"/> so the <c>lattice_capabilities</c> report can
/// tell a caller <b>where</b> each group is reached. Registered only by the
/// remote-host topology; absent under the in-silo topology, where every group is
/// co-hosted and no endpoint is advertised.
/// </summary>
internal interface ILatticeApiMcpGroupEndpointSource
{
    /// <summary>
    /// Returns the served endpoint for <paramref name="group"/>, or
    /// <see langword="null"/> when the group is not served remotely (or has no
    /// configured endpoint).
    /// </summary>
    /// <param name="group">The facade group to resolve the endpoint for.</param>
    string? EndpointFor(LatticeApiMcpGroup group);
}
