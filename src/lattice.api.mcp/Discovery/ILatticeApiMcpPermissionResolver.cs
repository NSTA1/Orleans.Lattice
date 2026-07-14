namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Resolves the set of facade groups a caller may use from their resolved
/// <see cref="LatticeCredential"/>. The discovery core consults this once per
/// session to scope the advertised tool set and the <c>lattice_capabilities</c>
/// report to the caller's effective permissions. The default implementation
/// reuses the <c>Api.Auth</c> effective-permissions surface; a host can register
/// its own implementation to source group access from elsewhere.
/// </summary>
internal interface ILatticeApiMcpPermissionResolver
{
    /// <summary>
    /// Resolves the groups <paramref name="credential"/> may use. Returns
    /// <see cref="LatticeApiMcpAccessSet.None"/> when the caller holds no
    /// matching grant. Implementations are fail-closed: any resolution failure
    /// yields an empty set rather than an open one.
    /// </summary>
    /// <param name="credential">The resolved caller credential.</param>
    /// <param name="cancellationToken">Cancels the resolution.</param>
    ValueTask<LatticeApiMcpAccessSet> ResolveAsync(
        LatticeCredential credential,
        CancellationToken cancellationToken);
}
