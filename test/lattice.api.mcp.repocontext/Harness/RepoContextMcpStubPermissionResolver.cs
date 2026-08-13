namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// A deterministic <see cref="ILatticeApiMcpPermissionResolver"/> stub that
/// returns a fixed <see cref="LatticeApiMcpAccessSet"/> for every credential, so
/// the harness scopes a session's usable groups without seeding a real Auth
/// policy tree or waiting for its change-feed compile step. It replaces the
/// package's <c>AuthAdminMcpPermissionResolver</c> in the harness's service
/// provider.
/// </summary>
internal sealed class RepoContextMcpStubPermissionResolver : ILatticeApiMcpPermissionResolver
{
    private readonly LatticeApiMcpAccessSet _access;

    /// <summary>
    /// Creates a stub resolver that grants every credential exactly
    /// <paramref name="access"/>.
    /// </summary>
    /// <param name="access">The fixed group access set to return.</param>
    public RepoContextMcpStubPermissionResolver(LatticeApiMcpAccessSet access)
        => _access = access;

    /// <inheritdoc />
    public ValueTask<LatticeApiMcpAccessSet> ResolveAsync(
        LatticeCredential credential,
        CancellationToken cancellationToken)
        => new(_access);
}
