namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>A permission resolver returning a fixed facade-group access set.</summary>
internal sealed class FixedPermissionResolver(LatticeApiMcpAccessSet access) : ILatticeApiMcpPermissionResolver
{
    public ValueTask<LatticeApiMcpAccessSet> ResolveAsync(LatticeCredential credential, CancellationToken cancellationToken)
        => new(access);
}
