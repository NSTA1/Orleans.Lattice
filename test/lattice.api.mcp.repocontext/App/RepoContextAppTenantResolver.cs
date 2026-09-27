namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.App;

/// <summary>Resolves the ambient asserted active tenant, or the default tenant when none is asserted.</summary>
internal sealed class RepoContextAppTenantResolver : ITenantContextResolver
{
    public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default)
        => new(LatticeActiveTenantContext.Current ?? TenantId.Default);
}
