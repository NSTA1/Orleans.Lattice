namespace Orleans.Lattice.Api.Replication.Tests;

/// <summary>
/// A default-tenant <see cref="ITenantContextResolver"/> for facade construction in
/// tests: it resolves the reserved default tenant synchronously, exactly as the
/// core no-op resolver does when the tenancy add-on is absent, so a facade test
/// that is not about tenancy sees the caller's bare tree name unchanged.
/// </summary>
internal sealed class DefaultTenantContextResolver : ITenantContextResolver
{
    public bool TryResolveCurrent(out TenantId tenant)
    {
        tenant = TenantId.Default;
        return true;
    }

    public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
        new(TenantId.Default);
}
