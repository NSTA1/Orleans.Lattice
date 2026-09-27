namespace Orleans.Lattice.Api.Apps.Tests;

/// <summary>A tenant resolver resolving a configurable tenant synchronously or asynchronously.</summary>
internal sealed class ConfigurableTenantResolver : ITenantContextResolver
{
    public TenantId Tenant { get; set; } = TenantId.Default;

    public bool Synchronous { get; set; } = true;

    public int AsyncResolutions { get; private set; }

    public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default)
    {
        AsyncResolutions++;
        return new ValueTask<TenantId>(Tenant);
    }

    public bool TryResolveCurrent(out TenantId tenant)
    {
        tenant = Tenant;
        return Synchronous;
    }
}
