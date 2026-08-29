namespace Orleans.Lattice.Api.Telemetry;

/// <summary>
/// The fallback <see cref="ITenantContextResolver"/> the telemetry facade uses when
/// a host registered no resolver at all, resolving every caller to the reserved
/// default tenant.
/// </summary>
/// <remarks>
/// <para>
/// The core library registers its own no-op resolver, so this is reached only by a
/// host that hosts the facade without the core library's registration. It matches
/// that no-op exactly - the reserved default tenant, resolved synchronously with no
/// allocation - so a facade in a minimal host behaves identically to one in a
/// tenancy-off cluster rather than failing to construct.
/// </para>
/// <para>
/// This is deliberately not a fail-open: the default tenant is a real, bounded
/// scope whose matcher excludes the platform sentinel, so a caller resolved through
/// it still cannot see platform-internal or other tenants' series.
/// </para>
/// </remarks>
internal sealed class NullTelemetryTenantContext : ITenantContextResolver
{
    /// <summary>The shared instance; the type carries no state.</summary>
    public static readonly NullTelemetryTenantContext Instance = new();

    private NullTelemetryTenantContext()
    {
    }

    /// <inheritdoc />
    public ValueTask<TenantId> ResolveCurrentAsync(CancellationToken cancellationToken = default) =>
        new(TenantId.Default);

    /// <inheritdoc />
    public bool TryResolveCurrent(out TenantId tenant)
    {
        tenant = TenantId.Default;
        return true;
    }
}
