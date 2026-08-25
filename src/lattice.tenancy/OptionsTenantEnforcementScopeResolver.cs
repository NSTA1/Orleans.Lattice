using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The default <see cref="ITenantEnforcementScopeResolver"/>: resolves every
/// tenant to the cluster-wide <see cref="TenantUsageAccountingOptions.DefaultEnforcementScope"/>.
/// A per-tenant override can replace this registration without touching the
/// admission controller.
/// </summary>
internal sealed class OptionsTenantEnforcementScopeResolver(
    IOptionsMonitor<TenantUsageAccountingOptions> options) : ITenantEnforcementScopeResolver
{
    private readonly IOptionsMonitor<TenantUsageAccountingOptions> _options =
        options ?? throw new ArgumentNullException(nameof(options));

    /// <inheritdoc />
    public TenantEnforcementScope Resolve(TenantId tenant) => _options.CurrentValue.DefaultEnforcementScope;
}
