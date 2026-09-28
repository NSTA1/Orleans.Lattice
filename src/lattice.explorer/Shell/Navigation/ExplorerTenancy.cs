using Orleans.Lattice.Explorer.Core.Tenancy;

namespace Orleans.Lattice.Explorer.Shell.Navigation;

/// <summary>
/// The navigation chrome's view of tenancy: whether addresses carry a tenant
/// root, which tenant is active, which tenants the caller may reach, and the
/// operator-gated switch. A thin, fail-closed reading of Core's tenancy seams.
/// </summary>
/// <remarks>
/// Tenancy is <em>on</em> exactly when the head registered Core's tenant view
/// and it is active. With it off there is no tenant node anywhere, and every
/// tenant question answers "none". Switching never widens what a caller sees:
/// it goes through <see cref="IExplorerTenantSwitcher"/>, which the cluster
/// re-enforces.
/// </remarks>
internal sealed class ExplorerTenancy
{
    private readonly IExplorerTenantView? _view;
    private readonly IExplorerTenantSwitcher? _switcher;
    private readonly IExplorerAccessibleTenantSource? _tenants;

    /// <summary>Reads tenancy from whichever of Core's seams the head registered.</summary>
    /// <param name="view">The tenant view, or <see langword="null"/> when tenancy is not registered.</param>
    /// <param name="switcher">The operator-gated switcher, or <see langword="null"/>.</param>
    /// <param name="tenants">The accessible-tenant source, or <see langword="null"/>.</param>
    public ExplorerTenancy(
        IExplorerTenantView? view = null,
        IExplorerTenantSwitcher? switcher = null,
        IExplorerAccessibleTenantSource? tenants = null)
    {
        _view = view;
        _switcher = switcher;
        _tenants = tenants;
    }

    /// <summary>Whether tenancy is on, so tenant-scoped addresses carry a tenant root.</summary>
    public bool IsActive => _view is { IsActive: true };

    /// <summary>The active tenant's id, or <see langword="null"/> when tenancy is off or none is established.</summary>
    public string? ActiveTenant => IsActive ? _view!.ActiveTenant?.Value : null;

    /// <summary>The tenants the caller may scope to, best first; empty when tenancy is off or none is known.</summary>
    /// <param name="cancellationToken">Cancels the lookup.</param>
    public async ValueTask<IReadOnlyList<string>> GetAccessibleTenantsAsync(CancellationToken cancellationToken = default)
    {
        if (!IsActive || _tenants is null)
        {
            return ActiveTenant is { } active ? [active] : [];
        }

        var tenants = await _tenants.GetAccessibleTenantsAsync(cancellationToken).ConfigureAwait(false);
        return [.. tenants.Select(tenant => tenant.Value)];
    }

    /// <summary>
    /// Makes <paramref name="tenant"/> the active tenant, through the
    /// operator-gated switcher. Returns <see langword="true"/> when it is (already,
    /// or now) active, and <see langword="false"/> when the switch was refused or
    /// tenancy is off.
    /// </summary>
    /// <param name="tenant">The tenant id.</param>
    /// <param name="cancellationToken">Cancels the operator validation.</param>
    public async ValueTask<bool> TrySwitchAsync(string tenant, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(tenant);

        if (!IsActive)
        {
            return false;
        }

        if (string.Equals(ActiveTenant, tenant, StringComparison.Ordinal))
        {
            return true;
        }

        return _switcher is not null
            && await _switcher.SwitchTenantAsync(new ExplorerTenantId(tenant), cancellationToken).ConfigureAwait(false);
    }
}
