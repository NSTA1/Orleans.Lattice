using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Navigation;

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
    private readonly ShellCaller _caller;
    private bool _operatorVerdict;
    private string? _verdictTenant;
    private ShellCallerKey _verdictCaller;

    /// <summary>Reads tenancy from whichever of Core's seams the head registered.</summary>
    /// <param name="view">The tenant view, or <see langword="null"/> when tenancy is not registered.</param>
    /// <param name="switcher">The operator-gated switcher, or <see langword="null"/>.</param>
    /// <param name="tenants">The accessible-tenant source, or <see langword="null"/>.</param>
    /// <param name="caller">The circuit's caller, which the operator verdict is filed under, or <see langword="null"/> for none.</param>
    public ExplorerTenancy(
        IExplorerTenantView? view = null,
        IExplorerTenantSwitcher? switcher = null,
        IExplorerAccessibleTenantSource? tenants = null,
        ShellCaller? caller = null)
    {
        _view = view;
        _switcher = switcher;
        _tenants = tenants;
        _caller = caller ?? new ShellCaller();
    }

    /// <summary>
    /// Whether tenancy is on for this caller, so tenant-scoped addresses carry a
    /// tenant root.
    /// </summary>
    /// <remarks>
    /// A caller scoped to the reserved default tenant who is not a platform
    /// operator sees no tenancy chrome at all (tenant-scope.md): for that caller
    /// the tenant root would only ever name <c>default</c>, so addresses stay
    /// plain and <c>/t/default/...</c> canonicalises to the plain form. The
    /// operator verdict is read by <see cref="RefreshAsync"/>, which the layout
    /// awaits before resolving each navigation; until it has answered for the
    /// active tenant the caller is treated as not an operator, which fails closed
    /// to the plain, unscoped view the cluster enforces anyway.
    /// </remarks>
    public bool IsActive => _view is { IsActive: true } && !HidesDefaultTenant;

    /// <summary>
    /// Whether the active tenant is the reserved default and the caller has not proven
    /// operator standing for it. A verdict read for another caller proves nothing.
    /// </summary>
    private bool HidesDefaultTenant =>
        _view!.ActiveTenant is { } active
        && string.Equals(active.Value, ExplorerTenantTrees.DefaultTenantId, StringComparison.Ordinal)
        && !(_operatorVerdict
            && string.Equals(_verdictTenant, active.Value, StringComparison.Ordinal)
            && _verdictCaller == _caller.Current);

    /// <summary>
    /// Refreshes the cached operator verdict that decides whether a caller scoped
    /// to the reserved default tenant sees tenancy chrome. It asks the
    /// operator-gated switcher only when the active tenant is the default one, so
    /// every other navigation costs nothing, and any fault reads as "not an
    /// operator".
    /// </summary>
    /// <param name="cancellationToken">Cancels the operator validation.</param>
    /// <returns>A task that completes once the verdict is current.</returns>
    public async ValueTask RefreshAsync(CancellationToken cancellationToken = default)
    {
        if (_view is not { IsActive: true, ActiveTenant: { } active }
            || !string.Equals(active.Value, ExplorerTenantTrees.DefaultTenantId, StringComparison.Ordinal))
        {
            _operatorVerdict = false;
            _verdictTenant = null;
            return;
        }

        var caller = _caller.Current;
        var verdict = false;
        if (_switcher is not null)
        {
            try
            {
                verdict = await _switcher.IsOperatorAsync(cancellationToken).ConfigureAwait(true);
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                verdict = false;
            }
        }

        _operatorVerdict = verdict;
        _verdictTenant = active.Value;
        _verdictCaller = caller;
    }

    /// <summary>
    /// Whether the tenant this circuit will hold is not known yet: a server prerender
    /// that cannot read the caller's remembered tenant, at an address that does not
    /// name one. The layout sets it on every navigation. While it is set no tenant is
    /// reported active and no switch is offered, so nothing in the chrome is drawn
    /// or addressed under the tenant the prerender would otherwise have guessed.
    /// </summary>
    public bool IsTenantPending { get; set; }

    /// <summary>
    /// The active tenant's id, or <see langword="null"/> when tenancy is off, none is
    /// established, or it is not known yet (<see cref="IsTenantPending"/>).
    /// </summary>
    public string? ActiveTenant => IsActive && !IsTenantPending ? _view!.ActiveTenant?.Value : null;

    /// <summary>
    /// Whether tenancy is on but no tenant is established for this circuit, for
    /// example because resolving the caller's tenant failed. A call made now would
    /// assert no tenant and be served as the reserved default tenant, so nothing
    /// tenant-scoped may be shown until a tenant is established.
    /// </summary>
    public bool IsTenantUnresolved => _view is { IsActive: true, ActiveTenant: null };

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
    /// Whether this caller may switch tenant at all: tenancy is on and the
    /// operator-gated switcher proves the caller's standing. Every fault, a head
    /// without a switcher, tenancy off and a tenant not known yet all read as "may
    /// not", so an affordance
    /// that depends on it fails closed; the switch itself is still re-checked.
    /// </summary>
    /// <param name="cancellationToken">Cancels the operator validation.</param>
    /// <returns><see langword="true"/> only for a proven operator with tenancy on.</returns>
    public async ValueTask<bool> CanSwitchAsync(CancellationToken cancellationToken = default)
    {
        if (!IsActive || IsTenantPending || _switcher is null)
        {
            return false;
        }

        try
        {
            return await _switcher.IsOperatorAsync(cancellationToken).ConfigureAwait(true);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            return false;
        }
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
