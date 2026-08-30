using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Plugins.Tenancy;

namespace Orleans.Lattice.Explorer.Plugins.MyTenant.Workspace;

/// <summary>
/// The Overview surface: the tenant's own descriptor and lifecycle status, the
/// credential's own tenant, and the tenants the caller may switch between.
/// </summary>
public sealed partial class MyTenantWorkspace
{
    private static readonly IReadOnlyList<ExplorerTenantSummary> NoTenants =
        Array.Empty<ExplorerTenantSummary>();

    /// <summary>
    /// The message shown when a cross-tenant visibility request was refused and
    /// degraded, fail-closed, back to the active tenant.
    /// </summary>
    public const string VisibilityDegradedMessage =
        "Cross-tenant visibility was not granted, so this surface stays scoped to your active "
        + "tenant. Only a validated platform operator can widen it.";

    /// <summary>
    /// The message shown when a tenant switch was refused. It is not an error:
    /// switching tenant is an operator action, and a tenant admin has exactly
    /// one tenant.
    /// </summary>
    public const string SwitchDeniedMessage =
        "Switching tenant was not applied: it is a platform-operator action, and nothing changed.";

    private bool _overviewLoaded;

    /// <summary>
    /// The tenant this surface administers, as the cluster describes it, or
    /// <see langword="null"/> before it is read.
    /// </summary>
    public ExplorerTenantDetail? Tenant { get; private set; }

    /// <summary>
    /// The tenant the caller's own credential operates as, which differs from
    /// <see cref="TenantId"/> exactly when a platform operator has switched to
    /// another tenant.
    /// </summary>
    public ExplorerTenantSummary? CredentialTenant { get; private set; }

    /// <summary>
    /// The tenants the caller may access, ascending by id. A caller who
    /// administers one tenant gets one entry, and the switcher stays hidden.
    /// </summary>
    public IReadOnlyList<ExplorerTenantSummary> AccessibleTenants { get; private set; } = NoTenants;

    /// <summary>
    /// Whether to offer the tenant switcher: the caller administers more than
    /// one tenant, so there is somewhere to switch to.
    /// </summary>
    public bool CanSwitchTenant => AccessibleTenants.Count > 1;

    /// <summary>
    /// The visibility the caller has currently requested. An unvalidated
    /// cross-tenant request has already degraded to
    /// <see cref="ExplorerTenantVisibility.ActiveTenant"/> by the time it is read
    /// here, which is the seam's fail-closed contract.
    /// </summary>
    public ExplorerTenantVisibility RequestedVisibility => _domain.RequestedVisibility;

    /// <summary>
    /// Whether the tenant is suspended, so its data-plane operations are being
    /// refused while its trees remain intact.
    /// </summary>
    public bool IsSuspended => Tenant?.Status == ExplorerTenantLifecycle.Suspended;

    /// <summary>
    /// Switches the caller's active tenant, then reloads the whole surface so
    /// every other tab is scoped to the new tenant rather than showing the
    /// previous one's data.
    /// </summary>
    /// <param name="tenantId">The tenant to switch to. Must not be <see langword="null"/> or empty.</param>
    /// <returns><see langword="true"/> when the switch was applied.</returns>
    public async Task<bool> SwitchTenantAsync(string tenantId)
    {
        if (!Allowed || string.IsNullOrEmpty(tenantId) || Busy)
        {
            return false;
        }

        // Only a tenant the cluster already told us the caller may access is
        // offered, so a switch cannot be used to probe for tenants.
        if (!IsAccessible(tenantId))
        {
            Refuse(TenantOperationStatus.Denied, SwitchDeniedMessage);
            return false;
        }

        Busy = true;
        RaiseChanged();
        bool switched;
        try
        {
            switched = await _domain
                .SwitchTenantAsync(new ExplorerTenantId(tenantId))
                .ConfigureAwait(false);
        }
        finally
        {
            Busy = false;
        }

        if (!switched)
        {
            // The switcher re-validates against the operator gate on every call,
            // so a refusal here is the fail-closed answer, not a fault. When the
            // head is running on the placeholder gate that is also the reason,
            // and the Overview surface says so beside this.
            Refuse(TenantOperationStatus.Denied, SwitchDeniedMessage);
            return false;
        }

        await ReloadAsync().ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Requests cross-tenant visibility. Honoured only for a validated platform
    /// operator; an unvalidated request changes nothing and says so, rather than
    /// degrading in silence.
    /// </summary>
    /// <param name="visibility">The visibility to request.</param>
    /// <returns><see langword="true"/> when the request was applied.</returns>
    public async Task<bool> RequestVisibilityAsync(ExplorerTenantVisibility visibility)
    {
        if (!Allowed || Busy)
        {
            return false;
        }

        var applied = await _domain.SetVisibilityAsync(visibility).ConfigureAwait(false);
        if (!applied)
        {
            Refuse(TenantOperationStatus.Denied, VisibilityDegradedMessage);
            return false;
        }

        RaiseChanged();
        return true;
    }

    /// <summary>
    /// Resolves the one tenant every surface is scoped to, and the caller's own
    /// credential tenant beside it.
    /// </summary>
    /// <remarks>
    /// The Explorer's tenant-identity seam is the authority: a platform operator
    /// who switched tenant must see the switched-to tenant, which is what
    /// <see cref="ITenancyDomain.ActiveTenant"/> reports and what
    /// <c>GetCurrentTenantAsync</c> - which answers for the caller's own
    /// credential - deliberately does not. The credential's tenant is the
    /// fallback only when no active tenant has been established at all.
    /// </remarks>
    private async Task LoadIdentityAsync()
    {
        var current = await _domain.Tenants.GetCurrentTenantAsync().ConfigureAwait(false);
        if (current.IsSuccess)
        {
            CredentialTenant = current.Value;
        }
        else
        {
            CredentialTenant = null;
            LastNotice = MyTenantNotice.For(current);
        }

        TenantId = _domain.ActiveTenant?.Value ?? CredentialTenant?.TenantId;

        // Every surface's cached data belongs to whichever tenant was resolved
        // last, so a switch invalidates all of it rather than leaving one tab
        // showing the previous tenant's rows.
        InvalidateTenantScopedState();
    }

    private async Task LoadOverviewAsync(bool force)
    {
        if (!force && _overviewLoaded)
        {
            return;
        }

        _overviewLoaded = true;

        var accessible = await _domain.Tenants.ListAccessibleTenantsAsync().ConfigureAwait(false);
        AccessibleTenants = accessible.IsSuccess && accessible.Value is { } tenants ? tenants : NoTenants;

        if (string.IsNullOrEmpty(TenantId))
        {
            Tenant = null;
            return;
        }

        var detail = await _domain.Tenants.GetTenantAsync(TenantId).ConfigureAwait(false);
        if (detail.IsSuccess)
        {
            Tenant = detail.Value;

            // The detail read already carries the tenant's per-region rows, so
            // the residency plan is seeded here rather than costing the Regions
            // surface a second call on first open.
            if (Tenant is { } loaded)
            {
                Regions.Reset(loaded.Regions);
                _regionsLoaded = true;
            }
        }
        else
        {
            Tenant = null;
            LastNotice = MyTenantNotice.For(detail);
        }
    }

    private bool IsAccessible(string tenantId)
    {
        for (var i = 0; i < AccessibleTenants.Count; i++)
        {
            if (string.Equals(AccessibleTenants[i].TenantId, tenantId, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Drops every surface's tenant-scoped cache, so nothing loaded for one
    /// tenant can survive into another. This is the isolation invariant at the
    /// caching layer: a switch leaves no row behind.
    /// </summary>
    private void InvalidateTenantScopedState()
    {
        _overviewLoaded = false;
        _adminSubjectsLoaded = false;
        _quotaLoaded = false;
        _regionsLoaded = false;
        _grantsLoaded = false;

        Tenant = null;
        AccessibleTenants = NoTenants;
        AdminSubjects = NoSubjects;
        Usage = null;
        ClearGrants();
        Regions.Reset(Array.Empty<ExplorerTenantRegion>());
    }
}
