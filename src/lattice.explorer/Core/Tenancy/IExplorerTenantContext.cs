namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// Holds the Explorer session's active tenant and the visibility the caller has
/// requested. It is the client-side counterpart to the cluster's ambient active
/// tenant context: the head populates <see cref="ActiveTenant"/> from the
/// authenticated session (a tenant claim, cluster info, or an operator's tenant
/// picker), and the operator all-tenant toggle sets
/// <see cref="RequestedVisibility"/>. <see cref="IExplorerTenantView"/> reads both
/// and validates any all-tenant request against the platform-operator gate.
/// </summary>
/// <remarks>
/// Mutable session state, scoped per Blazor circuit so each connection carries its
/// own active tenant and requested scope. Requesting
/// <see cref="ExplorerTenantVisibility.AllTenants"/> here is only a request: the
/// view honours it solely for a validated platform operator and otherwise falls
/// back, fail-closed, to the active tenant.
/// </remarks>
public interface IExplorerTenantContext
{
    /// <summary>
    /// The caller's active tenant, or <see langword="null"/> when none has been
    /// established. When the view is active and no active tenant is set, the
    /// default (active-tenant) scope reveals nothing - the fail-closed default.
    /// </summary>
    ExplorerTenantId? ActiveTenant { get; set; }

    /// <summary>
    /// The visibility the caller has requested. Defaults to
    /// <see cref="ExplorerTenantVisibility.ActiveTenant"/>; an operator sets
    /// <see cref="ExplorerTenantVisibility.AllTenants"/> to request the cross-tenant
    /// view, which the view grants only after validating the caller as an operator.
    /// </summary>
    ExplorerTenantVisibility RequestedVisibility { get; set; }
}
