namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// The visibility scope the Explorer applies when listing trees and data. It is
/// the explicit, fail-closed channel by which a platform operator opts out of the
/// per-tenant default and into the cross-tenant, all-tenant view.
/// </summary>
/// <remarks>
/// There is no ambient all-tenant view. The default,
/// <see cref="ActiveTenant"/>, always resolves to the caller's active tenant
/// (<see cref="IExplorerTenantContext.ActiveTenant"/>), so a tenant - and a
/// platform operator that has not asserted otherwise - sees only its own trees.
/// To see across tenants the caller must explicitly request
/// <see cref="AllTenants"/>, which <see cref="IExplorerTenantView"/> honours only
/// when the caller validates as a platform operator; an unvalidated request fails
/// closed to <see cref="ActiveTenant"/>.
/// </remarks>
public enum ExplorerTenantVisibility
{
    /// <summary>
    /// The default scope: show only the caller's own active tenant's trees and
    /// data. Never reveals another tenant's records.
    /// </summary>
    ActiveTenant = 0,

    /// <summary>
    /// The explicit cross-tenant scope: show every tenant's trees and data.
    /// Honoured only when the caller validates as a platform operator; otherwise
    /// the view falls back, fail-closed, to <see cref="ActiveTenant"/>.
    /// </summary>
    AllTenants = 1,
}
