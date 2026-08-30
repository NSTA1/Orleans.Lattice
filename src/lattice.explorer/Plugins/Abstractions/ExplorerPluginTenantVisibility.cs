namespace Orleans.Lattice.Explorer.Plugins;

/// <summary>
/// The tenant scope the Explorer is currently presenting. Mirrors the
/// Explorer's fail-closed visibility contract: there is no ambient cross-tenant
/// view, and an unvalidated request for one degrades to the active tenant
/// rather than failing loudly.
/// </summary>
public enum ExplorerPluginTenantVisibility
{
    /// <summary>
    /// The default scope: only the caller's own active tenant. Never reveals
    /// another tenant's records.
    /// </summary>
    ActiveTenant = 0,

    /// <summary>
    /// The explicit cross-tenant scope, already validated by the host. A plugin
    /// reads this as an already-resolved fact; it cannot request it.
    /// </summary>
    AllTenants = 1,
}
