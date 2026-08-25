namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// The default, mutable <see cref="IExplorerTenantContext"/>. Registered per
/// Blazor circuit so each connection carries its own active tenant and requested
/// visibility. Starts with no active tenant and the per-tenant default scope, so
/// an active view with no active tenant established reveals nothing until the head
/// sets one - the fail-closed default.
/// </summary>
internal sealed class ExplorerTenantContext : IExplorerTenantContext
{
    /// <inheritdoc />
    public ExplorerTenantId? ActiveTenant { get; set; }

    /// <inheritdoc />
    public ExplorerTenantVisibility RequestedVisibility { get; set; } = ExplorerTenantVisibility.ActiveTenant;
}
