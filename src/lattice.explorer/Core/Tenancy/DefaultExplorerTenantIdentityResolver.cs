using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Core.Tenancy;

/// <summary>
/// The default <see cref="IExplorerTenantIdentityResolver"/>. When the tenant view
/// is active it maps the current sign-in onto the circuit's active tenant,
/// fail-closed: an authenticated caller is scoped to the documented single-tenant
/// and development default (<see cref="ExplorerTenantId.Default"/>), and an
/// anonymous caller is scoped to no tenant at all (so the fail-closed view reveals
/// nothing until a real sign-in establishes a tenant).
/// </summary>
/// <remarks>
/// This is the documented default for single-tenant and development deployments -
/// every signed-in user shares the default tenant, which owns the legacy,
/// un-prefixed trees. A production multi-tenant head registers its own
/// <see cref="IExplorerTenantIdentityResolver"/> (before or after
/// <see cref="ExplorerTenantServiceCollectionExtensions.AddExplorerTenantView"/>,
/// using a non-<c>TryAdd</c> replacement) that reads the authenticated principal's
/// tenant claim and maps it onto <see cref="IExplorerTenantContext.ActiveTenant"/>.
/// The switch that turns this resolver on and off is the same one that registers
/// the active <see cref="IExplorerTenantView"/>; there is no separate flag.
/// </remarks>
internal sealed class DefaultExplorerTenantIdentityResolver(
    IExplorerTenantView view,
    IExplorerAuthSession session,
    IExplorerTenantContext context) : IExplorerTenantIdentityResolver
{
    private readonly IExplorerTenantView _view =
        view ?? throw new ArgumentNullException(nameof(view));

    private readonly IExplorerAuthSession _session =
        session ?? throw new ArgumentNullException(nameof(session));

    private readonly IExplorerTenantContext _context =
        context ?? throw new ArgumentNullException(nameof(context));

    /// <inheritdoc />
    public ValueTask ResolveAsync(CancellationToken cancellationToken = default)
    {
        // Tenancy disabled: never touch the context, so a non-tenant deployment is
        // byte-for-byte unchanged (no active tenant is ever established).
        if (!_view.IsActive)
        {
            return ValueTask.CompletedTask;
        }

        // Fail-closed identity mapping: a signed-in caller is scoped to the
        // documented single-tenant/dev default; an anonymous caller is scoped to
        // no tenant, so the active-tenant view reveals nothing.
        _context.ActiveTenant = _session.IsAuthenticated ? ExplorerTenantId.Default : null;
        return ValueTask.CompletedTask;
    }
}
