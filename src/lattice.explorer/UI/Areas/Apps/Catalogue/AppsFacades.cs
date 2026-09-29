using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// The transport-neutral facades the Apps area reads, resolved once per circuit and
/// each optional: a head that serves no app management leaves the facade
/// unregistered, and the area treats that exactly as a denial (fail closed).
/// </summary>
/// <remarks>
/// The facades are resolved lazily through the circuit's service provider, never as
/// constructor dependencies, so the area can be built - and answer
/// <see cref="Navigation.AreaAvailability.Hidden"/> - in a host without them.
/// </remarks>
/// <param name="services">The circuit's service provider.</param>
internal sealed class AppsFacades(IServiceProvider services)
{
    private readonly Lazy<ILatticeAppCatalog?> _catalog = new(services.GetShellFacade<ILatticeAppCatalog>);
    private readonly Lazy<ILatticeAppsControl?> _control = new(services.GetShellFacade<ILatticeAppsControl>);
    private readonly Lazy<ILatticeAppWorkspace?> _workspace = new(services.GetShellFacade<ILatticeAppWorkspace>);
    private readonly Lazy<ILatticeAuthAdmin?> _auth = new(services.GetShellFacade<ILatticeAuthAdmin>);
    private readonly Lazy<ILatticeAppRoleBindings?> _roleBindings = new(services.GetShellFacade<ILatticeAppRoleBindings>);
    private readonly Lazy<ILatticeActiveTenantProvider?> _tenant = new(services.GetService<ILatticeActiveTenantProvider>);

    /// <summary>
    /// The tenant the circuit's calls assert right now, or <see langword="null"/>
    /// when they assert none. Everything the area remembers is keyed on it, so an
    /// answer read under one tenant is never served under another.
    /// </summary>
    public string? AssertedTenant => _tenant.Value?.AssertedTenant;

    /// <summary>The administrative catalogue, or <see langword="null"/> when the head serves none.</summary>
    public ILatticeAppCatalog? Catalog => Resolve(_catalog);

    /// <summary>App lifecycle and consent management, or <see langword="null"/> when the head serves none.</summary>
    public ILatticeAppsControl? Control => Resolve(_control);

    /// <summary>The caller's own apps, or <see langword="null"/> when the head serves none.</summary>
    public ILatticeAppWorkspace? Workspace => Resolve(_workspace);

    /// <summary>The auth facade whose read-only group search binds roles, or <see langword="null"/>.</summary>
    public ILatticeAuthAdmin? Auth => Resolve(_auth);

    /// <summary>Re-binding an installed app's roles to groups, or <see langword="null"/> when the head serves none.</summary>
    public ILatticeAppRoleBindings? RoleBindings => Resolve(_roleBindings);

    private static T? Resolve<T>(Lazy<T?> facade)
        where T : class
    {
        try
        {
            return facade.Value;
        }
        catch (InvalidOperationException)
        {
            // A registered facade whose own dependencies are missing (no session in
            // this host) cannot serve this circuit: treat it as absent.
            return null;
        }
    }
}
