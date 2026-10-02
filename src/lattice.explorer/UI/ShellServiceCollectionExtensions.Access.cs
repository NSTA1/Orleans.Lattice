using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.UI.Areas.Access;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI;

internal static partial class ShellServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Access area (A4) and its circuit-scoped catalogue. The area
    /// binds to the transport-neutral <c>ILatticeAuthAdmin</c> the transport
    /// registers; without it the area hides itself.
    /// </summary>
    /// <param name="services">The service collection.</param>
    static partial void AddAccess(IServiceCollection services)
    {
        // A factory, not a type registration: the catalogue needs the auth facade,
        // which a head without the transport does not register. The area hides
        // itself then, so no page resolves the catalogue, and a container built
        // with ValidateOnBuild stays valid.
        services.TryAddScoped(provider => new AccessCatalog(
            provider.GetRequiredShellFacade<ILatticeAuthAdmin>(),
            provider.GetService<ShellAssertedTenant>(),
            ShellCaller.Of(provider)));
        AddTenantAccess(services);
        services.AddExplorerArea<AccessArea>();
    }
}
