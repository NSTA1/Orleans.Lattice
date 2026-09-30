using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Areas.Apps.App;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI;

internal static partial class ShellServiceCollectionExtensions
{
    /// <summary>
    /// The manifest-derived app pages and their framed Open section (A2, issue #3819). The
    /// loader is scoped, one per circuit, and resolves that circuit's credential-aware
    /// <see cref="ILatticeAppWorkspace"/> and <see cref="ILatticeAppsControl"/> optionally,
    /// so a host without either answers not found for every app rather than failing to
    /// start. It registers no area and no completion source: the Apps area (A1) owns both.
    /// </summary>
    /// <param name="services">The service collection to register into.</param>
    static partial void AddApp(IServiceCollection services)
    {
        services.TryAddScoped(provider => new AppPageLoader(
            provider.GetShellFacade<ILatticeAppWorkspace>(),
            provider.GetShellFacade<ILatticeAppsControl>(),
            provider.GetService<ILogger<AppPageLoader>>(),
            provider.GetShellFacade<ILatticeAppCatalog>()));
    }
}
