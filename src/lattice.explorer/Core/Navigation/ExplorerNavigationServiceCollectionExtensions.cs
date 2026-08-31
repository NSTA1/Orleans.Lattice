using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// Registration helpers for the Explorer shell's route model.
/// </summary>
public static class ExplorerNavigationServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="IExplorerShellRouter"/> as the session-scoped source
    /// of truth for the shell's current route.
    /// </summary>
    /// <remarks>
    /// Registered with <c>TryAdd</c>, so a head or a test that wants its own
    /// router registers it first and this call defers. The router alone does not
    /// touch the address bar: a head completes the loop by binding its routable
    /// page to <see cref="IExplorerShellRouter.SetAddress"/> and
    /// <see cref="IExplorerShellRouter.NavigationRequested"/>. Without that
    /// binding the shell still works - it simply keeps its route in memory - so a
    /// head that has not adopted routing degrades rather than breaks.
    /// </remarks>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerNavigation(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddScoped<IExplorerShellRouter, ExplorerShellRouter>();
        return services;
    }
}
