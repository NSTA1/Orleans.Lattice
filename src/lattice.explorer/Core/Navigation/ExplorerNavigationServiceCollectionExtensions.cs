using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// Registration helpers for the explorer's top-level navigation shell: the
/// advisory capability store that gates areas and per-scope actions.
/// </summary>
public static class ExplorerNavigationServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <see cref="IExplorerCapabilityStore"/> the shell consults to
    /// enable or disable areas. Idempotent.
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    public static IServiceCollection AddExplorerNavigation(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddSingleton<IExplorerCapabilityStore, ExplorerCapabilityStore>();
        return services;
    }
}
