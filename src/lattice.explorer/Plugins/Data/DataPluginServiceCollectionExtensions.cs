using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Plugins.Selection;

namespace Orleans.Lattice.Explorer.Plugins.Data;

/// <summary>
/// Registration for the key and value drill-down per-selection plugin.
/// </summary>
public static class DataPluginServiceCollectionExtensions
{
    /// <summary>
    /// Registers the value drill-down surface: the plugin itself, the domain
    /// contract the host resolves for it, and the shared per-selection kernel.
    /// Idempotent, so a head may call it alongside the composite registration.
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerDataPlugin(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddExplorerSelectionPluginHost();
        services.TryAddScoped<IDataSurface, DataSurface>();

        return services.AddExplorerPlugin<DataSelectionPlugin>();
    }
}
