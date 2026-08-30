using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Plugins.Selection;

namespace Orleans.Lattice.Explorer.Plugins.Metrics;

/// <summary>
/// Registration for the live-metrics per-selection plugin.
/// </summary>
public static class MetricsPluginServiceCollectionExtensions
{
    /// <summary>
    /// Registers the live-metrics surface: the plugin itself, the domain
    /// contract the host resolves for it, and the shared per-selection kernel.
    /// Idempotent, so a head may call it alongside the composite registration.
    /// <para>
    /// The metrics reader the domain model adapts is registered separately by
    /// the Explorer core, so a head that does not register it gets a surface
    /// that fails its own read rather than one that reaches around the contract.
    /// </para>
    /// </summary>
    /// <param name="services">The service collection. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is <see langword="null"/>.</exception>
    public static IServiceCollection AddExplorerMetricsPlugin(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddExplorerSelectionPluginHost();
        services.TryAddScoped<IMetricsSurface, MetricsSurface>();

        return services.AddExplorerPlugin<MetricsSelectionPlugin>();
    }
}
