using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Explorer.Core.Metrics;

/// <summary>
/// Registration helpers for the explorer's metrics reader.
/// </summary>
public static class ExplorerMetricsServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <see cref="IMetricsReader"/>. Call after
    /// <c>AddExplorerCatalog</c>, which exposes the state-API client facet the
    /// reader depends on.
    /// </summary>
    public static IServiceCollection AddExplorerMetrics(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddSingleton<IMetricsReader, MetricsReader>();
        return services;
    }
}
