using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Explorer.Core.Topology;

/// <summary>
/// Registration helpers for the explorer's topology reader.
/// </summary>
public static class ExplorerTopologyServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <see cref="ITopologyReader"/>. Call after
    /// <c>AddExplorerCatalog</c>, which exposes the state-API client facet the
    /// reader depends on.
    /// </summary>
    public static IServiceCollection AddExplorerTopology(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddSingleton<ITopologyReader, TopologyReader>();
        return services;
    }
}
