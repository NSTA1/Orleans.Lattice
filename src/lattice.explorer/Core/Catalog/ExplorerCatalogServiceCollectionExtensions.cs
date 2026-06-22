using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Core.Catalog;

/// <summary>
/// Registration helpers for the explorer's catalog reader and selection state.
/// </summary>
public static class ExplorerCatalogServiceCollectionExtensions
{
    /// <summary>
    /// Registers the catalog reader and the shared selection state. Exposes the
    /// connection's read-only client facet as <see cref="ILatticeStateClient"/>
    /// so the reader depends only on the narrow query surface. Call after
    /// <c>AddExplorerConfiguration</c>, which registers the connection.
    /// </summary>
    public static IServiceCollection AddExplorerCatalog(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddSingleton<ILatticeStateClient>(
            static sp => sp.GetRequiredService<ILatticeStateConnection>());
        services.TryAddSingleton<ICatalogReader, CatalogReader>();
        services.TryAddSingleton<IExplorerSelection, ExplorerSelection>();

        return services;
    }
}
