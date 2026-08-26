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
    /// Registers the catalog reader and the selection state. Exposes the
    /// connection's read-only client facet as <see cref="ILatticeStateClient"/>
    /// so the reader depends only on the narrow query surface. The client facet,
    /// the reader, and the selection state are all scoped per Blazor circuit so
    /// they read through the calling scope's authenticated connection and no
    /// per-operator state is shared between circuits.
    /// Call after <c>AddExplorerConfiguration</c>, which registers the connection.
    /// </summary>
    public static IServiceCollection AddExplorerCatalog(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddScoped<ILatticeStateClient>(
            static sp => sp.GetRequiredService<ILatticeStateConnection>());
        services.TryAddScoped<ICatalogReader, CatalogReader>();

        // Scoped, never singleton. The web head is multi-user (one Blazor circuit
        // per signed-in operator, each with its own credential and connection), so
        // a process-wide selection would publish one operator's selected
        // CatalogItem - tree ids, view source topology, restore-shadow links - into
        // every other circuit's detail panel with no authorization re-check, and
        // would let any operator re-target every other operator's panel.
        services.TryAddScoped<IExplorerSelection, ExplorerSelection>();

        return services;
    }
}
