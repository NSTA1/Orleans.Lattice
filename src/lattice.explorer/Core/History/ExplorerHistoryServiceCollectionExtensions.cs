using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Explorer.Core.History;

/// <summary>
/// Registration helpers for the explorer's history reader.
/// </summary>
public static class ExplorerHistoryServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <see cref="IHistoryReader"/>. Call after <c>AddExplorerCatalog</c>,
    /// which exposes the state-API client facet the reader depends on.
    /// </summary>
    public static IServiceCollection AddExplorerHistory(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddSingleton<IHistoryReader, HistoryReader>();
        return services;
    }
}
