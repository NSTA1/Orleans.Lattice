using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Explorer.Core.Data;

/// <summary>
/// Registration helpers for the explorer's data reader.
/// </summary>
public static class ExplorerDataServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <see cref="IDataReader"/>. Call after <c>AddExplorerCatalog</c>,
    /// which exposes the state-API client facet the reader depends on.
    /// </summary>
    public static IServiceCollection AddExplorerData(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddSingleton<IDataReader, DataReader>();
        return services;
    }
}
