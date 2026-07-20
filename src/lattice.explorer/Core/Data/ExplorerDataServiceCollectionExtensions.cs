using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Explorer.Core.Data;

/// <summary>
/// Registration helpers for the explorer's data reader.
/// </summary>
public static class ExplorerDataServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <see cref="IDataReader"/> and the
    /// <see cref="IEntryLiveFollower"/>, scoped per Blazor circuit so they read
    /// through the calling scope's authenticated connection. Call after
    /// <c>AddExplorerCatalog</c>, which exposes the state-API client facet they
    /// depend on.
    /// </summary>
    public static IServiceCollection AddExplorerData(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddScoped<IDataReader, DataReader>();
        services.TryAddScoped<IEntryLiveFollower, EntryLiveFollower>();
        return services;
    }
}
