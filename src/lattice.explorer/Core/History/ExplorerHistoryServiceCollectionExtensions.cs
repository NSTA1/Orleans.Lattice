using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Explorer.Core.History;

/// <summary>
/// Registration helpers for the explorer's history reader.
/// </summary>
public static class ExplorerHistoryServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <see cref="IHistoryReader"/> and the
    /// <see cref="IHistoryLiveFollower"/>, scoped per Blazor circuit so they read
    /// through the calling scope's authenticated connection. Call after
    /// <c>AddExplorerCatalog</c>, which exposes the state-API client facet they
    /// depend on.
    /// </summary>
    public static IServiceCollection AddExplorerHistory(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddScoped<IHistoryReader, HistoryReader>();
        services.TryAddScoped<IHistoryLiveFollower, HistoryLiveFollower>();
        return services;
    }
}
