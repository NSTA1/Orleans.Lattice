using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Explorer.Core.DeadLetter;

/// <summary>
/// Registration helpers for the explorer's dead-letter reader.
/// </summary>
public static class ExplorerDeadLetterServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <see cref="IDeadLetterReader"/>, scoped per Blazor circuit
    /// so it reads through the calling scope's authenticated connection. Call
    /// after <c>AddExplorerCatalog</c>, which exposes the state-API client facet
    /// it depends on.
    /// </summary>
    public static IServiceCollection AddExplorerDeadLetter(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddScoped<IDeadLetterReader, DeadLetterReader>();
        return services;
    }
}
