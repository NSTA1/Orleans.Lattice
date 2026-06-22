using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Orleans.Lattice.Explorer.Core.Connection;

/// <summary>
/// Registration helpers for the explorer's state-API connection.
/// </summary>
public static class LatticeExplorerConnectionServiceCollectionExtensions
{
    /// <summary>
    /// Registers the shared <see cref="ILatticeStateConnection"/> as a singleton.
    /// Every cluster read in the explorer resolves and flows through this single
    /// connection. The endpoint is supplied later via
    /// <see cref="ILatticeStateConnection.ConfigureAsync"/>.
    /// </summary>
    public static IServiceCollection AddLatticeStateConnection(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddSingleton<ILatticeStateConnection, LatticeStateConnection>();
        return services;
    }
}
