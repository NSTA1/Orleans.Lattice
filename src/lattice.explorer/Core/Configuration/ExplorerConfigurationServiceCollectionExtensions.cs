using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Core.Configuration;

/// <summary>
/// Registration helpers for the explorer's configuration store and session.
/// </summary>
public static class ExplorerConfigurationServiceCollectionExtensions
{
    /// <summary>
    /// Registers the local JSON config store, the per-scope state-API connection,
    /// and the <see cref="IExplorerSession"/> that ties them together. Pass
    /// <paramref name="configure"/> to point the store at a head-specific
    /// per-user app-data path. The session is scoped per Blazor circuit so each
    /// circuit drives its own connection; the config store and its options stay
    /// singletons.
    /// </summary>
    public static IServiceCollection AddExplorerConfiguration(
        this IServiceCollection services,
        Action<ExplorerConfigStoreOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new ExplorerConfigStoreOptions();
        configure?.Invoke(options);

        services.TryAddSingleton(options);
        services.TryAddSingleton<IExplorerConfigStore, JsonExplorerConfigStore>();
        services.AddLatticeStateConnection();
        services.TryAddScoped<IExplorerSession, ExplorerSession>();

        return services;
    }
}
