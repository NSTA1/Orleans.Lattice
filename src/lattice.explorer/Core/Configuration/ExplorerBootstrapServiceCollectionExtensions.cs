using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Core.Configuration;

/// <summary>
/// Registration helper for the launcher-friendly environment bootstrap, which
/// seeds the explorer's first-run endpoint (and optionally a sign-in credential)
/// from process environment variables. Opted into by both heads so a launcher
/// can point a fresh explorer at a cluster without hand-editing per-user
/// app-data.
/// </summary>
public static class ExplorerBootstrapServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="EnvironmentExplorerBootstrap"/> as both the
    /// <see cref="IExplorerConfigurationSeed"/> and the
    /// <see cref="IExplorerCredentialSeed"/>, backed by the live process
    /// environment. Idempotent: a repeated call leaves the first registration in
    /// place. Manual cog-based configuration is unaffected; the seed only fills
    /// in the first-run endpoint when nothing is persisted yet.
    /// </summary>
    /// <param name="services">The head's service collection.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    public static IServiceCollection AddExplorerEnvironmentBootstrap(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddSingleton<IExplorerEnvironment, ProcessExplorerEnvironment>();
        services.TryAddSingleton<EnvironmentExplorerBootstrap>();
        services.TryAddSingleton<IExplorerConfigurationSeed>(
            sp => sp.GetRequiredService<EnvironmentExplorerBootstrap>());
        services.TryAddSingleton<IExplorerCredentialSeed>(
            sp => sp.GetRequiredService<EnvironmentExplorerBootstrap>());

        return services;
    }
}
