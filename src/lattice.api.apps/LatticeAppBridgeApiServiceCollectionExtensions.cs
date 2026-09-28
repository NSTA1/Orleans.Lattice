using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// Extension methods for registering the app bridge facade (<see cref="ILatticeAppBridge"/>): the single,
/// cluster-side enforcement seam for everything an app UI does with data.
/// </summary>
public static class LatticeAppBridgeApiServiceCollectionExtensions
{
    /// <summary>
    /// Adds the app bridge facade to the silo, registering it as the <see cref="ILatticeAppBridge"/> singleton
    /// that transport bindings map. Also registers the app-control facades <c>AddLatticeAppsApi()</c> adds, so it
    /// must be called after <c>AddLatticeApps()</c>. Idempotent; a repeated <paramref name="configure"/> is
    /// applied as well.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <param name="configure">Optional bridge configuration, such as the per-caller, per-app rate limit.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is null.</exception>
    /// <exception cref="InvalidOperationException"><c>AddLatticeApps()</c> has not been called first.</exception>
    public static ISiloBuilder AddLatticeAppBridgeApi(this ISiloBuilder builder, Action<LatticeAppBridgeOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.AddLatticeAppBridgeApi(configure);
        return builder;
    }

    /// <summary>
    /// Adds the app bridge facade to the service collection, registering it as the
    /// <see cref="ILatticeAppBridge"/> singleton that transport bindings map. Also registers the app-control
    /// facades <c>AddLatticeAppsApi()</c> adds, so it must be called after <c>AddLatticeApps()</c>. Invalid
    /// options (a rate-limit permit limit below 1, or a non-positive window) fail when the bridge is first
    /// resolved. Idempotent.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Optional bridge configuration, such as the per-caller, per-app rate limit.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is null.</exception>
    /// <exception cref="InvalidOperationException"><c>AddLatticeApps()</c> has not been called first.</exception>
    public static IServiceCollection AddLatticeAppBridgeApi(this IServiceCollection services, Action<LatticeAppBridgeOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.AddLatticeAppsApi();
        services.AddOptions<LatticeAppBridgeOptions>();
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.TryAddSingleton(sp => new AppBridgeRateLimiter(
            sp.GetRequiredService<IOptions<LatticeAppBridgeOptions>>().Value,
            sp.GetService<TimeProvider>()));
        services.TryAddSingleton<ILatticeAppBridge>(sp => new LatticeAppBridge(
            sp.GetRequiredService<AppRoleGrantEvaluator>(),
            sp.GetService<IGrainFactory>(),
            sp.GetService<ITenantContextResolver>(),
            sp.GetService<ILatticeMembershipContext>(),
            sp.GetRequiredService<AppBridgeRateLimiter>(),
            sp.GetService<ILogger<LatticeAppBridge>>()));
        return services;
    }
}
