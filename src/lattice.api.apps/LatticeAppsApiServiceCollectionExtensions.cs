using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// Extension methods for registering the optional <c>Orleans.Lattice.Api.Apps</c>
/// app lifecycle and consent control facade.
/// </summary>
public static class LatticeAppsApiServiceCollectionExtensions
{
    /// <summary>
    /// Adds the transport-agnostic app-control facade to the silo, registering it as
    /// the <see cref="ILatticeAppsControl"/> singleton that transport bindings map.
    /// Must be called after <c>AddLatticeApps()</c>, whose registry, source seam and
    /// activation pipeline the facade composes. Idempotent.
    /// </summary>
    /// <param name="builder">The silo builder.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is null.</exception>
    /// <exception cref="InvalidOperationException"><c>AddLatticeApps()</c> has not been called first.</exception>
    public static ISiloBuilder AddLatticeAppsApi(this ISiloBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.AddLatticeAppsApi();
        return builder;
    }

    /// <summary>
    /// Adds the transport-agnostic app-control facade to the service collection,
    /// registering it as the <see cref="ILatticeAppsControl"/> singleton that transport
    /// bindings map, and as the <see cref="ILatticeAppRoleBindings"/> singleton that re-binds
    /// an installed app's roles. Must be called after <c>AddLatticeApps()</c>. Idempotent.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The same <paramref name="services"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is null.</exception>
    /// <exception cref="InvalidOperationException"><c>AddLatticeApps()</c> has not been called first.</exception>
    public static IServiceCollection AddLatticeAppsApi(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        // Ordering guard: the facade composes the apps engine, so its pipeline must already
        // be registered; failing here beats an opaque resolution failure at first call.
        if (!services.Any(d => d.ServiceType == typeof(IAppActivationPipeline)))
        {
            throw new InvalidOperationException(
                "AddLatticeAppsApi() must be called after AddLatticeApps(). Register the apps add-on " +
                "(siloBuilder.AddLatticeApps(...)) before adding the app-control API, which composes it.");
        }

        services.TryAddSingleton<ILatticeAppsControl>(sp => new LatticeAppsControl(
            sp.GetRequiredService<IAppRegistry>(),
            sp.GetRequiredService<IAppSource>(),
            sp.GetRequiredService<IAppActivationPipeline>(),
            sp.GetRequiredService<ILatticeAccessGate>(),
            sp.GetRequiredService<ITenantContextResolver>(),
            sp.GetService<ILatticeMembershipContext>()));
        services.TryAddSingleton<ILatticeAppRoleBindings>(sp =>
            sp.GetRequiredService<ILatticeAppsControl>() as ILatticeAppRoleBindings
            ?? new LatticeAppsControl(
                sp.GetRequiredService<IAppRegistry>(),
                sp.GetRequiredService<IAppSource>(),
                sp.GetRequiredService<IAppActivationPipeline>(),
                sp.GetRequiredService<ILatticeAccessGate>(),
                sp.GetRequiredService<ITenantContextResolver>(),
                sp.GetService<ILatticeMembershipContext>()));
        services.TryAddSingleton<ILatticeAppCatalog>(sp => new LatticeAppCatalog(
            LatticeAppCatalog.ToSourceSet(sp.GetRequiredService<IAppSource>()),
            sp.GetRequiredService<IAppRegistry>(),
            sp.GetRequiredService<IAppActivationPipeline>(),
            sp.GetRequiredService<ILatticeAccessGate>(),
            sp.GetRequiredService<ITenantContextResolver>(),
            sp.GetService<ILatticeMembershipContext>(),
            sp.GetService<ILogger<LatticeAppCatalog>>()));
        services.TryAddSingleton(sp => new AppRoleGrantEvaluator(
            sp.GetService<IAppRegistryProjection>(),
            sp.GetService<IAppSource>(),
            sp.GetService<ILatticeAccessGate>()));
        services.TryAddSingleton<ILatticeAppWorkspace>(sp =>
        {
            var source = sp.GetService<IAppSource>();
            return new LatticeAppWorkspace(
                sp.GetRequiredService<AppRoleGrantEvaluator>(),
                source is null ? null : LatticeAppCatalog.ToSourceSet(source),
                sp.GetService<ITenantContextResolver>(),
                sp.GetService<ILatticeMembershipContext>(),
                sp.GetService<IAppActivationPipeline>(),
                sp.GetService<ILogger<LatticeAppWorkspace>>());
        });
        return services;
    }
}
