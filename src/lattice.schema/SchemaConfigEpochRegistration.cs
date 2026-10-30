using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Schema;

/// <summary>Shared wiring of a <see cref="SchemaConfigEpochSynchronizer{TCache}"/> for one provider cache.</summary>
internal static class SchemaConfigEpochRegistration
{
    /// <summary>
    /// Registers one synchronizer for <typeparamref name="TCache"/>, as a mutation
    /// observer (to publish changes this silo commits) and as a hosted service (to
    /// follow changes committed elsewhere). Same instance for both roles. Call once
    /// per cache: the role registrations are not idempotent.
    /// </summary>
    /// <typeparam name="TCache">The concrete provider cache, already registered as a singleton.</typeparam>
    /// <param name="services">The silo's service collection.</param>
    /// <param name="configTree">The reserved configuration tree the cache reflects.</param>
    /// <param name="pollInterval">Resolves the configured propagation interval.</param>
    internal static void AddSchemaConfigEpochSynchronizer<TCache>(
        this IServiceCollection services, string configTree, Func<IServiceProvider, TimeSpan> pollInterval)
        where TCache : class, ISchemaConfigCache
    {
        services.TryAddSingleton(sp => new SchemaConfigEpochSynchronizer<TCache>(
            sp.GetRequiredService<IGrainFactory>(),
            sp.GetRequiredService<TCache>(),
            configTree,
            pollInterval(sp),
            sp.GetRequiredService<ILogger<SchemaConfigEpochSynchronizer<TCache>>>(),
            sp.GetService<IClusterMembershipService>(),
            sp.GetService<TimeProvider>()));
        services.AddSingleton<IMutationObserver>(
            sp => sp.GetRequiredService<SchemaConfigEpochSynchronizer<TCache>>());
        services.AddSingleton<IHostedService>(
            sp => sp.GetRequiredService<SchemaConfigEpochSynchronizer<TCache>>());
    }
}
