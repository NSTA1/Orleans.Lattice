using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using VehicleFleetSimulator.Abstractions;

namespace VehicleFleetSimulator.Benchmark.Sink;

/// <summary>
/// DI helpers for registering <see cref="LatticeSink"/> and its hosted-service shutdown drain
/// against an Orleans silo's service collection.
/// </summary>
public static class LatticeSinkServiceCollectionExtensions
{
    /// <summary>
    /// Replaces any registered <see cref="ITelemetrySink"/> with <see cref="LatticeSink"/>, binds
    /// <see cref="LatticeSinkOptions"/> from the supplied <paramref name="configurationSection"/>,
    /// and registers the drain loop as an <see cref="IHostedService"/>. The replacement (rather
    /// than chained registration) is required by §2 of <c>benchmark/benchmark-plan.md</c>: a
    /// second <see cref="ITelemetrySink"/> would silently double-write and contaminate the
    /// measurement.
    /// </summary>
    public static IServiceCollection AddLatticeSink(
        this IServiceCollection services,
        IConfiguration configurationSection,
        Action<LatticeSinkOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configurationSection);

        services.AddOptions<LatticeSinkOptions>().Bind(configurationSection);
        if (configure is not null)
            services.PostConfigure(configure);

        services.RemoveAll<ITelemetrySink>();
        services.AddSingleton<LatticeSink>();
        services.AddSingleton<ITelemetrySink>(sp => sp.GetRequiredService<LatticeSink>());
        services.AddHostedService(sp => sp.GetRequiredService<LatticeSink>());
        return services;
    }
}
