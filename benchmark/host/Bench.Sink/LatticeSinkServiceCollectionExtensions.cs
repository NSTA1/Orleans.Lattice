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
    /// than chained registration) is required by §2 of <c>benchmark/benchmark-scenarios.md</c>: a
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

    /// <summary>
    /// Registers <see cref="LatticeReadDriver"/> as an <see cref="IHostedService"/> bound to the
    /// supplied <paramref name="configurationSection"/>. Safe to call unconditionally - the
    /// driver short-circuits its <c>ExecuteAsync</c> when <c>ReadDriver:Enabled</c> is
    /// <c>false</c>, so scenarios that don't generate read load incur only the cost of an
    /// empty hosted-service slot.
    /// </summary>
    public static IServiceCollection AddLatticeReadDriver(
        this IServiceCollection services,
        IConfiguration configurationSection)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configurationSection);

        services.AddOptions<LatticeReadDriverOptions>().Bind(configurationSection);
        services.TryAddSingleton<LatticeReadDriverMetrics>();
        services.AddHostedService<LatticeReadDriver>();
        return services;
    }

    /// <summary>
    /// Registers <see cref="LatticeWriteDriver"/> as an <see cref="IHostedService"/> bound
    /// to the supplied <paramref name="configurationSection"/>. Safe to call
    /// unconditionally - the driver short-circuits its <c>ExecuteAsync</c> when
    /// <c>WriteDriver:Enabled</c> is <c>false</c>, so scenarios that don't generate
    /// in-silo write load incur only the cost of an empty hosted-service slot.
    /// </summary>
    /// <remarks>
    /// Intended for the bidirectional-replication scenario where the replica silo needs an
    /// in-process write producer (the simulator API only points at the origin cluster).
    /// </remarks>
    public static IServiceCollection AddLatticeWriteDriver(
        this IServiceCollection services,
        IConfiguration configurationSection)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configurationSection);

        services.AddOptions<LatticeWriteDriverOptions>().Bind(configurationSection);
        services.TryAddSingleton<LatticeWriteDriverMetrics>();
        services.AddHostedService<LatticeWriteDriver>();
        return services;
    }

    /// <summary>
    /// Registers <see cref="LatticeAtomicSagaDriver"/> as an <see cref="IHostedService"/>
    /// bound to the supplied <paramref name="configurationSection"/>. Safe to call
    /// unconditionally - the driver short-circuits its <c>ExecuteAsync</c> when
    /// <c>AtomicSagaDriver:Enabled</c> is <c>false</c>, so scenarios that don't generate
    /// atomic-saga load incur only the cost of an empty hosted-service slot.
    /// </summary>
    /// <remarks>
    /// Used by the atomic-write benchmarks (single-cluster and bidirectional-replication
    /// variants) to drive <c>SetManyAtomicAsync</c> sagas at a configured rate. Pair with
    /// <see cref="AddLatticeSink"/> on the same silo so the sink reuses the same
    /// <c>ILattice</c> tree and the resulting WAL captures both simulator-driven writes
    /// and atomic-saga writes under a single observable surface.
    /// </remarks>
    public static IServiceCollection AddLatticeAtomicSagaDriver(
        this IServiceCollection services,
        IConfiguration configurationSection)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configurationSection);

        services.AddOptions<LatticeAtomicSagaDriverOptions>().Bind(configurationSection);
        services.TryAddSingleton<LatticeAtomicSagaDriverMetrics>();
        services.AddHostedService<LatticeAtomicSagaDriver>();
        return services;
    }
}
