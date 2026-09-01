using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Hosting;

namespace Orleans.Lattice.Scaling;

/// <summary>
/// Extension methods for configuring the opt-in <c>Orleans.Lattice.Scaling</c>
/// autoscaling signal on an Orleans silo. Declared <see langword="partial"/> so
/// sibling packages (the scaling endpoint, #1188) can contribute their own
/// registration methods to the same class from a separate file without a merge
/// conflict.
/// </summary>
public static partial class LatticeScalingServiceCollectionExtensions
{
    /// <summary>
    /// Adds the <c>Orleans.Lattice.Scaling</c> read-only autoscaling signal to
    /// the silo. Registers the live <see cref="ILatticeScalingSignal"/> facade
    /// (a silo-scoped hosted service that samples cluster-aggregate compute
    /// pressure on a timer and caches the resulting <see cref="ScalingSignal"/>),
    /// its compute-axis collector and cluster runtime-statistics source, the
    /// storage-axis collector (#1187) and its WAL storage-state source, the
    /// split-activity probe that suppresses scale-in while an adaptive shard
    /// split is in flight (#1224), binds
    /// <see cref="LatticeScalingSignalOptions"/>, and ensures a
    /// <see cref="TimeProvider"/> is available for sampling timestamps.
    /// <para>
    /// The cluster runtime-statistics source resolves Orleans'
    /// <see cref="Orleans.Runtime.IManagementGrain"/> for per-silo CPU, memory,
    /// and activation counts; the host's default
    /// <see cref="Orleans.Statistics.IEnvironmentStatisticsProvider"/> (registered
    /// by the silo host) supplies the cgroup-honouring memory figures those
    /// statistics carry. All registrations use <c>TryAdd</c> so this is safe to
    /// call more than once and so #1187 can substitute a richer storage
    /// collector.
    /// </para>
    /// </summary>
    /// <param name="builder">The silo builder to register the signal on.</param>
    /// <param name="configure">
    /// Optional delegate to configure <see cref="LatticeScalingSignalOptions"/>.
    /// </param>
    /// <returns>The same <paramref name="builder"/> for fluent chaining.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="builder"/> is <see langword="null"/>.
    /// </exception>
    public static ISiloBuilder AddLatticeScalingSignal(
        this ISiloBuilder builder,
        Action<LatticeScalingSignalOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var options = builder.Services.AddOptions<LatticeScalingSignalOptions>();
        if (configure is not null)
        {
            options.Configure(configure);
        }

        var services = builder.Services;
        services.TryAddSingleton(TimeProvider.System);

        // Cluster runtime-statistics source, shared between the compute collector
        // and the replica-count provider so a tick costs a single management call.
        services.TryAddSingleton<ManagementClusterRuntimeStatisticsSource>();
        services.TryAddSingleton<IClusterRuntimeStatisticsSource>(
            sp => sp.GetRequiredService<ManagementClusterRuntimeStatisticsSource>());
        services.TryAddSingleton<IReplicaCountProvider>(
            sp => sp.GetRequiredService<ManagementClusterRuntimeStatisticsSource>());

        // Axis collectors and probes. The compute collector is #1186; the storage
        // collector (#1187) reads the WAL storage state through its own source
        // seam; the split probe (#1224) reads cluster split activity through the
        // core admin surface. All use TryAdd so a host may substitute richer
        // implementations - or register NoOpSplitActivityProbe first to keep the
        // split axis inert.
        services.TryAddSingleton<IComputePressureCollector, ComputePressureCollector>();
        services.TryAddSingleton<IWalStorageStateSource, LatticeWalStorageStateSource>();
        services.TryAddSingleton<IStoragePressureCollector, StoragePressureCollector>();
        services.TryAddSingleton<ISplitActivityProbe>(sp =>
            sp.GetRequiredService<IOptions<LatticeScalingSignalOptions>>().Value.SplitAwareScaleIn
                ? new LatticeSplitActivityProbe(
                    sp.GetService<IGrainFactory>(),
                    sp.GetService<ILogger<LatticeSplitActivityProbe>>())
                : new NoOpSplitActivityProbe());

        services.TryAddSingleton<ScalingSignalComputer>();

        // The live facade, exposed as ILatticeScalingSignal and driven as a
        // hosted service. Both resolve the same singleton instance.
        services.TryAddSingleton<LatticeScalingSignal>();
        services.TryAddSingleton<ILatticeScalingSignal>(
            sp => sp.GetRequiredService<LatticeScalingSignal>());
        services.AddHostedService(sp => sp.GetRequiredService<LatticeScalingSignal>());

        return builder;
    }
}
