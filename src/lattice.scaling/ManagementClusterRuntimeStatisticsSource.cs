using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Orleans.Runtime;

namespace Orleans.Lattice.Scaling;

/// <summary>
/// Production <see cref="IClusterRuntimeStatisticsSource"/> and
/// <see cref="IReplicaCountProvider"/>: sources the cluster-wide active-silo
/// count and per-silo CPU / memory / activation samples from Orleans'
/// <see cref="IManagementGrain"/> (a single <c>GetHosts</c> + <c>GetRuntimeStatistics</c>
/// round-trip), caching the result for one sample interval so the compute
/// collector and the replica-count provider share one round-trip per tick.
/// <para>
/// Memory pressure is measured against the provider's
/// <see cref="Orleans.Statistics.EnvironmentStatistics.MaximumAvailableMemoryBytes"/>,
/// which honours any cgroup / container memory cap, rather than the raw machine
/// total. When no <see cref="IGrainFactory"/> is available (the package added
/// outside a silo, as in a bare unit-test container) the source degrades to a
/// single-replica, no-pressure fallback rather than throwing.
/// </para>
/// </summary>
internal sealed class ManagementClusterRuntimeStatisticsSource(
    IOptions<LatticeScalingSignalOptions> options,
    TimeProvider timeProvider,
    IGrainFactory? grainFactory = null,
    ILogger<ManagementClusterRuntimeStatisticsSource>? logger = null)
    : IClusterRuntimeStatisticsSource, IReplicaCountProvider
{
    private static readonly ClusterRuntimeSnapshot Fallback = new() { ActiveSiloCount = 1 };

    private readonly IOptions<LatticeScalingSignalOptions> _options = options;
    private readonly TimeProvider _timeProvider = timeProvider;
    private readonly IGrainFactory? _grainFactory = grainFactory;
    private readonly ILogger _logger = logger ?? NullLogger<ManagementClusterRuntimeStatisticsSource>.Instance;
    private readonly SemaphoreSlim _refreshLock = new(1, 1);

    private ClusterRuntimeSnapshot _cached = Fallback;
    private DateTimeOffset _cachedAt = DateTimeOffset.MinValue;
    private bool _hasCached;

    /// <inheritdoc />
    public async ValueTask<ClusterRuntimeSnapshot> SampleAsync(CancellationToken cancellationToken)
    {
        var ttl = CacheTtl();
        var now = _timeProvider.GetUtcNow();
        if (_hasCached && now - _cachedAt < ttl)
        {
            return _cached;
        }

        await _refreshLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            now = _timeProvider.GetUtcNow();
            if (_hasCached && now - _cachedAt < ttl)
            {
                return _cached;
            }

            var fresh = await QueryClusterAsync(cancellationToken).ConfigureAwait(false);
            _cached = fresh;
            _cachedAt = now;
            _hasCached = true;
            return fresh;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Failed to sample cluster runtime statistics; reusing last known snapshot.");
            return _hasCached ? _cached : Fallback;
        }
        finally
        {
            _refreshLock.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask<int> GetActiveReplicaCountAsync(CancellationToken cancellationToken)
    {
        var snapshot = await SampleAsync(cancellationToken).ConfigureAwait(false);
        return Math.Max(1, snapshot.ActiveSiloCount);
    }

    private TimeSpan CacheTtl()
    {
        var interval = _options.Value.SampleInterval;
        return interval > TimeSpan.Zero ? interval : LatticeScalingSignalOptions.DefaultSampleInterval;
    }

    private async ValueTask<ClusterRuntimeSnapshot> QueryClusterAsync(CancellationToken cancellationToken)
    {
        var management = _grainFactory?.GetGrain<IManagementGrain>(0);
        if (management is null)
        {
            return Fallback;
        }

        var hosts = await management.GetHosts(onlyActive: true).ConfigureAwait(false);
        if (hosts is null || hosts.Count == 0)
        {
            return Fallback;
        }

        cancellationToken.ThrowIfCancellationRequested();

        var addresses = hosts.Keys.ToArray();
        var stats = await management.GetRuntimeStatistics(addresses).ConfigureAwait(false);

        var silos = new SiloResourceSample[stats.Length];
        for (var i = 0; i < stats.Length; i++)
        {
            var stat = stats[i];
            var env = stat.EnvironmentStatistics;
            silos[i] = new SiloResourceSample
            {
                CpuUsagePercent = env.FilteredCpuUsagePercentage,
                MemoryUsedBytes = env.FilteredMemoryUsageBytes,
                MaximumAvailableMemoryBytes = env.MaximumAvailableMemoryBytes,
                ActivationCount = stat.ActivationCount,
            };
        }

        return new ClusterRuntimeSnapshot
        {
            ActiveSiloCount = hosts.Count,
            Silos = silos,
        };
    }
}
