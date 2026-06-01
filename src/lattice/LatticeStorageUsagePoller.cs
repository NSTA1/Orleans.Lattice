using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice;

/// <summary>
/// Per-silo background service that drives the byte-accurate storage-usage
/// gauges so they populate without any caller invoking
/// <see cref="ILattice.GetStorageUsageAsync"/>. On its configured cadence
/// (<see cref="LatticeOptions.StorageUsagePollInterval"/>) it calls the
/// cluster-wide admin grain's
/// <see cref="ILatticeAdmin.GetTotalStorageUsageAsync"/>, which fans out to
/// every registered tree's storage-usage aggregator. Each aggregator is a
/// single cluster-wide activation, so its publish lands on
/// <i>its own host silo's</i> <see cref="LatticeStorageUsageMetrics"/> sink -
/// which means each tree contributes its series on exactly one silo and a
/// cross-silo <c>sum by (tree)</c> counts it once, regardless of how many
/// silos run this poller.
/// <para>
/// Running the poller on every silo is intentional and safe: the per-tree
/// aggregator coalesces repeat fan-outs behind its
/// <see cref="LatticeOptions.StorageUsageCacheTtl"/> cache, so redundant
/// polls from sibling silos re-publish the cached report cheaply rather than
/// re-fanning out. If the silo that would "own" a poll dies, the surviving
/// silos keep the gauges fresh with no leader election. Migration is handled
/// by the sink's <see cref="LatticeStorageUsageMetrics.StalenessHorizon"/>:
/// when a tree's aggregator moves to another silo the old silo stops
/// refreshing that series and it expires there, so the tree does not appear
/// on two scrape targets at once.
/// </para>
/// <para>
/// The poller sets the sink's staleness horizon to a small multiple of its
/// poll interval so a series survives a few missed polls (a transient admin
/// fan-out failure) but expires promptly after a real migration.
/// </para>
/// </summary>
internal sealed class LatticeStorageUsagePoller(
    IGrainFactory grainFactory,
    LatticeStorageUsageMetrics metrics,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ILogger<LatticeStorageUsagePoller> logger,
    TimeProvider? timeProvider = null) : BackgroundService
{
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    /// <summary>
    /// Multiple of the poll interval after which an un-refreshed series is
    /// dropped from the sink. Four polls tolerates a few transient fan-out
    /// failures before a series is treated as stale (migrated away).
    /// </summary>
    private const int StalenessHorizonPolls = 4;

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var interval = optionsMonitor.Get(Options.DefaultName).StorageUsagePollInterval;
        if (interval <= TimeSpan.Zero)
        {
            // Poller disabled: the gauges populate only when the public API
            // is called. Leave the sink's default staleness horizon in place.
            logger.LogDebug("Storage-usage poller disabled (StorageUsagePollInterval <= 0).");
            return;
        }

        // Size the sink's staleness horizon off the poll cadence so a series
        // survives a few missed polls but expires promptly after a real
        // aggregator migration to a sibling silo.
        var horizon = interval * StalenessHorizonPolls;
        if (horizon < LatticeStorageUsageMetrics.DefaultStalenessHorizon)
        {
            horizon = LatticeStorageUsageMetrics.DefaultStalenessHorizon;
        }
        metrics.StalenessHorizon = horizon;

        using var timer = new PeriodicTimer(interval, _time);
        do
        {
            try
            {
                var admin = grainFactory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey);
                _ = await admin.GetTotalStorageUsageAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                // A transient fan-out failure (silo restart, registry not yet
                // ready during startup) must not kill the poller; the next
                // tick retries. The staleness horizon keeps the last-known
                // series alive across a handful of these.
                logger.LogDebug(ex, "Storage-usage poll failed; will retry on the next tick.");
            }
        }
        while (await SafeWaitAsync(timer, stoppingToken).ConfigureAwait(false));
    }

    private static async Task<bool> SafeWaitAsync(PeriodicTimer timer, CancellationToken stoppingToken)
    {
        try
        {
            return await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }
}
