using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice;

/// <summary>
/// Per-silo background service that drives the byte-accurate storage-usage
/// gauges so the WAL-bytes and over-threshold series populate without any
/// caller invoking <see cref="ILattice.GetStorageUsageAsync"/>. On its
/// configured cadence (<see cref="LatticeOptions.StorageUsagePollInterval"/>)
/// it calls the cluster-wide admin grain's
/// <see cref="ILatticeAdmin.PollWalUsageAsync"/>, which fans out to every
/// registered tree's <i>WAL-only</i> aggregator. Each WAL-only aggregator
/// is a single cluster-wide activation, so its publish lands on
/// <i>its own host silo's</i> <see cref="LatticeStorageUsageMetrics"/> sink -
/// which means each tree contributes its series on exactly one silo and a
/// cross-silo <c>sum by (tree)</c> counts it once, regardless of how many
/// silos run this poller.
/// <para>
/// The WAL poll path is intentionally activation-free for leaves, internal
/// nodes, snapshot storage grains, and shard roots: it touches only WAL
/// partition grains. The previous design drove
/// <see cref="ILatticeAdmin.GetTotalStorageUsageAsync"/> on every tick,
/// which descended every shard's leaf chain and activated every per-leaf
/// snapshot grain - pinning cold trees fully resident and defeating the
/// activation-on-demand model. Snapshot, leaf-state, and total-bytes
/// gauges populate on demand via
/// <see cref="ILattice.GetStorageUsageAsync"/> and the operator-driven
/// <see cref="ILatticeAdmin.RefreshStorageUsageAsync"/>, and - when
/// <see cref="LatticeOptions.StorageUsageDeepPollInterval"/> is set to a
/// positive value - on a separate, slower deep-poll cadence that calls the
/// non-force <see cref="ILatticeAdmin.GetTotalStorageUsageAsync"/>. That deep
/// read consumes each shard root's O(1) incrementally-maintained byte totals
/// (it never walks the leaf chain), so it activates only the shard roots and
/// never pins idle leaves resident. The deep poll defaults to disabled
/// (<see cref="TimeSpan.Zero"/>) to preserve the activation-light poll.
/// </para>
/// <para>
/// Running the poller on every silo is intentional and safe: redundant
/// polls from sibling silos re-publish the cached WAL sample cheaply.
/// If the silo that would "own" a poll dies, the surviving silos keep
/// the gauges fresh with no leader election. Migration is handled by the
/// sink's <see cref="LatticeStorageUsageMetrics.StalenessHorizon"/>: when
/// a tree's aggregator moves to another silo the old silo stops refreshing
/// that series and it expires there, so the tree does not appear on two
/// scrape targets at once.
/// </para>
/// <para>
/// The poller sets the sink's staleness horizon to a small multiple of its
/// slowest active poll cadence so a series survives a few missed polls (a
/// transient admin fan-out failure) but expires promptly after a real
/// migration.
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

    /// <summary>
    /// Longest period <see cref="PeriodicTimer"/> accepts. A configured cadence
    /// beyond this is clamped rather than allowed to throw out of the loop: an
    /// out-of-range knob should degrade the poller, not fault
    /// <see cref="ExecuteAsync"/> and take the host down with it under
    /// <see cref="BackgroundServiceExceptionBehavior.StopHost"/>.
    /// </summary>
    private static readonly TimeSpan MaxPollInterval = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await RunAsync(stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Normal shutdown.
        }
        catch (Exception ex)
        {
            // Nothing the poller can fail at is worth stopping the host for.
            // BackgroundService's default StopHost behaviour would otherwise
            // turn an unusable options value or a startup-ordering hiccup into
            // a host-wide outage; degrade to "gauges populate on demand only",
            // which is exactly the configured-off behaviour.
            logger.LogError(
                ex,
                "Storage-usage poller stopped after an unrecoverable error; storage gauges will populate only when the public API is called.");
        }
    }

    private async Task RunAsync(CancellationToken stoppingToken)
    {
        var options = optionsMonitor.Get(Options.DefaultName);
        var walInterval = ClampInterval(options.StorageUsagePollInterval);
        var deepInterval = ClampInterval(options.StorageUsageDeepPollInterval);

        var walEnabled = walInterval > TimeSpan.Zero;
        var deepEnabled = deepInterval > TimeSpan.Zero;

        if (!walEnabled && !deepEnabled)
        {
            // Poller disabled: the gauges populate only when the public API
            // is called. Leave the sink's default staleness horizon in place.
            logger.LogDebug(
                "Storage-usage poller disabled (StorageUsagePollInterval and StorageUsageDeepPollInterval <= 0).");
            return;
        }

        // Size the sink's staleness horizon off the slowest active cadence so a
        // series survives a few missed polls but expires promptly after a real
        // aggregator migration to a sibling silo. The deep gauges are refreshed
        // only by the deep loop, so the horizon must outlast the deep cadence
        // when it is the slower of the two.
        var slowest = walEnabled ? walInterval : TimeSpan.Zero;
        if (deepEnabled && deepInterval > slowest)
        {
            slowest = deepInterval;
        }

        var horizon = slowest * StalenessHorizonPolls;
        if (horizon < LatticeStorageUsageMetrics.DefaultStalenessHorizon)
        {
            horizon = LatticeStorageUsageMetrics.DefaultStalenessHorizon;
        }
        metrics.StalenessHorizon = horizon;

        var loops = new List<Task>(2);
        if (walEnabled)
        {
            loops.Add(RunPollLoopAsync(walInterval, deep: false, stoppingToken));
        }
        if (deepEnabled)
        {
            loops.Add(RunPollLoopAsync(deepInterval, deep: true, stoppingToken));
        }

        await Task.WhenAll(loops).ConfigureAwait(false);
    }

    /// <summary>
    /// Clamps a configured cadence into the range <see cref="PeriodicTimer"/>
    /// accepts. A non-positive value is left alone: the caller treats that as
    /// "disabled" before any timer is built.
    /// </summary>
    private static TimeSpan ClampInterval(TimeSpan interval)
        => interval > MaxPollInterval ? MaxPollInterval : interval;

    /// <summary>
    /// Drives one poll cadence. The <paramref name="deep"/> loop calls the
    /// admin grain's non-force deep aggregator
    /// (<see cref="ILatticeAdmin.GetTotalStorageUsageAsync"/>) so the
    /// snapshot-bytes, leaf-state-bytes, and total-bytes gauges populate; the
    /// WAL loop calls <see cref="ILatticeAdmin.PollWalUsageAsync"/>. Both fire
    /// immediately, then on their own interval.
    /// </summary>
    private async Task RunPollLoopAsync(TimeSpan interval, bool deep, CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(interval, _time);
        do
        {
            try
            {
                var admin = grainFactory.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey);
                if (deep)
                {
                    // Non-force deep read: reads each shard root's O(1)
                    // incrementally-maintained byte totals; it never walks the
                    // leaf chain or activates per-leaf snapshot grains, so it
                    // does not pin cold trees resident.
                    await admin.GetTotalStorageUsageAsync(stoppingToken).ConfigureAwait(false);
                }
                else
                {
                    await admin.PollWalUsageAsync(stoppingToken).ConfigureAwait(false);
                }
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
                logger.LogDebug(
                    ex,
                    "Storage-usage {Kind} poll failed; will retry on the next tick.",
                    deep ? "deep" : "WAL");
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
