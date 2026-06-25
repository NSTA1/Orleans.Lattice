using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice;

/// <summary>
/// Per-silo background service that drives the WAL garbage collector
/// (<see cref="ILatticeWalGc"/>) for every registered tree on a fixed
/// cadence, so a durable-WAL host gets bounded WAL retention without
/// any caller invoking <see cref="ILatticeWalGc.RunOnceAsync"/> and
/// without depending on the replication package.
/// <para>
/// The core library ships the WAL GC but, before this scheduler, the
/// only production driver of <see cref="ILatticeWalGc.RunOnceAsync"/>
/// was the replication package's per-tree maintenance grain. That left
/// two retention gaps: a durable-WAL host without replication never
/// trimmed its WAL at all, and every <i>non-replicated</i> tree in a
/// replicated host was never collected. Both grew without bound and
/// made <see cref="LatticeOptions.WalRetention"/> inert. This scheduler
/// closes the gap by running a GC pass for every tree the registry
/// reports, replicated or not.
/// </para>
/// <para>
/// Enablement and cadence are controlled by
/// <see cref="LatticeOptions.WalGcInterval"/>, a global knob read from
/// the default (unnamed) options. It defaults to 1 hour
/// (<b>enabled</b>), so the WAL of every registered tree is trimmed
/// at least hourly and <see cref="LatticeOptions.WalRetention"/> is
/// effective out of the box. A pass is retention housekeeping rather
/// than a latency-sensitive operation, so the coarse default keeps
/// the storage cost low; set <see cref="TimeSpan.Zero"/> (or any
/// non-positive value) to disable the scheduler and restore the
/// historical caller-driven behaviour. The cadence is read once at
/// start.
/// </para>
/// <para>
/// Running on every silo is safe and composes with the replication
/// maintenance grain: <see cref="ILatticeWalGc.RunOnceAsync"/> and the
/// underlying <see cref="IWalStorageProvider.TrimAsync"/> are
/// idempotent, the GC scan is conservative (it stops at the first
/// non-eligible entry and never trims past the minimum consumer cursor
/// or the leaf-materialiser checkpoint floor), and a silo that cannot
/// resolve a partition's pinned provider skips it. A redundant pass
/// from a sibling silo therefore at worst issues a duplicate trim that
/// the provider collapses to a no-op.
/// </para>
/// </summary>
internal sealed class LatticeWalGcScheduler(
    IGrainFactory grainFactory,
    ILatticeWalGc gc,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ILogger<LatticeWalGcScheduler> logger,
    TimeProvider? timeProvider = null) : BackgroundService
{
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var interval = optionsMonitor.Get(Options.DefaultName).WalGcInterval;
        if (interval <= TimeSpan.Zero)
        {
            // Explicitly disabled: the WAL is trimmed only by an
            // explicit RunOnceAsync caller (an admin trigger or the
            // replication maintenance grain for replicated trees).
            logger.LogDebug(
                "WAL GC scheduler disabled (WalGcInterval <= 0).");
            return;
        }

        using var timer = new PeriodicTimer(interval, _time);
        do
        {
            await RunPassAsync(stoppingToken).ConfigureAwait(false);
        }
        while (await SafeWaitAsync(timer, stoppingToken).ConfigureAwait(false));
    }

    /// <summary>
    /// Runs a single GC pass over every registered tree. Per-tree
    /// failures are swallowed and logged so one wedged tree never
    /// stalls the cadence for the rest; the registry enumeration is
    /// likewise guarded so a transient registry fault is retried on the
    /// next tick rather than killing the scheduler.
    /// </summary>
    private async Task RunPassAsync(CancellationToken stoppingToken)
    {
        IReadOnlyList<string> treeIds;
        try
        {
            var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            treeIds = await registry.GetAllTreeIdsAsync().ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            // A transient fan-out failure (silo restart, registry not
            // yet ready during startup) must not kill the scheduler; the
            // next tick retries the whole pass.
            logger.LogDebug(
                ex,
                "WAL GC scheduler failed to enumerate trees; will retry on the next tick.");
            return;
        }

        foreach (var treeId in treeIds)
        {
            if (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            if (string.IsNullOrEmpty(treeId))
            {
                continue;
            }
            try
            {
                await gc.RunOnceAsync(treeId, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                logger.LogDebug(
                    ex,
                    "WAL GC pass failed for tree {Tree}; will retry on the next tick.",
                    treeId);
            }
        }
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
