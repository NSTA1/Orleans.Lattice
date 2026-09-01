using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice;

/// <summary>
/// Per-silo background service that drives the WAL garbage collector
/// (<see cref="ILatticeWalGc"/>) for every registered tree on a
/// backlog-responsive cadence, so a durable-WAL host gets bounded WAL
/// retention without any caller invoking
/// <see cref="ILatticeWalGc.RunOnceAsync"/> and without depending on the
/// replication package.
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
/// <b>Startup stagger.</b> The first pass is deliberately not run at
/// activation time: it is offset by a random delay in
/// <c>[<see cref="LatticeOptions.WalGcStartupDelay"/> / 2,
/// <see cref="LatticeOptions.WalGcStartupDelay"/>)</c> (30 seconds by
/// default, so 15 to 30 seconds) so the silo finishes activating before
/// the scheduler adds WAL scan/trim I/O, and so a rolling cluster restart
/// does not align every silo's full-tree fan-out into a correlated I/O
/// storm. The window is capped at <see cref="LatticeOptions.WalGcInterval"/>
/// so a host configured with a short cadence is never made to wait longer
/// than one interval. Before this knob existed the stagger was drawn from
/// <c>[interval / 2, interval)</c>, which at the default hourly cadence put
/// the first pass 30 to 60 minutes out - so a box recreated more often than
/// that never reclaimed a single WAL entry.
/// </para>
/// <para>
/// <b>Backlog-responsive cadence.</b> Each tree carries an independent
/// interval inside the closed band
/// <c>[<see cref="LatticeOptions.WalGcMinInterval"/>,
/// <see cref="LatticeOptions.WalGcInterval"/>]</c>. A pass that trims at
/// least one entry - the direct observation that the tree had backlog above
/// the trim floor - snaps that tree back to the floor so a fast-growing log
/// keeps being collected; a pass that trims nothing doubles the tree's
/// interval up to the configured ceiling, so an idle tree geometrically
/// relaxes and costs nothing. Because the state is per tree, a busy tree
/// never drags a quiet one into a tight loop and a quiet one never delays a
/// busy one. Setting <see cref="LatticeOptions.WalGcMinInterval"/> to zero
/// collapses the band to a single value and restores a fixed-interval tick.
/// </para>
/// <para>
/// This changes only <i>when</i> a pass runs. What a pass may reclaim is
/// unchanged: trim eligibility and the coverage-gated trim floor live in the
/// GC itself and are neither consulted nor relaxed from here.
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
/// <para>
/// Enablement is controlled by <see cref="LatticeOptions.WalGcInterval"/>,
/// a global knob read from the default (unnamed) options; set
/// <see cref="TimeSpan.Zero"/> (or any non-positive value) to disable the
/// scheduler and restore the historical caller-driven behaviour. All three
/// cadence knobs are read once at start.
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

    /// <summary>
    /// Per-tree cadence state, keyed by tree id. Bounded by the number of
    /// registered trees: an entry is seeded the first time a tree is seen and
    /// dropped once the registry stops reporting it, so a deleted tree cannot
    /// leak an entry for the life of the silo.
    /// <para>
    /// This and the two fields below are confined to the single
    /// <see cref="ExecuteAsync"/> loop - the only thing that ever runs a pass -
    /// so they need no synchronisation.
    /// </para>
    /// </summary>
    private readonly Dictionary<string, TreeCadence> _cadence = new(StringComparer.Ordinal);

    /// <summary>
    /// Wait applied when a pass observed no collectable tree at all - an empty
    /// registry, a registry that faulted, or a registry reporting only blank
    /// ids. Relaxes on each such pass exactly as a per-tree interval does, so a
    /// silo whose registry is briefly unavailable during startup retries soon
    /// while a permanently empty one settles at the configured ceiling.
    /// </summary>
    private TimeSpan _quietWait;

    /// <summary>
    /// Pass counter used to drop cadence state for trees the registry no longer
    /// reports. Pre-incremented, so a live generation is always 1 or greater and
    /// <c>0</c> is free to mark a never-observed entry.
    /// </summary>
    private int _generation;

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = optionsMonitor.Get(Options.DefaultName);
        var interval = options.WalGcInterval;
        if (interval <= TimeSpan.Zero)
        {
            // Explicitly disabled: the WAL is trimmed only by an
            // explicit RunOnceAsync caller (an admin trigger or the
            // replication maintenance grain for replicated trees).
            logger.LogDebug(
                "WAL GC scheduler disabled (WalGcInterval <= 0).");
            return;
        }

        // A non-positive floor disables the adaptive cadence, and a floor above
        // the ceiling is meaningless; both collapse the band to the configured
        // interval, which reproduces the historical fixed-interval tick exactly.
        var minInterval = options.WalGcMinInterval;
        if (minInterval <= TimeSpan.Zero || minInterval > interval)
        {
            minInterval = interval;
        }

        // Never make a host wait longer for its first pass than its own
        // configured ceiling.
        var startupWindow = options.WalGcStartupDelay;
        if (startupWindow > interval)
        {
            startupWindow = interval;
        }

        _quietWait = minInterval;

        if (!await SafeDelayAsync(RandomStartupDelay(startupWindow), stoppingToken).ConfigureAwait(false))
        {
            return;
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            var wait = await RunPassAsync(minInterval, interval, stoppingToken).ConfigureAwait(false);
            if (!await SafeDelayAsync(wait, stoppingToken).ConfigureAwait(false))
            {
                return;
            }
        }
    }

    /// <summary>
    /// Computes the randomized delay before the first GC pass: a uniform value
    /// in <c>[window / 2, window)</c>. The floor of half a window keeps the
    /// first pass out of the silo's activation storm, and the random component
    /// spreads the first pass across silos so a rolling restart does not align
    /// every silo's fan-out. A non-positive window means "no stagger": the first
    /// pass runs immediately.
    /// </summary>
    private static TimeSpan RandomStartupDelay(TimeSpan window)
    {
        if (window <= TimeSpan.Zero)
        {
            return TimeSpan.Zero;
        }

        var half = window / 2;
        return half + (half * Random.Shared.NextDouble());
    }

    private async Task<bool> SafeDelayAsync(TimeSpan delay, CancellationToken stoppingToken)
    {
        try
        {
            await Task.Delay(delay, _time, stoppingToken).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    /// <summary>
    /// Runs one scheduling pass: collects every registered tree whose adaptive
    /// interval has elapsed, updates each collected tree's next due time from
    /// what its pass reclaimed, and returns how long to sleep before the next
    /// pass (the earliest due time across every registered tree).
    /// <para>
    /// Per-tree failures are swallowed and logged so one wedged tree never
    /// stalls the cadence for the rest - a throwing tree relaxes on its own
    /// timeline while its siblings keep their own schedules. The registry
    /// enumeration is likewise guarded so a transient registry fault is retried
    /// rather than killing the scheduler.
    /// </para>
    /// </summary>
    private async Task<TimeSpan> RunPassAsync(TimeSpan minInterval, TimeSpan interval, CancellationToken stoppingToken)
    {
        IReadOnlyList<string> treeIds;
        try
        {
            var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            treeIds = await registry.GetAllTreeIdsAsync().ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            return minInterval;
        }
        catch (Exception ex)
        {
            // A transient fan-out failure (silo restart, registry not
            // yet ready during startup) must not kill the scheduler; the
            // next tick retries the whole pass.
            logger.LogDebug(
                ex,
                "WAL GC scheduler failed to enumerate trees; will retry on the next tick.");
            return Quiet(minInterval, interval);
        }

        var generation = ++_generation;
        var nowTicks = _time.GetUtcNow().UtcTicks;
        var earliestDueTicks = long.MaxValue;
        var tracked = 0;

        // Indexed rather than foreach: enumerating an IReadOnlyList<string>
        // through its interface boxes the underlying struct enumerator, and this
        // loop runs on every pass for every registered tree.
        for (var i = 0; i < treeIds.Count; i++)
        {
            var treeId = treeIds[i];
            if (stoppingToken.IsCancellationRequested)
            {
                return minInterval;
            }
            if (string.IsNullOrEmpty(treeId))
            {
                continue;
            }

            if (!_cadence.TryGetValue(treeId, out var cadence))
            {
                // First sighting: due immediately and seeded at the responsive
                // floor, so a freshly registered tree is collected on this pass
                // rather than waiting out an interval it was never scheduled in.
                // Generation 0 is the never-observed sentinel; a real generation
                // always starts at 1.
                cadence = new TreeCadence(minInterval.Ticks, nowTicks, 0);
            }

            // Counted per distinct entry, so a registry that reports an id twice
            // cannot inflate the count and suppress pruning.
            if (cadence.Generation != generation)
            {
                tracked++;
            }

            if (cadence.NextDueTicks > nowTicks)
            {
                _cadence[treeId] = cadence with { Generation = generation };
                if (cadence.NextDueTicks < earliestDueTicks)
                {
                    earliestDueTicks = cadence.NextDueTicks;
                }
                continue;
            }

            var next = await CollectTreeAsync(
                treeId,
                TimeSpan.FromTicks(cadence.IntervalTicks),
                minInterval,
                interval,
                stoppingToken).ConfigureAwait(false);

            var dueTicks = _time.GetUtcNow().UtcTicks + next.Ticks;
            _cadence[treeId] = new TreeCadence(next.Ticks, dueTicks, generation);
            if (dueTicks < earliestDueTicks)
            {
                earliestDueTicks = dueTicks;
            }
        }

        PruneRetiredTrees(generation, tracked);

        if (earliestDueTicks == long.MaxValue)
        {
            // No collectable tree is registered yet. Relax on the same schedule
            // a quiet tree would, so an empty silo costs nothing while a silo
            // whose first tree is about to register still picks it up promptly.
            return Quiet(minInterval, interval);
        }

        _quietWait = minInterval;
        var wait = earliestDueTicks - _time.GetUtcNow().UtcTicks;
        if (wait <= 0)
        {
            return TimeSpan.Zero;
        }

        return TimeSpan.FromTicks(wait > interval.Ticks ? interval.Ticks : wait);
    }

    /// <summary>
    /// Runs one GC pass for a single tree, publishes its metering, and returns
    /// the interval to wait before collecting that tree again: the floor when
    /// the pass reclaimed entries (backlog was present above the trim floor), a
    /// relaxed interval otherwise.
    /// </summary>
    private async Task<TimeSpan> CollectTreeAsync(
        string treeId,
        TimeSpan currentInterval,
        TimeSpan minInterval,
        TimeSpan interval,
        CancellationToken stoppingToken)
    {
        var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, treeId);
        var tenantTag = LatticeTenantLabel.ForTree(treeId);
        TimeSpan next;
        try
        {
            var report = await gc.RunOnceAsync(treeId, stoppingToken).ConfigureAwait(false);

            // EntriesTrimmed is the count the pass found eligible under the GC's
            // own predicate, so a positive value is a direct observation of
            // backlog above the trim floor. Reading it here neither widens nor
            // narrows that predicate.
            var reclaimed = report.EntriesTrimmed > 0;
            LatticeMetrics.WalGcPasses.Add(
                1,
                treeTag,
                reclaimed ? LatticeMetrics.OutcomeReclaimed : LatticeMetrics.OutcomeIdle,
                tenantTag);

            // Backlog metering. Byte accounting is a provider capability, so this
            // is an explicit two-branch decision rather than a silent skip; see
            // PublishBacklogBytes for the contract a consumer relies on.
            PublishBacklogBytes(report.RetainedBytesAfter, treeTag, tenantTag);

            next = reclaimed ? minInterval : Relax(currentInterval, minInterval, interval);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host shutdown, not a tree fault: leave the cadence where it was
            // and do not record a failed pass.
            return currentInterval;
        }
        catch (Exception ex)
        {
            LatticeMetrics.WalGcPasses.Add(1, treeTag, LatticeMetrics.OutcomeFailed, tenantTag);
            logger.LogDebug(
                ex,
                "WAL GC pass failed for tree {Tree}; will retry on the next tick.",
                treeId);

            // A wedged tree relaxes on its own timeline rather than retrying at
            // the floor forever, and its siblings keep their own schedules.
            next = Relax(currentInterval, minInterval, interval);
        }

        LatticeMetrics.WalGcInterval.Record(next.TotalSeconds, treeTag, tenantTag);
        return next;
    }

    /// <summary>
    /// Publishes the post-pass retained-byte backlog for a tree, when the pass
    /// measured one.
    /// <para>
    /// Byte accounting is a capability of the configured
    /// <see cref="IWalStorageProvider"/> gated behind the byte-pressure policy
    /// (<see cref="LatticeOptions.WalMaxRetainedBytes"/>), so
    /// <see cref="LatticeWalGcReport.RetainedBytesAfter"/> is
    /// <see langword="null"/> on a host that has either turned off. That is a
    /// defined branch, not an incidental skip: nothing is recorded, and the
    /// absence is positively identifiable because
    /// <see cref="LatticeMetrics.WalGcPasses"/> is emitted for every pass
    /// regardless. A tree reporting passes but no backlog-byte samples is
    /// therefore knowably "not measured" rather than ambiguously "no backlog",
    /// and its reclaimed volume is still observable in records through
    /// <see cref="LatticeMetrics.WalEntriesTrimmed"/> and the
    /// <see cref="LatticeMetrics.OutcomeReclaimed"/> pass outcome.
    /// </para>
    /// </summary>
    private static void PublishBacklogBytes(
        long? retainedBytesAfter,
        in KeyValuePair<string, object?> treeTag,
        in KeyValuePair<string, object?> tenantTag)
    {
        if (retainedBytesAfter is not { } backlogBytes)
        {
            return;
        }

        LatticeMetrics.WalGcBacklogBytes.Record(backlogBytes, treeTag, tenantTag);
    }

    /// <summary>
    /// Doubles <paramref name="current"/> toward <paramref name="max"/>, never
    /// below <paramref name="min"/>. The ceiling test also guards the overflow:
    /// the doubling only runs when the result is provably below the ceiling.
    /// </summary>
    private static TimeSpan Relax(TimeSpan current, TimeSpan min, TimeSpan max)
    {
        var ticks = current.Ticks;
        if (ticks < min.Ticks)
        {
            ticks = min.Ticks;
        }

        return ticks >= max.Ticks / 2 ? max : TimeSpan.FromTicks(ticks * 2);
    }

    /// <summary>
    /// Returns the current no-collectable-tree wait and relaxes it for next
    /// time, so a silo with an empty or faulting registry retries promptly once
    /// and then backs off on the same geometric schedule a quiet tree does,
    /// instead of polling at the floor indefinitely.
    /// </summary>
    private TimeSpan Quiet(TimeSpan minInterval, TimeSpan interval)
    {
        var wait = _quietWait < minInterval ? minInterval : _quietWait;
        _quietWait = Relax(wait, minInterval, interval);
        return wait;
    }

    /// <summary>
    /// Drops cadence state for trees the registry no longer reports. Only walks
    /// the map when it holds more entries than this pass tracked, and removes in
    /// place - <see cref="Dictionary{TKey, TValue}"/> permits removal during
    /// enumeration - so the common no-churn case is a single integer comparison
    /// and the churn case allocates nothing.
    /// </summary>
    private void PruneRetiredTrees(int generation, int tracked)
    {
        if (_cadence.Count <= tracked)
        {
            return;
        }

        foreach (var entry in _cadence)
        {
            if (entry.Value.Generation != generation)
            {
                _cadence.Remove(entry.Key);
            }
        }
    }

    /// <summary>
    /// One tree's adaptive cadence state.
    /// </summary>
    /// <param name="IntervalTicks">The interval most recently selected for the tree, in ticks.</param>
    /// <param name="NextDueTicks">UTC tick count at which the tree becomes collectable again.</param>
    /// <param name="Generation">Pass counter that last observed the tree in the registry.</param>
    private readonly record struct TreeCadence(long IntervalTicks, long NextDueTicks, int Generation);
}
