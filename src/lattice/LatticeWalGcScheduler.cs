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
    TimeProvider? timeProvider = null,
    BPlusTree.Grains.SnapshotPinCensus? snapshotPins = null) : BackgroundService
{
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;

    /// <summary>
    /// Longest delay <see cref="Task.Delay(TimeSpan, TimeProvider, CancellationToken)"/>
    /// accepts. A larger value throws <see cref="ArgumentOutOfRangeException"/>
    /// synchronously, so every delay this scheduler arms is clamped to it
    /// first. None of the three cadence knobs
    /// (<see cref="LatticeOptions.WalGcInterval"/>,
    /// <see cref="LatticeOptions.WalGcMinInterval"/>,
    /// <see cref="LatticeOptions.WalGcStartupDelay"/>) carries an upper bound or
    /// is validated, so a startup stagger, cadence wait, or quiet backoff longer
    /// than this (a configured interval above roughly 49 days) would otherwise
    /// throw out of <see cref="SafeDelayAsync"/> and, because this is a
    /// <see cref="BackgroundService"/> under the default
    /// <see cref="BackgroundServiceExceptionBehavior.StopHost"/>, take the whole
    /// silo host down. Clamping wakes the scheduler at least this often to
    /// re-evaluate, which is strictly more collection than the extreme interval
    /// asked for, never less. This mirrors the guard
    /// <see cref="LatticeStorageUsagePoller"/> already applies to its
    /// <see cref="System.Threading.PeriodicTimer"/> cadence.
    /// </summary>
    private static readonly TimeSpan MaxSchedulableDelay = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

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

    /// <summary>
    /// Trees whose zero-primed WAL-retention counter series have already been
    /// emitted. Confined to the <see cref="ExecuteAsync"/> loop like the fields
    /// above, and pruned alongside <see cref="_cadence"/> so a deleted tree
    /// cannot leak an entry for the life of the silo. Re-priming a tree that
    /// was retired and re-registered is harmless - the prime adds zero.
    /// </summary>
    private readonly HashSet<string> _primedTrees = new(StringComparer.Ordinal);

    /// <summary>
    /// Minimum time a tree must have been continuously blocked by the same
    /// consumer before the sweep will reactivate its leaf.
    /// </summary>
    /// <remarks>
    /// A blocked tree polls at the interval floor, so without this delay the
    /// very first blocked pass after a silo restart would reactivate leaves -
    /// exactly when a deployment is least able to absorb the load, and before
    /// the ordinary activation traffic that would have healed them for free has
    /// had any chance to. Blocked-ness that clears on its own inside this
    /// window costs nothing.
    /// </remarks>
    private static readonly TimeSpan ReactivationMinBlockAge = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Minimum interval between reactivation attempts for the same blocking
    /// consumer.
    /// </summary>
    /// <remarks>
    /// A touch that heals clears the block within a pass or two, so anything
    /// still blocked after this long did not heal and will not heal by being
    /// touched again immediately. Without the cooldown a blocked tree polling
    /// at the 30s floor would reactivate the same leaf 120 times an hour.
    /// </remarks>
    private static readonly TimeSpan ReactivationRetryCooldown = TimeSpan.FromMinutes(15);

    /// <summary>
    /// Reactivation attempts permitted per blocking consumer before the sweep
    /// gives up on it and reports it as abandoned.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The budget exists because reactivation is <b>not</b> guaranteed to heal.
    /// A leaf whose snapshot capture cannot complete - the live case is an
    /// <c>OutOfMemoryException</c> serialising an oversized leaf - activates,
    /// fails, and deactivates still holding its blocking pin. The
    /// activation-time repair already retries such a leaf without backpressure,
    /// so an unbounded sweep would stack a second retry loop on top of a first,
    /// aimed at precisely the leaves least able to absorb it.
    /// </para>
    /// <para>
    /// Giving up is therefore the correct behaviour and not a gap: the block is
    /// reported as an alarm that needs a different remedy, rather than being
    /// retried forever at the cadence floor.
    /// </para>
    /// </remarks>
    private const int MaxReactivationAttempts = 3;

    /// <summary>
    /// Per-tree record of the consumer currently blocking its cursor floor and
    /// what the sweep has done about it. Bounded by the number of blocked trees
    /// and pruned alongside <see cref="_cadence"/>.
    /// </summary>
    private readonly Dictionary<string, BlockedConsumerObservation> _blockedConsumers =
        new(StringComparer.Ordinal);

    /// <summary>
    /// One tree's blocked-consumer observation.
    /// </summary>
    /// <param name="ConsumerId">The consumer reported as blocking the floor.</param>
    /// <param name="FirstObserved">When this consumer was first seen blocking.</param>
    /// <param name="LastReactivationAttempt">When the sweep last touched it, if ever.</param>
    /// <param name="Attempts">How many reactivations the sweep has issued for it.</param>
    /// <param name="Abandoned">Whether the attempt budget has been spent and reported.</param>
    private readonly record struct BlockedConsumerObservation(
        string ConsumerId,
        DateTimeOffset FirstObserved,
        DateTimeOffset? LastReactivationAttempt,
        int Attempts,
        bool Abandoned);

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
        // Clamp before delegating: Task.Delay throws ArgumentOutOfRangeException
        // synchronously for a delay above MaxSchedulableDelay, and that throw
        // would escape ExecuteAsync and stop the host. An out-of-range cadence
        // knob must degrade the scheduler, never fault the silo.
        if (delay > MaxSchedulableDelay)
        {
            delay = MaxSchedulableDelay;
        }

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

            var dueTicks = SaturatingDueTicks(_time.GetUtcNow().UtcTicks, next.Ticks);
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

        // Zero-prime the WAL-retention series a reader has to interpret an
        // absence on. A Counter publishes no series until its first Add, so a
        // tree that has never shed a pin report, never held a snapshot pin, and
        // never reclaimed is silent on all three - a shape indistinguishable
        // from a dead subsystem or a broken instrument (issues #2694, #2774).
        // Priming here, once per tree, turns that silence into an exported
        // zero, which is a measurement. The WAL GC scheduler is the right
        // primer because it enumerates exactly the trees whose retention is
        // being collected, which is the population the question is asked about.
        // Adding zero cannot perturb any of the values.
        //
        // The call site is load-bearing and must stay the first statement of
        // the pass: above the snapshot reconcile, above the try, and above
        // every early return. That is what makes "no series at all for a tree"
        // mean precisely "the scheduler never evaluated this tree", rather than
        // leaving it ambiguous with "evaluated and returned early".
        PrimeRetentionSeries(treeId, treeTag, tenantTag);

        // Re-derive the tree's live snapshot-pin set from the cursor registry.
        // The gauge's membership is asserted at the two sites that mutate a
        // registry entry, but an assertion can be lost - an activation torn
        // between the unregister and the mark, a registry shared across silos,
        // a future consumer that releases a snapshot pin without going through
        // the cursor grain. Re-deriving here makes the series self-healing: the
        // registry is the authority on which pins actually hold the trim floor
        // down, and this is the pass that evaluates that floor. Best-effort, and
        // deliberately outside the try below: a metering read must not be able
        // to mark a GC pass failed.
        if (snapshotPins is not null)
        {
            try
            {
                await snapshotPins.ReconcileAsync(treeId, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return currentInterval;
            }
            catch (Exception ex)
            {
                logger.LogDebug(
                    ex,
                    "Snapshot pin census reconcile failed for tree {Tree}; the gauge keeps its last derived value.",
                    treeId);
            }
        }

        TimeSpan next;
        try
        {
            var report = await gc.RunOnceAsync(treeId, stoppingToken).ConfigureAwait(false);

            // EntriesTrimmed is the count the pass found eligible under the GC's
            // own predicate, so a positive value is a direct observation of
            // backlog above the trim floor. Reading it here neither widens nor
            // narrows that predicate.
            var reclaimed = report.EntriesTrimmed > 0;

            // A pass that reclaimed nothing did so for one of two opposite
            // reasons, and until now both were labelled "idle". Either the tree
            // was quiet - nothing above the trim floor, which is the healthy
            // steady state - or the cursor branch was disabled outright by an
            // unusable durable materialiser pin, in which case the tree cannot
            // reclaim at all and its WAL is growing without bound (issue #2702).
            var blocked = !reclaimed
                && report.CursorFloorState == WalGcCursorFloorState.BlockedByUnusablePin;

            RecordPass(
                1,
                reclaimed
                    ? LatticeMetrics.OutcomeReclaimed
                    : blocked ? LatticeMetrics.OutcomeBlocked : LatticeMetrics.OutcomeIdle,
                treeTag,
                tenantTag);

            // Backlog metering. Byte accounting is a provider capability, so this
            // is an explicit two-branch decision rather than a silent skip; see
            // PublishBacklogBytes for the contract a consumer relies on.
            PublishBacklogBytes(report.RetainedBytesAfter, report.ByteCeiling, treeTag, tenantTag);

            // Self-healing remedy for the blocked condition, not just a label
            // for it (issue #2710 Limitation 2). A blocked tree stays blocked
            // until the offending leaf activates and replays, and nothing on
            // the leaf can drive that: the floor skips every consumer present
            // in the live cursor registry before it evaluates the pin, so a
            // blocking consumer is by construction one whose leaf is NOT
            // activated. There is no activation for a leaf-local trigger to run
            // in, which is why the retention path has to be the one to act.
            if (blocked && report.BlockingConsumerId is { Length: > 0 } blockingConsumerId)
            {
                await ObserveAndHealBlockedTreeAsync(
                    treeId, blockingConsumerId, treeTag, tenantTag, stoppingToken).ConfigureAwait(false);
            }
            else
            {
                ClearBlockedObservation(treeId, treeTag, tenantTag);
            }

            // Hold a blocked tree at the floor instead of relaxing it. The same
            // boolean used to drive both the label and the backoff, so a starved
            // tree was backed off exactly like a quiet one and drifted toward the
            // ceiling - fewest passes precisely when it needed the most. The
            // backoff was therefore self-reinforcing: being unable to reclaim was
            // itself the evidence used to decide to look less often.
            //
            // This matters beyond presentation because a repair is only half the
            // story. Whatever unblocks the pin, the stranded bytes do not come
            // back until a GC pass runs and trims them, so this interval is the
            // time-to-reclaim even when it is not the time-to-unblock. At stock
            // defaults that is 30s here against up to 1h before, and against the
            // ~2h ceiling a tuned deployment can reach.
            //
            // The floor introduces no new load level: a reclaiming tree already
            // runs at minInterval indefinitely, so this is a cadence the system
            // sustains by construction. It is self-limiting - a healed tree stops
            // being blocked and either reclaims (floor, legitimately) or goes
            // idle (relaxes as before) - and genuinely quiet trees are untouched.
            // The residual is deliberate: a tree blocked and unable to heal polls
            // at the floor indefinitely. That is the alarm state, and its cost is
            // bounded by the floor while the damage it signals is not.
            next = reclaimed || blocked ? minInterval : Relax(currentInterval, minInterval, interval);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host shutdown, not a tree fault: leave the cadence where it was
            // and do not record a failed pass.
            return currentInterval;
        }
        catch (Exception ex)
        {
            RecordPass(1, LatticeMetrics.OutcomeFailed, treeTag, tenantTag);
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
    /// measured one, and otherwise publishes the positive "not measured" signal
    /// naming why.
    /// <para>
    /// Byte accounting is a capability of the configured
    /// <see cref="IWalStorageProvider"/> gated behind the byte-pressure policy
    /// (<see cref="LatticeOptions.WalMaxRetainedBytes"/>), so
    /// <see cref="LatticeWalGcReport.RetainedBytesAfter"/> is
    /// <see langword="null"/> on a host that has either turned off.
    /// </para>
    /// <para>
    /// That branch used to record nothing at all, leaving the absence knowable
    /// only by <i>inferring</i> it from <see cref="LatticeMetrics.WalGcBacklogBytes"/>
    /// having no series while <see cref="LatticeMetrics.WalGcPasses"/> did. An
    /// inference from silence is exactly the reasoning step that produced a
    /// wrong, retracted root-cause diagnosis on this repository (issue #2692),
    /// so the branch now states itself through
    /// <see cref="LatticeMetrics.WalGcBacklogBytesUnavailable"/>, tagged with the
    /// reason a reader would act on: <c>policy_disabled</c> when no ceiling is
    /// configured (set <see cref="LatticeOptions.WalMaxRetainedBytes"/>), or
    /// <c>provider_unsupported</c> when a ceiling <i>is</i> configured and the
    /// provider still reported no retained byte size (change provider).
    /// Reclaimed volume in that configuration remains observable in records
    /// through <see cref="LatticeMetrics.WalEntriesTrimmed"/> and the
    /// <see cref="LatticeMetrics.OutcomeReclaimed"/> pass outcome.
    /// </para>
    /// </summary>
    private static void PublishBacklogBytes(
        long? retainedBytesAfter,
        long? byteCeiling,
        in KeyValuePair<string, object?> treeTag,
        in KeyValuePair<string, object?> tenantTag)
    {
        if (retainedBytesAfter is not { } backlogBytes)
        {
            LatticeMetrics.WalGcBacklogBytesUnavailable.Add(
                1,
                treeTag,
                byteCeiling is null
                    ? LatticeMetrics.ReasonBytePolicyDisabled
                    : LatticeMetrics.ReasonByteProviderUnsupported,
                tenantTag);
            return;
        }

        LatticeMetrics.WalGcBacklogBytes.Record(backlogBytes, treeTag, tenantTag);
    }

    /// <summary>
    /// Emits a one-time zero observation for each WAL-retention series that a
    /// reader must be able to distinguish "measured, never happened" from "not
    /// reporting" on, so the series exists before its first real event.
    /// Idempotent per tree: the primed set is consulted on every pass and the
    /// instruments are touched only on the first.
    /// </summary>
    private void PrimeRetentionSeries(
        string treeId,
        in KeyValuePair<string, object?> treeTag,
        in KeyValuePair<string, object?> tenantTag)
    {
        if (!_primedTrees.Add(treeId))
        {
            return;
        }

        LatticeMetrics.MaterialiserPinReportsShed.Add(0, treeTag, tenantTag);

        // The snapshot-pin series is an observable gauge derived from the
        // cursor registry (issue #2700), so it is primed by registering the
        // tree rather than by adding zero: the callback then emits an explicit
        // 0 for a tree holding no pin, instead of no series at all.
        snapshotPins?.Track(treeId);

        // Every outcome arm of the pass counter is primed, not just one (issue
        // #2774). Two distinct reasons converge on the same remedy.
        //
        // The blocked arm is primed so it survives a return to zero. A repair
        // that unblocks a tree makes the counter stop advancing, and an
        // unprimed counter that never fired on this silo exports nothing at
        // all - so "healthy, never blocked" and "not reporting" would be
        // identical at exactly the moment a reader needs to tell them apart,
        // which is when confirming a fix held.
        //
        // The other three are primed so an absent arm cannot be read as a
        // verdict. The reclaimed arm is the acute case, because it backs an
        // acceptance criterion: unprimed, "reclamation never happened" and
        // "the instrument is unwired" are the same reading, so a system that
        // reclaimed perfectly is indistinguishable from one that never ran - a
        // success misread as a failure, which is the most expensive wrong
        // answer a predicate can give.
        //
        // There is a second, subtler property this replaces. The three
        // non-failed arms are selected by a ternary inside a single Add call,
        // so today the presence of any one of them proves the site executed
        // for this tree - which is why a scrape carrying only blocked and idle
        // was still readable as evidence. That inference is incidental to how
        // the expression happens to be written: splitting the ternary into
        // three calls would destroy it silently, with no test failing. Priming
        // each arm makes the guarantee structural, so a reader no longer has
        // to know the shape of the emission to interpret an absence.
        //
        // Priming per tree also keeps a single re-stranded tree visible rather
        // than averaged away across the fleet.
        RecordPass(0, LatticeMetrics.OutcomeReclaimed, treeTag, tenantTag);
        RecordPass(0, LatticeMetrics.OutcomeIdle, treeTag, tenantTag);
        RecordPass(0, LatticeMetrics.OutcomeBlocked, treeTag, tenantTag);
        RecordPass(0, LatticeMetrics.OutcomeFailed, treeTag, tenantTag);
    }

    /// <summary>
    /// The single site that writes <see cref="LatticeMetrics.WalGcPasses"/>.
    /// </summary>
    /// <remarks>
    /// Both the real pass emissions and the zero primes route through here, so
    /// a primed series and the emission it anticipates carry an identical tag
    /// set by construction rather than by inspection. Repeating the tag list at
    /// each call site makes a divergence expressible, and a prime whose tags
    /// differ from its emission is worse than no prime at all: it mints a
    /// second series that is permanently zero while the one a reader queries
    /// stays absent, so the absence the prime exists to remove survives behind
    /// a decoy that looks like the fix landed.
    /// </remarks>
    private static void RecordPass(
        long delta,
        in KeyValuePair<string, object?> outcome,
        in KeyValuePair<string, object?> treeTag,
        in KeyValuePair<string, object?> tenantTag)
        => LatticeMetrics.WalGcPasses.Add(delta, treeTag, outcome, tenantTag);

    /// <summary>
    /// Adds <paramref name="addTicks"/> to <paramref name="nowTicks"/>,
    /// saturating at <see cref="long.MaxValue"/> so the next-due time can never
    /// overflow to a negative (past) tick count. <see cref="LatticeOptions.WalGcInterval"/>
    /// carries no upper bound and is not validated, so an operator-supplied
    /// interval near <see cref="TimeSpan.MaxValue"/> would otherwise wrap the
    /// sum negative: the tree would then compare due on every subsequent pass,
    /// <c>earliestDueTicks</c> would go negative, and the pass would return
    /// <see cref="TimeSpan.Zero"/> - driving the scheduler into a zero-wait busy
    /// loop, the exact opposite of the rare collection an extreme interval
    /// expresses. Saturating instead schedules the tree far in the future and
    /// lets the scheduler quiesce.
    /// </summary>
    private static long SaturatingDueTicks(long nowTicks, long addTicks)
        => addTicks > long.MaxValue - nowTicks ? long.MaxValue : nowTicks + addTicks;

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
                _primedTrees.Remove(entry.Key);
                _blockedConsumers.Remove(entry.Key);
                snapshotPins?.Forget(entry.Key);
            }
        }
    }

    /// <summary>
    /// Observes a tree reported as blocked by a named consumer and, subject to
    /// a minimum block age, a retry cooldown and a hard attempt budget, touches
    /// the owning leaf so it activates.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Why reactivation heals, and why the ordering matters.</b> The touch
    /// itself fixes nothing. Activation replays the WAL forward, which advances
    /// the leaf's checkpoint, which makes the already-shipped zero-coverage
    /// repair applicable, which captures a snapshot and stamps coverage - and
    /// only then does the pin resolve to a real offset instead of the blocking
    /// sentinel. Read the other way round it looks like a no-op, which is why
    /// the sequence is spelled out here.
    /// </para>
    /// <para>
    /// <b>Blast radius.</b> At most one leaf per tree per pass, and that bound
    /// is structural rather than a configured cap that could be misconfigured:
    /// the floor short-circuits on the first unusable pin, so a pass cannot
    /// discover a second blocker to act on even in principle. A tree with many
    /// blocked leaves converges one leaf per pass and can never stampede. Live
    /// leaves are excluded by construction, because a live consumer never
    /// reaches the exit that reports it. The remedy is self-extinguishing - a
    /// healed tree stops reporting blocked and this path stops running - so
    /// steady-state cost on a healthy tree is zero.
    /// </para>
    /// <para>
    /// <b>It is not assumed to work.</b> See <see cref="MaxReactivationAttempts"/>:
    /// a leaf whose capture cannot complete is touched a bounded number of times
    /// and then reported as abandoned, rather than retried forever.
    /// </para>
    /// </remarks>
    private async Task ObserveAndHealBlockedTreeAsync(
        string treeId,
        string blockingConsumerId,
        KeyValuePair<string, object?> treeTag,
        KeyValuePair<string, object?> tenantTag,
        CancellationToken stoppingToken)
    {
        var now = _time.GetUtcNow();

        if (!_blockedConsumers.TryGetValue(treeId, out var observation)
            || !string.Equals(observation.ConsumerId, blockingConsumerId, StringComparison.Ordinal))
        {
            // A different leaf is now at the head of the queue, so the previous
            // one stopped blocking - credit it before replacing the record.
            CreditHealedIfSwept(observation, treeTag, tenantTag);

            observation = new BlockedConsumerObservation(
                blockingConsumerId, now, LastReactivationAttempt: null, Attempts: 0, Abandoned: false);
            _blockedConsumers[treeId] = observation;

            // Warn once per episode rather than every pass: a blocked tree
            // deliberately polls at the interval floor, so an unthrottled
            // warning would be loudest for exactly the population it stays
            // useless longest for.
            logger.LogWarning(
                "WAL GC for tree {Tree} cannot reclaim: durable materialiser pin {Consumer} carries no usable offset, so the cursor floor is blocked and the WAL is retained without bound. The consumer id embeds the owning leaf's grain id. Other leaves on this tree may also be blocked; the floor short-circuits on the first one found, so they are reported one at a time.",
                treeId,
                blockingConsumerId);
        }

        if (observation.Attempts >= MaxReactivationAttempts)
        {
            if (!observation.Abandoned)
            {
                // The budget is spent and the leaf is still blocking, so the
                // block is not one activation away from clearing. Say so once,
                // loudly, and stop paying for touches that do not work.
                _blockedConsumers[treeId] = observation with { Abandoned = true };
                LatticeMetrics.WalGcBlockedLeafReactivations.Add(
                    1, treeTag, LatticeMetrics.BlockedLeafReactivationAbandoned, tenantTag);

                logger.LogWarning(
                    "WAL GC gave up reactivating consumer {Consumer} on tree {Tree} after {Attempts} attempts; it is still blocking the cursor floor, so the block is not clearable by activation alone and the WAL stays retained. Investigate why the leaf's snapshot capture does not complete.",
                    blockingConsumerId,
                    treeId,
                    observation.Attempts);
            }

            return;
        }

        if (now - observation.FirstObserved < ReactivationMinBlockAge)
        {
            return;
        }

        if (observation.LastReactivationAttempt is { } lastAttempt
            && now - lastAttempt < ReactivationRetryCooldown)
        {
            return;
        }

        // Stamp the attempt BEFORE making it. A call that throws, times out or
        // is cancelled must still consume the budget and the cooldown, or a
        // leaf that fails fast would be retried every pass - turning a
        // rate-limited heal into the stampede this path is bounded to avoid.
        _blockedConsumers[treeId] = observation with
        {
            LastReactivationAttempt = now,
            Attempts = observation.Attempts + 1,
        };

        LatticeMetrics.WalGcBlockedLeafReactivations.Add(
            1, treeTag, LatticeMetrics.BlockedLeafReactivationAttempted, tenantTag);

        await TryReactivateBlockedLeafAsync(treeId, blockingConsumerId, stoppingToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Drops a tree's blocked-consumer record, crediting a heal when the sweep
    /// had actually touched that consumer.
    /// </summary>
    private void ClearBlockedObservation(
        string treeId,
        in KeyValuePair<string, object?> treeTag,
        in KeyValuePair<string, object?> tenantTag)
    {
        if (_blockedConsumers.Remove(treeId, out var observation))
        {
            CreditHealedIfSwept(observation, treeTag, tenantTag);
        }
    }

    /// <summary>
    /// Records that a consumer this sweep had reactivated has stopped blocking.
    /// </summary>
    /// <remarks>
    /// This is evidence that the sweep is achieving something, not proof that it
    /// caused the heal - an ordinary read or write could have touched the leaf
    /// first. Attributing precisely is not possible from here, and the useful
    /// question this answers is the coarse one: are reactivated leaves clearing
    /// at all, or is the sweep running without effect? A consumer that was never
    /// swept is not credited, so the ratio against <c>attempted</c> stays
    /// meaningful.
    /// </remarks>
    private static void CreditHealedIfSwept(
        BlockedConsumerObservation observation,
        in KeyValuePair<string, object?> treeTag,
        in KeyValuePair<string, object?> tenantTag)
    {
        if (observation.Attempts > 0 && !observation.Abandoned)
        {
            LatticeMetrics.WalGcBlockedLeafReactivations.Add(
                1, treeTag, LatticeMetrics.BlockedLeafReactivationHealed, tenantTag);
        }
    }

    /// <summary>
    /// Resolves a blocking consumer id back to its owning leaf and touches it,
    /// so the leaf activates and runs the activation-time snapshot repair.
    /// </summary>
    /// <remarks>
    /// Best-effort by design. A failure here retains WAL, which is the safe
    /// direction - the block simply persists, exactly as it did before this
    /// path existed - so a fault is logged and the pass continues rather than
    /// failing the collection of every other tree.
    /// </remarks>
    private async Task TryReactivateBlockedLeafAsync(
        string treeId,
        string blockingConsumerId,
        CancellationToken stoppingToken)
    {
        var factory = grainFactory;
        if (factory is null || !TryResolveLeafGrainId(treeId, blockingConsumerId, out var leafGrainId))
        {
            return;
        }

        try
        {
            // A read-only call is enough: the work is done by activation, not by
            // the call. GetTreeIdAsync is chosen precisely because it mutates
            // nothing, so a reactivation that races ordinary traffic cannot
            // disturb it.
            var leaf = factory.GetGrain<IBPlusLeafGrain>(leafGrainId);
            await leaf.GetTreeIdAsync().ConfigureAwait(false);

            logger.LogInformation(
                "WAL GC reactivated leaf {Leaf} on tree {Tree} to clear a blocking durable materialiser pin ({Consumer}). Activation replays the WAL forward, which lets the leaf's snapshot repair stamp coverage and resolve the pin to a real offset.",
                leafGrainId,
                treeId,
                blockingConsumerId);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "WAL GC could not reactivate leaf {Leaf} on tree {Tree} to clear blocking pin {Consumer}; the tree stays blocked and the attempt is retried after the cooldown.",
                leafGrainId,
                treeId,
                blockingConsumerId);
        }
    }

    /// <summary>
    /// Parses a materialiser consumer id back into the grain id of the leaf
    /// that published it.
    /// </summary>
    /// <remarks>
    /// Fail-closed: anything that does not match the exact expected shape
    /// resolves nothing and no leaf is touched. Reactivating the wrong grain
    /// would be worse than leaving the tree blocked, so an unrecognised id is
    /// treated as unresolvable rather than guessed at.
    /// </remarks>
    private bool TryResolveLeafGrainId(string treeId, string consumerId, out GrainId leafGrainId)
    {
        leafGrainId = default;

        var expectedStart = $"{BPlusTree.Grains.ILeafCursorReporter.MaterialiserConsumerIdPrefix}{treeId}_";
        if (!consumerId.StartsWith(expectedStart, StringComparison.Ordinal))
        {
            return false;
        }

        var remainder = consumerId[expectedStart.Length..];
        if (remainder.Length == 0)
        {
            return false;
        }

        // A multi-partition leaf appends "_{partition}". Strip it only when the
        // tree is actually partitioned, so a grain id that legitimately ends in
        // "_<digits>" on a single-partition tree is not silently truncated.
        if (optionsMonitor.Get(treeId).WalPartitions > 1)
        {
            var lastSeparator = remainder.LastIndexOf('_');
            if (lastSeparator > 0
                && remainder.AsSpan(lastSeparator + 1).Length > 0
                && ulong.TryParse(remainder.AsSpan(lastSeparator + 1), out _))
            {
                remainder = remainder[..lastSeparator];
            }
        }

        return GrainId.TryParse(remainder, out leafGrainId);
    }

    /// <summary>
    /// One tree's adaptive cadence state.
    /// </summary>
    /// <param name="IntervalTicks">The interval most recently selected for the tree, in ticks.</param>
    /// <param name="NextDueTicks">UTC tick count at which the tree becomes collectable again.</param>
    /// <param name="Generation">Pass counter that last observed the tree in the registry.</param>
    private readonly record struct TreeCadence(long IntervalTicks, long NextDueTicks, int Generation);
}
