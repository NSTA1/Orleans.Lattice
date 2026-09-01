using System.Buffers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-tree steady-state orchestrator for automatic over-split healing: the
/// driver that turns online shard consolidation from a capability an operator
/// must discover and invoke into something that simply happens on an existing
/// deployment.
/// <para>
/// Each sweep observes the tree's shape, decides with the pure
/// <see cref="ShardHealingDecisionCore"/>, drives any fold already in flight
/// forward by one bounded pass, and admits at most one new fold. The
/// scheduling policy - which pairs, in what order, how many at once, and when
/// to stand aside - lives here; the correctness of a single fold lives in
/// <see cref="ITreeShardConsolidationGrain"/>.
/// </para>
/// <para>
/// <b>Cost of a healthy tree.</b> A tree at or below its base shard count is
/// settled from the routing map alone
/// (<see cref="ShardHealingDecisionCore.DecideStructural"/>), so steady-state
/// observation costs one registry read per sweep and polls no shard at all.
/// Only a tree that really is over-split pays for a hotness sweep, and only
/// then in proportion to its own damage.
/// </para>
/// <para>
/// Key format: <c>{treeId}</c>.
/// </para>
/// </summary>
internal sealed class ShardHealingOrchestratorGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    LatticeOptionsResolver optionsResolver,
    ILogger<ShardHealingOrchestratorGrain> logger,
    [PersistentState("shard-healing", LatticeOptions.StorageProviderName)]
    IPersistentState<ShardHealingOrchestratorState> state) : IShardHealingOrchestratorGrain, IRemindable, IGrainBase
{
    private const string KeepaliveReminderName = "shard-healing";

    private string TreeId => context.GrainId.Key.ToString()!;
    private LatticeOptions Options => optionsMonitor.Get(TreeId);
    IGrainContext IGrainBase.GrainContext => context;

    /// <summary>
    /// Time source for the post-split stand-off window. Defaults to
    /// <see cref="TimeProvider.System"/>. Tests substitute an alternative
    /// <see cref="TimeProvider"/> so cooldown expiry is driven deterministically
    /// rather than by a wall-clock wait.
    /// </summary>
    internal TimeProvider TimeProvider { get; set; } = TimeProvider.System;

    private IGrainTimer? _timer;
    private bool _running;

    /// <summary>
    /// The most recent sweep's live load measurements. Deliberately not
    /// persisted: a skew ratio is an observation of traffic happening now, and
    /// replaying a pre-restart one would be a claim about load the silo did not
    /// see. A reactivated orchestrator reports zero here until its first sweep.
    /// </summary>
    private double _lastSkewRatio;
    private double _lastMedianOpsPerSecond;
    private int _lastInFlight;
    private int _lastPhysicalShardCount;
    private int _lastBaseShardCount;

    /// <inheritdoc />
    public async Task EnsureRunningAsync()
    {
        if (_running) return;

        // Deliberately not latched while healing is switched off, so flipping
        // the kill switch back on takes effect on the next call rather than
        // requiring a reactivation. The disabled path registers no reminder and
        // starts no timer, so it costs nothing to re-evaluate.
        if (!Options.ShardHealingEnabled) return;

        _running = true;

        await reminderRegistry.RegisterOrUpdateReminder(
            callingGrainId: context.GrainId,
            reminderName: KeepaliveReminderName,
            dueTime: TimeSpan.FromMinutes(1),
            period: TimeSpan.FromMinutes(1));

        StartTimer();
    }

    /// <inheritdoc />
    public async Task StopAsync()
    {
        _timer?.Dispose();
        _timer = null;
        _running = false;

        try
        {
            var reminder = await reminderRegistry.GetReminder(context.GrainId, KeepaliveReminderName);
            if (reminder is not null)
                await reminderRegistry.UnregisterReminder(context.GrainId, reminder);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to unregister shard-healing reminder for tree {TreeId}", TreeId);
        }

        this.DeactivateOnIdle();
    }

    /// <inheritdoc />
    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName != KeepaliveReminderName) return;
        if (!Options.ShardHealingEnabled) return;

        // Defensively re-register the keepalive if the current period drifts
        // from the configured value, matching the hot-shard monitor: a
        // stale-period reminder that survived an option change or an Orleans
        // upgrade would otherwise silently slow healing down.
        var desired = TimeSpan.FromMinutes(1);
        if (status.Period != desired)
        {
            await reminderRegistry.RegisterOrUpdateReminder(
                callingGrainId: context.GrainId,
                reminderName: KeepaliveReminderName,
                dueTime: desired,
                period: desired);
        }

        if (_timer is null) StartTimer();
        _running = true;
    }

    private void StartTimer()
    {
        var period = Options.ShardHealingInterval;
        if (period <= TimeSpan.Zero) period = LatticeOptions.DefaultShardHealingInterval;
        _timer = this.RegisterGrainTimer(
            OnTimerTickAsync,
            new GrainTimerCreationOptions(dueTime: period, period: period));
    }

    private async Task OnTimerTickAsync(CancellationToken ct)
    {
        try
        {
            await RunHealingPassAsync();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Shard-healing pass failed for tree {TreeId}", TreeId);
        }
    }

    /// <inheritdoc />
    public Task<ShardHealingReport> GetHealingReportAsync() => Task.FromResult(new ShardHealingReport
    {
        Decision = state.State.LastDecision,
        PhysicalShardCount = _lastPhysicalShardCount,
        BaseShardCount = _lastBaseShardCount,
        Backlog = state.State.LastBacklog,
        SkewRatio = _lastSkewRatio,
        MedianShardOpsPerSecond = _lastMedianOpsPerSecond,
        InFlightConsolidations = _lastInFlight,
        ObservedAtTicks = state.State.LastObservedAtTicks,
    });

    /// <inheritdoc />
    public async Task RunHealingPassAsync()
    {
        var options = Options;
        var policy = ShardHealingPolicy.FromOptions(options);
        var nowUtc = TimeProvider.GetUtcNow().UtcDateTime;

        // Structural observation. The routing map and the tree's pinned base
        // shard count are all the cheap clauses need, so a healthy tree never
        // reaches a single shard grain.
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var resolved = await optionsResolver.ResolveAsync(TreeId);
        var map = await registry.GetShardMapAsync(TreeId)
            ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, resolved.ShardCount);
        var physicalShards = map.GetPhysicalShardIndices();
        var shardCount = physicalShards.Count;
        var baseShardCount = resolved.ShardCount;

        _lastPhysicalShardCount = shardCount;
        _lastBaseShardCount = baseShardCount;

        if (ShardHealingDecisionCore.DecideStructural(shardCount, baseShardCount, in policy) is { } structural)
        {
            // A structurally-settled sweep has, by construction, no fold to
            // drive and none to admit, so the live load fields keep their last
            // real measurement rather than being zeroed by a cheap pass. The
            // in-flight count is read from durable state, which stays accurate
            // without an RPC - it matters when healing is switched off while a
            // fold is still running.
            _lastInFlight = state.State.InFlightDonorShardIndices.Count;
            await PublishAsync(structural, shardCount, baseShardCount, nowUtc);
            return;
        }

        var physicalTreeId = await registry.ResolveAsync(TreeId);

        // Reconcile the persisted in-flight set before anything else: it is
        // bounded by MaxConcurrentShardConsolidations, it is what a reactivated
        // orchestrator resumes from, and its size is an input to the decision.
        var inFlight = await ReconcileInFlightFoldsAsync(physicalTreeId);
        _lastInFlight = inFlight;

        // Load observation. Only an over-split tree pays for this.
        var lattice = grainFactory.GetGrain<ILattice>(TreeId);
        bool inTreeMaintenance;

        // These status verbs are access-gated (LatticeOperation.Read). This
        // timer-driven pass runs with no caller identity, so on a deny-by-default
        // tree the gate would fail closed, the catch-and-warn handler in
        // OnTimerTickAsync would swallow the denial as a routine warning, and
        // healing would be silently disabled on exactly the trees that need it.
        // Enter a system-origin scope, following the same precedent as
        // HotShardMonitorGrain's sampling pass: AccessGateSystemOrigin is a
        // reserved capability key that LatticeCapabilityStrippingCallFilter
        // strips from any genuine external client, so it cannot be forged
        // inbound and only ever marks in-silo library machinery such as this.
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            inTreeMaintenance =
                !await lattice.IsResizeCompleteAsync()
                || !await lattice.IsReshardCompleteAsync()
                || !await lattice.IsMergeCompleteAsync()
                || !await lattice.IsSnapshotCompleteAsync();
        }

        var sample = await ObserveLoadAsync(physicalTreeId, physicalShards, shardCount, inTreeMaintenance);
        sample = sample with
        {
            PhysicalShardCount = shardCount,
            BaseShardCount = baseShardCount,
            InFlightConsolidations = inFlight,
            InCooldown = state.State.CooldownUntilUtc is { } until && nowUtc < until,
        };

        _lastSkewRatio = sample.SkewRatio;
        _lastMedianOpsPerSecond = sample.MedianShardOpsPerSecond;

        // Observing a split arms the time-domain half of the hysteresis: after
        // a split the tree's shape has just changed, so healing stands off
        // until the new shape has had time to settle. Recorded before the
        // decision so the very sweep that saw the split also starts the window.
        if (sample.IsSplitting && options.ShardHealingCooldown > TimeSpan.Zero)
            state.State.CooldownUntilUtc = nowUtc + options.ShardHealingCooldown;

        var decision = ShardHealingDecisionCore.Decide(in sample, in policy);

        // Backpressure yields completely: healing neither admits a new fold nor
        // pushes an existing one along, so the only work left on the tree is
        // whatever the fold's own coordinator does on its background cadence.
        // That is what makes healing invisible to a user issuing queries.
        if (decision != ShardHealingDecision.Backpressure)
            await DriveInFlightFoldsAsync(physicalTreeId);

        if (decision == ShardHealingDecision.Admitted)
            decision = await AdmitNextFoldAsync(physicalTreeId, map);

        await PublishAsync(decision, shardCount, baseShardCount, nowUtc);
    }

    /// <summary>
    /// Polls every fold this orchestrator believes is in flight, drops the
    /// ones that have finished or been abandoned, and returns how many remain.
    /// <para>
    /// Bounded by <see cref="LatticeOptions.MaxConcurrentShardConsolidations"/>
    /// rather than by the tree's shard count, which is what lets a
    /// thousand-shard tree be reconciled in a couple of calls instead of a
    /// thousand.
    /// </para>
    /// </summary>
    private async Task<int> ReconcileInFlightFoldsAsync(string physicalTreeId)
    {
        var tracked = state.State.InFlightDonorShardIndices;
        _inFlightSurvivors.Clear();
        if (tracked.Count == 0) return 0;

        var completed = 0;
        for (var i = tracked.Count - 1; i >= 0; i--)
        {
            var donor = tracked[i];
            ShardConsolidationProgress progress;
            try
            {
                progress = await ConsolidationGrain(physicalTreeId, donor).GetProgressAsync();
            }
            catch (Exception ex)
            {
                // A coordinator that cannot be reached this sweep is not
                // evidence its fold finished. Keep tracking it: dropping it
                // here would let the orchestrator admit a second fold against a
                // tree already doing one. Its survivor stays unknown, which the
                // admission path treats as a reason to wait rather than guess.
                logger.LogDebug(ex,
                    "Could not read consolidation progress for donor shard {DonorShardIndex} of tree {TreeId}",
                    donor, TreeId);
                _inFlightSurvivors.Add(UnknownSurvivor);
                continue;
            }

            if (progress.InProgress)
            {
                _inFlightSurvivors.Add(progress.SurvivorShardIndex);
                continue;
            }

            tracked.RemoveAt(i);
            completed++;
            logger.LogInformation(
                "Consolidation of donor shard {DonorShardIndex} onto shard {SurvivorShardIndex} for tree {TreeId} finished (complete={Complete}, cancelled={Cancelled}, entriesDrained={EntriesDrained})",
                donor, progress.SurvivorShardIndex, TreeId, progress.Complete, progress.Cancelled, progress.EntriesDrained);
        }

        // The reverse walk above appends survivors in reverse tracked order and
        // skips the entries it removed, so restore the index alignment the
        // admission and projection paths rely on.
        _inFlightSurvivors.Reverse();

        if (completed > 0) await WriteStateAsync();
        return tracked.Count;
    }

    /// <summary>
    /// Sentinel survivor index for a tracked fold whose coordinator could not
    /// be polled this sweep. Distinct from every real physical shard index,
    /// which is non-negative.
    /// </summary>
    private const int UnknownSurvivor = -1;

    /// <summary>
    /// Pushes each in-flight fold forward by one bounded pass. The
    /// consolidation coordinator is also driven by its own reminder-anchored
    /// phase timer, so this is an accelerant rather than the only motor: it
    /// makes healing progress at the orchestrator's cadence and is idempotent
    /// if the two overlap.
    /// </summary>
    private async Task DriveInFlightFoldsAsync(string physicalTreeId)
    {
        var tracked = state.State.InFlightDonorShardIndices;
        for (var i = 0; i < tracked.Count; i++)
        {
            var donor = tracked[i];
            try
            {
                await ConsolidationGrain(physicalTreeId, donor).RunConsolidationPassAsync();
            }
            catch (Exception ex)
            {
                // Every phase transition is persisted before the next runs, so
                // a failed pass loses no progress; the next sweep resumes from
                // the same boundary.
                logger.LogDebug(ex,
                    "Consolidation pass failed for donor shard {DonorShardIndex} of tree {TreeId}",
                    donor, TreeId);
            }
        }
    }

    /// <summary>
    /// Selects and starts the next fold, returning the decision actually
    /// reached: <see cref="ShardHealingDecision.Admitted"/> when a fold was
    /// started, or <see cref="ShardHealingDecision.NoFoldablePair"/> when the
    /// map offered none this sweep.
    /// </summary>
    private async Task<ShardHealingDecision> AdmitNextFoldAsync(string physicalTreeId, ShardMap map)
    {
        var tracked = state.State.InFlightDonorShardIndices;

        // A tracked fold whose coordinator could not be polled leaves the
        // orchestrator without a survivor to reserve, so it cannot know which
        // pair is safe. Wait a sweep rather than guess.
        for (var i = 0; i < _inFlightSurvivors.Count; i++)
        {
            if (_inFlightSurvivors[i] == UnknownSurvivor) return ShardHealingDecision.NoFoldablePair;
        }

        // Plan against the map the in-flight folds will leave behind rather
        // than the one they started from. Re-pointing a fold's donor slots onto
        // its survivor is exactly the routing swap that fold is going to
        // commit, so the projection both removes the donor from selection - it
        // must not be picked twice - and makes the planner choose the pair that
        // will be cheapest next, instead of one it is about to invalidate.
        var planningMap = tracked.Count == 0 ? map : ProjectInFlightFolds(map);

        if (!ShardConsolidationPlanner.TryPlanNext(planningMap, out var plan))
            return ShardHealingDecision.NoFoldablePair;

        // The projection cannot hide an in-flight fold's survivor, which is
        // still a live shard. Folding a shard that is concurrently absorbing
        // another would have it draining and absorbing at once, so the pair is
        // refused rather than serialised: the next sweep re-plans against a map
        // in which that fold has committed.
        for (var i = 0; i < _inFlightSurvivors.Count; i++)
        {
            var survivor = _inFlightSurvivors[i];
            if (plan.DonorShardIndex == survivor || plan.SurvivorShardIndex == survivor)
                return ShardHealingDecision.NoFoldablePair;
        }

        // Record the intent BEFORE issuing the start, so the durable state can
        // only ever over-count folds, never under-count them. An over-count is
        // self-healing: the next reconcile finds the coordinator idle and drops
        // the entry. An under-count is not, because the orchestrator would then
        // admit a second fold while the forgotten one was still draining and
        // quietly exceed the concurrency cap it exists to enforce.
        tracked.Add(plan.DonorShardIndex);
        _inFlightSurvivors.Add(plan.SurvivorShardIndex);
        await WriteStateAsync();

        try
        {
            await ConsolidationGrain(physicalTreeId, plan.DonorShardIndex).StartAsync(plan.SurvivorShardIndex);
        }
        catch (InvalidOperationException ex)
        {
            // The coordinator is busy on a different survivor, or a split is in
            // flight on one of the pair. Both are transient and both are the
            // coordinator correctly refusing to be re-aimed; retry next sweep.
            tracked.RemoveAt(tracked.Count - 1);
            _inFlightSurvivors.RemoveAt(_inFlightSurvivors.Count - 1);
            await WriteStateAsync();

            logger.LogDebug(ex,
                "Could not start consolidation of donor shard {DonorShardIndex} onto shard {SurvivorShardIndex} for tree {TreeId}",
                plan.DonorShardIndex, plan.SurvivorShardIndex, TreeId);
            return ShardHealingDecision.NoFoldablePair;
        }

        logger.LogInformation(
            "Admitted consolidation of donor shard {DonorShardIndex} onto shard {SurvivorShardIndex} for tree {TreeId} ({SlotCount} virtual slots fold)",
            plan.DonorShardIndex, plan.SurvivorShardIndex, TreeId, plan.DonorSlots.Length);

        return ShardHealingDecision.Admitted;
    }

    /// <summary>
    /// Survivor index of each tracked in-flight fold, index-aligned with
    /// <see cref="ShardHealingOrchestratorState.InFlightDonorShardIndices"/>
    /// and rebuilt from each coordinator's own progress record on every
    /// reconcile.
    /// <para>
    /// Deliberately not persisted: the durable record of a fold's survivor
    /// lives in that fold's coordinator, which is the only place it can be
    /// correct after a crash, so duplicating it here would create a second copy
    /// that could disagree. A reactivated orchestrator repopulates the list on
    /// its first sweep, before any admission decision reads it.
    /// </para>
    /// </summary>
    private readonly List<int> _inFlightSurvivors = [];

    /// <summary>
    /// Builds the routing map the tree will have once every in-flight fold
    /// commits, by re-pointing each tracked donor's slots onto its survivor.
    /// <para>
    /// Allocates one slot array, and only on a sweep that is admitting a fold
    /// while another is already in flight - never on the steady-state observe
    /// path, and never at all under the default
    /// <see cref="LatticeOptions.MaxConcurrentShardConsolidations"/> of one.
    /// </para>
    /// </summary>
    private ShardMap ProjectInFlightFolds(ShardMap map)
    {
        var source = map.Slots;
        var projected = new int[source.Length];
        Array.Copy(source, projected, source.Length);

        var tracked = state.State.InFlightDonorShardIndices;
        var pairs = Math.Min(tracked.Count, _inFlightSurvivors.Count);
        for (var i = 0; i < pairs; i++)
        {
            var donor = tracked[i];
            var survivor = _inFlightSurvivors[i];
            if (survivor == UnknownSurvivor) continue;
            for (var slot = 0; slot < projected.Length; slot++)
            {
                if (projected[slot] == donor) projected[slot] = survivor;
            }
        }

        return new ShardMap { Slots = projected, Version = map.Version };
    }

    /// <summary>
    /// Polls every physical shard's hotness and split status and reduces them
    /// to the tree-level statistics the decision needs. The rates are computed
    /// with <see cref="ShardSplitAdmissionCore"/>'s own functions, so the
    /// healer and the splitter cannot form different views of the same tree.
    /// </summary>
    private async Task<ShardHealingSample> ObserveLoadAsync(
        string physicalTreeId,
        IReadOnlyList<int> physicalShards,
        int shardCount,
        bool inTreeMaintenance)
    {
        var hotnessTasks = new Task<ShardHotness>[shardCount];
        var splittingTasks = new Task<bool>[shardCount];
        var pendingBulkTasks = new Task<bool>[shardCount];

        for (var i = 0; i < shardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{physicalShards[i]}");
            hotnessTasks[i] = shard.GetHotnessAsync();
            splittingTasks[i] = shard.IsSplittingAsync();
            pendingBulkTasks[i] = shard.HasPendingBulkOperationAsync();
        }

        await Task.WhenAll(hotnessTasks);
        await Task.WhenAll(splittingTasks);
        await Task.WhenAll(pendingBulkTasks);

        var isSplitting = false;
        for (var i = 0; i < shardCount; i++)
        {
            if (splittingTasks[i].Result) { isSplitting = true; break; }
        }

        if (!inTreeMaintenance)
        {
            for (var i = 0; i < shardCount; i++)
            {
                // A pending bulk graft mutates topology in ways a fold cannot
                // interleave with, exactly as it suppresses an adaptive split.
                if (pendingBulkTasks[i].Result) { inTreeMaintenance = true; break; }
            }
        }

        // Two contiguous halves of one rented buffer: the per-shard rates, and
        // a scratch copy the median sort may reorder in place.
        var rateBuffer = ArrayPool<double>.Shared.Rent(shardCount * 2);
        try
        {
            var rates = rateBuffer.AsSpan(0, shardCount);
            var scratch = rateBuffer.AsSpan(shardCount, shardCount);
            var maxRate = 0d;
            for (var i = 0; i < shardCount; i++)
            {
                var hotness = hotnessTasks[i].Result;
                var rate = ShardSplitAdmissionCore.ComputeRate(hotness.Reads, hotness.Writes, hotness.Window);
                rates[i] = rate;
                if (rate > maxRate) maxRate = rate;
            }

            rates.CopyTo(scratch);
            var medianRate = ShardSplitAdmissionCore.ComputeMedianRate(scratch);

            return new ShardHealingSample
            {
                SkewRatio = ShardSplitAdmissionCore.ComputeSkewRatio(maxRate, medianRate),
                MedianShardOpsPerSecond = medianRate,
                IsSplitting = isSplitting,
                InTreeMaintenance = inTreeMaintenance,
            };
        }
        finally
        {
            ArrayPool<double>.Shared.Return(rateBuffer);
        }
    }

    /// <summary>
    /// Publishes the sweep's decision and the tree's healing backlog, and
    /// persists them when they moved.
    /// <para>
    /// State is flushed only on a material change - the decision or the
    /// backlog - rather than once per sweep, so a settled tree costs no storage
    /// write per tick. The in-memory copy is always current; only the durable
    /// copy lags, and only by observations that changed nothing.
    /// </para>
    /// </summary>
    private async Task PublishAsync(ShardHealingDecision decision, int shardCount, int baseShardCount, DateTime nowUtc)
    {
        var backlog = ShardHealingDecisionCore.ComputeBacklog(shardCount, baseShardCount);
        var changed = state.State.LastDecision != decision || state.State.LastBacklog != backlog;

        state.State.LastDecision = decision;
        state.State.LastBacklog = backlog;
        state.State.LastObservedAtTicks = nowUtc.Ticks;

        var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId);
        var tenantTag = LatticeTenantLabel.ForTree(TreeId);
        LatticeMetrics.ShardHealingBacklog.Record(backlog, treeTag, tenantTag);
        LatticeMetrics.ShardHealingDecisions.Add(1, treeTag, DecisionTag(decision), tenantTag);

        if (changed) await WriteStateAsync();
    }

    /// <summary>
    /// Maps a decision onto its pre-allocated metric tag, so publishing a
    /// per-sweep decision never allocates a tag on the observe path.
    /// </summary>
    private static KeyValuePair<string, object?> DecisionTag(ShardHealingDecision decision) => decision switch
    {
        ShardHealingDecision.Admitted => LatticeMetrics.HealingAdmittedDecisionTag,
        ShardHealingDecision.Disabled => LatticeMetrics.HealingDisabledDecisionTag,
        ShardHealingDecision.AdmissionClosed => LatticeMetrics.HealingAdmissionClosedDecisionTag,
        ShardHealingDecision.NotOverSplit => LatticeMetrics.HealingNotOverSplitDecisionTag,
        ShardHealingDecision.SkewedLoad => LatticeMetrics.HealingSkewedLoadDecisionTag,
        ShardHealingDecision.SplitInFlight => LatticeMetrics.HealingSplitInFlightDecisionTag,
        ShardHealingDecision.TreeMaintenance => LatticeMetrics.HealingTreeMaintenanceDecisionTag,
        ShardHealingDecision.Cooldown => LatticeMetrics.HealingCooldownDecisionTag,
        ShardHealingDecision.Backpressure => LatticeMetrics.HealingBackpressureDecisionTag,
        ShardHealingDecision.AtCapacity => LatticeMetrics.HealingAtCapacityDecisionTag,
        ShardHealingDecision.NoFoldablePair => LatticeMetrics.HealingNoFoldablePairDecisionTag,
        _ => LatticeMetrics.HealingNotObservedDecisionTag,
    };

    private ITreeShardConsolidationGrain ConsolidationGrain(string physicalTreeId, int donorShardIndex)
        => grainFactory.GetGrain<ITreeShardConsolidationGrain>($"{physicalTreeId}/{donorShardIndex}");

    /// <summary>
    /// Persists scheduling state, treating a storage failure as a warning
    /// rather than a fault. Healing is best-effort background repair: failing
    /// the sweep would achieve nothing the next sweep does not, and the durable
    /// record of every fold already lives in its own coordinator.
    /// <para>
    /// A dropped write can therefore leave the durable in-flight set stale, and
    /// the admission path is ordered so that staleness is always in the safe
    /// direction: intent is recorded before a fold is started, so a lost write
    /// can only make the orchestrator believe in a fold that is not running -
    /// which the next reconcile discovers and clears - never the reverse.
    /// </para>
    /// </summary>
    private async Task WriteStateAsync()
    {
        try
        {
            await state.WriteStateAsync();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to persist shard-healing state for tree {TreeId}", TreeId);
        }
    }
}
