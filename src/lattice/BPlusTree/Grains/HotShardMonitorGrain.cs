using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-tree autonomic monitor for adaptive shard splitting.
/// <para>
/// The monitor is started lazily by <c>LatticeGrain</c> on the first write
/// to a tree and re-activates on silo restart via a keepalive reminder. On
/// each tick it polls every physical shard's <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.GetHotnessAsync"/>
/// in parallel, computes ops-per-second, and triggers splits on up to
/// <see cref="LatticeOptions.MaxConcurrentAutoSplits"/> of the hottest
/// eligible shards in parallel.
/// </para>
/// <para>
/// Suppression rules:
/// </para>
/// <list type="bullet">
/// <item><description><see cref="LatticeOptions.AutoSplitEnabled"/> is <c>false</c> - entire pass returns.</description></item>
/// <item><description>The tree is younger than <see cref="LatticeOptions.AutoSplitMinTreeAge"/> (since this monitor activated) - entire pass returns.</description></item>
/// <item><description>A resize, merge, or snapshot is in progress (<see cref="ILattice.IsResizeCompleteAsync"/> etc.) - entire pass returns.</description></item>
/// <item><description>Any physical shard has a pending bulk graft (<see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.HasPendingBulkOperationAsync"/>) - entire pass returns.</description></item>
/// <item><description>A shard is already splitting - that shard is skipped and counts toward the in-flight cap.</description></item>
/// <item><description>The shard is in the per-shard cooldown window after a recent split - that shard is skipped.</description></item>
/// <item><description>The shard owns fewer than two virtual slots - that shard is skipped (nothing to subdivide).</description></item>
/// <item><description><see cref="LatticeOptions.MaxConcurrentAutoSplits"/> in-flight splits already running - no further splits this tick.</description></item>
/// <item><description><see cref="LatticeOptions.MaxClusterConcurrentAutoSplits"/> is set and the cluster-wide admission gate has no free slot (the aggregate in-flight ceiling across all trees is reached) - the affected candidates are deferred to a later tick.</description></item>
/// </list>
/// Key format: <c>{treeId}</c>.
/// </summary>
internal sealed class HotShardMonitorGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    LatticeOptionsResolver optionsResolver,
    ILogger<HotShardMonitorGrain> logger,
    [PersistentState("hot-shard-monitor", LatticeOptions.StorageProviderName)]
    IPersistentState<HotShardMonitorState> state) : IHotShardMonitorGrain, IRemindable, IGrainBase
{
    private const string KeepaliveReminderName = "hot-shard-monitor";

    /// <summary>Well-known singleton key of the cluster-wide split admission gate.</summary>
    private const long ClusterSplitConcurrencyKey = 0;

    /// <summary>
    /// Multiple of the sampling interval used as the time-to-live for this
    /// monitor's cluster-gate heartbeat footprint. A generous window so a single
    /// missed sampling pass (or a brief reactivation gap) does not prematurely
    /// expire the tree's reported in-flight footprint; a silo that stops
    /// reporting entirely still has its share reclaimed within this window.
    /// </summary>
    private const int ClusterFootprintTtlSampleMultiple = 3;

    private string TreeId => context.GrainId.Key.ToString()!;
    private LatticeOptions Options => optionsMonitor.Get(TreeId);
    IGrainContext IGrainBase.GrainContext => context;

    private IGrainTimer? _timer;
    private readonly Dictionary<int, DateTime> _shardCooldownUntilUtc = [];
    private bool _running;

    /// <summary>
    /// The in-flight footprint this monitor last published to the cluster gate.
    /// Tracked so the no-ceiling path can stay edge-triggered: it reports while
    /// splits are in flight and issues exactly one further call to clear the
    /// footprint once they finish, rather than heartbeating an idle zero forever.
    /// It also tells an aborted pass whether it has an outstanding footprint to
    /// keep alive. Reset to zero on activation, which is safe because an
    /// unrefreshed footprint expires on its own.
    /// </summary>
    private int _reportedFootprint;

    /// <summary>
    /// The time-to-live to attach to this monitor's cluster-gate footprint:
    /// a generous multiple of the sampling interval, so a single missed pass does
    /// not prematurely expire it while a silo that stops reporting entirely still
    /// has its share reclaimed.
    /// </summary>
    private static TimeSpan FootprintTtl(LatticeOptions options)
    {
        var sampleInterval = options.HotShardSampleInterval;
        if (sampleInterval <= TimeSpan.Zero) sampleInterval = LatticeOptions.DefaultHotShardSampleInterval;
        return sampleInterval * ClusterFootprintTtlSampleMultiple;
    }

    /// <summary>
    /// Publishes this tree's in-flight footprint to the cluster gate on the
    /// no-ceiling path, where the gate makes no admission decision and exists
    /// only as the cluster's readable split-activity source.
    /// <para>
    /// Never throws. The heartbeat is pure observability, but it sits upstream of
    /// the split triggers, so letting a transient gate failure escape would abort
    /// the pass and start no splits at all - a storage blip would silently cost
    /// the tree its elasticity. This mirrors the fail-open posture the reading
    /// side takes; a missed heartbeat costs at most a footprint that expires.
    /// </para>
    /// </summary>
    private async Task PublishFootprintAsync(int footprint, LatticeOptions options)
    {
        try
        {
            var gate = grainFactory.GetGrain<IClusterSplitConcurrencyGrain>(ClusterSplitConcurrencyKey);
            await gate.ReportInFlightAsync(TreeId, footprint, FootprintTtl(options));
            _reportedFootprint = footprint;
        }
        catch (Exception ex)
        {
            logger.LogDebug(ex, "Could not publish split-activity footprint for tree {TreeId}", TreeId);
        }
    }

    /// <summary>
    /// Keeps an already-published footprint alive when a pass aborts before it
    /// can recompute the authoritative in-flight count.
    /// <para>
    /// Four suppressors end a pass early - auto-split disabled, the min-tree-age
    /// grace period, a resize/reshard/merge/snapshot in progress, and a pending
    /// bulk graft - but none of them stop splits that are already draining, since
    /// those run on their own coordinators. Without this refresh the footprint
    /// would lapse after its time-to-live and the split-activity source would
    /// report an idle cluster while splits were genuinely in flight, which is the
    /// precise failure the scale-in gate exists to prevent. Re-reporting the last
    /// known count can only over-report for one pass if those splits have since
    /// finished, which holds scale-in marginally longer - the safe direction -
    /// and self-corrects on the next unsuppressed pass.
    /// </para>
    /// <para>
    /// Costs nothing when no footprint is outstanding, so an idle tree is
    /// unaffected. Routed through whichever path originally published the
    /// footprint, so a capped tree's entry stays in the admission ledger.
    /// </para>
    /// </summary>
    private async Task RefreshOutstandingFootprintAsync(LatticeOptions options)
    {
        if (_reportedFootprint <= 0) return;

        if (options.MaxClusterConcurrentAutoSplits is { } clusterCap)
        {
            try
            {
                var gate = grainFactory.GetGrain<IClusterSplitConcurrencyGrain>(ClusterSplitConcurrencyKey);
                await gate.AcquireSlotsAsync(
                    TreeId, _reportedFootprint, desiredNew: 0, clusterCap, FootprintTtl(options));
            }
            catch (Exception ex)
            {
                logger.LogDebug(ex, "Could not refresh split footprint for tree {TreeId}", TreeId);
            }

            return;
        }

        await PublishFootprintAsync(_reportedFootprint, options);
    }

    /// <summary>
    /// Returns the monitor's first-ever activation time for this tree,
    /// loading and persisting the value on first use so it survives silo
    /// restarts. Without persistence, the
    /// <see cref="LatticeOptions.AutoSplitMinTreeAge"/> grace period would
    /// restart on every activation and a cluster with frequent restarts
    /// would never trigger autonomic splits.
    /// </summary>
    /// <param name="nowUtc">
    /// The "now" reference used by the caller so the persisted value is
    /// never newer than the caller's own clock read. On first use this
    /// becomes the stored activation time.
    /// </param>
    private async Task<DateTime> GetOrSetActivationUtcAsync(DateTime nowUtc)
    {
        if (state.State.ActivationUtc is DateTime v) return v;
        var prev = state.State.ActivationUtc;
        state.State.ActivationUtc = nowUtc;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            // Restore in-memory state so the idempotency guard above does
            // not short-circuit subsequent retries from this activation.
            state.State.ActivationUtc = prev;
            throw;
        }
        return nowUtc;
    }

    /// <inheritdoc />
    public async Task EnsureRunningAsync()
    {
        if (_running) return;
        _running = true;

        var options = Options;
        if (!options.AutoSplitEnabled) return;

        await reminderRegistry.RegisterOrUpdateReminder(
            callingGrainId: context.GrainId,
            reminderName: KeepaliveReminderName,
            dueTime: TimeSpan.FromMinutes(1),
            period: TimeSpan.FromMinutes(1));

        // Initialize the persisted activation time on first use.
        await GetOrSetActivationUtcAsync(DateTime.UtcNow);
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
            logger.LogWarning(ex, "Failed to unregister hot-shard-monitor reminder for tree {TreeId}", TreeId);
        }

        this.DeactivateOnIdle();
    }

    /// <inheritdoc />
    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName != KeepaliveReminderName) return;
        if (!Options.AutoSplitEnabled) return;

        // Defensively re-register the keepalive if the current period drifts
        // from the configured value. Protects against stale-period reminders
        // that survived option changes or Orleans upgrades.
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
        var period = Options.HotShardSampleInterval;
        if (period <= TimeSpan.Zero) period = LatticeOptions.DefaultHotShardSampleInterval;
        _timer = this.RegisterGrainTimer(
            OnTimerTickAsync,
            new GrainTimerCreationOptions(dueTime: period, period: period));
    }

    private async Task OnTimerTickAsync(CancellationToken ct)
    {
        try
        {
            await RunSamplingPassAsync();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Hot-shard sampling pass failed for tree {TreeId}", TreeId);
        }
    }

    /// <inheritdoc />
    public async Task RunSamplingPassAsync()
    {
        var options = Options;
        if (!options.AutoSplitEnabled)
        {
            await RefreshOutstandingFootprintAsync(options);
            return;
        }

        var nowUtc = DateTime.UtcNow;

        // Use the persisted activation time, initializing it if first use.
        var activationUtc = await GetOrSetActivationUtcAsync(nowUtc);

        // Enforce the min-age grace period before allowing splits to trigger.
        if (nowUtc - activationUtc < options.AutoSplitMinTreeAge)
        {
            await RefreshOutstandingFootprintAsync(options);
            return;
        }

        // Suppress while bulk maintenance is in flight.
        //
        // These four status verbs are access-gated (LatticeOperation.Read). This
        // timer-driven pass runs with no caller identity, so on a deny-by-default
        // tree the gate would fail closed and deny every poll; the catch-and-warn
        // handler in OnTimerTickAsync would then swallow that denial as a routine
        // warning and auto-split would be silently disabled. Enter a system-origin
        // scope so these internal observations bypass the gate, following the same
        // precedent as the atomic-write saga's internal write leg (see
        // AtomicWriteGrain, which enters this scope because an internal leg "runs
        // without the caller's identity and would otherwise fail-closed").
        //
        // The scope is safe to assert here because AccessGateSystemOrigin is a
        // reserved capability key: LatticeCapabilityStrippingCallFilter strips it
        // from any genuine external Orleans client, so it cannot be forged inbound
        // and only ever marks in-silo library machinery such as this monitor.
        var lattice = grainFactory.GetGrain<ILattice>(TreeId);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            if (!await lattice.IsResizeCompleteAsync() ||
                !await lattice.IsReshardCompleteAsync() ||
                !await lattice.IsMergeCompleteAsync() ||
                !await lattice.IsSnapshotCompleteAsync())
            {
                await RefreshOutstandingFootprintAsync(options);
                return;
            }
        }

        // Resolve the current shard map and list of physical shards.
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var physicalTreeId = await registry.ResolveAsync(TreeId);
        var resolved = await optionsResolver.ResolveAsync(TreeId);
        var map = await registry.GetShardMapAsync(TreeId)
            ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, resolved.ShardCount);
        var physicalShards = map.GetPhysicalShardIndices();

        // Prune cooldown entries for shards no longer present in the current
        // map. Without this, the dictionary grows unbounded over the monitor's
        // lifetime as shards are retired by resizes or merges (audit bug #12).
        if (_shardCooldownUntilUtc.Count > 0)
        {
            List<int>? stale = null;
            foreach (var key in _shardCooldownUntilUtc.Keys)
            {
                if (!physicalShards.Contains(key))
                    (stale ??= []).Add(key);
            }
            if (stale is not null)
            {
                foreach (var key in stale)
                    _shardCooldownUntilUtc.Remove(key);
            }
        }

        // Poll hotness in parallel, plus pending-bulk + splitting status.
        var hotnessTasks = new Task<ShardHotness>[physicalShards.Count];
        var pendingBulkTasks = new Task<bool>[physicalShards.Count];
        var splittingTasks = new Task<bool>[physicalShards.Count];
        for (int i = 0; i < physicalShards.Count; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{physicalShards[i]}");
            hotnessTasks[i] = shard.GetHotnessAsync();
            pendingBulkTasks[i] = shard.HasPendingBulkOperationAsync();
            splittingTasks[i] = shard.IsSplittingAsync();
        }
        await Task.WhenAll(hotnessTasks);
        await Task.WhenAll(pendingBulkTasks);
        await Task.WhenAll(splittingTasks);

        // Count splits already in flight from the splitting-status results.
        // A shard reports IsSplitting==true while it is the source of an
        // unfinished split; this is our authoritative cluster-wide concurrency
        // counter, surviving silo restarts and monitor reactivation.
        var inFlight = 0;
        for (int i = 0; i < physicalShards.Count; i++)
            if (splittingTasks[i].Result) inFlight++;

        // Emit the per-tree in-flight split count every pass, regardless of
        // whether the cluster gate is enabled, so operators can compute the
        // cluster aggregate as a sum across the tree tag and decide whether they
        // need MaxClusterConcurrentAutoSplits at all.
        var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId);
        var tenantTag = LatticeTenantLabel.ForTree(TreeId);
        LatticeMetrics.SplitInFlight.Record(inFlight, treeTag, tenantTag);

        // Suppress all autonomic splits if any shard has a pending bulk graft.
        // (Bulk grafts mutate tree topology in ways the split coordinator
        // cannot interleave with safely.) Splits already draining are unaffected
        // by the graft, so publish the authoritative count we just measured
        // before bailing out rather than letting the footprint lapse.
        for (int i = 0; i < physicalShards.Count; i++)
        {
            if (!pendingBulkTasks[i].Result) continue;

            if (options.MaxClusterConcurrentAutoSplits is null && (inFlight > 0 || _reportedFootprint > 0))
                await PublishFootprintAsync(inFlight, options);
            else
                await RefreshOutstandingFootprintAsync(options);

            return;
        }

        var maxConcurrent = options.MaxConcurrentAutoSplits;
        if (maxConcurrent < 1) maxConcurrent = 1;
        var slotsAvailable = inFlight >= maxConcurrent ? 0 : maxConcurrent - inFlight;

        // Build the candidate list of hot, eligible shards. A shard is
        // eligible when it (a) is not already splitting, (b) is above the
        // ops/sec threshold, (c) is not in cooldown, and (d) owns at least
        // two virtual slots (otherwise there is nothing to subdivide).
        var threshold = options.HotShardOpsPerSecondThreshold;
        var candidates = new List<(double Rate, int ShardIndex)>(physicalShards.Count);
        if (slotsAvailable > 0)
        {
            for (int i = 0; i < physicalShards.Count; i++)
            {
                if (splittingTasks[i].Result) continue;

                var h = hotnessTasks[i].Result;
                if (h.Window <= TimeSpan.Zero) continue;
                var rate = (h.Reads + h.Writes) / h.Window.TotalSeconds;
                if (rate < threshold) continue;
                if (_shardCooldownUntilUtc.TryGetValue(physicalShards[i], out var until) && nowUtc < until) continue;

                var owned = 0;
                foreach (var slot in map.Slots)
                    if (slot == physicalShards[i]) { owned++; if (owned > 1) break; }
                if (owned < 2) continue;

                candidates.Add((rate, physicalShards[i]));
            }

            // Pick the top N hottest by rate (descending).
            candidates.Sort((a, b) => b.Rate.CompareTo(a.Rate));
        }

        // How many splits this tree would start under its own per-tree cap.
        var desiredNew = Math.Min(slotsAvailable, candidates.Count);

        // Cluster-wide admission gate. When a ceiling is configured, report this
        // tree's authoritative in-flight count every pass - even when it wants no
        // new splits - so other trees see this tree's drain footprint, and
        // receive a grant of new slots against the remaining cluster headroom.
        //
        // With no ceiling configured the gate makes no admission decision, but it
        // is still the cluster's readable split-activity source (surfaced by
        // ILatticeAdmin.GetSplitActivityAsync and consumed by the scaling
        // package's scale-in safety gate), so the footprint is published anyway.
        // That publication is edge-triggered - only while splits are actually in
        // flight, plus one final call to clear a footprint we previously reported
        // - so an idle tree issues no extra RPC at all and the disabled path
        // behaves exactly as before in steady state.
        int triggerCount;
        var clusterDeferred = 0;
        if (options.MaxClusterConcurrentAutoSplits is { } clusterCap)
        {
            var gate = grainFactory.GetGrain<IClusterSplitConcurrencyGrain>(ClusterSplitConcurrencyKey);
            var granted = await gate.AcquireSlotsAsync(
                TreeId, inFlight, desiredNew, clusterCap, FootprintTtl(options));
            triggerCount = granted;
            clusterDeferred = desiredNew - granted;
            _reportedFootprint = inFlight + granted;
        }
        else
        {
            triggerCount = desiredNew;

            // Report the post-trigger count, so splits started by this very pass
            // are visible to the safety gate immediately rather than one sampling
            // interval later. Over-reporting (a trigger that the coordinator
            // rejects as busy) is the conservative direction - it holds scale-in
            // slightly longer - and self-corrects on the next pass.
            var footprint = inFlight + triggerCount;
            if (footprint > 0 || _reportedFootprint > 0)
                await PublishFootprintAsync(footprint, options);
        }

        // Candidates the tree could not start this pass: those beyond its own
        // per-tree cap plus any held back by the cluster gate.
        var suppressed = (candidates.Count - desiredNew) + clusterDeferred;
        if (suppressed > 0)
            LatticeMetrics.SplitCandidatesSuppressed.Add(suppressed, treeTag, tenantTag);
        if (clusterDeferred > 0)
            LatticeMetrics.SplitAdmissionDeferred.Add(clusterDeferred, treeTag, LatticeMetrics.SplitDeferredClusterCapReasonTag, tenantTag);

        if (triggerCount == 0) return;

        // Trigger each split via its own per-shard coordinator key. Each
        // coordinator runs independently and persists its own state, so
        // multiple splits proceed in parallel without coordination on this
        // monitor grain.
        var triggers = new List<Task>(triggerCount);
        for (int i = 0; i < triggerCount; i++)
            triggers.Add(TriggerSplitAsync(candidates[i].ShardIndex, candidates[i].Rate, threshold, nowUtc + options.HotShardSplitCooldown));

        await Task.WhenAll(triggers);
    }

    private async Task TriggerSplitAsync(int shardIndex, double rate, int threshold, DateTime cooldownUntilUtc)
    {
        var splitGrain = grainFactory.GetGrain<ITreeShardSplitGrain>($"{TreeId}/{shardIndex}");
        try
        {
            await splitGrain.SplitAsync(shardIndex);
            _shardCooldownUntilUtc[shardIndex] = cooldownUntilUtc;
            logger.LogInformation(
                "Triggered autonomic split of shard {ShardIndex} for tree {TreeId} (rate={Rate:F1} ops/s, threshold={Threshold})",
                shardIndex, TreeId, rate, threshold);
        }
        catch (InvalidOperationException ex)
        {
            // Coordinator already busy on a different parameter set - ignore until next tick.
            logger.LogDebug(ex, "Could not trigger split for shard {ShardIndex} of tree {TreeId}", shardIndex, TreeId);
        }
    }
}
