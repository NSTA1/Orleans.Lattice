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
        if (!options.AutoSplitEnabled) return;

        var nowUtc = DateTime.UtcNow;

        // Use the persisted activation time, initializing it if first use.
        var activationUtc = await GetOrSetActivationUtcAsync(nowUtc);

        // Enforce the min-age grace period before allowing splits to trigger.
        if (nowUtc - activationUtc < options.AutoSplitMinTreeAge) return;

        // Suppress while bulk maintenance is in flight.
        var lattice = grainFactory.GetGrain<ILattice>(TreeId);
        if (!await lattice.IsResizeCompleteAsync()) return;
        if (!await lattice.IsReshardCompleteAsync()) return;
        if (!await lattice.IsMergeCompleteAsync()) return;
        if (!await lattice.IsSnapshotCompleteAsync()) return;

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

        // Suppress all autonomic splits if any shard has a pending bulk graft.
        // (Bulk grafts mutate tree topology in ways the split coordinator
        // cannot interleave with safely.)
        for (int i = 0; i < physicalShards.Count; i++)
            if (pendingBulkTasks[i].Result) return;

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
        LatticeMetrics.SplitInFlight.Record(inFlight, treeTag);

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

        // Cluster-wide admission gate (opt-in). When enabled, report this tree's
        // authoritative in-flight count every pass - even when it wants no new
        // splits - so other trees see this tree's drain footprint, and receive a
        // grant of new slots against the remaining cluster headroom. When the
        // option is null the gate grain is never consulted, so the disabled path
        // issues no extra RPC and behaves exactly as before.
        int triggerCount;
        var clusterDeferred = 0;
        if (options.MaxClusterConcurrentAutoSplits is { } clusterCap)
        {
            var sampleInterval = options.HotShardSampleInterval;
            if (sampleInterval <= TimeSpan.Zero) sampleInterval = LatticeOptions.DefaultHotShardSampleInterval;
            var gate = grainFactory.GetGrain<IClusterSplitConcurrencyGrain>(ClusterSplitConcurrencyKey);
            var granted = await gate.AcquireSlotsAsync(
                TreeId, inFlight, desiredNew, clusterCap, sampleInterval * ClusterFootprintTtlSampleMultiple);
            triggerCount = granted;
            clusterDeferred = desiredNew - granted;
        }
        else
        {
            triggerCount = desiredNew;
        }

        // Candidates the tree could not start this pass: those beyond its own
        // per-tree cap plus any held back by the cluster gate.
        var suppressed = (candidates.Count - desiredNew) + clusterDeferred;
        if (suppressed > 0)
            LatticeMetrics.SplitCandidatesSuppressed.Add(suppressed, treeTag);
        if (clusterDeferred > 0)
            LatticeMetrics.SplitAdmissionDeferred.Add(clusterDeferred, treeTag, LatticeMetrics.SplitDeferredClusterCapReasonTag);

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
