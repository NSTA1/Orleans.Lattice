using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Per-tree autonomic monitor for adaptive shard splitting (F-011).
/// <para>
/// The monitor is started lazily by <c>LatticeGrain</c> on the first write
/// to a tree and re-activates on silo restart via a keepalive reminder. On
/// each tick it polls every physical shard's <see cref="IShardRootGrain.GetHotnessAsync"/>
/// in parallel, computes ops-per-second, and triggers splits on up to
/// <see cref="LatticeOptions.MaxConcurrentAutoSplits"/> of the hottest
/// eligible shards in parallel.
/// </para>
/// <para>
/// Suppression rules:
/// </para>
/// <list type="bullet">
/// <item><description><see cref="LatticeOptions.AutoSplitEnabled"/> is <c>false</c> — entire pass returns.</description></item>
/// <item><description>The tree is younger than <see cref="LatticeOptions.AutoSplitMinTreeAge"/> (since this monitor activated) — entire pass returns.</description></item>
/// <item><description>A resize, merge, or snapshot is in progress (<see cref="ILattice.IsResizeCompleteAsync"/> etc.) — entire pass returns.</description></item>
/// <item><description>Any physical shard has a pending bulk graft (<see cref="IShardRootGrain.HasPendingBulkOperationAsync"/>) — entire pass returns.</description></item>
/// <item><description>A shard is already splitting — that shard is skipped and counts toward the in-flight cap.</description></item>
/// <item><description>The shard is in the per-shard cooldown window after a recent split — that shard is skipped.</description></item>
/// <item><description>The shard owns fewer than two virtual slots — that shard is skipped (nothing to subdivide).</description></item>
/// <item><description><see cref="LatticeOptions.MaxConcurrentAutoSplits"/> in-flight splits already running — no further splits this tick.</description></item>
/// </list>
/// Key format: <c>{treeId}</c>.
/// </summary>
internal sealed class HotShardMonitorGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ILogger<HotShardMonitorGrain> logger) : IHotShardMonitorGrain, IRemindable, IGrainBase
{
    private const string KeepaliveReminderName = "hot-shard-monitor";

    private string TreeId => context.GrainId.Key.ToString()!;
    private LatticeOptions Options => optionsMonitor.Get(TreeId);
    IGrainContext IGrainBase.GrainContext => context;

    private IGrainTimer? _timer;
    private DateTime? _activationUtc;
    private readonly Dictionary<int, DateTime> _shardCooldownUntilUtc = [];
    private bool _running;

    /// <inheritdoc />
    public async Task EnsureRunningAsync()
    {
        if (_running) return;
        _running = true;
        _activationUtc ??= DateTime.UtcNow;

        var options = Options;
        if (!options.AutoSplitEnabled) return;

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
            logger.LogWarning(ex, "Failed to unregister hot-shard-monitor reminder for tree {TreeId}", TreeId);
        }

        this.DeactivateOnIdle();
    }

    /// <inheritdoc />
    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName != KeepaliveReminderName) return;
        _activationUtc ??= DateTime.UtcNow;
        if (!Options.AutoSplitEnabled) return;
        if (_timer is null) StartTimer();
        _running = true;
        await Task.CompletedTask;
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

        _activationUtc ??= DateTime.UtcNow;
        var nowUtc = DateTime.UtcNow;

        if (nowUtc - _activationUtc.Value < options.AutoSplitMinTreeAge) return;

        // Suppress while bulk maintenance is in flight.
        var lattice = grainFactory.GetGrain<ILattice>(TreeId);
        if (!await lattice.IsResizeCompleteAsync()) return;
        if (!await lattice.IsMergeCompleteAsync()) return;
        if (!await lattice.IsSnapshotCompleteAsync()) return;

        // Resolve the current shard map and list of physical shards.
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var physicalTreeId = await registry.ResolveAsync(TreeId);
        var map = await registry.GetShardMapAsync(TreeId)
            ?? ShardMap.CreateDefault(options.VirtualShardCount, options.ShardCount);
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

        var maxConcurrent = options.MaxConcurrentAutoSplits;
        if (maxConcurrent < 1) maxConcurrent = 1;
        if (inFlight >= maxConcurrent) return;
        var slotsAvailable = maxConcurrent - inFlight;

        // Build the candidate list of hot, eligible shards. A shard is
        // eligible when it (a) is not already splitting, (b) is above the
        // ops/sec threshold, (c) is not in cooldown, and (d) owns at least
        // two virtual slots (otherwise there is nothing to subdivide).
        var threshold = options.HotShardOpsPerSecondThreshold;
        var candidates = new List<(double Rate, int ShardIndex)>(physicalShards.Count);
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

        if (candidates.Count == 0) return;

        // Pick the top N hottest by rate (descending).
        candidates.Sort((a, b) => b.Rate.CompareTo(a.Rate));
        var triggerCount = Math.Min(slotsAvailable, candidates.Count);

        // Trigger each split via its own per-shard coordinator key. Each
        // coordinator runs independently and persists its own state, so
        // multiple splits proceed in parallel without coordination on this
        // monitor grain.
        var triggers = new List<Task>(triggerCount);
        for (int i = 0; i < triggerCount; i++)
        {
            var shardIndex = candidates[i].ShardIndex;
            var rate = candidates[i].Rate;
            triggers.Add(TriggerSplitAsync(shardIndex, rate, threshold, nowUtc + options.HotShardSplitCooldown));
        }
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
            // Coordinator already busy on a different parameter set — ignore until next tick.
            logger.LogDebug(ex, "Could not trigger split for shard {ShardIndex} of tree {TreeId}", shardIndex, TreeId);
        }
    }
}
