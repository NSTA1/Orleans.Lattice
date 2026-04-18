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
/// in parallel, computes ops-per-second, and triggers a split on the hottest
/// eligible shard.
/// </para>
/// <para>
/// Suppression rules (a hot shard is <em>not</em> split if any apply):
/// </para>
/// <list type="bullet">
/// <item><description><see cref="LatticeOptions.AutoSplitEnabled"/> is <c>false</c>.</description></item>
/// <item><description>The tree is younger than <see cref="LatticeOptions.AutoSplitMinTreeAge"/> (since this monitor activated).</description></item>
/// <item><description>A resize, merge, or snapshot is in progress (<see cref="ILattice.IsResizeCompleteAsync"/> etc.).</description></item>
/// <item><description>Any physical shard is currently splitting (<see cref="IShardRootGrain.IsSplittingAsync"/>) or has a pending bulk graft (<see cref="IShardRootGrain.HasPendingBulkOperationAsync"/>).</description></item>
/// <item><description>The shard is in the per-shard cooldown window after a recent split.</description></item>
/// <item><description><see cref="LatticeOptions.MaxConcurrentAutoSplits"/> in-flight splits already running.</description></item>
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

        // Suppress when an existing split is still running (only one at a time per tree for v1).
        var splitGrain = grainFactory.GetGrain<ITreeShardSplitGrain>(TreeId);
        if (!await splitGrain.IsCompleteAsync()) return;

        // Resolve the current shard map and list of physical shards.
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var physicalTreeId = await registry.ResolveAsync(TreeId);
        var map = await registry.GetShardMapAsync(TreeId)
            ?? ShardMap.CreateDefault(options.VirtualShardCount, options.ShardCount);
        var physicalShards = map.GetPhysicalShardIndices();

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

        // Suppress all autonomic splits if any shard has a pending bulk graft
        // or is mid-split (the latter can occur if reminder fires during splits).
        for (int i = 0; i < physicalShards.Count; i++)
            if (pendingBulkTasks[i].Result || splittingTasks[i].Result) return;

        // Pick the hottest shard above threshold that is not in cooldown.
        var threshold = options.HotShardOpsPerSecondThreshold;
        var bestShardIndex = -1;
        double bestRate = 0;
        for (int i = 0; i < physicalShards.Count; i++)
        {
            var h = hotnessTasks[i].Result;
            if (h.Window <= TimeSpan.Zero) continue;
            var rate = (h.Reads + h.Writes) / h.Window.TotalSeconds;
            if (rate < threshold) continue;
            if (_shardCooldownUntilUtc.TryGetValue(physicalShards[i], out var until) && nowUtc < until) continue;

            // Don't try to split shards owning a single virtual slot — geometric
            // convergence already isolated them.
            var owned = 0;
            foreach (var slot in map.Slots)
                if (slot == physicalShards[i]) { owned++; if (owned > 1) break; }
            if (owned < 2) continue;

            if (rate > bestRate)
            {
                bestRate = rate;
                bestShardIndex = physicalShards[i];
            }
        }

        if (bestShardIndex < 0) return;

        // Trigger the split. Coordinator runs asynchronously after persisting intent.
        try
        {
            await splitGrain.SplitAsync(bestShardIndex);
            _shardCooldownUntilUtc[bestShardIndex] = nowUtc + options.HotShardSplitCooldown;
            logger.LogInformation(
                "Triggered autonomic split of shard {ShardIndex} for tree {TreeId} (rate={Rate:F1} ops/s, threshold={Threshold})",
                bestShardIndex, TreeId, bestRate, threshold);
        }
        catch (InvalidOperationException ex)
        {
            // Coordinator already busy — ignore until next tick.
            logger.LogDebug(ex, "Could not trigger split for shard {ShardIndex} of tree {TreeId}", bestShardIndex, TreeId);
        }
    }
}
