using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Merges all entries from a source tree into the target tree using LWW semantics,
/// preserving original timestamps. Works shard-by-shard: each source shard's leaf
/// chain is drained into memory (including tombstones) and then merged into the
/// target tree's shards.
/// <para>
/// Follows the same reminder + keepalive + grain-timer pattern used by
/// <see cref="TreeSnapshotGrain"/>.
/// Progress is persisted per-shard so that a silo restart mid-merge can
/// resume without data loss.
/// </para>
/// Key format: <c>{targetTreeId}</c>.
/// </summary>
internal sealed class TreeMergeGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ILogger<TreeMergeGrain> logger,
    [PersistentState("tree-merge", LatticeOptions.StorageProviderName)]
    IPersistentState<TreeMergeState> state) : ITreeMergeGrain, IRemindable, IGrainBase
{
    private const string KeepaliveReminderName = "merge-keepalive";
    private const int MaxRetriesPerShard = 1;

    private string TargetTreeId => context.GrainId.Key.ToString()!;
    private LatticeOptions TargetOptions => optionsMonitor.Get(TargetTreeId);
    IGrainContext IGrainBase.GrainContext => context;

    private IGrainTimer? _mergeTimer;

    public async Task MergeAsync(string sourceTreeId)
    {
        ArgumentNullException.ThrowIfNull(sourceTreeId);

        if (string.Equals(TargetTreeId, sourceTreeId, StringComparison.Ordinal))
            throw new ArgumentException("Source tree ID must differ from the target tree ID.", nameof(sourceTreeId));

        if (sourceTreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
            throw new ArgumentException($"Source tree ID must not start with the reserved prefix '{LatticeConstants.SystemTreePrefix}'.", nameof(sourceTreeId));

        if (state.State.InProgress)
        {
            // Idempotent if same source.
            if (state.State.SourceTreeId == sourceTreeId)
                return;

            throw new InvalidOperationException(
                $"A merge is already in progress for tree '{TargetTreeId}' from source '{state.State.SourceTreeId}'.");
        }

        // Validate source tree exists.
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        if (!await registry.ExistsAsync(sourceTreeId))
            throw new InvalidOperationException(
                $"Source tree '{sourceTreeId}' does not exist.");

        var sourceOptions = optionsMonitor.Get(sourceTreeId);

        await InitiateMergeStateAsync(sourceTreeId, sourceOptions.ShardCount);
        await StartMergeAsync();
    }

    /// <summary>
    /// Persists merge intent. Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task InitiateMergeStateAsync(string sourceTreeId, int sourceShardCount)
    {
        state.State.InProgress = true;
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        state.State.SourceTreeId = sourceTreeId;
        state.State.SourceShardCount = sourceShardCount;
        state.State.Complete = false;
        await state.WriteStateAsync();
    }

    public async Task RunMergePassAsync()
    {
        if (!state.State.InProgress) return;

        while (state.State.NextShardIndex < state.State.SourceShardCount)
        {
            await ProcessCurrentShardAsync();
        }

        await CompleteMergeAsync();
    }

    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName == KeepaliveReminderName)
        {
            if (state.State.InProgress && _mergeTimer is null)
            {
                await StartMergeTimerAsync();
            }
            else if (!state.State.InProgress)
            {
                await UnregisterKeepaliveAsync();
                this.DeactivateOnIdle();
            }
        }
    }

    private async Task StartMergeAsync()
    {
        await reminderRegistry.RegisterOrUpdateReminder(
            callingGrainId: context.GrainId,
            reminderName: KeepaliveReminderName,
            dueTime: TimeSpan.FromMinutes(1),
            period: TimeSpan.FromMinutes(1));

        await StartMergeTimerAsync();
    }

    private Task StartMergeTimerAsync()
    {
        _mergeTimer = this.RegisterGrainTimer(
            OnMergeTimerTick,
            new GrainTimerCreationOptions(dueTime: TimeSpan.Zero, period: TimeSpan.FromSeconds(2)));
        return Task.CompletedTask;
    }

    /// <summary>
    /// Persists the in-progress marker and registers the keepalive reminder
    /// without starting the grain timer. Used by unit tests.
    /// </summary>
    internal async Task BeginMergeStateAsync(int startFromShard)
    {
        state.State.NextShardIndex = startFromShard;
        state.State.ShardRetries = 0;
        await state.WriteStateAsync();

        await reminderRegistry.RegisterOrUpdateReminder(
            callingGrainId: context.GrainId,
            reminderName: KeepaliveReminderName,
            dueTime: TimeSpan.FromMinutes(1),
            period: TimeSpan.FromMinutes(1));
    }

    private async Task OnMergeTimerTick(CancellationToken ct)
    {
        await ProcessNextShardAsync();
    }

    /// <summary>
    /// Processes the next source shard. If all shards are done,
    /// completes the merge. Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task ProcessNextShardAsync()
    {
        if (state.State.NextShardIndex >= state.State.SourceShardCount)
        {
            await CompleteMergeAsync();
            return;
        }

        await ProcessCurrentShardAsync();
    }

    private async Task ProcessCurrentShardAsync()
    {
        var shardIndex = state.State.NextShardIndex;

        try
        {
            await MergeShardAsync(shardIndex);

            state.State.NextShardIndex++;
            state.State.ShardRetries = 0;
            await state.WriteStateAsync();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Merge failed for source shard {ShardIndex} of tree {SourceTreeId} into {TargetTreeId}",
                shardIndex, state.State.SourceTreeId, TargetTreeId);

            if (state.State.ShardRetries < MaxRetriesPerShard)
            {
                state.State.ShardRetries++;
                await state.WriteStateAsync();
            }
            else
            {
                throw;
            }
        }
    }

    /// <summary>
    /// Drains all entries (including tombstones) from the source shard's leaf chain,
    /// groups them by target shard, and merges into each target shard.
    /// </summary>
    private async Task MergeShardAsync(int sourceShardIndex)
    {
        var sourceTreeId = state.State.SourceTreeId!;
        var sourceShardKey = $"{sourceTreeId}/{sourceShardIndex}";
        var sourceShard = grainFactory.GetGrain<IShardRootGrain>(sourceShardKey);
        var leafId = await sourceShard.GetLeftmostLeafIdAsync();

        // Collect all raw entries from this source shard using delta extraction
        // with an empty version vector (returns all entries since everything is
        // newer than zero).
        var emptyVector = new VersionVector();
        var allEntries = new Dictionary<string, LwwValue<byte[]>>();
        while (leafId is not null)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
            var delta = await leaf.GetDeltaSinceAsync(emptyVector);
            foreach (var (key, lww) in delta.Entries)
            {
                allEntries[key] = lww;
            }
            leafId = await leaf.GetNextSiblingAsync();
        }

        if (allEntries.Count == 0) return;

        // Group entries by target shard.
        var targetShardCount = TargetOptions.ShardCount;
        var targetBuckets = new Dictionary<int, Dictionary<string, LwwValue<byte[]>>>();
        foreach (var (key, lww) in allEntries)
        {
            var targetIdx = LatticeSharding.GetShardIndex(key, targetShardCount);
            if (!targetBuckets.TryGetValue(targetIdx, out var bucket))
            {
                bucket = [];
                targetBuckets[targetIdx] = bucket;
            }
            bucket[key] = lww;
        }

        // Merge into each target shard.
        var tasks = new List<Task>(targetBuckets.Count);
        foreach (var (targetIdx, bucket) in targetBuckets)
        {
            var targetShardKey = $"{TargetTreeId}/{targetIdx}";
            var targetShard = grainFactory.GetGrain<IShardRootGrain>(targetShardKey);
            tasks.Add(targetShard.MergeManyAsync(bucket));
        }

        await Task.WhenAll(tasks);
    }

    internal async Task CompleteMergeAsync()
    {
        _mergeTimer?.Dispose();
        _mergeTimer = null;

        state.State.InProgress = false;
        state.State.Complete = true;
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        await state.WriteStateAsync();

        await UnregisterKeepaliveAsync();
        this.DeactivateOnIdle();
    }

    private async Task UnregisterKeepaliveAsync()
    {
        try
        {
            var reminder = await reminderRegistry.GetReminder(context.GrainId, KeepaliveReminderName);
            if (reminder is not null)
            {
                await reminderRegistry.UnregisterReminder(context.GrainId, reminder);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to unregister merge keepalive reminder for tree {TreeId}", TargetTreeId);
        }
    }

    /// <inheritdoc />
    public Task<bool> IsCompleteAsync() =>
        Task.FromResult(!state.State.InProgress);
}
