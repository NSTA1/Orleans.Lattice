using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Manages tree-level soft deletion and deferred purge. When a tree is deleted,
/// all shards are marked as deleted (blocking reads/writes), and a grain reminder
/// is registered to fire after <see cref="LatticeOptions.SoftDeleteDuration"/>.
/// When the reminder fires and the soft-delete window has elapsed, a grain timer
/// walks each shard one-by-one (same pattern as <see cref="TombstoneCompactionGrain"/>),
/// clearing all leaf and internal node state and deactivating grains.
/// </summary>
internal sealed class TreeDeletionGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    LatticeOptionsResolver optionsResolver,
    ILogger<TreeDeletionGrain> logger,
    [PersistentState("tree-deletion", LatticeOptions.StorageProviderName)]
    IPersistentState<TreeDeletionState> state) : ITreeDeletionGrain, IRemindable, IGrainBase
{
    private const string ReminderName = "tree-deletion";
    private const string KeepaliveReminderName = "deletion-keepalive";
    private const int MaxRetriesPerShard = 1;

    private string TreeId => context.GrainId.Key.ToString()!;
    private LatticeOptions Options => optionsMonitor.Get(TreeId);
    IGrainContext IGrainBase.GrainContext => context;

    private IGrainTimer? _purgeTimer;

    public async Task DeleteTreeAsync()
    {
        if (state.State.IsDeleted) return;

        var resolved = await optionsResolver.ResolveAsync(TreeId);

        // Mark all shards as deleted first.
        var shardCount = resolved.ShardCount;
        var tasks = new Task[shardCount];
        for (int i = 0; i < shardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            tasks[i] = shard.MarkDeletedAsync();
        }
        await Task.WhenAll(tasks);

        // Persist the deletion state.
        state.State.IsDeleted = true;
        state.State.DeletedAtUtc = DateTimeOffset.UtcNow;
        await state.WriteStateAsync();

        // Unregister the tombstone compaction reminder — no longer needed.
        var compaction = grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId);
        await compaction.UnregisterReminderAsync();

        // Register the purge reminder.
        var period = ClampPeriod(Options.SoftDeleteDuration);
        await reminderRegistry.RegisterOrUpdateReminder(
            callingGrainId: context.GrainId,
            reminderName: ReminderName,
            dueTime: period,
            period: period);
    }

    public Task<bool> IsDeletedAsync() => Task.FromResult(state.State.IsDeleted);

    public async Task RecoverAsync()
    {
        if (!state.State.IsDeleted)
            throw new InvalidOperationException("Cannot recover a tree that has not been deleted.");

        if (state.State.PurgeComplete)
            throw new InvalidOperationException("Cannot recover a tree whose data has already been purged.");

        if (state.State.PurgeInProgress)
            throw new InvalidOperationException("Cannot recover a tree while a purge is in progress.");

        // Unmark all shards.
        var resolved = await optionsResolver.ResolveAsync(TreeId);
        var tasks = new Task[resolved.ShardCount];
        for (int i = 0; i < resolved.ShardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}");
            tasks[i] = shard.UnmarkDeletedAsync();
        }
        await Task.WhenAll(tasks);

        // Clear deletion state.
        state.State.IsDeleted = false;
        state.State.DeletedAtUtc = null;
        await state.WriteStateAsync();

        // Unregister the purge reminder.
        await UnregisterAllRemindersAsync();

        // Re-instate the tombstone compaction reminder.
        var compaction = grainFactory.GetGrain<ITombstoneCompactionGrain>(TreeId);
        await compaction.EnsureReminderAsync();
    }

    public async Task PurgeNowAsync()
    {
        if (!state.State.IsDeleted)
            throw new InvalidOperationException("Cannot purge a tree that has not been deleted.");

        if (state.State.PurgeComplete)
            throw new InvalidOperationException("This tree has already been fully purged.");

        // Run purge synchronously shard-by-shard (no timer needed for manual purge).
        var resolved = await optionsResolver.ResolveAsync(TreeId);
        for (int i = 0; i < resolved.ShardCount; i++)
        {
            await PurgeShardAsync(i);
        }

        // Mark complete and clean up.
        state.State.PurgeInProgress = false;
        state.State.PurgeComplete = true;
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        await state.WriteStateAsync();

        await UnregisterAllRemindersAsync();
        this.DeactivateOnIdle();
    }

    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (!state.State.IsDeleted) return;

        if (state.State.PurgeComplete)
        {
            // Already done — unregister all reminders and deactivate.
            await UnregisterAllRemindersAsync();
            this.DeactivateOnIdle();
            return;
        }

        if (reminderName == ReminderName)
        {
            // Check if the soft-delete window has elapsed.
            var elapsed = DateTimeOffset.UtcNow - (state.State.DeletedAtUtc ?? DateTimeOffset.UtcNow);
            if (elapsed < Options.SoftDeleteDuration)
                return; // Not yet — wait for the next tick.

            if (_purgeTimer is not null) return;
            await StartPurgeAsync(startFromShard: 0);
        }
        else if (reminderName == KeepaliveReminderName)
        {
            if (state.State.PurgeInProgress && _purgeTimer is null)
            {
                await StartPurgeAsync(startFromShard: state.State.NextShardIndex);
            }
            else if (!state.State.PurgeInProgress && state.State.PurgeComplete)
            {
                await UnregisterAllRemindersAsync();
                this.DeactivateOnIdle();
            }
        }
    }

    internal async Task StartPurgeAsync(int startFromShard)
    {
        await BeginPurgeStateAsync(startFromShard);

        _purgeTimer = this.RegisterGrainTimer(
            OnPurgeTimerTick,
            new GrainTimerCreationOptions(dueTime: TimeSpan.Zero, period: TimeSpan.FromSeconds(2)));
    }

    internal async Task BeginPurgeStateAsync(int startFromShard)
    {
        state.State.PurgeInProgress = true;
        state.State.NextShardIndex = startFromShard;
        state.State.ShardRetries = 0;
        await state.WriteStateAsync();

        await reminderRegistry.RegisterOrUpdateReminder(
            callingGrainId: context.GrainId,
            reminderName: KeepaliveReminderName,
            dueTime: TimeSpan.FromMinutes(1),
            period: TimeSpan.FromMinutes(1));
    }

    private async Task OnPurgeTimerTick(CancellationToken ct)
    {
        await ProcessNextShardAsync();
    }

    internal async Task ProcessNextShardAsync()
    {
        var resolved = await optionsResolver.ResolveAsync(TreeId);
        var shardCount = resolved.ShardCount;

        if (state.State.NextShardIndex >= shardCount)
        {
            await CompletePurgeAsync();
            return;
        }

        try
        {
            await PurgeShardAsync(state.State.NextShardIndex);
            state.State.NextShardIndex++;
            state.State.ShardRetries = 0;
            await state.WriteStateAsync();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Purge failed for shard {ShardIndex} of tree {TreeId}", state.State.NextShardIndex, TreeId);
            if (state.State.ShardRetries < MaxRetriesPerShard)
            {
                state.State.ShardRetries++;
                await state.WriteStateAsync();
            }
            else
            {
                state.State.NextShardIndex++;
                state.State.ShardRetries = 0;
                await state.WriteStateAsync();
            }
        }
    }

    internal async Task CompletePurgeAsync()
    {
        _purgeTimer?.Dispose();
        _purgeTimer = null;

        state.State.PurgeInProgress = false;
        state.State.PurgeComplete = true;
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        await state.WriteStateAsync();

        // Remove the tree from the registry.
        if (!TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
        {
            var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            await registry.UnregisterAsync(TreeId);
        }

        await UnregisterAllRemindersAsync();
        this.DeactivateOnIdle();
    }

    private async Task PurgeShardAsync(int shardIndex)
    {
        var shardKey = $"{TreeId}/{shardIndex}";
        var shardRoot = grainFactory.GetGrain<IShardRootGrain>(shardKey);

        await shardRoot.PurgeAsync();
    }

    private async Task UnregisterAllRemindersAsync()
    {
        try
        {
            var reminder = await reminderRegistry.GetReminder(context.GrainId, ReminderName);
            if (reminder is not null)
                    await reminderRegistry.UnregisterReminder(context.GrainId, reminder);
                }
                catch (Exception ex) { logger.LogWarning(ex, "Failed to unregister deletion reminder for tree {TreeId}", TreeId); }

                try
                {
                    var reminder = await reminderRegistry.GetReminder(context.GrainId, KeepaliveReminderName);
                    if (reminder is not null)
                        await reminderRegistry.UnregisterReminder(context.GrainId, reminder);
                }
                catch (Exception ex) { logger.LogWarning(ex, "Failed to unregister deletion keepalive reminder for tree {TreeId}", TreeId); }
    }

    private static TimeSpan ClampPeriod(TimeSpan duration) =>
        duration < TimeSpan.FromMinutes(1) ? TimeSpan.FromMinutes(1) : duration;
}
