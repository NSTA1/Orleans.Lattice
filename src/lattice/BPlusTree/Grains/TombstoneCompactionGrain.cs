using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Singleton-per-tree grain that owns a single reminder for tombstone compaction.
/// When the reminder fires, a grain timer is started that processes one shard per
/// tick — avoiding a long-running grain call that could hit Orleans timeouts for
/// large trees. Failed shards are retried once before being skipped.
/// <para>
/// Compaction progress is persisted so that a silo restart mid-compaction can
/// resume where it left off. A one-minute keepalive reminder is registered at the
/// start of compaction and unregistered on completion; if the silo restarts, the
/// keepalive fires and resumes the in-flight pass.
/// </para>
/// Key format: <c>{treeId}</c>.
/// </summary>
internal sealed class TombstoneCompactionGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ILogger<TombstoneCompactionGrain> logger,
    [PersistentState("tombstone-compaction", LatticeOptions.StorageProviderName)]
    IPersistentState<TombstoneCompactionState> state) : ITombstoneCompactionGrain, IRemindable, IGrainBase
{
    private const string ReminderName = "tombstone-compaction";
    private const string KeepaliveReminderName = "compaction-keepalive";
    private const int MaxRetriesPerShard = 1;

    private string TreeId => context.GrainId.Key.ToString()!;
    private LatticeOptions Options => optionsMonitor.Get(TreeId);
    IGrainContext IGrainBase.GrainContext => context;

    private IGrainTimer? _compactionTimer;

    private bool IsCompactionDisabled => Options.TombstoneGracePeriod == Timeout.InfiniteTimeSpan;

    public async Task EnsureReminderAsync()
    {
        if (IsCompactionDisabled) return;

        var period = ClampPeriod(Options.TombstoneGracePeriod);
        await reminderRegistry.RegisterOrUpdateReminder(
            callingGrainId: context.GrainId,
            reminderName: ReminderName,
            dueTime: period,
            period: period);
    }

    public async Task RunCompactionPassAsync()
    {
        if (IsCompactionDisabled) return;

        var options = Options;
        for (int i = 0; i < options.ShardCount; i++)
        {
            await CompactShardAsync(i, options.TombstoneGracePeriod);
        }
    }

    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (IsCompactionDisabled) return;

        if (reminderName == ReminderName)
        {
            // Periodic compaction trigger — start a new pass if idle.
            if (_compactionTimer is not null) return;
            await StartCompactionAsync(startFromShard: 0);
        }
        else if (reminderName == KeepaliveReminderName)
        {
            // Keepalive fired — either resume a persisted in-flight pass or
            // clean up if compaction already finished.
            if (state.State.InProgress && _compactionTimer is null)
            {
                await StartCompactionAsync(startFromShard: state.State.NextShardIndex);
            }
            else if (!state.State.InProgress)
            {
                await UnregisterKeepaliveAsync();
            }
        }
    }

    /// <summary>
    /// Begins a compaction pass: persists in-progress state, registers the
    /// keepalive reminder, and starts the grain timer. Exposed as
    /// <c>internal</c> for unit testing (tests call
    /// <see cref="BeginCompactionStateAsync"/> + <see cref="ProcessNextShardAsync"/>
    /// directly to avoid the Orleans timer infrastructure).
    /// </summary>
    internal async Task StartCompactionAsync(int startFromShard)
    {
        await BeginCompactionStateAsync(startFromShard);

        // Fire immediately, then tick every 2 seconds per shard.
        _compactionTimer = this.RegisterGrainTimer(
            OnCompactionTimerTick,
            new GrainTimerCreationOptions(dueTime: TimeSpan.Zero, period: TimeSpan.FromSeconds(2)));
    }

    /// <summary>
    /// Persists the in-progress marker and registers the keepalive reminder
    /// without starting the grain timer. Used by <see cref="StartCompactionAsync"/>
    /// and directly by unit tests.
    /// </summary>
    internal async Task BeginCompactionStateAsync(int startFromShard)
    {
        // Persist in-progress state before starting.
        state.State.InProgress = true;
        state.State.NextShardIndex = startFromShard;
        state.State.ShardRetries = 0;
        await state.WriteStateAsync();

        // Register a 1-minute keepalive so the grain is reactivated after a
        // silo restart. The minimum Orleans reminder period is 1 minute.
        await reminderRegistry.RegisterOrUpdateReminder(
            callingGrainId: context.GrainId,
            reminderName: KeepaliveReminderName,
            dueTime: TimeSpan.FromMinutes(1),
            period: TimeSpan.FromMinutes(1));
    }

    private async Task OnCompactionTimerTick(CancellationToken ct)
    {
        await ProcessNextShardAsync();
    }

    /// <summary>
    /// Processes the next shard in the compaction pass. If all shards are done,
    /// completes the pass. Called by the grain timer tick; exposed as
    /// <c>internal</c> for unit testing without the Orleans timer infrastructure.
    /// </summary>
    internal async Task ProcessNextShardAsync()
    {
        var options = Options;
        var shardCount = options.ShardCount;

        if (state.State.NextShardIndex >= shardCount)
        {
            await CompleteCompactionAsync();
            return;
        }

        try
        {
            await CompactShardAsync(state.State.NextShardIndex, options.TombstoneGracePeriod);
            state.State.NextShardIndex++;
            state.State.ShardRetries = 0;
            await state.WriteStateAsync();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Tombstone compaction failed for shard {ShardIndex} of tree {TreeId}", state.State.NextShardIndex, TreeId);
            if (state.State.ShardRetries < MaxRetriesPerShard)
            {
                state.State.ShardRetries++;
                await state.WriteStateAsync();
            }
            else
            {
                // Exhausted retries for this shard — skip to next.
                state.State.NextShardIndex++;
                state.State.ShardRetries = 0;
                await state.WriteStateAsync();
            }
        }
    }

    internal async Task CompleteCompactionAsync()
    {
        _compactionTimer?.Dispose();
        _compactionTimer = null;

        state.State.InProgress = false;
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        await state.WriteStateAsync();

        await UnregisterKeepaliveAsync();

        // This grain does no work between passes — free the activation.
        // The next reminder tick will reactivate it.
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
            // Best effort — the keepalive will be cleaned up on the next tick
            // if it fires while InProgress is false.
            logger.LogWarning(ex, "Failed to unregister keepalive reminder for tree {TreeId}", TreeId);
        }
    }

    public async Task UnregisterReminderAsync()
    {
        try
        {
            var reminder = await reminderRegistry.GetReminder(context.GrainId, ReminderName);
            if (reminder is not null)
                await reminderRegistry.UnregisterReminder(context.GrainId, reminder);
        }
        catch (Exception ex) { logger.LogWarning(ex, "Failed to unregister compaction reminder for tree {TreeId}", TreeId); }

        await UnregisterKeepaliveAsync();

        _compactionTimer?.Dispose();
        _compactionTimer = null;

        this.DeactivateOnIdle();
    }

    private async Task CompactShardAsync(int shardIndex, TimeSpan gracePeriod)
    {
        var shardKey = $"{TreeId}/{shardIndex}";
        var shardRoot = grainFactory.GetGrain<IShardRootGrain>(shardKey);

        var leafId = await shardRoot.GetLeftmostLeafIdAsync();

        while (leafId is not null)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
            await leaf.CompactTombstonesAsync(gracePeriod);
            leafId = await leaf.GetNextSiblingAsync();
        }
    }

    private static TimeSpan ClampPeriod(TimeSpan gracePeriod) =>
        gracePeriod < TimeSpan.FromMinutes(1) ? TimeSpan.FromMinutes(1) : gracePeriod;
}
