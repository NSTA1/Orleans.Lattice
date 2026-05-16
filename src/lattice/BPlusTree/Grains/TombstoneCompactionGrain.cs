using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Singleton-per-tree grain that owns a single reminder for tombstone compaction.
/// When the reminder fires, a grain timer is started that processes one shard per
/// tick - avoiding a long-running grain call that could hit Orleans timeouts for
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
    LatticeOptionsResolver optionsResolver,
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
    private readonly PublishEventsGate _eventsGate = new();

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

        var (physicalTreeId, physicalShards) = await ResolveShardTopologyAsync();
        foreach (var shardIndex in physicalShards)
        {
            await CompactShardAsync(physicalTreeId, shardIndex, Options.TombstoneGracePeriod);
        }
    }

    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (IsCompactionDisabled) return;

        if (reminderName == ReminderName)
        {
            // Defensively re-register if the configured period drifts
            // from whatever Orleans is firing. Compare against the
            // TombstoneGracePeriod option each tick; if the effective period
            // differs, RegisterOrUpdateReminder replaces the schedule.
            var desired = ClampPeriod(Options.TombstoneGracePeriod);
            var actualPeriod = status.Period;
            if (actualPeriod != desired)
            {
                await reminderRegistry.RegisterOrUpdateReminder(
                    callingGrainId: context.GrainId,
                    reminderName: ReminderName,
                    dueTime: desired,
                    period: desired);
            }

            // Periodic compaction trigger - start a new pass if idle.
            if (_compactionTimer is not null) return;
            await StartCompactionAsync(startFromShard: 0);
        }
        else if (reminderName == KeepaliveReminderName)
        {
            // Keepalive fired - either resume a persisted in-flight pass or
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
        // Resolve the current shard topology (alias + physical shard list) and
        // persist it so the pass is resumable across silo restarts and immune
        // to mid-pass shard-map mutations (audit bug #1).
        var (physicalTreeId, physicalShards) = await ResolveShardTopologyAsync();

        // Snapshot before mutating so a transient WriteStateAsync failure
        // does not leave the in-memory InProgress / shard cursor ahead of
        // disk. The keepalive ReceiveReminder branch reads
        // state.State.InProgress directly and would short-circuit against
        // a dirty value otherwise.
        var prevInProgress = state.State.InProgress;
        var prevNextShardIndex = state.State.NextShardIndex;
        var prevShardRetries = state.State.ShardRetries;
        var prevPhysicalTreeId = state.State.PhysicalTreeId;
        var prevPhysicalShardIndices = state.State.PhysicalShardIndices;

        state.State.InProgress = true;
        state.State.NextShardIndex = startFromShard;
        state.State.ShardRetries = 0;
        state.State.PhysicalTreeId = physicalTreeId;
        state.State.PhysicalShardIndices = [.. physicalShards];
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.InProgress = prevInProgress;
            state.State.NextShardIndex = prevNextShardIndex;
            state.State.ShardRetries = prevShardRetries;
            state.State.PhysicalTreeId = prevPhysicalTreeId;
            state.State.PhysicalShardIndices = prevPhysicalShardIndices;
            throw;
        }

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
        var physicalShards = state.State.PhysicalShardIndices;
        if (physicalShards is null || physicalShards.Length == 0)
        {
            // State pre-dates the fix - refresh from the registry.
            var (physTreeId, shards) = await ResolveShardTopologyAsync();
            state.State.PhysicalTreeId = physTreeId;
            state.State.PhysicalShardIndices = [.. shards];
            physicalShards = state.State.PhysicalShardIndices;
        }

        if (state.State.NextShardIndex >= physicalShards.Length)
        {
            await CompleteCompactionAsync();
            return;
        }

        var physicalTreeId = state.State.PhysicalTreeId ?? TreeId;
        var shardIndex = physicalShards[state.State.NextShardIndex];

        // Distinguish "compaction failed -> apply retry/skip policy" from
        // "compaction succeeded but persist failed -> revert and rethrow".
        // Lifting the success-path persist out of the outer try ensures a
        // rethrown InvalidOperationException from the snapshot/restore
        // doesn't accidentally trip the compaction-failure handler and
        // bump ShardRetries.
        bool compactionSucceeded = false;
        try
        {
            await CompactShardAsync(physicalTreeId, shardIndex, Options.TombstoneGracePeriod);
            compactionSucceeded = true;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Tombstone compaction failed for shard {ShardIndex} of tree {TreeId}", shardIndex, TreeId);
            if (state.State.ShardRetries < MaxRetriesPerShard)
            {
                var prevShardRetries = state.State.ShardRetries;
                state.State.ShardRetries++;
                try
                {
                    await state.WriteStateAsync();
                }
                catch
                {
                    state.State.ShardRetries = prevShardRetries;
                    throw;
                }
            }
            else
            {
                // Exhausted retries for this shard - skip to next.
                var prevNextShardIndex = state.State.NextShardIndex;
                var prevShardRetries = state.State.ShardRetries;
                state.State.NextShardIndex++;
                state.State.ShardRetries = 0;
                try
                {
                    await state.WriteStateAsync();
                }
                catch
                {
                    state.State.NextShardIndex = prevNextShardIndex;
                    state.State.ShardRetries = prevShardRetries;
                    throw;
                }
            }
        }

        if (compactionSucceeded)
        {
            // Snapshot the cursor before advancing so a transient
            // WriteStateAsync failure does not leave NextShardIndex /
            // ShardRetries ahead of disk, which would cause the next tick to
            // skip a shard or believe a retry budget has been spent.
            var prevNextShardIndex = state.State.NextShardIndex;
            var prevShardRetries = state.State.ShardRetries;
            state.State.NextShardIndex++;
            state.State.ShardRetries = 0;
            try
            {
                await state.WriteStateAsync();
            }
            catch
            {
                state.State.NextShardIndex = prevNextShardIndex;
                state.State.ShardRetries = prevShardRetries;
                throw;
            }
        }
    }

    internal async Task CompleteCompactionAsync()
    {
        _compactionTimer?.Dispose();
        _compactionTimer = null;

        // Snapshot before mutating so a transient WriteStateAsync failure
        // does not leave InProgress / cursor / topology cleared in-memory
        // ahead of disk. The keepalive ReceiveReminder branch reads
        // state.State.InProgress directly: with a dirty (false) in-memory
        // value it would unregister the keepalive against a still-in-progress
        // disk record, forcing the next regular reminder tick to restart the
        // pass from shard 0 instead of resuming where it left off.
        var prevInProgress = state.State.InProgress;
        var prevNextShardIndex = state.State.NextShardIndex;
        var prevShardRetries = state.State.ShardRetries;
        var prevPhysicalTreeId = state.State.PhysicalTreeId;
        var prevPhysicalShardIndices = state.State.PhysicalShardIndices;

        state.State.InProgress = false;
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        state.State.PhysicalTreeId = null;
        state.State.PhysicalShardIndices = [];
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.InProgress = prevInProgress;
            state.State.NextShardIndex = prevNextShardIndex;
            state.State.ShardRetries = prevShardRetries;
            state.State.PhysicalTreeId = prevPhysicalTreeId;
            state.State.PhysicalShardIndices = prevPhysicalShardIndices;
            throw;
        }

        await UnregisterKeepaliveAsync();

        LatticeMetrics.CoordinatorCompleted.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagKind, "compaction"));

        await PublishCompactionCompletedAsync();

        // This grain does no work between passes - free the activation.
        // The next reminder tick will reactivate it.
        this.DeactivateOnIdle();
    }

    private async Task PublishCompactionCompletedAsync()
    {
        var opts = Options;
        if (!await _eventsGate.IsEnabledAsync(grainFactory, TreeId, opts)) return;
        var evt = LatticeEventPublisher.CreateEvent(LatticeTreeEventKind.CompactionCompleted, TreeId);
        await LatticeEventPublisher.PublishAsync(context.ActivationServices, opts, evt, logger);
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
            // Best effort - the keepalive will be cleaned up on the next tick
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

    private async Task CompactShardAsync(string physicalTreeId, int shardIndex, TimeSpan gracePeriod)
    {
        // Compaction is a maintenance pass: every WAL envelope emitted
        // by the leaf-level reap loop must be classified
        // `MutationCategory.Maintenance` so producer-side replication
        // filtering can short-circuit it. The scope flows via
        // `RequestContext` into the leaf grain call, where the inner
        // `LatticeMaintenanceContext.BeginScope()` in
        // `BPlusLeafGrain.CompactTombstonesAsync` is then a structural
        // no-op (the bit is already set). Belt-and-braces: the outer
        // scope here covers any future leaf path that emits without its
        // own inner scope.
        using var maintenanceScope = LatticeMaintenanceContext.BeginScope();

        var shardKey = $"{physicalTreeId}/{shardIndex}";
        var shardRoot = grainFactory.GetGrain<IShardRootGrain>(shardKey);

        var leafId = await shardRoot.GetLeftmostLeafIdAsync();

        while (leafId is not null)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
            await leaf.CompactTombstonesAsync(gracePeriod);
            leafId = await leaf.GetNextSiblingAsync();
        }
    }

    /// <summary>
    /// Resolves the physical tree id and the list of distinct physical shard
    /// indices for the current tree from the registry. Falls back to the
    /// logical tree id and a default identity <see cref="ShardMap"/> when
    /// the registry has no record (e.g. an unregistered tree in a test
    /// harness).
    /// </summary>
    private async Task<(string physicalTreeId, IReadOnlyList<int> physicalShards)> ResolveShardTopologyAsync()
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var resolved = await registry.ResolveAsync(TreeId);
        var physicalTreeId = string.IsNullOrEmpty(resolved) ? TreeId : resolved;
        var resolvedOpts = await optionsResolver.ResolveAsync(TreeId);
        var map = await registry.GetShardMapAsync(TreeId)
            ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, resolvedOpts.ShardCount);
        return (physicalTreeId, map.GetPhysicalShardIndices());
    }

    private static TimeSpan ClampPeriod(TimeSpan gracePeriod) =>
        gracePeriod < TimeSpan.FromMinutes(1) ? TimeSpan.FromMinutes(1) : gracePeriod;
}
