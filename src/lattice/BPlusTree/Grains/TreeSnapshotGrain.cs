using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Snapshots a source tree into a new destination tree, copying all live entries
/// shard-by-shard. Supports offline mode (source tree locked during copy) and
/// online mode (source tree remains available).
/// <para>
/// Follows the same reminder + keepalive + grain-timer pattern used by
/// <see cref="TombstoneCompactionGrain"/> and <see cref="TreeResizeGrain"/>.
/// Progress is persisted per-phase so that a silo restart mid-snapshot can
/// resume without data loss.
/// </para>
/// Key format: <c>{sourceTreeId}</c>.
/// </summary>
internal sealed class TreeSnapshotGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ILogger<TreeSnapshotGrain> logger,
    [PersistentState("tree-snapshot", LatticeOptions.StorageProviderName)]
    IPersistentState<TreeSnapshotState> state) : ITreeSnapshotGrain, IRemindable, IGrainBase
{
    private const string KeepaliveReminderName = "snapshot-keepalive";
    private const int MaxRetriesPerPhase = 1;

    private string SourceTreeId => context.GrainId.Key.ToString()!;
    private LatticeOptions Options => optionsMonitor.Get(SourceTreeId);
    IGrainContext IGrainBase.GrainContext => context;

    private IGrainTimer? _snapshotTimer;

    public async Task SnapshotAsync(string destinationTreeId, SnapshotMode mode,
        int? maxLeafKeys = null, int? maxInternalChildren = null)
    {
        ArgumentNullException.ThrowIfNull(destinationTreeId);

        if (maxLeafKeys is not null && maxLeafKeys <= 1)
            throw new ArgumentOutOfRangeException(nameof(maxLeafKeys), "Must be greater than 1.");
        if (maxInternalChildren is not null && maxInternalChildren <= 2)
            throw new ArgumentOutOfRangeException(nameof(maxInternalChildren), "Must be greater than 2.");

        if (string.Equals(SourceTreeId, destinationTreeId, StringComparison.Ordinal))
            throw new ArgumentException("Destination tree ID must differ from the source tree ID.", nameof(destinationTreeId));

        if (destinationTreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
            throw new ArgumentException($"Destination tree ID must not start with the reserved prefix '{LatticeConstants.SystemTreePrefix}'.", nameof(destinationTreeId));

        if (state.State.InProgress)
        {
            // Idempotent if same parameters.
            if (state.State.DestinationTreeId == destinationTreeId &&
                state.State.Mode == mode &&
                state.State.MaxLeafKeys == maxLeafKeys &&
                state.State.MaxInternalChildren == maxInternalChildren)
                return;

            throw new InvalidOperationException(
                $"A snapshot is already in progress for tree '{SourceTreeId}' to destination '{state.State.DestinationTreeId}'.");
        }

        if (state.State.Complete)
        {
            state.State.Complete = false;
        }

        // Validate shard counts match.
        var sourceOptions = Options;
        var destOptions = optionsMonitor.Get(destinationTreeId);
        if (sourceOptions.ShardCount != destOptions.ShardCount)
            throw new InvalidOperationException(
                $"Source tree '{SourceTreeId}' has {sourceOptions.ShardCount} shards but destination " +
                $"'{destinationTreeId}' is configured with {destOptions.ShardCount} shards. Shard counts must match.");

        // Validate destination tree doesn't already exist.
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        if (await registry.ExistsAsync(destinationTreeId))
            throw new InvalidOperationException(
                $"Destination tree '{destinationTreeId}' already exists. Choose a new tree ID.");

        await InitiateSnapshotStateAsync(destinationTreeId, mode, sourceOptions.ShardCount,
            maxLeafKeys, maxInternalChildren);
        await StartSnapshotAsync();
    }

    /// <summary>
    /// Persists snapshot intent and registers the destination tree in the registry.
    /// For offline mode, sets <see cref="SnapshotPhase.Lock"/> so that shard marking
    /// is deferred to <see cref="LockSourceShardsAsync"/>. Exposed as <c>internal</c>
    /// for unit testing.
    /// </summary>
    internal async Task InitiateSnapshotStateAsync(string destinationTreeId, SnapshotMode mode,
        int shardCount, int? maxLeafKeys = null, int? maxInternalChildren = null)
    {
        // Register the destination tree in the registry before any data is written.
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var entry = (maxLeafKeys is not null || maxInternalChildren is not null)
            ? new TreeRegistryEntry
            {
                MaxLeafKeys = maxLeafKeys,
                MaxInternalChildren = maxInternalChildren,
            }
            : null;
        await registry.RegisterAsync(destinationTreeId, entry);

        // Persist intent BEFORE any shard-marking side effects.
        state.State.InProgress = true;
        state.State.Phase = mode == SnapshotMode.Offline ? SnapshotPhase.Lock : SnapshotPhase.Copy;
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        state.State.DestinationTreeId = destinationTreeId;
        state.State.Mode = mode;
        state.State.OperationId = Guid.NewGuid().ToString("N");
        state.State.ShardCount = shardCount;
        state.State.MaxLeafKeys = maxLeafKeys;
        state.State.MaxInternalChildren = maxInternalChildren;
        state.State.Complete = false;
        await state.WriteStateAsync();
    }

    /// <summary>
    /// Marks all source shards as deleted. Called once when the
    /// <see cref="SnapshotPhase.Lock"/> phase is processed (offline mode only).
    /// Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task LockSourceShardsAsync()
    {
        var shardCount = state.State.ShardCount;
        var tasks = new Task[shardCount];
        for (int i = 0; i < shardCount; i++)
        {
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{i}");
            tasks[i] = shard.MarkDeletedAsync();
        }
        await Task.WhenAll(tasks);

        state.State.Phase = SnapshotPhase.Copy;
        state.State.ShardRetries = 0;
        await state.WriteStateAsync();
    }

    public async Task RunSnapshotPassAsync()
    {
        if (!state.State.InProgress) return;

        if (state.State.Phase == SnapshotPhase.Lock)
        {
            await LockSourceShardsAsync();
        }

        while (state.State.NextShardIndex < state.State.ShardCount)
        {
            await ProcessCurrentPhaseAsync();
        }

        await CompleteSnapshotAsync();
    }

    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName == KeepaliveReminderName)
        {
            if (state.State.InProgress && _snapshotTimer is null)
            {
                await StartSnapshotTimerAsync();
            }
            else if (!state.State.InProgress)
            {
                await UnregisterKeepaliveAsync();
                this.DeactivateOnIdle();
            }
        }
    }

    private async Task StartSnapshotAsync()
    {
        await reminderRegistry.RegisterOrUpdateReminder(
            callingGrainId: context.GrainId,
            reminderName: KeepaliveReminderName,
            dueTime: TimeSpan.FromMinutes(1),
            period: TimeSpan.FromMinutes(1));

        await StartSnapshotTimerAsync();
    }

    private Task StartSnapshotTimerAsync()
    {
        _snapshotTimer = this.RegisterGrainTimer(
            OnSnapshotTimerTick,
            new GrainTimerCreationOptions(dueTime: TimeSpan.Zero, period: TimeSpan.FromSeconds(2)));
        return Task.CompletedTask;
    }

    /// <summary>
    /// Persists the in-progress marker and registers the keepalive reminder
    /// without starting the grain timer. Used by unit tests.
    /// </summary>
    internal async Task BeginSnapshotStateAsync(int startFromShard)
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

    private async Task OnSnapshotTimerTick(CancellationToken ct)
    {
        await ProcessNextPhaseAsync();
    }

    /// <summary>
    /// Processes the next phase of the current shard. If all shards are done,
    /// completes the snapshot. Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task ProcessNextPhaseAsync()
    {
        if (state.State.Phase == SnapshotPhase.Lock)
        {
            await LockSourceShardsAsync();
            return;
        }

        if (state.State.NextShardIndex >= state.State.ShardCount)
        {
            await CompleteSnapshotAsync();
            return;
        }

        await ProcessCurrentPhaseAsync();
    }

    private async Task ProcessCurrentPhaseAsync()
    {
        var shardIndex = state.State.NextShardIndex;

        try
        {
            switch (state.State.Phase)
            {
                case SnapshotPhase.Copy:
                    await CopyShardAsync(shardIndex);

                    if (state.State.Mode == SnapshotMode.Offline)
                    {
                        state.State.Phase = SnapshotPhase.Unmark;
                    }
                    else
                    {
                        // Online mode: skip unmark, advance to next shard.
                        state.State.NextShardIndex++;
                        state.State.Phase = SnapshotPhase.Copy;
                    }
                    state.State.ShardRetries = 0;
                    await state.WriteStateAsync();
                    break;

                case SnapshotPhase.Unmark:
                    await UnmarkSourceShardAsync(shardIndex);
                    state.State.NextShardIndex++;
                    state.State.Phase = SnapshotPhase.Copy;
                    state.State.ShardRetries = 0;
                    await state.WriteStateAsync();
                    break;
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Snapshot phase {Phase} failed for shard {ShardIndex} of tree {TreeId}",
                state.State.Phase, shardIndex, SourceTreeId);

            if (state.State.ShardRetries < MaxRetriesPerPhase)
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
    /// Drains live entries from the source shard's leaf chain and bulk-loads
    /// them into the destination shard.
    /// </summary>
    private async Task CopyShardAsync(int shardIndex)
    {
        var sourceShardKey = $"{SourceTreeId}/{shardIndex}";
        var sourceShard = grainFactory.GetGrain<IShardRootGrain>(sourceShardKey);
        var leafId = await sourceShard.GetLeftmostLeafIdAsync();

        var entries = new List<KeyValuePair<string, byte[]>>();
        while (leafId is not null)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
            var liveEntries = await leaf.GetLiveEntriesAsync();
            entries.AddRange(liveEntries);
            leafId = await leaf.GetNextSiblingAsync();
        }

        if (entries.Count == 0) return;

        // Sort by key for bulk load.
        entries.Sort((a, b) => string.Compare(a.Key, b.Key, StringComparison.Ordinal));

        var destShardKey = $"{state.State.DestinationTreeId}/{shardIndex}";
        var destShard = grainFactory.GetGrain<IShardRootGrain>(destShardKey);
        var operationId = $"{state.State.OperationId}-snapshot-{shardIndex}";
        await destShard.BulkLoadAsync(operationId, entries);
    }

    private async Task UnmarkSourceShardAsync(int shardIndex)
    {
        var shardKey = $"{SourceTreeId}/{shardIndex}";
        var shard = grainFactory.GetGrain<IShardRootGrain>(shardKey);
        await shard.UnmarkDeletedAsync();
    }

    internal async Task CompleteSnapshotAsync()
    {
        _snapshotTimer?.Dispose();
        _snapshotTimer = null;

        state.State.InProgress = false;
        state.State.Complete = true;
        state.State.NextShardIndex = 0;
        state.State.ShardRetries = 0;
        state.State.Phase = SnapshotPhase.Lock;
        await state.WriteStateAsync();

        await UnregisterKeepaliveAsync();

        // Ensure tombstone compaction is active on the destination tree.
        var destCompaction = grainFactory.GetGrain<ITombstoneCompactionGrain>(state.State.DestinationTreeId!);
        await destCompaction.EnsureReminderAsync();

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
            logger.LogWarning(ex, "Failed to unregister snapshot keepalive reminder for tree {TreeId}", SourceTreeId);
        }
    }

    /// <inheritdoc />
    public Task<bool> IsCompleteAsync() =>
        Task.FromResult(!state.State.InProgress);
}
