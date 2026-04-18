using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Coordinator that drives an online adaptive shard split (F-011) end-to-end.
/// <para>
/// Phase machine:
/// </para>
/// <list type="number">
/// <item><description><see cref="ShardSplitPhase.BeginShadowWrite"/> — persist
/// intent and call <see cref="IShardRootGrain.BeginSplitAsync"/> on the source
/// so that subsequent live writes to moved virtual slots are mirrored to the
/// target.</description></item>
/// <item><description><see cref="ShardSplitPhase.Drain"/> — walk the source
/// shard's leaf chain and merge all entries (including tombstones) for moved
/// virtual slots into the target via <see cref="IShardRootGrain.MergeManyAsync"/>,
/// preserving original HLC timestamps.</description></item>
/// <item><description><see cref="ShardSplitPhase.Swap"/> — atomically update
/// the persisted <see cref="ShardMap"/> so that moved virtual slots route to
/// the new target shard.</description></item>
/// <item><description><see cref="ShardSplitPhase.Reject"/> — flip the source
/// into reject mode so any stale <c>LatticeGrain</c> activations still
/// targeting the source for moved-slot keys receive
/// <see cref="StaleShardRoutingException"/> and refresh.</description></item>
/// <item><description><see cref="ShardSplitPhase.Complete"/> — final drain
/// pass to capture any post-shadow tombstones, clear the source's
/// <c>SplitInProgress</c> state, and deactivate.</description></item>
/// </list>
/// Key format: <c>{treeId}</c>.
/// </summary>
internal sealed class TreeShardSplitGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ILogger<TreeShardSplitGrain> logger,
    [PersistentState("tree-shard-split", LatticeOptions.StorageProviderName)]
    IPersistentState<TreeShardSplitState> state) : ITreeShardSplitGrain, IRemindable, IGrainBase
{
    private const string KeepaliveReminderName = "shard-split-keepalive";

    private string TreeId => context.GrainId.Key.ToString()!;
    private LatticeOptions Options => optionsMonitor.Get(TreeId);
    IGrainContext IGrainBase.GrainContext => context;

    private IGrainTimer? _splitTimer;

    /// <inheritdoc />
    public async Task SplitAsync(int sourceShardIndex)
    {
        if (sourceShardIndex < 0)
            throw new ArgumentOutOfRangeException(nameof(sourceShardIndex), "Must be non-negative.");

        if (state.State.InProgress)
        {
            if (state.State.SourceShardIndex == sourceShardIndex) return;
            throw new InvalidOperationException(
                $"A shard split is already in progress for tree '{TreeId}' (source={state.State.SourceShardIndex}).");
        }

        if (state.State.Complete) state.State.Complete = false;

        await InitiateSplitStateAsync(sourceShardIndex);
        await StartSplitAsync();
    }

    /// <summary>
    /// Persists the split intent and invokes <see cref="IShardRootGrain.BeginSplitAsync"/>
    /// on the source shard so that shadow-writes start immediately.
    /// Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task InitiateSplitStateAsync(int sourceShardIndex)
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var options = Options;

        var currentMap = await registry.GetShardMapAsync(TreeId)
            ?? ShardMap.CreateDefault(options.VirtualShardCount, options.ShardCount);

        // Find virtual slots currently owned by the source shard.
        var ownedSlots = new List<int>();
        for (int i = 0; i < currentMap.Slots.Length; i++)
            if (currentMap.Slots[i] == sourceShardIndex) ownedSlots.Add(i);

        if (ownedSlots.Count < 2)
            throw new InvalidOperationException(
                $"Shard {sourceShardIndex} cannot be split because it owns fewer than 2 virtual slots.");

        // Move the upper half to a new physical shard. The new shard index is
        // (max existing physical index + 1) so that no existing slot is disturbed.
        var maxExisting = -1;
        foreach (var idx in currentMap.Slots) if (idx > maxExisting) maxExisting = idx;
        var targetShardIndex = maxExisting + 1;

        var splitPoint = ownedSlots.Count / 2;
        var movedSlots = new int[ownedSlots.Count - splitPoint];
        for (int i = 0; i < movedSlots.Length; i++)
            movedSlots[i] = ownedSlots[splitPoint + i];
        Array.Sort(movedSlots);

        state.State.InProgress = true;
        state.State.Complete = false;
        state.State.OperationId = Guid.NewGuid().ToString("N");
        state.State.Phase = ShardSplitPhase.BeginShadowWrite;
        state.State.SourceShardIndex = sourceShardIndex;
        state.State.TargetShardIndex = targetShardIndex;
        state.State.MovedSlots = new List<int>(movedSlots);
        state.State.OriginalShardMap = currentMap;
        await state.WriteStateAsync();

        // Kick off shadow-writing on the source shard.
        var physicalTreeId = await registry.ResolveAsync(TreeId);
        var source = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{sourceShardIndex}");
        await source.BeginSplitAsync(targetShardIndex, movedSlots, currentMap.Slots.Length);

        state.State.Phase = ShardSplitPhase.Drain;
        await state.WriteStateAsync();
    }

    /// <inheritdoc />
    public async Task RunSplitPassAsync()
    {
        if (!state.State.InProgress) return;

        // Phase order: Drain → Swap → Reject → Complete.
        if (state.State.Phase == ShardSplitPhase.BeginShadowWrite)
        {
            // Re-issue the shadow-write begin in case of a crash between persist
            // and the source-shard call. Idempotent on the source side.
            var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            var physicalTreeId = await registry.ResolveAsync(TreeId);
            var source = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{state.State.SourceShardIndex}");
            await source.BeginSplitAsync(
                state.State.TargetShardIndex,
                state.State.MovedSlots.ToArray(),
                state.State.OriginalShardMap!.Slots.Length);

            state.State.Phase = ShardSplitPhase.Drain;
            await state.WriteStateAsync();
        }

        if (state.State.Phase == ShardSplitPhase.Drain)
            await DrainAsync();

        if (state.State.Phase == ShardSplitPhase.Swap)
            await SwapAsync();

        if (state.State.Phase == ShardSplitPhase.Reject)
            await EnterRejectAsync();

        if (state.State.Phase == ShardSplitPhase.Complete)
            await FinaliseAsync();
    }

    /// <inheritdoc />
    public Task<bool> IsCompleteAsync() => Task.FromResult(!state.State.InProgress);

    /// <inheritdoc />
    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (reminderName != KeepaliveReminderName) return;

        if (state.State.InProgress && _splitTimer is null)
        {
            await StartSplitTimerAsync();
        }
        else if (!state.State.InProgress)
        {
            await UnregisterKeepaliveAsync();
            this.DeactivateOnIdle();
        }
    }

    private async Task StartSplitAsync()
    {
        await reminderRegistry.RegisterOrUpdateReminder(
            callingGrainId: context.GrainId,
            reminderName: KeepaliveReminderName,
            dueTime: TimeSpan.FromMinutes(1),
            period: TimeSpan.FromMinutes(1));

        await StartSplitTimerAsync();
    }

    private Task StartSplitTimerAsync()
    {
        _splitTimer = this.RegisterGrainTimer(
            OnSplitTimerTick,
            new GrainTimerCreationOptions(dueTime: TimeSpan.Zero, period: TimeSpan.FromSeconds(2)));
        return Task.CompletedTask;
    }

    private async Task OnSplitTimerTick(CancellationToken ct)
    {
        await ProcessNextPhaseAsync();
    }

    /// <summary>
    /// Processes a single phase of the split. Exposed as <c>internal</c> for
    /// unit testing.
    /// </summary>
    internal async Task ProcessNextPhaseAsync()
    {
        if (!state.State.InProgress) return;

        try
        {
            switch (state.State.Phase)
            {
                case ShardSplitPhase.BeginShadowWrite:
                    await RunSplitPassAsync();
                    break;
                case ShardSplitPhase.Drain:
                    await DrainAsync();
                    break;
                case ShardSplitPhase.Swap:
                    await SwapAsync();
                    break;
                case ShardSplitPhase.Reject:
                    await EnterRejectAsync();
                    break;
                case ShardSplitPhase.Complete:
                    await FinaliseAsync();
                    break;
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Shard-split phase {Phase} failed for tree {TreeId}",
                state.State.Phase, TreeId);
        }
    }

    /// <summary>
    /// Drains all moved-slot entries from the source shard's leaf chain to the
    /// target shard, preserving HLC timestamps via
    /// <see cref="IShardRootGrain.MergeManyAsync"/>. Idempotent: re-running
    /// after a crash converges via CRDT LWW. Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task DrainAsync()
    {
        await ForwardMovedSlotEntriesAsync();
        state.State.Phase = ShardSplitPhase.Swap;
        await state.WriteStateAsync();
    }

    /// <summary>
    /// Updates the persisted <see cref="ShardMap"/> so that moved virtual slots
    /// route to the target physical shard. Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task SwapAsync()
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var newSlots = (int[])state.State.OriginalShardMap!.Slots.Clone();
        foreach (var slot in state.State.MovedSlots)
            newSlots[slot] = state.State.TargetShardIndex;
        await registry.SetShardMapAsync(TreeId, new ShardMap { Slots = newSlots });

        state.State.Phase = ShardSplitPhase.Reject;
        await state.WriteStateAsync();
    }

    /// <summary>
    /// Transitions the source shard to reject moved-slot operations so stale
    /// <c>LatticeGrain</c> activations refresh their cached
    /// <see cref="ShardMap"/>. Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task EnterRejectAsync()
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var physicalTreeId = await registry.ResolveAsync(TreeId);
        var source = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{state.State.SourceShardIndex}");
        await source.EnterRejectPhaseAsync();

        state.State.Phase = ShardSplitPhase.Complete;
        await state.WriteStateAsync();
    }

    /// <summary>
    /// Final drain pass to forward any tombstones written during the shadow
    /// phase that were not mirrored on the hot path, then clears the source
    /// shard's <c>SplitInProgress</c> state. Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task FinaliseAsync()
    {
        // Final drain captures any deletes that occurred between drain and reject.
        await ForwardMovedSlotEntriesAsync();

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var physicalTreeId = await registry.ResolveAsync(TreeId);
        var source = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{state.State.SourceShardIndex}");
        await source.CompleteSplitAsync();

        await CompleteSplitInternalAsync();
    }

    private async Task CompleteSplitInternalAsync()
    {
        _splitTimer?.Dispose();
        _splitTimer = null;

        state.State.InProgress = false;
        state.State.Complete = true;
        state.State.Phase = ShardSplitPhase.None;
        await state.WriteStateAsync();

        await UnregisterKeepaliveAsync();
        this.DeactivateOnIdle();
    }

    /// <summary>
    /// Walks the source shard's leaf chain and merges every entry whose key
    /// hashes to a moved virtual slot into the target shard, preserving the
    /// original HLC timestamp. Tombstones are forwarded the same way (their
    /// <see cref="LwwValue{T}.IsTombstone"/> flag is preserved through
    /// <see cref="IShardRootGrain.MergeManyAsync"/>). Idempotent under retry.
    /// </summary>
    private async Task ForwardMovedSlotEntriesAsync()
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var physicalTreeId = await registry.ResolveAsync(TreeId);

        var source = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{state.State.SourceShardIndex}");
        var leafId = await source.GetLeftmostLeafIdAsync();
        if (leafId is null) return;

        var movedSlotsArray = state.State.MovedSlots.ToArray();
        Array.Sort(movedSlotsArray);
        var virtualShardCount = state.State.OriginalShardMap!.Slots.Length;

        var batch = new Dictionary<string, LwwValue<byte[]>>();
        var emptyVector = new VersionVector();

        while (leafId is not null)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
            var delta = await leaf.GetDeltaSinceAsync(emptyVector);
            foreach (var (key, lww) in delta.Entries)
            {
                var slot = ShardMap.GetVirtualSlot(key, virtualShardCount);
                if (Array.BinarySearch(movedSlotsArray, slot) < 0) continue;
                batch[key] = lww;
            }
            leafId = await leaf.GetNextSiblingAsync();
        }

        if (batch.Count == 0) return;

        var target = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{state.State.TargetShardIndex}");
        await target.MergeManyAsync(batch);
    }

    private async Task UnregisterKeepaliveAsync()
    {
        try
        {
            var reminder = await reminderRegistry.GetReminder(context.GrainId, KeepaliveReminderName);
            if (reminder is not null)
                await reminderRegistry.UnregisterReminder(context.GrainId, reminder);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to unregister shard-split keepalive reminder for tree {TreeId}", TreeId);
        }
    }
}
