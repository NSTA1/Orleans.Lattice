using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Coordinator that drives an online adaptive shard split end-to-end.
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
    LatticeOptionsResolver optionsResolver,
    ILogger<TreeShardSplitGrain> logger,
    [PersistentState("tree-shard-split", LatticeOptions.StorageProviderName)]
    IPersistentState<TreeShardSplitState> state)
    : CoordinatorGrain<TreeShardSplitGrain>(context, reminderRegistry, logger), ITreeShardSplitGrain
{
    /// <inheritdoc />
    protected override string KeepaliveReminderName => "shard-split-keepalive";

    /// <inheritdoc />
    protected override bool InProgress => state.State.InProgress;

    /// <inheritdoc />
    protected override string LogContext => $"tree {TreeId}";

    /// <summary>
    /// Parses the grain key as <c>{treeId}/{sourceShardIndex}</c>. The trailing
    /// integer suffix is the source shard; everything before the final '/' is
    /// the tree ID. A key without a '/' is treated as a tree-level coordinator
    /// (legacy behaviour) — <see cref="SourceShardIndexFromKey"/> returns
    /// <c>-1</c> in that case.
    /// </summary>
    private string TreeId
    {
        get
        {
            var key = Context.GrainId.Key.ToString()!;
            var slash = key.LastIndexOf('/');
            return slash < 0 ? key : key[..slash];
        }
    }

    /// <summary>
    /// The source shard index encoded in the grain key, or <c>-1</c> for
    /// keys without a slash separator. When non-negative,
    /// <see cref="SplitAsync"/> validates that the caller-supplied source
    /// shard matches this value.
    /// </summary>
    private int SourceShardIndexFromKey
    {
        get
        {
            var key = Context.GrainId.Key.ToString()!;
            var slash = key.LastIndexOf('/');
            if (slash < 0 || slash == key.Length - 1) return -1;
            return int.TryParse(key.AsSpan(slash + 1), out var idx) ? idx : -1;
        }
    }

    private LatticeOptions Options => optionsMonitor.Get(TreeId);

    private string? _physicalTreeId;

    private async Task<string> GetPhysicalTreeIdAsync()
    {
        if (_physicalTreeId is not null) return _physicalTreeId;
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        _physicalTreeId = await registry.ResolveAsync(TreeId);
        return _physicalTreeId;
    }

    /// <inheritdoc />
    public async Task SplitAsync(int sourceShardIndex)
    {
        if (sourceShardIndex < 0)
            throw new ArgumentOutOfRangeException(nameof(sourceShardIndex), "Must be non-negative.");

        var keyShard = SourceShardIndexFromKey;
        if (keyShard >= 0 && keyShard != sourceShardIndex)
            throw new ArgumentException(
                $"Source shard {sourceShardIndex} does not match coordinator key shard {keyShard} (key='{Context.GrainId.Key}').",
                nameof(sourceShardIndex));

        if (state.State.InProgress)
        {
            if (state.State.SourceShardIndex == sourceShardIndex) return;
            throw new InvalidOperationException(
                $"A shard split is already in progress for tree '{TreeId}' (source={state.State.SourceShardIndex}).");
        }

        if (state.State.Complete) state.State.Complete = false;

        await InitiateSplitStateAsync(sourceShardIndex);
        await StartCoordinatorAsync();
    }

    /// <summary>
    /// Persists the split intent and invokes <see cref="IShardRootGrain.BeginSplitAsync"/>
    /// on the source shard so that shadow-writes start immediately.
    /// Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task InitiateSplitStateAsync(int sourceShardIndex)
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var resolved = await optionsResolver.ResolveAsync(TreeId);

        var currentMap = await registry.GetShardMapAsync(TreeId)
            ?? ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, resolved.ShardCount);

        // Find virtual slots currently owned by the source shard.
        var ownedSlots = new List<int>();
        for (int i = 0; i < currentMap.Slots.Length; i++)
            if (currentMap.Slots[i] == sourceShardIndex) ownedSlots.Add(i);

        if (ownedSlots.Count < 2)
            throw new InvalidOperationException(
                $"Shard {sourceShardIndex} cannot be split because it owns fewer than 2 virtual slots.");

        // Atomically allocate a fresh target physical shard index via the
        // registry — the registry's non-reentrant scheduling guarantees that
        // concurrent split coordinators each receive a distinct index even
        // when the persisted shard map is the same.
        var maxExisting = -1;
        foreach (var idx in currentMap.Slots) if (idx > maxExisting) maxExisting = idx;
        var targetShardIndex = await registry.AllocateNextShardIndexAsync(TreeId, maxExisting);

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
        var physicalTreeId = await GetPhysicalTreeIdAsync();
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
            var physicalTreeId = await GetPhysicalTreeIdAsync();
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
    public Task<bool> IsIdleAsync() => Task.FromResult(!state.State.InProgress);

    /// <summary>
    /// Processes a single phase of the split. Exposed as <c>internal</c> via
    /// <c>protected</c> override for unit testing.
    /// </summary>
    protected internal override async Task ProcessNextPhaseAsync()
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
            Logger.LogWarning(ex, "Shard-split phase {Phase} failed for tree {TreeId}",
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
        // Re-read the current map so concurrent splits compose correctly:
        // each swap applies its own moved-slot diff onto whatever is now
        // persisted, preventing one coordinator from clobbering another's
        // earlier swap. The registry grain is non-reentrant so the
        // get-modify-set sequence is atomic across callers.
        var currentMap = await registry.GetShardMapAsync(TreeId)
            ?? state.State.OriginalShardMap!;
        var newSlots = (int[])currentMap.Slots.Clone();
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
        var physicalTreeId = await GetPhysicalTreeIdAsync();
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

        var physicalTreeId = await GetPhysicalTreeIdAsync();
        var source = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{state.State.SourceShardIndex}");
        await source.CompleteSplitAsync();

        state.State.InProgress = false;
        state.State.Complete = true;
        state.State.Phase = ShardSplitPhase.None;
        await state.WriteStateAsync();

        // Fire-and-forget notification to the diagnostics ring buffer; failures
        // are swallowed so the commit path never waits on diagnostics plumbing.
        NotifyDiagnosticsOfSplit(state.State.SourceShardIndex);

        LatticeMetrics.ShardSplitsCommitted.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagShard, state.State.SourceShardIndex));

        await PublishSplitCommittedAsync(state.State.SourceShardIndex);

        await CompleteCoordinatorAsync();
    }

    private async Task PublishSplitCommittedAsync(int shardIndex)
    {
        var opts = optionsMonitor.Get(TreeId);
        if (!await _eventsGate.IsEnabledAsync(grainFactory, TreeId, opts)) return;
        var evt = LatticeEventPublisher.CreateEvent(LatticeTreeEventKind.SplitCommitted, TreeId, key: null, shardIndex: shardIndex);
        await LatticeEventPublisher.PublishAsync(Context.ActivationServices, opts, evt, Logger);
    }

    private readonly PublishEventsGate _eventsGate = new();

    private void NotifyDiagnosticsOfSplit(int shardIndex)
    {
        try
        {
            var stats = grainFactory.GetGrain<ILatticeStats>(TreeId);
            var log = Logger;
            _ = stats.RecordSplitAsync(shardIndex, DateTime.UtcNow)
                .ContinueWith(
                    t => log.LogDebug(t.Exception, "Diagnostics split notification faulted; ignoring."),
                    TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously);
        }
        catch
        {
            // Never let diagnostics plumbing affect split completion.
        }
    }

    /// <summary>
    /// Walks the source shard's leaf chain and merges every entry whose key
    /// hashes to a moved virtual slot into the target shard, preserving the
    /// original HLC timestamp. Tombstones are forwarded the same way (their
    /// <see cref="LwwValue{T}.IsTombstone"/> flag is preserved through
    /// <see cref="IShardRootGrain.MergeManyAsync"/>). Idempotent under retry.
    /// <para>
    /// Memory and message size are bounded by
    /// <see cref="LatticeOptions.SplitDrainBatchSize"/>: entries are flushed
    /// to the target whenever the in-flight batch reaches that size, and
    /// each leaf is asked only for moved-slot entries via
    /// <see cref="IBPlusLeafGrain.GetDeltaSinceForSlotsAsync"/> so unrelated
    /// data is never serialised on the wire.
    /// </para>
    /// </summary>
    private async Task ForwardMovedSlotEntriesAsync()
    {
        var physicalTreeId = await GetPhysicalTreeIdAsync();

        var source = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{state.State.SourceShardIndex}");
        var leafId = await source.GetLeftmostLeafIdAsync();
        if (leafId is null) return;

        var movedSlotsArray = state.State.MovedSlots.ToArray();
        Array.Sort(movedSlotsArray);
        var virtualShardCount = state.State.OriginalShardMap!.Slots.Length;
        var batchSize = Options.SplitDrainBatchSize;
        if (batchSize <= 0) batchSize = LatticeOptions.DefaultSplitDrainBatchSize;

        var target = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{state.State.TargetShardIndex}");
        var batch = new Dictionary<string, LwwValue<byte[]>>(batchSize);
        var emptyVector = new VersionVector();

        while (leafId is not null)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
            // Slot filtering is pushed into the leaf so only moved-slot
            // entries are serialised on the response — saves bandwidth and
            // coordinator-side allocations on hot shards where moved slots
            // are a minority of the keyspace.
            var delta = await leaf.GetDeltaSinceForSlotsAsync(emptyVector, movedSlotsArray, virtualShardCount);
            foreach (var (key, lww) in delta.Entries)
            {
                batch[key] = lww;
                if (batch.Count >= batchSize)
                {
                    await target.MergeManyAsync(batch);
                    batch.Clear();
                }
            }
            leafId = await leaf.GetNextSiblingAsync();
        }

        if (batch.Count > 0)
            await target.MergeManyAsync(batch);
    }
}
