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
/// <item><description><see cref="Orleans.Lattice.BPlusTree.State.ShardSplitPhase.BeginShadowWrite"/> - persist
/// intent and call <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.BeginSplitAsync"/> on the source
/// so that subsequent live writes to moved virtual slots are mirrored to the
/// target.</description></item>
/// <item><description><see cref="Orleans.Lattice.BPlusTree.State.ShardSplitPhase.Drain"/> - walk the source
/// shard's leaf chain and merge all entries (including tombstones) for moved
/// virtual slots into the target via <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.MergeManyAsync"/>,
/// preserving original HLC timestamps.</description></item>
/// <item><description><see cref="ShardSplitPhase.Swap"/> - atomically update
/// the persisted <see cref="ShardMap"/> so that moved virtual slots route to
/// the new target shard.</description></item>
/// <item><description><see cref="Orleans.Lattice.BPlusTree.State.ShardSplitPhase.Reject"/> - flip the source
/// into reject mode so any stale <c>LatticeGrain</c> activations still
/// targeting the source for moved-slot keys receive
/// <see cref="StaleShardRoutingException"/> and refresh.</description></item>
/// <item><description><see cref="ShardSplitPhase.Complete"/> - final drain
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
    /// (legacy behaviour) - <see cref="SourceShardIndexFromKey"/> returns
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

        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            Context.ActivationServices, TreeId, LatticeOperation.Admin);

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
    /// Persists the split intent and invokes <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.BeginSplitAsync"/>
    /// on the source shard so that shadow-writes start immediately.
    /// Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task InitiateSplitStateAsync(int sourceShardIndex)
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var resolved = await optionsResolver.ResolveAsync(TreeId);

        var currentMap = await registry.GetShardMapAsync(TreeId)
            ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, resolved.ShardCount);

        // Find virtual slots currently owned by the source shard.
        var ownedSlots = new List<int>();
        for (int i = 0; i < currentMap.Slots.Length; i++)
            if (currentMap.Slots[i] == sourceShardIndex) ownedSlots.Add(i);

        if (ownedSlots.Count < 2)
            throw new InvalidOperationException(
                $"Shard {sourceShardIndex} cannot be split because it owns fewer than 2 virtual slots.");

        // Atomically allocate a fresh target physical shard index via the
        // registry - the registry's non-reentrant scheduling guarantees that
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

        // Snapshot prior in-memory state before mutating so a failing
        // WriteStateAsync can be unwound. Without this, the in-memory
        // dictionary records the split intent while disk does not, and
        // SplitAsync's `if (state.State.InProgress)` guard short-circuits
        // every retry from the same activation.
        var prevInProgress = state.State.InProgress;
        var prevComplete = state.State.Complete;
        var prevOperationId = state.State.OperationId;
        var prevPhase = state.State.Phase;
        var prevSourceShardIndex = state.State.SourceShardIndex;
        var prevTargetShardIndex = state.State.TargetShardIndex;
        var prevMovedSlots = state.State.MovedSlots;
        var prevOriginalShardMap = state.State.OriginalShardMap;

        state.State.InProgress = true;
        state.State.Complete = false;
        state.State.OperationId = Guid.NewGuid().ToString("N");
        state.State.Phase = ShardSplitPhase.BeginShadowWrite;
        state.State.SourceShardIndex = sourceShardIndex;
        state.State.TargetShardIndex = targetShardIndex;
        state.State.MovedSlots = new List<int>(movedSlots);
        state.State.OriginalShardMap = currentMap;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.InProgress = prevInProgress;
            state.State.Complete = prevComplete;
            state.State.OperationId = prevOperationId;
            state.State.Phase = prevPhase;
            state.State.SourceShardIndex = prevSourceShardIndex;
            state.State.TargetShardIndex = prevTargetShardIndex;
            state.State.MovedSlots = prevMovedSlots;
            state.State.OriginalShardMap = prevOriginalShardMap;
            throw;
        }

        // Kick off shadow-writing on the source shard.
        var physicalTreeId = await GetPhysicalTreeIdAsync();
        var source = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{sourceShardIndex}");
        await source.BeginSplitAsync(targetShardIndex, movedSlots, currentMap.Slots.Length);

        // Retroactive shadow-forward of in-flight prepared
        // mutations. The shadow-forward window opened by BeginSplitAsync
        // mirrors new writes from this point on, but prepares that
        // landed on the source BEFORE the window opened were never
        // replicated to the destination's pending-tx bucket. The sweep
        // walks the source leaf chain and re-issues each pending
        // mutation through the destination's standard write path so
        // both shards converge on identical pending-tx state before
        // the drain phase begins. LWW idempotence makes the sweep
        // safe under retry on crash recovery.
        await RetroactiveSweepPreparedMutationsAsync(
            physicalTreeId,
            sourceShardIndex,
            targetShardIndex,
            movedSlots,
            currentMap.Slots.Length);

        var prevPhaseAtDrainAdvance = state.State.Phase;
        state.State.Phase = ShardSplitPhase.Drain;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Phase = prevPhaseAtDrainAdvance;
            throw;
        }
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
            var movedSlots = state.State.MovedSlots.ToArray();
            var virtualShardCount = state.State.OriginalShardMap!.Slots.Length;
            await source.BeginSplitAsync(
                state.State.TargetShardIndex,
                movedSlots,
                virtualShardCount);

            // Re-run the retroactive sweep on crash recovery.
            // LWW per (txid, key) on the destination's pending bucket
            // makes the re-run idempotent - a second snapshot for an
            // already-bucketed key merges via
            // LwwValue<byte[]>.Merge, and an identical Timestamp +
            // value produces a fixed point.
            await RetroactiveSweepPreparedMutationsAsync(
                physicalTreeId,
                state.State.SourceShardIndex,
                state.State.TargetShardIndex,
                movedSlots,
                virtualShardCount);

            var prevPhase = state.State.Phase;
            state.State.Phase = ShardSplitPhase.Drain;
            try
            {
                await state.WriteStateAsync();
            }
            catch
            {
                state.State.Phase = prevPhase;
                throw;
            }
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
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.MergeManyAsync"/>. Idempotent: re-running
    /// after a crash converges via CRDT LWW. Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task DrainAsync()
    {
        await ForwardMovedSlotEntriesAsync();
        var prevPhase = state.State.Phase;
        state.State.Phase = ShardSplitPhase.Swap;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Phase = prevPhase;
            throw;
        }
    }

    /// <summary>
    /// Updates the persisted <see cref="ShardMap"/> so that moved virtual slots
    /// route to the target physical shard. Exposed as <c>internal</c> for unit testing.
    /// <para>
    /// <b>Ordering invariant.</b> The source shard root MUST enter
    /// <see cref="Orleans.Lattice.BPlusTree.State.ShardSplitPhase.Reject"/> via
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.EnterRejectPhaseAsync"/> BEFORE the registry's
    /// shard map flips. The reverse order opens a multi-RPC window in which the
    /// registry already routes moved slots to the destination but the source's
    /// hot-path reject gate (<c>ThrowIfRejectedForKey</c>) does not yet fire
    /// (<see cref="Orleans.Lattice.BPlusTree.State.ShardRootState.SplitInProgress"/>.Phase is still pre-Reject
    /// and <see cref="Orleans.Lattice.BPlusTree.State.ShardRootState.MovedAwaySlots"/> is empty until
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.CompleteSplitAsync"/> runs). A reader whose
    /// <see cref="Orleans.Lattice.BPlusTree.Grains.LatticeGrain"/> activation holds a stale routing cache then
    /// routes the moved-slot key to the source, the source serves the read
    /// (no <see cref="StaleShardRoutingException"/>), and the reader surfaces
    /// the pre-saga <c>Entries</c> value while every other key on a non-stale
    /// routing path shows the post-saga value - the exact
    /// <c>round=N: split (pre=1, post=15)</c> shape the reshard chaos fixture
    /// catches. Entering reject first forces stale-routing readers onto the
    /// retry path; their refresh either picks up the post-flip map (route to
    /// destination, succeed) or spins briefly against the still-pre-flip map
    /// until the immediately-following <see cref="ILatticeRegistry.SetShardMapAsync"/>
    /// commits. The downstream <see cref="EnterRejectAsync"/> coordinator
    /// phase remains a no-op because
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.EnterRejectPhaseAsync"/> is idempotent
    /// (returns immediately when the source is already in Reject).
    /// </para>
    /// <para>
    /// <b>Final-drain invariant.</b> After the source enters Reject (which
    /// freezes moved-slot writes) and BEFORE the registry map flips, a final
    /// <see cref="ForwardMovedSlotEntriesAsync"/> pass re-synchronises the
    /// destination with the source's now-frozen committed state. Without it,
    /// a moved-slot write that committed on the source in the window between
    /// the Drain-phase scan and the source entering Reject - whose best-effort
    /// shadow-forward lagged or missed the destination - would leave the
    /// destination serving the drained pre-saga value (<c>IsMigrated=true</c>,
    /// no shadow marker) for that key after the map flips, producing the
    /// non-atomic mixed-round batch the reshard chaos fixture catches. The
    /// final drain is LWW-idempotent, so a crash-recovery re-entry into
    /// <see cref="SwapAsync"/> re-drains harmlessly.
    /// </para>
    /// </summary>
    internal async Task SwapAsync()
    {
        var physicalTreeId = await GetPhysicalTreeIdAsync();
        var source = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{state.State.SourceShardIndex}");

        // Mark every source leaf with the moved-slot set BEFORE the
        // source enters Reject phase, so no read crosses the Swap
        // boundary observing an unmarked leaf under a Reject-phase
        // shard. The leaf-side moved-away gate then hides stale
        // source-side snapshots from every read entrypoint, including
        // the LeafCacheGrain pending-key delegation path that bypasses
        // the shard front door. Idempotent under crash recovery:
        // MarkSlotsMovedAwayAsync is a no-op when the slot set is
        // already recorded under the same virtual shard count.
        var movedSlotsForMark = state.State.MovedSlots.ToArray();
        var vscForMark = state.State.OriginalShardMap!.Slots.Length;
        await source.MarkLeavesMovedAwayAsync(movedSlotsForMark, vscForMark);

        // Source enters reject mode AFTER the leaves are marked. See
        // the ordering invariant in the method summary.
        // EnterRejectPhaseAsync is idempotent under crash recovery: a
        // coordinator re-entry into SwapAsync after a crash between
        // this call and the registry flip below finds the source
        // already in Reject and the call returns immediately.
        await source.EnterRejectPhaseAsync();

        // Final authoritative drain BEFORE the registry map flips.
        //
        // The Drain phase already forwarded every moved-slot entry the
        // source held at that time, but the source kept accepting and
        // committing moved-slot writes through the Swap phase (the
        // write gate only rejects at Reject - see ThrowIfRejectedForKey).
        // Those interim commits are mirrored to the destination by the
        // shadow-forward pipeline, but that mirror is best-effort under
        // LWW and can lag or miss a commit that lands in the narrow
        // window between the Drain-phase scan and the source entering
        // Reject. If the map flips while the destination still holds the
        // drained pre-saga value for such a key, a reader that routes to
        // the destination surfaces a stale historical value (IsMigrated
        // =true, no shadow marker) for that key while every other key
        // shows the post-saga value - the non-atomic mixed-round batch
        // the reshard chaos fixture catches.
        //
        // EnterRejectPhaseAsync above has now frozen moved-slot writes on
        // the source, so the source's authoritative committed state for
        // the migrating slots can no longer change. Re-running the drain
        // here therefore provably synchronises the destination with the
        // source's final committed state before any reader can route to
        // the destination. The drain scans the source leaf chain directly
        // (bypassing the shard read gate) so the post-reject freeze does
        // not block it, and MergeManyAsync is LWW-idempotent so a crash-
        // recovery re-entry into SwapAsync re-drains harmlessly.
        await ForwardMovedSlotEntriesAsync();

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

        // The registry SetShardMapAsync side effect is cross-grain and
        // idempotent on re-apply; only the in-memory Phase mutation needs
        // to be reverted on a failing WriteStateAsync.
        var prevPhase = state.State.Phase;
        state.State.Phase = ShardSplitPhase.Reject;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Phase = prevPhase;
            throw;
        }
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

        // The source.EnterRejectPhaseAsync side effect is cross-grain and
        // idempotent on re-apply; only the in-memory Phase mutation needs
        // to be reverted on a failing WriteStateAsync.
        var prevPhase = state.State.Phase;
        state.State.Phase = ShardSplitPhase.Complete;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Phase = prevPhase;
            throw;
        }
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

        // Snapshot the terminal triple before mutating. Without the revert,
        // an in-memory InProgress=false causes RunSplitPassAsync's
        // `if (!state.State.InProgress) return;` guard to short-circuit any
        // retry from the same activation, while disk still has InProgress=true
        // and Phase=Complete - the activation thinks the split finished, the
        // persisted state says otherwise.
        var prevInProgress = state.State.InProgress;
        var prevComplete = state.State.Complete;
        var prevPhase = state.State.Phase;

        state.State.InProgress = false;
        state.State.Complete = true;
        state.State.Phase = ShardSplitPhase.None;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.InProgress = prevInProgress;
            state.State.Complete = prevComplete;
            state.State.Phase = prevPhase;
            throw;
        }

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
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}.IsTombstone"/> flag is preserved through
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.MergeManyAsync"/>). Idempotent under retry.
    /// <para>
    /// Memory and message size are bounded by
    /// <see cref="LatticeOptions.SplitDrainBatchSize"/>: entries are flushed
    /// to the target whenever the in-flight batch reaches that size, and
    /// each leaf is asked only for moved-slot entries via
    /// <see cref="Orleans.Lattice.BPlusTree.IBPlusLeafGrain.GetDeltaSinceForSlotsAsync"/> so unrelated
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
            // entries are serialised on the response - saves bandwidth and
            // coordinator-side allocations on hot shards where moved slots
            // are a minority of the keyspace.
            var delta = await leaf.GetDeltaSinceForSlotsAsync(emptyVector, movedSlotsArray, virtualShardCount);
            foreach (var (key, lww) in delta.Entries)
            {
                batch[key] = lww;
                if (batch.Count >= batchSize)
                {
                    await target.MergeManyAsync(batch, isCrossShardMigration: true);
                    batch.Clear();
                }
            }
            leafId = await leaf.GetNextSiblingAsync();
        }

        if (batch.Count > 0)
            await target.MergeManyAsync(batch, isCrossShardMigration: true);
    }

    /// <summary>
    /// Retroactive shadow-forward of in-flight prepared
    /// mutations at the entry of the <see cref="Orleans.Lattice.BPlusTree.State.ShardSplitPhase.BeginShadowWrite"/>
    /// phase. Walks the source shard's leaf chain, snapshots every
    /// prepared mutation whose key hashes into a migrating virtual
    /// slot, and replays each snapshot through the destination shard's
    /// standard write path so the destination leaf buckets the value
    /// into its own <c>_pendingTx[txid][key]</c> with the source-side
    /// <c>(Timestamp, OriginClusterId, VectorClock)</c> preserved
    /// verbatim. The saga's terminal mark then drains both source and
    /// destination buckets identically via the existing per-shard
    /// terminal broadcast and the saga's transitive split-forward
    /// fan-out
    /// (<see cref="TerminalFanOutResolver.ResolveTransitiveAsync"/>),
    /// which reaches the destination shard via the source's
    /// <see cref="Orleans.Lattice.BPlusTree.State.ShardRootState.SplitInProgress"/> /
    /// <see cref="Orleans.Lattice.BPlusTree.State.ShardRootState.MovedAwaySlots"/> records.
    /// <para>
    /// <b>Idempotence.</b> LWW per <c>(txid, key)</c> on the
    /// destination's pending bucket makes the sweep safe under retry:
    /// a re-replayed snapshot with the same <see cref="HybridLogicalClock"/>
    /// timestamp produces a fixed point in
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}.Merge(LwwValue{T}, LwwValue{T})"/>. A
    /// crash mid-sweep is recovered by the
    /// <see cref="Orleans.Lattice.BPlusTree.State.ShardSplitPhase.BeginShadowWrite"/> branch of
    /// <see cref="RunSplitPassAsync"/> which re-runs the entire sweep
    /// before transitioning to <see cref="Orleans.Lattice.BPlusTree.State.ShardSplitPhase.Drain"/>.
    /// </para>
    /// <para>
    /// <b>Cost.</b> Bounded by active-saga concurrency at split-begin
    /// × per-key replay cost. The chain walk is sequential (each step
    /// needs the previous leaf's next-sibling pointer); the per-key
    /// destination writes execute serially in source-leaf order so a
    /// large active-saga set does not unboundedly fan out into the
    /// Orleans scheduler. For typical workloads (sub-second saga
    /// turnaround on the saga acceptance benchmark) the sweep's
    /// active-saga floor is &lt;= ~10 mutations.
    /// </para>
    /// <para>
    /// <b>Orphan-window closure.</b> Each snapshot's replay races with
    /// the saga's own commit-phase terminal broadcast. The saga calls
    /// <see cref="Orleans.Lattice.BPlusTree.ITxRegistryGrain.GetParticipantsAsync"/> once at the
    /// top of the broadcast; if that query returns BEFORE the sweep's
    /// per-snapshot <c>SetAsync</c> registers the destination shard
    /// (via <c>RecordAffectedLeafIfPreparedAsync</c>), the saga's
    /// terminal fan-out goes only to source and the prepared entry we
    /// install on the destination becomes orphaned. After the saga
    /// runs <see cref="Orleans.Lattice.BPlusTree.ITxRegistryGrain.ForgetAsync"/>, the registry
    /// status read by a later reader returns <see cref="TxStatus.InFlight"/>
    /// (the default-when-absent fallback), the reader's dial-back
    /// surfaces the orphaned prepared value, and a later saga's value
    /// for the same key is shadowed - producing the
    /// <c>unknown-round</c> chaos failure shape where an older saga's
    /// value surfaces after newer sagas have committed. Two defenses
    /// close this window: (1) <b>per-snapshot pre-check</b> short-
    /// circuits the replay when the saga's status is already
    /// terminalized at sweep-time and applies the terminal directly to
    /// the destination with the snapshot value as <c>committedValues</c>
    /// backstop, never installing the orphan in the first place; (2)
    /// <b>post-sweep cleanup</b> re-checks every replayed saga's
    /// status and, for any that have flipped to Committed/Aborted in
    /// the meantime, applies the terminal directly to drain the
    /// pending bucket. Both calls are idempotent via the leaf-side
    /// <c>_recentlyTerminal</c> dedup, so the cleanup pass is a no-op
    /// when the saga's normal broadcast already reached destination.
    /// </para>
    /// </summary>
    private async Task RetroactiveSweepPreparedMutationsAsync(
        string physicalTreeId,
        int sourceShardIndex,
        int targetShardIndex,
        int[] movedSlots,
        int virtualShardCount)
    {
        if (movedSlots.Length == 0) return;
        if (targetShardIndex == sourceShardIndex) return;

        var sortedSlots = (int[])movedSlots.Clone();
        Array.Sort(sortedSlots);

        var source = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{sourceShardIndex}");
        var leafId = await source.GetLeftmostLeafIdAsync();
        if (leafId is null) return;

        var target = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{targetShardIndex}");
        var registry = grainFactory.GetGrain<ITxRegistryGrain>(physicalTreeId);
        var startTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        long replayed = 0;

        // Track per-txid snapshots so the post-sweep cleanup pass can
        // build per-saga committedValues payloads without re-walking
        // the source chain. Lazily allocated - the steady state is
        // zero pending mutations across the moved slots.
        Dictionary<Guid, List<PendingMutationSnapshot>>? perTxSnapshots = null;

        try
        {
            while (leafId is not null)
            {
                var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);
                var snapshots = await leaf.GetPendingMutationsForSlotsAsync(sortedSlots, virtualShardCount);
                foreach (var snapshot in snapshots)
                {
                    // Per-snapshot pre-check: if the saga has already
                    // terminalized at sweep-time, the saga's own
                    // commit-phase broadcast has finished and the
                    // destination cannot be reached via that path
                    // (destination was not yet a participant when the
                    // broadcast captured its participant set). Replaying
                    // the prepare here would install an orphan in
                    // destination's _pendingTx that no terminal will
                    // ever drain. Instead, apply the terminal directly
                    // with the snapshot value as the committedValues
                    // backstop; the destination's leaf-side per-key
                    // backstop path handles WAL durability and HLC
                    // stamping. Aborted sagas drop the entry without
                    // surfacing.
                    var preStatus = await registry.GetStatusAsync(snapshot.TransactionId);
                    if (preStatus == TxStatus.Committed)
                    {
                        Dictionary<string, byte[]>? committedValues = null;
                        if (!snapshot.IsTombstone && snapshot.Value is not null)
                            committedValues = new Dictionary<string, byte[]>(1) { [snapshot.Key] = snapshot.Value };
                        await target.AppendTxTerminalAsync(snapshot.TransactionId, committed: true, committedValues);
                        replayed++;
                        continue;
                    }
                    if (preStatus == TxStatus.Aborted)
                    {
                        await target.AppendTxTerminalAsync(snapshot.TransactionId, committed: false);
                        replayed++;
                        continue;
                    }

                    // Saga still in flight: replay the prepare normally.
                    // The replay's SetAsync also registers destination
                    // as a participant via RecordAffectedLeafIfPreparedAsync,
                    // so any saga broadcast that runs AFTER this point
                    // will reach destination.
                    await ReplayPreparedSnapshotAsync(target, snapshot);
                    replayed++;

                    // Install the destination-side shadow marker for
                    // this in-flight saga. The drain pass that runs
                    // AFTER the retroactive sweep imports the source's
                    // pre-saga value with IsMigrated=true into dest's
                    // Entries; without this marker, a reader observing
                    // the saga as Committed after MarkCommittedAsync
                    // (but BEFORE the backstop terminal reaches dest)
                    // would surface that migrated pre-saga value and
                    // split observation against any sibling whose
                    // backstop has landed. The marker is cleared
                    // automatically by ApplyTxTerminalAsync when the
                    // saga's terminal reaches dest.
                    //
                    // Per-snapshot single-key array allocation is
                    // intentional and cold-path: bounded by the count
                    // of in-flight sagas at split-begin x keys-per-
                    // saga in moved slots (the chaos suite caps this
                    // at ~10 entries). Batching across snapshots would
                    // entangle ordering with the per-snapshot
                    // ReplayPreparedSnapshotAsync above, which must
                    // register dest as a participant BEFORE its
                    // shadow marker lands so that a terminal arriving
                    // mid-replay cannot install an un-clearable marker.
                    await target.MarkSagaShadowAsync(snapshot.TransactionId, new[] { snapshot.Key });

                    // Track for post-sweep cleanup. Lazy allocation - the
                    // chaos-free path leaves the dictionary null.
                    perTxSnapshots ??= new Dictionary<Guid, List<PendingMutationSnapshot>>();
                    if (!perTxSnapshots.TryGetValue(snapshot.TransactionId, out var list))
                    {
                        list = new List<PendingMutationSnapshot>();
                        perTxSnapshots[snapshot.TransactionId] = list;
                    }
                    list.Add(snapshot);
                }
                leafId = await leaf.GetNextSiblingAsync();
            }

            // Post-sweep cleanup: close the orphan window for sagas
            // that were in-flight at per-snapshot pre-check time but
            // have since terminalized. Such a saga's commit broadcast
            // may have queried participants before the sweep registered
            // destination, sent the terminal only to source, and called
            // ForgetAsync - leaving the prepared entry on destination
            // orphaned. The registry's GetStatusManyAsync returns
            // Committed/Aborted if the decision is still persisted,
            // and InFlight (the default fallback) if the saga has been
            // forgotten. For Committed/Aborted we apply the terminal
            // directly. For InFlight at this point we leave the entry
            // pending - either the saga is genuinely still in flight
            // (its eventual broadcast will reach destination, which is
            // now registered as a participant) or it has been forgotten
            // and the entry is a true orphan. The latter is shadowed by
            // any later prepare for the same key via the highest-HLC
            // tie-break in TryFindPendingForKey.
            if (perTxSnapshots is { Count: > 0 })
            {
                var txids = new List<Guid>(perTxSnapshots.Keys);
                var statuses = await registry.GetStatusManyAsync(txids);
                foreach (var (txid, status) in statuses)
                {
                    if (status == TxStatus.InFlight) continue;

                    var committed = status == TxStatus.Committed;
                    Dictionary<string, byte[]>? committedValues = null;
                    if (committed)
                    {
                        committedValues = new Dictionary<string, byte[]>();
                        foreach (var snap in perTxSnapshots[txid])
                        {
                            if (!snap.IsTombstone && snap.Value is not null)
                                committedValues[snap.Key] = snap.Value;
                        }
                    }
                    await target.AppendTxTerminalAsync(txid, committed, committedValues);
                }
            }
        }
        finally
        {
            if (replayed > 0)
            {
                LatticeMetrics.SplitRetroactiveForwardEntries.Add(replayed,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
                    new KeyValuePair<string, object?>(LatticeMetrics.TagShard, sourceShardIndex));
            }

            var elapsedMs = (System.Diagnostics.Stopwatch.GetTimestamp() - startTicks)
                * 1000.0 / System.Diagnostics.Stopwatch.Frequency;
            LatticeMetrics.SplitRetroactiveForwardDuration.Record(elapsedMs,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
                new KeyValuePair<string, object?>(LatticeMetrics.TagShard, sourceShardIndex));
        }
    }

    /// <summary>
    /// Replays a single <see cref="Orleans.Lattice.BPlusTree.PendingMutationSnapshot"/> through
    /// the destination shard's standard write path. The four ambient
    /// scopes - transaction id, prepared flag, origin cluster, vector
    /// clock, HLC override - propagate via Orleans
    /// <see cref="Orleans.Runtime.RequestContext"/> so the destination
    /// leaf reads the same values at its HLC-tick site that the source
    /// leaf observed at prepare time. The destination's
    /// <c>BPlusLeafGrain.CommitSetAsync</c> then routes the mutation
    /// into its own pending-tx map (because <c>LatticePreparedContext.Current</c>
    /// is true) under the original <c>(txid, key)</c> identity.
    /// <para>
    /// Tombstones are replayed via <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.DeleteAsync"/>
    /// rather than the TTL-aware <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.SetAsync(string, byte[], long)"/>
    /// overload, so the destination's <c>CommitDeleteAsync</c> path
    /// stamps the prepared tombstone correctly. Non-tombstone replays
    /// use the TTL-aware Set overload so <c>ExpiresAtTicks</c> is
    /// preserved verbatim.
    /// </para>
    /// </summary>
    private static async Task ReplayPreparedSnapshotAsync(IShardRootGrain target, PendingMutationSnapshot snapshot)
    {
        var previousTxId = LatticeTransactionContext.Current;
        LatticeTransactionContext.Set(snapshot.TransactionId);
        try
        {
            using var preparedScope = LatticePreparedContext.BeginScope();
            using var originScope = LatticeOriginContext.With(snapshot.OriginClusterId);
            using var vcScope = LatticeVectorClockContext.With(snapshot.VectorClock);
            using var hlcScope = LatticeHlcOverrideContext.With(snapshot.Timestamp);
            // Carry the typed CRDT delta so the destination leaf's prepared
            // commit records it in its pending-tx delta side-map and folds it
            // on the saga's terminal (the per-replica union) rather than
            // installing the resharded LWW value verbatim. A plain LWW
            // snapshot (Delta null / Mode LwwRegister) opens no scope and
            // replays byte-for-byte as before.
            using var deltaScope = snapshot.Mode != LatticeMergeMode.LwwRegister
                    && snapshot.Delta is not null
                ? LatticeDeltaContext.With(snapshot.Delta)
                : null;

            if (snapshot.IsTombstone)
            {
                await target.DeleteAsync(snapshot.Key);
            }
            else
            {
                // Empty byte[] is the conventional value-of-a-tombstone
                // placeholder. Snapshots only carry a non-null
                // Value when IsTombstone is false, but defensively
                // substitute Array.Empty so the destination's
                // SetAsync(byte[]) parameter contract is satisfied
                // regardless of upstream shape.
                var value = snapshot.Value ?? Array.Empty<byte>();
                if (snapshot.ExpiresAtTicks > 0)
                    await target.SetAsync(snapshot.Key, value, snapshot.ExpiresAtTicks);
                else
                    await target.SetAsync(snapshot.Key, value);
            }
        }
        finally
        {
            LatticeTransactionContext.Set(previousTxId);
        }
    }
}
