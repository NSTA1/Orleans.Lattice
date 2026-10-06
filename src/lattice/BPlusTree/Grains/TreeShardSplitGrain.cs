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
/// <item><description><see cref="ShardSplitPhase.Swap"/> - mark the source's
/// leaves with the moved-slot set, put the source into reject mode (so stale
/// <c>LatticeGrain</c> activations still targeting it for moved-slot keys
/// receive <see cref="StaleShardRoutingException"/> and refresh), run a final
/// authoritative drain of the now-frozen moved slots, and only then reassign
/// those slots to the target in the persisted <see cref="ShardMap"/> in one
/// registry call.</description></item>
/// <item><description><see cref="Orleans.Lattice.BPlusTree.State.ShardSplitPhase.Reject"/> - re-assert the
/// source's reject mode (idempotent; <see cref="ShardSplitPhase.Swap"/> already
/// entered it) and advance to <see cref="ShardSplitPhase.Complete"/>.</description></item>
/// <item><description><see cref="ShardSplitPhase.Complete"/> - final drain
/// pass to capture any post-shadow tombstones, clear the source's
/// <c>SplitInProgress</c> state, record the committed split (the metric and,
/// when event publishing is enabled, a split-committed event), unregister the
/// keepalive, and deactivate.</description></item>
/// </list>
/// Key format: <c>{treeId}/{sourceShardIndex}</c>.
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

    /// <inheritdoc />
    protected override string MetricsTreeId => TreeId;

    /// <inheritdoc />
    protected override bool AbandonsSagaOnPurgedTree => true;

    /// <inheritdoc />
    protected override Task<bool> IsTreePurgedAsync() => PurgedTreeRegistrationGuard.IsPurgedAsync(grainFactory, TreeId);

    /// <inheritdoc />
    protected override Task ClearSagaStateForPurgedTreeAsync() => state.ClearStateAsync();

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

    /// <summary>
    /// The physical tree this split drains and swaps. An in-flight split stays
    /// bound to the tree it started on, even across a reactivation, so a
    /// cutover that re-points the logical id is detected rather than followed
    /// (issue #4264); a new split resolves the logical id afresh.
    /// </summary>
    private async Task<string> GetPhysicalTreeIdAsync()
    {
        if (_physicalTreeId is not null) return _physicalTreeId;
        if (state.State.InProgress && state.State.PhysicalTreeId is { } bound)
        {
            _physicalTreeId = bound;
            return bound;
        }

        var registry = grainFactory.GetLatticeRegistry();
        _physicalTreeId = await registry.ResolveAsync(TreeId);
        return _physicalTreeId;
    }

    /// <summary>
    /// Whether the logical tree's registry entry still describes the physical
    /// tree this split is bound to. See <see cref="ShardMapCommitFence"/>.
    /// </summary>
    private async Task<bool> IsBoundTreeCurrentAsync()
    {
        var entry = await grainFactory.GetLatticeRegistry().GetEntryAsync(TreeId);
        return ShardMapCommitFence.Admits(entry, TreeId, await GetPhysicalTreeIdAsync());
    }

    /// <summary>
    /// Abandons a split whose logical tree was cut over to another physical
    /// tree before the split committed (issue #4264). Nothing is written to the
    /// routing map: the moved slots keep routing to the source in the map the
    /// cutover carried, and that map describes the copy, which holds the moved
    /// entries on its own source shard. The source's migration record on the
    /// replaced tree is cleared when it is still reversible
    /// (<paramref name="sourceFrozen"/> false), so that tree is unchanged if the
    /// cutover is later undone.
    /// </summary>
    private async Task AbandonRetargetedSplitAsync(bool sourceFrozen)
    {
        var physicalTreeId = await GetPhysicalTreeIdAsync();
        Logger.LogWarning(
            "Shard split {OperationId} of shard {SourceShardIndex} on tree {TreeId} abandoned in phase {Phase}: the tree no longer resolves to physical tree {PhysicalTreeId}, whose shards the split migrated.",
            state.State.OperationId, state.State.SourceShardIndex, TreeId, state.State.Phase, physicalTreeId);
        await AbandonSplitAsync(sourceFrozen);
    }

    /// <summary>
    /// Abandons a split resumed in <see cref="ShardSplitPhase.BeginShadowWrite"/>
    /// while a resize of the tree holds shard migrations (issue #4452). The split's intent
    /// was persisted before the resize read the shards' migration records, but
    /// its source record was not yet open, so the resize started; the split backs
    /// out instead. Nothing has been drained or committed, so the tree is
    /// unchanged and a later split can be proposed afresh.
    /// </summary>
    private async Task AbandonSplitForResizeAsync()
    {
        Logger.LogWarning(
            "Shard split {OperationId} of shard {SourceShardIndex} on tree {TreeId} abandoned before its drain: a resize of the tree holds shard migrations.",
            state.State.OperationId, state.State.SourceShardIndex, TreeId);
        await AbandonSplitAsync(sourceFrozen: false);
    }

    private async Task AbandonSplitAsync(bool sourceFrozen)
    {
        var physicalTreeId = await GetPhysicalTreeIdAsync();
        if (!sourceFrozen)
        {
            var source = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{state.State.SourceShardIndex}");
            try
            {
                await source.AbortSplitAsync();
            }
            catch (InvalidOperationException ex)
            {
                // The source already entered reject (a Swap interrupted after its
                // freeze); that is no longer reversible, and the replaced tree no
                // longer serves the logical id.
                Logger.LogWarning(ex,
                    "Shard split {OperationId} on tree {TreeId} left the source shard's migration record in place on physical tree {PhysicalTreeId}.",
                    state.State.OperationId, TreeId, physicalTreeId);
            }
        }

        var prevInProgress = state.State.InProgress;
        var prevComplete = state.State.Complete;
        var prevPhase = state.State.Phase;
        var prevCursor = state.State.DrainCursorKey;
        state.State.InProgress = false;
        state.State.Complete = false;
        state.State.Phase = ShardSplitPhase.None;
        state.State.DrainCursorKey = null;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.InProgress = prevInProgress;
            state.State.Complete = prevComplete;
            state.State.Phase = prevPhase;
            state.State.DrainCursorKey = prevCursor;
            throw;
        }

        await CompleteCoordinatorAsync();
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

        // A new split binds to the physical tree the logical id resolves to now,
        // not to whichever one an earlier split on this coordinator used.
        _physicalTreeId = null;

        // Refuse while a resize of the tree holds splits (issues #4452, #4478):
        // the resize fixed the shards it copies and fences, and the map it
        // carries at its flip, when it started, so a split may not run until it
        // has completed and no undo is pending. Once it has, the copy it replaced
        // keeps mirroring into the resized one, but that mirror follows a split's
        // refusal to the shard that owns the slot now. Checked again once the
        // source's record is open, which is what closes the race; this read only
        // spares a refused split an allocated shard index and a persisted intent.
        if (await ShardMigrationResizeInterlock.ResizeHoldsShardSplitsAsync(grainFactory, TreeId))
            throw new InvalidOperationException(
                $"Shard {sourceShardIndex} of tree '{TreeId}' cannot be split while a resize of the tree is in progress or being undone.");

        // Serialise behind any migration already in flight on the source
        // shard. A shard carries a single migration record, and an online
        // consolidation opens its donor-side shadow-write window through the
        // same primitive, so a split that ran here would re-aim the fold's
        // window and strand the folded slots. The consolidation coordinator
        // already refuses symmetrically. This check is the cheap one - it
        // spares a refused split a freshly-allocated physical shard index and
        // a persisted intent; ShardRootGrain.BeginSplitAsync is what enforces
        // it atomically.
        var sourceShard = grainFactory.GetGrain<IShardRootGrain>(
            $"{await GetPhysicalTreeIdAsync()}/{sourceShardIndex}");
        if (await sourceShard.IsSplittingAsync())
            throw new InvalidOperationException(
                $"Shard {sourceShardIndex} of tree '{TreeId}' cannot be split while a migration is already in progress on it.");

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
        var registry = grainFactory.GetLatticeRegistry();
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

        // Bind to the physical tree now, and refuse while an alias cutover has
        // carried another tree's map onto the logical entry but not yet swapped
        // the alias: the map read above would then describe the copy while the
        // shards below belong to the replaced tree (issue #4264). Checked before
        // a shard index is allocated, so a refusal leaves nothing behind.
        var physicalTreeId = await GetPhysicalTreeIdAsync();
        if (!ShardMapCommitFence.Admits(await registry.GetEntryAsync(TreeId), TreeId, physicalTreeId))
            throw new InvalidOperationException(
                $"Shard {sourceShardIndex} of tree '{TreeId}' cannot be split while an alias cutover of the tree is in progress.");

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
        state.State.PhysicalTreeId = physicalTreeId;
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
        var source = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{sourceShardIndex}");
        try
        {
            await source.BeginSplitAsync(targetShardIndex, movedSlots, currentMap.Slots.Length);
        }
        catch (InvalidOperationException)
        {
            // The source shard acquired a migration record between the
            // pre-check in SplitAsync and this call - an online consolidation
            // taking it as a donor, in practice. Unwind the intent just
            // persisted rather than leaving this coordinator InProgress with
            // no reminder anchored on it: SplitAsync short-circuits on
            // InProgress, so a half-committed intent would make this shard
            // permanently unsplittable, and resuming later would drive a slot
            // plan the other migration has since invalidated.
            state.State.InProgress = prevInProgress;
            state.State.Complete = prevComplete;
            state.State.OperationId = prevOperationId;
            state.State.Phase = prevPhase;
            state.State.SourceShardIndex = prevSourceShardIndex;
            state.State.TargetShardIndex = prevTargetShardIndex;
            state.State.MovedSlots = prevMovedSlots;
            state.State.OriginalShardMap = prevOriginalShardMap;
            await state.WriteStateAsync();
            throw;
        }

        // The source's migration record is open; only now is a resize that
        // started after the read in SplitAsync guaranteed to see it, so read the
        // resize again and back out if one is in flight (issue #4452). The record
        // is still reversible: nothing has been swept or drained yet.
        if (await ShardMigrationResizeInterlock.ResizeHoldsShardSplitsAsync(grainFactory, TreeId))
        {
            await source.AbortSplitAsync();
            state.State.InProgress = prevInProgress;
            state.State.Complete = prevComplete;
            state.State.OperationId = prevOperationId;
            state.State.Phase = prevPhase;
            state.State.SourceShardIndex = prevSourceShardIndex;
            state.State.TargetShardIndex = prevTargetShardIndex;
            state.State.MovedSlots = prevMovedSlots;
            state.State.OriginalShardMap = prevOriginalShardMap;
            await state.WriteStateAsync();
            throw new InvalidOperationException(
                $"Shard {sourceShardIndex} of tree '{TreeId}' cannot be split while a resize of the tree is in progress or can still be undone.");
        }

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

        // A split whose tree was cut over before it committed is abandoned
        // rather than driven on against the replaced tree (issue #4264).
        // SwapAsync makes the same check for itself.
        if (state.State.Phase is ShardSplitPhase.BeginShadowWrite or ShardSplitPhase.Drain
            && !await IsBoundTreeCurrentAsync())
        {
            await AbandonRetargetedSplitAsync(sourceFrozen: false);
            return;
        }

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

            // A resize that started between this split persisting its intent and
            // opening the source's record could not see the split (issue #4452).
            if (await ShardMigrationResizeInterlock.ResizeHoldsShardSplitsAsync(grainFactory, TreeId))
            {
                await AbandonSplitForResizeAsync();
                return;
            }

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
        {
            // Drive the bounded drain to completion inside this one call. The
            // per-tick path (ProcessNextPhaseAsync) runs a single bounded pass
            // instead, so the work bound is what a live split actually gets;
            // this API keeps its "run the pass through" contract for callers
            // that drive a split explicitly.
            while (state.State.Phase == ShardSplitPhase.Drain)
                await DrainAsync();
        }

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
    /// Trees whose split coordinators skip their timer-driven background drain
    /// passes; see <see cref="HoldBackgroundDrainForTest"/>.
    /// </summary>
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, byte> BackgroundDrainHeldForTest =
        new(StringComparer.Ordinal);

    /// <summary>
    /// Test seam (issue #4613): stops the split coordinator of
    /// <paramref name="treeId"/> running a background
    /// <see cref="ShardSplitPhase.Drain"/> pass from its phase timer until
    /// <see cref="ReleaseBackgroundDrainForTest"/>, so a fixture can interleave
    /// writes before the final drain without a background pass importing the
    /// source's rows in between. An explicit <see cref="RunSplitPassAsync"/>
    /// still drives the drain.
    /// </summary>
    internal static void HoldBackgroundDrainForTest(string treeId) => BackgroundDrainHeldForTest[treeId] = 0;

    /// <summary>Lifts <see cref="HoldBackgroundDrainForTest"/> for <paramref name="treeId"/>.</summary>
    internal static void ReleaseBackgroundDrainForTest(string treeId) => BackgroundDrainHeldForTest.TryRemove(treeId, out _);

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
                    if (!BackgroundDrainHeldForTest.IsEmpty && BackgroundDrainHeldForTest.ContainsKey(TreeId))
                        break;
                    if (await IsBoundTreeCurrentAsync())
                        await DrainAsync();
                    else
                        await AbandonRetargetedSplitAsync(sourceFrozen: false);
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
            if (await TryAbandonSagaOnPurgedTreeAsync()) return;

            Logger.LogWarning(ex, "Shard-split phase {Phase} failed for tree {TreeId}",
                state.State.Phase, TreeId);
        }
    }

    /// <summary>
    /// Runs one bounded pass of the background drain, forwarding moved-slot
    /// entries from the source shard's leaf chain to the target shard and
    /// advancing to <see cref="ShardSplitPhase.Swap"/> only once the source's
    /// whole leaf chain has been swept. Idempotent: re-running after a crash
    /// converges via CRDT LWW. Returns whether the sweep finished on this pass.
    /// Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task<bool> DrainAsync()
    {
        var (sweepComplete, resumeFrom, _) = await ForwardMovedSlotEntriesAsync(
            state.State.DrainCursorKey,
            LeafWalkBudget.ForBackgroundDrain(Options));

        if (!sweepComplete)
        {
            // Persist the resume position and stay in Drain; the next tick
            // continues from the key. The phase is deliberately not advanced -
            // Swap's ordering invariants assume the historical sweep is done.
            var prevCursor = state.State.DrainCursorKey;
            state.State.DrainCursorKey = resumeFrom;
            try
            {
                await state.WriteStateAsync();
            }
            catch
            {
                state.State.DrainCursorKey = prevCursor;
                throw;
            }
            return false;
        }

        var prevPhase = state.State.Phase;
        var prevCursorOnComplete = state.State.DrainCursorKey;
        state.State.Phase = ShardSplitPhase.Swap;
        state.State.DrainCursorKey = null;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Phase = prevPhase;
            state.State.DrainCursorKey = prevCursorOnComplete;
            throw;
        }
        return true;
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
    /// <para>
    /// <b>Alias-cutover fence.</b> The diff names shard indices of the physical
    /// tree this split is bound to, so it is applied only while the logical tree
    /// still resolves to that tree and no cutover has carried another tree's map
    /// onto it (issue #4264). A split that loses the fence is abandoned, before
    /// the freeze when that is detected first, and never re-drives onto the copy.
    /// </para>
    /// </summary>
    internal async Task SwapAsync()
    {
        var physicalTreeId = await GetPhysicalTreeIdAsync();
        var source = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{state.State.SourceShardIndex}");

        // Abandon before anything irreversible when the tree was cut over to
        // another physical tree since this split started (issue #4264): the
        // freeze below cannot be undone, and the diff could not be applied.
        if (!await IsBoundTreeCurrentAsync())
        {
            await AbandonRetargetedSplitAsync(sourceFrozen: false);
            return;
        }

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
        await ForwardMovedSlotEntriesAtomicallyAsync("SplitSwapFinalDrain");

        var registry = grainFactory.GetLatticeRegistry();
        // Apply this swap's moved-slot diff inside a single registry call so
        // concurrent topology changes compose: ReassignSlotsAsync re-reads the
        // live map and persists the reassigned copy without interleaving, so a
        // consolidation fold landing alongside this split cannot erase either
        // coordinator's reassignment. Performing the same get-modify-set here
        // across two calls would not be atomic - the registry grain's
        // non-reentrancy serialises each individual call, not a sequence of
        // them, so a fold persisting in the gap would be clobbered by the
        // write below and its folded slots would keep routing to the donor it
        // had already drained.
        //
        // The fenced overload also refuses, inside that same call, a diff for a
        // tree an alias cutover has re-pointed since the check above: the diff's
        // shard indices describe the replaced tree, not the copy whose map the
        // cutover carried onto the logical entry (issue #4264).
        var reassigned = await registry.ReassignSlotsAsync(
            TreeId,
            state.State.MovedSlots.ToArray(),
            state.State.TargetShardIndex,
            state.State.OriginalShardMap!,
            physicalTreeId);
        if (reassigned is null)
        {
            await AbandonRetargetedSplitAsync(sourceFrozen: true);
            return;
        }

        // The registry ReassignSlotsAsync side effect is cross-grain and
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
        await ForwardMovedSlotEntriesAtomicallyAsync("SplitFinaliseFinalDrain");

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
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, optionsResolver.GetMetricTreeId(TreeId)),
            new KeyValuePair<string, object?>(LatticeMetrics.TagShard, state.State.SourceShardIndex),
            LatticeTenantLabel.ForTree(TreeId));

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
    /// <para>
    /// <b>Work is bounded by <paramref name="budget"/>, and only in the
    /// background <see cref="ShardSplitPhase.Drain"/> phase</b> (issue 1973).
    /// The authoritative sweeps in <see cref="SwapAsync"/> and
    /// <see cref="FinaliseAsync"/> pass an unbounded budget - see the note at
    /// each of those call sites for why yielding there would be unsafe.
    /// </para>
    /// <para>
    /// <b>What a pass boundary makes observable during Drain.</b> Nothing that
    /// a batch flush did not already. This walk has never been atomic: it
    /// flushes to the target every <see cref="LatticeOptions.SplitDrainBatchSize"/>
    /// entries, so the target has always been observable part-drained. Nor is
    /// the target observable to a reader yet - the registry's
    /// <see cref="ShardMap"/> still routes every moved slot to the source
    /// throughout Drain, and only flips in <see cref="SwapAsync"/>, after the
    /// sweep has completed and after a further authoritative sweep over the
    /// frozen source. Writes accepted by the source during Drain are mirrored
    /// onto the target by the hot-path shadow-forward, and every entry is
    /// forwarded under its original HLC, so a re-drained leaf and a concurrent
    /// shadow write converge to the same LWW fixed point regardless of
    /// ordering. A pass boundary therefore lengthens the drain window without
    /// widening what any caller can observe.
    /// </para>
    /// </summary>
    /// <returns>
    /// Whether the source's whole leaf chain has been swept, the key the next
    /// pass resumes from when it has not, and the leaves this pass visited.
    /// </returns>
    private async Task<(bool SweepComplete, string? ResumeFromInclusive, int LeavesVisited)> ForwardMovedSlotEntriesAsync(
        string? resumeFromInclusive,
        LeafWalkBudget budget)
    {
        var physicalTreeId = await GetPhysicalTreeIdAsync();

        var source = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{state.State.SourceShardIndex}");
        var movedSlotsArray = state.State.MovedSlots.ToArray();
        Array.Sort(movedSlotsArray);
        var virtualShardCount = state.State.OriginalShardMap!.Slots.Length;
        var batchSize = Options.SplitDrainBatchSize;
        if (batchSize <= 0) batchSize = LatticeOptions.DefaultSplitDrainBatchSize;

        var target = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{state.State.TargetShardIndex}");
        var emptyVector = new VersionVector();

        // Bounded retry: BoundedLeafWalk's cursor is always a key, so
        // restarting from the pass's ORIGINAL resumeFromInclusive (never a
        // partial cursor this attempt advanced to) re-descends the shard root
        // fresh and routes around a leaf a concurrent fold retired mid-walk.
        // A retired-then-reactivated leaf has no persisted row and no
        // in-memory create-intent for this caller, so BPlusLeafGrain's
        // row-loss guard cannot tell it apart from a genuinely lost row and
        // fails closed with LeafStateRowLostException - correctly, since the
        // guard has no way to recognise "legitimately retired" from here.
        // Entries already flushed to target before the fault are idempotent
        // re-merges under LWW on retry, so re-walking from the original
        // resume key never double-applies or loses an entry. See the
        // matching rationale on RetroactiveSweepPreparedMutationsAsync.
        const int maxAttempts = 5;
        for (var attempt = 1; ; attempt++)
        {
            var walk = await BoundedLeafWalk.StartAsync(grainFactory, source, resumeFromInclusive, budget);
            if (!walk.HasLeaf) return (true, null, 0);

            var batch = new Dictionary<string, LwwValue<byte[]>>(batchSize);

            try
            {
                while (walk.HasLeaf)
                {
                    var leaf = walk.CurrentLeaf;
                    // Slot filtering is pushed into the leaf so only
                    // moved-slot entries are serialised on the response -
                    // saves bandwidth and coordinator-side allocations on
                    // hot shards where moved slots are a minority of the
                    // keyspace.
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

                    if (!await walk.MoveNextAsync()) break;
                }

                // Flush before returning, so the cursor this pass persists is
                // never ahead of the entries the target has actually
                // accepted. Persisting a cursor past an unflushed batch would
                // drop those entries permanently: the next pass resumes
                // beyond them and no later sweep re-reads them.
                if (batch.Count > 0)
                    await target.MergeManyAsync(batch, isCrossShardMigration: true);

                return (walk.Completed, walk.ResumeFromInclusive, walk.LeavesVisited);
            }
            catch (LeafStateRowLostException) when (attempt < maxAttempts)
            {
            }
        }
    }

    /// <summary>
    /// Sweeps the source's whole leaf chain in one turn, for the two
    /// authoritative drains that run once the source can no longer accept a
    /// moved-slot write.
    /// <para>
    /// <b>DELIBERATELY NOT WORK-BOUNDED</b> (issues 1956, 1973). Both callers
    /// are the step that makes the target provably equal to the source's final
    /// committed state, immediately before or immediately after routing flips
    /// onto the target. Yielding mid-sweep would stretch that window across
    /// timer ticks while readers are already being pushed off the source, for
    /// no correctness gain: the source is frozen for the moved slots, so no
    /// amount of extra time can change what the sweep would read. It is instead
    /// made <em>attributable</em> through <see cref="AtomicLeafWalk"/>, so a
    /// long hold names itself in the log rather than surfacing only as a
    /// coordinator that stopped answering.
    /// </para>
    /// </summary>
    private async Task ForwardMovedSlotEntriesAtomicallyAsync(string operation)
    {
        var walk = new AtomicLeafWalk(operation);
        var (_, _, leavesVisited) = await ForwardMovedSlotEntriesAsync(
            resumeFromInclusive: null, LeafWalkBudget.Unbounded());
        walk.RecordLeavesVisited(leavesVisited);
        walk.ReportIfSlow(Logger, Context.GrainId);
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
    /// the saga's own commit-phase terminal broadcast. The saga fans out
    /// to its touched shards, expanded through each shard's split-forward
    /// records as they stood when it read them, and then re-fetches
    /// <see cref="Orleans.Lattice.BPlusTree.ITxRegistryGrain.GetParticipantsAsync"/> in a
    /// bounded number of late-pickup rounds. If that expansion predates this
    /// split's record and the saga's last participant fetch returns BEFORE
    /// the sweep's per-snapshot <c>SetAsync</c> registers the destination
    /// shard (via <c>RecordAffectedLeafIfPreparedAsync</c>), the saga's
    /// terminal fan-out never reaches the destination and the prepared
    /// entry we install there becomes orphaned. After the saga runs
    /// <see cref="Orleans.Lattice.BPlusTree.ITxRegistryGrain.ForgetAsync"/>, the registry
    /// keeps reporting the recorded decision for
    /// <see cref="LatticeOptions.TxDecisionRetention"/>, then
    /// <see cref="TxStatus.Indeterminate"/> until the row is pruned, and
    /// <see cref="TxStatus.InFlight"/> (the default-when-absent fallback)
    /// after that - or at once under a zero retention. While the orphan
    /// resolves as Committed, a reader's dial-back surfaces its prepared
    /// value over any later saga's committed value for the same key -
    /// producing the <c>unknown-round</c> chaos failure shape where an
    /// older saga's value surfaces after newer sagas have committed - and
    /// once it resolves as Indeterminate the key is hidden. Two defenses
    /// narrow this window, closing it only while the saga's decision is
    /// still reported: (1) <b>per-snapshot pre-check</b> short-
    /// circuits the replay when the saga's status is already
    /// terminalized at sweep-time and applies the terminal directly to
    /// the destination with the snapshot value as <c>committedValues</c>
    /// backstop, never installing the orphan in the first place; (2)
    /// <b>post-sweep cleanup</b> re-checks every replayed saga's
    /// status and, for any that have flipped to Committed/Aborted in
    /// the meantime, applies the terminal directly to drain the
    /// pending bucket. A replayed saga whose status already reads
    /// Indeterminate or InFlight at cleanup time is left pending. Both
    /// calls are idempotent via the leaf-side
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
        if (await source.GetLeftmostLeafIdAsync() is null) return;

        var target = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{targetShardIndex}");
        var startTicks = System.Diagnostics.Stopwatch.GetTimestamp();
        var progress = new PreparedBucketSweepProgress();

        // DELIBERATELY NOT WORK-BOUNDED (issues 1956, 1973). Do not route this
        // walk through BoundedLeafWalk, and do not give it a persisted cursor.
        //
        // Two invariants make whole-sweep atomicity load-bearing here, and both
        // are about what the NEXT phase assumes rather than about this walk:
        //
        // 1. The Drain phase, which runs immediately after this sweep, imports
        //    the source's pre-saga values into the destination with
        //    IsMigrated=true. The destination-side shadow markers this sweep
        //    installs are the only thing that stops a reader surfacing one of
        //    those migrated pre-saga values for a saga it observes as
        //    committed. Persisting a half-finished sweep and advancing on a
        //    later turn would let Drain start with markers installed for some
        //    in-flight sagas and not others - which is precisely the torn
        //    observation against a backstopped sibling this sweep exists to
        //    prevent.
        //
        // 2. The recovery contract is "re-run the entire sweep", asserted by
        //    the BeginShadowWrite branch of RunSplitPassAsync. That is what
        //    makes the per-snapshot pre-check below sound for every leaf: each
        //    re-entry re-reads every prepared mutation still on the source and
        //    re-decides against the saga's status as of that moment. A durable
        //    mid-sweep cursor would replace it with "resume where you stopped",
        //    under which leaves visited before an interruption are never
        //    re-examined, so a saga that terminalized during the gap is never
        //    re-decided on those leaves.
        //
        // The cost is bounded in practice: this reads only PREPARED mutations
        // on the moved slots, whose count is active-saga concurrency at
        // split-begin, and the phase runs once per split. It is made
        // attributable instead of bounded, so a long hold names itself.
        var atomicWalk = new AtomicLeafWalk(nameof(RetroactiveSweepPreparedMutationsAsync));

        try
        {
            // Bounded retry: the walk's own leftmost-leaf id can name a leaf a
            // concurrent fold retires mid-sweep. A retired-then-reactivated
            // leaf has no persisted row and no in-memory create-intent for
            // this caller, so BPlusLeafGrain's row-loss guard cannot tell it
            // apart from a genuinely lost row and fails closed with
            // LeafStateRowLostException - correctly, since the guard has no
            // way to recognise "legitimately retired" from here. Retirement
            // itself is refused while any prepared/pending mutation remains
            // on the leaf (see BPlusLeafGrain.Reclaim.HasReclaimBlockingState),
            // so a leaf this sweep still needed to visit can never have been
            // retired out from under it - the exception marks a stale chain
            // read, never a dropped mutation. PreparedBucketSweep.RunAsync's
            // own contract is "re-run the whole sweep" on any fault; re-
            // resolving the leftmost leaf id on each attempt re-reads the
            // sibling chain the retirement already repointed, routing the
            // retry around the retired leaf.
            const int maxAttempts = 5;
            for (var attempt = 1; ; attempt++)
            {
                var attemptLeafId = await source.GetLeftmostLeafIdAsync();
                if (attemptLeafId is null) break;

                progress.Replayed = 0;
                progress.LeavesVisited = 0;

                try
                {
                    await PreparedBucketSweep.RunAsync(
                        grainFactory, TreeId, attemptLeafId.Value, target, sortedSlots, virtualShardCount,
                        progress, carryOriginalStamps: true);
                    break;
                }
                catch (LeafStateRowLostException) when (attempt < maxAttempts)
                {
                }
            }
        }
        finally
        {
            atomicWalk.RecordLeavesVisited(progress.LeavesVisited);
            atomicWalk.ReportIfSlow(Logger, Context.GrainId);

            if (progress.Replayed > 0)
            {
                LatticeMetrics.SplitRetroactiveForwardEntries.Add(progress.Replayed,
                    new KeyValuePair<string, object?>(LatticeMetrics.TagTree, optionsResolver.GetMetricTreeId(TreeId)),
                    new KeyValuePair<string, object?>(LatticeMetrics.TagShard, sourceShardIndex),
                    LatticeTenantLabel.ForTree(TreeId));
            }

            var elapsedMs = (System.Diagnostics.Stopwatch.GetTimestamp() - startTicks)
                * 1000.0 / System.Diagnostics.Stopwatch.Frequency;
            LatticeMetrics.SplitRetroactiveForwardDuration.Record(elapsedMs,
                new KeyValuePair<string, object?>(LatticeMetrics.TagTree, optionsResolver.GetMetricTreeId(TreeId)),
                new KeyValuePair<string, object?>(LatticeMetrics.TagShard, sourceShardIndex),
                LatticeTenantLabel.ForTree(TreeId));
        }
    }
}
