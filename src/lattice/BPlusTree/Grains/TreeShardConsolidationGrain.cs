using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Coordinator that drives an online shard consolidation end-to-end: the
/// inverse of an adaptive shard split.
/// <para>
/// An adaptive split is a one-way door. A tree that has been split has no way
/// back however wrong the split turned out to be, and neither of the two
/// things in this codebase named "merge" is the missing inverse:
/// <c>TreeMergeGrain</c> bulk-loads one whole tree into another, and
/// <c>ShardRootGrain.TraverseForMergeAsync</c> merges a B+ tree <em>node</em>
/// during traversal. Neither reduces a tree's physical shard count. This
/// coordinator does, by folding one physical donor shard's virtual slots back
/// onto an adjacent physical survivor shard and retiring the donor from the
/// routing map.
/// </para>
/// <para>
/// <b>Fully online.</b> No shard is taken offline and the tree is never
/// quiesced, because consolidation has to be usable on exactly the busy,
/// already-damaged deployments it exists to heal. That is achieved by reusing
/// the split's per-shard shadow-write primitive verbatim on the donor: the
/// donor mirrors every accepted write on the folding slots to the survivor
/// while a bounded background drain copies its history across, and CRDT LWW
/// makes the two streams converge regardless of interleaving.
/// </para>
/// <para>
/// Phase machine:
/// </para>
/// <list type="number">
/// <item><description>
/// <see cref="ShardConsolidationPhase.BeginShadowWrite"/> - persist intent and
/// open the donor's shadow-write window onto the survivor.
/// </description></item>
/// <item><description>
/// <see cref="ShardConsolidationPhase.Drain"/> - copy the donor's entries for
/// the folding slots to the survivor, a bounded number of leaves per pass,
/// resuming from a persisted cursor. Tombstones, expiries and CRDT causality
/// metadata ride along verbatim because the drain moves whole
/// <see cref="LwwValue{T}"/> records, not values.
/// </description></item>
/// <item><description>
/// <see cref="ShardConsolidationPhase.Swap"/> - the single freeze-and-flip
/// step: seal the donor's leaves, freeze the donor, run one authoritative
/// final drain over the now-frozen donor, reclaim the slots on the survivor,
/// and re-point the registry's <see cref="ShardMap"/>.
/// </description></item>
/// <item><description>
/// <see cref="ShardConsolidationPhase.Reject"/> - the donor refuses folded-slot
/// operations so stale routing caches self-heal onto the survivor.
/// </description></item>
/// <item><description>
/// <see cref="ShardConsolidationPhase.Complete"/> - a last drain pass, then the
/// donor records the folded slots permanently and the coordinator retires.
/// </description></item>
/// </list>
/// <para>
/// <b>Durability.</b> Consolidation never deletes donor leaf state and never
/// releases a WAL materialiser pin. The donor is retired from the routing map
/// only, so the WAL GC's trim horizon - a minimum over live pins - cannot move
/// forward as a result of a fold, and no prefix becomes trimmable that was not
/// trimmable before. Reclaiming the retired donor's <em>storage</em> is a
/// separate concern that would need a durable-absorption proof, and is
/// deliberately not attempted here.
/// </para>
/// <para>
/// Key format: <c>{treeId}/{donorShardIndex}</c>.
/// </para>
/// </summary>
internal sealed class TreeShardConsolidationGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    LatticeOptionsResolver optionsResolver,
    ILogger<TreeShardConsolidationGrain> logger,
    [PersistentState("tree-shard-consolidation", LatticeOptions.StorageProviderName)]
    IPersistentState<TreeShardConsolidationState> state)
    : CoordinatorGrain<TreeShardConsolidationGrain>(context, reminderRegistry, logger), ITreeShardConsolidationGrain
{
    /// <inheritdoc />
    protected override string KeepaliveReminderName => "shard-consolidation-keepalive";

    /// <inheritdoc />
    protected override bool InProgress => state.State.InProgress;

    /// <inheritdoc />
    protected override string LogContext => $"tree {TreeId} donor {DonorShardIndexFromKey}";

    /// <summary>
    /// Clock used for progress timestamps. Defaults to
    /// <see cref="TimeProvider.System"/>; unit tests substitute a controllable
    /// provider so no assertion depends on the wall clock.
    /// </summary>
    internal TimeProvider Clock { get; set; } = TimeProvider.System;

    /// <summary>
    /// Parses the grain key as <c>{treeId}/{donorShardIndex}</c>: everything
    /// before the final '/' is the tree id.
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
    /// The donor shard index encoded in the grain key, or <c>-1</c> when the
    /// key carries no parseable integer suffix.
    /// </summary>
    private int DonorShardIndexFromKey
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
    /// Activation-scoped cache of the folding slot set, so the drain does not
    /// rebuild an array from the persisted list on every pass.
    /// </summary>
    private int[]? _donorSlotsCache;

    /// <summary>
    /// Activation-scoped grain references for the two shards this fold moves
    /// data between. Resolving them per pass would format two interpolated
    /// keys and take two factory lookups every timer tick for the whole life
    /// of a fold; the pair is fixed for the duration of an operation, so it is
    /// resolved once and reset when a new intent is persisted.
    /// </summary>
    private IShardRootGrain? _donorCache;
    private IShardRootGrain? _survivorCache;

    private async Task<string> GetPhysicalTreeIdAsync()
    {
        if (_physicalTreeId is not null) return _physicalTreeId;
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        _physicalTreeId = await registry.ResolveAsync(TreeId);
        return _physicalTreeId;
    }

    private int[] DonorSlots
    {
        get
        {
            if (_donorSlotsCache is { } cached && cached.Length == state.State.DonorSlots.Count)
                return cached;
            var slots = state.State.DonorSlots.ToArray();
            Array.Sort(slots);
            _donorSlotsCache = slots;
            return slots;
        }
    }

    private int VirtualShardCount => state.State.OriginalShardMap!.Slots.Length;

    /// <inheritdoc />
    public async Task StartAsync(int survivorShardIndex)
    {
        if (survivorShardIndex < 0)
            throw new ArgumentOutOfRangeException(nameof(survivorShardIndex), "Must be non-negative.");

        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            Context.ActivationServices, TreeId, LatticeOperation.Admin);

        var donorShardIndex = DonorShardIndexFromKey;
        if (donorShardIndex < 0)
            throw new InvalidOperationException(
                $"Consolidation coordinator key '{Context.GrainId.Key}' does not carry a donor shard index; expected '{{treeId}}/{{donorShardIndex}}'.");

        if (donorShardIndex == survivorShardIndex)
            throw new ArgumentException(
                "Donor and survivor must be different physical shards.", nameof(survivorShardIndex));

        if (state.State.InProgress)
        {
            // Idempotent re-entry for the same target; refused for a different
            // one so a driver cannot silently re-aim an in-flight fold.
            if (state.State.SurvivorShardIndex == survivorShardIndex) return;
            throw new InvalidOperationException(
                $"A consolidation of shard {donorShardIndex} into shard {state.State.SurvivorShardIndex} is already in progress for tree '{TreeId}'.");
        }

        await InitiateConsolidationStateAsync(donorShardIndex, survivorShardIndex);

        // A donor that already owns nothing leaves InProgress false: the pair
        // is already consolidated and there is no work to anchor a reminder on.
        if (!state.State.InProgress) return;

        await StartCoordinatorAsync();
    }

    /// <summary>
    /// Validates the pair, persists the consolidation intent, and opens the
    /// donor's shadow-write window onto the survivor. Leaves
    /// <see cref="TreeShardConsolidationState.InProgress"/> <see langword="false"/>
    /// when the donor already owns no virtual slot, which is what makes
    /// consolidating an already-consolidated pair a clean no-op. Exposed as
    /// <c>internal</c> for unit testing.
    /// </summary>
    internal async Task InitiateConsolidationStateAsync(int donorShardIndex, int survivorShardIndex)
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var resolved = await optionsResolver.ResolveAsync(TreeId);

        var currentMap = await registry.GetShardMapAsync(TreeId)
            ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, resolved.ShardCount);

        if (!ShardConsolidationPlanner.TryPlan(
                currentMap, donorShardIndex, survivorShardIndex, out var plan, out var reason))
        {
            // "Already consolidated" is the idempotent no-op, not a fault:
            // a driver re-running a healing sweep must be able to re-issue
            // every pair it planned without special-casing the finished ones.
            if (ShardConsolidationPlanner.CountOwnedSlots(currentMap, donorShardIndex) == 0)
            {
                Logger.LogDebug(
                    "Consolidation of shard {Donor} into shard {Survivor} on tree {TreeId} is a no-op: the donor owns no virtual slot.",
                    donorShardIndex, survivorShardIndex, TreeId);
                return;
            }

            throw new InvalidOperationException(
                $"Shard {donorShardIndex} of tree '{TreeId}' cannot be consolidated into shard {survivorShardIndex}: {reason}");
        }

        var physicalTreeId = await GetPhysicalTreeIdAsync();
        var donor = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{donorShardIndex}");
        var survivor = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{survivorShardIndex}");

        // Serialise behind any adaptive split touching either side. A split
        // and a consolidation contending for the same shard's single
        // SplitInProgress record would each clobber the other's migration
        // intent, and the loser's slots would be stranded.
        if (await donor.IsSplittingAsync())
            throw new InvalidOperationException(
                $"Shard {donorShardIndex} of tree '{TreeId}' cannot be consolidated while an adaptive split is in progress on it.");
        if (await survivor.IsSplittingAsync())
            throw new InvalidOperationException(
                $"Shard {donorShardIndex} of tree '{TreeId}' cannot be consolidated into shard {survivorShardIndex} while an adaptive split is in progress on the survivor.");

        var previous = Snapshot();

        var now = Clock.GetUtcNow().UtcTicks;
        state.State.InProgress = true;
        state.State.Complete = false;
        state.State.Cancelled = false;
        state.State.CancelRequested = false;
        state.State.OperationId = Guid.NewGuid().ToString("N");
        state.State.Phase = ShardConsolidationPhase.BeginShadowWrite;
        state.State.DonorShardIndex = donorShardIndex;
        state.State.SurvivorShardIndex = survivorShardIndex;
        state.State.DonorSlots = new List<int>(plan.DonorSlots);
        state.State.OriginalShardMap = currentMap;
        state.State.DrainCursorLeafId = null;
        state.State.DrainSweepComplete = false;
        state.State.EntriesDrained = 0;
        state.State.LeavesScanned = 0;
        state.State.StartedAtTicks = now;
        state.State.UpdatedAtTicks = now;
        _donorSlotsCache = null;
        _donorCache = null;
        _survivorCache = null;

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            Restore(previous);
            throw;
        }

        // Open the shadow-write window. The donor plays exactly the role a
        // split gives its source shard, so this is the split primitive reused
        // rather than a parallel implementation.
        try
        {
            await donor.BeginSplitAsync(survivorShardIndex, plan.DonorSlots, plan.VirtualShardCount);
        }
        catch (InvalidOperationException)
        {
            // The donor acquired a migration record between the IsSplittingAsync
            // pre-check above and this call. Unwind the intent just persisted
            // rather than leaving this coordinator InProgress with no reminder
            // anchored on it: StartAsync short-circuits on InProgress, so a
            // half-committed intent would deadlock the pair - never idle, never
            // driven, and never restartable.
            Restore(previous);
            await state.WriteStateAsync();
            throw;
        }

        await AdvancePhaseAsync(ShardConsolidationPhase.Drain);

        Logger.LogInformation(
            "Consolidation {OperationId} started on tree {TreeId}: folding {SlotCount} virtual slot(s) from shard {Donor} into shard {Survivor}.",
            state.State.OperationId, TreeId, plan.DonorSlots.Length, donorShardIndex, survivorShardIndex);
    }

    /// <inheritdoc />
    public async Task RunConsolidationPassAsync()
    {
        if (!state.State.InProgress) return;

        // Bounded by the phase count plus the drain's own per-pass cap, so a
        // caller driving the fold synchronously still yields between passes on
        // a large donor rather than blocking the activation indefinitely.
        for (var guard = 0; guard < 8 && state.State.InProgress; guard++)
        {
            var phaseBefore = state.State.Phase;
            var drainedToCompletion = await ProcessPhaseAsync();
            if (!drainedToCompletion && state.State.Phase == phaseBefore) return;
        }
    }

    /// <inheritdoc />
    public Task<ShardConsolidationProgress> GetProgressAsync()
        => Task.FromResult(new ShardConsolidationProgress
        {
            InProgress = state.State.InProgress,
            Complete = state.State.Complete,
            Cancelled = state.State.Cancelled,
            Phase = state.State.Phase,
            DonorShardIndex = state.State.DonorShardIndex,
            SurvivorShardIndex = state.State.SurvivorShardIndex,
            SlotsToFold = state.State.DonorSlots.Count,
            EntriesDrained = state.State.EntriesDrained,
            LeavesScanned = state.State.LeavesScanned,
            OperationId = state.State.OperationId,
            StartedAtTicks = state.State.StartedAtTicks,
            UpdatedAtTicks = state.State.UpdatedAtTicks,
            CancelRequested = state.State.CancelRequested,
        });

    /// <inheritdoc />
    public async Task<bool> CancelAsync()
    {
        if (!state.State.InProgress) return false;

        var acceptable = IsCancellable(state.State.Phase);

        // Record the request either way so a poll shows it was received, but
        // only report success when it can actually be honoured. Past the swap
        // the routing map has already flipped and abandoning would strand the
        // donor mid-retirement, so the fold deliberately runs to completion.
        if (!state.State.CancelRequested)
        {
            state.State.CancelRequested = true;
            state.State.UpdatedAtTicks = Clock.GetUtcNow().UtcTicks;
            await state.WriteStateAsync();
        }

        return acceptable;
    }

    /// <inheritdoc />
    public Task<bool> IsIdleAsync() => Task.FromResult(!state.State.InProgress);

    /// <summary>
    /// Whether a consolidation in <paramref name="phase"/> can still be
    /// abandoned without having changed the tree's routing.
    /// </summary>
    private static bool IsCancellable(ShardConsolidationPhase phase)
        => phase is ShardConsolidationPhase.BeginShadowWrite or ShardConsolidationPhase.Drain;

    /// <inheritdoc />
    protected internal override async Task ProcessNextPhaseAsync()
    {
        if (!state.State.InProgress) return;

        try
        {
            await ProcessPhaseAsync();
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex,
                "Shard-consolidation phase {Phase} failed for {Context}", state.State.Phase, LogContext);
        }
    }

    /// <summary>
    /// Advances the phase machine by one step. Returns <see langword="true"/>
    /// when the step made progress that a synchronous driver should follow
    /// immediately, and <see langword="false"/> when the current phase still
    /// has bounded work outstanding.
    /// </summary>
    private async Task<bool> ProcessPhaseAsync()
    {
        // Honour a cancel only at a boundary where the tree's routing is still
        // untouched. Checked before the phase body so a request that lands
        // mid-drain takes effect at the start of the next pass rather than
        // tearing the one in flight.
        if (state.State.CancelRequested && IsCancellable(state.State.Phase))
        {
            await AbandonAsync();
            return true;
        }

        switch (state.State.Phase)
        {
            case ShardConsolidationPhase.BeginShadowWrite:
                await ReopenShadowWriteAsync();
                return true;

            case ShardConsolidationPhase.Drain:
                return await DrainAsync();

            case ShardConsolidationPhase.Swap:
                await SwapAsync();
                return true;

            case ShardConsolidationPhase.Reject:
                await EnterRejectAsync();
                return true;

            case ShardConsolidationPhase.Complete:
                await FinaliseAsync();
                return true;

            default:
                return false;
        }
    }

    /// <summary>
    /// Crash-recovery re-entry for
    /// <see cref="ShardConsolidationPhase.BeginShadowWrite"/>: re-issues the
    /// donor's shadow-write window in case the coordinator crashed between
    /// persisting intent and reaching the donor. Idempotent on the donor side.
    /// Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task ReopenShadowWriteAsync()
    {
        var donor = await GetDonorAsync();
        await donor.BeginSplitAsync(state.State.SurvivorShardIndex, DonorSlots, VirtualShardCount);
        await AdvancePhaseAsync(ShardConsolidationPhase.Drain);
    }

    /// <summary>
    /// Runs one bounded drain pass and advances to
    /// <see cref="ShardConsolidationPhase.Swap"/> once the donor's whole leaf
    /// chain has been swept. Returns <see langword="true"/> when the sweep
    /// completed on this pass. Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task<bool> DrainAsync()
    {
        var sweepComplete = await DrainPassAsync(Options.ConsolidationDrainLeavesPerPass);
        if (!sweepComplete) return false;

        await AdvancePhaseAsync(ShardConsolidationPhase.Swap);
        return true;
    }

    /// <summary>
    /// The single freeze-and-flip step.
    /// <para>
    /// <b>Ordering invariant.</b> The donor must be sealed and frozen
    /// <em>before</em> the registry's map flips, and the survivor must reclaim
    /// the slots <em>after</em> the authoritative final drain and
    /// <em>before</em> the flip. Each of those three orderings closes a
    /// distinct hole:
    /// </para>
    /// <list type="bullet">
    /// <item><description>Freezing before the flip forces a stale-routing
    /// reader off the donor instead of letting it read a value the survivor
    /// has since superseded - the same invariant the split's swap documents.
    /// </description></item>
    /// <item><description>Draining after the freeze makes the survivor's copy
    /// provably identical to the donor's final committed state, because the
    /// donor can no longer accept a folded-slot write. The hot-path
    /// shadow-forward is best-effort under LWW and can lag; this pass is what
    /// turns "eventually equal" into "equal now".</description></item>
    /// <item><description>Reclaiming before the flip is the step that has no
    /// analogue in a split, and the one whose absence would make every folded
    /// key permanently unreachable: the survivor is usually the shard the
    /// donor was split out of, so until it reclaims, it refuses the very slots
    /// the map is about to send it.</description></item>
    /// </list>
    /// <para>
    /// Every action is idempotent, so a crash anywhere inside this step is
    /// recovered by simply re-running it. Exposed as <c>internal</c> for unit
    /// testing.
    /// </para>
    /// </summary>
    internal async Task SwapAsync()
    {
        var donor = await GetDonorAsync();
        var slots = DonorSlots;
        var vsc = VirtualShardCount;

        // Seal the donor's leaves first so no read crosses the freeze
        // observing an unsealed leaf under a frozen shard.
        await donor.MarkLeavesMovedAwayAsync(slots, vsc);
        await donor.EnterRejectPhaseAsync();

        // Authoritative final sweep over the now-frozen donor. Unbounded by
        // the per-pass leaf cap on purpose: the donor's folded slots can no
        // longer change, and leaving the freeze window open across many timer
        // ticks would be a real availability cost for no correctness gain.
        state.State.DrainCursorLeafId = null;
        state.State.DrainSweepComplete = false;
        await DrainPassAsync(int.MaxValue);

        // Lift the survivor's seal only now that its copy is authoritative.
        var survivor = await GetSurvivorAsync();
        await survivor.ReclaimSlotsAsync(slots, vsc);

        // Re-read the live map so concurrent topology changes compose: this
        // fold applies its own slot diff onto whatever is currently persisted
        // rather than clobbering someone else's swap. The registry grain is
        // non-reentrant, so get-modify-set is atomic across callers.
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var currentMap = await registry.GetShardMapAsync(TreeId) ?? state.State.OriginalShardMap!;
        var newSlots = (int[])currentMap.Slots.Clone();
        var survivorIndex = state.State.SurvivorShardIndex;
        for (var i = 0; i < slots.Length; i++)
            newSlots[slots[i]] = survivorIndex;
        await registry.SetShardMapAsync(TreeId, new ShardMap { Slots = newSlots });

        await AdvancePhaseAsync(ShardConsolidationPhase.Reject);
    }

    /// <summary>
    /// Idempotent re-assertion of the donor's frozen state after the flip, so
    /// a coordinator that crashed between the flip and this phase still leaves
    /// stale routes self-healing. Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task EnterRejectAsync()
    {
        var donor = await GetDonorAsync();
        await donor.EnterRejectPhaseAsync();
        await AdvancePhaseAsync(ShardConsolidationPhase.Complete);
    }

    /// <summary>
    /// Final drain pass to capture anything written during the freeze window,
    /// then retires the donor: the folded slots move into its permanent
    /// moved-away map so every later stale route self-heals onto the survivor,
    /// and the coordinator clears its own in-progress state. Exposed as
    /// <c>internal</c> for unit testing.
    /// </summary>
    internal async Task FinaliseAsync()
    {
        state.State.DrainCursorLeafId = null;
        state.State.DrainSweepComplete = false;
        await DrainPassAsync(int.MaxValue);

        var donor = await GetDonorAsync();
        await donor.CompleteSplitAsync();

        var previous = Snapshot();
        state.State.InProgress = false;
        state.State.Complete = true;
        state.State.Cancelled = false;
        state.State.CancelRequested = false;
        state.State.Phase = ShardConsolidationPhase.None;
        state.State.DrainCursorLeafId = null;
        state.State.UpdatedAtTicks = Clock.GetUtcNow().UtcTicks;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            Restore(previous);
            throw;
        }

        Logger.LogInformation(
            "Consolidation {OperationId} committed on tree {TreeId}: shard {Donor} retired into shard {Survivor} after draining {Entries} entr(y/ies) across {Leaves} leaf/leaves.",
            state.State.OperationId, TreeId, state.State.DonorShardIndex, state.State.SurvivorShardIndex,
            state.State.EntriesDrained, state.State.LeavesScanned);

        // Fired only after the terminal write succeeded, so an increment always
        // corresponds to a durably-committed fold rather than an attempt. The
        // shard tag carries the donor - the shard this fold retired.
        LatticeMetrics.ShardConsolidationsCommitted.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagShard, state.State.DonorShardIndex),
            LatticeTenantLabel.ForTree(TreeId));

        await CompleteCoordinatorAsync();
    }

    /// <summary>
    /// Honours a cancel at a pre-swap boundary: clears the donor's migration
    /// record so the tree is exactly as it was, and retires the coordinator.
    /// The survivor keeps whatever entries the drain already copied, which is
    /// harmless - every one carries its original HLC, so the survivor's copy
    /// is LWW-equal to the donor's and is invisible to readers, whose routing
    /// map still points the slots at the donor. Exposed as <c>internal</c> for
    /// unit testing.
    /// </summary>
    internal async Task AbandonAsync()
    {
        var donor = await GetDonorAsync();
        await donor.AbortSplitAsync();

        var previous = Snapshot();
        state.State.InProgress = false;
        state.State.Complete = false;
        state.State.Cancelled = true;
        state.State.CancelRequested = false;
        state.State.Phase = ShardConsolidationPhase.None;
        state.State.DrainCursorLeafId = null;
        state.State.DrainSweepComplete = false;
        state.State.UpdatedAtTicks = Clock.GetUtcNow().UtcTicks;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            Restore(previous);
            throw;
        }

        Logger.LogInformation(
            "Consolidation {OperationId} on tree {TreeId} abandoned before the routing swap; shard {Donor} is unchanged.",
            state.State.OperationId, TreeId, state.State.DonorShardIndex);

        await CompleteCoordinatorAsync();
    }

    /// <summary>
    /// Copies the donor's entries for the folding slots onto the survivor,
    /// visiting at most <paramref name="maxLeaves"/> donor leaves and resuming
    /// from the persisted cursor. Returns <see langword="true"/> when the
    /// donor's whole leaf chain has been swept.
    /// <para>
    /// <b>Allocation.</b> The per-entry path allocates nothing: the batch
    /// dictionary is created once per pass at the configured capacity and
    /// cleared rather than reallocated between flushes, the delta is walked
    /// through <c>Dictionary</c>'s struct enumerator, and each entry is a
    /// reference copy of the <see cref="LwwValue{T}"/> the donor already
    /// materialised for the response. Nothing on this path formats a string,
    /// projects through LINQ, or boxes.
    /// </para>
    /// <para>
    /// <b>Idempotence.</b> Every entry carries its original HLC, so
    /// re-draining a leaf - after a crash, a cursor reset, or the
    /// authoritative final sweep - is a fixed point under LWW merge.
    /// </para>
    /// </summary>
    private async Task<bool> DrainPassAsync(int maxLeaves)
    {
        if (state.State.DrainSweepComplete) return true;

        var donor = await GetDonorAsync();

        var leafId = state.State.DrainCursorLeafId ?? await donor.GetLeftmostLeafIdAsync();
        if (leafId is null)
        {
            state.State.DrainSweepComplete = true;
            await PersistDrainProgressAsync();
            return true;
        }

        var slots = DonorSlots;
        var vsc = VirtualShardCount;

        var batchSize = Options.ConsolidationDrainBatchSize;
        if (batchSize <= 0) batchSize = LatticeOptions.DefaultConsolidationDrainBatchSize;

        var survivor = await GetSurvivorAsync();
        var batch = new Dictionary<string, LwwValue<byte[]>>(batchSize);
        var sinceVersion = new VersionVector();

        var leavesVisited = 0;
        var entriesForwarded = 0L;
        var sweepComplete = false;

        while (leafId is not null && leavesVisited < maxLeaves)
        {
            var leaf = grainFactory.GetGrain<IBPlusLeafGrain>(leafId.Value);

            // Slot filtering is pushed into the leaf so only folding-slot
            // entries are serialised onto the response - the donor's other
            // keys never cross the wire and never enter this batch.
            var delta = await leaf.GetDeltaSinceForSlotsAsync(sinceVersion, slots, vsc);
            foreach (var (key, lww) in delta.Entries)
            {
                batch[key] = lww;
                if (batch.Count >= batchSize)
                {
                    entriesForwarded += batch.Count;
                    await survivor.MergeManyAsync(batch, isCrossShardMigration: true);
                    batch.Clear();
                }
            }

            leavesVisited++;

            var next = await leaf.GetNextSiblingAsync();
            if (next is null)
            {
                sweepComplete = true;
                leafId = null;
                break;
            }
            leafId = next;
        }

        if (batch.Count > 0)
        {
            entriesForwarded += batch.Count;
            await survivor.MergeManyAsync(batch, isCrossShardMigration: true);
        }

        state.State.DrainCursorLeafId = leafId;
        state.State.DrainSweepComplete = sweepComplete;
        state.State.EntriesDrained += entriesForwarded;
        state.State.LeavesScanned += leavesVisited;
        await PersistDrainProgressAsync();

        return sweepComplete;
    }

    /// <summary>
    /// Persists drain progress, reverting the in-memory cursor and counters if
    /// the write fails so the activation and storage never disagree about how
    /// far the sweep got.
    /// </summary>
    private async Task PersistDrainProgressAsync()
    {
        var previousUpdatedAt = state.State.UpdatedAtTicks;
        state.State.UpdatedAtTicks = Clock.GetUtcNow().UtcTicks;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.UpdatedAtTicks = previousUpdatedAt;
            throw;
        }
    }

    /// <summary>
    /// Persists a phase transition, reverting the in-memory phase when the
    /// write fails. Without the revert an activation would believe it had
    /// advanced while storage still held the previous phase, and every retry
    /// from that activation would skip the work the persisted state still owes.
    /// </summary>
    private async Task AdvancePhaseAsync(ShardConsolidationPhase next)
    {
        var previousPhase = state.State.Phase;
        var previousUpdatedAt = state.State.UpdatedAtTicks;
        var previousCursor = state.State.DrainCursorLeafId;
        var previousSweep = state.State.DrainSweepComplete;

        state.State.Phase = next;
        state.State.UpdatedAtTicks = Clock.GetUtcNow().UtcTicks;

        // Each phase gets a fresh sweep: the drain's completion flag is scoped
        // to the phase that set it, so the authoritative post-freeze sweep can
        // never be skipped because an earlier phase already finished one.
        state.State.DrainCursorLeafId = null;
        state.State.DrainSweepComplete = false;

        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Phase = previousPhase;
            state.State.UpdatedAtTicks = previousUpdatedAt;
            state.State.DrainCursorLeafId = previousCursor;
            state.State.DrainSweepComplete = previousSweep;
            throw;
        }
    }

    private async Task<IShardRootGrain> GetDonorAsync()
    {
        if (_donorCache is { } cached) return cached;
        var physicalTreeId = await GetPhysicalTreeIdAsync();
        _donorCache = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{state.State.DonorShardIndex}");
        return _donorCache;
    }

    private async Task<IShardRootGrain> GetSurvivorAsync()
    {
        if (_survivorCache is { } cached) return cached;
        var physicalTreeId = await GetPhysicalTreeIdAsync();
        _survivorCache = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{state.State.SurvivorShardIndex}");
        return _survivorCache;
    }

    /// <summary>
    /// Captures the mutable coordinator fields so a failed
    /// <c>WriteStateAsync</c> can unwind the in-memory mutation. Orleans leaves
    /// the in-memory object mutated when a persist throws, and a coordinator
    /// whose memory says "finished" while storage says "in flight" short-
    /// circuits every retry from the same activation.
    /// </summary>
    private (bool InProgress, bool Complete, bool Cancelled, bool CancelRequested, ShardConsolidationPhase Phase, GrainId? Cursor, bool Sweep, long UpdatedAt) Snapshot()
        => (state.State.InProgress, state.State.Complete, state.State.Cancelled, state.State.CancelRequested,
            state.State.Phase, state.State.DrainCursorLeafId, state.State.DrainSweepComplete, state.State.UpdatedAtTicks);

    private void Restore(
        (bool InProgress, bool Complete, bool Cancelled, bool CancelRequested, ShardConsolidationPhase Phase, GrainId? Cursor, bool Sweep, long UpdatedAt) previous)
    {
        state.State.InProgress = previous.InProgress;
        state.State.Complete = previous.Complete;
        state.State.Cancelled = previous.Cancelled;
        state.State.CancelRequested = previous.CancelRequested;
        state.State.Phase = previous.Phase;
        state.State.DrainCursorLeafId = previous.Cursor;
        state.State.DrainSweepComplete = previous.Sweep;
        state.State.UpdatedAtTicks = previous.UpdatedAt;
    }
}
