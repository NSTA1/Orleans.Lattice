using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Coordinator that drives an online reshard end-to-end.
/// <para>
/// Phase machine:
/// </para>
/// <list type="number">
/// <item><description><see cref="ReshardPhase.Planning"/> - persist the
/// target shard count and transition to
/// <see cref="ReshardPhase.Migrating"/>.</description></item>
/// <item><description><see cref="ReshardPhase.Migrating"/> - each tick
/// inspects the current <see cref="ShardMap"/>, counts distinct physical
/// shards, and - while below target - dispatches up to
/// <see cref="LatticeOptions.MaxConcurrentMigrations"/> per-shard
/// <see cref="ITreeShardSplitGrain.SplitAsync"/> calls against the
/// largest-slot-owning eligible shards (those owning at least two virtual
/// slots and not already splitting). Every completed split atomically
/// grows the map by one distinct physical shard via its swap phase; the
/// next tick simply re-evaluates.</description></item>
/// <item><description><see cref="ReshardPhase.Complete"/> - target
/// reached; coordinator clears <see cref="TreeReshardState.InProgress"/>,
/// unregisters its keepalive, and deactivates.</description></item>
/// </list>
/// Key format: <c>{treeId}</c>.
/// </summary>
internal sealed class TreeReshardGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    LatticeOptionsResolver optionsResolver,
    ILogger<TreeReshardGrain> logger,
    ITagIndexReconcileTrigger tagIndexReconcileTrigger,
    [PersistentState("tree-reshard", LatticeOptions.StorageProviderName)]
    IPersistentState<TreeReshardState> state)
    : CoordinatorGrain<TreeReshardGrain>(context, reminderRegistry, logger), ITreeReshardGrain
{
    private string TreeId => Context.GrainId.Key.ToString()!;
    private LatticeOptions Options => optionsMonitor.Get(TreeId);

    /// <inheritdoc />
    protected override string KeepaliveReminderName => "reshard-keepalive";

    /// <inheritdoc />
    protected override bool InProgress => state.State.InProgress;

    /// <inheritdoc />
    protected override string LogContext => $"tree {TreeId}";

    /// <inheritdoc />
    public async Task ReshardAsync(int newShardCount)
    {
        LatticeInternalOriginContext.EnsureInternalGrainOrigin(
            Context.ActivationServices, TreeId, LatticeOperation.Admin);

        // Reshard activity counters: record the in-flight observation
        // (0 or 1 per call) and tag with TreeId so a wedge cohort can
        // correlate reshard activity with wedge onset directly. The
        // initiated counter increments AFTER the validation gate below
        // so it counts only invocations that actually start a reshard.
        var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId);
        var tenantTag = LatticeTenantLabel.ForTree(TreeId);
        LatticeMetrics.ShardRootReshardInFlight.Record(state.State.InProgress ? 1L : 0L, treeTag, tenantTag);

        if (newShardCount < 2)
        {
            LatticeMetrics.ShardRootReshardRejected.Add(1, treeTag, new KeyValuePair<string, object?>("reason", "argument_out_of_range_min"), tenantTag);
            throw new ArgumentOutOfRangeException(nameof(newShardCount),
                "Target shard count must be at least 2.");
        }

        var resolved = await optionsResolver.ResolveAsync(TreeId);
        if (newShardCount > LatticeConstants.DefaultVirtualShardCount)
        {
            LatticeMetrics.ShardRootReshardRejected.Add(1, treeTag, new KeyValuePair<string, object?>("reason", "argument_out_of_range_max"), tenantTag);
            throw new ArgumentOutOfRangeException(nameof(newShardCount),
                $"Target shard count ({newShardCount}) cannot exceed the virtual shard space ({LatticeConstants.DefaultVirtualShardCount}).");
        }

        if (state.State.InProgress)
        {
            if (state.State.TargetShardCount == newShardCount) return;
            LatticeMetrics.ShardRootReshardRejected.Add(1, treeTag, new KeyValuePair<string, object?>("reason", "already_in_progress"), tenantTag);
            throw new InvalidOperationException(
                $"A reshard is already in progress for tree '{TreeId}' (target={state.State.TargetShardCount}).");
        }

        // Inspect the current map to validate grow-only semantics.
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var currentMap = await registry.GetShardMapAsync(TreeId)
            ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, resolved.ShardCount);
        var currentCount = currentMap.GetPhysicalShardIndices().Count;

        // Empty-tree fast-path: if the tree has no live entries yet,
        // repin ShardCount atomically and rebuild the default identity map
        // without activating the coordinator machinery. This also relaxes
        // the grow-only restriction so callers (including test fixtures)
        // can set any desired shard count on a freshly-created tree.
        if (newShardCount != currentCount && await IsObservablyEmptyAsync(resolved, currentMap))
        {
            await ApplyEmptyTreeResharAsync(registry, newShardCount, LatticeConstants.DefaultVirtualShardCount);
            return;
        }

        // Idempotent re-pin: a caller asking for the count the tree is
        // already at is a safe no-op. The shard map already matches the
        // request, so there is nothing to migrate; treating this as an
        // error has crashed hosts whose start-up unconditionally pins the
        // tree's configured shard count on every run.
        if (newShardCount == currentCount)
        {
            return;
        }

        if (newShardCount < currentCount)
        {
            LatticeMetrics.ShardRootReshardRejected.Add(1, treeTag, new KeyValuePair<string, object?>("reason", "shrink_unsupported"), tenantTag);
#if LATTICE_DIAG
            // DIAG-PATH1: diagnose why currentCount mis-tracks the pinned ShardCount.
            var diagRegistry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            var diagEntry = await diagRegistry.GetEntryAsync(TreeId);
            var diagMap = await diagRegistry.GetShardMapAsync(TreeId);
            var diagPhysical = diagMap?.GetPhysicalShardIndices()?.Count;
            var diagVsc = diagMap?.Slots.Length;
            throw new ArgumentOutOfRangeException(nameof(newShardCount),
                $"Target shard count ({newShardCount}) must be greater than current count ({currentCount}). Shrink is not supported. " +
                $"[DIAG-PATH1 resolved.ShardCount={resolved.ShardCount} entry.ShardCount={diagEntry?.ShardCount} entry.MaxLeafKeys={diagEntry?.MaxLeafKeys} entry.MaxInternalChildren={diagEntry?.MaxInternalChildren} map.VirtualShardCount={diagVsc} map.PhysicalCount={diagPhysical} map.Version={diagMap?.Version}]");
#else
            throw new ArgumentOutOfRangeException(nameof(newShardCount),
                $"Target shard count ({newShardCount}) must be greater than current count ({currentCount}). Shrink is not supported.");
#endif
        }

        // Interlock: refuse to start a reshard while a resize is in flight.
        // Resize crosses physical trees; concurrent ShardMap mutation on the
        // source would invalidate the resize snapshot's per-slot routing
        // assumptions. Checked after argument validation so that callers
        // providing invalid parameters always receive an argument exception.
        var resize = grainFactory.GetGrain<ITreeResizeGrain>(TreeId);
        if (!await resize.IsIdleAsync())
        {
            LatticeMetrics.ShardRootReshardRejected.Add(1, treeTag, new KeyValuePair<string, object?>("reason", "resize_in_flight"), tenantTag);
            throw new InvalidOperationException(
                $"A resize is already in progress for tree '{TreeId}'; reshard refused until resize completes.");
        }

        // Snapshot every field the mutation set touches so a failing
        // WriteStateAsync leaves the activation observably equal to what
        // disk (and any future reactivation) see. Without this, the
        // in-memory InProgress / Phase / TargetShardCount would survive
        // the throw and the ReshardAsync idempotency guard at the top of
        // this method (`if (state.State.InProgress) ...`) would
        // short-circuit retries on dirty values - a transient storage
        // failure becoming a permanent "reshard never started" state until
        // the activation recycles. Snapshot Complete *before* the L111
        // `if (state.State.Complete) state.State.Complete = false;` reset
        // so a previously-completed reshard isn't observably lost on throw.
        var prevComplete = state.State.Complete;
        var prevInProgress = state.State.InProgress;
        var prevOperationId = state.State.OperationId;
        var prevPhase = state.State.Phase;
        var prevTargetShardCount = state.State.TargetShardCount;

        if (state.State.Complete) state.State.Complete = false;

        state.State.InProgress = true;
        state.State.OperationId = Guid.NewGuid().ToString("N");
        state.State.Phase = ReshardPhase.Migrating;
        state.State.TargetShardCount = newShardCount;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Complete = prevComplete;
            state.State.InProgress = prevInProgress;
            state.State.OperationId = prevOperationId;
            state.State.Phase = prevPhase;
            state.State.TargetShardCount = prevTargetShardCount;
            LatticeMetrics.ShardRootReshardRejected.Add(1, treeTag, new KeyValuePair<string, object?>("reason", "state_write_failed"), tenantTag);
            throw;
        }

        // Reshard activity counter: a reshard coordinator has been
        // successfully started.
        LatticeMetrics.ShardRootReshardInitiated.Add(1, treeTag, tenantTag);

        await StartCoordinatorAsync();
    }

    /// <summary>
    /// Bounded, conservative probe for the empty-tree fast path.
    /// <para>
    /// The fast path needs a boolean - "does this tree hold any live key?" -
    /// but <see cref="ILattice.CountAsync(CancellationToken)"/> answers it with
    /// a strongly-consistent whole-tree fan-out that discards its result and
    /// retries whenever the shard map moves under it, then throws once
    /// <see cref="LatticeOptions.MaxScanRetries"/> is exhausted. Reshard
    /// initiation is exactly when that map is most likely to be churning - a
    /// caller may be writing concurrently, and a small leaf fan-out splits
    /// continuously - so an unbounded probe here can consume the whole
    /// caller-side response budget and time the reshard out before it has
    /// started.
    /// </para>
    /// <para>
    /// Both inconclusive outcomes - the budget elapsing, and the count
    /// abandoning under churn - are reported as "not empty". That is not
    /// merely the safe direction but the accurate one: the only thing that
    /// makes this probe slow or unstable is concurrent split churn, and a tree
    /// whose topology is churning necessarily holds keys. A genuinely empty
    /// tree has nothing to churn, answers well inside the budget, and still
    /// takes the fast path.
    /// </para>
    /// <para>
    /// See <see cref="TreeEmptinessProbe"/> for why this deliberately does not
    /// go through <see cref="ILattice.CountAsync(CancellationToken)"/>, and why
    /// an existence question needs no reconciliation against a moving shard map.
    /// </para>
    /// </summary>
    /// <param name="resolved">The resolved per-tree options supplying the budget.</param>
    /// <param name="currentMap">The shard map observed by the caller, used to enumerate physical shards.</param>
    /// <returns><see langword="true"/> only when the tree was positively observed to be empty.</returns>
    private async Task<bool> IsObservablyEmptyAsync(LatticeOptions resolved, ShardMap currentMap) =>
        await TreeEmptinessProbe.IsObservablyEmptyAsync(
            grainFactory,
            await ResolvePhysicalTreeIdAsync(),
            currentMap.GetPhysicalShardIndices(),
            resolved.EmptyTreeProbeBudget);

    /// <inheritdoc />
    public async Task RunReshardPassAsync()
    {
        if (!state.State.InProgress) return;

        if (state.State.Phase == ReshardPhase.Planning)
        {
            // Snapshot Phase so a failing persist of the Planning->Migrating
            // flip doesn't leak an in-memory Phase=Migrating ahead of disk.
            // Bundled with the high-priority guarded sites above per the
            // same-grain Class B rule: this site self-heals via Phase
            // replay on a subsequent reactivation, but a concurrent reader
            // on the dirty in-memory Phase could observe Migrating while
            // disk still says Planning.
            var prevPhase = state.State.Phase;
            state.State.Phase = ReshardPhase.Migrating;
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

        if (state.State.Phase == ReshardPhase.Migrating)
            await MigrateAsync();

        if (state.State.Phase == ReshardPhase.Complete)
            await FinaliseAsync();
    }

    /// <inheritdoc />
    public Task<bool> IsIdleAsync() => Task.FromResult(!state.State.InProgress);

    /// <summary>
    /// Processes a single phase of the reshard. Exposed as <c>internal</c> for
    /// unit testing.
    /// </summary>
    protected internal override async Task ProcessNextPhaseAsync()
    {
        if (!state.State.InProgress) return;

        switch (state.State.Phase)
        {
            case ReshardPhase.Planning:
                // Snapshot Phase so a failing persist of the Planning->Migrating
                // flip doesn't leak an in-memory Phase=Migrating ahead of
                // disk. Bundled with the high-priority guarded sites in
                // ReshardAsync / FinaliseAsync above per the same-grain
                // Class B rule.
                var prevPhase = state.State.Phase;
                state.State.Phase = ReshardPhase.Migrating;
                try
                {
                    await state.WriteStateAsync();
                }
                catch
                {
                    state.State.Phase = prevPhase;
                    throw;
                }
                break;
            case ReshardPhase.Migrating:
                await MigrateAsync();
                break;
            case ReshardPhase.Complete:
                await FinaliseAsync();
                break;
        }
    }

    /// <summary>
    /// Evaluates the current <see cref="ShardMap"/>, terminates if the
    /// target count has been reached, and otherwise dispatches up to
    /// <see cref="LatticeOptions.MaxConcurrentMigrations"/> per-shard splits
    /// against the largest-slot-owning eligible shards. Exposed as
    /// <c>internal</c> for unit testing.
    /// </summary>
    internal async Task MigrateAsync()
    {
        var resolved = await optionsResolver.ResolveAsync(TreeId);
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var currentMap = await registry.GetShardMapAsync(TreeId)
            ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, resolved.ShardCount);

        var physicalShards = currentMap.GetPhysicalShardIndices();
        if (physicalShards.Count >= state.State.TargetShardCount)
        {
            // Snapshot Phase so a failing persist of the Migrating->Complete
            // flip doesn't leak an in-memory Phase=Complete ahead of disk.
            // Bundled with the high-priority guarded sites in ReshardAsync /
            // FinaliseAsync per the same-grain Class B rule: a dirty
            // in-memory Phase=Complete here would trigger RunReshardPassAsync's
            // `if (Phase == Complete) await FinaliseAsync()` clause on the
            // next tick (without a fresh reload), advancing the workflow
            // past Migrating while disk still says we're mid-migration.
            var prevPhase = state.State.Phase;
            state.State.Phase = ReshardPhase.Complete;
            try
            {
                await state.WriteStateAsync();
            }
            catch
            {
                state.State.Phase = prevPhase;
                throw;
            }
            return;
        }

        // Count virtual-slot ownership per physical shard, aligned to the
        // physicalShards ordinals so the eligibility scan below reads the
        // count by position rather than re-hashing the physical index.
        var slotCounts = CountSlotsPerPhysicalShard(physicalShards, currentMap.Slots);

        // Filter to eligible sources: owns ≥ 2 slots AND is not already
        // splitting. Splits-in-flight are counted separately and reduce the
        // remaining dispatch budget so we do not over-dispatch.
        var physicalTreeId = await ResolvePhysicalTreeIdAsync();
        var splittingTasks = new List<Task<bool>>(physicalShards.Count);
        var splittingIndices = new List<int>(physicalShards.Count);
        var splittingSlotCounts = new List<int>(physicalShards.Count);
        for (var i = 0; i < physicalShards.Count; i++)
        {
            var owned = slotCounts[i];
            if (owned < 2) continue;
            var idx = physicalShards[i];
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{idx}");
            splittingTasks.Add(shard.IsSplittingAsync());
            splittingIndices.Add(idx);
            splittingSlotCounts.Add(owned);
        }
        await Task.WhenAll(splittingTasks);

        var inFlight = 0;
        var eligible = new List<(int Shard, int Slots)>(splittingIndices.Count);
        for (int i = 0; i < splittingIndices.Count; i++)
        {
            if (splittingTasks[i].Result) { inFlight++; continue; }
            eligible.Add((splittingIndices[i], splittingSlotCounts[i]));
        }

        var maxConcurrent = resolved.MaxConcurrentMigrations;
        if (maxConcurrent < 1) maxConcurrent = 1;
        if (inFlight >= maxConcurrent) return; // Wait for in-flight splits to commit before dispatching more.

        // Pick the hottest-by-slot-count sources for the remaining dispatch budget.
        eligible.Sort((a, b) => b.Slots.CompareTo(a.Slots));

        // Clamp the dispatch budget to how many more distinct shards are
        // still needed. Over-dispatching here would still be correct (the
        // split coordinators are idempotent) but wastes I/O.
        var needed = state.State.TargetShardCount - physicalShards.Count - inFlight;
        if (needed <= 0) return;

        var dispatchBudget = Math.Min(maxConcurrent - inFlight, Math.Min(eligible.Count, needed));
        if (dispatchBudget <= 0) return;

        var dispatches = new List<Task>(dispatchBudget);
        for (int i = 0; i < dispatchBudget; i++)
        {
            var sourceShardIndex = eligible[i].Shard;
            var split = grainFactory.GetGrain<ITreeShardSplitGrain>($"{TreeId}/{sourceShardIndex}");
            dispatches.Add(DispatchSplitAsync(split, sourceShardIndex));
        }
        await Task.WhenAll(dispatches);
    }

    /// <summary>
    /// Counts how many virtual slots each physical shard owns, returning the
    /// counts aligned to <paramref name="physicalShards"/> ordinals - that is,
    /// the result at index <c>i</c> is the slot count for
    /// <c>physicalShards[i]</c>.
    /// </summary>
    /// <remarks>
    /// Physical shard indices form a small, dense, non-negative domain
    /// (typically 1..16) while <paramref name="slots"/> spans the virtual slot
    /// space (4096 by default), so the prior
    /// <c>Dictionary&lt;int, int&gt;</c> histogram paid a hash read plus a hash
    /// write for every virtual slot on every migrating tick. A dense counter
    /// array indexed by physical shard hashes nothing per slot.
    /// <para>
    /// <see cref="ShardMap.GetPhysicalShardIndices"/> returns distinct indices
    /// in ascending order, so the last element bounds the counter array.
    /// Pathologically large indices - never emitted by
    /// <c>ShardMap.CreateDefault</c> or the split path, and the same case
    /// <see cref="ShardMap.GetPhysicalShardIndices"/> guards - fall back to a
    /// binary search over that ascending list rather than over-allocating.
    /// </para>
    /// </remarks>
    internal static int[] CountSlotsPerPhysicalShard(IReadOnlyList<int> physicalShards, int[] slots)
    {
        var counts = new int[physicalShards.Count];
        if (counts.Length == 0) return counts;

        const int DenseCounterLimit = 1 << 20;
        var max = physicalShards[physicalShards.Count - 1];
        if (max < DenseCounterLimit)
        {
            var byPhysicalIndex = new int[max + 1];
            for (var i = 0; i < slots.Length; i++)
            {
                // Slots are sourced from the same map as physicalShards, so
                // every value is in range; the explicit guard replaces the
                // implicit bounds check rather than adding one, and keeps a
                // mismatched pair a no-op instead of a throw.
                var owner = (uint)slots[i];
                if (owner < (uint)byPhysicalIndex.Length) byPhysicalIndex[owner]++;
            }
            for (var i = 0; i < counts.Length; i++) counts[i] = byPhysicalIndex[physicalShards[i]];
            return counts;
        }

        for (var i = 0; i < slots.Length; i++)
        {
            var ordinal = IndexOfAscending(physicalShards, slots[i]);
            if (ordinal >= 0) counts[ordinal]++;
        }
        return counts;
    }

    /// <summary>
    /// Binary-searches an ascending, distinct index list, returning the
    /// ordinal of <paramref name="value"/> or <c>-1</c> when absent.
    /// </summary>
    private static int IndexOfAscending(IReadOnlyList<int> ascending, int value)
    {
        var lo = 0;
        var hi = ascending.Count - 1;
        while (lo <= hi)
        {
            var mid = lo + ((hi - lo) >> 1);
            var candidate = ascending[mid];
            if (candidate == value) return mid;
            if (candidate < value) lo = mid + 1;
            else hi = mid - 1;
        }
        return -1;
    }

    private async Task DispatchSplitAsync(ITreeShardSplitGrain split, int sourceShardIndex)
    {
        try
        {
            await split.SplitAsync(sourceShardIndex);
            Logger.LogInformation(
                "Reshard dispatched split of shard {ShardIndex} for tree {TreeId}",
                sourceShardIndex, TreeId);
        }
        catch (InvalidOperationException ex)
        {
            // Split already in progress for a different parameter set, or
            // source owns fewer than two slots - skip this shard and let the
            // next tick try another candidate.
            Logger.LogDebug(ex,
                "Could not dispatch split for shard {ShardIndex} during reshard of tree {TreeId}",
                sourceShardIndex, TreeId);
        }
    }

    private async Task<string> ResolvePhysicalTreeIdAsync()
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        return await registry.ResolveAsync(TreeId);
    }

    /// <summary>
    /// Clears in-progress state, marks the reshard complete, unregisters the
    /// keepalive, and deactivates. Exposed as <c>internal</c> for unit testing.
    /// </summary>
    internal async Task FinaliseAsync()
    {
        // Repin the structural ShardCount on the registry so future
        // resolver calls see the new physical shard count. The shard map
        // itself was updated incrementally by each per-shard split; this
        // reconciles the scalar pin with the map's physical shard count.
        await UpdateShardCountPinAsync(state.State.TargetShardCount);

        // Snapshot every field the completion flip mutates. Without this,
        // a failing WriteStateAsync would leave InProgress=false and
        // Complete=true in memory while disk still says the reshard is
        // running. IsIdleAsync (defined as `!InProgress`) would then lie
        // to callers; the keepalive reminder would still tick and re-enter
        // RunReshardPassAsync which now short-circuits at its !InProgress
        // guard - the reshard halts on this activation while disk-loaded
        // reactivations would resume.
        var prevInProgress = state.State.InProgress;
        var prevComplete = state.State.Complete;
        var prevPhase = state.State.Phase;

        state.State.InProgress = false;
        state.State.Complete = true;
        state.State.Phase = ReshardPhase.None;
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

        LatticeMetrics.CoordinatorCompleted.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagKind, "reshard"),
            LatticeTenantLabel.ForTree(TreeId));

        // Reshard activity counter: coordinator-driven reshard
        // completed successfully.
        LatticeMetrics.ShardRootReshardCompleted.Add(1, new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId), LatticeTenantLabel.ForTree(TreeId));

        await PublishReshardCompletedAsync();

        // The tree's shard structure changed under its logical id. Converge any tag
        // index covering this tree onto the new structure promptly rather than at
        // the next scheduled reconcile sweep. Best-effort: the trigger swallows its
        // own failures, and the scheduled sweep remains the backstop.
        await tagIndexReconcileTrigger.TriggerForTreeAsync(TreeId);

        await CompleteCoordinatorAsync();
    }

    private async Task PublishReshardCompletedAsync()
    {
        var opts = Options;
        if (!await _eventsGate.IsEnabledAsync(grainFactory, TreeId, opts)) return;
        var evt = LatticeEventPublisher.CreateEvent(LatticeTreeEventKind.ReshardCompleted, TreeId);
        await LatticeEventPublisher.PublishAsync(Context.ActivationServices, opts, evt, Logger);
    }

    private readonly PublishEventsGate _eventsGate = new();

    /// <summary>
    /// Atomically updates the <see cref="State.TreeRegistryEntry.ShardCount"/>
    /// pin for this tree, preserving every other field on the existing entry.
    /// </summary>
    private async Task UpdateShardCountPinAsync(int newShardCount)
    {
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var existing = await registry.GetEntryAsync(TreeId);
        var updated = (existing ?? new State.TreeRegistryEntry()) with { ShardCount = newShardCount };
        await registry.UpdateAsync(TreeId, updated);
    }

    /// <summary>
    /// Empty-tree fast-path for <see cref="ReshardAsync"/>: with no
    /// live entries the reshard reduces to a single registry write that
    /// updates the <see cref="State.TreeRegistryEntry.ShardCount"/> pin and
    /// rebuilds the default identity <see cref="ShardMap"/> for the new
    /// count. The grow-only restriction does not apply because no data has
    /// to be migrated.
    /// </summary>
    private async Task ApplyEmptyTreeResharAsync(ILatticeRegistry registry, int newShardCount, int virtualShardCount)
    {
        await UpdateShardCountPinAsync(newShardCount);
        var newMap = ShardMap.CreateDefault(virtualShardCount, newShardCount);
        await registry.SetShardMapAsync(TreeId, newMap);
        // Snapshot the three fields the empty-tree fast-path mutates so a
        // failing persist doesn't leak Complete=true / Phase=None /
        // TargetShardCount=N into in-memory state while disk holds the
        // pre-call values. No coordinator is active on this path, so a
        // post-throw dirty Complete=true would make IsCompleteAsync lie
        // to callers, and a subsequent ReshardAsync retry from the same
        // activation would observe TargetShardCount=newShardCount on the
        // empty-tree fast-path's `if (newShardCount != currentCount)`
        // re-evaluation. Bundled with the high-priority guarded sites
        // above per the same-grain Class B rule.
        var prevComplete = state.State.Complete;
        var prevPhase = state.State.Phase;
        var prevTargetShardCount = state.State.TargetShardCount;

        state.State.Complete = true;
        state.State.Phase = ReshardPhase.None;
        state.State.TargetShardCount = newShardCount;
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State.Complete = prevComplete;
            state.State.Phase = prevPhase;
            state.State.TargetShardCount = prevTargetShardCount;
            throw;
        }

        // Reshard activity counters: the empty-tree fast path is a
        // successful reshard (registry pin + map are atomically updated
        // to the new shard count) - count it via Initiated + Completed
        // in lockstep so dashboard sums match the coordinator-driven
        // path.
        var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId);
        var tenantTag = LatticeTenantLabel.ForTree(TreeId);
        LatticeMetrics.ShardRootReshardInitiated.Add(1, treeTag, tenantTag);
        LatticeMetrics.ShardRootReshardCompleted.Add(1, treeTag, tenantTag);
    }
}
