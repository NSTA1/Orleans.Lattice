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
/// inspects the current <see cref="ShardMap"/> and moves it towards the target.
/// A <b>grow</b> dispatches up to
/// <see cref="LatticeOptions.MaxConcurrentMigrations"/> per-shard
/// <see cref="ITreeShardSplitGrain.SplitAsync"/> calls against the
/// largest-slot-owning eligible shards (those owning at least two virtual
/// slots and not already splitting); every completed split atomically
/// grows the map by one distinct physical shard via its swap phase. A
/// <b>shrink</b> (<see cref="TreeReshardState.Shrinking"/>) starts up to the
/// same number of online shard consolidations
/// (<see cref="ITreeShardConsolidationGrain"/>) against the cheapest adjacent
/// shard pairs; every completed fold retires one physical shard from the map
/// and releases its storage. The next tick simply re-evaluates.</description></item>
/// <item><description><see cref="ReshardPhase.Complete"/> - target
/// reached (and, for a shrink, every fold finished); coordinator re-pins the registry's <c>ShardCount</c> to the
/// target, clears <see cref="TreeReshardState.InProgress"/>, records the
/// completion metrics, publishes the reshard-completed event when event
/// publishing is enabled, triggers a reconcile of any tag index covering the
/// tree, unregisters its keepalive, and deactivates.</description></item>
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
        var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, optionsResolver.GetMetricTreeId(TreeId));
        var tenantTag = LatticeTenantLabel.ForTree(TreeId);

        // Zero-prime every member of the rejection taxonomy before any of the five
        // sites below can arm one (issue #2918). The counter carries a bounded
        // `reason` domain, and before this only the reason that had already fired
        // existed as a series - so "no reshard was ever rejected for
        // already_in_progress" and "this build has no already_in_progress call site"
        // scraped identically, and neither could be told from "the rejection
        // counter is not wired at all". Adding zero to a counter is the identity,
        // so the arms below read exactly as they did.
        //
        // Placed above all five rejection sites, which is the whole point: a prime
        // below any one of them is unreachable on precisely the path whose absence
        // it exists to make readable. It sits BELOW the origin gate deliberately -
        // a call refused for a non-internal origin is not a reshard rejection in
        // this taxonomy and never reaches any of the five, so the population this
        // primes is exactly the population that can arm it.
        //
        // The issue filed this as unprimable because LatticeMetrics is a static
        // class with no silo-startup hook. That premise is too pessimistic: the
        // instrument is EMITTED from a grain, and the emitting grain's own entry
        // point is a lifecycle seam with all the reachability the prime needs.
        //
        // The five are written out rather than looped because the enrolment gate
        // in test/lattice/Hygiene reads zero-primed values by matching literal
        // `new KeyValuePair<string, object?>(...)` arguments on a zero-valued Add;
        // a foreach over a collection of tags is invisible to it, so a loop would
        // prime correctly at runtime and still leave the instrument classified
        // `unprimed`. Writing them out also makes each primed arm textually
        // identical to the site that arms it, so the two cannot drift into
        // different series.
        LatticeMetrics.ShardRootReshardRejected.Add(0, treeTag, new KeyValuePair<string, object?>("reason", "argument_out_of_range_min"), tenantTag);
        LatticeMetrics.ShardRootReshardRejected.Add(0, treeTag, new KeyValuePair<string, object?>("reason", "argument_out_of_range_max"), tenantTag);
        LatticeMetrics.ShardRootReshardRejected.Add(0, treeTag, new KeyValuePair<string, object?>("reason", "already_in_progress"), tenantTag);
        LatticeMetrics.ShardRootReshardRejected.Add(0, treeTag, new KeyValuePair<string, object?>("reason", "resize_in_flight"), tenantTag);
        LatticeMetrics.ShardRootReshardRejected.Add(0, treeTag, new KeyValuePair<string, object?>("reason", "state_write_failed"), tenantTag);

        LatticeMetrics.ShardRootReshardInFlight.Record(state.State.InProgress ? 1L : 0L, treeTag, tenantTag);

        if (newShardCount < 2)
        {
            LatticeMetrics.ShardRootReshardRejected.Add(1, treeTag, new KeyValuePair<string, object?>("reason", "argument_out_of_range_min"), tenantTag);
            throw new ArgumentOutOfRangeException(nameof(newShardCount),
                "Target shard count must be at least 2.");
        }

        var resolved = await optionsResolver.ResolveAsync(TreeId);

        // The ceiling is the tree's own virtual slot space, not the 4096 default:
        // an installed app can pin a smaller one, and a split needs a source that
        // owns at least two slots, so a target above the slot count can never be
        // reached and would leave the coordinator in progress indefinitely. The
        // 4096 cap still applies to a tree whose map declares more slots.
        var registry = grainFactory.GetLatticeRegistry();
        var currentMap = await registry.GetShardMapAsync(TreeId)
            ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, resolved.ShardCount);
        var virtualShardCount = currentMap.VirtualShardCount;
        var maxShardCount = Math.Min(virtualShardCount, LatticeConstants.DefaultVirtualShardCount);
        if (newShardCount > maxShardCount)
        {
            LatticeMetrics.ShardRootReshardRejected.Add(1, treeTag, new KeyValuePair<string, object?>("reason", "argument_out_of_range_max"), tenantTag);
            throw new ArgumentOutOfRangeException(nameof(newShardCount),
                $"Target shard count ({newShardCount}) cannot exceed {maxShardCount}: the tree's virtual shard space holds {virtualShardCount} slots, and no tree may exceed {LatticeConstants.DefaultVirtualShardCount} shards.");
        }

        if (state.State.InProgress)
        {
            if (state.State.TargetShardCount == newShardCount) return;
            LatticeMetrics.ShardRootReshardRejected.Add(1, treeTag, new KeyValuePair<string, object?>("reason", "already_in_progress"), tenantTag);
            throw new InvalidOperationException(
                $"A reshard is already in progress for tree '{TreeId}' (target={state.State.TargetShardCount}).");
        }

        // Inspect the current map to pick the direction.
        var currentCount = currentMap.GetPhysicalShardIndices().Count;

        // Empty-tree fast-path: if the tree has no live entries yet,
        // repin ShardCount atomically and rebuild the default identity map
        // without activating the coordinator machinery. No data has to move,
        // so a grow and a shrink are the same single registry write.
        if (newShardCount != currentCount && await IsObservablyEmptyAsync(resolved, currentMap))
        {
            await ApplyEmptyTreeResharAsync(registry, newShardCount, virtualShardCount);
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

        // A populated tree below its current count shrinks by online shard
        // consolidation - the inverse of the split a grow dispatches - folding
        // adjacent shards together until the map holds the target count.
        var shrinking = newShardCount < currentCount;

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
        var prevStartShardCount = state.State.StartShardCount;
        var prevShrinking = state.State.Shrinking;
        var prevDonors = state.State.ConsolidationDonorShardIndices;

        if (state.State.Complete) state.State.Complete = false;

        state.State.InProgress = true;
        state.State.OperationId = Guid.NewGuid().ToString("N");
        state.State.Phase = ReshardPhase.Migrating;
        state.State.TargetShardCount = newShardCount;
        state.State.StartShardCount = currentCount;
        state.State.Shrinking = shrinking;
        state.State.ConsolidationDonorShardIndices = [];
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
            state.State.StartShardCount = prevStartShardCount;
            state.State.Shrinking = prevShrinking;
            state.State.ConsolidationDonorShardIndices = prevDonors;
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

    /// <inheritdoc />
    public Task<ReshardProgress> GetProgressAsync() =>
        Task.FromResult(state.State.InProgress
            ? new ReshardProgress(true, state.State.TargetShardCount, state.State.StartShardCount)
            : new ReshardProgress(false, 0, 0));

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
        if (state.State.Shrinking)
        {
            await ConsolidateAsync();
            return;
        }

        var resolved = await optionsResolver.ResolveAsync(TreeId);
        var registry = grainFactory.GetLatticeRegistry();
        var currentMap = await registry.GetShardMapAsync(TreeId)
            ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, resolved.ShardCount);

        var physicalShards = currentMap.GetPhysicalShardIndices();
        if (physicalShards.Count >= state.State.TargetShardCount)
        {
            await EnterCompletePhaseAsync();
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
    /// Flips the persisted phase from <see cref="ReshardPhase.Migrating"/> to
    /// <see cref="ReshardPhase.Complete"/> once the target is reached.
    /// </summary>
    private async Task EnterCompletePhaseAsync()
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
    }

    /// <summary>
    /// One shrink tick: reconciles the folds this reshard started, terminates
    /// once the map holds at most the target number of physical shards with no
    /// fold still running, and otherwise starts up to
    /// <see cref="LatticeOptions.MaxConcurrentMigrations"/> new folds against
    /// the cheapest adjacent pairs. Exposed as <c>internal</c> for unit testing.
    /// <para>
    /// Each fold is an <see cref="ITreeShardConsolidationGrain"/> - the online,
    /// crash-safe inverse of the split a grow dispatches - which also releases
    /// the retired donor's storage when it commits. Waiting for every started
    /// fold to finish, not merely for the map to reach the target, is what
    /// makes a completed shrink mean the old shards have been cleaned up.
    /// </para>
    /// <para>
    /// Pair selection mirrors the healing orchestrator's: plan against the map
    /// the in-flight folds will leave, never fold a shard that is concurrently
    /// absorbing another, and record intent before starting a fold so the
    /// tracked set can over-count but never under-count.
    /// </para>
    /// </summary>
    internal async Task ConsolidateAsync()
    {
        var resolved = await optionsResolver.ResolveAsync(TreeId);
        var registry = grainFactory.GetLatticeRegistry();
        var physicalTreeId = await registry.ResolveAsync(TreeId);

        // Reconcile: drop folds that finished or were abandoned, and learn the
        // survivor of each one still running.
        var tracked = state.State.ConsolidationDonorShardIndices;
        var survivors = new List<int>(tracked.Count);
        var trackedChanged = false;
        for (var i = tracked.Count - 1; i >= 0; i--)
        {
            ShardConsolidationProgress progress;
            try
            {
                progress = await ConsolidationGrain(tracked[i]).GetProgressAsync();
            }
            catch (Exception ex)
            {
                // Unreachable this tick is not evidence the fold finished; keep
                // tracking it and plan nothing against an unknown survivor.
                Logger.LogDebug(ex,
                    "Could not read consolidation progress for donor shard {DonorShardIndex} during reshard of tree {TreeId}",
                    tracked[i], TreeId);
                survivors.Add(UnknownSurvivor);
                continue;
            }

            if (progress.InProgress)
            {
                survivors.Add(progress.SurvivorShardIndex);
                continue;
            }

            tracked.RemoveAt(i);
            trackedChanged = true;
            Logger.LogInformation(
                "Reshard of tree {TreeId} observed consolidation of shard {DonorShardIndex} into shard {SurvivorShardIndex} finish (complete={Complete}, cancelled={Cancelled})",
                TreeId, progress.DonorShardIndex, progress.SurvivorShardIndex, progress.Complete, progress.Cancelled);
        }

        // The reverse walk collected survivors in reverse tracked order.
        survivors.Reverse();
        if (trackedChanged) await PersistTrackedFoldsAsync();

        // Running folds are not driven from here: each fold's reminder-anchored
        // timer is its motor, and a fold's swap and finalise steps - including
        // the release of its donor's storage - are deliberately unbounded, so
        // driving them inline would hold this coordinator's turn, and with it
        // every IsReshardCompleteAsync poll, for as long as they take.

        var map = await registry.GetShardMapAsync(TreeId)
            ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, resolved.ShardCount);
        var physicalCount = map.GetPhysicalShardIndices().Count;

        if (physicalCount <= state.State.TargetShardCount)
        {
            // Every fold that started must also have finished - including the
            // release of its donor's storage - before the reshard reports done.
            if (tracked.Count == 0) await EnterCompletePhaseAsync();
            return;
        }

        var maxConcurrent = Math.Max(1, resolved.MaxConcurrentMigrations);

        // A fold removes its donor from the map at its swap, well before it
        // finishes, so only the tracked folds whose donor the map still
        // references are reductions still to come.
        var physicalShards = map.GetPhysicalShardIndices();
        var pendingReductions = 0;
        foreach (var donor in tracked)
        {
            if (IndexOfAscending(physicalShards, donor) >= 0) pendingReductions++;
        }

        var needed = physicalCount - state.State.TargetShardCount - pendingReductions;
        var budget = Math.Min(maxConcurrent - tracked.Count, needed);
        if (budget <= 0) return;

        if (survivors.Contains(UnknownSurvivor)) return;

        // Folds automatic healing admitted before this reshard began are still
        // driven by their own coordinators. Wait them out rather than plan
        // around pairs this coordinator cannot see.
        if (await HasHealingFoldInFlightAsync(physicalTreeId)) return;

        // A snapshot or merge reads the shards it recorded at its start, and a
        // fold releases its donor's storage when it commits. Start no fold
        // while one runs, as healing stands off them; a resize cannot run
        // alongside a reshard at all.
        if (await IsSnapshotOrMergeInFlightAsync()) return;

        var reserved = new HashSet<int>(tracked);
        foreach (var survivor in survivors) reserved.Add(survivor);
        var planningMap = tracked.Count == 0 ? map : ProjectFolds(map, tracked, survivors);

        for (var started = 0; started < budget; started++)
        {
            if (!ShardConsolidationPlanner.TryPlanNext(planningMap, out var plan)) return;

            // The projection hides an in-flight fold's donor but not its
            // survivor; a pair touching a shard already part of a fold waits for
            // the next tick, when that fold has committed.
            if (reserved.Contains(plan.DonorShardIndex) || reserved.Contains(plan.SurvivorShardIndex)) return;

            tracked.Add(plan.DonorShardIndex);
            await PersistTrackedFoldsAsync();

            try
            {
                await ConsolidationGrain(plan.DonorShardIndex).StartAsync(plan.SurvivorShardIndex);
            }
            catch (InvalidOperationException ex)
            {
                // A split or another fold holds one side of the pair. Transient;
                // un-record the intent and re-plan next tick.
                tracked.Remove(plan.DonorShardIndex);
                await PersistTrackedFoldsAsync();
                Logger.LogDebug(ex,
                    "Could not start consolidation of shard {DonorShardIndex} into shard {SurvivorShardIndex} during reshard of tree {TreeId}",
                    plan.DonorShardIndex, plan.SurvivorShardIndex, TreeId);
                return;
            }

            Logger.LogInformation(
                "Reshard of tree {TreeId} started consolidation of shard {DonorShardIndex} into shard {SurvivorShardIndex} ({SlotCount} virtual slots)",
                TreeId, plan.DonorShardIndex, plan.SurvivorShardIndex, plan.DonorSlots.Length);

            reserved.Add(plan.DonorShardIndex);
            reserved.Add(plan.SurvivorShardIndex);
            planningMap = ProjectFolds(planningMap, [plan.DonorShardIndex], [plan.SurvivorShardIndex]);
        }
    }

    /// <summary>
    /// Sentinel survivor for a tracked fold whose coordinator could not be read.
    /// Distinct from every real physical shard index, which is non-negative.
    /// </summary>
    private const int UnknownSurvivor = -1;

    /// <summary>
    /// The consolidation coordinator for <paramref name="donorShardIndex"/>,
    /// keyed by the logical tree id exactly as the grow path keys its split
    /// coordinators, so it reads and reassigns the same routing map.
    /// </summary>
    private ITreeShardConsolidationGrain ConsolidationGrain(int donorShardIndex)
        => grainFactory.GetGrain<ITreeShardConsolidationGrain>($"{TreeId}/{donorShardIndex}");

    /// <summary>
    /// Whether automatic healing still has a fold running on this tree. Reads
    /// the orchestrator's tracked donors and asks each donor's coordinator,
    /// because the orchestrator's own record can over-count.
    /// </summary>
    private async Task<bool> HasHealingFoldInFlightAsync(string physicalTreeId)
    {
        var donors = await grainFactory.GetGrain<IShardHealingOrchestratorGrain>(TreeId)
            .GetInFlightDonorShardIndicesAsync();
        foreach (var donor in donors)
        {
            var fold = grainFactory.GetGrain<ITreeShardConsolidationGrain>($"{physicalTreeId}/{donor}");
            if (!await fold.IsIdleAsync()) return true;
        }

        return false;
    }

    /// <summary>
    /// Whether a snapshot of, or a merge into, this tree is running. Read through
    /// the tree's own status verbs under a system-origin scope, as the healing
    /// orchestrator's stand-off reads them. Deliberately never asks
    /// <see cref="ILattice.IsReshardCompleteAsync"/>, which would call back into
    /// this coordinator.
    /// </summary>
    private async Task<bool> IsSnapshotOrMergeInFlightAsync()
    {
        var lattice = grainFactory.GetGrain<ILattice>(TreeId);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            return !await lattice.IsSnapshotCompleteAsync()
                || !await lattice.IsMergeCompleteAsync();
        }
    }

    /// <summary>
    /// Builds the routing map the tree will have once each listed fold commits,
    /// by re-pointing every donor's slots onto its survivor. Pairs whose
    /// survivor is unknown are skipped.
    /// </summary>
    private static ShardMap ProjectFolds(ShardMap map, IReadOnlyList<int> donors, IReadOnlyList<int> survivors)
    {
        var projected = (int[])map.Slots.Clone();
        var pairs = Math.Min(donors.Count, survivors.Count);
        for (var i = 0; i < pairs; i++)
        {
            var donor = donors[i];
            var survivor = survivors[i];
            if (survivor == UnknownSurvivor) continue;
            for (var slot = 0; slot < projected.Length; slot++)
            {
                if (projected[slot] == donor) projected[slot] = survivor;
            }
        }

        return new ShardMap { Slots = projected, Version = map.Version };
    }

    /// <summary>
    /// Persists the tracked fold set. A failure propagates: the intent record
    /// is what bounds concurrency and holds the shrink open, so a fold must not
    /// be started when its intent could not be written.
    /// </summary>
    private Task PersistTrackedFoldsAsync() => state.WriteStateAsync();

    /// <summary>
    /// Counts how many virtual slots each physical shard owns, returning the
    /// counts aligned to <paramref name="physicalShards"/> ordinals - that is,
    /// the result at index <c>i</c> is the slot count for
    /// <c>physicalShards[i]</c>.
    /// </summary>
    /// <remarks>
    /// Physical shard indices form a small, dense, non-negative domain bounded
    /// by the tree's pinned physical shard count (64 by default, and at most
    /// <see cref="LatticeOptions.MaxPhysicalShardsPerTree"/> - 256 by default -
    /// under autonomic splitting) while <paramref name="slots"/> spans the virtual slot
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
        var registry = grainFactory.GetLatticeRegistry();
        return await registry.ResolveAsync(TreeId);
    }

    /// <summary>
    /// Re-pins the registry's structural shard count to the target, clears
    /// in-progress state, marks the reshard complete, records the completion
    /// metrics, publishes the reshard-completed event (when enabled), triggers
    /// a tag-index reconcile for the tree, unregisters the keepalive, and
    /// deactivates. Exposed as <c>internal</c> for unit testing.
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
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, optionsResolver.GetMetricTreeId(TreeId)),
            new KeyValuePair<string, object?>(LatticeMetrics.TagKind, "reshard"),
            LatticeTenantLabel.ForTree(TreeId));

        // Reshard activity counter: coordinator-driven reshard
        // completed successfully.
        LatticeMetrics.ShardRootReshardCompleted.Add(1, new KeyValuePair<string, object?>(LatticeMetrics.TagTree, optionsResolver.GetMetricTreeId(TreeId)), LatticeTenantLabel.ForTree(TreeId));

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
    /// Fails closed on a tree with no registry row rather than creating one
    /// (issue #4230): <see cref="ReshardAsync"/> registers a never-created tree
    /// through its options resolve, so a missing row here means it was purged.
    /// </summary>
    private async Task UpdateShardCountPinAsync(int newShardCount)
    {
        var registry = grainFactory.GetLatticeRegistry();
        var existing = await registry.GetEntryAsync(TreeId)
            ?? throw new LatticeTreeNotRegisteredException(TreeId, nameof(ReshardAsync));
        var updated = existing with { ShardCount = newShardCount };
        await registry.UpdateAsync(TreeId, updated);
    }

    /// <summary>
    /// Empty-tree fast-path for <see cref="ReshardAsync"/>: with no
    /// live entries the reshard reduces to a single registry write that
    /// updates the <see cref="State.TreeRegistryEntry.ShardCount"/> pin and
    /// rebuilds the default identity <see cref="ShardMap"/> for the new
    /// count over the tree's existing virtual slot count. With no data to
    /// move, a grow and a shrink are the same single registry write.
    /// </summary>
    private async Task ApplyEmptyTreeResharAsync(ILatticeRegistry registry, int newShardCount, int virtualShardCount)
    {
        var newMap = ShardMap.CreateDefault(virtualShardCount, newShardCount);

        // The identity map routes to indices 0..n-1, which may include shards a
        // shrink retired. Return them to service before any router can be
        // handed the map, or every operation on their slots would be refused as
        // stale routing. The tree is observably empty, so they hold nothing.
        var physicalTreeId = await ResolvePhysicalTreeIdAsync();
        var indices = newMap.GetPhysicalShardIndices();
        var ownedSlots = new List<int>[indices.Count];
        for (var i = 0; i < ownedSlots.Length; i++) ownedSlots[i] = [];
        for (var slot = 0; slot < newMap.Slots.Length; slot++)
        {
            var ordinal = IndexOfAscending(indices, newMap.Slots[slot]);
            if (ordinal >= 0) ownedSlots[ordinal].Add(slot);
        }

        await BoundedFanOut.RunAsync(indices.Count, BoundedFanOut.DefaultWidth, i =>
            grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{indices[i]}")
                .ReviveAsync([.. ownedSlots[i]], virtualShardCount));

        await UpdateShardCountPinAsync(newShardCount);
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
        var treeTag = new KeyValuePair<string, object?>(LatticeMetrics.TagTree, optionsResolver.GetMetricTreeId(TreeId));
        var tenantTag = LatticeTenantLabel.ForTree(TreeId);
        LatticeMetrics.ShardRootReshardInitiated.Add(1, treeTag, tenantTag);
        LatticeMetrics.ShardRootReshardCompleted.Add(1, treeTag, tenantTag);
    }
}
