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
/// <item><description><see cref="ReshardPhase.Planning"/> — persist the
/// target shard count and transition to
/// <see cref="ReshardPhase.Migrating"/>.</description></item>
/// <item><description><see cref="ReshardPhase.Migrating"/> — each tick
/// inspects the current <see cref="ShardMap"/>, counts distinct physical
/// shards, and — while below target — dispatches up to
/// <see cref="LatticeOptions.MaxConcurrentMigrations"/> per-shard
/// <see cref="ITreeShardSplitGrain.SplitAsync"/> calls against the
/// largest-slot-owning eligible shards (those owning at least two virtual
/// slots and not already splitting). Every completed split atomically
/// grows the map by one distinct physical shard via its swap phase; the
/// next tick simply re-evaluates.</description></item>
/// <item><description><see cref="ReshardPhase.Complete"/> — target
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
        if (newShardCount < 2)
            throw new ArgumentOutOfRangeException(nameof(newShardCount),
                "Target shard count must be at least 2.");

        var resolved = await optionsResolver.ResolveAsync(TreeId);
        if (newShardCount > LatticeConstants.DefaultVirtualShardCount)
            throw new ArgumentOutOfRangeException(nameof(newShardCount),
                $"Target shard count ({newShardCount}) cannot exceed the virtual shard space ({LatticeConstants.DefaultVirtualShardCount}).");

        if (state.State.InProgress)
        {
            if (state.State.TargetShardCount == newShardCount) return;
            throw new InvalidOperationException(
                $"A reshard is already in progress for tree '{TreeId}' (target={state.State.TargetShardCount}).");
        }

        // Inspect the current map to validate grow-only semantics.
        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var currentMap = await registry.GetShardMapAsync(TreeId)
            ?? ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, resolved.ShardCount);
        var currentCount = currentMap.GetPhysicalShardIndices().Count;

        // Empty-tree fast-path: if the tree has no live entries yet,
        // repin ShardCount atomically and rebuild the default identity map
        // without activating the coordinator machinery. This also relaxes
        // the grow-only restriction so callers (including test fixtures)
        // can set any desired shard count on a freshly-created tree.
        if (newShardCount != currentCount)
        {
            var lattice = grainFactory.GetGrain<ILattice>(TreeId);
            var liveCount = await lattice.CountAsync();
            if (liveCount == 0)
            {
                await ApplyEmptyTreeResharAsync(registry, newShardCount, LatticeConstants.DefaultVirtualShardCount);
                return;
            }
        }

        if (newShardCount <= currentCount)
            throw new ArgumentOutOfRangeException(nameof(newShardCount),
                $"Target shard count ({newShardCount}) must be greater than current count ({currentCount}). Shrink is not supported.");

        // Interlock: refuse to start a reshard while a resize is in flight.
        // Resize crosses physical trees; concurrent ShardMap mutation on the
        // source would invalidate the resize snapshot's per-slot routing
        // assumptions. Checked after argument validation so that callers
        // providing invalid parameters always receive an argument exception.
        var resize = grainFactory.GetGrain<ITreeResizeGrain>(TreeId);
        if (!await resize.IsIdleAsync())
            throw new InvalidOperationException(
                $"A resize is already in progress for tree '{TreeId}'; reshard refused until resize completes.");

        if (state.State.Complete) state.State.Complete = false;

        state.State.InProgress = true;
        state.State.OperationId = Guid.NewGuid().ToString("N");
        state.State.Phase = ReshardPhase.Migrating;
        state.State.TargetShardCount = newShardCount;
        await state.WriteStateAsync();

        await StartCoordinatorAsync();
    }

    /// <inheritdoc />
    public async Task RunReshardPassAsync()
    {
        if (!state.State.InProgress) return;

        if (state.State.Phase == ReshardPhase.Planning)
        {
            state.State.Phase = ReshardPhase.Migrating;
            await state.WriteStateAsync();
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
                state.State.Phase = ReshardPhase.Migrating;
                await state.WriteStateAsync();
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
            ?? ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, resolved.ShardCount);

        var physicalShards = currentMap.GetPhysicalShardIndices();
        if (physicalShards.Count >= state.State.TargetShardCount)
        {
            state.State.Phase = ReshardPhase.Complete;
            await state.WriteStateAsync();
            return;
        }

        // Count virtual-slot ownership per physical shard.
        var slotCounts = new Dictionary<int, int>(physicalShards.Count);
        foreach (var idx in physicalShards) slotCounts[idx] = 0;
        foreach (var slot in currentMap.Slots) slotCounts[slot]++;

        // Filter to eligible sources: owns ≥ 2 slots AND is not already
        // splitting. Splits-in-flight are counted separately and reduce the
        // remaining dispatch budget so we do not over-dispatch.
        var physicalTreeId = await ResolvePhysicalTreeIdAsync();
        var splittingTasks = new List<Task<bool>>(physicalShards.Count);
        var splittingIndices = new List<int>(physicalShards.Count);
        foreach (var idx in physicalShards)
        {
            if (slotCounts[idx] < 2) continue;
            var shard = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{idx}");
            splittingTasks.Add(shard.IsSplittingAsync());
            splittingIndices.Add(idx);
        }
        await Task.WhenAll(splittingTasks);

        var inFlight = 0;
        var eligible = new List<(int Shard, int Slots)>(splittingIndices.Count);
        for (int i = 0; i < splittingIndices.Count; i++)
        {
            if (splittingTasks[i].Result) { inFlight++; continue; }
            eligible.Add((splittingIndices[i], slotCounts[splittingIndices[i]]));
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
            // source owns fewer than two slots — skip this shard and let the
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

        state.State.InProgress = false;
        state.State.Complete = true;
        state.State.Phase = ReshardPhase.None;
        await state.WriteStateAsync();

        LatticeMetrics.CoordinatorCompleted.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, TreeId),
            new KeyValuePair<string, object?>(LatticeMetrics.TagKind, "reshard"));

        await PublishReshardCompletedAsync();

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

        state.State.Complete = true;
        state.State.Phase = ReshardPhase.None;
        state.State.TargetShardCount = newShardCount;
        await state.WriteStateAsync();
    }
}
