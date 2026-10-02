using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A split in flight across an alias cutover must not apply its slot diff to
/// the map the cutover carried onto the logical entry (issue #4264). The split
/// drains and swaps the physical tree it started on, so its moved-slot diff
/// names shard indices of that tree; applied to the copy's map it would route
/// the moved slots to a shard the copy never populated.
/// </summary>
public partial class TreeShardSplitGrainTests
{
    private const string CutoverCopyTreeId = "split-test-tree-copy";

    private static void ArrangeMidSplitState(FakePersistentState<TreeShardSplitState> state, ShardSplitPhase phase)
    {
        state.State.InProgress = true;
        state.State.Phase = phase;
        state.State.SourceShardIndex = 0;
        state.State.TargetShardIndex = 2;
        state.State.MovedSlots = [2, 4, 6];
        state.State.OriginalShardMap = ShardMap.CreateDefault(8, 2);
    }

    private static void ArrangeCutoverTo(ILatticeRegistry registry, string physicalTreeId)
    {
        registry.ResolveAsync(TreeId).Returns(physicalTreeId);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { ShardCount = 2, PhysicalTreeId = physicalTreeId }));
    }

    private static async Task AssertNoSlotDiffAppliedAsync(ILatticeRegistry registry)
    {
        await registry.DidNotReceiveWithAnyArgs().ReassignSlotsAsync(default!, default!, default, default!);
        await registry.DidNotReceive().ReassignSlotsAsync(
            Arg.Any<string>(), Arg.Any<int[]>(), Arg.Any<int>(), Arg.Any<ShardMap>(), Arg.Any<string>());
        await registry.DidNotReceive().SetShardMapAsync(Arg.Any<string>(), Arg.Any<ShardMap>());
    }

    [Test]
    public async Task Swap_after_an_alias_cutover_does_not_apply_the_slot_diff_to_the_logical_map()
    {
        var (grain, state, _, registry, source, _) = CreateGrain();
        ArrangeMidSplitState(state, ShardSplitPhase.Drain);

        // The drain binds the split to the physical tree the logical id resolves
        // to now; the cutover then re-points the logical id at a copy.
        await grain.DrainAsync();
        Assert.That(state.State.Phase, Is.EqualTo(ShardSplitPhase.Swap), "Precondition: the drain finished.");
        ArrangeCutoverTo(registry, CutoverCopyTreeId);

        await grain.SwapAsync();

        await AssertNoSlotDiffAppliedAsync(registry);
        Assert.That(state.State.InProgress, Is.False, "The split must be abandoned, not left in flight.");
        Assert.That(state.State.Complete, Is.False);
        Assert.That(state.State.Phase, Is.EqualTo(ShardSplitPhase.None));
        // Detected before the freeze, so the replaced tree's source is restored
        // rather than sealed: an undo of the cutover finds it unchanged.
        await source.DidNotReceive().MarkLeavesMovedAwayAsync(Arg.Any<int[]>(), Arg.Any<int>());
        await source.DidNotReceive().EnterRejectPhaseAsync();
        await source.Received(1).AbortSplitAsync();
    }

    [Test]
    public async Task Swap_while_a_cutover_has_carried_the_copy_map_but_not_swapped_the_alias_does_not_apply_the_diff()
    {
        var (grain, state, _, registry, source, _) = CreateGrain();
        ArrangeMidSplitState(state, ShardSplitPhase.Swap);
        state.State.PhysicalTreeId = TreeId;

        // The map carry and the alias swap are two registry calls: in between,
        // the logical entry still resolves to the bound tree but its map is the
        // copy's.
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { ShardCount = 2, AliasCutoverTarget = CutoverCopyTreeId }));

        await grain.SwapAsync();

        await AssertNoSlotDiffAppliedAsync(registry);
        Assert.That(state.State.InProgress, Is.False);
        await source.Received(1).AbortSplitAsync();
    }

    [Test]
    public async Task Swap_abandons_without_unwinding_the_freeze_when_the_registry_refuses_the_fenced_diff()
    {
        var (grain, state, _, registry, source, _) = CreateGrain();
        ArrangeMidSplitState(state, ShardSplitPhase.Swap);
        state.State.PhysicalTreeId = TreeId;

        // The cutover lands after the pre-check, so the registry's own fence -
        // checked inside the same call as the write - is what refuses it.
        registry.ReassignSlotsAsync(TreeId, Arg.Any<int[]>(), Arg.Any<int>(), Arg.Any<ShardMap>(), TreeId)
            .Returns(Task.FromResult<ShardMap?>(null));

        await grain.SwapAsync();

        await registry.Received(1).ReassignSlotsAsync(
            TreeId, Arg.Any<int[]>(), 2, Arg.Any<ShardMap>(), TreeId);
        await registry.DidNotReceiveWithAnyArgs().ReassignSlotsAsync(default!, default!, default, default!);
        Assert.That(state.State.InProgress, Is.False);
        Assert.That(state.State.Phase, Is.EqualTo(ShardSplitPhase.None),
            "A refused split must not advance to Reject as though its diff had committed.");
        await source.Received(1).EnterRejectPhaseAsync();
        await source.DidNotReceive().AbortSplitAsync();
    }

    [Test]
    public async Task Swap_abandons_even_when_the_source_was_already_frozen_by_an_interrupted_swap()
    {
        var (grain, state, _, registry, source, _) = CreateGrain();
        ArrangeMidSplitState(state, ShardSplitPhase.Swap);
        state.State.PhysicalTreeId = TreeId;
        ArrangeCutoverTo(registry, CutoverCopyTreeId);
        source.AbortSplitAsync().Returns(Task.FromException(new InvalidOperationException("already in Reject")));

        await grain.SwapAsync();

        await AssertNoSlotDiffAppliedAsync(registry);
        Assert.That(state.State.InProgress, Is.False);
    }

    [Test]
    public async Task Split_resumed_on_a_new_activation_after_a_cutover_stays_bound_to_the_tree_it_started_on()
    {
        var existing = new FakePersistentState<TreeShardSplitState>();
        ArrangeMidSplitState(existing, ShardSplitPhase.Drain);
        existing.State.PhysicalTreeId = TreeId;

        var (grain, state, grainFactory, registry, _, _) = CreateGrain(existingState: existing);
        ArrangeCutoverTo(registry, CutoverCopyTreeId);

        await grain.ProcessNextPhaseAsync();

        // Re-resolving the logical id would have drained the copy's shards and
        // later applied the diff to the copy's map.
        grainFactory.DidNotReceive().GetGrain<IShardRootGrain>($"{CutoverCopyTreeId}/0");
        grainFactory.Received().GetGrain<IShardRootGrain>($"{TreeId}/0");
        await AssertNoSlotDiffAppliedAsync(registry);
        Assert.That(state.State.InProgress, Is.False);
    }

    [Test]
    public async Task RunSplitPass_after_a_cutover_abandons_instead_of_driving_the_split()
    {
        var (grain, state, _, registry, source, _) = CreateGrain();
        ArrangeMidSplitState(state, ShardSplitPhase.Drain);
        state.State.PhysicalTreeId = TreeId;
        ArrangeCutoverTo(registry, CutoverCopyTreeId);

        await grain.RunSplitPassAsync();

        await AssertNoSlotDiffAppliedAsync(registry);
        await source.DidNotReceive().CompleteSplitAsync();
        Assert.That(state.State.InProgress, Is.False);
        Assert.That(state.State.Complete, Is.False);
    }

    [Test]
    public async Task InitiateSplit_binds_the_split_to_the_physical_tree_the_logical_id_resolves_to()
    {
        var (grain, state, _, registry, _, _) = CreateGrain();
        const string physical = "split-test-tree-physical";
        registry.ResolveAsync(TreeId).Returns(physical);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { ShardCount = 2, PhysicalTreeId = physical }));

        await grain.InitiateSplitStateAsync(0);

        Assert.That(state.State.PhysicalTreeId, Is.EqualTo(physical));
    }

    [Test]
    public void InitiateSplit_is_refused_while_a_cutover_is_between_its_map_carry_and_its_alias_swap()
    {
        var (grain, state, _, registry, source, _) = CreateGrain();
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { ShardCount = 2, AliasCutoverTarget = CutoverCopyTreeId }));

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.InitiateSplitStateAsync(0));

        Assert.That(state.State.InProgress, Is.False);
        registry.DidNotReceiveWithAnyArgs().AllocateNextShardIndexAsync(default!, default);
        source.DidNotReceiveWithAnyArgs().BeginSplitAsync(default, default!, default);
    }

    [Test]
    public async Task A_new_split_rebinds_to_the_current_physical_tree_rather_than_a_previous_splits()
    {
        var existing = new FakePersistentState<TreeShardSplitState>();
        existing.State.Complete = true;
        existing.State.PhysicalTreeId = "a-retired-physical-tree";
        var (grain, state, _, _, _, _) = CreateGrain(existingState: existing);

        await grain.InitiateSplitStateAsync(0);

        Assert.That(state.State.PhysicalTreeId, Is.EqualTo(TreeId));
    }
}
