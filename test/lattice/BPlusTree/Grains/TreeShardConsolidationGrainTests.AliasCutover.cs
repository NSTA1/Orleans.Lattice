using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// A fold in flight across an alias cutover must not apply its slot diff to
/// the map the cutover carried onto the logical entry (issue #4264). The fold
/// drains and swaps the physical tree it started on, so its diff names shard
/// indices of that tree, not of the copy the logical id now resolves to.
/// </summary>
public partial class TreeShardConsolidationGrainTests
{
    private const string CutoverCopyTreeId = "consolidation-test-tree-copy";

    private static void ArrangeCutoverTo(Harness h, string physicalTreeId)
    {
        h.Registry.ResolveAsync(TreeId).Returns(physicalTreeId);
        h.Registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { ShardCount = 2, PhysicalTreeId = physicalTreeId }));
    }

    private static FakePersistentState<TreeShardConsolidationState> BoundInFlightState(ShardConsolidationPhase phase)
    {
        var state = InFlightState(phase);
        state.State.PhysicalTreeId = TreeId;
        return state;
    }

    [Test]
    public async Task Swap_after_an_alias_cutover_leaves_the_routing_map_untouched_and_abandons_the_fold()
    {
        var h = CreateGrain(existingState: BoundInFlightState(ShardConsolidationPhase.Swap));
        var before = (int[])h.PersistedMap!.Slots.Clone();
        ArrangeCutoverTo(h, CutoverCopyTreeId);

        await h.Grain.SwapAsync();

        Assert.That(h.PersistedMap!.Slots, Is.EqualTo(before).AsCollection);
        Assert.That(h.Log.Entries, Does.Not.Contain("registry.ReassignSlots"));
        Assert.That(h.Log.Entries, Does.Not.Contain("survivor.ReclaimSlots"),
            "The survivor's seal must not be lifted for a fold that will never commit.");
        Assert.That(h.Log.Entries, Does.Not.Contain("donor.EnterReject"));
        Assert.That(h.Log.Entries, Does.Contain("donor.AbortSplit"));
        Assert.That(h.State.State.InProgress, Is.False);
        Assert.That(h.State.State.Complete, Is.False);
        Assert.That(h.State.State.Cancelled, Is.False, "Abandoned by a cutover, not by a cancel.");
    }

    [Test]
    public async Task Swap_while_a_cutover_has_carried_the_copy_map_but_not_swapped_the_alias_does_not_apply_the_diff()
    {
        var h = CreateGrain(existingState: BoundInFlightState(ShardConsolidationPhase.Swap));
        h.Registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { ShardCount = 2, AliasCutoverTarget = CutoverCopyTreeId }));

        await h.Grain.SwapAsync();

        Assert.That(h.Log.Entries, Does.Not.Contain("registry.ReassignSlots"));
        Assert.That(h.State.State.InProgress, Is.False);
    }

    [Test]
    public async Task Swap_abandons_without_advancing_when_the_registry_refuses_the_fenced_diff()
    {
        var h = CreateGrain(existingState: BoundInFlightState(ShardConsolidationPhase.Swap));
        h.Registry.ReassignSlotsAsync(TreeId, Arg.Any<int[]>(), Arg.Any<int>(), Arg.Any<ShardMap>(), TreeId)
            .Returns(Task.FromResult<ShardMap?>(null));

        await h.Grain.SwapAsync();

        Assert.That(h.State.State.InProgress, Is.False);
        Assert.That(h.State.State.Phase, Is.EqualTo(ShardConsolidationPhase.None),
            "A refused fold must not advance to Reject as though its diff had committed.");
        Assert.That(h.Log.Entries, Does.Contain("donor.EnterReject"));
        Assert.That(h.Log.Entries, Does.Not.Contain("donor.AbortSplit"));
    }

    [Test]
    public async Task Drain_tick_after_a_cutover_abandons_instead_of_draining_the_copy()
    {
        var h = CreateGrain(existingState: BoundInFlightState(ShardConsolidationPhase.Drain));
        ArrangeCutoverTo(h, CutoverCopyTreeId);

        await h.Grain.ProcessNextPhaseAsync();

        h.Factory.DidNotReceive().GetGrain<IShardRootGrain>($"{CutoverCopyTreeId}/1");
        Assert.That(h.Log.Entries, Does.Contain("donor.AbortSplit"));
        Assert.That(h.State.State.InProgress, Is.False);
    }

    [Test]
    public async Task Initiate_binds_the_fold_to_the_physical_tree_the_logical_id_resolves_to()
    {
        var h = CreateGrain();

        await h.Grain.InitiateConsolidationStateAsync(1, 0);

        Assert.That(h.State.State.PhysicalTreeId, Is.EqualTo(TreeId));
    }

    [Test]
    public void Initiate_is_refused_while_a_cutover_is_between_its_map_carry_and_its_alias_swap()
    {
        var h = CreateGrain();
        h.Registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { ShardCount = 2, AliasCutoverTarget = CutoverCopyTreeId }));

        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.InitiateConsolidationStateAsync(1, 0));

        Assert.That(h.State.State.InProgress, Is.False);
        Assert.That(h.Log.Entries, Does.Not.Contain("donor.BeginSplit"));
    }
}
