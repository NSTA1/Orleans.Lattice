using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for <see cref="ShardRootGrain.FenceMovedSlotsAsync"/>: the durable
/// moved-away fence the empty-tree reshard fast path records on a slot's
/// previous owner before the new shard map is published (#4066).
/// </summary>
[TestFixture]
public class ShardRootGrainFenceTests
{
    private const string TreeId = "fence-tree";
    private const int VirtualShardCount = 16;

    private sealed class Harness
    {
        public required ShardRootGrain Grain { get; init; }
        public required IBPlusLeafGrain Leaf { get; init; }
        public required FakePersistentState<ShardRootState> State { get; init; }
    }

    private static Harness CreateHarness()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", $"{TreeId}/1"));

        var state = new FakePersistentState<ShardRootState>();
        state.State.RootNodeId = GrainId.Create("leaf", "leaf-0");
        state.State.RootIsLeaf = true;

        var factory = Substitute.For<IGrainFactory>();
        var optionsResolver = TestOptionsResolver.Create(
            baseOptions: new LatticeOptions(), shardCount: 2, factory: factory);

        var leaf = Substitute.For<IBPlusLeafGrain>();
        leaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(leaf);

        var grain = new ShardRootGrain(
            context,
            state,
            factory,
            optionsResolver,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<ShardRootGrain>.Instance,
            TestMutationObservers.NoObservers());

        return new Harness { Grain = grain, Leaf = leaf, State = state };
    }

    [Test]
    public async Task FenceMovedSlots_records_each_slot_against_its_new_owner_and_persists()
    {
        var h = CreateHarness();

        await h.Grain.FenceMovedSlotsAsync([5, 1], [3, 2], VirtualShardCount);

        Assert.That(h.State.State.MovedAwaySlots, Is.EquivalentTo(new Dictionary<int, int> { [1] = 2, [5] = 3 }));
        Assert.That(h.State.State.MovedAwayVirtualShardCount, Is.EqualTo(VirtualShardCount));
        Assert.That(h.State.WriteCount, Is.EqualTo(1));
    }

    [Test]
    public async Task FenceMovedSlots_marks_the_leaves_with_the_slots_in_ascending_order()
    {
        var h = CreateHarness();

        await h.Grain.FenceMovedSlotsAsync([9, 2, 5], [1, 1, 1], VirtualShardCount);

        await h.Leaf.Received(1).MarkSlotsMovedAwayAsync(
            Arg.Is<int[]>(s => s.SequenceEqual(new[] { 2, 5, 9 })),
            VirtualShardCount);
    }

    [Test]
    public async Task FenceMovedSlots_does_not_reorder_the_callers_array()
    {
        var h = CreateHarness();
        int[] slots = [9, 2, 5];

        await h.Grain.FenceMovedSlotsAsync(slots, [1, 1, 1], VirtualShardCount);

        Assert.That(slots, Is.EqualTo(new[] { 9, 2, 5 }));
    }

    [Test]
    public async Task FenceMovedSlots_repeat_does_not_rewrite_state_but_marks_leaves_again()
    {
        var h = CreateHarness();

        await h.Grain.FenceMovedSlotsAsync([1, 5], [2, 3], VirtualShardCount);
        await h.Grain.FenceMovedSlotsAsync([1, 5], [2, 3], VirtualShardCount);

        Assert.That(h.State.WriteCount, Is.EqualTo(1));
        await h.Leaf.Received(2).MarkSlotsMovedAwayAsync(Arg.Any<int[]>(), VirtualShardCount);
    }

    [Test]
    public async Task FenceMovedSlots_under_the_same_slot_space_merges_with_the_existing_fence()
    {
        var h = CreateHarness();

        await h.Grain.FenceMovedSlotsAsync([1], [2], VirtualShardCount);
        await h.Grain.FenceMovedSlotsAsync([7], [3], VirtualShardCount);

        Assert.That(h.State.State.MovedAwaySlots, Is.EquivalentTo(new Dictionary<int, int> { [1] = 2, [7] = 3 }));
        Assert.That(h.State.WriteCount, Is.EqualTo(2));
    }

    [Test]
    public async Task FenceMovedSlots_with_a_changed_owner_rewrites_that_slot()
    {
        var h = CreateHarness();

        await h.Grain.FenceMovedSlotsAsync([1], [2], VirtualShardCount);
        await h.Grain.FenceMovedSlotsAsync([1], [4], VirtualShardCount);

        Assert.That(h.State.State.MovedAwaySlots, Is.EquivalentTo(new Dictionary<int, int> { [1] = 4 }));
        Assert.That(h.State.WriteCount, Is.EqualTo(2));
    }

    [Test]
    public async Task FenceMovedSlots_under_a_different_slot_space_replaces_the_fence()
    {
        var h = CreateHarness();
        h.State.State.MovedAwaySlots[1] = 0;
        h.State.State.MovedAwayVirtualShardCount = 8;

        await h.Grain.FenceMovedSlotsAsync([12], [3], VirtualShardCount);

        Assert.That(h.State.State.MovedAwaySlots, Is.EquivalentTo(new Dictionary<int, int> { [12] = 3 }));
        Assert.That(h.State.State.MovedAwayVirtualShardCount, Is.EqualTo(VirtualShardCount));
        Assert.That(h.State.WriteCount, Is.EqualTo(1));
    }

    [Test]
    public async Task FenceMovedSlots_with_no_slots_writes_nothing_and_marks_no_leaf()
    {
        var h = CreateHarness();

        await h.Grain.FenceMovedSlotsAsync([], [], VirtualShardCount);

        Assert.That(h.State.WriteCount, Is.Zero);
        Assert.That(h.State.State.MovedAwayVirtualShardCount, Is.Null);
        await h.Leaf.DidNotReceive().MarkSlotsMovedAwayAsync(Arg.Any<int[]>(), Arg.Any<int>());
    }

    [Test]
    public async Task FenceMovedSlots_restores_the_previous_fence_when_the_write_fails()
    {
        var h = CreateHarness();
        await h.Grain.FenceMovedSlotsAsync([1], [2], VirtualShardCount);
        h.State.ThrowOnWrite = new TimeoutException("storage down");

        Assert.ThrowsAsync<TimeoutException>(
            () => h.Grain.FenceMovedSlotsAsync([7], [3], VirtualShardCount));

        Assert.That(h.State.State.MovedAwaySlots, Is.EquivalentTo(new Dictionary<int, int> { [1] = 2 }));
        Assert.That(h.State.State.MovedAwayVirtualShardCount, Is.EqualTo(VirtualShardCount));
        await h.Leaf.Received(1).MarkSlotsMovedAwayAsync(Arg.Any<int[]>(), Arg.Any<int>());
    }

    [Test]
    public void FenceMovedSlots_rejects_null_slots()
    {
        var h = CreateHarness();

        var ex = Assert.ThrowsAsync<ArgumentNullException>(
            () => h.Grain.FenceMovedSlotsAsync(null!, [1], VirtualShardCount));

        Assert.That(ex!.ParamName, Is.EqualTo("slots"));
    }

    [Test]
    public void FenceMovedSlots_rejects_null_owners()
    {
        var h = CreateHarness();

        var ex = Assert.ThrowsAsync<ArgumentNullException>(
            () => h.Grain.FenceMovedSlotsAsync([1], null!, VirtualShardCount));

        Assert.That(ex!.ParamName, Is.EqualTo("newOwners"));
    }

    [Test]
    public void FenceMovedSlots_rejects_mismatched_lengths()
    {
        var h = CreateHarness();

        var ex = Assert.ThrowsAsync<ArgumentException>(
            () => h.Grain.FenceMovedSlotsAsync([1, 2], [1], VirtualShardCount));

        Assert.That(ex!.ParamName, Is.EqualTo("newOwners"));
    }

    [TestCase(0)]
    [TestCase(-1)]
    public void FenceMovedSlots_rejects_a_non_positive_slot_space(int virtualShardCount)
    {
        var h = CreateHarness();

        var ex = Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => h.Grain.FenceMovedSlotsAsync([0], [1], virtualShardCount));

        Assert.That(ex!.ParamName, Is.EqualTo("virtualShardCount"));
    }

    [TestCase(-1)]
    [TestCase(VirtualShardCount)]
    public void FenceMovedSlots_rejects_a_slot_outside_the_slot_space(int slot)
    {
        var h = CreateHarness();

        var ex = Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => h.Grain.FenceMovedSlotsAsync([slot], [1], VirtualShardCount));

        Assert.That(ex!.ParamName, Is.EqualTo("slots"));
        Assert.That(h.State.WriteCount, Is.Zero);
    }

    [Test]
    public void FenceMovedSlots_rejects_a_negative_owner()
    {
        var h = CreateHarness();

        var ex = Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => h.Grain.FenceMovedSlotsAsync([1], [-1], VirtualShardCount));

        Assert.That(ex!.ParamName, Is.EqualTo("newOwners"));
        Assert.That(h.State.WriteCount, Is.Zero);
    }
}
