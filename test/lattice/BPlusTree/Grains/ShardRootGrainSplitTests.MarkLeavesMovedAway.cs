using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for <c>ShardRootGrain.MarkLeavesMovedAwayAsync</c>: input
/// validation, no-op short-circuits, and the leaf-chain walk that
/// propagates the moved-slot set to every leaf under the shard.
/// </summary>
public partial class ShardRootGrainSplitTests
{
    [Test]
    public void MarkLeavesMovedAway_throws_when_slots_is_null()
    {
        var h = CreateHarness();
        Assert.That(
            async () => await h.Grain.MarkLeavesMovedAwayAsync(null!, VirtualShardCount),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void MarkLeavesMovedAway_throws_when_vsc_non_positive()
    {
        var h = CreateHarness();
        Assert.That(
            async () => await h.Grain.MarkLeavesMovedAwayAsync(new[] { 0, 2 }, 0),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
        Assert.That(
            async () => await h.Grain.MarkLeavesMovedAwayAsync(new[] { 0, 2 }, -1),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task MarkLeavesMovedAway_empty_array_returns_zero_and_does_not_call_leaf()
    {
        var h = CreateHarness();
        var result = await h.Grain.MarkLeavesMovedAwayAsync(Array.Empty<int>(), VirtualShardCount);

        Assert.That(result, Is.EqualTo(0));
        await h.Leaf.DidNotReceive().MarkSlotsMovedAwayAsync(Arg.Any<int[]>(), Arg.Any<int>());
    }

    [Test]
    public async Task MarkLeavesMovedAway_returns_zero_when_root_node_is_null()
    {
        var h = CreateHarness();
        // The harness default-assigns RootNodeId; reset it after construction
        // to reach the "no tree yet" short-circuit branch.
        h.State.State.RootNodeId = null;

        var result = await h.Grain.MarkLeavesMovedAwayAsync(new[] { 0, 2 }, VirtualShardCount);

        Assert.That(result, Is.EqualTo(0));
        await h.Leaf.DidNotReceive().MarkSlotsMovedAwayAsync(Arg.Any<int[]>(), Arg.Any<int>());
    }

    [Test]
    public async Task MarkLeavesMovedAway_root_is_leaf_marks_exactly_one_leaf()
    {
        var h = CreateHarness();
        // CreateHarness defaults RootIsLeaf=true with RootNodeId set.
        h.Leaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));

        var slots = new[] { 0, 2, 4 };
        var result = await h.Grain.MarkLeavesMovedAwayAsync(slots, VirtualShardCount);

        Assert.That(result, Is.EqualTo(1));
        await h.Leaf.Received(1).MarkSlotsMovedAwayAsync(
            Arg.Is<int[]>(a => a.Length == 3 && a[0] == 0 && a[1] == 2 && a[2] == 4),
            VirtualShardCount);
    }

    [Test]
    public async Task MarkLeavesMovedAway_walks_full_leaf_chain_via_GetNextSibling()
    {
        // Wire three leaves linked in a chain: leaf0 -> leaf1 -> leaf2 -> null.
        // The harness factory routes every GetGrain<IBPlusLeafGrain>(...) call
        // to the same stub, so GetNextSiblingAsync only needs to yield the
        // chain of GrainIds on successive calls; the stub is reused for every
        // leaf and MarkSlotsMovedAwayAsync should fire once per chain step.
        var h = CreateHarness();
        var leaf1Id = GrainId.Create("leaf", "leaf-1");
        var leaf2Id = GrainId.Create("leaf", "leaf-2");

        h.Leaf.GetNextSiblingAsync().Returns(
            Task.FromResult<GrainId?>(leaf1Id),
            Task.FromResult<GrainId?>(leaf2Id),
            Task.FromResult<GrainId?>(null));

        var result = await h.Grain.MarkLeavesMovedAwayAsync(new[] { 0, 4 }, VirtualShardCount);

        Assert.That(result, Is.EqualTo(3));
        await h.Leaf.Received(3).MarkSlotsMovedAwayAsync(
            Arg.Is<int[]>(a => a.SequenceEqual(new[] { 0, 4 })),
            VirtualShardCount);
    }

    [Test]
    public async Task MarkLeavesMovedAway_propagates_exact_vsc_to_every_leaf()
    {
        var h = CreateHarness();
        h.Leaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(null));

        const int customVsc = 32;
        await h.Grain.MarkLeavesMovedAwayAsync(new[] { 7 }, customVsc);

        await h.Leaf.Received(1).MarkSlotsMovedAwayAsync(
            Arg.Is<int[]>(a => a.Length == 1 && a[0] == 7),
            customVsc);
    }
}