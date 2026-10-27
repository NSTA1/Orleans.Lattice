using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public sealed partial class ShardRootGrainPurgeTests
{
    [Test]
    public async Task Purge_reuses_topology_reads_and_uses_one_clear_call_per_chain_leaf()
    {
        var h = new PurgeHarness();
        var left = h.Leaf("left", compose: false);
        var right = h.Leaf("right", compose: false);
        var child = h.Internal("child", childrenAreLeaves: true, [left.Id, right.Id]);
        var root = h.Internal("root", childrenAreLeaves: false, [child.Id]);
        h.State.State.RootNodeId = root.Id;
        h.State.State.RootIsLeaf = false;

        // Raw substitutes count actual RPC seams, not calls made by composition.
        left.Grain.ClearGrainStateAndGetNextSiblingAsync(true).Returns(right.Id);
        right.Grain.ClearGrainStateAndGetNextSiblingAsync(true).Returns((GrainId?)null);
        left.Grain.ClearReceivedCalls();
        right.Grain.ClearReceivedCalls();
        root.Grain.ClearReceivedCalls();
        child.Grain.ClearReceivedCalls();

        await h.Grain.PurgeAsync();

        await root.Grain.Received(1).GetRoutingTableAsync();
        await child.Grain.Received(1).GetRoutingTableAsync();
        await left.Grain.Received(1).ClearGrainStateAndGetNextSiblingAsync(true);
        await right.Grain.Received(1).ClearGrainStateAndGetNextSiblingAsync(true);
        await left.Grain.DidNotReceive().GetNextSiblingAsync();
        await right.Grain.DidNotReceive().GetNextSiblingAsync();
        await left.Grain.DidNotReceive().ClearGrainStateForPurgeAsync();
        await right.Grain.DidNotReceive().ClearGrainStateForPurgeAsync();
    }
}
