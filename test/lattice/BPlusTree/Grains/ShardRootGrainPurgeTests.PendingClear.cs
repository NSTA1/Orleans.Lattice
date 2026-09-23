using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #2207 on the purge path: a purge must remove the state of every leaf
/// the shard still owns, including leaves no walk can reach - those owed a clear
/// by an earlier reclaim or orphan repair, and those stranded past a chain
/// that a failed-then-retried purge has already broken.
/// </summary>
public sealed partial class ShardRootGrainPurgeTests
{
    [Test]
    public async Task PurgeAsync_clears_leaves_still_owed_a_clear_by_an_earlier_removal()
    {
        var harness = new PurgeHarness();
        var root = harness.Leaf("L0", nextSibling: null);
        var owed = harness.Leaf("folded", nextSibling: null);
        harness.State.State.RootNodeId = root.Id;
        harness.State.State.RootIsLeaf = true;
        harness.State.State.PendingLeafClears.Add(owed.Id);

        await harness.Grain.PurgeAsync();

        await owed.Grain.Received(1).ClearGrainStateAsync();
        await root.Grain.Received(1).ClearGrainStateAsync();
        Assert.That(harness.State.State.PendingLeafClears, Is.Empty,
            "the shard row, and with it the record, is cleared only after every owed leaf");
    }

    [Test]
    public async Task PurgeAsync_clears_owed_leaves_even_when_the_shard_has_no_root()
    {
        var harness = new PurgeHarness();
        var owed = harness.Leaf("folded", nextSibling: null);
        harness.State.State.PendingLeafClears.Add(owed.Id);

        await harness.Grain.PurgeAsync();

        await owed.Grain.Received(1).ClearGrainStateAsync();
    }

    [Test]
    public async Task PurgeAsync_keeps_the_owed_record_when_an_owed_clear_fails()
    {
        // The failure propagates so the tree-deletion retry re-runs the purge.
        // The shard row must survive it, or the retry would have no record of
        // the owed leaf and would strand its state for good.
        var harness = new PurgeHarness();
        var owed = harness.Leaf("folded", nextSibling: null);
        owed.Grain.ClearGrainStateAsync()
            .Returns(_ => Task.FromException(new InvalidOperationException("storage unavailable")));
        harness.State.State.PendingLeafClears.Add(owed.Id);
        var rootId = harness.Leaf("L0", nextSibling: null).Id;
        harness.State.State.RootNodeId = rootId;
        harness.State.State.RootIsLeaf = true;

        Assert.That(async () => await harness.Grain.PurgeAsync(),
            Throws.InstanceOf<InvalidOperationException>());

        Assert.Multiple(() =>
        {
            Assert.That(harness.State.State.PendingLeafClears, Is.EqualTo(new[] { owed.Id }));
            Assert.That(harness.State.State.RootNodeId, Is.EqualTo(rootId));
        });
    }

    [Test]
    public async Task A_retried_purge_clears_routed_leaves_past_a_chain_the_first_attempt_broke()
    {
        // I1 routes [L0, L1, L2]. A first purge cleared L0 and then failed, so
        // L0 now has no sibling pointer: the retry's chain walk stops at L0.
        // The routing table still names L1 and L2 (internal nodes are cleared
        // last), and the purge must reach them through it.
        var harness = new PurgeHarness();
        var l0 = harness.Leaf("L0", nextSibling: null);
        var l1 = harness.Leaf("L1");
        var l2 = harness.Leaf("L2");
        PurgeHarness.Chain(l1, l2);

        var i1 = harness.Internal("I1", childrenAreLeaves: true, children: [l0.Id, l1.Id, l2.Id]);
        harness.State.State.RootNodeId = i1.Id;
        harness.State.State.RootIsLeaf = false;

        await harness.Grain.PurgeAsync();

        await l0.Grain.Received(1).ClearGrainStateAsync();
        await l1.Grain.Received(1).ClearGrainStateAsync();
        await l2.Grain.Received(1).ClearGrainStateAsync();
        await i1.Grain.Received(1).ClearGrainStateAsync();
    }

    [Test]
    public async Task A_first_attempt_purge_does_not_clear_a_chained_leaf_twice()
    {
        // Falsifier for the test above: on an intact chain the routed-leaf
        // sweep finds nothing the walk missed, so each leaf is cleared once.
        var harness = new PurgeHarness();
        var l0 = harness.Leaf("L0");
        var l1 = harness.Leaf("L1");
        PurgeHarness.Chain(l0, l1);
        var i1 = harness.Internal("I1", childrenAreLeaves: true, children: [l0.Id, l1.Id]);
        harness.State.State.RootNodeId = i1.Id;
        harness.State.State.RootIsLeaf = false;

        await harness.Grain.PurgeAsync();

        await l0.Grain.Received(1).ClearGrainStateAsync();
        await l1.Grain.Received(1).ClearGrainStateAsync();
        await l1.Grain.Received(1).GetNextSiblingAsync();
    }
}
