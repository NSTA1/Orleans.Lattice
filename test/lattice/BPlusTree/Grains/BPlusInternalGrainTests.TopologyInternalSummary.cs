using NUnit.Framework;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for the internal-child branch of
/// <see cref="IBPlusInternalGrain.GetTopologyAsync"/>: when a node's children
/// are themselves internal nodes and the caller's depth budget is exhausted,
/// each child is summarised in place via <c>InternalSummaryNode</c> rather than
/// recursed into. Also exercises the trivial child-id / tree-id accessors.
/// </summary>
public partial class BPlusInternalGrainTests
{
    [Test]
    public async Task GetTopologyAsync_depth_zero_with_internal_children_summarises_in_place()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: false);

        var topology = await grain.GetTopologyAsync(0);

        Assert.That(topology.IsLeaf, Is.False);
        Assert.That(topology.Children.Count, Is.EqualTo(2));
        Assert.That(topology.Children, Has.All.Matches<ShardTopologyNode>(n => !n.IsLeaf),
            "an internal child summarised past the depth budget must report IsLeaf=false");
    }

    [Test]
    public async Task GetChildIdsAsync_returns_all_child_ids()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        await grain.InitializeAsync("fox", Child0, Child1, childrenAreLeaves: false);

        var ids = await grain.GetChildIdsAsync();

        Assert.That(ids, Is.EquivalentTo(new[] { Child0, Child1 }));
    }

    [Test]
    public async Task GetTreeIdAsync_returns_persisted_tree_id()
    {
        var state = new FakePersistentState<InternalNodeState>();
        var grain = CreateGrain(state);
        await grain.SetTreeIdAsync("topology-tree");

        var treeId = await grain.GetTreeIdAsync();

        Assert.That(treeId, Is.EqualTo("topology-tree"));
    }
}
