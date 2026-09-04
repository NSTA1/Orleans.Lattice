using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="ShardRootGrain.PurgeAsync"/> - the destructive
/// offline sweep that <c>PurgeTreeAsync</c> drives once a tree has been
/// deleted. The sweep walks the leaf sibling chain clearing every leaf, then
/// clears every internal node collected by the pre-walk, and finally clears
/// the shard-root row itself.
/// <para>
/// The internal-rooted shapes are the ones a flat-tree fixture never reaches:
/// the leftmost-leaf descent, the depth-first internal-node collection, and
/// the issue-899 case where a corrupt <c>RootIsLeaf</c> flag sits over an
/// internal root and must still purge the whole subtree rather than treating
/// the internal root as a leaf.
/// </para>
/// </summary>
[TestFixture]
public sealed class ShardRootGrainPurgeTests
{
    private const string ShardKey = "purge-tree/0";

    [Test]
    public async Task PurgeAsync_clears_shard_row_when_shard_has_no_root()
    {
        var harness = new PurgeHarness();

        await harness.Grain.PurgeAsync();

        Assert.That(harness.State.State.RootNodeId, Is.Null);
    }

    [Test]
    public async Task PurgeAsync_clears_the_single_root_leaf_when_tree_is_flat()
    {
        var harness = new PurgeHarness();
        var leaf = harness.Leaf("L0", nextSibling: null);
        harness.State.State.RootNodeId = leaf.Id;
        harness.State.State.RootIsLeaf = true;

        await harness.Grain.PurgeAsync();

        await leaf.Grain.Received(1).ClearGrainStateAsync();
        Assert.That(harness.State.State.RootNodeId, Is.Null);
    }

    [Test]
    public async Task PurgeAsync_walks_leaf_chain_and_clears_every_internal_node_for_internal_root()
    {
        // Two-level topology:
        //   I0 (children are internal) -> [I1, I2]
        //   I1 (children are leaves)   -> [L0, L1]
        //   I2 (children are leaves)   -> [L2]
        // The leaf sibling chain is L0 -> L1 -> L2 -> null.
        var harness = new PurgeHarness();
        var l0 = harness.Leaf("L0");
        var l1 = harness.Leaf("L1");
        var l2 = harness.Leaf("L2");
        PurgeHarness.Chain(l0, l1, l2);

        var i1 = harness.Internal("I1", childrenAreLeaves: true, children: [l0.Id, l1.Id]);
        var i2 = harness.Internal("I2", childrenAreLeaves: true, children: [l2.Id]);
        var i0 = harness.Internal("I0", childrenAreLeaves: false, children: [i1.Id, i2.Id]);

        harness.State.State.RootNodeId = i0.Id;
        harness.State.State.RootIsLeaf = false;

        await harness.Grain.PurgeAsync();

        // Every leaf on the chain was cleared, in chain order.
        await l0.Grain.Received(1).ClearGrainStateAsync();
        await l1.Grain.Received(1).ClearGrainStateAsync();
        await l2.Grain.Received(1).ClearGrainStateAsync();

        // The depth-first pre-walk collected the root and both of its
        // internal children, and each was cleared exactly once.
        await i0.Grain.Received(1).ClearGrainStateAsync();
        await i1.Grain.Received(1).ClearGrainStateAsync();
        await i2.Grain.Received(1).ClearGrainStateAsync();

        Assert.That(harness.State.State.RootNodeId, Is.Null);
    }

    [Test]
    public async Task PurgeAsync_collects_internal_nodes_depth_first_across_three_levels()
    {
        // Three levels, so the collector's push-children branch runs on more
        // than the root: I0 -> [I1] -> [I2] -> [L0].
        var harness = new PurgeHarness();
        var l0 = harness.Leaf("L0", nextSibling: null);
        var i2 = harness.Internal("I2", childrenAreLeaves: true, children: [l0.Id]);
        var i1 = harness.Internal("I1", childrenAreLeaves: false, children: [i2.Id]);
        var i0 = harness.Internal("I0", childrenAreLeaves: false, children: [i1.Id]);

        harness.State.State.RootNodeId = i0.Id;
        harness.State.State.RootIsLeaf = false;

        await harness.Grain.PurgeAsync();

        await l0.Grain.Received(1).ClearGrainStateAsync();
        await i0.Grain.Received(1).ClearGrainStateAsync();
        await i1.Grain.Received(1).ClearGrainStateAsync();
        await i2.Grain.Received(1).ClearGrainStateAsync();
    }

    [Test]
    public async Task PurgeAsync_purges_internal_subtree_when_RootIsLeaf_flag_is_corrupt()
    {
        // Issue 899: a baked-inconsistent topology can leave RootIsLeaf true
        // over an internal root. Deciding by node TYPE rather than by the flag
        // means the purge still walks the internal subtree and the leaf chain
        // instead of blind-casting the internal root to IBPlusLeafGrain.
        var harness = new PurgeHarness(resolveLeafGrainType: true);
        var l0 = harness.Leaf("L0", nextSibling: null);
        var i0 = harness.Internal("I0", childrenAreLeaves: true, children: [l0.Id]);

        harness.State.State.RootNodeId = i0.Id;
        harness.State.State.RootIsLeaf = true;

        await harness.Grain.PurgeAsync();

        await l0.Grain.Received(1).ClearGrainStateAsync();
        await i0.Grain.Received(1).ClearGrainStateAsync();
        Assert.That(harness.State.State.RootNodeId, Is.Null);
    }

    /// <summary>
    /// Builds a directly-constructed <see cref="ShardRootGrain"/> over a
    /// substituted grain factory, plus helpers for registering leaf and
    /// internal node substitutes under stable grain ids.
    /// </summary>
    private sealed class PurgeHarness
    {
        public PurgeHarness(bool resolveLeafGrainType = false)
        {
            var context = Substitute.For<IGrainContext>();
            context.GrainId.Returns(GrainId.Create("shard", ShardKey));

            if (resolveLeafGrainType)
            {
                // TryGetLeafGrainType probes the factory for a leaf reference
                // and reads its grain type. Only a substitute that also
                // implements IGrainBase can answer GetGrainId(), so without
                // this the leaf type stays unresolved and every id is treated
                // as a leaf (the historical fake-factory behaviour).
                var probeContext = Substitute.For<IGrainContext>();
                probeContext.GrainId.Returns(GrainId.Create(LeafGrainType, "probe"));
                var probe = Substitute.For<IBPlusLeafGrain, IGrainBase>();
                ((IGrainBase)probe).GrainContext.Returns(probeContext);
                Factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(probe);
            }

            Grain = new ShardRootGrain(
                context,
                State,
                Factory,
                TestOptionsResolver.Create(baseOptions: new LatticeOptions(), factory: Factory),
                NullLogger<ShardRootGrain>.Instance,
                TestMutationObservers.NoObservers());
        }

        private const string LeafGrainType = "leaf";

        public IGrainFactory Factory { get; } = Substitute.For<IGrainFactory>();

        public FakePersistentState<ShardRootState> State { get; } = new();

        public ShardRootGrain Grain { get; }

        public LeafNode Leaf(string key, GrainId? nextSibling = null)
        {
            var id = GrainId.Create(LeafGrainType, $"{ShardKey}:{key}");
            var grain = Substitute.For<IBPlusLeafGrain>();
            grain.GetNextSiblingAsync().Returns(Task.FromResult(nextSibling));
            grain.ClearGrainStateAsync().Returns(Task.CompletedTask);
            Factory.GetGrain<IBPlusLeafGrain>(id).Returns(grain);
            return new LeafNode(id, grain);
        }

        /// <summary>Links leaves into a sibling chain terminated by <c>null</c>.</summary>
        public static void Chain(params LeafNode[] leaves)
        {
            for (var i = 0; i < leaves.Length; i++)
            {
                var next = i + 1 < leaves.Length ? leaves[i + 1].Id : (GrainId?)null;
                leaves[i].Grain.GetNextSiblingAsync().Returns(Task.FromResult(next));
            }
        }

        public InternalNode Internal(string key, bool childrenAreLeaves, IReadOnlyList<GrainId> children)
        {
            var id = GrainId.Create("internal", $"{ShardKey}:{key}");
            var grain = Substitute.For<IBPlusInternalGrain>();
            grain.AreChildrenLeavesAsync().Returns(Task.FromResult(childrenAreLeaves));
            grain.GetChildIdsAsync().Returns(_ => Task.FromResult(new List<GrainId>(children)));
            grain.GetRoutingTableAsync().Returns(Task.FromResult(new RoutingTableSnapshot
            {
                SeparatorKeys = new string?[children.Count],
                ChildIds = [.. children],
                ChildrenAreLeaves = childrenAreLeaves,
            }));
            grain.ClearGrainStateAsync().Returns(Task.CompletedTask);
            Factory.GetGrain<IBPlusInternalGrain>(id).Returns(grain);
            return new InternalNode(id, grain);
        }

        public readonly record struct LeafNode(GrainId Id, IBPlusLeafGrain Grain);

        public readonly record struct InternalNode(GrainId Id, IBPlusInternalGrain Grain);
    }
}
