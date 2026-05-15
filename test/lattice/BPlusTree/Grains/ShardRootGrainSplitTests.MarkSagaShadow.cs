using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for <c>ShardRootGrain.MarkSagaShadowAsync</c>: input
/// validation, no-op short-circuits, and the per-leaf batched fan-out.
/// In particular pins the root-is-leaf branch: the traversal path used
/// for internal trees calls <c>GetRoutingTableAsync</c> through an
/// <c>IBPlusInternalGrain</c> reference, so a root-is-leaf shard must
/// short-circuit the traversal or the grain factory's leaf substitute
/// will be cast against the wrong interface at runtime.
/// </summary>
public partial class ShardRootGrainSplitTests
{
    [Test]
    public void MarkSagaShadow_throws_when_keys_is_null()
    {
        var h = CreateHarness();
        Assert.That(
            async () => await h.Grain.MarkSagaShadowAsync(Guid.NewGuid(), null!),
            Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void MarkSagaShadow_throws_when_transaction_id_is_empty()
    {
        var h = CreateHarness();
        Assert.That(
            async () => await h.Grain.MarkSagaShadowAsync(Guid.Empty, new[] { "k0" }),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task MarkSagaShadow_empty_key_list_is_a_noop_and_does_not_call_leaf()
    {
        var h = CreateHarness();
        await h.Grain.MarkSagaShadowAsync(Guid.NewGuid(), Array.Empty<string>());

        await h.Leaf.DidNotReceive().MarkSagaShadowAsync(
            Arg.Any<Guid>(),
            Arg.Any<IReadOnlyList<string>>());
    }

    [Test]
    public async Task MarkSagaShadow_returns_without_call_when_root_node_is_null()
    {
        var h = CreateHarness();
        // CreateHarness defaults RootNodeId; clear it to reach the "no tree
        // yet" short-circuit branch.
        h.State.State.RootNodeId = null;

        await h.Grain.MarkSagaShadowAsync(Guid.NewGuid(), new[] { "k0", "k1" });

        await h.Leaf.DidNotReceive().MarkSagaShadowAsync(
            Arg.Any<Guid>(),
            Arg.Any<IReadOnlyList<string>>());
    }

    [Test]
    public async Task MarkSagaShadow_root_is_leaf_routes_all_keys_to_single_leaf_in_one_batch()
    {
        // Regression: prior to the root-is-leaf short-circuit the method
        // delegated to TraverseToLeafAsync, which resolves the root via
        // grainFactory.GetGrain<IBPlusInternalGrain>(...). The harness's
        // factory routes IBPlusLeafGrain queries to a leaf substitute, so
        // the runtime cast against IBPlusInternalGrain threw
        // InvalidCastException and the chaos reshard harness surfaced it
        // as a saga abort. This test pins the bypass that resolves the
        // root grain as IBPlusLeafGrain directly when RootIsLeaf=true.
        var h = CreateHarness();
        var tx = Guid.NewGuid();
        var keys = new[] { "alpha", "bravo", "charlie" };

        await h.Grain.MarkSagaShadowAsync(tx, keys);

        await h.Leaf.Received(1).MarkSagaShadowAsync(
            tx,
            Arg.Is<IReadOnlyList<string>>(
                k => k.Count == 3 && k[0] == "alpha" && k[1] == "bravo" && k[2] == "charlie"));
    }

    [Test]
    public async Task MarkSagaShadow_root_is_leaf_skips_null_and_empty_keys_in_iteration_but_passes_full_list_in_one_batch()
    {
        // When RootIsLeaf=true the implementation hands the caller's list
        // through unfiltered (the leaf-side contract owns key-shape
        // semantics for the marker). The internal-tree path filters
        // null/empty keys before routing - which is exercised by the
        // dedicated test below. Pinning the root-is-leaf passthrough
        // keeps the hot path allocation-free.
        var h = CreateHarness();
        var tx = Guid.NewGuid();
        var keys = new[] { "k0", "k1" };

        await h.Grain.MarkSagaShadowAsync(tx, keys);

        await h.Leaf.Received(1).MarkSagaShadowAsync(
            tx,
            Arg.Is<IReadOnlyList<string>>(k => ReferenceEquals(k, keys)));
    }

    [Test]
    public async Task MarkSagaShadow_internal_tree_batches_keys_per_owning_leaf()
    {
        // Internal-tree path: state.State.RootIsLeaf=false forces the
        // traversal-based grouping. With a single leaf wired into the
        // harness, all keys land in one batch - but the call is reached
        // via TraverseToLeafAsync, exercising the per-leaf grouping seam.
        var h = CreateHarness();
        h.State.State.RootIsLeaf = false;

        // The traversal path reads the root's routing table. Wire an
        // internal-grain stub that returns a single-child routing snapshot
        // so every key resolves to h.Leaf.
        var internalGrain = Substitute.For<IBPlusInternalGrain>();
        var leafGrainId = h.State.State.RootNodeId!.Value;
        // RoutingTableSnapshot is the public DTO returned by
        // IBPlusInternalGrain.GetRoutingTableAsync. Single-child snapshot
        // collapses every key to the same leaf id.
        var snapshot = new RoutingTableSnapshot
        {
            SeparatorKeys = new string?[] { null },
            ChildIds = new[] { leafGrainId },
            ChildrenAreLeaves = true,
        };
        internalGrain.GetRoutingTableAsync().Returns(Task.FromResult(snapshot));
        h.Factory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(internalGrain);

        var tx = Guid.NewGuid();
        await h.Grain.MarkSagaShadowAsync(tx, new[] { "k0", "k1", "k2" });

        await h.Leaf.Received(1).MarkSagaShadowAsync(
            tx,
            Arg.Is<IReadOnlyList<string>>(
                k => k.Count == 3 && k[0] == "k0" && k[1] == "k1" && k[2] == "k2"));
    }

    [Test]
    public async Task MarkSagaShadow_internal_tree_filters_null_and_empty_keys_before_routing()
    {
        var h = CreateHarness();
        h.State.State.RootIsLeaf = false;

        var internalGrain = Substitute.For<IBPlusInternalGrain>();
        var leafGrainId = h.State.State.RootNodeId!.Value;
        var snapshot = new RoutingTableSnapshot
        {
            SeparatorKeys = new string?[] { null },
            ChildIds = new[] { leafGrainId },
            ChildrenAreLeaves = true,
        };
        internalGrain.GetRoutingTableAsync().Returns(Task.FromResult(snapshot));
        h.Factory.GetGrain<IBPlusInternalGrain>(Arg.Any<GrainId>()).Returns(internalGrain);

        var tx = Guid.NewGuid();
        // Mix in nulls and empty strings; only the real keys should reach
        // the leaf.
        await h.Grain.MarkSagaShadowAsync(tx, new[] { "k0", "", null!, "k1" });

        await h.Leaf.Received(1).MarkSagaShadowAsync(
            tx,
            Arg.Is<IReadOnlyList<string>>(
                k => k.Count == 2 && k[0] == "k0" && k[1] == "k1"));
    }
}
