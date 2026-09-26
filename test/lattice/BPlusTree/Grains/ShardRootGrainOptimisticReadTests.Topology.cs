using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public sealed partial class ShardRootGrainOptimisticReadTests
{
    [TestCase("split", false)]
    [TestCase("split", true)]
    [TestCase("seal", false)]
    [TestCase("seal", true)]
    [TestCase("merge", false)]
    [TestCase("merge", true)]
    [TestCase("retire", true)]
    public async Task In_flight_read_detects_real_leaf_topology_change_below_unchanged_internal_root(
        string mutation, bool absent)
    {
        var (grain, root, _, _, leafProxy) = CreateGrainWithInternalRoot();
        root.GetRoutingTableAsync().Returns(new RoutingTableSnapshot
        {
            SeparatorKeys = [null], ChildIds = [RightLeafId], ChildrenAreLeaves = true,
        });
        var state = new FakePersistentState<LeafNodeState>();
        state.State.TreeId = "optimistic-tree";
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(RightLeafId);
        var factory = Substitute.For<IGrainFactory>();
        var sibling = Substitute.For<IBPlusLeafGrain, IGrainBase>();
        var siblingContext = Substitute.For<IGrainContext>();
        siblingContext.GrainId.Returns(GrainId.Create("leaf", "ownership-split-sibling"));
        ((IGrainBase)sibling).GrainContext.Returns(siblingContext);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(sibling);
        factory.GetGrain<IBPlusLeafGrain>(Arg.Any<Guid>()).Returns(sibling);
        var leaf = new BPlusLeafGrain(context, state, factory,
            TestOptionsResolver.Create(maxLeafKeys: 4, factory: factory),
            TestMutationObservers.NoObservers(), TestOriginClusterIdResolver.Default());
        if (mutation != "retire")
            foreach (var key in new[] { "a", "b", "c", "d" })
                await leaf.SetAsync(key, [1]);
        if (mutation == "merge")
            await leaf.SetKeyRangeAsync(null, "m");
        var readKey = absent ? "aa" : "a";
        leafProxy.GetWithVersionAsync(readKey).Returns(_ => leaf.GetWithVersionAsync(readKey));
        await grain.TryGetOptimisticAsync(readKey);
        await grain.GetAsync(readKey);
        Assert.That((await grain.TryGetOptimisticAsync(readKey)).IsValidated, Is.True);
        var epoch = grain.RoutingEpoch;

        var reply = new TaskCompletionSource<VersionedValue>(TaskCreationOptions.RunContinuationsAsynchronously);
        leafProxy.GetWithVersionAsync(readKey).Returns(reply.Task);
        var read = grain.TryGetOptimisticAsync(readKey);
        Assert.That(read.IsCompleted, Is.False);
        switch (mutation)
        {
            case "split":
                Assert.That(await leaf.SetAsync("e", [1]), Is.Not.Null);
                break;
            case "seal":
                await leaf.MarkSlotsMovedAwayAsync([ShardMap.GetVirtualSlot(readKey, 64)], 64);
                break;
            case "merge":
                await leaf.AbsorbSuccessorRangeAsync(null);
                break;
            case "retire":
                Assert.That(await leaf.TryBeginOrphanRetirementAsync(), Is.True);
                break;
        }
        reply.SetResult(await leaf.GetWithVersionAsync(readKey));
        Assert.That((await read).IsValidated, Is.False);
        Assert.That(grain.RoutingEpoch, Is.EqualTo(epoch),
            "This race must be detected by the leaf, not by a shard-root epoch change.");
    }
}
