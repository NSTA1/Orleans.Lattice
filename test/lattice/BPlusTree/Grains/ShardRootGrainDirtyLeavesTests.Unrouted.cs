using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

public partial class ShardRootGrainDirtyLeavesTests
{
    private static (ShardRootGrain Grain, FakePersistentState<ShardRootState> State, IGrainFactory Factory)
        CreateRoutingGrain()
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shard", ShardKey));
        var state = new FakePersistentState<ShardRootState>();
        var factory = Substitute.For<IGrainFactory>();
        var options = new LatticeOptions { BackgroundDrainMaxDuration = TimeSpan.FromMinutes(1) };
        var grain = new ShardRootGrain(context, state, factory,
            TestOptionsResolver.Create(baseOptions: options, factory: factory),
            NullLogger<ShardRootGrain>.Instance, TestMutationObservers.NoObservers());
        return (grain, state, factory);
    }

    [Test]
    public async Task DiscardUnroutedDirtyLeaf_single_leaf_root_is_never_discarded()
    {
        var (grain, state, _, leafId) = CreateGrain();
        state.State.DirtyLeavesSinceLastCompaction[leafId.ToString()] = default;

        Assert.That(await grain.DiscardUnroutedDirtyLeafAsync(leafId), Is.False);
        Assert.That(state.State.DirtyLeavesSinceLastCompaction, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task DiscardUnroutedDirtyLeaf_proves_absence_without_activating_the_missing_leaf()
    {
        var (grain, state, factory) = CreateRoutingGrain();
        var rootId = GrainId.Create("internal", "root");
        var childId = GrainId.Create("internal", "child");
        var live = GrainId.Create("leaf", "live");
        var absent = GrainId.Create("leaf", "absent");
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;
        state.State.DirtyLeavesSinceLastCompaction[absent.ToString()] = default;
        state.State.DirtyLeavesSinceLastCompaction[live.ToString()] = default;
        var root = Substitute.For<IBPlusInternalGrain>();
        root.GetRoutingTableAsync().Returns(new RoutingTableSnapshot { ChildIds = [childId] });
        var child = Substitute.For<IBPlusInternalGrain>();
        child.GetRoutingTableAsync().Returns(new RoutingTableSnapshot { ChildIds = [live], ChildrenAreLeaves = true });
        factory.GetGrain<IBPlusInternalGrain>(rootId).Returns(root);
        factory.GetGrain<IBPlusInternalGrain>(childId).Returns(child);

        Assert.That(await grain.DiscardUnroutedDirtyLeafAsync(absent), Is.True);
        Assert.That(state.State.DirtyLeavesSinceLastCompaction.Keys, Is.EqualTo(new[] { live.ToString() }));
        Assert.That(state.WriteCount, Is.GreaterThan(0));
        factory.DidNotReceive().GetGrain<IBPlusLeafGrain>(absent);
        Assert.That(await grain.DiscardUnroutedDirtyLeafAsync(live), Is.False);
        Assert.That(state.State.DirtyLeavesSinceLastCompaction, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task DiscardUnroutedDirtyLeaf_empty_routing_fails_closed()
    {
        var (grain, state, factory) = CreateRoutingGrain();
        var rootId = GrainId.Create("internal", "root");
        var absent = GrainId.Create("leaf", "absent");
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;
        state.State.DirtyLeavesSinceLastCompaction[absent.ToString()] = default;
        var root = Substitute.For<IBPlusInternalGrain>();
        root.GetRoutingTableAsync().Returns(new RoutingTableSnapshot());
        factory.GetGrain<IBPlusInternalGrain>(rootId).Returns(root);

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.DiscardUnroutedDirtyLeafAsync(absent));
        Assert.That(state.State.DirtyLeavesSinceLastCompaction, Has.Count.EqualTo(1));
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task DiscardUnroutedDirtyLeaf_invalid_or_unreadable_routing_keeps_the_mark(bool cycle)
    {
        var (grain, state, factory) = CreateRoutingGrain();
        var rootId = GrainId.Create("internal", "root");
        var absent = GrainId.Create("leaf", "absent");
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;
        state.State.DirtyLeavesSinceLastCompaction[absent.ToString()] = default;
        var root = Substitute.For<IBPlusInternalGrain>();
        root.GetRoutingTableAsync().Returns(_ => cycle
            ? Task.FromResult(new RoutingTableSnapshot { ChildIds = [rootId] })
            : Task.FromException<RoutingTableSnapshot>(new InvalidOperationException("routing unavailable")));
        factory.GetGrain<IBPlusInternalGrain>(rootId).Returns(root);

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.DiscardUnroutedDirtyLeafAsync(absent));
        Assert.That(state.State.DirtyLeavesSinceLastCompaction, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task DiscardUnroutedDirtyLeaf_failed_persist_restores_the_mark()
    {
        var (grain, state, _, live) = CreateGrain();
        var absent = GrainId.Create("leaf", "absent");
        var mark = HybridLogicalClock.Tick(default);
        state.State.DirtyLeavesSinceLastCompaction[absent.ToString()] = mark;
        state.OnWriteState = _ => throw new InvalidOperationException("storage unavailable");

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.DiscardUnroutedDirtyLeafAsync(absent));
        Assert.That(state.State.DirtyLeavesSinceLastCompaction[absent.ToString()], Is.EqualTo(mark));
        Assert.That(state.State.RootNodeId, Is.EqualTo(live));
    }

    [Test]
    public async Task DiscardUnroutedDirtyLeaf_changed_routing_keeps_the_mark()
    {
        var (grain, state, factory) = CreateRoutingGrain();
        var rootId = GrainId.Create("internal", "root");
        var absent = GrainId.Create("leaf", "absent");
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;
        state.State.DirtyLeavesSinceLastCompaction[absent.ToString()] = default;
        var root = Substitute.For<IBPlusInternalGrain>();
        root.GetRoutingTableAsync().Returns(_ =>
        {
            grain.InvalidateRoutingTable(rootId);
            return new RoutingTableSnapshot { ChildIds = [GrainId.Create("leaf", "live")], ChildrenAreLeaves = true };
        });
        factory.GetGrain<IBPlusInternalGrain>(rootId).Returns(root);

        Assert.That(await grain.DiscardUnroutedDirtyLeafAsync(absent), Is.False);
        Assert.That(state.State.DirtyLeavesSinceLastCompaction, Has.Count.EqualTo(1));
    }

    [Test]
    public async Task DiscardUnroutedDirtyLeaf_incomplete_bounded_walk_keeps_the_mark()
    {
        var (grain, state, factory) = CreateRoutingGrain();
        var rootId = GrainId.Create("internal", "root");
        var absent = GrainId.Create("leaf", "absent");
        state.State.RootNodeId = rootId;
        state.State.RootIsLeaf = false;
        state.State.DirtyLeavesSinceLastCompaction[absent.ToString()] = default;
        var root = Substitute.For<IBPlusInternalGrain>();
        root.GetRoutingTableAsync().Returns(new RoutingTableSnapshot
        {
            ChildIds = Enumerable.Range(0, 4_097).Select(i => GrainId.Create("leaf", $"live-{i}")).ToArray(),
            ChildrenAreLeaves = true,
        });
        factory.GetGrain<IBPlusInternalGrain>(rootId).Returns(root);

        Assert.That(await grain.DiscardUnroutedDirtyLeafAsync(absent), Is.False);
        Assert.That(state.State.DirtyLeavesSinceLastCompaction, Has.Count.EqualTo(1));
    }
}
