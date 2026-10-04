using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #4452, split side: an adaptive split must not
/// start while a resize of the tree is in flight, and one that opened its
/// source record after a resize started must back out. A split that commits
/// during a resize routes its moved slots to a target the resize never copies
/// or fences (the shard-ownership spec's UniqueOwnerSplitDuringResize).
/// </summary>
public partial class TreeShardSplitGrainTests
{
    [Test]
    public async Task SplitAsync_refuses_while_a_completed_resize_still_has_the_replaced_copy_mirroring()
    {
        // The soft-delete window: the resize coordinator is idle (the resize
        // completed), but the copy it replaced still mirrors into the resized
        // copy until the purge, so a split there moves keys that mirror cannot
        // follow and loses an acknowledged write (shard-ownership spec,
        // NoKeyLost under the relaxed interlock).
        var (grain, state, grainFactory, registry, source, _) = CreateGrain();
        var resize = grainFactory.StubResizeIdle();
        resize.HoldsShardMigrationsAsync().Returns(Task.FromResult(true));

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.SplitAsync(0));

        await registry.DidNotReceiveWithAnyArgs().AllocateNextShardIndexAsync(default!, default);
        await source.DidNotReceiveWithAnyArgs().BeginSplitAsync(default, default!, default);
        Assert.That(state.State.InProgress, Is.False);
    }

    [Test]
    public async Task SplitAsync_refuses_while_a_resize_of_the_tree_is_in_flight()
    {
        var (grain, state, grainFactory, registry, source, _) = CreateGrain();
        grainFactory.StubResizeIdle().HoldsShardMigrationsAsync().Returns(Task.FromResult(true));

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => grain.SplitAsync(0));

        Assert.That(ex!.Message, Does.Contain("resize of the tree is in progress"));
        await registry.DidNotReceiveWithAnyArgs().AllocateNextShardIndexAsync(default!, default);
        await source.DidNotReceiveWithAnyArgs().BeginSplitAsync(default, default!, default);
        Assert.That(state.State.InProgress, Is.False);
    }

    [Test]
    public async Task InitiateSplit_backs_out_when_a_resize_is_in_flight_once_the_source_record_is_open()
    {
        // The race the pre-check cannot close: the resize read the shards'
        // migration records before this split opened its record, so only the
        // read after opening it can see the resize.
        var (grain, state, grainFactory, _, source, _) = CreateGrain();
        grainFactory.StubResizeIdle().HoldsShardMigrationsAsync().Returns(Task.FromResult(true));

        Assert.ThrowsAsync<InvalidOperationException>(() => grain.InitiateSplitStateAsync(0));

        Received.InOrder(() =>
        {
            source.BeginSplitAsync(Arg.Any<int>(), Arg.Any<int[]>(), Arg.Any<int>());
            source.AbortSplitAsync();
        });
        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.False);
            Assert.That(state.State.Phase, Is.Not.EqualTo(ShardSplitPhase.Drain));
        });
        await source.DidNotReceive().GetLeftmostLeafIdAsync();
    }

    [Test]
    public async Task A_split_resumed_before_its_drain_abandons_when_a_resize_is_in_flight()
    {
        var (grain, state, grainFactory, _, source, _) = CreateGrain(
            existingState: new FakePersistentState<TreeShardSplitState>
            {
                State = new TreeShardSplitState
                {
                    InProgress = true,
                    Phase = ShardSplitPhase.BeginShadowWrite,
                    OperationId = "resumed",
                    SourceShardIndex = 0,
                    TargetShardIndex = 2,
                    MovedSlots = [8, 10, 12, 14],
                    OriginalShardMap = ShardMap.CreateDefault(16, 2),
                    PhysicalTreeId = TreeId,
                },
            });
        grainFactory.StubResizeIdle().HoldsShardMigrationsAsync().Returns(Task.FromResult(true));

        await grain.RunSplitPassAsync();

        await source.Received(1).AbortSplitAsync();
        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.False);
            Assert.That(state.State.Complete, Is.False);
            Assert.That(state.State.Phase, Is.EqualTo(ShardSplitPhase.None));
        });
    }
}
