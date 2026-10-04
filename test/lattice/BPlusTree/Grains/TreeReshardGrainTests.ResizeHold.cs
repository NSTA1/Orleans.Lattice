using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4452: a completed resize holds the splits and folds a reshard is made
/// of for as long as it can be undone and the copy it replaced still mirrors into
/// the resized one. A reshard must be refused up front with its own reason and a
/// remedy, and a reshard that somehow reaches its migrate step under the hold
/// must pause visibly rather than dispatch migrations that are each refused.
/// </summary>
public partial class TreeReshardGrainTests
{
    private static ITreeResizeGrain StubCompletedResizeHoldingMigrations(IGrainFactory grainFactory)
    {
        var resize = Substitute.For<ITreeResizeGrain>();
        resize.IsIdleAsync().Returns(Task.FromResult(true));
        resize.HoldsShardMigrationsAsync().Returns(Task.FromResult(true));
        grainFactory.GetGrain<ITreeResizeGrain>(TreeId).Returns(resize);
        return resize;
    }

    [Test]
    [NonParallelizable]
    public async Task ReshardAsync_is_refused_as_resize_undoable_while_a_completed_resize_holds_migrations()
    {
        var (grain, state, grainFactory, _) = CreateGrain(virtualShardCount: 16, physicalShardCount: 2);
        StubCompletedResizeHoldingMigrations(grainFactory);
        var (listener, totals) = ListenForRejectionReasons();
        using (listener)
        {
            var ex = Assert.ThrowsAsync<InvalidOperationException>(() => grain.ReshardAsync(4));

            Assert.That(ex!.Message, Does.Contain("Undo the resize").And.Contain("SoftDeleteDuration"));
        }

        Assert.Multiple(() =>
        {
            Assert.That(totals.TryGetValue("resize_undoable", out var undoable) ? undoable : -1, Is.EqualTo(1));
            Assert.That(totals.TryGetValue("resize_in_flight", out var inFlight) ? inFlight : -1, Is.Zero,
                "a completed resize is not one in flight; the reasons must stay distinct");
            Assert.That(state.State.InProgress, Is.False);
        });
        await Task.CompletedTask;
    }

    [Test]
    public async Task MigrateAsync_pauses_without_dispatching_while_a_resize_holds_migrations()
    {
        var (grain, state, grainFactory, _) = CreateGrain(
            virtualShardCount: 16, physicalShardCount: 2, existingMap: ShardMap.CreateDefault(16, 2));
        state.State.InProgress = true;
        state.State.Phase = ReshardPhase.Migrating;
        state.State.TargetShardCount = 4;
        StubCompletedResizeHoldingMigrations(grainFactory);
        var split0 = Substitute.For<ITreeShardSplitGrain>();
        var split1 = Substitute.For<ITreeShardSplitGrain>();
        grainFactory.GetGrain<ITreeShardSplitGrain>($"{TreeId}/0").Returns(split0);
        grainFactory.GetGrain<ITreeShardSplitGrain>($"{TreeId}/1").Returns(split1);

        await grain.MigrateAsync();

        await split0.DidNotReceive().SplitAsync(Arg.Any<int>());
        await split1.DidNotReceive().SplitAsync(Arg.Any<int>());
        Assert.That(state.State.InProgress, Is.True, "the reshard is paused, not abandoned");
    }

    [Test]
    public async Task MigrateAsync_pauses_a_shrink_without_starting_a_fold_while_a_resize_holds_migrations()
    {
        var (grain, state, grainFactory, _) = CreateGrain(
            virtualShardCount: 16, physicalShardCount: 4, existingMap: ShardMap.CreateDefault(16, 4));
        state.State.InProgress = true;
        state.State.Phase = ReshardPhase.Migrating;
        state.State.TargetShardCount = 2;
        state.State.Shrinking = true;
        StubCompletedResizeHoldingMigrations(grainFactory);
        var fold = Substitute.For<ITreeShardConsolidationGrain>();
        grainFactory.GetGrain<ITreeShardConsolidationGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(fold);

        await grain.MigrateAsync();

        await fold.DidNotReceive().StartAsync(Arg.Any<int>());
    }
}
