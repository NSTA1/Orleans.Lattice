using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #5464: a standalone snapshot whose phase ticks
/// kept faulting retried forever with its source shards locked and no terminal
/// failure anywhere. It now fails terminally after
/// <see cref="TreeSnapshotGrain.MaxFailedPhaseTicks"/> consecutive failed ticks,
/// giving its source back and recording why.
/// </summary>
public partial class TreeSnapshotGrainTests
{
    private static async Task TickExpectingFaultsAsync(TreeSnapshotGrain grain, int ticks)
    {
        for (var i = 0; i < ticks; i++)
        {
            try
            {
                await grain.ProcessPhaseTickAsync();
            }
            catch (InvalidOperationException)
            {
                // The fault the test injected; the coordinator's timer would log it.
            }
        }
    }

    [Test]
    public async Task A_standalone_offline_snapshot_that_keeps_failing_is_failed_terminally_and_unlocks_its_source()
    {
        var (grain, state, factory) = CreatePacedOfflineSnapshot(4);
        state.State.ReleasesShadowForwardOnCompletion = true;
        factory.GetGrain<IShardRootGrain>($"{SourceTreeId}/2")
            .MarkDeletedAsync().ThrowsAsync(new InvalidOperationException("shard unavailable"));

        await TickExpectingFaultsAsync(grain, TreeSnapshotGrain.MaxFailedPhaseTicks);

        var progress = await grain.GetProgressAsync();
        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.False);
            Assert.That(state.State.Complete, Is.False);
            Assert.That(state.State.FailureReason, Does.Contain("shard unavailable"));
            Assert.That(progress.InProgress, Is.False);
            Assert.That(progress.FailureReason, Is.EqualTo(state.State.FailureReason));
        });
        for (var shard = 0; shard < 4; shard++)
        {
            await factory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{shard}").Received().UnmarkDeletedAsync();
        }
    }

    [Test]
    public async Task A_standalone_snapshot_stays_in_progress_below_the_failed_tick_bound()
    {
        var (grain, state, factory) = CreatePacedOfflineSnapshot(4);
        state.State.ReleasesShadowForwardOnCompletion = true;
        factory.GetGrain<IShardRootGrain>($"{SourceTreeId}/2")
            .MarkDeletedAsync().ThrowsAsync(new InvalidOperationException("shard unavailable"));

        await TickExpectingFaultsAsync(grain, TreeSnapshotGrain.MaxFailedPhaseTicks - 1);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.True);
            Assert.That(state.State.FailedTicks, Is.EqualTo(TreeSnapshotGrain.MaxFailedPhaseTicks - 1));
            Assert.That(state.State.FailureReason, Is.Null);
        });
    }

    [Test]
    public async Task A_successful_tick_resets_the_failed_tick_count()
    {
        var (grain, state, factory) = CreatePacedOfflineSnapshot(4);
        state.State.ReleasesShadowForwardOnCompletion = true;
        var failing = factory.GetGrain<IShardRootGrain>($"{SourceTreeId}/2");
        failing.MarkDeletedAsync().ThrowsAsync(new InvalidOperationException("shard unavailable"));

        await TickExpectingFaultsAsync(grain, TreeSnapshotGrain.MaxFailedPhaseTicks - 1);
        failing.MarkDeletedAsync().Returns(Task.CompletedTask);
        await grain.ProcessPhaseTickAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.FailedTicks, Is.Zero);
            Assert.That(state.State.FailureReason, Is.Null);
            Assert.That(state.State.Complete, Is.True);
        });
    }

    [Test]
    public async Task A_snapshot_driven_by_a_resize_is_never_failed_terminally()
    {
        var (grain, state, factory) = CreatePacedOfflineSnapshot(4);
        factory.GetGrain<IShardRootGrain>($"{SourceTreeId}/2")
            .MarkDeletedAsync().ThrowsAsync(new InvalidOperationException("shard unavailable"));

        await TickExpectingFaultsAsync(grain, TreeSnapshotGrain.MaxFailedPhaseTicks + 2);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.True,
                "a resize reads an idle snapshot as a finished copy, so it must not be failed here");
            Assert.That(state.State.FailureReason, Is.Null);
        });
    }

    [Test]
    public async Task A_failed_online_snapshot_releases_its_shadow_forward()
    {
        var (grain, state, factory) = CreatePacedOfflineSnapshot(2);
        state.State.Mode = SnapshotMode.Online;
        state.State.ReleasesShadowForwardOnCompletion = true;

        await grain.FailSnapshotAsync("test failure");

        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.False);
            Assert.That(state.State.FailureReason, Is.EqualTo("test failure"));
            Assert.That(state.State.ReleasesShadowForwardOnCompletion, Is.False);
        });
        for (var shard = 0; shard < 2; shard++)
        {
            var source = factory.GetGrain<IShardRootGrain>($"{SourceTreeId}/{shard}");
            await source.Received(1).ClearShadowForwardAsync("paced");
            await source.DidNotReceive().UnmarkDeletedAsync();
        }
    }

    [Test]
    public async Task A_new_snapshot_clears_the_previous_failure_reason()
    {
        var (g, s, _, factory, _) = CreateGrain();
        SetupShardMocks(factory, SourceTreeId);
        s.State.FailureReason = "earlier failure";
        s.State.FailedTicks = 3;

        await g.InitiateSnapshotStateAsync(DestTreeId, SnapshotMode.Online, ShardCount);

        Assert.Multiple(() =>
        {
            Assert.That(s.State.FailureReason, Is.Null);
            Assert.That(s.State.FailedTicks, Is.Zero);
        });
    }
}
