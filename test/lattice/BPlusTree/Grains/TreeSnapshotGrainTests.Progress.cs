using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// <see cref="Orleans.Lattice.BPlusTree.Grains.TreeSnapshotGrain.GetProgressAsync"/>
/// (issue 3958): the progress a status read reports comes from the snapshot
/// state as last persisted, so it survives reactivation and never runs ahead of
/// a write that has not landed.
/// </summary>
public partial class TreeSnapshotGrainTests
{
    private static FakePersistentState<TreeSnapshotState> ProgressState(
        SnapshotPhase phase, SnapshotMode mode, int nextShardIndex, int shardCount, HashSet<int>? drained = null) =>
        new()
        {
            State = new TreeSnapshotState
            {
                InProgress = true,
                Phase = phase,
                Mode = mode,
                NextShardIndex = nextShardIndex,
                ShardCount = shardCount,
                DrainedPositions = drained,
                DestinationTreeId = DestTreeId,
                OperationId = "op-progress",
            },
        };

    [Test]
    public async Task GetProgressAsync_reports_nothing_in_flight_for_an_idle_coordinator()
    {
        var (grain, _, _, _, _) = CreateGrain();

        var progress = await grain.GetProgressAsync();

        Assert.Multiple(() =>
        {
            Assert.That(progress.InProgress, Is.False);
            Assert.That(progress.CopiedShardCount, Is.Zero);
            Assert.That(progress.ShardCount, Is.Zero);
        });
    }

    [Test]
    public async Task GetProgressAsync_counts_the_head_and_every_shard_a_concurrent_drain_finished()
    {
        var state = ProgressState(SnapshotPhase.Copy, SnapshotMode.Online, nextShardIndex: 1, shardCount: 5, drained: [3]);
        var (grain, _, _, _, _) = CreateGrain(existingState: state);

        var progress = await grain.GetProgressAsync();

        Assert.Multiple(() =>
        {
            Assert.That(progress.InProgress, Is.True);
            Assert.That(progress.Phase, Is.EqualTo(SnapshotPhase.Copy));
            Assert.That(progress.CopiedShardCount, Is.EqualTo(2), "position 0 is behind the head and position 3 has drained");
            Assert.That(progress.ShardCount, Is.EqualTo(5));
            Assert.That(progress.OperationId, Is.EqualTo("op-progress"));
        });
    }

    [Test]
    public async Task GetProgressAsync_measures_against_the_routed_shard_set_when_one_was_pinned()
    {
        var state = ProgressState(SnapshotPhase.Copy, SnapshotMode.Online, nextShardIndex: 2, shardCount: 2);
        state.State.ShardIndices = [0, 1, 7];
        var (grain, _, _, _, _) = CreateGrain(existingState: state);

        var progress = await grain.GetProgressAsync();

        Assert.That((progress.CopiedShardCount, progress.ShardCount), Is.EqualTo((2, 3)),
            "a shard an adaptive split allocated above the pinned count is part of the copy");
    }

    [Test]
    public async Task GetProgressAsync_does_not_report_a_shard_advance_whose_write_has_not_landed()
    {
        // Offline, head shard 0 copied and waiting to be unmarked. The Unmark
        // step advances NextShardIndex in memory, then persists it.
        var state = ProgressState(SnapshotPhase.Unmark, SnapshotMode.Offline, nextShardIndex: 0, shardCount: ShardCount);
        var (grain, _, _, grainFactory, _) = CreateGrain(existingState: state);
        SetupShardMocks(grainFactory, SourceTreeId);
        // The runtime activates the grain once its state has loaded; that is
        // where the durable copy the status read answers from is captured.
        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        var writing = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        state.BeforeWrite = () =>
        {
            writing.TrySetResult();
            return release.Task;
        };

        var step = grain.ProcessNextPhaseAsync();
        await writing.Task;

        var during = await grain.GetProgressAsync();
        Assert.That(state.State.NextShardIndex, Is.EqualTo(1), "the advance is applied in memory before the write");
        Assert.Multiple(() =>
        {
            Assert.That(during.CopiedShardCount, Is.Zero, "the advance is not durable yet");
            Assert.That(during.Phase, Is.EqualTo(SnapshotPhase.Unmark));
        });

        state.BeforeWrite = null;
        release.SetResult();
        await step;

        var after = await grain.GetProgressAsync();
        Assert.Multiple(() =>
        {
            Assert.That(after.CopiedShardCount, Is.EqualTo(1));
            Assert.That(after.Phase, Is.EqualTo(SnapshotPhase.Copy));
        });
    }

    [Test]
    public async Task GetProgressAsync_does_not_report_an_advance_whose_write_failed()
    {
        var state = ProgressState(SnapshotPhase.Unmark, SnapshotMode.Offline, nextShardIndex: 0, shardCount: ShardCount);
        var (grain, _, _, grainFactory, _) = CreateGrain(existingState: state);
        SetupShardMocks(grainFactory, SourceTreeId);
        state.ThrowOnWrite = new InvalidOperationException("simulated storage failure");

        await grain.ProcessNextPhaseAsync();

        var progress = await grain.GetProgressAsync();
        Assert.That(progress.CopiedShardCount, Is.Zero);
    }

    [Test]
    public async Task GetProgressAsync_survives_reactivation()
    {
        var state = ProgressState(SnapshotPhase.Unmark, SnapshotMode.Offline, nextShardIndex: 0, shardCount: ShardCount);
        var (first, _, _, grainFactory, _) = CreateGrain(existingState: state);
        SetupShardMocks(grainFactory, SourceTreeId);
        await first.ProcessNextPhaseAsync();

        // A new activation over the same persisted row.
        var (second, _, _, _, _) = CreateGrain(existingState: state);
        await ((IGrainBase)second).OnActivateAsync(CancellationToken.None);

        var progress = await second.GetProgressAsync();
        Assert.Multiple(() =>
        {
            Assert.That(progress.InProgress, Is.True);
            Assert.That(progress.CopiedShardCount, Is.EqualTo(1));
            Assert.That(progress.ShardCount, Is.EqualTo(ShardCount));
            Assert.That(progress.Phase, Is.EqualTo(SnapshotPhase.Copy));
        });
    }

    [Test]
    public async Task GetProgressAsync_reports_completion_and_keeps_the_operation_id()
    {
        var state = ProgressState(SnapshotPhase.Copy, SnapshotMode.Offline, nextShardIndex: ShardCount, shardCount: ShardCount);
        var (grain, _, _, grainFactory, _) = CreateGrain(existingState: state);
        SetupShardMocks(grainFactory, SourceTreeId);
        SetupShardMocks(grainFactory, DestTreeId);

        await grain.CompleteSnapshotAsync();

        var progress = await grain.GetProgressAsync();
        Assert.Multiple(() =>
        {
            Assert.That(progress.InProgress, Is.False);
            Assert.That(progress.Complete, Is.True);
            Assert.That(progress.OperationId, Is.EqualTo("op-progress"));
        });
    }
}
