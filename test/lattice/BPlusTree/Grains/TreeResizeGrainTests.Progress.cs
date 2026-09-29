using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// <see cref="Orleans.Lattice.BPlusTree.Grains.TreeResizeGrain.GetProgressAsync"/>
/// (issue 3958): a resize is measured as one unit per copied shard plus the three
/// steps after the copy, every unit counted only once it is durable.
/// </summary>
public partial class TreeResizeGrainTests
{
    private const string ProgressOperation = "op-resize-progress";
    private const string ProgressDestination = "test-tree/resized/op-resize-progress";

    private static FakePersistentState<TreeResizeState> ResizingState(ResizePhase phase, int shardCount = 4) =>
        new()
        {
            State = new TreeResizeState
            {
                InProgress = true,
                Phase = phase,
                OperationId = ProgressOperation,
                OldPhysicalTreeId = TreeId,
                SnapshotTreeId = ProgressDestination,
                ShardCount = shardCount,
                NewMaxLeafKeys = 256,
                NewMaxInternalChildren = 64,
            },
        };

    private static ITreeSnapshotGrain SnapshotReporting(IGrainFactory grainFactory, SnapshotProgress progress)
    {
        var snapshot = Substitute.For<ITreeSnapshotGrain>();
        snapshot.GetProgressAsync().Returns(progress);
        grainFactory.GetGrain<ITreeSnapshotGrain>(TreeId).Returns(snapshot);
        return snapshot;
    }

    [Test]
    public async Task GetProgressAsync_reports_nothing_in_flight_for_an_idle_coordinator()
    {
        var (grain, _, _, _, _) = CreateGrain();

        var progress = await grain.GetProgressAsync();

        Assert.Multiple(() =>
        {
            Assert.That(progress.InProgress, Is.False);
            Assert.That(progress.CompletedUnits, Is.Zero);
            Assert.That(progress.TotalUnits, Is.Zero);
        });
    }

    [Test]
    public async Task GetProgressAsync_counts_the_shards_its_own_snapshot_has_copied()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain(existingState: ResizingState(ResizePhase.Snapshot));
        SnapshotReporting(grainFactory, new SnapshotProgress(true, false, ProgressOperation, SnapshotPhase.Copy, 3, 4));

        var progress = await grain.GetProgressAsync();

        Assert.Multiple(() =>
        {
            Assert.That(progress.InProgress, Is.True);
            Assert.That(progress.Phase, Is.EqualTo(ResizePhase.Snapshot));
            Assert.That(progress.CompletedUnits, Is.EqualTo(3));
            Assert.That(progress.TotalUnits, Is.EqualTo(4 + ResizeProgress.StepsAfterCopy));
        });
    }

    [Test]
    public async Task GetProgressAsync_ignores_a_snapshot_that_belongs_to_another_operation()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain(existingState: ResizingState(ResizePhase.Snapshot));
        SnapshotReporting(grainFactory, new SnapshotProgress(true, false, "someone-elses-snapshot", SnapshotPhase.Copy, 3, 4));

        var progress = await grain.GetProgressAsync();

        Assert.That(progress.CompletedUnits, Is.Zero);
    }

    [Test]
    public async Task GetProgressAsync_counts_the_whole_copy_once_its_snapshot_has_finished()
    {
        var (grain, _, _, grainFactory, _) = CreateGrain(existingState: ResizingState(ResizePhase.Snapshot));
        SnapshotReporting(grainFactory, new SnapshotProgress(false, true, ProgressOperation, SnapshotPhase.Lock, 0, 0));

        var progress = await grain.GetProgressAsync();

        Assert.That(progress.CompletedUnits, Is.EqualTo(4));
    }

    [TestCase(1, 4)]
    [TestCase(3, 5)]
    [TestCase(2, 6)]
    public async Task GetProgressAsync_counts_one_unit_per_step_after_the_copy(int phase, int expected)
    {
        var (grain, _, _, _, _) = CreateGrain(existingState: ResizingState((ResizePhase)phase));

        var progress = await grain.GetProgressAsync();

        Assert.Multiple(() =>
        {
            Assert.That(progress.CompletedUnits, Is.EqualTo(expected));
            Assert.That(progress.TotalUnits, Is.EqualTo(7));
        });
    }

    [Test]
    public async Task GetProgressAsync_reports_an_unknown_total_when_the_shard_set_was_not_recorded()
    {
        var (grain, _, _, _, _) = CreateGrain(existingState: ResizingState(ResizePhase.Swap, shardCount: 0));

        var progress = await grain.GetProgressAsync();

        Assert.Multiple(() =>
        {
            Assert.That(progress.InProgress, Is.True);
            Assert.That(progress.TotalUnits, Is.Zero);
        });
    }

    [Test]
    public async Task GetProgressAsync_does_not_report_a_step_whose_write_has_not_landed()
    {
        var state = ResizingState(ResizePhase.Reject);
        var (grain, _, _, grainFactory, _) = CreateGrain(existingState: state);
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(Substitute.For<IShardRootGrain>());
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

        var step = grain.RejectOldShardsAsync();
        await writing.Task;

        var during = await grain.GetProgressAsync();
        Assert.That(state.State.Phase, Is.EqualTo(ResizePhase.Cleanup), "the step is applied in memory before the write");
        Assert.Multiple(() =>
        {
            Assert.That(during.Phase, Is.EqualTo(ResizePhase.Reject));
            Assert.That(during.CompletedUnits, Is.EqualTo(5));
        });

        state.BeforeWrite = null;
        release.SetResult();
        await step;

        var after = await grain.GetProgressAsync();
        Assert.Multiple(() =>
        {
            Assert.That(after.Phase, Is.EqualTo(ResizePhase.Cleanup));
            Assert.That(after.CompletedUnits, Is.EqualTo(6));
        });
    }

    [Test]
    public async Task GetProgressAsync_survives_reactivation()
    {
        var state = ResizingState(ResizePhase.Reject);
        var (first, _, _, grainFactory, _) = CreateGrain(existingState: state);
        grainFactory.GetGrain<IShardRootGrain>(Arg.Any<string>()).Returns(Substitute.For<IShardRootGrain>());
        await first.RejectOldShardsAsync();

        var (second, _, _, _, _) = CreateGrain(existingState: state);
        await ((IGrainBase)second).OnActivateAsync(CancellationToken.None);

        var progress = await second.GetProgressAsync();
        Assert.Multiple(() =>
        {
            Assert.That(progress.Phase, Is.EqualTo(ResizePhase.Cleanup));
            Assert.That(progress.CompletedUnits, Is.EqualTo(6));
            Assert.That(progress.TotalUnits, Is.EqualTo(7));
        });
    }
}
