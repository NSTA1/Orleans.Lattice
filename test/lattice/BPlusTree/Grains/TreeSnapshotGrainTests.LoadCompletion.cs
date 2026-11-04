using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5812: an offline copy whose bulk load outran the response timeout is not
/// re-walked and re-issued to queue behind the destination's running load. Its retry
/// asks the destination first: a completed load completes the copy, and a running one
/// is waited for without spending the retry budget. The first attempt reads no status.
/// </summary>
public partial class TreeSnapshotGrainTests
{
    private static (TreeSnapshotGrain Grain, FakePersistentState<TreeSnapshotState> State, IShardRootGrain DestShard, Func<int> Loads) OfflineCopyTimingOut()
    {
        var h = CreateCopyingSnapshot(leafCount: 3, leavesPerPass: 1, SnapshotMode.Offline);
        var loads = 0;
        h.DestShard.BulkLoadRawAsync(Arg.Any<string>(), Arg.Any<List<LwwEntry>>()).Returns(_ =>
        {
            loads++;
            return Task.FromException(new TimeoutException("response timeout"));
        });
        return (h.Grain, h.State, h.DestShard, () => loads);
    }

    [Test]
    public async Task An_offline_copy_whose_load_timed_out_completes_when_the_destination_finished_it()
    {
        var h = OfflineCopyTimingOut();

        await h.Grain.ProcessNextPhaseAsync();
        Assert.That(h.State.State.ShardRetries, Is.EqualTo(1), "precondition: the timeout spent the retry");
        await h.DestShard.DidNotReceive().GetBulkOperationStatusAsync(Arg.Any<string>());

        h.DestShard.GetBulkOperationStatusAsync("op-1-snapshot-0").Returns(BulkOperationStatus.Completed);
        await h.Grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.Loads(), Is.EqualTo(1), "the completed load is not issued again");
            Assert.That(h.State.State.Phase, Is.EqualTo(SnapshotPhase.Unmark));
            Assert.That(h.State.State.ShardRetries, Is.Zero);
        });
    }

    [Test]
    public async Task An_offline_copy_waits_for_a_load_still_running_without_spending_its_retry()
    {
        var h = OfflineCopyTimingOut();
        await h.Grain.ProcessNextPhaseAsync();

        h.DestShard.GetBulkOperationStatusAsync("op-1-snapshot-0").Returns(BulkOperationStatus.Running);
        await h.Grain.ProcessNextPhaseAsync();
        await h.Grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.Loads(), Is.EqualTo(1), "the running load is not issued again behind itself");
            Assert.That(h.State.State.Phase, Is.EqualTo(SnapshotPhase.Copy));
            Assert.That(h.State.State.ShardRetries, Is.EqualTo(1), "waiting spends no retry");
            Assert.That(h.State.State.InProgress, Is.True);
        });
    }

    [Test]
    public async Task An_offline_copy_waits_out_a_transient_status_read_failure_rather_than_reissue()
    {
        // Review S2 of #5815: a status read that failed transiently says nothing about
        // the load, so the copy waits and reads again.
        var h = OfflineCopyTimingOut();
        await h.Grain.ProcessNextPhaseAsync();

        h.DestShard.GetBulkOperationStatusAsync("op-1-snapshot-0")
            .Returns(Task.FromException<BulkOperationStatus>(new TimeoutException("status read timed out")));
        await h.Grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.Loads(), Is.EqualTo(1), "the load is not issued again behind itself");
            Assert.That(h.State.State.Phase, Is.EqualTo(SnapshotPhase.Copy));
            Assert.That(h.State.State.ShardRetries, Is.EqualTo(1), "waiting spends no retry");
        });
    }

    [Test]
    public async Task An_offline_copy_stops_waiting_for_a_load_still_running_past_its_hang_guard()
    {
        // Review S2 of #5815: the wait for a running load is bounded.
        var h = OfflineCopyTimingOut();
        h.Grain.OfflineLoadWaitCeiling = TimeSpan.Zero;
        await h.Grain.ProcessNextPhaseAsync();

        h.DestShard.GetBulkOperationStatusAsync("op-1-snapshot-0").Returns(BulkOperationStatus.Running);
        await h.Grain.ProcessNextPhaseAsync();

        Assert.ThrowsAsync<TimeoutException>(() => h.Grain.ProcessNextPhaseAsync(),
            "past the guard the wait fails the copy step, which has spent its retry");
        Assert.That(h.Loads(), Is.EqualTo(1));
    }

    [Test]
    public async Task An_offline_copy_whose_destination_cannot_report_reissues_as_before()
    {
        var h = OfflineCopyTimingOut();
        await h.Grain.ProcessNextPhaseAsync();

        h.DestShard.GetBulkOperationStatusAsync(Arg.Any<string>())
            .Returns(Task.FromException<BulkOperationStatus>(ShardBulkLoadCompletionTests.MethodUnknown()));
        h.DestShard.BulkLoadRawAsync(Arg.Any<string>(), Arg.Any<List<LwwEntry>>()).Returns(Task.CompletedTask);
        await h.Grain.ProcessNextPhaseAsync();

        Assert.That(h.State.State.Phase, Is.EqualTo(SnapshotPhase.Unmark), "a 10.2.0 destination is re-issued to, as before");
    }
}
