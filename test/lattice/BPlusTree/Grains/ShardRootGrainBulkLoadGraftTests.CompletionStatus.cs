using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #5812: a caller whose bulk load outran the response timeout reads the load's
/// status from the shard root instead of re-issuing it behind the first. The read
/// interleaves with the running load and reports it running, then completed.
/// </summary>
public sealed partial class ShardRootGrainBulkLoadGraftTests
{
    [Test]
    public async Task A_bulk_load_reads_running_while_it_runs_and_completed_once_it_has()
    {
        var rig = CreateHintRig();
        var headRead = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
        rig.Coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>()).Returns(_ => headRead.Task);

        Assert.That(await rig.H.Grain.GetBulkOperationStatusAsync("load"), Is.EqualTo(BulkOperationStatus.NotRunning),
            "precondition: nothing has run");

        var load = rig.H.Grain.BulkLoadRawAsync("load", RawEntries(5));
        Assert.Multiple(async () =>
        {
            Assert.That(await rig.H.Grain.GetBulkOperationStatusAsync("load"), Is.EqualTo(BulkOperationStatus.Running),
                "the load is suspended reading its birth heads");
            Assert.That(await rig.H.Grain.GetBulkOperationStatusAsync("other"), Is.EqualTo(BulkOperationStatus.NotRunning),
                "another operation id is not the running load");
        });

        headRead.SetResult(BirthHead);
        await load;

        Assert.Multiple(async () =>
        {
            Assert.That(await rig.H.Grain.GetBulkOperationStatusAsync("load"), Is.EqualTo(BulkOperationStatus.Completed));
            Assert.That(await rig.H.Grain.GetBulkOperationStatusAsync("other"), Is.EqualTo(BulkOperationStatus.NotRunning));
        });
    }

    [Test]
    public async Task A_bulk_load_that_faults_reads_not_running()
    {
        var rig = CreateHintRig();
        rig.Coordinator.GetHeadOffsetAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromException<long>(new InvalidOperationException("head read failed")));

        Assert.ThrowsAsync<InvalidOperationException>(() => rig.H.Grain.BulkLoadRawAsync("load", RawEntries(5)));

        Assert.That(await rig.H.Grain.GetBulkOperationStatusAsync("load"), Is.EqualTo(BulkOperationStatus.NotRunning),
            "a faulted load is issued again");
    }
}
