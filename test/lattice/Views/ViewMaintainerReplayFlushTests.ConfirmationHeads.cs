using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

public partial class ViewMaintainerReplayFlushTests
{
    [TestCase(1, false)]
    [TestCase(8, false)]
    [TestCase(40, false)]
    [TestCase(8, true)]
    public async Task Source_confirmation_samples_a_bounded_complete_head_vector_after_the_source_read(
        int partitions, bool failProbe)
    {
        var reader = Substitute.For<ICommitLogReader>();
        var view = new FakeViewTree();
        var h = Create(view, new FakePersistentState<ViewCheckpointState>(),
            walPartitions: partitions, reader: reader);
        var txid = Guid.NewGuid();
        List<LatticeMutation> log = [Prepared(txid, 2, 20), Commit(txid, 21)];
        ArmLog(h, log);
        h.SourceRows[Key] = [3];
        h.SourceVersions[Key] = Clock(30);
        var sourceRead = false;
        h.SourceRoot.When(r => r.GetVisibleRawEntriesAsync(Arg.Any<List<string>>()))
            .Do(_ => sourceRead = true);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var launched = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var expectedPeak = Math.Min(partitions, BoundedFanOut.DefaultWidth);
        var active = 0;
        var peak = 0;
        var finished = 0;
        var observed = new List<int>();
        var sync = new object();
        reader.GetHeadOffsetAsync(SourceTreeId, Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(call => ProbeAsync(call.ArgAt<int>(1)));

        var drain = h.Grain.DrainAsync();
        var rendezvous = await Task.WhenAny(launched.Task, Task.Delay(TimeSpan.FromSeconds(10)));
        release.TrySetResult();

        if (failProbe)
            Assert.ThrowsAsync<InvalidOperationException>(async () => await drain);
        else
            await drain;

        Assert.Multiple(() =>
        {
            Assert.That(rendezvous, Is.SameAs(launched.Task), "the first wave must overlap before release");
            Assert.That(peak, Is.EqualTo(expectedPeak), "serial probes cannot satisfy the rendezvous");
            Assert.That(active, Is.Zero, "all launched probes must settle before returning or failing");
            Assert.That(finished, Is.EqualTo(partitions * (failProbe ? 1 : 2)),
                "successful drains also sample the final lag vector");
            Assert.That(observed.Take(partitions).Order(), Is.EqualTo(Enumerable.Range(0, partitions)));
            Assert.That(view.Snapshots, Is.Empty, "an unread superseder must not authorize partial publication");
            Assert.That(h.State.State.AppliedOffsets.GetValueOrDefault(0, -1), Is.EqualTo(-1));
        });

        async Task<long> ProbeAsync(int partition)
        {
            Assert.That(sourceRead, Is.True, "heads must be captured after observing the mismatching source row");
            lock (sync)
            {
                observed.Add(partition);
                active++;
                peak = Math.Max(peak, active);
                if (active == expectedPeak) launched.TrySetResult();
            }
            try
            {
                await release.Task;
                if (failProbe && partition == 0) throw new InvalidOperationException("head read failed");
                return partition + 100;
            }
            finally
            {
                lock (sync)
                {
                    active--;
                    finished++;
                }
            }
        }
    }
}
