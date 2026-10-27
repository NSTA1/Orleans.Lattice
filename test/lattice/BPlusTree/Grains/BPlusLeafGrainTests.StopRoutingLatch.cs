using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #5100: once a stopping silo has observed the
/// replay coordinator grain type withdrawn cluster-wide, a leaf must stop sending
/// replay calls that can only fail with a placement error, and must end its replay
/// cancelled instead of faulted.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string WithdrawnCoordinatorMessage =
        "No active nodes are compatible with grain leafreplaycoordinator and interface ol.lrc version 0. "
        + "Known nodes with grain type: none. All known nodes compatible with interface version: none";

    [Test]
    public async Task Latched_replay_coordinator_cancels_the_replay_without_calling_the_coordinator()
    {
        var latch = new SiloStopRoutingLatch(() => true);
        Assert.That(
            latch.Observe(StopRoutedGrainType.ReplayCoordinator, new OrleansException(WithdrawnCoordinatorMessage)),
            Is.True, "precondition: the latch must be latched for the replay coordinator");

        var coord = BuildChunkingCoordinator(head: 4, sliceSize: 4, tail: 0, Set(1, "k01"));
        var state = NewResumableState();
        var (grain, _) = BuildResumableLeaf(
            state, coord, new InMemorySnapshotStore().Stub, reclassifyEveryN: 1, stopLatch: latch);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(async () => await grain.GetAsync("k01"), Throws.InstanceOf<OperationCanceledException>(),
                "a request on a stopping silo whose coordinator type is withdrawn must fail cancelled");
            Assert.That(state.State.ProjectionCheckpointOffset, Is.Zero,
                "a short-circuited replay must not advance the checkpoint");
        });
        await coord.DidNotReceive().GetHeadOffsetAsync(Arg.Any<CancellationToken>());
        await coord.DidNotReceive().ReadSliceAsync(
            Arg.Any<long>(), Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Latch_is_inert_while_the_host_is_not_stopping()
    {
        // Negative control: a latched bit is ignored unless the host is stopping,
        // so a running silo replays exactly as before.
        var stopping = true;
        var latch = new SiloStopRoutingLatch(() => stopping);
        latch.Observe(StopRoutedGrainType.ReplayCoordinator, new OrleansException(WithdrawnCoordinatorMessage));
        stopping = false;

        var coord = BuildChunkingCoordinator(head: 4, sliceSize: 4, tail: 0, Set(1, "k01"));
        var state = NewResumableState();
        var (grain, _) = BuildResumableLeaf(
            state, coord, new InMemorySnapshotStore().Stub, reclassifyEveryN: 1, stopLatch: latch);

        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        Assert.That(await grain.GetAsync("k01"), Is.Not.Null,
            "with the host running, the replay must complete and apply the WAL entry");
    }

    [Test]
    public async Task Withdrawn_coordinator_fault_while_stopping_latches_and_suppresses_later_calls()
    {
        var latch = new SiloStopRoutingLatch(() => true);
        var coord = Substitute.For<ILeafReplayCoordinatorGrain>();
        coord.GetHeadOffsetAsync(Arg.Any<CancellationToken>())
            .ThrowsAsync(new OrleansException(WithdrawnCoordinatorMessage));
        coord.GetTailOffsetAsync(Arg.Any<CancellationToken>())
            .ThrowsAsync(new OrleansException(WithdrawnCoordinatorMessage));

        var state = NewResumableState();
        var (grain, _) = BuildResumableLeaf(
            state, coord, new InMemorySnapshotStore().Stub, reclassifyEveryN: 1, stopLatch: latch);

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        Assert.That(async () => await grain.GetAsync("k"), Throws.Exception,
            "the first replay meets the withdrawn coordinator and fails");

        Assert.That(latch.IsWithdrawn(StopRoutedGrainType.ReplayCoordinator), Is.True,
            "the catalog-withdrawn fault observed while stopping must latch the coordinator type");

        var headCalls = coord.ReceivedCalls().Count();

        Assert.That(async () => await grain.GetAsync("k"), Throws.InstanceOf<OperationCanceledException>(),
            "once latched, a re-armed replay must end cancelled rather than fault again");
        Assert.That(coord.ReceivedCalls().Count(), Is.EqualTo(headCalls),
            "once latched, no further coordinator call may be sent from this silo");
    }
}
