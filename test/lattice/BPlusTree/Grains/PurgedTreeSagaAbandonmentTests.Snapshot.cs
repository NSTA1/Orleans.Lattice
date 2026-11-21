using Microsoft.Extensions.Logging;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #5464: the snapshot coordinator did not opt in
/// to purged-tree abandonment, so a snapshot whose source tree was purged
/// mid-flight faulted on every phase tick for as long as the cluster ran. Unlike
/// the resize sagas it keeps its state row, because the row now carries the
/// failure reason the snapshot status surfaces.
/// </summary>
public sealed partial class PurgedTreeSagaAbandonmentTests
{
    private static (Harness Harness, FakePersistentState<TreeSnapshotState> State) SnapshotSaga()
    {
        var w = Wire("snapshot", TreeId);
        var state = new FakePersistentState<TreeSnapshotState>();
        state.State.InProgress = true;
        state.State.Phase = SnapshotPhase.Copy;
        state.State.Mode = SnapshotMode.Offline;
        state.State.ShardCount = 2;
        state.State.DestinationTreeId = "purged-saga-dest";
        state.State.OperationId = "snapshot-op";
        // The copy's one per-phase retry is already spent, so the faulted copy
        // propagates out of the tick instead of being absorbed as a retry.
        state.State.ShardRetries = 1;
        state.State.ReleasesShadowForwardOnCompletion = true;

        var grain = new TreeSnapshotGrain(
            w.Context, w.Factory, w.Reminders, w.Options, w.Resolver,
            new LoggerFactory().CreateLogger<TreeSnapshotGrain>(), state);

        return (Build(w, grain, "snapshot-keepalive", () => state.State.InProgress, () => state.RecordExists), state);
    }

    [Test]
    public async Task Faulted_tick_on_a_purged_tree_fails_the_snapshot_and_retires_its_coordinator()
    {
        var (h, state) = SnapshotSaga();
        var tick = await ArmAsync(h);

        await tick(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.False, "The snapshot must stop.");
            Assert.That(state.State.Complete, Is.False, "A purged source never completes its copy.");
            Assert.That(state.State.FailureReason, Does.Contain("purged"));
        });
        h.Timer.Received(1).Dispose();
        await h.Reminders.Received(1).UnregisterReminder(h.GrainId, Arg.Any<IGrainReminder>());
    }

    [Test]
    public async Task Faulted_tick_on_a_deleted_but_unpurged_tree_keeps_the_snapshot_retrying()
    {
        var (h, state) = SnapshotSaga();
        h.Deletion.HoldsCompletedPurgeAsync().Returns(false);
        var tick = await ArmAsync(h);

        await tick(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.InProgress, Is.True);
            Assert.That(state.State.FailureReason, Is.Null);
            Assert.That(state.State.FailedTicks, Is.EqualTo(1));
        });
        h.Timer.DidNotReceive().Dispose();
    }

    [Test]
    public async Task Faulted_tick_keeps_the_snapshot_retrying_when_the_purge_verdict_cannot_be_read()
    {
        var (h, state) = SnapshotSaga();
        h.Deletion.HoldsCompletedPurgeAsync().ThrowsAsync(new TimeoutException("deletion grain unreachable"));
        var tick = await ArmAsync(h);

        await tick(CancellationToken.None);

        Assert.That(state.State.InProgress, Is.True);
        h.Timer.DidNotReceive().Dispose();
    }
}
