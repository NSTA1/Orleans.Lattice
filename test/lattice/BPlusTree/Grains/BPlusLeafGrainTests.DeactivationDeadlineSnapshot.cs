using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for the third untokened flush on the graceful-deactivation
/// path (issue #1965). <c>BPlusLeafGrain.OnDeactivateAsync</c> performs four
/// awaits; the deactivation-time snapshot capture added by #1537 reached
/// <c>ILeafSnapshotStorageGrain.SaveAsync</c> with a hard-coded
/// <see cref="CancellationToken.None"/>, so the single most expensive operation
/// on the path - serialising and persisting the whole leaf blob - could not be
/// interrupted by Orleans' deactivation deadline.
/// <para>
/// This matters because the hook <em>looks</em> defended: it already wraps its
/// body in a bare <c>catch</c>. But an uncancellable await means the grain never
/// <b>returns</b>, so Orleans raises the overrun as a
/// <c>TaskCanceledException</c> in its own frame
/// (<c>ActivationData.FinishDeactivating</c>) - which the grain's catch can never
/// observe. At the end of WAL replay thousands of leaves go idle together and
/// contend for the same snapshot store, so the overruns arrive as a burst.
/// </para>
/// <para>
/// These tests use the same rig as the #1537 cadence fixture (a Guid-keyed leaf
/// with a real snapshot-storage stub) so the capture path genuinely runs rather
/// than short-circuiting - a vacuous pass here would hide the very defect the
/// tests exist to catch.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>
    /// The precise defect: the capture must be handed the caller's deactivation
    /// token, not <see cref="CancellationToken.None"/>. Asserting on the token
    /// the store actually received pins the fix at the exact seam that was
    /// wrong, and fails on the pre-fix code rather than passing vacuously.
    /// <para>
    /// The token is deliberately left <b>live</b>: an already-cancelled one
    /// short-circuits at the earlier checkpoint flush, so the capture is never
    /// reached and the assertion below could not observe anything.
    /// </para>
    /// </summary>
    [Test]
    public async Task Deactivation_snapshot_capture_receives_the_caller_token_not_none()
    {
        var captured = new List<CancellationToken>();
        var stub = Substitute.For<ILeafSnapshotStorageGrain>();
        stub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(null));
        stub.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(callInfo =>
            {
                captured.Add(callInfo.ArgAt<CancellationToken>(1));
                return Task.CompletedTask;
            });

        var (leaf, _, _, _, _) =
            CreateLeafWithDurablePinAndSnapshotStore(treeId: PinSeamTreeId, snapshotStub: stub);

        // Latch _checkpointAdvancedThisActivation so the deactivation capture
        // gate actually opens; without this the assertions below would pass
        // trivially because no capture ever runs.
        await CheckpointLeafAsync(leaf, "k1", hlcPhysical: 100, offset: 1);

        using var deadline = new CancellationTokenSource();

        await ((IGrainBase)leaf).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            deadline.Token);

        Assert.That(captured, Is.Not.Empty,
            "precondition: the deactivation capture must actually reach the snapshot "
            + "store, otherwise this test cannot observe which token it was given.");
        Assert.That(captured[0], Is.EqualTo(deadline.Token),
            "the snapshot capture must be handed the caller's deactivation token so it "
            + "can be interrupted; a hard-coded CancellationToken.None here is what let "
            + "the blob write outrun Orleans' deactivation deadline (issue 1965)");
    }

    /// <summary>
    /// The behaviour that token actually buys: a snapshot store that only
    /// completes when its token fires must not pin the hook open. This is the
    /// end-to-end property - during the end-of-replay burst the contended
    /// store, not the grain, is the slow party.
    /// <para>
    /// On the pre-fix code the store received <see cref="CancellationToken.None"/>,
    /// so firing the deadline had no effect and the hook stayed open until the
    /// runtime cancelled it in its own frame. That is the failure this asserts
    /// against.
    /// </para>
    /// </summary>
    [Test]
    public async Task Deactivation_returns_when_the_deadline_cancels_an_in_flight_capture()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var stub = Substitute.For<ILeafSnapshotStorageGrain>();
        stub.LoadAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<LeafSnapshotBlob?>(null));
        // Model a store that never completes on its own and is only released by
        // cancellation - the shape of a contended provider at end-of-replay.
        stub.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(async callInfo =>
            {
                var token = callInfo.ArgAt<CancellationToken>(1);
                entered.TrySetResult();
                await Task.Delay(Timeout.Infinite, token);
            });

        var (leaf, _, _, _, _) =
            CreateLeafWithDurablePinAndSnapshotStore(treeId: PinSeamTreeId, snapshotStub: stub);
        await CheckpointLeafAsync(leaf, "k1", hlcPhysical: 100, offset: 1);

        using var deadline = new CancellationTokenSource();
        var deactivate = ((IGrainBase)leaf).OnDeactivateAsync(
            new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
            deadline.Token);

        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.That(deactivate.IsCompleted, Is.False,
            "precondition: the hanging store must actually be holding the hook open, "
            + "otherwise the cancellation below proves nothing.");

        // Orleans' deactivation deadline fires while the blob write is in flight.
        await deadline.CancelAsync();

        var finished = await Task.WhenAny(deactivate, Task.Delay(TimeSpan.FromSeconds(5)));

        Assert.That(finished, Is.SameAs(deactivate),
            "the deadline must interrupt the in-flight blob write and let the hook "
            + "return; with a hard-coded CancellationToken.None the store never sees "
            + "the cancellation and the grain never returns");
        Assert.That(deactivate.IsFaulted, Is.False,
            "the abandoned capture is best-effort and must stay swallowed - surfacing "
            + "it would simply trade one error-level log flood for another");
    }
}
