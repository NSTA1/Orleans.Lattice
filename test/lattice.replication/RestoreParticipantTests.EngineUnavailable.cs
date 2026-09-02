using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage of the two boundary behaviours a restore participant must honour on the
/// single-tree path: it stays inert when the backup engine is not wired (votes abort
/// on prepare, and merely lifts any defensive fence on commit and abort because it
/// never prepared anything), and it lets cooperative cancellation propagate cleanly
/// rather than mistranslating an <see cref="OperationCanceledException"/> into a
/// deliberate abort vote or a swallowed fault. Both are load-bearing for the saga:
/// an unwired cluster must not wedge a restore, and a cancelled prepare must surface
/// as cancellation so the coordinator retries rather than reading it as a refusal.
/// </summary>
public partial class RestoreParticipantTests
{
    [Test]
    public async Task PrepareAsync_engine_unwired_votes_abort_with_explanation()
    {
        var participant = Participant(engine: null, FactoryFor(Substitute.For<ISagaWriteFenceGrain>()));

        var result = await participant.PrepareAsync(Request());

        Assert.That(result.Vote, Is.EqualTo(SagaVote.Abort));
        Assert.That(result.Detail, Does.Contain("unavailable"),
            "the vote explains the backup package is not wired on this cluster");
    }

    [Test]
    public async Task CommitAsync_engine_unwired_lifts_fence_and_never_engages_it()
    {
        var fence = Substitute.For<ISagaWriteFenceGrain>();
        var participant = Participant(engine: null, FactoryFor(fence));

        await participant.CommitAsync(Request());

        // Nothing was prepared, so commit only lifts any defensive fence.
        await fence.Received(1).LiftAsync();
        await fence.DidNotReceive().EngageAsync(Arg.Any<SagaWriteFenceRequest>());
        await fence.DidNotReceive().UnblockWritesAsync();
    }

    [Test]
    public async Task AbortAsync_engine_unwired_lifts_fence_and_returns()
    {
        var fence = Substitute.For<ISagaWriteFenceGrain>();
        var participant = Participant(engine: null, FactoryFor(fence));

        await participant.AbortAsync(Request());

        await fence.Received(1).LiftAsync();
    }

    [Test]
    public void PrepareAsync_admission_probe_cancelled_rethrows_cancellation()
    {
        var engine = HealthyEngine();
        engine.ProbeAdmissionAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new OperationCanceledException());
        var participant = Participant(engine, FactoryFor(Substitute.For<ISagaWriteFenceGrain>()));

        // Cancellation must escape as-is, never be reshaped into an abort vote. If it
        // were caught by the infeasible-probe handler this would return Abort instead.
        Assert.That(
            async () => await participant.PrepareAsync(Request()),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public void PrepareAsync_shadow_build_cancelled_rethrows_without_retrying()
    {
        var engine = HealthyEngine();
        engine.BuildShadowAsync(Arg.Any<LatticeRestoreRequest>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new OperationCanceledException());
        var participant = Participant(engine, FactoryFor(Substitute.For<ISagaWriteFenceGrain>()));

        // The build's cancellation catch precedes the transient-retry catch, so a
        // cancelled build propagates at once instead of being retried to an abort.
        Assert.That(
            async () => await participant.PrepareAsync(Request()),
            Throws.InstanceOf<OperationCanceledException>());
    }
}
