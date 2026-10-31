using NSubstitute;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage of a decided restore saga that is delivered with no user identity
/// (issue #5506). This cluster's own coordinator redelivers a decision from its
/// keepalive reminder, and the participant from its fence expiry, so the commit or
/// abort of an already-prepared saga can arrive as <c>anonymous</c>. Such a step
/// runs with the authority of the prepare this participant ran - only on the shadow
/// that prepare built - and a cutover that fails never leaves local writes fenced.
/// </summary>
public partial class RestoreParticipantTests
{
    private static SagaControlRequest OwnRequest() => Request() with { CoordinatorClusterId = LocalCluster };

    [Test]
    public async Task CommitAsync_of_this_clusters_own_prepared_saga_cuts_over_with_the_prepares_authority()
    {
        var (engine, calls) = ObservedEngine();
        var participant = PeerParticipant(engine, [TargetTree], [CoordinatorCluster]);
        await participant.PrepareAsync(OwnRequest());
        calls.Clear();

        await participant.CommitAsync(OwnRequest());

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(new[] { ("commit", true) }),
                "a redelivered decision for the shadow this participant built is not re-authorized as anonymous");
            Assert.That(LatticeSystemOrigin.IsActive, Is.False, "the scope ends with the saga step");
        });
    }

    [Test]
    public async Task CommitAsync_of_this_clusters_own_saga_that_must_rebuild_runs_as_the_ambient_caller()
    {
        var (engine, calls) = ObservedEngine();
        var participant = PeerParticipant(engine, [TargetTree], [CoordinatorCluster]);

        await participant.CommitAsync(OwnRequest());

        Assert.That(calls.Select(c => c.Elevated), Is.All.False,
            "without the shadow of an authorized prepare there is no authority to borrow");
    }

    [Test]
    public async Task CommitAsync_without_the_options_seam_runs_as_the_ambient_caller()
    {
        var (engine, calls) = ObservedEngine();
        var participant = Participant(
            engine, FactoryFor(Substitute.For<ISagaWriteFenceGrain>()), membership: MembershipFor(TargetTree));
        await participant.PrepareAsync(OwnRequest());
        calls.Clear();

        await participant.CommitAsync(OwnRequest());

        Assert.That(calls.Select(c => c.Elevated), Is.All.False, "a missing seam is a refusal, never a pass");
    }

    [Test]
    public async Task AbortAsync_of_this_clusters_own_prepared_saga_compensates_with_the_prepares_authority()
    {
        var (engine, calls) = ObservedEngine();
        var participant = PeerParticipant(engine, [TargetTree], [CoordinatorCluster]);
        await participant.PrepareAsync(OwnRequest());
        calls.Clear();

        await participant.AbortAsync(OwnRequest());

        Assert.That(calls, Is.EqualTo(new[] { ("delete", true) }));
    }

    [Test]
    public async Task CommitSetAsync_of_this_clusters_own_prepared_set_cuts_over_with_the_prepares_authority()
    {
        var (engine, calls) = ObservedEngine();
        var participant = PeerParticipant(
            engine, [TreeA, TreeB], [CoordinatorCluster], setResolver: TwoMemberResolver());
        var request = SetRequestFor(SetId, coordinator: LocalCluster);
        await participant.PrepareAsync(request);
        calls.Clear();

        await participant.CommitAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(calls.Count(c => c.Call == "commit"), Is.EqualTo(2));
            Assert.That(calls.Select(c => c.Elevated), Is.All.True);
        });
    }

    [Test]
    public async Task CommitAsync_whose_cutover_fails_unblocks_local_writes_and_stays_retryable()
    {
        var engine = HealthyEngine();
        var fence = Substitute.For<ISagaWriteFenceGrain>();
        var participant = Participant(engine, FactoryFor(fence));
        await participant.PrepareAsync(Request());
        engine.CommitShadowAsync(Arg.Any<LatticeRestoreResult>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new LatticeAuthorizationDeniedException("denied")));

        Assert.That(async () => await participant.CommitAsync(Request()),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>(),
            "the failure reaches the participant grain, which keeps the saga prepared for redelivery");
        await fence.Received(1).EngageAsync(Arg.Any<SagaWriteFenceRequest>());
        await fence.Received(1).UnblockWritesAsync();

        engine.CommitShadowAsync(Arg.Any<LatticeRestoreResult>(), Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);
        await participant.CommitAsync(Request());

        await fence.Received(2).EngageAsync(Arg.Any<SagaWriteFenceRequest>());
        await engine.Received(2).CommitShadowAsync(Arg.Any<LatticeRestoreResult>(), Arg.Any<CancellationToken>());
        await fence.DidNotReceive().LiftAsync();
    }

    [Test]
    public async Task CommitSetAsync_whose_cutover_fails_unblocks_local_writes()
    {
        var engine = HealthyEngine();
        var fence = Substitute.For<ISagaWriteFenceGrain>();
        var participant = Participant(engine, FactoryFor(fence), setResolver: TwoMemberResolver());
        await participant.PrepareAsync(SetRequestFor(SetId));
        engine.CommitShadowAsync(Arg.Any<LatticeRestoreResult>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("swap faulted")));

        Assert.That(async () => await participant.CommitAsync(SetRequestFor(SetId)),
            Throws.InstanceOf<InvalidOperationException>());
        await fence.Received(1).UnblockWritesAsync();
    }

    [Test]
    public async Task CommitAsync_whose_cutover_fails_still_surfaces_the_failure_when_the_unblock_fails()
    {
        var engine = HealthyEngine();
        var fence = Substitute.For<ISagaWriteFenceGrain>();
        fence.UnblockWritesAsync().Returns(Task.FromException(new TimeoutException("fence unreachable")));
        var participant = Participant(engine, FactoryFor(fence));
        await participant.PrepareAsync(Request());
        engine.CommitShadowAsync(Arg.Any<LatticeRestoreResult>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("swap faulted")));

        Assert.That(async () => await participant.CommitAsync(Request()),
            Throws.InstanceOf<InvalidOperationException>().With.Message.EqualTo("swap faulted"),
            "the cutover's own failure, not the unblock's, reaches the participant grain");
    }
}
