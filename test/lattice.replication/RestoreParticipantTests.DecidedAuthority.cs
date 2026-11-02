using NSubstitute;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Coverage of a decided restore saga that is delivered with no user identity
/// (issue #5506). This cluster's own coordinator redelivers a decision from its
/// keepalive reminder, and the participant from its fence expiry, so the commit or
/// abort of an already-prepared saga can arrive as <c>anonymous</c>. Such a step
/// runs with the authority of the prepare this participant ran - only when this
/// cluster's own coordinator has durably recorded the matching decision, and only
/// on the shadow that prepare built. A request that merely names this cluster as
/// its coordinator grants nothing: a commit the coordinator has not decided is
/// refused before any cutover. A cutover that fails never leaves local writes fenced.
/// </summary>
public partial class RestoreParticipantTests
{
    private static SagaControlRequest OwnRequest() => Request() with { CoordinatorClusterId = LocalCluster };

    private static CrossClusterSagaStatus SagaStatus(CrossClusterSagaPhase phase, CrossClusterSagaOutcome? outcome = null) =>
        new(phase, outcome, IsDriving: false, RequiresRepair: false, Detail: null);

    private static CrossClusterSagaStatus DecidedCommit(bool completed) => completed
        ? SagaStatus(CrossClusterSagaPhase.Completed, CrossClusterSagaOutcome.Committed)
        : SagaStatus(CrossClusterSagaPhase.Committed, CrossClusterSagaOutcome.Committed);

    private static CrossClusterSagaStatus DecidedAbort(bool completed) => completed
        ? SagaStatus(CrossClusterSagaPhase.Completed, CrossClusterSagaOutcome.Aborted)
        : SagaStatus(CrossClusterSagaPhase.Aborted, CrossClusterSagaOutcome.Aborted);

    /// <summary>
    /// A participant on <see cref="LocalCluster"/> whose own coordinator for every
    /// saga id reports <paramref name="status"/>, or faults its status read when
    /// <paramref name="status"/> is <c>null</c>.
    /// </summary>
    private static (RestoreParticipant Participant, ISagaWriteFenceGrain Fence, ICrossClusterSagaCoordinatorGrain Coordinator)
        OwnParticipant(
            ILatticeCoordinatedRestoreEngine engine,
            CrossClusterSagaStatus? status,
            string[]? replicated = null,
            ILatticeBackupSetResolver? setResolver = null)
    {
        var fence = Substitute.For<ISagaWriteFenceGrain>();
        var coordinator = Substitute.For<ICrossClusterSagaCoordinatorGrain>();
        if (status is { } known)
        {
            coordinator.GetStatusAsync().Returns(Task.FromResult(known));
        }
        else
        {
            coordinator.GetStatusAsync().Returns(
                Task.FromException<CrossClusterSagaStatus>(new TimeoutException("coordinator unreachable")));
        }

        var factory = FactoryFor(fence);
        factory.GetGrain<ICrossClusterSagaCoordinatorGrain>(Arg.Any<string>()).Returns(coordinator);
        var participant = Participant(
            engine,
            factory,
            setResolver: setResolver,
            membership: MembershipFor(replicated ?? [TargetTree]),
            options: OptionsFor(LocalCluster),
            topology: TopologyFor(CoordinatorCluster));
        return (participant, fence, coordinator);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task CommitAsync_of_a_prepared_saga_this_clusters_coordinator_decided_to_commit_cuts_over_with_the_prepares_authority(bool completed)
    {
        var (engine, calls) = ObservedEngine();
        var (participant, fence, _) = OwnParticipant(engine, DecidedCommit(completed));
        await participant.PrepareAsync(OwnRequest());
        calls.Clear();

        await participant.CommitAsync(OwnRequest());

        Assert.Multiple(() =>
        {
            Assert.That(calls, Is.EqualTo(new[] { ("commit", true) }),
                "a redelivered decision for the shadow this participant built is not re-authorized as anonymous");
            Assert.That(LatticeSystemOrigin.IsActive, Is.False, "the scope ends with the saga step");
        });
        await fence.Received(1).UnblockWritesAsync();
    }

    [Test]
    public async Task CommitAsync_naming_this_cluster_while_its_coordinator_is_undecided_is_refused_before_any_cutover()
    {
        var (engine, calls) = ObservedEngine();
        var (participant, fence, _) = OwnParticipant(engine, SagaStatus(CrossClusterSagaPhase.Preparing));
        await participant.PrepareAsync(OwnRequest());
        calls.Clear();

        Assert.That(async () => await participant.CommitAsync(OwnRequest()),
            Throws.InstanceOf<UnauthorizedAccessException>(),
            "a forged or early commit must not cut over a saga a peer may still abort");
        Assert.That(calls, Is.Empty, "no engine step ran");
        await fence.DidNotReceive().EngageAsync(Arg.Any<SagaWriteFenceRequest>());
    }

    [Test]
    public async Task CommitAsync_naming_this_cluster_for_a_saga_its_coordinator_never_started_is_refused()
    {
        var (engine, calls) = ObservedEngine();
        var (participant, fence, _) = OwnParticipant(engine, SagaStatus(CrossClusterSagaPhase.NotStarted));
        await participant.PrepareAsync(OwnRequest());
        calls.Clear();

        Assert.That(async () => await participant.CommitAsync(OwnRequest()),
            Throws.InstanceOf<UnauthorizedAccessException>(),
            "a saga this cluster does not coordinate is not its own, whatever the request names");
        Assert.That(calls, Is.Empty);
        await fence.DidNotReceive().EngageAsync(Arg.Any<SagaWriteFenceRequest>());
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task CommitAsync_of_a_saga_this_clusters_coordinator_decided_to_abort_is_refused(bool completed)
    {
        var (engine, calls) = ObservedEngine();
        var (participant, fence, _) = OwnParticipant(engine, DecidedAbort(completed));
        await participant.PrepareAsync(OwnRequest());
        calls.Clear();

        Assert.That(async () => await participant.CommitAsync(OwnRequest()),
            Throws.InstanceOf<UnauthorizedAccessException>());
        Assert.That(calls, Is.Empty);
        await fence.DidNotReceive().EngageAsync(Arg.Any<SagaWriteFenceRequest>());
    }

    [Test]
    public async Task CommitAsync_whose_coordinator_decision_cannot_be_read_is_refused()
    {
        var (engine, calls) = ObservedEngine();
        var (participant, fence, _) = OwnParticipant(engine, status: null);
        await participant.PrepareAsync(OwnRequest());
        calls.Clear();

        Assert.That(async () => await participant.CommitAsync(OwnRequest()),
            Throws.InstanceOf<UnauthorizedAccessException>(), "an unreadable decision fails closed");
        Assert.That(calls, Is.Empty);
        await fence.DidNotReceive().EngageAsync(Arg.Any<SagaWriteFenceRequest>());
    }

    [Test]
    public async Task CommitAsync_of_a_decided_saga_that_must_rebuild_runs_as_the_ambient_caller()
    {
        var (engine, calls) = ObservedEngine();
        var (participant, _, _) = OwnParticipant(engine, DecidedCommit(completed: false));

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
    public async Task CommitAsync_from_a_peer_never_reads_this_clusters_coordinator()
    {
        var (engine, _) = ObservedEngine();
        var (participant, _, coordinator) = OwnParticipant(engine, status: null);
        await participant.PrepareAsync(Request());

        await participant.CommitAsync(Request());

        await coordinator.DidNotReceive().GetStatusAsync();
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task AbortAsync_of_a_prepared_saga_this_clusters_coordinator_decided_to_abort_compensates_with_the_prepares_authority(bool completed)
    {
        var (engine, calls) = ObservedEngine();
        var (participant, fence, _) = OwnParticipant(engine, DecidedAbort(completed));
        await participant.PrepareAsync(OwnRequest());
        calls.Clear();

        await participant.AbortAsync(OwnRequest());

        Assert.That(calls, Is.EqualTo(new[] { ("delete", true) }));
        await fence.Received(1).LiftAsync();
    }

    [Test]
    public async Task AbortAsync_this_clusters_coordinator_has_not_decided_runs_as_the_ambient_caller()
    {
        var (engine, calls) = ObservedEngine();
        var (participant, fence, _) = OwnParticipant(engine, DecidedCommit(completed: false));
        await participant.PrepareAsync(OwnRequest());
        calls.Clear();

        await participant.AbortAsync(OwnRequest());

        Assert.That(calls.Select(c => c.Elevated), Is.All.False,
            "an abort against the recorded decision is an operator's repair, authorized as that operator");
        await fence.Received(1).LiftAsync();
    }

    [Test]
    public async Task AbortAsync_whose_coordinator_decision_cannot_be_read_runs_as_the_ambient_caller()
    {
        var (engine, calls) = ObservedEngine();
        var (participant, fence, _) = OwnParticipant(engine, status: null);
        await participant.PrepareAsync(OwnRequest());
        calls.Clear();

        await participant.AbortAsync(OwnRequest());

        Assert.That(calls.Select(c => c.Elevated), Is.All.False, "an unreadable decision fails closed");
        await fence.Received(1).LiftAsync();
    }

    [Test]
    public async Task CommitSetAsync_of_a_prepared_set_this_clusters_coordinator_decided_to_commit_cuts_over_with_the_prepares_authority()
    {
        var (engine, calls) = ObservedEngine();
        var (participant, _, _) = OwnParticipant(
            engine, DecidedCommit(completed: false), [TreeA, TreeB], TwoMemberResolver());
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
    public async Task CommitSetAsync_naming_this_cluster_while_its_coordinator_is_undecided_is_refused_before_any_cutover()
    {
        var (engine, calls) = ObservedEngine();
        var (participant, fence, _) = OwnParticipant(
            engine, SagaStatus(CrossClusterSagaPhase.Preparing), [TreeA, TreeB], TwoMemberResolver());
        var request = SetRequestFor(SetId, coordinator: LocalCluster);
        await participant.PrepareAsync(request);
        calls.Clear();

        Assert.That(async () => await participant.CommitAsync(request), Throws.InstanceOf<UnauthorizedAccessException>());
        Assert.That(calls, Is.Empty);
        await fence.DidNotReceive().EngageAsync(Arg.Any<SagaWriteFenceRequest>());
    }

    [Test]
    public async Task AbortSetAsync_of_a_prepared_set_this_clusters_coordinator_decided_to_abort_compensates_with_the_prepares_authority()
    {
        var (engine, calls) = ObservedEngine();
        var (participant, fence, _) = OwnParticipant(
            engine, DecidedAbort(completed: false), [TreeA, TreeB], TwoMemberResolver());
        var request = SetRequestFor(SetId, coordinator: LocalCluster);
        await participant.PrepareAsync(request);
        calls.Clear();

        await participant.AbortAsync(request);

        Assert.Multiple(() =>
        {
            Assert.That(calls.Count(c => c.Call == "delete"), Is.EqualTo(2));
            Assert.That(calls.Select(c => c.Elevated), Is.All.True);
        });
        await fence.Received(1).LiftAsync();
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
