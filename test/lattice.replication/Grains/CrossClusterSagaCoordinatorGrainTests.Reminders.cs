using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// The reminder, retention, and stall-detection half of
/// <see cref="CrossClusterSagaCoordinatorGrain"/>: the keepalive arms that fire on
/// terminal and never-started phases, the prepare-progress deadline that refuses
/// to fence a target tree forever behind an unresponsive participant, the
/// retention sweep, participant-set canonicalisation rejection, and the
/// reminder-registry faults the coordinator treats as non-fatal.
/// </summary>
public partial class CrossClusterSagaCoordinatorGrainTests
{
    private const string RetentionReminder = "saga-coordinator-retention";

    private static IReminderRegistry FaultingReminders(bool faultRegister = false, bool faultGet = false)
    {
        var reminders = Substitute.For<IReminderRegistry>();

        reminders.RegisterOrUpdateReminder(
                Arg.Any<GrainId>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>())
            .Returns(_ => faultRegister
                ? Task.FromException<IGrainReminder>(new InvalidOperationException("reminder service down"))
                : Task.FromResult(Substitute.For<IGrainReminder>()));

        reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(_ => faultGet
                ? Task.FromException<IGrainReminder?>(new InvalidOperationException("reminder service down"))
                : Task.FromResult<IGrainReminder?>(Substitute.For<IGrainReminder>()));

        return reminders;
    }

    /// <summary>Durable state parked mid-saga, as a crash leaves it.</summary>
    private static FakePersistentState<CrossClusterSagaCoordinatorState> PreparingState(
        params string[] clusters)
    {
        var state = new FakePersistentState<CrossClusterSagaCoordinatorState>();
        state.State.SagaId = SagaId;
        state.State.TargetTree = TargetTree;
        state.State.ManifestId = ManifestId;
        state.State.CoordinatorClusterId = CoordinatorCluster;
        state.State.StartedAtTicks = DateTime.UtcNow.Ticks;
        state.State.Participants = [.. clusters.Select(c => new CrossClusterSagaParticipantRef { ClusterId = c })];
        state.State.Phase = CrossClusterSagaPhase.Preparing;
        return state;
    }

    [Test]
    public async Task Retention_expiry_clears_the_coordinator_state()
    {
        var channel = Substitute.For<ISagaControlChannel>();
        StubPrepare(channel, "site-a", SagaVote.Commit);
        var (grain, state, _, _) = CreateGrain(channel: channel);
        await Run(grain, "site-a");

        Assert.That(state.State.Phase, Is.EqualTo(CrossClusterSagaPhase.Completed));

        await grain.ReceiveReminder(RetentionReminder, default);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.SagaId, Is.Empty);
            Assert.That(state.State.Phase, Is.EqualTo(CrossClusterSagaPhase.NotStarted));
            Assert.That(state.State.Outcome, Is.Null,
                "the retention sweep reclaims a decided coordinator's state rather than keeping every saga forever");
        });
    }

    [Test]
    public async Task A_keepalive_resume_fault_is_swallowed_so_the_reminder_keeps_beating()
    {
        var channel = Substitute.For<ISagaControlChannel>();
        channel.PrepareAsync("site-a", Arg.Any<SagaControlRequest>())
            .Returns(Task.FromException<SagaControlResponse>(new TimeoutException("still down")));
        var (grain, state, _, _) = CreateGrain(PreparingState("site-a"), channel);

        // A reminder tick that rethrew would be reported as a failed reminder and
        // the recovery attempt lost; the coordinator must simply try again next
        // beat.
        Assert.That(() => grain.ReceiveReminder(KeepaliveReminder, default), Throws.Nothing);

        Assert.That(state.State.Phase, Is.EqualTo(CrossClusterSagaPhase.Preparing),
            "and the saga stays resumable rather than being wrongly decided");
    }

    [Test]
    public async Task A_keepalive_after_completion_arms_retention_and_stands_down()
    {
        var reminders = FaultingReminders();
        var state = new FakePersistentState<CrossClusterSagaCoordinatorState>();
        state.State.SagaId = SagaId;
        state.State.Phase = CrossClusterSagaPhase.Completed;
        state.State.Outcome = CrossClusterSagaOutcome.Committed;
        var (grain, _, channel, _) = CreateGrain(state, reminderRegistry: reminders);

        // A crash between persisting Completed and arming retention leaves the
        // keepalive registered with nothing left to drive.
        await grain.ReceiveReminder(KeepaliveReminder, default);

        await reminders.Received().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), RetentionReminder, Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
        await reminders.Received().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
        await channel.DidNotReceive().PrepareAsync(Arg.Any<string>(), Arg.Any<SagaControlRequest>());
    }

    [Test]
    public async Task A_keepalive_on_a_never_started_coordinator_stands_down()
    {
        var reminders = FaultingReminders();
        var (grain, state, channel, _) = CreateGrain(reminderRegistry: reminders);

        await grain.ReceiveReminder(KeepaliveReminder, default);

        Assert.That(state.State.Phase, Is.EqualTo(CrossClusterSagaPhase.NotStarted));
        await reminders.Received().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
        await reminders.DidNotReceive().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), RetentionReminder, Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
        await channel.DidNotReceive().PrepareAsync(Arg.Any<string>(), Arg.Any<SagaControlRequest>());
    }

    [Test]
    public async Task The_prepare_progress_deadline_aborts_a_saga_no_participant_ever_answers()
    {
        var channel = Substitute.For<ISagaControlChannel>();
        StubPrepare(channel, "site-a", SagaVote.Commit);
        var state = PreparingState("site-a");

        // Past the coordinator's build deadline. Leaving it Preparing would hold
        // the participants' cutover fences open indefinitely, so the coordinator
        // gives up and decides Abort rather than retrying forever.
        state.State.StartedAtTicks = DateTime.UtcNow.AddHours(-2).Ticks;

        var (grain, _, _, _) = CreateGrain(state, channel);
        await grain.ReceiveReminder(KeepaliveReminder, default);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Outcome, Is.EqualTo(CrossClusterSagaOutcome.Aborted));
            Assert.That(state.State.FailureMessage, Does.Contain("prepare-progress deadline"));
        });
        await channel.DidNotReceive().PrepareAsync("site-a", Arg.Any<SagaControlRequest>());
    }

    [Test]
    public void An_empty_participant_cluster_id_is_rejected()
    {
        var (grain, _, _, _) = CreateGrain();

        // A blank id would canonicalise to a participant nothing can route to,
        // and the saga would stall on a vote that never arrives.
        Assert.ThrowsAsync<ArgumentException>(() =>
            grain.RunAsync(["site-a", string.Empty], TargetTree, ManifestId, CoordinatorCluster));
    }

    [Test]
    public void A_long_identifier_is_folded_into_the_fingerprint()
    {
        // Longer than the fingerprint's stack buffer, so the pooled-buffer arm of
        // the length-prefixed hash runs. A fingerprint that silently truncated
        // here would let two different targets re-attach to one in-flight saga id.
        var longTree = new string('t', 1024);
        var channel = Substitute.For<ISagaControlChannel>();
        channel.PrepareAsync("site-a", Arg.Any<SagaControlRequest>())
            .Returns(Task.FromException<SagaControlResponse>(new TimeoutException("transport")));
        var (grain, state, _, _) = CreateGrain(channel: channel);

        // Park the coordinator in-flight with the long target's fingerprint durable.
        Assert.ThrowsAsync<TimeoutException>(() =>
            grain.RunAsync(["site-a"], longTree, ManifestId, CoordinatorCluster));
        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(CrossClusterSagaPhase.Preparing));
            Assert.That(state.State.Fingerprint, Is.Not.Null.And.Not.Empty);
        });

        // A different long target of the same length is a different saga and must
        // be refused, not silently re-attached.
        Assert.ThrowsAsync<InvalidOperationException>(() =>
            grain.RunAsync(["site-a"], new string('u', 1024), ManifestId, CoordinatorCluster));

        // The identical long target is the same saga, so it passes the stability
        // check and resumes (faulting again on the still-broken transport).
        Assert.ThrowsAsync<TimeoutException>(() =>
            grain.RunAsync(["site-a"], longTree, ManifestId, CoordinatorCluster));
    }

    [Test]
    public async Task Registering_the_keepalive_survives_a_reminder_service_fault()
    {
        // The keepalive only accelerates crash recovery. Failing the saga because
        // the reminder service is down would turn a recoverable outage into a
        // refused cutover.
        var channel = Substitute.For<ISagaControlChannel>();
        StubPrepare(channel, "site-a", SagaVote.Commit);
        var (grain, state, _, _) = CreateGrain(
            channel: channel, reminderRegistry: FaultingReminders(faultRegister: true));

        var outcome = await Run(grain, "site-a");

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(CrossClusterSagaOutcome.Committed));
            Assert.That(state.State.Phase, Is.EqualTo(CrossClusterSagaPhase.Completed));
        });
    }

    [Test]
    public async Task Unregistering_the_keepalive_survives_a_reminder_service_fault()
    {
        var channel = Substitute.For<ISagaControlChannel>();
        StubPrepare(channel, "site-a", SagaVote.Commit);
        var (grain, state, _, _) = CreateGrain(
            channel: channel, reminderRegistry: FaultingReminders(faultGet: true));

        var outcome = await Run(grain, "site-a");

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(CrossClusterSagaOutcome.Committed),
                "a teardown fault must not undo a durable decision");
            Assert.That(state.State.Phase, Is.EqualTo(CrossClusterSagaPhase.Completed));
        });
    }
}
