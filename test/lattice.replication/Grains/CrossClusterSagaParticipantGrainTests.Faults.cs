using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// The retention, out-of-phase-decision, and non-fatal-fault half of
/// <see cref="CrossClusterSagaParticipantGrain"/>: an abort that arrives before
/// any prepare or after a commit, a local participant whose compensation faults,
/// the retention sweep, and the fence-reminder registry faults the participant
/// deliberately survives.
/// </summary>
public partial class CrossClusterSagaParticipantGrainTests
{
    private const string RetentionReminder = "saga-participant-retention";

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

    [Test]
    public async Task Abort_before_any_prepare_records_the_abort_durably()
    {
        var participant = new RecordingSagaParticipant();
        var (grain, state, _) = CreateGrain([participant]);

        // The coordinator decided abort before this cluster's prepare arrived (or
        // it never arrived at all). Recording the abort is what stops a late
        // prepare resurrecting a saga the coordinator has already given up on.
        var response = await grain.AbortAsync(Request());

        Assert.Multiple(() =>
        {
            Assert.That(response.Phase, Is.EqualTo(SagaPhase.Aborted));
            Assert.That(state.State.Phase, Is.EqualTo(SagaPhase.Aborted));
            Assert.That(state.State.Vote, Is.EqualTo(SagaVote.Abort));
            Assert.That(state.State.SagaId, Is.EqualTo(SagaId));
            Assert.That(state.State.Detail, Is.EqualTo("Aborted before prepare."));
            Assert.That(participant.AbortCount, Is.Zero, "nothing was prepared, so nothing is compensated");
        });

        // And a later prepare cannot resurrect it.
        var late = await grain.PrepareAsync(Request());
        Assert.Multiple(() =>
        {
            Assert.That(late.Vote, Is.EqualTo(SagaVote.Abort));
            Assert.That(participant.PrepareCount, Is.Zero);
        });
    }

    [Test]
    public async Task Abort_after_commit_is_refused_and_returns_the_durable_phase()
    {
        var participant = new RecordingSagaParticipant();
        var (grain, state, _) = CreateGrain([participant]);
        await grain.PrepareAsync(Request());
        await grain.CommitAsync(Request());

        // A commit cannot be un-committed. The participant reports the durable
        // phase so the coordinator observes the conflict rather than believing
        // the rollback happened.
        var response = await grain.AbortAsync(Request());

        Assert.Multiple(() =>
        {
            Assert.That(response.Phase, Is.EqualTo(SagaPhase.Committed));
            Assert.That(state.State.Phase, Is.EqualTo(SagaPhase.Committed));
            Assert.That(participant.AbortCount, Is.Zero, "the committed work is not rolled back");
            Assert.That(participant.CommitCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_faulting_local_compensation_does_not_strand_the_other_participants()
    {
        var faulting = Substitute.For<ISagaParticipant>();
        faulting.PrepareAsync(Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new SagaParticipantPrepareResult(SagaVote.Commit, null)));
        faulting.AbortAsync(Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("rollback storage unavailable")));
        var healthy = new RecordingSagaParticipant();

        var (grain, state, _) = CreateGrain([faulting, healthy]);
        await grain.PrepareAsync(Request());

        await grain.AbortAsync(Request());

        Assert.Multiple(() =>
        {
            Assert.That(healthy.AbortCount, Is.EqualTo(1),
                "one participant's rollback fault must not skip the rest of the compensation fan-out");
            Assert.That(state.State.Phase, Is.EqualTo(SagaPhase.Aborted));
        });
    }

    [Test]
    public async Task Retention_expiry_clears_the_participant_state()
    {
        var (grain, state, _) = CreateGrain([new RecordingSagaParticipant()]);
        await grain.PrepareAsync(Request());
        await grain.CommitAsync(Request());

        await grain.ReceiveReminder(RetentionReminder, default);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.SagaId, Is.Empty);
            Assert.That(state.State.Phase, Is.EqualTo(SagaPhase.None));
            Assert.That(state.State.FenceDeadlineTicks, Is.Zero);
        });
    }

    [Test]
    public async Task Arming_the_cutover_fence_survives_a_reminder_service_fault()
    {
        // The fence reminder is only the coordinator-loss safety net. Failing the
        // prepare because the reminder service is down would refuse a cutover
        // that the coordinator can still drive to a decision directly.
        var participant = new RecordingSagaParticipant();
        var (grain, state, _) = CreateGrain(
            [participant], reminderRegistry: FaultingReminders(faultRegister: true));

        var response = await grain.PrepareAsync(Request());

        Assert.Multiple(() =>
        {
            Assert.That(response.Vote, Is.EqualTo(SagaVote.Commit));
            Assert.That(state.State.Phase, Is.EqualTo(SagaPhase.Prepared));
            Assert.That(state.State.FenceDeadlineTicks, Is.GreaterThan(0),
                "the durable deadline is still recorded, so a later activation can still self-compensate");
        });
    }

    [Test]
    public async Task Cancelling_the_cutover_fence_survives_a_reminder_service_fault()
    {
        var participant = new RecordingSagaParticipant();
        var (grain, state, _) = CreateGrain(
            [participant], reminderRegistry: FaultingReminders(faultGet: true));
        await grain.PrepareAsync(Request());

        // The commit tears the fence down. A teardown fault must not fail the
        // commit, or the participant would report a conflict for work it did.
        var response = await grain.CommitAsync(Request());

        Assert.Multiple(() =>
        {
            Assert.That(response.Phase, Is.EqualTo(SagaPhase.Committed));
            Assert.That(state.State.Phase, Is.EqualTo(SagaPhase.Committed));
            Assert.That(participant.CommitCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task An_obsolete_fence_tick_survives_a_reminder_service_fault()
    {
        var participant = new RecordingSagaParticipant();
        var (grain, state, _) = CreateGrain(
            [participant], reminderRegistry: FaultingReminders(faultGet: true));
        await grain.PrepareAsync(Request());
        await grain.CommitAsync(Request());

        // A fence tick that races the decision finds a terminal phase and cancels
        // itself; the cancellation faulting must not auto-compensate committed work.
        Assert.That(() => grain.ReceiveReminder(FenceReminder, default), Throws.Nothing);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(SagaPhase.Committed));
            Assert.That(participant.AbortCount, Is.Zero);
        });
    }
}
