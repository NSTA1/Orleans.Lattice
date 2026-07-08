using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Unit coverage for <see cref="CrossClusterSagaParticipantGrain"/>, the durable
/// participant model. Drives prepare / commit / abort, the coordinator-loss
/// fence-expiry auto-compensation, and idempotency against recording
/// <see cref="ISagaParticipant"/> doubles, without a silo.
/// </summary>
[TestFixture]
public class CrossClusterSagaParticipantGrainTests
{
    private const string SagaId = "saga-1";
    private const string FenceReminder = "saga-participant-fence";

    private static (CrossClusterSagaParticipantGrain grain,
                    FakePersistentState<CrossClusterSagaParticipantState> state,
                    IReminderRegistry reminders) CreateGrain(
        IEnumerable<ISagaParticipant> participants,
        FakePersistentState<CrossClusterSagaParticipantState>? existingState = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("saga-participant", SagaId));

        var reminders = Substitute.For<IReminderRegistry>();
        reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(new LatticeOptions());

        var state = existingState ?? new FakePersistentState<CrossClusterSagaParticipantState>();
        var grain = new CrossClusterSagaParticipantGrain(
            context, participants, reminders, optionsMonitor,
            NullLogger<CrossClusterSagaParticipantGrain>.Instance, state);
        return (grain, state, reminders);
    }

    private static SagaControlRequest Request() => new()
    {
        SagaId = SagaId,
        TargetTree = "orders",
        ManifestId = "manifest-1",
        CoordinatorClusterId = "site-home",
    };

    [Test]
    public async Task PrepareAsync_all_prepared_votes_commit_and_arms_fence()
    {
        var p1 = new RecordingSagaParticipant();
        var p2 = new RecordingSagaParticipant();
        var (grain, state, reminders) = CreateGrain([p1, p2]);

        var response = await grain.PrepareAsync(Request());

        Assert.That(response.Vote, Is.EqualTo(SagaVote.Commit));
        Assert.That(response.Phase, Is.EqualTo(SagaPhase.Prepared));
        Assert.That(state.State.FenceDeadlineTicks, Is.GreaterThan(0));
        Assert.That(p1.PrepareCount, Is.EqualTo(1));
        Assert.That(p2.PrepareCount, Is.EqualTo(1));
        await reminders.Received().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), FenceReminder, Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task PrepareAsync_participant_declines_votes_abort_and_compensates_prepared_subset()
    {
        var committing = new RecordingSagaParticipant(SagaVote.Commit);
        var declining = new RecordingSagaParticipant(SagaVote.Abort, "precondition failed");
        var (grain, state, reminders) = CreateGrain([committing, declining]);

        var response = await grain.PrepareAsync(Request());

        Assert.That(response.Vote, Is.EqualTo(SagaVote.Abort));
        Assert.That(state.State.Phase, Is.EqualTo(SagaPhase.Aborted));
        // The participant that prepared is compensated so no prepared state leaks.
        Assert.That(committing.AbortCount, Is.EqualTo(1));
        // No fence is armed on an abort vote.
        await reminders.DidNotReceive().RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), FenceReminder, Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
    }

    [Test]
    public async Task PrepareAsync_no_local_participant_votes_abort()
    {
        var (grain, state, _) = CreateGrain([]);

        var response = await grain.PrepareAsync(Request());

        Assert.That(response.Vote, Is.EqualTo(SagaVote.Abort));
        Assert.That(state.State.Phase, Is.EqualTo(SagaPhase.Aborted));
    }

    [Test]
    public async Task PrepareAsync_duplicate_returns_recorded_vote_without_rerun()
    {
        var p1 = new RecordingSagaParticipant();
        var (grain, _, _) = CreateGrain([p1]);

        await grain.PrepareAsync(Request());
        var second = await grain.PrepareAsync(Request());

        Assert.That(second.Vote, Is.EqualTo(SagaVote.Commit));
        Assert.That(p1.PrepareCount, Is.EqualTo(1), "duplicate prepare must not re-run the participant");
    }

    [Test]
    public async Task CommitAsync_delivers_commit_and_cancels_fence()
    {
        var p1 = new RecordingSagaParticipant();
        var (grain, state, reminders) = CreateGrain([p1]);
        await grain.PrepareAsync(Request());

        var response = await grain.CommitAsync(Request());

        Assert.That(response.Phase, Is.EqualTo(SagaPhase.Committed));
        Assert.That(p1.CommitCount, Is.EqualTo(1));
        Assert.That(state.State.FenceDeadlineTicks, Is.EqualTo(0));
        await reminders.Received().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    [Test]
    public async Task CommitAsync_is_idempotent()
    {
        var p1 = new RecordingSagaParticipant();
        var (grain, _, _) = CreateGrain([p1]);
        await grain.PrepareAsync(Request());

        await grain.CommitAsync(Request());
        await grain.CommitAsync(Request());

        Assert.That(p1.CommitCount, Is.EqualTo(1), "duplicate commit must not double-apply");
    }

    [Test]
    public async Task AbortAsync_compensates_and_records_aborted()
    {
        var p1 = new RecordingSagaParticipant();
        var (grain, state, _) = CreateGrain([p1]);
        await grain.PrepareAsync(Request());

        var response = await grain.AbortAsync(Request());

        Assert.That(response.Phase, Is.EqualTo(SagaPhase.Aborted));
        Assert.That(p1.AbortCount, Is.EqualTo(1));
        Assert.That(state.State.FenceDeadlineTicks, Is.EqualTo(0));
    }

    [Test]
    public async Task AbortAsync_is_idempotent()
    {
        var p1 = new RecordingSagaParticipant();
        var (grain, _, _) = CreateGrain([p1]);
        await grain.PrepareAsync(Request());

        await grain.AbortAsync(Request());
        await grain.AbortAsync(Request());

        Assert.That(p1.AbortCount, Is.EqualTo(1), "duplicate abort must not double-compensate");
    }

    [Test]
    public async Task CommitAsync_after_abort_does_not_apply()
    {
        var p1 = new RecordingSagaParticipant();
        var (grain, _, _) = CreateGrain([p1]);
        await grain.PrepareAsync(Request());
        await grain.AbortAsync(Request());

        var response = await grain.CommitAsync(Request());

        Assert.That(response.Phase, Is.EqualTo(SagaPhase.Aborted));
        Assert.That(p1.CommitCount, Is.EqualTo(0), "a commit after abort must not be applied");
    }

    [Test]
    public async Task Fence_expiry_auto_compensates_on_coordinator_loss()
    {
        var p1 = new RecordingSagaParticipant();
        var (grain, state, _) = CreateGrain([p1]);
        await grain.PrepareAsync(Request());

        // The coordinator never delivers a decision: move the fence deadline
        // into the past and fire the fence reminder.
        state.State.FenceDeadlineTicks = DateTime.UtcNow.Ticks - TimeSpan.FromMinutes(1).Ticks;
        await grain.ReceiveReminder(FenceReminder, default);

        Assert.That(state.State.Phase, Is.EqualTo(SagaPhase.Aborted));
        Assert.That(p1.AbortCount, Is.EqualTo(1), "fence expiry must auto-compensate the prepared participant");
        Assert.That(state.State.Detail, Does.Contain("fence"));
    }

    [Test]
    public async Task Fence_reminder_before_deadline_is_a_noop()
    {
        var p1 = new RecordingSagaParticipant();
        var (grain, state, _) = CreateGrain([p1]);
        await grain.PrepareAsync(Request());

        // Deadline is in the future: the reminder tick must not compensate.
        await grain.ReceiveReminder(FenceReminder, default);

        Assert.That(state.State.Phase, Is.EqualTo(SagaPhase.Prepared));
        Assert.That(p1.AbortCount, Is.EqualTo(0));
    }

    [Test]
    public async Task Fence_reminder_when_terminal_unregisters_without_compensating()
    {
        var p1 = new RecordingSagaParticipant();
        var (grain, state, reminders) = CreateGrain([p1]);
        await grain.PrepareAsync(Request());
        await grain.CommitAsync(Request());
        reminders.ClearReceivedCalls();

        await grain.ReceiveReminder(FenceReminder, default);

        Assert.That(state.State.Phase, Is.EqualTo(SagaPhase.Committed));
        Assert.That(p1.AbortCount, Is.EqualTo(0));
        await reminders.Received().UnregisterReminder(Arg.Any<GrainId>(), Arg.Any<IGrainReminder>());
    }

    [Test]
    public async Task GetStatusAsync_returns_durable_phase()
    {
        var p1 = new RecordingSagaParticipant();
        var (grain, _, _) = CreateGrain([p1]);
        await grain.PrepareAsync(Request());

        var response = await grain.GetStatusAsync(Request());

        Assert.That(response.Phase, Is.EqualTo(SagaPhase.Prepared));
    }
}
