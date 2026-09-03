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
/// Unit coverage for <see cref="CrossClusterSagaCoordinatorGrain"/>, the
/// durable cross-cluster saga coordinator. Drives the phase machine against a
/// substitute <see cref="ISagaControlChannel"/> so unanimous-commit, abort,
/// idempotency, and reminder-driven crash-resume paths are exercised without a
/// silo.
/// </summary>
[TestFixture]
public partial class CrossClusterSagaCoordinatorGrainTests
{
    private const string SagaId = "saga-1";
    private const string TargetTree = "orders";
    private const string ManifestId = "manifest-1";
    private const string CoordinatorCluster = "site-home";
    private const string KeepaliveReminder = "saga-coordinator-keepalive";

    private static (CrossClusterSagaCoordinatorGrain grain,
                    FakePersistentState<CrossClusterSagaCoordinatorState> state,
                    ISagaControlChannel channel,
                    IReminderRegistry reminders) CreateGrain(
        FakePersistentState<CrossClusterSagaCoordinatorState>? existingState = null,
        ISagaControlChannel? channel = null,
        IReminderRegistry? reminderRegistry = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("saga-coordinator", SagaId));

        channel ??= Substitute.For<ISagaControlChannel>();

        var reminders = reminderRegistry ?? Substitute.For<IReminderRegistry>();
        if (reminderRegistry is null)
        {
            reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
                .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));
        }

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(new LatticeOptions());

        var state = existingState ?? new FakePersistentState<CrossClusterSagaCoordinatorState>();
        var grain = new CrossClusterSagaCoordinatorGrain(
            context, channel, reminders, optionsMonitor,
            NullLogger<CrossClusterSagaCoordinatorGrain>.Instance, state);
        return (grain, state, channel, reminders);
    }

    private static SagaControlResponse Vote(SagaVote vote, string? detail = null) => new()
    {
        SagaId = SagaId,
        Phase = vote == SagaVote.Commit ? SagaPhase.Prepared : SagaPhase.Aborted,
        Vote = vote,
        Detail = detail ?? string.Empty,
    };

    private static void StubPrepare(ISagaControlChannel channel, string clusterId, SagaVote vote) =>
        channel.PrepareAsync(clusterId, Arg.Any<SagaControlRequest>()).Returns(Task.FromResult(Vote(vote)));

    private Task<CrossClusterSagaOutcome> Run(CrossClusterSagaCoordinatorGrain grain, params string[] clusters) =>
        grain.RunAsync([.. clusters], TargetTree, ManifestId, CoordinatorCluster);

    [Test]
    public async Task RunAsync_all_commit_commits_and_finalizes_with_commit()
    {
        var channel = Substitute.For<ISagaControlChannel>();
        StubPrepare(channel, "site-a", SagaVote.Commit);
        StubPrepare(channel, "site-b", SagaVote.Commit);
        var (grain, state, _, _) = CreateGrain(channel: channel);

        var outcome = await Run(grain, "site-a", "site-b");

        Assert.That(outcome, Is.EqualTo(CrossClusterSagaOutcome.Committed));
        Assert.That(state.State.Phase, Is.EqualTo(CrossClusterSagaPhase.Completed));
        await channel.Received(1).CommitAsync("site-a", Arg.Any<SagaControlRequest>());
        await channel.Received(1).CommitAsync("site-b", Arg.Any<SagaControlRequest>());
        await channel.DidNotReceive().AbortAsync(Arg.Any<string>(), Arg.Any<SagaControlRequest>());
    }

    [Test]
    public async Task RunAsync_one_abort_aborts_and_compensates_only_prepared_participants()
    {
        var channel = Substitute.For<ISagaControlChannel>();
        StubPrepare(channel, "site-a", SagaVote.Commit);
        StubPrepare(channel, "site-b", SagaVote.Abort);
        var (grain, state, _, _) = CreateGrain(channel: channel);

        var outcome = await Run(grain, "site-a", "site-b");

        Assert.That(outcome, Is.EqualTo(CrossClusterSagaOutcome.Aborted));
        Assert.That(state.State.Phase, Is.EqualTo(CrossClusterSagaPhase.Completed));
        // Only the prepared participant has staged state to compensate; the one
        // that voted Abort already self-terminated locally.
        await channel.Received(1).AbortAsync("site-a", Arg.Any<SagaControlRequest>());
        await channel.DidNotReceive().AbortAsync("site-b", Arg.Any<SagaControlRequest>());
        await channel.DidNotReceive().CommitAsync(Arg.Any<string>(), Arg.Any<SagaControlRequest>());
    }

    [Test]
    public async Task RunAsync_empty_participants_is_vacuous_commit()
    {
        var (grain, state, channel, _) = CreateGrain();

        var outcome = await grain.RunAsync([], TargetTree, ManifestId, CoordinatorCluster);

        Assert.That(outcome, Is.EqualTo(CrossClusterSagaOutcome.Committed));
        Assert.That(state.State.Phase, Is.EqualTo(CrossClusterSagaPhase.Completed));
        await channel.DidNotReceive().PrepareAsync(Arg.Any<string>(), Arg.Any<SagaControlRequest>());
    }

    [Test]
    public void RunAsync_null_participants_throws()
    {
        var (grain, _, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentNullException>(() =>
            grain.RunAsync(null!, TargetTree, ManifestId, CoordinatorCluster));
    }

    [Test]
    public void RunAsync_empty_coordinator_cluster_throws()
    {
        var (grain, _, _, _) = CreateGrain();
        Assert.ThrowsAsync<ArgumentException>(() =>
            grain.RunAsync(["site-a"], TargetTree, ManifestId, string.Empty));
    }

    [Test]
    public async Task RunAsync_resubmit_same_arguments_returns_memoized_without_reprepare()
    {
        var channel = Substitute.For<ISagaControlChannel>();
        StubPrepare(channel, "site-a", SagaVote.Commit);
        var (grain, _, _, _) = CreateGrain(channel: channel);

        await Run(grain, "site-a");
        var second = await Run(grain, "site-a");

        Assert.That(second, Is.EqualTo(CrossClusterSagaOutcome.Committed));
        await channel.Received(1).PrepareAsync("site-a", Arg.Any<SagaControlRequest>());
    }

    [Test]
    public async Task RunAsync_resubmit_different_participants_throws()
    {
        var channel = Substitute.For<ISagaControlChannel>();
        StubPrepare(channel, "site-a", SagaVote.Commit);
        var state = new FakePersistentState<CrossClusterSagaCoordinatorState>();
        var (grain, _, _, _) = CreateGrain(state, channel);

        await Run(grain, "site-a");

        // Force the in-flight stability branch, then re-submit a changed set.
        state.State.Phase = CrossClusterSagaPhase.Preparing;
        Assert.ThrowsAsync<InvalidOperationException>(() => Run(grain, "site-a", "site-b"));
    }

    [Test]
    public async Task GetDecisionAsync_reflects_phase()
    {
        var (grain, state, _, _) = CreateGrain();

        state.State.Phase = CrossClusterSagaPhase.Preparing;
        Assert.That(await grain.GetDecisionAsync(), Is.EqualTo(CrossClusterSagaDecision.InFlight));

        state.State.Phase = CrossClusterSagaPhase.Committed;
        Assert.That(await grain.GetDecisionAsync(), Is.EqualTo(CrossClusterSagaDecision.Committed));

        state.State.Phase = CrossClusterSagaPhase.Aborted;
        Assert.That(await grain.GetDecisionAsync(), Is.EqualTo(CrossClusterSagaDecision.Aborted));

        state.State.Phase = CrossClusterSagaPhase.Completed;
        state.State.Outcome = CrossClusterSagaOutcome.Committed;
        Assert.That(await grain.GetDecisionAsync(), Is.EqualTo(CrossClusterSagaDecision.Committed));

        state.State.Outcome = CrossClusterSagaOutcome.Aborted;
        Assert.That(await grain.GetDecisionAsync(), Is.EqualTo(CrossClusterSagaDecision.Aborted));
    }

    [Test]
    public async Task IsCompleteAsync_true_when_terminal_or_never_started()
    {
        var (grain, state, _, _) = CreateGrain();
        Assert.That(await grain.IsCompleteAsync(), Is.True);

        state.State.Phase = CrossClusterSagaPhase.Preparing;
        Assert.That(await grain.IsCompleteAsync(), Is.False);

        state.State.Phase = CrossClusterSagaPhase.Completed;
        Assert.That(await grain.IsCompleteAsync(), Is.True);
    }

    [Test]
    public async Task Keepalive_reminder_resumes_prepare_after_transport_fault()
    {
        var channel = Substitute.For<ISagaControlChannel>();
        // First prepare dispatch faults; the coordinator stays Preparing.
        channel.PrepareAsync("site-a", Arg.Any<SagaControlRequest>())
            .Returns(Task.FromException<SagaControlResponse>(new TimeoutException("transport")));
        var (grain, state, _, _) = CreateGrain(channel: channel);

        Assert.ThrowsAsync<TimeoutException>(() => Run(grain, "site-a"));
        Assert.That(state.State.Phase, Is.EqualTo(CrossClusterSagaPhase.Preparing));

        // Recovery: clear the fault and let the keepalive reminder drive resume.
        channel.PrepareAsync("site-a", Arg.Any<SagaControlRequest>()).Returns(Task.FromResult(Vote(SagaVote.Commit)));
        await grain.ReceiveReminder(KeepaliveReminder, default);

        Assert.That(state.State.Phase, Is.EqualTo(CrossClusterSagaPhase.Completed));
        Assert.That(state.State.Outcome, Is.EqualTo(CrossClusterSagaOutcome.Committed));
        await channel.Received(1).CommitAsync("site-a", Arg.Any<SagaControlRequest>());
    }

    [Test]
    public async Task Keepalive_reminder_resumes_finalize_after_crash_between_decision_and_finalize()
    {
        var channel = Substitute.For<ISagaControlChannel>();
        StubPrepare(channel, "site-a", SagaVote.Commit);
        StubPrepare(channel, "site-b", SagaVote.Commit);
        // Finalize fan-out faults on the first participant, parking the
        // coordinator at Committed with the decision already durable.
        channel.CommitAsync("site-a", Arg.Any<SagaControlRequest>())
            .Returns(Task.FromException<SagaControlResponse>(new TimeoutException("crash")));
        var (grain, state, _, _) = CreateGrain(channel: channel);

        Assert.ThrowsAsync<TimeoutException>(() => Run(grain, "site-a", "site-b"));
        Assert.That(state.State.Phase, Is.EqualTo(CrossClusterSagaPhase.Committed),
            "a crash mid-finalize must leave the durable Committed decision in place");

        // Recovery: clear the fault; the keepalive reminder drives finalize to completion.
        channel.CommitAsync("site-a", Arg.Any<SagaControlRequest>()).Returns(Task.FromResult(Vote(SagaVote.None)));
        await grain.ReceiveReminder(KeepaliveReminder, default);

        Assert.That(state.State.Phase, Is.EqualTo(CrossClusterSagaPhase.Completed));
    }

    [Test]
    public async Task Reactivation_resumes_from_persisted_preparing_phase_to_terminal()
    {
        // Simulate a coordinator crash mid-saga: a fresh activation over the
        // same durable state (Phase = Preparing) reaches a terminal decision
        // when the keepalive reminder fires on the new activation.
        var channel = Substitute.For<ISagaControlChannel>();
        StubPrepare(channel, "site-a", SagaVote.Commit);
        StubPrepare(channel, "site-b", SagaVote.Commit);

        var state = new FakePersistentState<CrossClusterSagaCoordinatorState>();
        state.State.SagaId = SagaId;
        state.State.TargetTree = TargetTree;
        state.State.ManifestId = ManifestId;
        state.State.CoordinatorClusterId = CoordinatorCluster;
        state.State.StartedAtTicks = DateTime.UtcNow.Ticks;
        state.State.Participants =
        [
            new CrossClusterSagaParticipantRef { ClusterId = "site-a" },
            new CrossClusterSagaParticipantRef { ClusterId = "site-b" },
        ];
        state.State.Phase = CrossClusterSagaPhase.Preparing;

        var (grain, _, _, _) = CreateGrain(state, channel);

        await grain.ReceiveReminder(KeepaliveReminder, default);

        Assert.That(state.State.Phase, Is.EqualTo(CrossClusterSagaPhase.Completed));
        Assert.That(state.State.Outcome, Is.EqualTo(CrossClusterSagaOutcome.Committed));
        await channel.Received(1).CommitAsync("site-a", Arg.Any<SagaControlRequest>());
        await channel.Received(1).CommitAsync("site-b", Arg.Any<SagaControlRequest>());
    }
}
