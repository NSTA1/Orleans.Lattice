using Microsoft.Extensions.DependencyInjection;
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
/// Issue #4637, end to end over real <see cref="CrossClusterSagaCoordinatorGrain"/>
/// and <see cref="CrossClusterSagaParticipantGrain"/> activations: a commit that
/// reaches one participant only after its cutover-fence timer fired used to find
/// it already compensated, and the coordinator counted that refusal as success,
/// so a coordinated restore ended with one cluster on the restored copy and
/// another on its pre-restore tree. A prepared participant now asks the
/// coordinator for its decision when the timer fires, keeps its fence while the
/// coordinator is unreachable, and the coordinator never reports a refused
/// commit as a committed saga.
/// </summary>
[TestFixture]
[NonParallelizable]
public class CrossClusterSagaFenceDecisionTests
{
    private const string SagaId = "saga-4637";
    private const string TargetTree = "orders";
    private const string ManifestId = "manifest-1";
    private const string CoordinatorCluster = "site-home";
    private const string FenceReminder = "saga-participant-fence";

    [SetUp]
    public void SetUp() => SagaParticipantFenceCensus.ResetForTest();

    [TearDown]
    public void TearDown() => SagaParticipantFenceCensus.ResetForTest();

    private sealed record Participant(
        CrossClusterSagaParticipantGrain Grain,
        FakePersistentState<CrossClusterSagaParticipantState> State,
        RecordingSagaParticipant Local);

    private static Participant CreateParticipant(string clusterId, InProcessSagaControlChannel channel, SagaVote vote = SagaVote.Commit)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("saga-participant", SagaId));
        context.ActivationServices.Returns(new ServiceCollection()
            .AddSingleton(channel.ViewFrom(clusterId))
            .BuildServiceProvider());

        var reminders = Substitute.For<IReminderRegistry>();
        reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(new LatticeOptions());

        var local = new RecordingSagaParticipant(vote);
        var state = new FakePersistentState<CrossClusterSagaParticipantState>();
        var grain = new CrossClusterSagaParticipantGrain(
            context, [local], reminders, optionsMonitor,
            NullLogger<CrossClusterSagaParticipantGrain>.Instance, state);
        channel.Register(clusterId, grain);
        return new Participant(grain, state, local);
    }

    private static (CrossClusterSagaCoordinatorGrain Grain, FakePersistentState<CrossClusterSagaCoordinatorState> State)
        CreateCoordinator(InProcessSagaControlChannel channel)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("saga-coordinator", SagaId));

        var reminders = Substitute.For<IReminderRegistry>();
        reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(new LatticeOptions());

        var state = new FakePersistentState<CrossClusterSagaCoordinatorState>();
        var grain = new CrossClusterSagaCoordinatorGrain(
            context, channel, reminders, optionsMonitor,
            NullLogger<CrossClusterSagaCoordinatorGrain>.Instance, state);
        channel.RegisterCoordinator(CoordinatorCluster, grain);
        return (grain, state);
    }

    /// <summary>Moves <paramref name="participant"/>'s fence deadline into the past and fires its fence reminder.</summary>
    private static async Task ExpireFenceAsync(Participant participant)
    {
        participant.State.State.FenceDeadlineTicks = DateTime.UtcNow.Ticks - TimeSpan.FromMinutes(1).Ticks;
        await participant.Grain.ReceiveReminder(FenceReminder, default);
    }

    private static async Task WithTimeout(Task task, string because)
    {
        if (await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(30))) != task)
            Assert.Fail(because);
        await task;
    }

    [Test]
    public async Task A_commit_that_outlives_a_participants_fence_timer_still_commits_every_cluster()
    {
        var channel = new InProcessSagaControlChannel();
        var a = CreateParticipant("site-a", channel);
        var b = CreateParticipant("site-b", channel);
        var (coordinator, _) = CreateCoordinator(channel);
        var bCommitReached = channel.HoldCommit("site-b");

        var run = coordinator.RunAsync(["site-a", "site-b"], TargetTree, ManifestId, CoordinatorCluster);
        await WithTimeout(bCommitReached, "the commit for site-b never reached the channel");

        // The commit is delivered to site-a and decided, but held on its way to
        // site-b past site-b's cutover-fence window.
        await ExpireFenceAsync(b);

        channel.ReleaseCommit("site-b");
        var outcome = await run;

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.EqualTo(CrossClusterSagaOutcome.Committed));
            Assert.That(a.Local.CommitCount, Is.EqualTo(1));
            Assert.That(b.Local.CommitCount, Is.EqualTo(1),
                "site-b must commit the restore the coordinator committed, not compensate on its timer");
            Assert.That(b.Local.AbortCount, Is.Zero);
            Assert.That(b.State.State.Phase, Is.EqualTo(SagaPhase.Committed));
        });
    }

    [Test]
    public async Task A_participant_whose_fence_expires_while_its_coordinator_is_unreachable_keeps_the_fence()
    {
        var channel = new InProcessSagaControlChannel();
        var a = CreateParticipant("site-a", channel);
        var b = CreateParticipant("site-b", channel);
        var (coordinator, _) = CreateCoordinator(channel);
        var bCommitReached = channel.HoldCommit("site-b");

        var run = coordinator.RunAsync(["site-a", "site-b"], TargetTree, ManifestId, CoordinatorCluster);
        await WithTimeout(bCommitReached, "the commit for site-b never reached the channel");

        channel.CoordinatorUnreachable = true;
        await ExpireFenceAsync(b);

        Assert.Multiple(() =>
        {
            Assert.That(b.State.State.Phase, Is.EqualTo(SagaPhase.Prepared),
                "a participant that voted commit must not compensate while it cannot learn the decision");
            Assert.That(b.Local.AbortCount, Is.Zero);
            Assert.That(SagaParticipantFenceCensus.OldestAgeSeconds(SagaParticipantFenceCensus.ReasonCoordinatorUnreachable),
                Is.GreaterThanOrEqualTo(60), "the held fence's age past its window is published");
        });

        // The coordinator's delivery still lands, and the saga completes whole.
        channel.ReleaseCommit("site-b");
        Assert.That(await run, Is.EqualTo(CrossClusterSagaOutcome.Committed));
        Assert.Multiple(() =>
        {
            Assert.That(a.Local.CommitCount, Is.EqualTo(1));
            Assert.That(b.Local.CommitCount, Is.EqualTo(1));
            Assert.That(SagaParticipantFenceCensus.OldestAgeSeconds(SagaParticipantFenceCensus.ReasonCoordinatorUnreachable),
                Is.Null, "a resolved participant leaves the census");
        });
    }

    [Test]
    public async Task A_participant_whose_fence_expires_after_an_abort_decision_compensates()
    {
        var channel = new InProcessSagaControlChannel();
        var a = CreateParticipant("site-a", channel);
        var b = CreateParticipant("site-b", channel, SagaVote.Abort);
        var (coordinator, _) = CreateCoordinator(channel);

        // site-b votes abort, so the coordinator decides abort; the abort's
        // delivery to site-a is lost, and site-a's fence expires.
        Assert.That(await coordinator.RunAsync(["site-a", "site-b"], TargetTree, ManifestId, CoordinatorCluster),
            Is.EqualTo(CrossClusterSagaOutcome.Aborted));
        Assert.That(a.Local.AbortCount, Is.EqualTo(1), "PRECONDITION: the coordinator delivered its abort");

        var c = CreateParticipant("site-c", channel);
        await c.Grain.PrepareAsync(new SagaControlRequest { SagaId = SagaId, TargetTree = TargetTree, ManifestId = ManifestId, CoordinatorClusterId = CoordinatorCluster });
        Assert.ThrowsAsync<UnauthorizedAccessException>(() => coordinator.ResolveDecisionForParticipantAsync("site-c"),
            "a cluster that is not one of the saga's participants is refused");
    }

    [Test]
    public async Task The_coordinator_never_reports_a_refused_commit_as_a_committed_saga()
    {
        var channel = new InProcessSagaControlChannel();
        var a = CreateParticipant("site-a", channel);
        var b = CreateParticipant("site-b", channel);
        var (coordinator, coordinatorState) = CreateCoordinator(channel);
        var bCommitReached = channel.HoldCommit("site-b");

        var run = coordinator.RunAsync(["site-a", "site-b"], TargetTree, ManifestId, CoordinatorCluster);
        await WithTimeout(bCommitReached, "the commit for site-b never reached the channel");

        // site-b has reached the other terminal phase out of band - a participant
        // from before #4637 that compensated on its timer - so it refuses the commit.
        await b.Grain.AbortAsync(new SagaControlRequest { SagaId = SagaId, TargetTree = TargetTree, ManifestId = ManifestId, CoordinatorClusterId = CoordinatorCluster });
        channel.ReleaseCommit("site-b");

        Assert.ThrowsAsync<InvalidOperationException>(async () => await run,
            "a commit one cluster refused is a split saga, never a committed one");
        Assert.Multiple(() =>
        {
            Assert.That(coordinatorState.State.Phase, Is.EqualTo(CrossClusterSagaPhase.Committed),
                "the saga stays un-completed so the coordinator keeps re-delivering");
            Assert.That(coordinatorState.State.FailureMessage, Does.Contain("site-b"));
            Assert.That(a.Local.CommitCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_coordinator_that_never_started_the_saga_answers_aborted()
    {
        var channel = new InProcessSagaControlChannel();
        var (coordinator, _) = CreateCoordinator(channel);

        Assert.That(await coordinator.ResolveDecisionForParticipantAsync("site-a"), Is.EqualTo(CrossClusterSagaDecision.Aborted));
    }

    [Test]
    public async Task The_coordinator_record_is_durable_before_any_participant_is_asked_to_prepare()
    {
        // NotStarted answers Aborted, so a prepared participant must never be
        // able to see NotStarted from a coordinator that could still commit. That
        // holds only if the Preparing record is persisted before the first
        // prepare leaves the coordinator.
        var channel = Substitute.For<ISagaControlChannel>();
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("saga-coordinator", SagaId));
        var reminders = Substitute.For<IReminderRegistry>();
        reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(new LatticeOptions());
        var state = new FakePersistentState<CrossClusterSagaCoordinatorState>();
        var persistedPhase = CrossClusterSagaPhase.NotStarted;
        state.OnAfterWrite = s => persistedPhase = s.Phase;
        var coordinator = new CrossClusterSagaCoordinatorGrain(
            context, channel, reminders, optionsMonitor,
            NullLogger<CrossClusterSagaCoordinatorGrain>.Instance, state);

        var persistedAtFirstPrepare = new List<CrossClusterSagaPhase>();
        var answeredAtFirstPrepare = new List<CrossClusterSagaDecision>();
        channel.PrepareAsync(Arg.Any<string>(), Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>())
            .Returns(async call =>
            {
                persistedAtFirstPrepare.Add(persistedPhase);
                answeredAtFirstPrepare.Add(await coordinator.ResolveDecisionForParticipantAsync(call.ArgAt<string>(0)));
                return new SagaControlResponse { SagaId = SagaId, Phase = SagaPhase.Aborted, Vote = SagaVote.Abort };
            });
        channel.AbortAsync(Arg.Any<string>(), Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new SagaControlResponse { SagaId = SagaId, Phase = SagaPhase.Aborted, Vote = SagaVote.Abort }));

        await coordinator.RunAsync(["site-a", "site-b"], TargetTree, ManifestId, CoordinatorCluster);

        Assert.Multiple(() =>
        {
            Assert.That(persistedAtFirstPrepare, Has.Count.EqualTo(2));
            Assert.That(persistedAtFirstPrepare, Is.All.EqualTo(CrossClusterSagaPhase.Preparing),
                "the coordinator must persist its Preparing record before it sends any prepare");
            Assert.That(answeredAtFirstPrepare, Is.All.EqualTo(CrossClusterSagaDecision.InFlight),
                "a participant asking while the coordinator prepares must be told InFlight, never Aborted");
        });
    }

    [Test]
    public async Task An_operator_resolution_applies_the_coordinators_decision_when_it_answers()
    {
        var channel = new InProcessSagaControlChannel();
        var a = CreateParticipant("site-a", channel);
        var b = CreateParticipant("site-b", channel);
        var (coordinator, _) = CreateCoordinator(channel);
        var bCommitReached = channel.HoldCommit("site-b");
        var run = coordinator.RunAsync(["site-a", "site-b"], TargetTree, ManifestId, CoordinatorCluster);
        await WithTimeout(bCommitReached, "the commit for site-b never reached the channel");

        Assert.ThrowsAsync<InvalidOperationException>(() => b.Grain.OperatorResolveAsync(commit: false),
            "an abort that contradicts the coordinator's commit is refused");
        Assert.That(b.State.State.Phase, Is.EqualTo(SagaPhase.Committed), "the coordinator's commit is applied instead");

        channel.ReleaseCommit("site-b");
        Assert.That(await run, Is.EqualTo(CrossClusterSagaOutcome.Committed));
        Assert.That(b.Local.AbortCount, Is.Zero);
        Assert.That(a.Local.CommitCount, Is.EqualTo(1));
    }

    [Test]
    public async Task An_operator_resolution_applies_the_request_only_when_the_coordinator_is_unreachable()
    {
        var channel = new InProcessSagaControlChannel();
        var b = CreateParticipant("site-b", channel);
        await b.Grain.PrepareAsync(new SagaControlRequest
        {
            SagaId = SagaId, TargetTree = TargetTree, ManifestId = ManifestId, CoordinatorClusterId = CoordinatorCluster, SetId = "set-1",
        });
        channel.CoordinatorUnreachable = true;

        Assert.That(await b.Grain.OperatorResolveAsync(commit: true), Is.True);
        Assert.That(await b.Grain.OperatorResolveAsync(commit: true), Is.False, "an already-resolved participant is unchanged");
        Assert.ThrowsAsync<InvalidOperationException>(() => b.Grain.OperatorResolveAsync(commit: false));

        Assert.Multiple(() =>
        {
            Assert.That(b.State.State.Phase, Is.EqualTo(SagaPhase.Committed));
            Assert.That(b.Local.CommitCount, Is.EqualTo(1));
            Assert.That(b.Local.LastCommitRequest?.SetId, Is.EqualTo("set-1"),
                "a decision the participant applies itself reaches every member of the set");
        });
    }

    [Test]
    public async Task A_participant_whose_coordinator_is_still_deciding_keeps_the_fence_without_alarming()
    {
        var channel = new InProcessSagaControlChannel();
        var b = CreateParticipant("site-b", channel);
        var deciding = Substitute.For<ICrossClusterSagaCoordinatorGrain>();
        deciding.ResolveDecisionForParticipantAsync("site-b").Returns(CrossClusterSagaDecision.InFlight);
        channel.RegisterCoordinator(CoordinatorCluster, deciding);
        await b.Grain.PrepareAsync(new SagaControlRequest { SagaId = SagaId, TargetTree = TargetTree, ManifestId = ManifestId, CoordinatorClusterId = CoordinatorCluster });

        await ExpireFenceAsync(b);

        Assert.Multiple(() =>
        {
            Assert.That(b.State.State.Phase, Is.EqualTo(SagaPhase.Prepared));
            Assert.That(b.Local.AbortCount, Is.Zero);
            Assert.That(SagaParticipantFenceCensus.OldestAgeSeconds(SagaParticipantFenceCensus.ReasonDecisionPending), Is.Not.Null);
            Assert.That(SagaParticipantFenceCensus.OldestAgeSeconds(SagaParticipantFenceCensus.ReasonCoordinatorUnreachable), Is.Null);
        });
    }

    [Test]
    public void The_fence_held_age_gauge_reports_the_oldest_held_fence_per_reason()
    {
        var now = DateTime.UtcNow.Ticks;
        SagaParticipantFenceCensus.Hold("s-old", now - TimeSpan.FromMinutes(10).Ticks, SagaParticipantFenceCensus.ReasonCoordinatorUnreachable);
        SagaParticipantFenceCensus.Hold("s-new", now - TimeSpan.FromMinutes(1).Ticks, SagaParticipantFenceCensus.ReasonCoordinatorUnreachable);
        SagaParticipantFenceCensus.Hold("s-pending", now - TimeSpan.FromMinutes(2).Ticks, SagaParticipantFenceCensus.ReasonDecisionPending);
        var observed = new Dictionary<string, double>(StringComparer.Ordinal);
        using var listener = Orleans.Lattice.Testing.MeterListening.StartForInstrument(
            SagaParticipantFenceCensus.FenceHeldAge,
            l => l.SetMeasurementEventCallback<double>((_, value, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeReplicationMetrics.TagReason && tag.Value is string reason)
                        observed[reason] = value;
                }
            }));

        listener.RecordObservableInstruments();
        SagaParticipantFenceCensus.Release("s-old");
        SagaParticipantFenceCensus.Release("s-new");
        SagaParticipantFenceCensus.Release("s-pending");
        var afterRelease = observed.Count;
        observed.Clear();
        listener.RecordObservableInstruments();

        Assert.Multiple(() =>
        {
            Assert.That(afterRelease, Is.EqualTo(2));
            Assert.That(observed, Is.Empty, "no held fence, no series");
        });
    }

    [Test]
    public async Task The_coordinator_side_handler_answers_with_the_coordinators_decision_for_the_stamped_requester()
    {
        var coordinator = Substitute.For<ICrossClusterSagaCoordinatorGrain>();
        coordinator.ResolveDecisionForParticipantAsync("site-b").Returns(CrossClusterSagaDecision.Committed);
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ICrossClusterSagaCoordinatorGrain>(SagaId, null).Returns(coordinator);
        var handler = new LatticeSagaControlHandler(factory);

        var response = await handler.GetDecisionAsync(new SagaControlRequest { SagaId = SagaId, RequesterClusterId = "site-b" });

        Assert.That(response.Phase, Is.EqualTo(SagaPhase.Committed));
        Assert.ThrowsAsync<UnauthorizedAccessException>(() => handler.GetDecisionAsync(new SagaControlRequest { SagaId = SagaId }),
            "a query that carries no authenticated requester is refused");
    }

}
