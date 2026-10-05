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
/// End-to-end, in-process coverage of a host-defined <see cref="ISagaParticipant"/>
/// (the worked-sample <see cref="ExampleSagaParticipant"/>) driven through a full
/// cross-cluster saga <b>alongside</b> the built-in restore participant. A real
/// <see cref="CrossClusterSagaCoordinatorGrain"/> drives real
/// <see cref="CrossClusterSagaParticipantGrain"/> activations, each hosting both a
/// <see cref="RestoreParticipant"/> (over a fake restore engine) and an
/// <see cref="ExampleSagaParticipant"/>, across two clusters. Verifies commit,
/// unanimous-abort compensation, the fence-expiry decision query (an unreachable
/// coordinator keeps the fence; a recorded abort, or a coordinator whose record is
/// gone, compensates; issue #4637), and idempotent re-attach for the custom
/// participant.
/// </summary>
[TestFixture]
[NonParallelizable]
public class CustomSagaParticipantModelTests
{
    private const string SagaId = "custom-saga-e2e";
    private const string TargetTree = "orders";
    private const string ManifestId = "backup-1";
    private const string CoordinatorCluster = "site-home";
    private const string FenceReminder = "saga-participant-fence";

    private sealed class ClusterHarness
    {
        public required CrossClusterSagaParticipantGrain Grain { get; init; }
        public required FakeCoordinatedRestoreEngine Engine { get; init; }
        public required ISagaWriteFenceGrain Fence { get; init; }
        public required ExampleSagaParticipant Example { get; init; }
        public required FakePersistentState<CrossClusterSagaParticipantState> State { get; init; }
    }

    [SetUp]
    public void SetUp() => SagaParticipantFenceCensus.ResetForTest();

    [TearDown]
    public void TearDown() => SagaParticipantFenceCensus.ResetForTest();

    private static ClusterHarness CreateCluster(
        SagaVote exampleVote = SagaVote.Commit,
        InProcessSagaControlChannel? decisionChannel = null,
        string? clusterId = null)
    {
        var engine = new FakeCoordinatedRestoreEngine { TargetTree = TargetTree };

        var capacity = Substitute.For<IRestoreCapacityProbe>();
        capacity.CanHostAsync(Arg.Any<Backup.RestoreAdmissionReport>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(true));

        var fence = Substitute.For<ISagaWriteFenceGrain>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ISagaWriteFenceGrain>(Arg.Any<string>()).Returns(fence);

        var restoreParticipant = new RestoreParticipant(
            engine, engine, capacity, factory, NullLogger<RestoreParticipant>.Instance);

        var example = new ExampleSagaParticipant { PrepareVote = exampleVote };

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("saga-participant", SagaId));
        if (decisionChannel is not null)
        {
            // The participant asks its coordinator for the decision through the
            // saga-control channel, stamped as this cluster.
            context.ActivationServices.Returns(new ServiceCollection()
                .AddSingleton(decisionChannel.ViewFrom(clusterId!))
                .BuildServiceProvider());
        }

        var reminders = Substitute.For<IReminderRegistry>();
        reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(new LatticeOptions());

        var state = new FakePersistentState<CrossClusterSagaParticipantState>();

        // Both the built-in restore participant and the custom participant run in
        // the same saga on this cluster.
        var grain = new CrossClusterSagaParticipantGrain(
            context, [restoreParticipant, example], reminders, optionsMonitor,
            NullLogger<CrossClusterSagaParticipantGrain>.Instance, state);

        return new ClusterHarness { Grain = grain, Engine = engine, Fence = fence, Example = example, State = state };
    }

    private static CrossClusterSagaCoordinatorGrain CreateCoordinator(
        ISagaControlChannel channel,
        FakePersistentState<CrossClusterSagaCoordinatorState>? state = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("saga-coordinator", SagaId));

        var reminders = Substitute.For<IReminderRegistry>();
        reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(new LatticeOptions());

        return new CrossClusterSagaCoordinatorGrain(
            context, channel, reminders, optionsMonitor,
            NullLogger<CrossClusterSagaCoordinatorGrain>.Instance,
            state ?? new FakePersistentState<CrossClusterSagaCoordinatorState>());
    }

    private static SagaControlRequest Request() => new()
    {
        SagaId = SagaId,
        TargetTree = TargetTree,
        ManifestId = ManifestId,
        CoordinatorClusterId = CoordinatorCluster,
    };

    [Test]
    public async Task Custom_participant_commits_alongside_restore_on_unanimous_prepare()
    {
        var a = CreateCluster();
        var b = CreateCluster();

        var channel = new InProcessSagaControlChannel();
        channel.Register("site-a", a.Grain);
        channel.Register("site-b", b.Grain);

        var coordinator = CreateCoordinator(channel);

        var outcome = await coordinator.RunAsync(
            ["site-a", "site-b"], TargetTree, ManifestId, CoordinatorCluster);

        Assert.That(outcome, Is.EqualTo(CrossClusterSagaOutcome.Committed));

        foreach (var cluster in new[] { a, b })
        {
            // Restore committed its cut.
            Assert.That(cluster.Engine.CommitCount, Is.EqualTo(1));
            Assert.That(cluster.Engine.RevertCount, Is.EqualTo(0));

            // The custom participant was resolved, prepared, and committed its
            // staged value alongside restore.
            Assert.That(cluster.Example.PrepareCount, Is.EqualTo(1));
            Assert.That(cluster.Example.CommitCount, Is.EqualTo(1));
            Assert.That(cluster.Example.AbortCount, Is.EqualTo(0));
            Assert.That(cluster.Example.CommittedValue, Is.EqualTo("example-value"));
            Assert.That(cluster.Example.HasPendingValue, Is.False);
        }
    }

    [Test]
    public async Task Custom_participant_abort_vote_aborts_saga_and_compensates_every_prepared_participant()
    {
        // Cluster A prepares cleanly; on cluster B the custom participant votes
        // abort, so the whole saga must abort and every prepared participant
        // (restore and the custom participant on A) must be compensated.
        var a = CreateCluster();
        var b = CreateCluster(exampleVote: SagaVote.Abort);

        var channel = new InProcessSagaControlChannel();
        channel.Register("site-a", a.Grain);
        channel.Register("site-b", b.Grain);

        var coordinator = CreateCoordinator(channel);

        var outcome = await coordinator.RunAsync(
            ["site-a", "site-b"], TargetTree, ManifestId, CoordinatorCluster);

        Assert.That(outcome, Is.EqualTo(CrossClusterSagaOutcome.Aborted));

        // Nothing committed anywhere.
        Assert.That(a.Engine.CommitCount, Is.EqualTo(0));
        Assert.That(b.Engine.CommitCount, Is.EqualTo(0));
        Assert.That(a.Example.CommittedValue, Is.Null);
        Assert.That(b.Example.CommittedValue, Is.Null);

        // Cluster A prepared, so both of its participants are compensated.
        Assert.That(a.Engine.RevertCount, Is.EqualTo(1));
        Assert.That(a.Example.AbortCount, Is.EqualTo(1));
        Assert.That(a.Example.HasPendingValue, Is.False);
        await a.Fence.Received(1).LiftAsync();
    }

    private static async Task ExpireFenceAsync(ClusterHarness cluster)
    {
        cluster.State.State.FenceDeadlineTicks = DateTime.UtcNow.Ticks - TimeSpan.FromMinutes(1).Ticks;
        await cluster.Grain.ReceiveReminder(FenceReminder, default);
    }

    [Test]
    public async Task Fence_expiry_with_an_unreachable_coordinator_keeps_the_custom_participant_prepared_and_reports_the_held_fence()
    {
        var channel = new InProcessSagaControlChannel { CoordinatorUnreachable = true };
        var a = CreateCluster(decisionChannel: channel, clusterId: "site-a");
        var b = CreateCluster(decisionChannel: channel, clusterId: "site-b");

        // Prepare both clusters directly, so both hold a prepared custom
        // participant under an armed fence, and the coordinator never answers.
        await a.Grain.PrepareAsync(Request());
        await b.Grain.PrepareAsync(Request());

        foreach (var cluster in new[] { a, b })
        {
            await ExpireFenceAsync(cluster);

            // Issue #4637: compensating here could contradict a commit the
            // coordinator already delivered elsewhere, so the fence holds.
            Assert.Multiple(() =>
            {
                Assert.That(cluster.Example.CommitCount, Is.EqualTo(0), "a lost coordinator never commits");
                Assert.That(cluster.Example.AbortCount, Is.EqualTo(0),
                    "fence expiry alone must not compensate the custom participant");
                Assert.That(cluster.Example.HasPendingValue, Is.True);
                Assert.That(cluster.Engine.RevertCount, Is.EqualTo(0));
                Assert.That(cluster.State.State.Phase, Is.EqualTo(SagaPhase.Prepared));
            });
        }

        Assert.That(SagaParticipantFenceCensus.OldestAgeSeconds(SagaParticipantFenceCensus.ReasonCoordinatorUnreachable),
            Is.GreaterThan(0), "the held fence must be reported on the fence-held age gauge");
    }

    [TestCase(true, TestName = "Fence_expiry_after_a_recorded_abort_compensates_the_custom_participant")]
    [TestCase(false, TestName = "Fence_expiry_against_a_coordinator_whose_record_is_gone_compensates_the_custom_participant")]
    public async Task Fence_expiry_compensates_the_custom_participant_when_the_coordinator_answers_abort(bool recordedAbort)
    {
        var channel = new InProcessSagaControlChannel();
        var a = CreateCluster(decisionChannel: channel, clusterId: "site-a");
        var b = CreateCluster(decisionChannel: channel, clusterId: "site-b");

        var coordinatorState = new FakePersistentState<CrossClusterSagaCoordinatorState>();
        if (recordedAbort)
        {
            coordinatorState.State.SagaId = SagaId;
            coordinatorState.State.Phase = CrossClusterSagaPhase.Aborted;
            coordinatorState.State.Outcome = CrossClusterSagaOutcome.Aborted;
            coordinatorState.State.Participants =
            [
                new CrossClusterSagaParticipantRef { ClusterId = "site-a" },
                new CrossClusterSagaParticipantRef { ClusterId = "site-b" },
            ];
        }

        // NotStarted (no record) is answered as Aborted: the coordinator persists
        // its record before any prepare, so a missing record is an aborted saga
        // whose record has since expired.
        channel.RegisterCoordinator(CoordinatorCluster, CreateCoordinator(channel, coordinatorState));

        await a.Grain.PrepareAsync(Request());
        await b.Grain.PrepareAsync(Request());

        foreach (var cluster in new[] { a, b })
        {
            await ExpireFenceAsync(cluster);

            Assert.Multiple(() =>
            {
                Assert.That(cluster.Example.CommitCount, Is.EqualTo(0));
                Assert.That(cluster.Example.AbortCount, Is.EqualTo(1),
                    "the coordinator's abort must compensate the custom participant");
                Assert.That(cluster.Example.HasPendingValue, Is.False);
                Assert.That(cluster.Engine.RevertCount, Is.EqualTo(1));
                Assert.That(cluster.State.State.Phase, Is.EqualTo(SagaPhase.Aborted));
            });
        }

        Assert.That(SagaParticipantFenceCensus.OldestAgeSeconds(SagaParticipantFenceCensus.ReasonCoordinatorUnreachable),
            Is.Null, "a resolved participant holds no fence");
    }

    [Test]
    public async Task Duplicate_commit_re_attach_is_a_noop_for_the_custom_participant()
    {
        var cluster = CreateCluster();
        await cluster.Grain.PrepareAsync(Request());

        await cluster.Grain.CommitAsync(Request());
        await cluster.Grain.CommitAsync(Request());

        // The model forwards commit once; the custom participant applied its value
        // exactly once and holds no dangling prepared state.
        Assert.That(cluster.Example.CommitCount, Is.EqualTo(1), "duplicate commit must not re-drive the participant");
        Assert.That(cluster.Example.CommittedValue, Is.EqualTo("example-value"));
        Assert.That(cluster.Example.HasPendingValue, Is.False);
    }

    [Test]
    public async Task Duplicate_abort_re_attach_is_a_noop_for_the_custom_participant()
    {
        var cluster = CreateCluster();
        await cluster.Grain.PrepareAsync(Request());

        await cluster.Grain.AbortAsync(Request());
        await cluster.Grain.AbortAsync(Request());

        Assert.That(cluster.Example.AbortCount, Is.EqualTo(1), "duplicate abort must not re-compensate the participant");
        Assert.That(cluster.Example.CommittedValue, Is.Null);
        Assert.That(cluster.Example.HasPendingValue, Is.False);
    }

    [Test]
    public async Task Example_participant_is_idempotent_when_driven_directly()
    {
        // Demonstrates the contract guardrail at the participant level: duplicate
        // commit and duplicate abort are safe no-ops.
        var participant = new ExampleSagaParticipant { StagedValue = "v1" };
        var request = Request();

        var vote = await participant.PrepareAsync(request);
        Assert.That(vote.Vote, Is.EqualTo(SagaVote.Commit));

        await participant.CommitAsync(request);
        await participant.CommitAsync(request);
        Assert.That(participant.CommittedValue, Is.EqualTo("v1"));

        // Aborting after commit does not resurrect or corrupt the committed value.
        await participant.AbortAsync(request);
        await participant.AbortAsync(request);
        Assert.That(participant.CommittedValue, Is.EqualTo("v1"));
        Assert.That(participant.HasPendingValue, Is.False);
    }

    [Test]
    public async Task Example_participant_abort_after_prepare_discards_the_staged_value()
    {
        var participant = new ExampleSagaParticipant { StagedValue = "v2" };
        var request = Request();

        await participant.PrepareAsync(request);
        Assert.That(participant.HasPendingValue, Is.True);

        await participant.AbortAsync(request);

        Assert.That(participant.HasPendingValue, Is.False);
        Assert.That(participant.CommittedValue, Is.Null, "compensation must restore the pre-prepare view");
    }
}
