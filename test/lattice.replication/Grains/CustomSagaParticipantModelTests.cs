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
/// unanimous-abort compensation, coordinator-loss fence-timer auto-compensation,
/// and idempotent re-attach for the custom participant.
/// </summary>
[TestFixture]
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

    private static ClusterHarness CreateCluster(SagaVote exampleVote = SagaVote.Commit)
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

    private static CrossClusterSagaCoordinatorGrain CreateCoordinator(ISagaControlChannel channel)
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
            new FakePersistentState<CrossClusterSagaCoordinatorState>());
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

    [Test]
    public async Task Coordinator_loss_fence_expiry_auto_compensates_the_custom_participant()
    {
        var a = CreateCluster();
        var b = CreateCluster();

        // Prepare both clusters directly (no coordinator decision), so both hold a
        // prepared custom participant under an armed fence.
        await a.Grain.PrepareAsync(Request());
        await b.Grain.PrepareAsync(Request());

        Assert.That(a.Example.HasPendingValue, Is.True);
        Assert.That(b.Example.HasPendingValue, Is.True);

        foreach (var cluster in new[] { a, b })
        {
            // The coordinator never returns: move the fence deadline into the past
            // and fire the fence reminder, exercising the auto-compensation path.
            cluster.State.State.FenceDeadlineTicks = DateTime.UtcNow.Ticks - TimeSpan.FromMinutes(1).Ticks;
            await cluster.Grain.ReceiveReminder(FenceReminder, default);

            Assert.That(cluster.Example.CommitCount, Is.EqualTo(0), "a lost coordinator never commits");
            Assert.That(cluster.Example.AbortCount, Is.EqualTo(1), "fence expiry must auto-compensate the custom participant");
            Assert.That(cluster.Example.HasPendingValue, Is.False);
            Assert.That(cluster.Engine.RevertCount, Is.EqualTo(1));
        }
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
