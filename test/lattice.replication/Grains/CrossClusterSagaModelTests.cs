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
/// End-to-end, in-process coverage of the cross-cluster saga model: a real
/// <see cref="CrossClusterSagaCoordinatorGrain"/> drives real
/// <see cref="CrossClusterSagaParticipantGrain"/> activations over an in-process
/// <see cref="InProcessSagaControlChannel"/> (no gRPC). Exercises the
/// unanimous-commit and one-abort-compensates acceptance paths across two
/// participant clusters.
/// </summary>
[TestFixture]
public class CrossClusterSagaModelTests
{
    private const string SagaId = "saga-e2e";
    private const string TargetTree = "orders";
    private const string ManifestId = "manifest-1";
    private const string CoordinatorCluster = "site-home";

    private static CrossClusterSagaParticipantGrain CreateParticipant(
        IEnumerable<ISagaParticipant> participants)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("saga-participant", SagaId));

        var reminders = Substitute.For<IReminderRegistry>();
        reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(new LatticeOptions());

        return new CrossClusterSagaParticipantGrain(
            context, participants, reminders, optionsMonitor,
            NullLogger<CrossClusterSagaParticipantGrain>.Instance,
            new FakePersistentState<CrossClusterSagaParticipantState>());
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

    [Test]
    public async Task Unanimous_prepare_commits_every_participant()
    {
        var a1 = new RecordingSagaParticipant();
        var b1 = new RecordingSagaParticipant();
        var participantA = CreateParticipant([a1]);
        var participantB = CreateParticipant([b1]);

        var channel = new InProcessSagaControlChannel();
        channel.Register("site-a", participantA);
        channel.Register("site-b", participantB);

        var coordinator = CreateCoordinator(channel);

        var outcome = await coordinator.RunAsync(
            ["site-a", "site-b"], TargetTree, ManifestId, CoordinatorCluster);

        Assert.That(outcome, Is.EqualTo(CrossClusterSagaOutcome.Committed));
        Assert.That(a1.CommitCount, Is.EqualTo(1));
        Assert.That(b1.CommitCount, Is.EqualTo(1));
        Assert.That(a1.AbortCount, Is.EqualTo(0));
        Assert.That(b1.AbortCount, Is.EqualTo(0));
        Assert.That(await participantA.GetStatusAsync(new SagaControlRequest { SagaId = SagaId }),
            Has.Property(nameof(SagaControlResponse.Phase)).EqualTo(SagaPhase.Committed));
    }

    [Test]
    public async Task One_participant_declines_aborts_and_compensates_the_prepared_one()
    {
        var a1 = new RecordingSagaParticipant(SagaVote.Commit);
        var b1 = new RecordingSagaParticipant(SagaVote.Abort, "precondition failed");
        var participantA = CreateParticipant([a1]);
        var participantB = CreateParticipant([b1]);

        var channel = new InProcessSagaControlChannel();
        channel.Register("site-a", participantA);
        channel.Register("site-b", participantB);

        var coordinator = CreateCoordinator(channel);

        var outcome = await coordinator.RunAsync(
            ["site-a", "site-b"], TargetTree, ManifestId, CoordinatorCluster);

        Assert.That(outcome, Is.EqualTo(CrossClusterSagaOutcome.Aborted));
        // The prepared participant is compensated; it never commits.
        Assert.That(a1.CommitCount, Is.EqualTo(0));
        Assert.That(a1.AbortCount, Is.EqualTo(1));
        Assert.That(await participantA.GetStatusAsync(new SagaControlRequest { SagaId = SagaId }),
            Has.Property(nameof(SagaControlResponse.Phase)).EqualTo(SagaPhase.Aborted));
    }
}
