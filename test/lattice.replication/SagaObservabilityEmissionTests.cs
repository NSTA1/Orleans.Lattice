using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Proves the coordinated-restore saga observability instruments actually fire
/// on the <b>real</b> saga path: a real <see cref="CrossClusterSagaCoordinatorGrain"/>
/// drives real <see cref="CrossClusterSagaParticipantGrain"/> activations hosting
/// a real <see cref="RestoreParticipant"/> over a fake restore engine. Asserts the
/// phase-duration histogram, participant vote / commit counters, and the
/// compensation counter emit with the expected tags on commit and on abort paths.
/// Complements <see cref="SagaObservabilityMetricsTests"/>, which asserts the
/// instrument shapes in isolation.
/// </summary>
[TestFixture]
public class SagaObservabilityEmissionTests
{
    private const string SagaId = "restore-saga-obs";
    private const string TargetTree = "orders";
    private const string ManifestId = "backup-1";
    private const string CoordinatorCluster = "site-home";

    private sealed class ClusterHarness
    {
        public required CrossClusterSagaParticipantGrain Grain { get; init; }
        public required FakeCoordinatedRestoreEngine Engine { get; init; }
    }

    private static ClusterHarness CreateCluster()
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

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("saga-participant", SagaId));

        var reminders = Substitute.For<IReminderRegistry>();
        reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(new LatticeOptions());

        var grain = new CrossClusterSagaParticipantGrain(
            context, [restoreParticipant], reminders, optionsMonitor,
            NullLogger<CrossClusterSagaParticipantGrain>.Instance,
            new FakePersistentState<CrossClusterSagaParticipantState>());

        return new ClusterHarness { Grain = grain, Engine = engine };
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
    public async Task Committing_saga_emits_prepare_and_commit_phase_durations_and_participant_counters()
    {
        using var phases = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.SagaPhaseDurationName);
        using var votes = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.SagaParticipantVotesName);
        using var commits = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.SagaParticipantCommitsName);

        var a = CreateCluster();
        var b = CreateCluster();

        var channel = new InProcessSagaControlChannel();
        channel.Register("site-a", a.Grain);
        channel.Register("site-b", b.Grain);

        var coordinator = CreateCoordinator(channel);

        var outcome = await coordinator.RunAsync(
            ["site-a", "site-b"], TargetTree, ManifestId, CoordinatorCluster);

        Assert.That(outcome, Is.EqualTo(CrossClusterSagaOutcome.Committed));

        // Both phase transitions recorded: one prepare, one commit.
        Assert.Multiple(() =>
        {
            Assert.That(phases.Measurements.Any(m => TagValue(m.Tags, "phase") == "prepare"), Is.True,
                "prepare phase duration was not recorded");
            Assert.That(phases.Measurements.Any(m => TagValue(m.Tags, "phase") == "commit"), Is.True,
                "commit phase duration was not recorded");

            // Both participants voted commit; both committed the single-tree restore.
            Assert.That(votes.Measurements.Count(m => TagValue(m.Tags, "reason") == "commit"),
                Is.EqualTo(2), "expected two commit votes");
            Assert.That(commits.Measurements.Count(m => TagValue(m.Tags, "reason") == "single"),
                Is.EqualTo(2), "expected two single-tree commits");
        });
    }

    [Test]
    public async Task Aborting_saga_emits_abort_phase_duration_and_a_vote_abort_compensation()
    {
        using var phases = new MeterCollector<double>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.SagaPhaseDurationName);
        using var votes = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.SagaParticipantVotesName);
        using var compensations = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.SagaCompensationsName);

        var a = CreateCluster();
        var b = CreateCluster();
        // B cannot build its shadow: the whole saga aborts and A (which prepared) is
        // compensated by the coordinator decision (cause = vote-abort).
        b.Engine.BuildFailure = new InvalidOperationException("capacity exhausted");

        var channel = new InProcessSagaControlChannel();
        channel.Register("site-a", a.Grain);
        channel.Register("site-b", b.Grain);

        var coordinator = CreateCoordinator(channel);

        var outcome = await coordinator.RunAsync(
            ["site-a", "site-b"], TargetTree, ManifestId, CoordinatorCluster);

        Assert.That(outcome, Is.EqualTo(CrossClusterSagaOutcome.Aborted));

        Assert.Multiple(() =>
        {
            Assert.That(phases.Measurements.Any(m => TagValue(m.Tags, "phase") == "abort"), Is.True,
                "abort phase duration was not recorded");

            // A voted commit; B voted abort with the build-failed reason.
            Assert.That(votes.Measurements.Any(m => TagValue(m.Tags, "reason") == "commit"), Is.True,
                "expected a commit vote from the prepared cluster");
            Assert.That(votes.Measurements.Any(m => TagValue(m.Tags, "reason") == "build-failed"), Is.True,
                "expected a build-failed abort vote from the failing cluster");

            // The prepared cluster (A) was compensated by the coordinator decision.
            Assert.That(compensations.Measurements.Any(m => TagValue(m.Tags, "cause") == "vote-abort"), Is.True,
                "expected a vote-abort compensation for the prepared cluster");
        });
    }

    private static string? TagValue(IReadOnlyList<KeyValuePair<string, object?>> tags, string key)
    {
        foreach (var tag in tags)
        {
            if (tag.Key == key)
            {
                return tag.Value as string;
            }
        }

        return null;
    }
}
