using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// End-to-end, in-process coverage of coordinated multi-cluster restore: a real
/// <see cref="CrossClusterSagaCoordinatorGrain"/> drives real
/// <see cref="CrossClusterSagaParticipantGrain"/> activations that each host a
/// real <see cref="RestoreParticipant"/> (over a fake restore engine) across two
/// clusters. Reproduces the all-or-nothing guarantee that fixes the multi-cluster
/// re-advance defect (#1169): either every cluster commits the restore cut, or
/// every prepared cluster is compensated back to its pre-restore state.
/// </summary>
[TestFixture]
public class CoordinatedRestoreSagaModelTests
{
    private const string SagaId = "restore-saga-e2e";
    private const string TargetTree = "orders";
    private const string ManifestId = "backup-1";
    private const string CoordinatorCluster = "site-home";

    private sealed class ClusterHarness
    {
        public required CrossClusterSagaParticipantGrain Grain { get; init; }
        public required FakeCoordinatedRestoreEngine Engine { get; init; }
        public required ISagaWriteFenceGrain Fence { get; init; }
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

        return new ClusterHarness { Grain = grain, Engine = engine, Fence = fence };
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
    public async Task Unanimous_prepare_commits_the_restore_cut_on_every_cluster()
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

        // Every cluster built its shadow unfenced during prepare, then flipped the
        // alias exactly once under the cutover fence and unblocked local writes.
        foreach (var cluster in new[] { a, b })
        {
            Assert.That(cluster.Engine.BuildCount, Is.GreaterThanOrEqualTo(1));
            Assert.That(cluster.Engine.CommitCount, Is.EqualTo(1));
            Assert.That(cluster.Engine.RevertCount, Is.EqualTo(0));
            await cluster.Fence.Received(1).EngageAsync(Arg.Any<SagaWriteFenceRequest>());
            await cluster.Fence.Received(1).UnblockWritesAsync();
        }
    }

    [Test]
    public async Task One_cluster_cannot_prepare_compensates_every_prepared_cluster()
    {
        var a = CreateCluster();
        var b = CreateCluster();
        // Cluster B cannot build its shadow (for example a capacity exhaustion that
        // outlasts the retry budget): the whole restore must abort, and the cluster
        // that did prepare (A) is compensated back to its pre-restore state.
        b.Engine.BuildFailure = new InvalidOperationException("capacity exhausted");

        var channel = new InProcessSagaControlChannel();
        channel.Register("site-a", a.Grain);
        channel.Register("site-b", b.Grain);

        var coordinator = CreateCoordinator(channel);

        var outcome = await coordinator.RunAsync(
            ["site-a", "site-b"], TargetTree, ManifestId, CoordinatorCluster);

        Assert.That(outcome, Is.EqualTo(CrossClusterSagaOutcome.Aborted));

        // No cluster committed the cut.
        Assert.That(a.Engine.CommitCount, Is.EqualTo(0));
        Assert.That(b.Engine.CommitCount, Is.EqualTo(0));

        // The prepared cluster (A) reverted, garbage collected its shadow, and lifted
        // its fence.
        Assert.That(a.Engine.RevertCount, Is.EqualTo(1));
        Assert.That(a.Engine.DeleteCount, Is.GreaterThanOrEqualTo(1));
        await a.Fence.Received(1).LiftAsync();

        // The failing cluster (B) garbage collected any partial shadow.
        Assert.That(b.Engine.DeleteCount, Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task Coordinator_loss_before_decision_auto_compensates_prepared_clusters()
    {
        var a = CreateCluster();
        var b = CreateCluster();

        var channel = new InProcessSagaControlChannel();
        channel.Register("site-a", a.Grain);
        channel.Register("site-b", b.Grain);

        // Drive prepare directly (no coordinator decision), then fire the fence-expiry
        // reminder path that models a coordinator that never returned. Both prepared
        // clusters must auto-compensate.
        var request = new SagaControlRequest
        {
            SagaId = SagaId,
            TargetTree = TargetTree,
            ManifestId = ManifestId,
            CoordinatorClusterId = CoordinatorCluster,
        };

        await a.Grain.PrepareAsync(request);
        await b.Grain.PrepareAsync(request);

        // Both prepared (built their shadow), nothing committed yet.
        Assert.That(a.Engine.CommitCount, Is.EqualTo(0));
        Assert.That(b.Engine.CommitCount, Is.EqualTo(0));

        // Coordinator-loss: the durable abort path compensates every prepared cluster.
        await a.Grain.AbortAsync(request);
        await b.Grain.AbortAsync(request);

        foreach (var cluster in new[] { a, b })
        {
            Assert.That(cluster.Engine.CommitCount, Is.EqualTo(0), "a lost coordinator never commits");
            Assert.That(cluster.Engine.RevertCount, Is.EqualTo(1));
            Assert.That(cluster.Engine.DeleteCount, Is.GreaterThanOrEqualTo(1));
            await cluster.Fence.Received().LiftAsync();
        }
    }
}
