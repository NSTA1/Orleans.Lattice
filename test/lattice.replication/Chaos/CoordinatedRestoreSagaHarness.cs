using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Chaos;

/// <summary>
/// Shared in-process harness for the coordinated cross-cluster restore chaos and
/// soak fixtures. Each <see cref="Cluster"/> hosts a real
/// <see cref="CrossClusterSagaParticipantGrain"/> driving a real
/// <see cref="RestoreParticipant"/> over a <see cref="FakeCoordinatedRestoreEngine"/>
/// whose fault knobs (build failures, transient exhaustion, capacity refusal) let a
/// chaos test inject duress. A real <see cref="CrossClusterSagaCoordinatorGrain"/>
/// drives the clusters through an <see cref="InProcessSagaControlChannel"/>, so the
/// full prepare / decide / finalize state machine runs without a gRPC transport.
/// This mirrors <c>CoordinatedRestoreSagaModelTests</c>'s wiring so the chaos loops
/// reuse a proven harness.
/// </summary>
internal static class CoordinatedRestoreSagaHarness
{
    /// <summary>A single cluster's participant grain and the fake engine backing it.</summary>
    internal sealed class Cluster
    {
        public required string ClusterId { get; init; }
        public required CrossClusterSagaParticipantGrain Grain { get; init; }
        public required FakeCoordinatedRestoreEngine Engine { get; init; }
        public required ISagaWriteFenceGrain Fence { get; init; }
    }

    /// <summary>
    /// Builds one cluster harness for <paramref name="sagaId"/>. The optional
    /// <paramref name="configureEngine"/> and <paramref name="refuseCapacity"/>
    /// hooks let a chaos test inject a build fault or an infeasible-target refusal.
    /// </summary>
    public static Cluster CreateCluster(
        string clusterId,
        string sagaId,
        string targetTree,
        Action<FakeCoordinatedRestoreEngine>? configureEngine = null,
        bool refuseCapacity = false)
    {
        var engine = new FakeCoordinatedRestoreEngine { TargetTree = targetTree };
        configureEngine?.Invoke(engine);

        var capacity = Substitute.For<IRestoreCapacityProbe>();
        capacity.CanHostAsync(Arg.Any<Backup.RestoreAdmissionReport>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(!refuseCapacity));

        var fence = Substitute.For<ISagaWriteFenceGrain>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ISagaWriteFenceGrain>(Arg.Any<string>()).Returns(fence);

        var restoreParticipant = new RestoreParticipant(
            engine, engine, capacity, factory, NullLogger<RestoreParticipant>.Instance);

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("saga-participant", sagaId));

        var reminders = Substitute.For<IReminderRegistry>();
        reminders.GetReminder(Arg.Any<GrainId>(), Arg.Any<string>())
            .Returns(Task.FromResult(Substitute.For<IGrainReminder>()));

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.CurrentValue.Returns(new LatticeOptions());

        var grain = new CrossClusterSagaParticipantGrain(
            context, [restoreParticipant], reminders, optionsMonitor,
            NullLogger<CrossClusterSagaParticipantGrain>.Instance,
            new FakePersistentState<CrossClusterSagaParticipantState>());

        return new Cluster { ClusterId = clusterId, Grain = grain, Engine = engine, Fence = fence };
    }

    /// <summary>Builds a real coordinator grain keyed by <paramref name="sagaId"/> over the channel.</summary>
    public static CrossClusterSagaCoordinatorGrain CreateCoordinator(string sagaId, ISagaControlChannel channel)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("saga-coordinator", sagaId));

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

    /// <summary>Builds a control request for a single-tree restore saga.</summary>
    public static SagaControlRequest Request(string sagaId, string targetTree, string manifestId, string coordinatorCluster) =>
        new()
        {
            SagaId = sagaId,
            TargetTree = targetTree,
            ManifestId = manifestId,
            CoordinatorClusterId = coordinatorCluster,
        };
}
