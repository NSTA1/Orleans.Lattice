using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage of <see cref="RestoreSagaDispatcher"/>, the dispatch gate that
/// promotes a restore whose target tree is replicated into a coordinated saga and
/// declines (returns <c>null</c>, so the local shadow-cutover runs) otherwise.
/// Covers the four dispatch cases keyed on the target tree's replication status,
/// the admission refusal of an infeasible target, the all-or-nothing refusal of
/// an unreachable peer, and the committed / aborted saga outcomes.
/// </summary>
[TestFixture]
public class RestoreSagaDispatcherTests
{
    private const string TargetTree = "orders";
    private const string BackupId = "backup-1";
    private const string Self = "site-a";
    private const string Peer = "site-b";

    private sealed class Harness
    {
        public required RestoreSagaDispatcher Dispatcher { get; init; }
        public required FakeCoordinatedRestoreEngine Engine { get; init; }
        public required ICrossClusterSagaCoordinatorGrain Coordinator { get; init; }
        public required ISagaControlChannel Channel { get; init; }
    }

    private static Harness CreateHarness(
        bool targetReplicated = true,
        bool anyReplicated = true,
        IReadOnlyCollection<string>? peers = null,
        bool canHost = true,
        CrossClusterSagaOutcome outcome = CrossClusterSagaOutcome.Committed,
        string? unreachablePeer = null)
    {
        peers ??= [Peer];

        var membership = Substitute.For<IReplicatedTreeMembership>();
        membership.IsReplicated(TargetTree).Returns(targetReplicated);
        membership.ReplicatedTrees.Returns(anyReplicated ? [TargetTree] : Array.Empty<string>());

        var topology = Substitute.For<IReplicationTopology>();
        topology.CurrentPeers.Returns(peers);

        var engine = new FakeCoordinatedRestoreEngine { TargetTree = TargetTree };

        var capacity = Substitute.For<IRestoreCapacityProbe>();
        capacity.CanHostAsync(Arg.Any<RestoreAdmissionReport>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(canHost));

        var channel = Substitute.For<ISagaControlChannel>();
        channel.GetStatusAsync(Arg.Any<string>(), Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new SagaControlResponse()));
        if (unreachablePeer is not null)
        {
            channel.GetStatusAsync(unreachablePeer, Arg.Any<SagaControlRequest>(), Arg.Any<CancellationToken>())
                .Returns<Task<SagaControlResponse>>(_ => throw new InvalidOperationException("peer unreachable"));
        }

        var coordinator = Substitute.For<ICrossClusterSagaCoordinatorGrain>();
        coordinator.RunAsync(Arg.Any<List<string>>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>())
            .Returns(Task.FromResult(outcome));

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ICrossClusterSagaCoordinatorGrain>(Arg.Any<string>()).Returns(coordinator);
        factory.GetGrain<ISagaWriteFenceGrain>(Arg.Any<string>()).Returns(Substitute.For<ISagaWriteFenceGrain>());

        var participant = new RestoreParticipant(
            engine, engine, capacity, factory, NullLogger<RestoreParticipant>.Instance);

        var options = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        options.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = Self });

        var dispatcher = new RestoreSagaDispatcher(
            membership, topology, engine, capacity, channel, factory, participant, options,
            NullLogger<RestoreSagaDispatcher>.Instance);

        return new Harness
        {
            Dispatcher = dispatcher,
            Engine = engine,
            Coordinator = coordinator,
            Channel = channel,
        };
    }

    [Test]
    public async Task TryDispatchAsync_explicit_unreplicated_target_returns_null_without_probing()
    {
        var h = CreateHarness(targetReplicated: false);
        var request = new LatticeRestoreRequest(BackupId, targetTreeId: "logs", mode: LatticeRestoreMode.ShadowCutover);

        var result = await h.Dispatcher.TryDispatchAsync(request);

        Assert.That(result, Is.Null, "an unreplicated target takes the local path");
        Assert.That(h.Engine.ProbeCount, Is.EqualTo(0), "the fast local path avoids manifest I/O");
        await h.Coordinator.DidNotReceive().RunAsync(
            Arg.Any<List<string>>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>());
    }

    [Test]
    public async Task TryDispatchAsync_no_replicated_trees_returns_null()
    {
        var h = CreateHarness(anyReplicated: false);
        var request = new LatticeRestoreRequest(BackupId, mode: LatticeRestoreMode.ShadowCutover);

        var result = await h.Dispatcher.TryDispatchAsync(request);

        Assert.That(result, Is.Null);
        Assert.That(h.Engine.ProbeCount, Is.EqualTo(0));
    }

    [Test]
    public async Task TryDispatchAsync_replicated_target_runs_saga_over_peers_and_self()
    {
        var h = CreateHarness();
        var request = new LatticeRestoreRequest(BackupId, targetTreeId: TargetTree, mode: LatticeRestoreMode.ShadowCutover);

        var result = await h.Dispatcher.TryDispatchAsync(request);

        Assert.That(result, Is.Not.Null);
        Assert.That(result!.TargetTreeId, Is.EqualTo(TargetTree));
        Assert.That(result.Mode, Is.EqualTo(LatticeRestoreMode.ShadowCutover));
        await h.Coordinator.Received(1).RunAsync(
            Arg.Is<List<string>>(l => l.Contains(Self) && l.Contains(Peer)),
            TargetTree, BackupId, Self);
    }

    [Test]
    public async Task TryDispatchAsync_null_request_target_resolves_from_manifest_then_runs_saga()
    {
        var h = CreateHarness();
        // No explicit target: the effective target is resolved from the manifest
        // (the fake resolves it to the replicated tree), which then dispatches.
        var request = new LatticeRestoreRequest(BackupId, mode: LatticeRestoreMode.ShadowCutover);

        var result = await h.Dispatcher.TryDispatchAsync(request);

        Assert.That(result, Is.Not.Null);
        Assert.That(h.Engine.ProbeCount, Is.GreaterThanOrEqualTo(1));
        await h.Coordinator.Received(1).RunAsync(
            Arg.Any<List<string>>(), TargetTree, BackupId, Self);
    }

    [Test]
    public void TryDispatchAsync_infeasible_target_refused_at_admission_before_saga()
    {
        var h = CreateHarness(canHost: false);
        var request = new LatticeRestoreRequest(BackupId, targetTreeId: TargetTree, mode: LatticeRestoreMode.ShadowCutover);

        Assert.That(async () => await h.Dispatcher.TryDispatchAsync(request),
            Throws.InstanceOf<LatticeRestoreValidationException>());
        h.Coordinator.DidNotReceive().RunAsync(
            Arg.Any<List<string>>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>());
    }

    [Test]
    public void TryDispatchAsync_unreachable_peer_refuses_before_saga()
    {
        var h = CreateHarness(unreachablePeer: Peer);
        var request = new LatticeRestoreRequest(BackupId, targetTreeId: TargetTree, mode: LatticeRestoreMode.ShadowCutover);

        Assert.That(async () => await h.Dispatcher.TryDispatchAsync(request),
            Throws.InstanceOf<LatticeRestoreValidationException>());
        h.Coordinator.DidNotReceive().RunAsync(
            Arg.Any<List<string>>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>());
    }

    [Test]
    public void TryDispatchAsync_saga_abort_throws_all_or_nothing()
    {
        var h = CreateHarness(outcome: CrossClusterSagaOutcome.Aborted);
        var request = new LatticeRestoreRequest(BackupId, targetTreeId: TargetTree, mode: LatticeRestoreMode.ShadowCutover);

        Assert.That(async () => await h.Dispatcher.TryDispatchAsync(request),
            Throws.InstanceOf<LatticeRestoreValidationException>());
    }

    [Test]
    public async Task TryDispatchAsync_null_request_throws()
    {
        var h = CreateHarness();

        await Task.CompletedTask;
        Assert.That(async () => await h.Dispatcher.TryDispatchAsync(null!),
            Throws.InstanceOf<ArgumentNullException>());
    }
}
