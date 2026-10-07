using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// The security seams of the de-enrolment drop path (issue #4692). The path acts
/// on a wire-supplied tree id and cross-tree operation id: it may tell a
/// receiver barrier a participant is absent only for an origin a configured
/// <see cref="LatticeReplicationOptions.ReplicationPeers"/> list admits, and it
/// may discard a pending bucket only on a tree that is registered here.
/// </summary>
public partial class ReplicationApplierTests
{
    private const string DeenrolledTree = "deenrolled-tree";
    private const string Operation = "xop-deenrol";

    /// <summary>Every tree is last-writer-wins replicated here except <see cref="DeenrolledTree"/>.</summary>
    private sealed class AllButDeenrolledContext : ILatticeReplicationContext
    {
        public bool IsReplicationEnabled => true;

        public string LocalReplicaId => LocalCluster;

        public LatticeMergeMode? ResolveMergeMode(string treeId) =>
            string.Equals(treeId, DeenrolledTree, StringComparison.Ordinal) ? null : LatticeMergeMode.LwwRegister;
    }

    private static (ReplicationApplier Applier, IGrainFactory Factory, ILatticeCrossTreeReceiverGrain Barrier, ILatticeRegistry Registry)
        CreateDeenrolApplier(IReadOnlyCollection<string>? peers, bool treeRegistered)
    {
        var factory = Substitute.For<IGrainFactory>();
        var monitor = Substitute.For<Microsoft.Extensions.Options.IOptionsMonitor<LatticeReplicationOptions>>();
        var options = new LatticeReplicationOptions { ClusterId = LocalCluster, ReplicationPeers = peers };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);

        var barrier = Substitute.For<ILatticeCrossTreeReceiverGrain>();
        barrier.NotifyParticipantAbsentAsync(Arg.Any<string>()).Returns(CrossTreeReceiverDecision.InFlight);
        factory.GetGrain<ILatticeCrossTreeReceiverGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(barrier);

        var registry = Substitute.For<ILatticeRegistry>();
        registry.ExistsAsync(DeenrolledTree).Returns(treeRegistered);
        registry.ResolveAsync(DeenrolledTree).Returns(DeenrolledTree);
        registry.GetShardMapAsync(DeenrolledTree).Returns((ShardMap?)null);
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);

        var shard = Substitute.For<IShardRootGrain>();
        shard.GetLeftmostLeafIdAsync().Returns((GrainId?)null);
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>()).Returns(shard);

        var applier = new ReplicationApplier(factory, monitor, replicationContext: new AllButDeenrolledContext());
        return (applier, factory, barrier, registry);
    }

    private static WalRecord DeenrolledTerminal() => new()
    {
        TreeId = DeenrolledTree,
        Op = MutationKind.TxCommit,
        Key = "0",
        ShardIndex = 0,
        Timestamp = Hlc(500),
        OriginClusterId = RemoteCluster,
        TransactionId = Guid.NewGuid(),
        AtomicShardCount = 1,
        CrossTreeOperationId = Operation,
        CrossTreeParticipants = new[] { Tree, DeenrolledTree },
    };

    [Test]
    public async Task A_dropped_cross_tree_terminal_tells_its_barrier_the_tree_is_absent()
    {
        var (applier, _, barrier, _) = CreateDeenrolApplier(peers: null, treeRegistered: true);

        var result = await applier.ApplyAsync(DeenrolledTerminal());

        Assert.That(result.Applied, Is.False);
        await barrier.Received(1).NotifyParticipantAbsentAsync(DeenrolledTree);
    }

    [Test]
    public async Task A_dropped_cross_tree_terminal_from_an_origin_outside_the_peer_list_touches_no_barrier()
    {
        var (applier, factory, barrier, registry) = CreateDeenrolApplier(peers: new[] { "some-other-peer" }, treeRegistered: true);

        await applier.ApplyAsync(DeenrolledTerminal());

        await barrier.DidNotReceiveWithAnyArgs().NotifyParticipantAbsentAsync(default!);
        factory.DidNotReceiveWithAnyArgs().GetGrain<ILatticeCrossTreeReceiverGrain>(default!, default!);
        await registry.DidNotReceiveWithAnyArgs().ExistsAsync(default!);
    }

    [Test]
    public async Task A_dropped_cross_tree_terminal_for_an_unregistered_tree_discards_nothing()
    {
        var (applier, factory, barrier, registry) = CreateDeenrolApplier(peers: null, treeRegistered: false);

        await applier.ApplyAsync(DeenrolledTerminal());

        await barrier.Received(1).NotifyParticipantAbsentAsync(DeenrolledTree);
        await registry.DidNotReceiveWithAnyArgs().ResolveAsync(default!);
        factory.DidNotReceiveWithAnyArgs().GetGrain<IShardRootGrain>(default!, default!);
    }

    [Test]
    public async Task A_dropped_cross_tree_terminal_for_a_registered_tree_discards_its_bucket()
    {
        var (applier, factory, _, registry) = CreateDeenrolApplier(peers: null, treeRegistered: true);

        await applier.ApplyAsync(DeenrolledTerminal());

        await registry.Received(1).ResolveAsync(DeenrolledTree);
        factory.ReceivedWithAnyArgs().GetGrain<IShardRootGrain>(default!, default!);
    }

    [Test]
    public async Task A_dropped_run_of_cross_tree_terminals_on_the_batch_path_tells_the_barrier()
    {
        var (applier, _, barrier, _) = CreateDeenrolApplier(peers: null, treeRegistered: true);

        await applier.ApplyBatchAsync(new[] { DeenrolledTerminal(), DeenrolledTerminal() with { TransactionId = Guid.NewGuid() } });

        await barrier.Received(2).NotifyParticipantAbsentAsync(DeenrolledTree);
    }
}
