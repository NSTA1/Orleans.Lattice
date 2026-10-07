using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4684's cross-tree decision hold keeps a cross-tree sub-saga's
/// decision until every peer that ever enrolled on a participant tree has
/// acknowledged past it - even once that peer is removed from
/// <c>ReplicationPeers</c> (a reversible detach the hold deliberately keeps
/// waiting through). A peer gone for good would otherwise pin decisions
/// forever, which is exactly what <see cref="ILatticeReplicationPeerDecommissioner"/>
/// exists to resolve: it drops the peer from every tree's durable enrolment,
/// which both releases the hold and makes a later re-add of the same cluster
/// id a fresh bootstrap rather than a resumed one. Runs the real registry,
/// the real cross-tree decision hold, its tracker and enrolment grains, and
/// the real shippers; the peer is never present in <c>ReplicationPeers</c>
/// (it is enrolled dynamically, as production enrolment works, the first time
/// a shipper reads its log) so that the decommissioner's "still configured"
/// guard is satisfied throughout.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class DecommissionPeerIntegrationTests
{
    private const string LocalClusterId = "dcp-site-a";
    private const string PeerClusterId = "dcp-site-b";

    /// <summary>The silo's replication topology, so a test can re-add a peer at runtime.</summary>
    private static readonly FakeReplicationTopology Topology = new();

    private TestCluster _cluster = null!;

    private IServiceProvider SiloServices => ((InProcessSiloHandle)_cluster.Primary).SiloHost.Services;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        PerTreeGatedTransport.Refused.Clear();
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }

        PerTreeGatedTransport.Refused.Clear();
    }

    [Test]
    public async Task Decommissioning_the_peer_releases_a_cross_tree_decision_hold_it_was_pinning()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var peer = PeerClusterId + "-" + suffix;
        var treeA = "dcp-a-" + suffix;
        var treeB = "dcp-b-" + suffix;
        var client = _cluster.Client;
        foreach (var tree in new[] { treeA, treeB })
        {
            await client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(
                tree, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 64, MaxInternalChildren = 4 });
            await client.GetGrain<IReplicationShipperGrain>($"{tree}/{peer}").EnsureActiveAsync(CancellationToken.None);
        }

        await client.SetManyAtomicAsync(
            [
                new LatticeTreeBatch(treeA, [new("k", [1])]),
                new LatticeTreeBatch(treeB, [new("k", [2])]),
            ],
            "dcp-op-" + suffix);
        var txA = await CrossTreeSubSagaAsync(treeA);
        var txB = await CrossTreeSubSagaAsync(treeB);
        var registryA = TxRegistryRouting.GetRegistry(client, treeA, txA);
        var registryB = TxRegistryRouting.GetRegistry(client, treeB, txB);
        await AwaitShippedAsync(treeA, peer);
        await AwaitShippedAsync(treeB, peer);

        // The peer stops acknowledging tree B for good - it never catches up
        // again - and tree B is written once more. Tree A's own log is
        // trimmed entirely, so only the cross-tree hold can keep its decision.
        // Both registries are aged and pruned: each participant's own registry
        // is what records that participant's boundary (see
        // ReplicationCrossTreeDecisionHold), so tree B's boundary needs tree
        // B's own registry to be asked, not only tree A's.
        PerTreeGatedTransport.Refused[treeB] = true;
        await client.GetGrain<ILattice>(treeB).SetAsync("late", [9]);
        await TrimAllAsync(treeA);
        await TrimAllAsync(treeB);

        for (var i = 0; i < 6; i++)
        {
            await AgeAndPruneAsync(registryB);
            await AgeAndPruneAsync(registryA);
        }

        Assert.That(await registryA.GetRecordedStatusAsync(txA), Is.EqualTo(TxStatus.Committed),
            "tree A's cross-tree decision must outlive its retention while the peer has not acknowledged past tree B's part");

        // The peer is decommissioned (permanently, unlike a mere detach):
        // every tree's enrolment of it is dropped, which is what the hold
        // actually reads to decide whether the peer is still owed an
        // acknowledgement.
        var decommissioner = SiloServices.GetRequiredService<ILatticeReplicationPeerDecommissioner>();
        await decommissioner.DecommissionPeerAsync(peer, CancellationToken.None);

        await TestPoll.UntilAsync(
            async () =>
            {
                await AgeAndPruneAsync(registryB);
                await AgeAndPruneAsync(registryA);
                return await registryA.GetRecordedStatusAsync(txA) == TxStatus.InFlight;
            },
            "tree A's cross-tree decision to be purged once the peer that was never going to acknowledge is decommissioned",
            TimeSpan.FromSeconds(60));
    }

    [Test]
    public async Task A_peer_re_added_after_decommission_is_a_fresh_bootstrap_not_a_resumed_one()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var peer = PeerClusterId + "-" + suffix;
        var tree = "dcp-reboot-" + suffix;
        var client = _cluster.Client;
        await client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(
            tree, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 64, MaxInternalChildren = 4 });
        var shipper = client.GetGrain<IReplicationShipperGrain>($"{tree}/{peer}");
        await shipper.EnsureActiveAsync(CancellationToken.None);
        await client.GetGrain<ILattice>(tree).SetAsync("k", [1]);
        await AwaitShippedAsync(tree, peer);

        var decommissioner = SiloServices.GetRequiredService<ILatticeReplicationPeerDecommissioner>();
        await decommissioner.DecommissionPeerAsync(peer, CancellationToken.None);

        // A later re-add drives the shipper active again exactly as the
        // activation service would for a freshly-configured peer.
        await shipper.EnsureActiveAsync(CancellationToken.None);

        var stats = SiloServices.GetRequiredService<ReplicationPeerStats>();
        var rows = stats.ReadStatusPage(new ReplicationPeerStatusReadRequest { TreeId = tree, Peer = peer, Limit = 10 });
        Assert.That(rows, Has.Length.EqualTo(1), "the re-added peer's status row must exist");
        Assert.That(rows[0].ReseedRequiredSeconds, Is.Not.Null,
            "a peer re-added after decommission must bootstrap fresh (reseed required), never resume where a decommissioned peer left off");
    }

    [Test]
    public async Task Decommissioning_the_peer_unblocks_a_receiver_side_barrier_that_was_still_waiting_on_it()
    {
        // Issue #4723: the receiver half. A receiver cluster that never
        // acknowledges one of a cross-tree batch's participant trees (the
        // peer dropped the terminal at its own enrollment gate, or is simply
        // gone for good) otherwise pins that batch's receiver-side barrier
        // forever. Decommissioning the peer must notice every undecided
        // barrier it originated and tell it the missing tree is never
        // coming, exactly as NotifyParticipantAbsentAsync does.
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var peer = PeerClusterId + "-" + suffix;
        var treeA = "dcp-recv-a-" + suffix;
        var treeB = "dcp-recv-b-" + suffix;
        var operationId = "dcp-recv-op-" + suffix;
        var client = _cluster.Client;
        foreach (var tree in new[] { treeA, treeB })
        {
            await client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(
                tree, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 64, MaxInternalChildren = 4 });
        }

        var barrierKey = LatticeCrossTreeReceiverGrain.ComputeKey(peer, operationId);
        var receiver = client.GetGrain<ILatticeCrossTreeReceiverGrain>(barrierKey);

        // Only tree A's terminal ever arrives; tree B's never will, because
        // the peer that would have sent it is about to be decommissioned.
        await receiver.NotifyTerminalAsync(new CrossTreeReceiverTerminal
        {
            OriginClusterId = peer,
            OperationId = operationId,
            TreeId = treeA,
            TransactionId = Guid.NewGuid(),
            Committed = true,
            WaitSet = [treeA, treeB],
            ObservedSourceShards = [],
            TerminalHlc = HybridLogicalClock.Zero,
        });

        Assert.That((await receiver.GetStatusAsync()).Decided, Is.False,
            "the barrier must still be waiting on tree B's terminal before the peer is decommissioned");

        var decommissioner = SiloServices.GetRequiredService<ILatticeReplicationPeerDecommissioner>();
        await decommissioner.DecommissionPeerAsync(peer, CancellationToken.None);

        // Every tree of the barrier is a replica of the decommissioned peer, so
        // none is left to vote (#4742): the barrier is abandoned whole, deciding
        // nothing, and withdrawn from both trees' indexes so it pins no fence.
        var status = await receiver.GetStatusAsync();
        Assert.Multiple(async () =>
        {
            Assert.That(status.Opened, Is.False, "the barrier must no longer wait on a tree that will never arrive");
            Assert.That(status.Decided, Is.False, "an abandoned barrier decides nothing");
            Assert.That(await client.GetGrain<ICrossTreeBarrierIndexGrain>(treeA).GetAsync(), Does.Not.Contain(barrierKey));
            Assert.That(await client.GetGrain<ICrossTreeBarrierIndexGrain>(treeB).GetAsync(), Does.Not.Contain(barrierKey));
        });
    }

    [Test]
    public async Task Re_adding_a_decommissioned_peer_resets_a_barrier_a_late_terminal_reopened_so_the_fence_lifts()
    {
        // A terminal already past the enrolment gate when the decommission
        // abandoned its barrier reopens a fresh one, which stays open and
        // indexed and so would hold the tree's import fence against the re-add's
        // fresh bootstrap. The re-add resets every barrier the peer left (#4742).
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var peer = PeerClusterId + "-" + suffix;
        var treeA = "dcp-readd-a-" + suffix;
        var treeB = "dcp-readd-b-" + suffix;
        var operationId = "dcp-readd-op-" + suffix;
        var client = _cluster.Client;
        foreach (var tree in new[] { treeA, treeB })
        {
            await client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(
                tree, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 64, MaxInternalChildren = 4 });
        }

        var barrierKey = LatticeCrossTreeReceiverGrain.ComputeKey(peer, operationId);
        var receiver = client.GetGrain<ILatticeCrossTreeReceiverGrain>(barrierKey);
        CrossTreeReceiverTerminal Arrival() => new()
        {
            OriginClusterId = peer,
            OperationId = operationId,
            TreeId = treeA,
            TransactionId = Guid.NewGuid(),
            Committed = true,
            WaitSet = [treeA, treeB],
            ObservedSourceShards = [],
            TerminalHlc = HybridLogicalClock.Zero,
        };

        await receiver.NotifyTerminalAsync(Arrival());
        await SiloServices.GetRequiredService<ILatticeReplicationPeerDecommissioner>().DecommissionPeerAsync(peer, CancellationToken.None);
        await receiver.NotifyTerminalAsync(Arrival());
        Assert.That(await client.GetGrain<ICrossTreeBarrierIndexGrain>(treeB).GetAsync(), Does.Contain(barrierKey),
            "precondition: the late terminal reopened the barrier, which holds tree B's fence");

        Topology.EmitAdded(peer);

        await TestPoll.UntilAsync(
            async () => !(await receiver.GetStatusAsync()).Opened
                && !(await client.GetGrain<ICrossTreeBarrierIndexGrain>(treeA).GetAsync()).Contains(barrierKey)
                && !(await client.GetGrain<ICrossTreeBarrierIndexGrain>(treeB).GetAsync()).Contains(barrierKey),
            "the re-add to reset the reopened barrier and lift both trees' fences",
            TimeSpan.FromSeconds(15));
        Assert.That(
            await client.GetGrain<IReplicationDecommissionedPeerRegistryGrain>(IReplicationDecommissionedPeerRegistryGrain.SingletonKey).IsDecommissionedAsync(peer),
            Is.False, "the re-add clears the decommissioned marker once the barriers are reset");
    }

    [Test]
    public async Task A_decommissioned_peer_cannot_re_enrol_without_a_fresh_bootstrap()
    {
        // Issue #4723 (F7): the decommissioned-peer registry must be read on
        // every enrolment attempt, not only honoured at decommission time -
        // otherwise a peer that never truly comes back (no fresh entry in
        // ReplicationPeers) could still talk its way back onto a tree's
        // enrolment the moment anything calls EnrolAsync for it again.
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var peer = PeerClusterId + "-" + suffix;
        var tree = "dcp-noreenrol-" + suffix;
        var client = _cluster.Client;
        await client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(
            tree, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 64, MaxInternalChildren = 4 });

        var enrolment = client.GetGrain<ICrossTreePeerEnrolmentGrain>(tree);
        await enrolment.EnrolAsync(peer);
        Assert.That(await enrolment.GetAsync(), Does.Contain(peer),
            "the peer must be enrolled before it is decommissioned");

        var decommissioner = SiloServices.GetRequiredService<ILatticeReplicationPeerDecommissioner>();
        await decommissioner.DecommissionPeerAsync(peer, CancellationToken.None);

        // The peer is still not configured in ReplicationPeers (it never is,
        // in this fixture), so a bare re-enrol attempt - the shape a resumed
        // (rather than freshly bootstrapped) shipper would make - must be
        // refused: only an explicit fresh bootstrap (the peer back in
        // ReplicationPeers) may clear a decommissioned peer's slate.
        await enrolment.EnrolAsync(peer);

        Assert.That(await enrolment.GetAsync(), Does.Not.Contain(peer),
            "a decommissioned peer must not be able to re-enrol itself without a fresh bootstrap");
    }

    [Test]
    public async Task Detaching_the_peer_from_the_log_does_not_decommission_it()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var peer = PeerClusterId + "-" + suffix;
        var tree = "dcp-detach-" + suffix;
        var client = _cluster.Client;
        await client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(
            tree, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 64, MaxInternalChildren = 4 });
        var shipper = client.GetGrain<IReplicationShipperGrain>($"{tree}/{peer}");
        await shipper.EnsureActiveAsync(CancellationToken.None);
        await client.GetGrain<ILattice>(tree).SetAsync("k", [1]);
        await AwaitShippedAsync(tree, peer);

        // A detach is reversible and must leave the tree's durable enrolment
        // alone - only decommission removes it. Call the exact mechanism the
        // driver uses when a peer drops out of ReplicationPeers, without ever
        // decommissioning.
        await shipper.DetachFromLogAsync(CancellationToken.None);

        var enrolled = await client.GetGrain<ICrossTreePeerEnrolmentGrain>(tree).GetAsync();
        Assert.That(enrolled, Does.Contain(peer),
            "a mere detach must keep the peer's enrolment - only DecommissionPeerAsync removes it");
    }

    [Test]
    public async Task Decommissioning_the_peer_discards_its_pending_buckets_whose_terminals_never_arrived()
    {
        // A receiver stages a prepare on delivery, before its terminal. Once the
        // peer is decommissioned no terminal will ever arrive, so every bucket
        // it left pending - a cross-tree participant whose terminal had not
        // arrived, and a single-tree saga - must be discarded rather than
        // stranded (the model's RNoStrandedPrepare).
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var peer = PeerClusterId + "-" + suffix;
        var treeA = "dcp-strand-a-" + suffix;
        var treeB = "dcp-strand-b-" + suffix;
        var treeS = "dcp-strand-s-" + suffix;
        var client = _cluster.Client;
        foreach (var tree in new[] { treeA, treeB, treeS })
        {
            await client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(tree, new TreeRegistryEntry());
        }

        var applier = SiloServices.GetRequiredService<ReplicationApplier>();
        var (txA, txB, txS) = (Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());
        await applier.ApplyBatchAsync([PeerPrepare(peer, treeA, "k", txA, size: 1, index: 0, ticks: 1_000)]);
        await applier.ApplyBatchAsync([PeerPrepare(peer, treeB, "k", txB, size: 1, index: 0, ticks: 1_000)]);
        await applier.ApplyBatchAsync([PeerCommit(peer, treeA, "k", txA, 1_100) with
        {
            AtomicShardCount = 1,
            CrossTreeOperationId = "dcp-strand-op-" + suffix,
            CrossTreeParticipants = [treeA, treeB],
        }]);
        await applier.ApplyBatchAsync([PeerPrepare(peer, treeS, "s1", txS, size: 2, index: 0, ticks: 2_000)]);
        await applier.ApplyBatchAsync([PeerPrepare(peer, treeS, "s2", txS, size: 2, index: 1, ticks: 2_001)]);

        Assert.That(await CountPendingAsync(treeB, peer), Is.GreaterThan(0),
            "precondition: tree B holds the cross-tree prepare whose terminal never arrived");
        Assert.That(await CountPendingAsync(treeS, peer), Is.GreaterThan(0),
            "precondition: the single-tree saga's prepares are staged with no terminal");

        var decommissioner = SiloServices.GetRequiredService<ILatticeReplicationPeerDecommissioner>();
        await decommissioner.DecommissionPeerAsync(peer, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await CountPendingAsync(treeA, peer), Is.Zero, "tree A's sub-saga is settled");
            Assert.That(await CountPendingAsync(treeB, peer), Is.Zero,
                "the cross-tree prepare whose terminal never arrived must be discarded, not stranded");
            Assert.That(await CountPendingAsync(treeS, peer), Is.Zero,
                "the single-tree saga's prepares must be discarded, not stranded");
        });
    }

    [Test]
    public async Task Decommission_drains_a_decided_cross_tree_operation_whose_finalize_never_ran_on_every_tree()
    {
        // Issue #4742's second phase: a barrier from the peer that had already
        // decided commit when the peer was decommissioned, but whose finalize
        // of its trees never ran (it failed, and no redelivery will come), so
        // both trees still hold the operation's buckets under a delegation to
        // the barrier. The decommission leaves a decided barrier alone and
        // settles each tree's buckets by the verdict its registry resolves -
        // through the delegation, the barrier's commit - so both trees are
        // drained post-saga. A tree's own row is undecided here, so settling
        // by it would discard both buckets and lose a committed write.
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var peer = PeerClusterId + "-" + suffix;
        var treeA = $"dcp-decided-a-{suffix}";
        var treeB = $"dcp-decided-b-{suffix}";
        var client = _cluster.Client;
        foreach (var tree in new[] { treeA, treeB })
        {
            await client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(tree, new TreeRegistryEntry());
        }

        var applier = SiloServices.GetRequiredService<ReplicationApplier>();
        var (txA, txB) = (Guid.NewGuid(), Guid.NewGuid());
        var operationId = "dcp-decided-op-" + suffix;
        await applier.ApplyBatchAsync([PeerPrepare(peer, treeA, "k", txA, size: 1, index: 0, ticks: 1_000)]);
        await applier.ApplyBatchAsync([PeerPrepare(peer, treeB, "k", txB, size: 1, index: 0, ticks: 1_000)]);

        // Tree A's terminal arrives through the applier and delegates tree A to
        // the barrier, which waits for tree B.
        await applier.ApplyBatchAsync([PeerCommit(peer, treeA, "k", txA, 1_100) with
        {
            AtomicShardCount = 1,
            CrossTreeOperationId = operationId,
            CrossTreeParticipants = [treeA, treeB],
        }]);

        // Tree B's terminal registers and notifies, deciding the barrier commit,
        // and then its finalize of both trees fails: the returned finalize set
        // is never acted on.
        var barrierKey = LatticeCrossTreeReceiverGrain.ComputeKey(peer, operationId);
        var barrier = client.GetGrain<ILatticeCrossTreeReceiverGrain>(barrierKey);
        await TxRegistryRouting.GetRegistry(client, treeB, txB).RegisterReceiverDecisionAuthorityAsync(txB, barrierKey);
        var decided = await barrier.NotifyTerminalAsync(new CrossTreeReceiverTerminal
        {
            OriginClusterId = peer,
            OperationId = operationId,
            TreeId = treeB,
            TransactionId = txB,
            Committed = true,
            WaitSet = [treeA, treeB],
            ObservedSourceShards = [LatticeSharding.GetShardIndex("k", LatticeConstants.DefaultShardCount)],
            TerminalHlc = Hlc(1_100),
        });

        Assert.Multiple(async () =>
        {
            Assert.That(decided.Decided && decided.Committed, Is.True, "precondition: the barrier decided commit");
            Assert.That(await CountPendingAsync(treeA, peer), Is.GreaterThan(0), "precondition: tree A's finalize never ran");
            Assert.That(await CountPendingAsync(treeB, peer), Is.GreaterThan(0), "precondition: tree B's finalize never ran");
            Assert.That(await TxRegistryRouting.GetRegistry(client, treeA, txA).GetRecordedStatusAsync(txA), Is.Not.EqualTo(TxStatus.Committed),
                "precondition: tree A's own row is undecided, so only the delegation carries the verdict");
        });

        await SiloServices.GetRequiredService<ILatticeReplicationPeerDecommissioner>().DecommissionPeerAsync(peer, CancellationToken.None);

        Assert.Multiple(async () =>
        {
            Assert.That(await CountPendingAsync(treeA, peer), Is.Zero, "tree A's bucket is settled");
            Assert.That(await CountPendingAsync(treeB, peer), Is.Zero, "tree B's bucket is settled");
            Assert.That(await client.GetGrain<ILattice>(treeA).GetAsync("k"), Is.EqualTo(new byte[] { 1 }),
                "tree A is drained post-saga by the barrier's commit, not discarded by its own undecided row");
            Assert.That(await client.GetGrain<ILattice>(treeB).GetAsync("k"), Is.EqualTo(new byte[] { 1 }),
                "tree B is drained post-saga with it");
            Assert.That(await barrier.GetDecisionAsync(), Is.EqualTo(TxStatus.Committed), "a decided barrier is not abandoned");
        });
    }

    [TestCase("z", "a", TestName = "Decommission_serves_a_cross_tree_operation_all_or_nothing_when_its_arrived_tree_is_walked_last")]
    [TestCase("a", "z", TestName = "Decommission_serves_a_cross_tree_operation_all_or_nothing_when_its_arrived_tree_is_walked_first")]
    public async Task Decommission_serves_a_cross_tree_operation_all_or_nothing_whatever_the_tree_order(string arrivedTag, string missingTag)
    {
        // Issue #4742: a committed cross-tree operation from the peer with one
        // tree's terminal arrived and the other's prepare staged only. The
        // registry enumerates trees in name order, so the two cases walk the
        // arrived tree last and first. Whatever the order, the decommission must
        // leave the operation all-or-nothing: never one tree post-saga and the
        // other pre-saga.
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var peer = PeerClusterId + "-" + suffix;
        var treeArrived = $"dcp-order-{arrivedTag}-{suffix}";
        var treeMissing = $"dcp-order-{missingTag}-{suffix}";
        var client = _cluster.Client;
        foreach (var tree in new[] { treeArrived, treeMissing })
        {
            await client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(tree, new TreeRegistryEntry());
        }

        var applier = SiloServices.GetRequiredService<ReplicationApplier>();
        var (txArrived, txMissing) = (Guid.NewGuid(), Guid.NewGuid());
        var operationId = "dcp-order-op-" + suffix;
        await applier.ApplyBatchAsync([PeerPrepare(peer, treeArrived, "k", txArrived, size: 1, index: 0, ticks: 1_000)]);
        await applier.ApplyBatchAsync([PeerPrepare(peer, treeMissing, "k", txMissing, size: 1, index: 0, ticks: 1_000)]);
        await applier.ApplyBatchAsync([PeerCommit(peer, treeArrived, "k", txArrived, 1_100) with
        {
            AtomicShardCount = 1,
            CrossTreeOperationId = operationId,
            CrossTreeParticipants = [treeArrived, treeMissing],
        }]);

        var barrier = client.GetGrain<ILatticeCrossTreeReceiverGrain>(LatticeCrossTreeReceiverGrain.ComputeKey(peer, operationId));
        Assert.That((await barrier.GetStatusAsync()).Decided, Is.False, "precondition: the barrier waits for the missing tree");

        await SiloServices.GetRequiredService<ILatticeReplicationPeerDecommissioner>().DecommissionPeerAsync(peer, CancellationToken.None);

        var arrived = await client.GetGrain<ILattice>(treeArrived).GetAsync("k");
        var missing = await client.GetGrain<ILattice>(treeMissing).GetAsync("k");
        var verdict = await barrier.GetDecisionAsync();
        Assert.Multiple(async () =>
        {
            Assert.That(arrived is null, Is.EqualTo(missing is null),
                $"the operation must be served all-or-nothing after the decommission (arrived={(arrived is null ? "pre" : "post")}, missing={(missing is null ? "pre" : "post")})");
            Assert.That(verdict == TxStatus.Committed, Is.EqualTo(arrived is not null),
                $"the barrier's verdict ({verdict}) must agree with what the trees serve (arrived={(arrived is null ? "pre" : "post")}): a committed barrier over aborted buckets is a split");
            Assert.That(await CountPendingAsync(treeArrived, peer), Is.Zero, "no bucket of the peer is stranded on the arrived tree");
            Assert.That(await CountPendingAsync(treeMissing, peer), Is.Zero, "no bucket of the peer is stranded on the missing tree");
        });
    }

    [Test]
    public async Task Decommissioning_one_peer_leaves_another_peers_open_barrier_and_buckets_untouched()
    {
        // The decommission abandons only the barriers the decommissioned peer
        // originated. Another peer's undecided barrier, its arrivals and its
        // staged buckets must survive, or that peer's sagas would be stranded.
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var gone = PeerClusterId + "-gone-" + suffix;
        var other = PeerClusterId + "-other-" + suffix;
        var treeA = "dcp-other-a-" + suffix;
        var treeB = "dcp-other-b-" + suffix;
        var operationId = "dcp-other-op-" + suffix;
        var client = _cluster.Client;
        foreach (var tree in new[] { treeA, treeB })
        {
            await client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(tree, new TreeRegistryEntry());
        }

        var applier = SiloServices.GetRequiredService<ReplicationApplier>();
        var (txA, txB) = (Guid.NewGuid(), Guid.NewGuid());
        await applier.ApplyBatchAsync([PeerPrepare(other, treeA, "k", txA, size: 1, index: 0, ticks: 1_000)]);
        await applier.ApplyBatchAsync([PeerPrepare(other, treeB, "k", txB, size: 1, index: 0, ticks: 1_000)]);
        await applier.ApplyBatchAsync([PeerCommit(other, treeA, "k", txA, 1_100) with
        {
            AtomicShardCount = 1,
            CrossTreeOperationId = operationId,
            CrossTreeParticipants = [treeA, treeB],
        }]);
        var barrierKey = LatticeCrossTreeReceiverGrain.ComputeKey(other, operationId);
        var barrier = client.GetGrain<ILatticeCrossTreeReceiverGrain>(barrierKey);
        Assert.That((await barrier.GetStatusAsync()).Opened, Is.True, "precondition: the other peer's barrier is open");

        await SiloServices.GetRequiredService<ILatticeReplicationPeerDecommissioner>().DecommissionPeerAsync(gone, CancellationToken.None);

        var status = await barrier.GetStatusAsync();
        Assert.Multiple(async () =>
        {
            Assert.That(status.Opened, Is.True, "another peer's barrier must not be abandoned");
            Assert.That(status.Decided, Is.False);
            Assert.That(status.ArrivedTrees, Is.EquivalentTo(new[] { treeA }), "its arrivals are kept");
            Assert.That(await client.GetGrain<ICrossTreeBarrierIndexGrain>(treeB).GetAsync(), Does.Contain(barrierKey),
                "it stays indexed under the tree it still waits for");
            Assert.That(await CountPendingAsync(treeA, other), Is.GreaterThan(0), "its delegated sub-saga on tree A is kept");
            Assert.That(await CountPendingAsync(treeB, other), Is.GreaterThan(0), "its staged prepare on tree B is kept");
        });
    }

    [Test]
    public async Task Re_adding_a_decommissioned_peer_clears_its_decommissioned_marker()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var peer = PeerClusterId + "-" + suffix;
        var registry = _cluster.Client.GetGrain<IReplicationDecommissionedPeerRegistryGrain>(IReplicationDecommissionedPeerRegistryGrain.SingletonKey);
        await SiloServices.GetRequiredService<ILatticeReplicationPeerDecommissioner>().DecommissionPeerAsync(peer, CancellationToken.None);
        Assert.That(await registry.IsDecommissionedAsync(peer), Is.True, "precondition: the peer is marked decommissioned");

        Topology.EmitAdded(peer);

        await TestPoll.UntilAsync(
            async () => !await registry.IsDecommissionedAsync(peer),
            "the re-add to clear the peer's decommissioned marker",
            TimeSpan.FromSeconds(15));
    }

    [Test]
    public async Task A_tree_dropped_from_a_barrier_that_then_decides_is_withdrawn_from_that_trees_index()
    {
        // The dropped tree leaves the wait set before the barrier decides, so
        // the decided barrier's unindex must name it explicitly; otherwise its
        // entry in the dropped tree's index outlives the barrier.
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var peer = PeerClusterId + "-" + suffix;
        var treeA = "dcp-unindex-a-" + suffix;
        var treeB = "dcp-unindex-b-" + suffix;
        var operationId = "dcp-unindex-op-" + suffix;
        var client = _cluster.Client;
        var barrierKey = LatticeCrossTreeReceiverGrain.ComputeKey(peer, operationId);
        var receiver = client.GetGrain<ILatticeCrossTreeReceiverGrain>(barrierKey);
        await receiver.NotifyTerminalAsync(new CrossTreeReceiverTerminal
        {
            OriginClusterId = peer,
            OperationId = operationId,
            TreeId = treeA,
            TransactionId = Guid.NewGuid(),
            Committed = true,
            WaitSet = [treeA, treeB],
            ObservedSourceShards = [],
            TerminalHlc = HybridLogicalClock.Zero,
        });
        Assert.That(await client.GetGrain<ICrossTreeBarrierIndexGrain>(treeB).GetAsync(), Does.Contain(barrierKey),
            "precondition: the barrier is indexed under the tree it waits for");

        var decision = await receiver.NotifyParticipantAbsentAsync(treeB);

        Assert.Multiple(async () =>
        {
            Assert.That(decision.Decided, Is.True, "precondition: dropping tree B decides the barrier on tree A");
            Assert.That(await client.GetGrain<ICrossTreeBarrierIndexGrain>(treeB).GetAsync(), Does.Not.Contain(barrierKey),
                "the dropped tree's index entry must be withdrawn with the decided barrier");
            Assert.That(await client.GetGrain<ICrossTreeBarrierIndexGrain>(treeA).GetAsync(), Does.Not.Contain(barrierKey));
        });
    }

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks, Counter = 0 };

    private static WalRecord PeerPrepare(string peer, string tree, string key, Guid txid, int size, int index, long ticks) => new()
    {
        TreeId = tree,
        Op = MutationKind.Set,
        Key = key,
        Value = [1],
        Timestamp = Hlc(ticks),
        OriginClusterId = peer,
        TransactionId = txid,
        IsPrepared = true,
        AtomicBatchSize = size,
        AtomicBatchIndex = index,
    };

    private static WalRecord PeerCommit(string peer, string tree, string key, Guid txid, long ticks)
    {
        var shard = LatticeSharding.GetShardIndex(key, LatticeConstants.DefaultShardCount);
        return new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.TxCommit,
            Key = shard.ToString(System.Globalization.CultureInfo.InvariantCulture),
            Timestamp = Hlc(ticks),
            OriginClusterId = peer,
            TransactionId = txid,
            ShardIndex = shard,
        };
    }

    /// <summary>How many pending buckets <paramref name="tree"/> holds from <paramref name="origin"/>.</summary>
    private async Task<int> CountPendingAsync(string tree, string origin)
    {
        var registry = _cluster.Client.GetLatticeRegistry();
        var physical = await registry.ResolveAsync(tree);
        var map = await registry.GetShardMapAsync(tree)
            ?? ShardMap.GetOrCreateDefaultShared(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount);
        var slots = Enumerable.Range(0, map.VirtualShardCount).ToArray();
        var count = 0;
        foreach (var shardIndex in map.GetPhysicalShardIndices())
        {
            var leafId = await _cluster.Client.GetGrain<IShardRootGrain>($"{physical}/{shardIndex}").GetLeftmostLeafIdAsync();
            while (leafId is not null)
            {
                var leaf = _cluster.Client.GetGrain<IBPlusLeafGrain>(leafId.Value);
                count += (await leaf.GetPendingMutationsForSlotsAsync(slots, map.VirtualShardCount))
                    .Count(m => m.TransactionId != Guid.Empty && m.OriginClusterId == origin);
                leafId = await leaf.GetNextSiblingAsync();
            }
        }

        return count;
    }

    /// <summary>The txid of <paramref name="tree"/>'s sub-saga of the cross-tree write, from the registry membership.</summary>
    private async Task<Guid> CrossTreeSubSagaAsync(string tree)
    {
        var shards = TxRegistryRouting.ResolveShardCountFromServices(SiloServices);
        foreach (var key in TxRegistryRouting.EnumerateKeys(tree, shards))
        {
            var registry = _cluster.Client.GetGrain<ITxRegistryGrain>(key);
            var decided = await registry.SnapshotAsync();
            var memberships = await registry.GetCrossTreeMembershipsAsync([.. decided.Keys]);
            if (memberships.Count > 0)
            {
                return memberships.Keys.Single();
            }
        }

        Assert.Fail($"tree '{tree}' recorded no cross-tree sub-saga");
        return Guid.Empty;
    }

    private async Task<long[]> TailsAsync(string tree)
    {
        var partitions = await SiloServices.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(tree);
        var tails = new long[partitions];
        for (var p = 0; p < partitions; p++)
        {
            tails[p] = await _cluster.Client.GetGrain<IWalShardGrain>($"{tree}/{p}").GetNextSequenceAsync(CancellationToken.None);
        }

        return tails;
    }

    private async Task AwaitShippedAsync(string tree, string peer)
    {
        var tails = await TailsAsync(tree);
        var consumer = _cluster.Client.GetGrain<IReplicationShipperGrain>($"{tree}/{peer}").AsReference<IWalOffsetConsumer>();
        await TestPoll.UntilAsync(
            async () => ReplicationCrossTreeDecisionHold.IsPast(await consumer.GetDurableReadPositionsAsync(tree), [.. tails]),
            $"the peer to acknowledge everything tree '{tree}' holds",
            TimeSpan.FromSeconds(30));
    }

    private async Task TrimAllAsync(string tree)
    {
        var tails = await TailsAsync(tree);
        var provider = WalProvider();
        for (var p = 0; p < tails.Length; p++)
        {
            if (tails[p] > 0)
            {
                await provider.TrimAsync(tree, p, tails[p] - 1, CancellationToken.None);
            }
        }
    }

    /// <summary>
    /// Waits out the retention and the guards' refresh intervals, then retires
    /// another saga on the registry: its forget refreshes the guards and prunes
    /// every tombstone that may go.
    /// </summary>
    private static async Task AgeAndPruneAsync(ITxRegistryGrain registry)
    {
        await Task.Delay(TimeSpan.FromMilliseconds(500));
        var other = Guid.NewGuid();
        await registry.MarkCommittedAsync(other);
        await registry.ForgetAsync(other);
    }

    private IWalStorageProvider WalProvider()
    {
        Assert.That(
            SiloServices.GetRequiredService<IWalStorageProviderCatalog>().TryGet(IWalStorageProviderCatalog.DefaultProviderKey, out var provider),
            Is.True);
        return provider!;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.ConfigureLattice(o =>
            {
                o.TxDecisionRetention = TimeSpan.FromMilliseconds(300);
                // The test trims the logs itself.
                o.WalGcInterval = TimeSpan.Zero;
            });
            siloBuilder.AddLatticeReplication(o =>
            {
                o.ClusterId = LocalClusterId;
                // The peer is never configured: it is enrolled dynamically the
                // first time a shipper reads its log (as production enrolment
                // works), which keeps the decommissioner's "still configured in
                // ReplicationPeers" guard satisfied throughout every test here.
                o.ReplicationPeers = [];
                // One statically replicated tree, so the driver activation
                // service runs and subscribes to the topology: a test re-adds a
                // peer at runtime through it. No test writes to this tree.
                o.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
                {
                    ["dcp-activation-anchor"] = LatticeMergeMode.LwwRegister,
                };
                o.ShipCursorWriteInterval = 1;
            });
            siloBuilder.Services.AddSingleton<IReplicationTransport, PerTreeGatedTransport>();
            siloBuilder.Services.AddSingleton<IReplicationTopology>(Topology);
            siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, LwwResolver>();
        }
    }

    private sealed class LwwResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }

    /// <summary>A peer that acknowledges every batch except those of a refused tree.</summary>
    private sealed class PerTreeGatedTransport : IReplicationTransport
    {
        public static readonly ConcurrentDictionary<string, bool> Refused = new(StringComparer.Ordinal);

        public Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken) =>
            Task.FromResult(new ReplicationAck
            {
                Accepted = !Refused.ContainsKey(batch.TreeName),
                HighestAppliedHlc = HybridLogicalClock.Zero,
            });
    }
}
