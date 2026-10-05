using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4615: the replication tombstone reap gate over the real receiver tree
/// frontier, origin frontiers and shipper. A replicated tree reaps a tombstone
/// only below min(D, P): D, the point below which every write of every origin is
/// applied here and none is still held; P, the point below which every peer
/// acknowledged every write this cluster stamped. These are the gate detectors
/// the replication model cites.
/// </summary>
public partial class CrossClusterAtomicVisibilityTests
{
    private static ReplicationTombstoneReapGate ReapGate(IGrainFactory factory, string tree, string local, params string[] peers)
    {
        var membership = Substitute.For<IReplicatedTreeMembership>();
        membership.IsReplicated(tree).Returns(true);
        var topology = Substitute.For<IReplicationTopology>();
        topology.CurrentPeers.Returns(peers);
        var options = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions { ClusterId = local });
        return new ReplicationTombstoneReapGate(factory, membership, topology, options);
    }

    private static async Task CoverAsync(ReplicationTreeFrontierGrain frontier, string origin, HybridLogicalClock watermark)
    {
        var epoch = await frontier.ObserveAsync(origin, null, CancellationToken.None);
        await frontier.ObserveAsync(origin, new ReplicationSourceFrontier
        {
            ReceiverLineage = epoch,
            TreeLowWatermark = watermark,
            OriginLowWatermark = watermark,
            OriginGeneration = 1,
        }, CancellationToken.None);
    }

    [Test]
    public async Task An_idle_origins_heartbeat_advances_the_reap_ceiling()
    {
        const string tree = FrontierReceiver.Tree + "-reap-idle";
        var ticks = DateTime.UtcNow.Ticks;
        var floor = Hlc(ticks, 50);
        var (shipper, feeds, transport, _) = FrontierShipper(tree, floor);
        var receiver = new FrontierReceiver(tree, Guid.NewGuid());
        receiver.Factory.GetGrain<IReplicationTreeFrontierGrain>(tree, Arg.Any<string?>()).Returns(receiver.Frontier);
        var gate = ReapGate(receiver.Factory, tree, TwoSiteClusterFixture.SiteBClusterId);
        await receiver.Frontier.ObserveAsync(TwoSiteClusterFixture.SiteAClusterId, null, CancellationToken.None);

        var beforeAnyShipment = await gate.GetReapCeilingAsync(tree);
        feeds[0].Append(LocalSet(tree, "a", Hlc(ticks, 10)));
        feeds[1].Append(LocalSet(tree, "b", Hlc(ticks, 11)));
        await receiver.ExchangeAsync(shipper, transport, ticks: 1);
        feeds[0].Append(LocalSet(tree, "c", Hlc(ticks, 60)));
        await receiver.ExchangeAsync(shipper, transport, ticks: 2);
        var afterWrites = await gate.GetReapCeilingAsync(tree);

        // The origin writes nothing more; its clock floor keeps advancing, and
        // only a liveness probe can carry it.
        var advanced = Hlc(ticks, 90);
        foreach (var feed in feeds)
        {
            feed.ClockFloor = advanced;
        }

        await receiver.ExchangeAsync(shipper, transport, ticks: 1);
        await Task.Delay(ReplicationShipperGrain.SourceFrontierHeartbeatInterval + TimeSpan.FromMilliseconds(200));
        var shipped = transport.Batches.Count;
        await receiver.ExchangeAsync(shipper, transport, ticks: 2);
        var afterIdle = await gate.GetReapCeilingAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(beforeAnyShipment, Is.EqualTo(HybridLogicalClock.Zero), "an origin with no watermark reaps nothing");
            Assert.That(afterWrites, Is.EqualTo(floor), "precondition: the origin's watermark bounds the ceiling");
            Assert.That(transport.Batches.Skip(shipped).All(b => b.EncodedEnvelope?.EncodedEntries.Length is null or 0), Is.True,
                "precondition: the idle origin shipped only probes");
            Assert.That(afterIdle, Is.EqualTo(advanced), "an idle origin's heartbeat advances the reap ceiling");
        });
    }

    [Test]
    public async Task The_reap_ceiling_stays_below_a_parked_or_dead_lettered_write_and_ignores_a_lost_one()
    {
        const string tree = FrontierReceiver.Tree + "-reap-held";
        var ticks = DateTime.UtcNow.Ticks;
        var covered = Hlc(ticks, 90);
        var receiver = new FrontierReceiver(tree, Guid.NewGuid());
        receiver.Factory.GetGrain<IReplicationTreeFrontierGrain>(tree, Arg.Any<string?>()).Returns(receiver.Frontier);
        var gate = ReapGate(receiver.Factory, tree, TwoSiteClusterFixture.SiteBClusterId);
        await CoverAsync(receiver.Frontier, TwoSiteClusterFixture.SiteAClusterId, covered);
        var origin = receiver.Factory.GetGrain<IReplicationOriginFrontierGrain>(TwoSiteClusterFixture.SiteAClusterId);

        var uncontended = await gate.GetReapCeilingAsync(tree);
        var parked = Hlc(ticks, 40);
        await origin.SetHeldAsync(ReplicationOriginFrontierGrain.BufferSource(tree), [parked]);
        var whileParked = await gate.GetReapCeilingAsync(tree);

        var deadLettered = Hlc(ticks, 30);
        await origin.SetHeldAsync(ReplicationOriginFrontierGrain.BufferSource(tree), []);
        await origin.SetHeldAsync(ReplicationOriginFrontierGrain.DeadLetterSource(tree), [deadLettered]);
        var whileDeadLettered = await gate.GetReapCeilingAsync(tree);

        // An operator discards the dead letter: it is lost, never to be applied.
        await origin.SetHeldAsync(ReplicationOriginFrontierGrain.DeadLetterSource(tree), []);
        await origin.RecordLostAsync([deadLettered]);
        var afterDiscard = await gate.GetReapCeilingAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(uncontended, Is.EqualTo(covered), "precondition: the origin's watermark bounds the ceiling");
            Assert.That(whileParked, Is.EqualTo(parked), "a parked write the tombstone may beat is still to be applied");
            Assert.That(whileDeadLettered, Is.EqualTo(deadLettered), "so is a dead-lettered one, until it is discarded");
            Assert.That(afterDiscard, Is.EqualTo(covered), "a lost write is never applied, so it holds nothing back");
        });
    }

    [Test]
    public async Task The_reap_ceiling_is_zero_while_a_peer_is_off_the_log_and_resumes_after_its_re_seed()
    {
        const string tree = "ccv-reap-peer";
        var ticks = DateTime.UtcNow.Ticks;
        var floor = Hlc(ticks, 50);
        var deleteStamp = Hlc(ticks, 40);
        var (shipper, feeds, transport, _) = FrontierShipper(tree, floor);
        var receiver = new FrontierReceiver(tree, Guid.NewGuid());
        receiver.Factory.GetGrain<IReplicationTreeFrontierGrain>(tree, Arg.Any<string?>()).Returns(receiver.Frontier);
        receiver.Factory.GetGrain<IReplicationShipperGrain>($"{tree}/{TwoSiteClusterFixture.SiteBClusterId}", Arg.Any<string?>())
            .Returns(shipper);

        // This cluster (the origin side) ships to the peer, and the peer's own
        // writes to the tree here are covered far past the tombstone.
        var gate = ReapGate(receiver.Factory, tree, TwoSiteClusterFixture.SiteAClusterId, TwoSiteClusterFixture.SiteBClusterId);
        await CoverAsync(receiver.Frontier, TwoSiteClusterFixture.SiteBClusterId, Hlc(ticks, 900));

        transport.Lineage = Guid.NewGuid();
        feeds[0].Append(LocalSet(tree, "a", Hlc(ticks, 10)));
        feeds[1].Append(LocalSet(tree, "b", Hlc(ticks, 11)));
        await PumpAsync(shipper, ticks: 2);
        var onTheLog = await gate.GetReapCeilingAsync(tree);

        // The peer falls off the log.
        transport.Lineage = Guid.NewGuid();
        feeds[1].Append(LocalSet(tree, "c", Hlc(ticks, 61)));
        await PumpAsync(shipper, ticks: 2);
        var offTheLog = await gate.GetReapCeilingAsync(tree);

        transport.Echo = 1;
        feeds[1].Append(LocalSet(tree, "d", Hlc(ticks, 63)));
        await PumpAsync(shipper, ticks: 6);
        feeds[0].Append(LocalSet(tree, "e", Hlc(ticks, 64)));
        await PumpAsync(shipper, ticks: 1);
        var reSeeded = await gate.GetReapCeilingAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(onTheLog, Is.EqualTo(floor), "precondition: the peer's acknowledged watermark bounds the ceiling");
            Assert.That(offTheLog, Is.EqualTo(HybridLogicalClock.Zero),
                "a tombstone is not reaped while a peer that may lack its delete is off the log");
            Assert.That(reSeeded!.Value.CompareTo(deleteStamp), Is.GreaterThan(0),
                "and is reaped once the peer re-seeded and acknowledged past it");
        });
    }

    [Test]
    public async Task The_reap_gate_fails_closed_on_every_origin_it_cannot_vouch_for()
    {
        const string tree = FrontierReceiver.Tree + "-reap-closed";
        var ticks = DateTime.UtcNow.Ticks;
        var receiver = new FrontierReceiver(tree, Guid.NewGuid());
        receiver.Factory.GetGrain<IReplicationTreeFrontierGrain>(tree, Arg.Any<string?>()).Returns(receiver.Frontier);
        var peerless = ReapGate(receiver.Factory, tree, TwoSiteClusterFixture.SiteBClusterId);
        var nothingInFlight = await peerless.GetReapCeilingAsync(tree);

        // A configured peer that never pushed may still have writes in flight.
        var shipper = Substitute.For<IReplicationShipperGrain>();
        shipper.GetReapLowWatermarkAsync().Returns(Hlc(ticks, 900));
        receiver.Factory.GetGrain<IReplicationShipperGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(shipper);
        var withPeer = ReapGate(receiver.Factory, tree, TwoSiteClusterFixture.SiteBClusterId, TwoSiteClusterFixture.SiteAClusterId);
        var silentPeer = await withPeer.GetReapCeilingAsync(tree);

        // An origin that pushed but is not a configured peer counts too.
        await receiver.Frontier.ObserveAsync("trg-unlisted", null, CancellationToken.None);
        await CoverAsync(receiver.Frontier, TwoSiteClusterFixture.SiteAClusterId, Hlc(ticks, 90));
        var unlistedPending = await withPeer.GetReapCeilingAsync(tree);
        await CoverAsync(receiver.Frontier, "trg-unlisted", Hlc(ticks, 80));
        var allCovered = await withPeer.GetReapCeilingAsync(tree);

        var membership = Substitute.For<IReplicatedTreeMembership>();
        var notReplicated = await new ReplicationTombstoneReapGate(
            receiver.Factory, membership, Substitute.For<IReplicationTopology>(),
            Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>()).GetReapCeilingAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(nothingInFlight, Is.Null, "a replicated tree with no peer and no origin has nothing in flight");
            Assert.That(silentPeer, Is.EqualTo(HybridLogicalClock.Zero), "a configured peer with no watermark reaps nothing");
            Assert.That(unlistedPending, Is.EqualTo(HybridLogicalClock.Zero), "an origin that pushed and is pending reaps nothing");
            Assert.That(allCovered, Is.EqualTo(Hlc(ticks, 80)), "the ceiling is the lowest bound over every origin and peer");
            Assert.That(notReplicated, Is.Null, "a tree replication is not enabled for is ungated");
        });
    }
}
