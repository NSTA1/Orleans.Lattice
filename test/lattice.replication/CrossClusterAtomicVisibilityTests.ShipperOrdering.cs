using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Lattice.Replication.Tests.Grains;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Cross-cluster atomic visibility end to end through the shipper (issue #4480):
/// the real <see cref="ReplicationShipperGrain"/> drains a two-partition origin
/// WAL and its batches are applied by site B's real receiver
/// (<see cref="IReplicationApplier"/>). The origin log is shaped the way skewed
/// leaf clocks shape it: a higher-HLC entry was appended to partition 0 ahead of
/// keyB's prepare, so a merge by HLC alone ships both shard terminals before
/// that prepare, the receiver commits the saga with keyB's bucket missing, and
/// the late prepare is refused - keyA visible, keyB never.
/// </summary>
public partial class CrossClusterAtomicVisibilityTests
{
    [Test]
    public async Task Saga_shipped_from_a_skewed_multi_partition_log_is_visible_whole_on_the_receiver()
    {
        const string tree = "ccv-shipper-ordering";
        var (keyA, keyB) = TwoKeysOnDistinctShards();
        var applier = SiteBApplier;
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var txid = Guid.NewGuid();
        var ticks = DateTime.UtcNow.Ticks;

        var walEncoder = new ReplicationShipperGrainTests.StubWalRecordEncoder();
        var feeds = new[]
        {
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
        };
        feeds[0].Append(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = "ccv-shipper-unrelated",
            Value = new byte[] { 9 },
            Timestamp = Hlc(ticks + TimeSpan.TicksPerSecond),
            OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        });
        feeds[1].Append(PreparedSet(tree, keyA, 1, txid, Hlc(ticks, 1), index: 0));
        feeds[0].Append(PreparedSet(tree, keyB, 2, txid, Hlc(ticks, 2), index: 1));
        feeds[1].Append(CommitTerminal(tree, txid, ShardOf(keyA), Hlc(ticks, 3), atomicShardCount: 2));
        feeds[1].Append(CommitTerminal(tree, txid, ShardOf(keyB), Hlc(ticks, 4), atomicShardCount: 2));

        var transport = Substitute.For<IReplicationTransport>();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(async call =>
            {
                var batch = call.Arg<ReplicationBatch>();
                var segments = batch.EncodedEnvelope!.Value.EncodedEntries.ToArray();
                var records = segments.Select(s => walEncoder.Decode(s.AsSpan())).ToList();
                await applier.ApplyBatchAsync(records);
                return new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero };
            });

        var shipper = CreateShipper(tree, feeds, walEncoder, transport);
        await shipper.PumpForTestingAsync(CancellationToken.None);

        var finalA = await lattice.GetAsync(keyA);
        var finalB = await lattice.GetAsync(keyB);
        Assert.Multiple(() =>
        {
            Assert.That(finalA, Is.EqualTo(new byte[] { 1 }), "keyA must be visible once the saga committed");
            Assert.That(finalB, Is.EqualTo(new byte[] { 2 }),
                "keyB must be visible beside keyA: a terminal that overtook keyB's prepare commits the saga without it");
        });
    }

    private static ReplicationShipperGrain CreateShipper(
        string tree,
        ReplicationShipperGrainTests.StubReplogShardGrain[] feeds,
        ReplicationShipperGrainTests.StubWalRecordEncoder walEncoder,
        IReplicationTransport transport,
        ReplicationPeerStats? peerStats = null)
    {
        var options = new LatticeReplicationOptions
        {
            ClusterId = TwoSiteClusterFixture.SiteAClusterId,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = feeds.Length,
            ShipBatchSize = 16,
            WireVersionNegotiationEnabled = false,
        };
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);

        var factory = Substitute.For<IGrainFactory>();
        for (var p = 0; p < feeds.Length; p++)
        {
            factory.GetGrain<IWalShardGrain>($"{tree}/{p}").Returns(feeds[p]);
        }

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shipper", $"{tree}/{TwoSiteClusterFixture.SiteBClusterId}"));
        var shipper = new ReplicationShipperGrain(
            context, Substitute.For<IReminderRegistry>(), NullLogger<ReplicationShipperGrain>.Instance,
            monitor, transport, Substitute.For<IReplicationBatchEncoder>(), walEncoder,
            Substitute.For<IWalCursorRegistry>(), factory, new FakePersistentState<ReplicationShipperState>(),
            peerStats ?? new ReplicationPeerStats(), Substitute.For<ILatticeMergeModeResolver>(),
            new WireVersionNegotiationState(), new NoOpReplicationDigestProbeTransport());
        shipper.InitializeForTesting(tree, TwoSiteClusterFixture.SiteBClusterId);
        return shipper;
    }
}
