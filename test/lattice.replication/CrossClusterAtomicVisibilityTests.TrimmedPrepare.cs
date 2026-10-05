using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Lattice.Replication.Tests.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4534: a <c>WalRetention</c> trim past a lagging shipper's unacked
/// cursor loses records the peer never received. When the lost record is one
/// of a saga's prepares and its terminal is still retained, the shipper must
/// not deliver the terminal: the receiver would commit the saga and drain the
/// other keys while the trimmed key has no bucket. Runs the real shipper
/// against site B's real receiver.
/// </summary>
public partial class CrossClusterAtomicVisibilityTests
{
    [Test]
    public async Task Saga_whose_prepare_was_trimmed_unshipped_is_never_delivered_torn()
    {
        const string tree = "ccv-shipper-trimmed-prepare";
        var (keyA, keyB) = TwoKeysOnDistinctShards();
        var lattice = _fixture.SiteB.Client.GetGrain<ILattice>(tree);
        var txid = Guid.NewGuid();
        var ticks = DateTime.UtcNow.Ticks;

        var walEncoder = new ReplicationShipperGrainTests.StubWalRecordEncoder();
        var feeds = new[]
        {
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
        };
        // keyB's prepare is the first record of partition 0, followed by an
        // unrelated write; the TTL ceiling trims the prepare before the
        // shipper ever reads it.
        feeds[0].Append(PreparedSet(tree, keyB, 2, txid, Hlc(ticks, 2), index: 1));
        feeds[0].Append(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = "ccv-trimmed-unrelated",
            Value = new byte[] { 9 },
            Timestamp = Hlc(ticks, 5),
            OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        });
        feeds[0].TrimmedThrough = 1;
        feeds[1].Append(PreparedSet(tree, keyA, 1, txid, Hlc(ticks, 1), index: 0));
        feeds[1].Append(CommitTerminal(tree, txid, ShardOf(keyA), Hlc(ticks, 3), atomicShardCount: 2));
        feeds[1].Append(CommitTerminal(tree, txid, ShardOf(keyB), Hlc(ticks, 4), atomicShardCount: 2));

        var transport = Substitute.For<IReplicationTransport>();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(async call =>
            {
                var batch = call.Arg<ReplicationBatch>();
                var segments = batch.EncodedEnvelope?.EncodedEntries.ToArray() ?? [];
                var records = segments.Select(s => walEncoder.Decode(s.AsSpan())).ToList();
                if (records.Count > 0)
                {
                    await SiteBApplier.ApplyBatchAsync(records);
                }

                return new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero };
            });

        var shipper = CreateShipper(tree, feeds, walEncoder, transport);
        for (var tick = 0; tick < 4; tick++)
        {
            shipper.ResetBackoffForTesting();
            await shipper.PumpForTestingAsync(CancellationToken.None);
        }

        var finalA = await lattice.GetAsync(keyA);
        var finalB = await lattice.GetAsync(keyB);
        Assert.Multiple(async () =>
        {
            Assert.That(finalA is not null && finalB is null, Is.False,
                "keyA is visible while keyB is missing: the saga was delivered torn after its prepare was trimmed unshipped");
            Assert.That(await lattice.GetAsync("ccv-trimmed-unrelated"), Is.EqualTo(new byte[] { 9 }),
                "plain writes past the gap must keep shipping");
        });
    }

    [Test]
    public async Task Shipper_asks_a_peer_it_took_off_the_log_to_reseed_and_resumes_once_it_has()
    {
        const string tree = "ccv-shipper-reseed";
        var txid = Guid.NewGuid();
        var ticks = DateTime.UtcNow.Ticks;
        var walEncoder = new ReplicationShipperGrainTests.StubWalRecordEncoder();
        var feeds = new[]
        {
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
        };
        feeds[0].Append(PreparedSet(tree, "trimmed", 2, txid, Hlc(ticks, 2), index: 1));
        feeds[0].Append(PreparedSet(tree, "retained", 1, txid, Hlc(ticks, 1), index: 0));
        feeds[0].TrimmedThrough = 1;

        long? echo = null;
        var requested = new List<long?>();
        var shippedSagaRecords = 0;
        var transport = Substitute.For<IReplicationTransport>();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var batch = call.Arg<ReplicationBatch>();
                requested.Add(batch.ReseedAfterEpoch);
                var segments = batch.EncodedEnvelope?.EncodedEntries.ToArray() ?? [];
                shippedSagaRecords += segments.Select(s => walEncoder.Decode(s.AsSpan())).Count(r => r.IsPrepared);
                return Task.FromResult(new ReplicationAck
                {
                    Accepted = true,
                    HighestAppliedHlc = HybridLogicalClock.Zero,
                    BootstrapEpoch = echo,
                });
            });

        var stats = new ReplicationPeerStats();

        // The saga is still in flight: its participants are registered, so the
        // re-seed's replay filter ships it (#4533).
        var registry = ReplayRegistry(Guid.NewGuid(), Guid.NewGuid(), txid);
        var shipper = CreateShipper(tree, feeds, walEncoder, transport, stats, configureFactory: factory =>
            factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(registry));
        await PumpAsync(shipper, ticks: 2);

        Assert.Multiple(() =>
        {
            Assert.That(shipper.ReseedRequired, Is.True, "a trimmed gap must take the peer off the log");
            Assert.That(ReseedSeconds(stats, tree), Is.Not.Null, "the peer-status row must show the outstanding re-seed");
            Assert.That(shippedSagaRecords, Is.Zero, "no saga record may reach a peer that lost records");
        });

        // A liveness probe or the next push carries the request.
        feeds[1].Append(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = "plain",
            Value = new byte[] { 3 },
            Timestamp = Hlc(ticks, 9),
            OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        });
        await PumpAsync(shipper, ticks: 1);
        Assert.That(requested, Has.Some.EqualTo(0L), "pushes must ask the peer to re-seed past the recorded epoch");

        // The peer completed a bootstrap from a later export.
        echo = 1;
        feeds[1].Append(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = "plain-2",
            Value = new byte[] { 4 },
            Timestamp = Hlc(ticks, 10),
            OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        });
        await PumpAsync(shipper, ticks: 3);

        Assert.Multiple(() =>
        {
            Assert.That(shipper.ReseedRequired, Is.False, "an echoed later epoch clears the marker");
            Assert.That(ReseedSeconds(stats, tree), Is.Null, "the peer-status row must clear with the marker");
            Assert.That(shippedSagaRecords, Is.EqualTo(1),
                "after the re-seed the shipper re-ships every retained saga record from the lowest retained entry");
        });
    }

    [Test]
    public async Task Bootstrap_whose_export_opened_before_the_reseed_epoch_does_not_clear_the_marker()
    {
        const string tree = "ccv-shipper-stale-reseed-echo";
        var txid = Guid.NewGuid();
        var ticks = DateTime.UtcNow.Ticks;
        var walEncoder = new ReplicationShipperGrainTests.StubWalRecordEncoder();
        var feeds = new[]
        {
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
        };
        feeds[0].Append(PreparedSet(tree, "trimmed", 2, txid, Hlc(ticks, 2), index: 1));
        feeds[0].Append(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = "retained-plain",
            Value = new byte[] { 4 },
            Timestamp = Hlc(ticks, 3),
            OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        });
        feeds[0].TrimmedThrough = 1;

        var requested = new List<long?>();
        var transport = Substitute.For<IReplicationTransport>();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var batch = call.Arg<ReplicationBatch>();
                requested.Add(batch.ReseedAfterEpoch);
                return Task.FromResult(new ReplicationAck
                {
                    Accepted = true,
                    HighestAppliedHlc = HybridLogicalClock.Zero,
                    BootstrapEpoch = batch.ReseedAfterEpoch,
                });
            });

        var shipper = CreateShipper(tree, feeds, walEncoder, transport);
        await PumpAsync(shipper, ticks: 3);

        Assert.Multiple(() =>
        {
            Assert.That(requested, Has.Some.EqualTo(0L),
                "the stale completed bootstrap epoch is echoed back to the shipper");
            Assert.That(shipper.ReseedRequired, Is.True,
                "an export epoch equal to the marker opened before the gap was recorded and must not clear it");
        });
    }

    [TestCase(true, false, TestName = "Shipper_does_not_take_a_peer_off_the_log_for_a_hole_directly_above_the_trim_watermark")]
    [TestCase(false, true, TestName = "Shipper_without_a_trusted_trim_watermark_treats_a_jump_as_a_trim")]
    public async Task Shipper_tells_a_hole_above_the_trim_point_from_a_trim(bool reportsWatermark, bool reseedExpected)
    {
        // Issue #4621: offsets are not dense. A flush abandoned at its deadline
        // that never lands leaves a permanent hole the allocator has moved past.
        // Here offset 0 was trimmed and offset 1 is a hole, so the lowest stored
        // offset (2) sits above the shipper's cursor (1) although nothing it needs
        // was trimmed. Only the trim watermark (0) tells the two apart; without a
        // trusted one the shipper must fail closed and treat the jump as a trim.
        var tree = "ccv-shipper-hole-above-trim-" + reportsWatermark;
        var ticks = DateTime.UtcNow.Ticks;
        var walEncoder = new ReplicationShipperGrainTests.StubWalRecordEncoder();
        var feeds = new[]
        {
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
        };
        for (var i = 0; i < 4; i++)
        {
            feeds[0].Append(new WalRecord
            {
                TreeId = tree,
                Op = MutationKind.Set,
                Key = $"hole-{i}",
                Value = new byte[] { (byte)i },
                Timestamp = Hlc(ticks, i + 1),
                OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
            });
        }

        feeds[0].TrimmedThrough = 1;
        feeds[0].Holes.Add(1);
        feeds[0].ReportsTrimWatermark = reportsWatermark;

        var shipped = new List<string>();
        var transport = Substitute.For<IReplicationTransport>();
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var segments = call.Arg<ReplicationBatch>().EncodedEnvelope?.EncodedEntries.ToArray() ?? [];
                shipped.AddRange(segments.Select(s => walEncoder.Decode(s.AsSpan()).Key));
                return Task.FromResult(new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });
            });

        // The shipper already delivered offset 0 before it was trimmed.
        var state = new FakePersistentState<ReplicationShipperState>();
        state.State.PartitionCursors[0] = 1;
        var shipper = CreateShipper(tree, feeds, walEncoder, transport, state: state);
        await PumpAsync(shipper, ticks: 3);

        Assert.Multiple(() =>
        {
            Assert.That(shipper.ReseedRequired, Is.EqualTo(reseedExpected),
                reportsWatermark
                    ? "a hole above the trim watermark is not a trim"
                    : "without a trusted trim watermark every jump must be treated as a trim");
            Assert.That(shipped, Is.SupersetOf(new[] { "hole-2", "hole-3" }), "every retained plain record ships");
        });
    }
    private static double? ReseedSeconds(ReplicationPeerStats stats, string tree) =>
        stats.ReadStatusPage(new ReplicationPeerStatusReadRequest { TreeId = tree, Limit = 10 })
            .Single(r => r.Direction == ReplicationContactDirection.Outbound)
            .ReseedRequiredSeconds;

    private static async Task PumpAsync(Orleans.Lattice.Replication.Grains.ReplicationShipperGrain shipper, int ticks)
    {
        for (var tick = 0; tick < ticks; tick++)
        {
            shipper.ResetBackoffForTesting();
            await shipper.PumpForTestingAsync(CancellationToken.None);
        }
    }
}
