using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Regression coverage for the silent cross-cluster data-loss bug where the
/// outbound shipper drops genuinely-new entries on a COLD WAL partition (one
/// that has never shipped/acked) whenever the entry's per-leaf source HLC is
/// at or below the global scalar ship cursor. HLCs are stamped per leaf and
/// are non-monotonic across partitions, so a brand-new write on a cold
/// partition can legitimately carry an HLC below the max-shipped frontier.
/// The legacy-migration scalar-cursor drop must NOT fire for a cold partition
/// once the durable state proves the build is past the one-time migration
/// (any partition cursor saved => modern build).
/// </summary>
public partial class ReplicationShipperGrainTests
{
    [Test]
    public async Task DrainBatchAsync_cold_partition_new_below_cursor_entry_is_shipped_not_dropped()
    {
        var partitions = 2;
        var resolved = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = partitions,
            ShipBatchSize = 16,
        };
        var ctx = Substitute.For<IGrainContext>();
        ctx.GrainId.Returns(GrainId.Create("shipper", $"{Tree}/{Peer}"));
        var monitor = Monitor(resolved);
        var walEncoder = new StubWalRecordEncoder();
        var feeds = new[]
        {
            new StubReplogShardGrain(walEncoder),
            new StubReplogShardGrain(walEncoder),
        };

        // Partition 0 is WARM: it already has a saved partition cursor at
        // sequence 1, which makes the durable PartitionCursors map non-empty
        // and therefore proves the build is past the one-time legacy
        // migration. It resumes from sequence 1 and ships a fresh entry at
        // HLC 200 (above the scalar cursor).
        feeds[0].Append(MakeEntry("p0/already-shipped", ticks: 90));
        feeds[0].Append(MakeEntry("p0/new-above-cursor", ticks: 200));

        // Partition 1 is COLD: it has never shipped, so it has no entry in
        // PartitionCursors. Its single brand-new entry carries a per-leaf HLC
        // of 50 - genuinely new, but BELOW the global scalar cursor (100).
        feeds[1].Append(MakeEntry("p1/cold-below-cursor", ticks: 50));

        var transport = Substitute.For<IReplicationTransport>();
        var captured = CaptureMergeOrder(transport, walEncoder, ackHlc: 200);
        var encoder = new TestEncoder();
        var registry = Substitute.For<IWalCursorRegistry>();

        // Durable state from a MODERN build that is well past the legacy
        // migration: global scalar Cursor at HLC 100, and partition 0 already
        // has a saved cursor.
        var fakeState = new FakePersistentState<ReplicationShipperState>();
        var seed = new ReplicationShipperState
        {
            Cursor = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 },
        };
        seed.PartitionCursors[0] = 1;
        fakeState.State = seed;

        var factory = BuildGrainFactory(null, feeds, Tree);
        var grain = new ReplicationShipperGrain(
            ctx, Substitute.For<IReminderRegistry>(),
            NullLogger<ReplicationShipperGrain>.Instance,
            monitor, transport, encoder, walEncoder, registry, factory, fakeState,
            new ReplicationPeerStats(),
            Substitute.For<ILatticeMergeModeResolver>(),
            new WireVersionNegotiationState(), new NoOpReplicationDigestProbeTransport());
        grain.InitializeForTesting(Tree, Peer);

        await grain.PumpForTestingAsync(CancellationToken.None);

        // The cold partition's brand-new entry must be shipped. Today it is
        // silently dropped by the legacy scalar-cursor filter (which fires
        // for any partition without a saved cursor, not just a true legacy
        // migration), so the receiver never learns about it: permanent,
        // silent cross-cluster data loss with no fall-off and no dead-letter.
        var shippedKeys = captured.Select(e => e.Key).ToList();
        Assert.That(shippedKeys, Contains.Item("p1/cold-below-cursor"),
            "A genuinely-new entry on a cold partition whose per-leaf HLC is below the global scalar ship cursor must be shipped, not dropped by the legacy-migration filter.");
    }
}
