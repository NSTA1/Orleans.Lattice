using NSubstitute;
using NSubstitute.Core;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Sender-side coverage for the strict-serial ship path's continuous drain:
/// a single pump tick must carve and ship every batch the WAL tail holds
/// back-to-back (one prime, many batches), instead of shipping a single
/// batch per phase-timer tick and leaving the remaining backlog to drain one
/// batch per tick. The receiver's flow-control hints still gate the loop at
/// batch granularity, so a struggling receiver throttles the producer exactly
/// as before.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    /// <summary>
    /// With a backlog larger than one batch and no receiver flow control, a
    /// single serial pump tick ships every batch back-to-back rather than one
    /// batch per tick. Six entries with a cap of two must produce three
    /// transport sends in one tick, covering all six entries in HLC order.
    /// </summary>
    [Test]
    public async Task PumpOnceAsync_serial_path_ships_all_backlog_batches_in_one_tick()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = 1,
            ShipBatchSize = 2,
            // Pin the effective cap at ShipBatchSize so the batch count is
            // deterministic (the AIMD controller would otherwise ramp the cap).
            AdaptiveBatchSizingEnabled = false,
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });

        for (var i = 1; i <= 6; i++)
        {
            feed.Append(MakeEntry($"k{i}", ticks: i));
        }

        await grain.PumpForTestingAsync(CancellationToken.None);

        var sends = SendCalls(transport);
        Assert.Multiple(() =>
        {
            Assert.That(sends.Count, Is.EqualTo(3),
                "the serial path must drain all three batches back-to-back in a single pump tick, not one batch per tick");
            Assert.That(sends.Sum(EntryCountOf), Is.EqualTo(6),
                "every backlog entry must ship within the single tick");
            // Each batch is capped at ShipBatchSize=2.
            Assert.That(sends.Select(EntryCountOf), Is.EqualTo(new[] { 2, 2, 2 }));
        });
    }

    /// <summary>
    /// A short final batch (the WAL tail comes up under the cap) still ships
    /// within the same tick, and the loop then stops. Five entries with a cap
    /// of two produce two full batches and one short batch of one - three
    /// sends in a single tick.
    /// </summary>
    [Test]
    public async Task PumpOnceAsync_serial_path_ships_short_tail_batch_in_same_tick()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = 1,
            ShipBatchSize = 2,
            AdaptiveBatchSizingEnabled = false,
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });

        for (var i = 1; i <= 5; i++)
        {
            feed.Append(MakeEntry($"k{i}", ticks: i));
        }

        await grain.PumpForTestingAsync(CancellationToken.None);

        var sends = SendCalls(transport);
        Assert.Multiple(() =>
        {
            Assert.That(sends.Select(EntryCountOf), Is.EqualTo(new[] { 2, 2, 1 }),
                "the short tail batch must ship in the same tick, then the drain loop stops");
            Assert.That(sends.Sum(EntryCountOf), Is.EqualTo(5));
        });
    }

    /// <summary>
    /// Receiver flow control still gates the drain loop at batch granularity:
    /// when the receiver stamps a strictly-positive
    /// <see cref="ReplicationAck.SuggestedBatchSize"/> hint on every ack, the
    /// serial path ships exactly one batch per tick even though more backlog
    /// is available, so the throttle handshake is unchanged by the continuous
    /// drain.
    /// </summary>
    [Test]
    public async Task PumpOnceAsync_serial_path_stops_draining_when_receiver_hints_shrink()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = 1,
            ShipBatchSize = 2,
            AdaptiveBatchSizingEnabled = false,
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = HybridLogicalClock.Zero,
                SuggestedBatchSize = 2,
            });

        for (var i = 1; i <= 6; i++)
        {
            feed.Append(MakeEntry($"k{i}", ticks: i));
        }

        await grain.PumpForTestingAsync(CancellationToken.None);

        var sends = SendCalls(transport);
        Assert.That(sends.Count, Is.EqualTo(1),
            "an active receiver shrink hint must stop the drain loop after one batch, deferring the rest to the next tick");
    }

    private static IReadOnlyList<ICall> SendCalls(IReplicationTransport transport) =>
        transport.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IReplicationTransport.SendAsync))
            .ToList();

    private static int EntryCountOf(ICall call)
    {
        var batch = (ReplicationBatch)call.GetArguments()[0]!;
        Assert.That(batch.EncodedEnvelope, Is.Not.Null,
            "the shipper must populate EncodedEnvelope on every batch on the framing-only path");
        return batch.EncodedEnvelope!.Value.EncodedEntries.Length;
    }
}
