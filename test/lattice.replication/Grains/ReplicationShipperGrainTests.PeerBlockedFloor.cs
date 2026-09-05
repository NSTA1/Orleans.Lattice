using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Regression coverage for the cross-cluster receiver blocked-floor pin.
/// The receiver stamps its lowest partially-buffered atomic-batch HLC on
/// every ack (<see cref="ReplicationAck.BlockedAtHlc"/>); the producer
/// republishes it into its own <see cref="IWalCursorRegistry"/> under a
/// per-peer consumer id so the local WAL GC AND-s a strict-less
/// <c>entry.Timestamp &lt; blockedFloor</c> clause into its trim
/// predicate and cannot trim past an entry the receiver still needs to
/// recover from buffer state.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    private const string PeerBlockedFloorConsumerId = "shipper:peer-blocked-floor:" + Peer;

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    /// <summary>
    /// Every blocked-floor report the shipper made against
    /// <paramref name="registry"/>, in call order, as the reported pin.
    /// Filters on the four-argument blocked-floor overload of
    /// <see cref="IWalCursorRegistry.ReportCursorAsync(string, string, HybridLogicalClock, HybridLogicalClock?, CancellationToken)"/>
    /// so plain cursor reports (the three-argument overload) are excluded.
    /// </summary>
    private static List<HybridLogicalClock?> PeerBlockedFloorReports(IWalCursorRegistry registry) =>
        registry.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IWalCursorRegistry.ReportCursorAsync))
            .Select(c => c.GetArguments())
            .Where(a => a.Length == 5 && a[1] as string == PeerBlockedFloorConsumerId)
            .Select(a => (HybridLogicalClock?)a[3])
            .ToList();

    [Test]
    public async Task PumpOnceAsync_publishes_the_receiver_blocked_floor_pin_from_the_ack()
    {
        var (grain, _, feed, transport, _, registry, _) = Create();
        var receiverPin = Hlc(4242, 7);
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = Hlc(5000),
                BlockedAtHlc = receiverPin,
            });

        feed.Append(MakeEntry("k1"));
        await grain.PumpForTestingAsync(CancellationToken.None);

        // The producer must republish the peer's pin under the per-peer
        // consumer id, with a Zero cursor so the registry's min(cursor)
        // branch is not double-counted (the per-peer cursor advance
        // already feeds that branch).
        await registry.Received(1).ReportCursorAsync(
            Tree,
            PeerBlockedFloorConsumerId,
            HybridLogicalClock.Zero,
            receiverPin,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task PumpOnceAsync_publishes_the_blocked_floor_pin_from_a_rejected_ack()
    {
        // A receiver that defers a batch behind its inbound receive
        // fence returns Accepted=false but still stamps its pin - and
        // that is precisely the window in which the producer must not
        // trim, so the rejected path has to publish too.
        var (grain, _, feed, transport, _, registry, _) = Create();
        var receiverPin = Hlc(900, 1);
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = false,
                HighestAppliedHlc = Hlc(800),
                BlockedAtHlc = receiverPin,
                PauseForMs = 25,
            });

        feed.Append(MakeEntry("k1"));
        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(PeerBlockedFloorReports(registry), Is.EqualTo(
            new HybridLogicalClock?[] { receiverPin }));
    }

    [Test]
    public async Task PumpOnceAsync_clears_the_blocked_floor_pin_when_the_receiver_buffer_drains()
    {
        // Replace semantics: the receiver is the authority on its own
        // pin, so a later ack carrying no pin must clear the previously
        // published one rather than leave the producer's WAL pinned
        // forever.
        var (grain, _, feed, transport, _, registry, _) = Create();
        var receiverPin = Hlc(1500);
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(
                new ReplicationAck
                {
                    Accepted = true,
                    HighestAppliedHlc = Hlc(10),
                    BlockedAtHlc = receiverPin,
                },
                new ReplicationAck
                {
                    Accepted = true,
                    HighestAppliedHlc = Hlc(20),
                    BlockedAtHlc = null,
                });

        feed.Append(MakeEntry("k1", ticks: 5));
        await grain.PumpForTestingAsync(CancellationToken.None);
        feed.Append(MakeEntry("k2", ticks: 15));
        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(PeerBlockedFloorReports(registry), Is.EqualTo(
            new HybridLogicalClock?[] { receiverPin, null }));
    }

    [Test]
    public async Task PumpOnceAsync_does_not_re_report_an_unchanged_blocked_floor_pin()
    {
        // The registry already enforces replace semantics, but each
        // report still costs a per-tree lock acquisition, so an
        // unchanged pin is suppressed.
        var (grain, _, feed, transport, _, registry, _) = Create();
        var receiverPin = Hlc(77);
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = Hlc(10),
                BlockedAtHlc = receiverPin,
            });

        feed.Append(MakeEntry("k1", ticks: 5));
        await grain.PumpForTestingAsync(CancellationToken.None);
        feed.Append(MakeEntry("k2", ticks: 15));
        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(PeerBlockedFloorReports(registry), Is.EqualTo(
            new HybridLogicalClock?[] { receiverPin }));
    }

    [Test]
    public async Task PumpOnceAsync_advances_the_cursor_when_the_blocked_floor_report_throws()
    {
        // A registry outage must not unwind the ship: the pin is
        // re-published on the next ack, and the cursor advance the ack
        // already earned still lands.
        var (grain, state, feed, transport, _, registry, _) = Create();
        var ackHlc = Hlc(64);
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = ackHlc,
                BlockedAtHlc = Hlc(32),
            });
        registry.ReportCursorAsync(
                Arg.Any<string>(),
                PeerBlockedFloorConsumerId,
                Arg.Any<HybridLogicalClock>(),
                Arg.Any<HybridLogicalClock?>(),
                Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("registry-down"));

        feed.Append(MakeEntry("k1", ticks: 5));
        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(state.State.Cursor, Is.EqualTo(ackHlc));
    }

    [Test]
    public async Task PumpPipelinedOnceAsync_publishes_the_receiver_blocked_floor_pin()
    {
        // The pipelined ship leg completes its acks in a separate
        // drain loop from the serial one, so it needs its own wiring.
        var opts = ElisionPipelinedOptions(shipMaxInFlight: 4, shipBatchSize: 2);
        var (grain, _, feed, transport, _, registry, _) = Create(opts);
        var receiverPin = Hlc(2024, 3);
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = Hlc(9000),
                BlockedAtHlc = receiverPin,
            });

        for (var i = 1; i <= 4; i++)
        {
            feed.Append(MakeEntry($"k{i}", ticks: i));
        }

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(PeerBlockedFloorReports(registry), Is.EqualTo(
            new HybridLogicalClock?[] { receiverPin }));
    }

    [Test]
    public async Task Liveness_probe_publishes_the_receiver_blocked_floor_pin()
    {
        // An idle link ships nothing, so the probe ack is the only
        // channel that can re-arm a newly-raised pin or clear a stale
        // one while the shipper has no work.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            LivenessProbeInterval = TimeSpan.FromMilliseconds(1),
        };
        var (grain, _, _, transport, _, registry, _) = Create(opts);
        var receiverPin = Hlc(31337);
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = HybridLogicalClock.Zero,
                BlockedAtHlc = receiverPin,
            });

        // First tick anchors the probe-interval timer; the second, past
        // the interval, fires the probe.
        await grain.PumpForTestingAsync(CancellationToken.None);
        await Task.Delay(50);
        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(PeerBlockedFloorReports(registry), Is.EqualTo(
            new HybridLogicalClock?[] { receiverPin }));
    }
}
