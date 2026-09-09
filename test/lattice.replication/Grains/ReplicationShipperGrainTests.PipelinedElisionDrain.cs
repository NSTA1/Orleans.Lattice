using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Coverage for three pipelined legs that the existing elision fixtures do not
/// exercise: the fully-elided batch's window-full inner drain observing an
/// earlier real batch's rejected ack (window of two, not four), the successful
/// ack's <c>PauseForMs</c> back-pressure clamp, and the deferred cursor persist
/// when the pending-write count has not yet reached
/// <c>ShipCursorWriteInterval</c>. Pauses are asserted deterministically with a
/// large pause value plus a follow-up gated pump - no sleeping.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    [Test]
    public async Task PumpPipelinedOnceAsync_elided_inner_drain_observes_earlier_real_batch_rejection()
    {
        // Window of two, one-entry batches: k1 (real, rejected ack) then k2
        // (fully elided). Enqueuing the elided k2 fills the window, so the
        // elided branch's inner DrainOneInFlightAsync drains the FIFO head
        // (real k1), sees the rejection, and fails the tick.
        var opts = ElisionPipelinedOptions(shipMaxInFlight: 2, shipBatchSize: 1);
        var fake = ManifestTransportHolding("k2");
        var (grain, state, feed, transport, _, _, _) = Create(opts, digestProbeTransport: fake);
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = false, HighestAppliedHlc = HybridLogicalClock.Zero });
        feed.Append(MakeEntryWithValue("k1", new byte[] { 1 }, ticks: 1));
        feed.Append(MakeEntryWithValue("k2", new byte[] { 2 }, ticks: 2));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(SendAsyncCallCount(transport), Is.EqualTo(1),
                "only the real k1 batch ships; the elided k2 never puts an envelope on the wire");
            Assert.That(state.State.ConsecutiveFailures, Is.GreaterThan(0),
                "the inner drain observing k1's rejected ack must back off");
            Assert.That(state.State.Cursor, Is.EqualTo(HybridLogicalClock.Zero),
                "a rejected head batch must block every later cursor advance in the window");
        });
    }

    [Test]
    public async Task PumpPipelinedOnceAsync_successful_ack_pause_for_ms_gates_the_next_pump()
    {
        // A successful ack carrying a large PauseForMs must clamp the next
        // retry time so a follow-up pump is gated and ships nothing further.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = 1,
            WireVersionNegotiationEnabled = false,
            ShipMaxInFlight = 2,
            ShipBatchSize = 1,
        };
        var entryHlc = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = entryHlc,
                PauseForMs = 600_000,
            });
        feed.Append(MakeEntry("k1", ticks: 1));

        await grain.PumpForTestingAsync(CancellationToken.None);
        // A second entry arrives, but the peer-requested pause must hold the
        // stream: the follow-up pump is gated and ships nothing.
        feed.Append(MakeEntry("k2", ticks: 2));
        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(SendAsyncCallCount(transport), Is.EqualTo(1),
            "the peer-requested pause must gate the follow-up pump so k2 does not ship");
    }

    [Test]
    public async Task PumpPipelinedOnceAsync_deferred_cursor_persist_below_write_interval_does_not_write_state()
    {
        // ShipCursorWriteInterval 2 with a single advancing batch: the cursor
        // moves in memory but the pending-write count (1) is below the
        // interval, so no durable WriteStateAsync happens this tick.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 2,
            ReplogPartitions = 1,
            WireVersionNegotiationEnabled = false,
            ShipMaxInFlight = 2,
            ShipBatchSize = 1,
        };
        var entryHlc = new HybridLogicalClock { WallClockTicks = 1, Counter = 0 };
        var (grain, state, feed, transport, _, _, _) = Create(opts);
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = true, HighestAppliedHlc = entryHlc });
        feed.Append(MakeEntry("k1", ticks: 1));

        var writesBefore = state.WriteCount;
        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Cursor, Is.EqualTo(entryHlc),
                "the cursor must advance in memory even when the write interval defers the persist");
            Assert.That(state.WriteCount, Is.EqualTo(writesBefore),
                "a pending-write count below ShipCursorWriteInterval must defer the durable persist");
        });
    }
}
