using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Sender-side coverage for receiver-driven flow control: the shipper
/// must respect the <see cref="ReplicationAck.SuggestedBatchSize"/>
/// hint by clamping the per-tick batch cap, and the
/// <see cref="ReplicationAck.PauseForMs"/> hint by extending the
/// retry deadline so the next pump tick is gated. A struggling
/// receiver therefore throttles the producer without timing out the
/// shipper, and a recovered receiver re-accelerates simply by
/// returning a null (or higher) hint on its next ack.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    /// <summary>
    /// A receiver-stamped <see cref="ReplicationAck.SuggestedBatchSize"/>
    /// hint below the configured <see cref="LatticeReplicationOptions.ShipBatchSize"/>
    /// must throttle the next pump tick to the hinted ceiling. Tick 1
    /// drains the single available entry, and the receiver returns a
    /// hint of <c>2</c>. Tick 2 has three new entries to choose from;
    /// the shipper must drain exactly two.
    /// </summary>
    [Test]
    public async Task PumpOnceAsync_clamps_drain_to_receiver_suggested_batch_size()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ShipBatchSize = 10,
        };
        var (grain, _, feed, transport, encoder, _, _) = Create(opts);
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = HybridLogicalClock.Zero,
                SuggestedBatchSize = 2,
            });

        // Tick 1: one entry, ships it, picks up the hint of 2.
        feed.Append(MakeEntry("k1", ticks: 1));
        await grain.OnDoorbellAsync(CancellationToken.None);
        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(1),
            "tick 1 is unhinted; the shipper drains everything available");

        // Tick 2: three new entries available. The previous ack's
        // hint of 2 must clamp this tick's drain.
        feed.Append(MakeEntry("k2", ticks: 2));
        feed.Append(MakeEntry("k3", ticks: 3));
        feed.Append(MakeEntry("k4", ticks: 4));
        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(2),
            "tick 2 must respect the receiver's suggested cap");
    }

    /// <summary>
    /// A hint above the configured <see cref="LatticeReplicationOptions.ShipBatchSize"/>
    /// is clamped down to the configured ceiling: the receiver cannot
    /// push the producer past its own configured wire budget.
    /// </summary>
    [Test]
    public async Task PumpOnceAsync_clamps_suggested_batch_size_to_configured_ceiling()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ShipBatchSize = 2,
        };
        var (grain, _, feed, transport, encoder, _, _) = Create(opts);
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = HybridLogicalClock.Zero,
                SuggestedBatchSize = 100,
            });

        feed.Append(MakeEntry("k1", ticks: 1));
        await grain.OnDoorbellAsync(CancellationToken.None);

        feed.Append(MakeEntry("k2", ticks: 2));
        feed.Append(MakeEntry("k3", ticks: 3));
        feed.Append(MakeEntry("k4", ticks: 4));
        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(opts.ShipBatchSize),
            "an oversize hint must never exceed the configured ShipBatchSize");
    }

    /// <summary>
    /// A non-positive hint is treated as the canonical re-acceleration
    /// signal: the cap reverts to the configured ceiling. The malformed
    /// hint must not pin the cap at 0 (which would stall the stream)
    /// or at a negative value (which would underflow the drain
    /// budget). Zero specifically is treated as "drop the hint".
    /// </summary>
    [Test]
    public async Task PumpOnceAsync_ignores_zero_suggested_batch_size_hint()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ShipBatchSize = 3,
        };
        var (grain, _, feed, transport, encoder, _, _) = Create(opts);
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = HybridLogicalClock.Zero,
                SuggestedBatchSize = 0,
            });

        feed.Append(MakeEntry("k1", ticks: 1));
        await grain.OnDoorbellAsync(CancellationToken.None);

        feed.Append(MakeEntry("k2", ticks: 2));
        feed.Append(MakeEntry("k3", ticks: 3));
        feed.Append(MakeEntry("k4", ticks: 4));
        feed.Append(MakeEntry("k5", ticks: 5));
        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(opts.ShipBatchSize),
            "a zero hint must collapse back to the configured ShipBatchSize");
    }

    /// <summary>
    /// After the receiver throttles via a hint, returning a null hint
    /// on the next ack must re-accelerate the producer: the next pump
    /// tick once again drains up to the configured
    /// <see cref="LatticeReplicationOptions.ShipBatchSize"/>. This is
    /// the recovery half of the throttle / re-accelerate handshake.
    /// </summary>
    [Test]
    public async Task PumpOnceAsync_reaccelerates_when_receiver_clears_hint()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ShipBatchSize = 5,
        };
        var (grain, _, feed, transport, encoder, _, _) = Create(opts);

        // Tick 1: under load; hint clamps next tick to 1.
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = HybridLogicalClock.Zero,
                SuggestedBatchSize = 1,
            });
        feed.Append(MakeEntry("k1", ticks: 1));
        await grain.OnDoorbellAsync(CancellationToken.None);

        // Tick 2: hint still applies (clamped to 1).
        feed.Append(MakeEntry("k2", ticks: 2));
        feed.Append(MakeEntry("k3", ticks: 3));
        await grain.OnDoorbellAsync(CancellationToken.None);
        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(1),
            "throttle is active");

        // Tick 3: receiver recovered; null hint clears the clamp.
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = HybridLogicalClock.Zero,
                SuggestedBatchSize = null,
            });
        await grain.OnDoorbellAsync(CancellationToken.None);

        // Tick 4: many new entries; cap reverts to ShipBatchSize.
        feed.Append(MakeEntry("k4", ticks: 4));
        feed.Append(MakeEntry("k5", ticks: 5));
        feed.Append(MakeEntry("k6", ticks: 6));
        feed.Append(MakeEntry("k7", ticks: 7));
        feed.Append(MakeEntry("k8", ticks: 8));
        feed.Append(MakeEntry("k9", ticks: 9));
        feed.Append(MakeEntry("k10", ticks: 10));
        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(LastShippedEntryCount(transport), Is.EqualTo(opts.ShipBatchSize),
            "recovered receiver returning null hint must restore full ShipBatchSize");
    }

    /// <summary>
    /// A <see cref="ReplicationAck.PauseForMs"/> hint on a successful
    /// ack must gate the next pump tick: a subsequent doorbell inside
    /// the pause window must not invoke the transport again. The
    /// receiver therefore throttles a struggling sender without ever
    /// having to reject a batch.
    /// </summary>
    [Test]
    public async Task PumpOnceAsync_pauses_next_tick_when_receiver_requests_pause()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = HybridLogicalClock.Zero,
                PauseForMs = 60_000, // 1 minute - safely past test wall-clock
            });

        feed.Append(MakeEntry("k1", ticks: 1));
        await grain.OnDoorbellAsync(CancellationToken.None);
        await transport.Received(1).SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());

        // Second doorbell inside the pause window: pump must short-circuit.
        feed.Append(MakeEntry("k2", ticks: 2));
        await grain.OnDoorbellAsync(CancellationToken.None);
        await transport.Received(1).SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// A null or non-positive <see cref="ReplicationAck.PauseForMs"/>
    /// hint must not gate the next pump tick: the steady-state happy
    /// path remains pause-free. This guards against a regression where
    /// the success path inadvertently parks the retry deadline on
    /// every ack.
    /// </summary>
    [Test]
    public async Task PumpOnceAsync_does_not_pause_when_receiver_omits_pause_hint()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = HybridLogicalClock.Zero,
                PauseForMs = null,
            });

        feed.Append(MakeEntry("k1", ticks: 1));
        await grain.OnDoorbellAsync(CancellationToken.None);

        feed.Append(MakeEntry("k2", ticks: 2));
        await grain.OnDoorbellAsync(CancellationToken.None);

        await transport.Received(2).SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    /// <summary>
    /// A zero <see cref="ReplicationAck.PauseForMs"/> hint is treated
    /// the same as the missing-hint case: no pause is applied. This
    /// matches the docstring contract that the pause hint is opt-in
    /// and that <c>0</c> means "no pause".
    /// </summary>
    [Test]
    public async Task PumpOnceAsync_ignores_zero_pause_hint()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = HybridLogicalClock.Zero,
                PauseForMs = 0,
            });

        feed.Append(MakeEntry("k1", ticks: 1));
        await grain.OnDoorbellAsync(CancellationToken.None);

        feed.Append(MakeEntry("k2", ticks: 2));
        await grain.OnDoorbellAsync(CancellationToken.None);

        await transport.Received(2).SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }

    // --- Bounded multi-batch pipelining (ShipMaxInFlight > 1) ---------

    private static int SendCallCount(IReplicationTransport transport) =>
        transport.ReceivedCalls()
            .Count(c => c.GetMethodInfo().Name == nameof(IReplicationTransport.SendAsync));

    private static int TotalShippedEntries(IReplicationTransport transport) =>
        transport.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IReplicationTransport.SendAsync))
            .Select(c => (ReplicationBatch)c.GetArguments()[0]!)
            .Sum(b => b.EncodedEnvelope!.Value.EncodedEntries.Length);

    /// <summary>
    /// With a window above one the shipper drains and ships several
    /// batches inside a single pump tick rather than one batch per
    /// ack round-trip. Six entries at a per-batch cap of two yield
    /// three batches, all launched within the bounded window.
    /// </summary>
    [Test]
    public async Task PumpOnceAsync_pipelines_multiple_batches_when_window_above_one()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ShipBatchSize = 2,
            ShipMaxInFlight = 3,
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        for (var i = 1; i <= 6; i++)
        {
            feed.Append(MakeEntry($"k{i}", ticks: i));
        }

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(SendCallCount(transport), Is.EqualTo(3),
                "the pipeline must carve six entries into three batches of two");
            Assert.That(TotalShippedEntries(transport), Is.EqualTo(6),
                "every drained entry must be shipped exactly once");
        });
    }

    /// <summary>
    /// FIFO ack accounting advances the durable cursor strictly to the
    /// highest shipped HLC once every in-flight batch has acked, with
    /// no hole left behind a lower-HLC batch.
    /// </summary>
    [Test]
    public async Task PumpOnceAsync_advances_cursor_to_highest_hlc_after_pipelined_tick()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ShipBatchSize = 2,
            ShipMaxInFlight = 3,
        };
        var (grain, state, feed, _, _, _, _) = Create(opts);
        for (var i = 1; i <= 6; i++)
        {
            feed.Append(MakeEntry($"k{i}", ticks: i));
        }

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(state.State.Cursor,
            Is.EqualTo(new HybridLogicalClock { WallClockTicks = 6, Counter = 0 }));
    }

    /// <summary>
    /// A receiver-supplied <see cref="ReplicationAck.SuggestedBatchSize"/>
    /// hint collapses the pipeline window back to one: the next tick
    /// ships a single batch even though a wider window is configured,
    /// composing the depth throttle with the batch-size throttle.
    /// </summary>
    [Test]
    public async Task PumpOnceAsync_collapses_window_to_one_when_receiver_suggests_batch_size()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ShipBatchSize = 2,
            ShipMaxInFlight = 3,
        };
        var (grain, _, feed, transport, _, _, _) = Create(opts);
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = HybridLogicalClock.Zero,
                SuggestedBatchSize = 1,
            });

        // Tick 1: two entries -> one full batch; receiver asks to slow down.
        feed.Append(MakeEntry("k1", ticks: 1));
        feed.Append(MakeEntry("k2", ticks: 2));
        await grain.OnDoorbellAsync(CancellationToken.None);
        var afterTick1 = SendCallCount(transport);

        // Tick 2: six new entries. With the hint active the window
        // collapses to one and the cap clamps to one, so the tick ships
        // exactly one batch of one entry.
        for (var i = 3; i <= 8; i++)
        {
            feed.Append(MakeEntry($"k{i}", ticks: i));
        }
        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(SendCallCount(transport) - afterTick1, Is.EqualTo(1),
                "the collapsed window must ship exactly one batch in tick 2");
            Assert.That(LastShippedEntryCount(transport), Is.EqualTo(1),
                "the suggested-batch-size hint clamps the batch to one entry");
        });
    }

    /// <summary>
    /// When a batch in the middle of the window is rejected, the
    /// advance-strictly-on-ack rule keeps the durable cursor at the
    /// last positively-acked lower-HLC batch and never folds the
    /// cursor of the failed batch or any batch behind it.
    /// </summary>
    [Test]
    public async Task PumpOnceAsync_does_not_advance_cursor_past_unacked_batch_on_ack_rejection()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ShipBatchSize = 1,
            ShipMaxInFlight = 3,
        };
        var (grain, state, feed, transport, _, _, _) = Create(opts);
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(
                new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero },
                new ReplicationAck { Accepted = false },
                new ReplicationAck { Accepted = true, HighestAppliedHlc = HybridLogicalClock.Zero });
        feed.Append(MakeEntry("k1", ticks: 1));
        feed.Append(MakeEntry("k2", ticks: 2));
        feed.Append(MakeEntry("k3", ticks: 3));

        await grain.OnDoorbellAsync(CancellationToken.None);

        Assert.That(state.State.Cursor,
            Is.EqualTo(new HybridLogicalClock { WallClockTicks = 1, Counter = 0 }),
            "only the first batch acked positively, so the cursor stops at its HLC");
    }

    /// <summary>
    /// An idle pipelined tick mirrors the serial empty-drain contract:
    /// it anchors the liveness-probe timer and ships nothing on the
    /// first activation rather than emitting a spurious empty batch.
    /// </summary>
    [Test]
    public async Task PumpOnceAsync_ships_nothing_on_idle_pipelined_tick()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ShipMaxInFlight = 3,
        };
        var (grain, _, _, transport, _, _, _) = Create(opts);

        await grain.OnDoorbellAsync(CancellationToken.None);

        await transport.DidNotReceive().SendAsync(
            Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>());
    }
}

