using System.IO;
using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Coverage for the pipelined ship path's failure legs (window &gt; 1):
/// an <c>InitializeDrainTickAsync</c> throw that backs off before the window
/// opens, a wire-version negotiation gate that returns without shipping, a
/// mid-tick <c>MergeOneBatchAsync</c> throw, a synchronous <c>SendAsync</c>
/// throw, an in-flight batch whose send task faults (drained FIFO to a false
/// result), and the finally-block that observes still-pending faulted sends
/// after an earlier failure broke the window. All failures are driven with
/// deterministic transport / feed doubles - no timing.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    private static LatticeReplicationOptions PipelinedOptions(int shipMaxInFlight = 2, int shipBatchSize = 1)
        => new()
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = 1,
            WireVersionNegotiationEnabled = false,
            ShipMaxInFlight = shipMaxInFlight,
            ShipBatchSize = shipBatchSize,
        };

    [Test]
    public async Task PumpPipelinedOnceAsync_initialize_drain_throw_backs_off_before_window_opens()
    {
        var opts = PipelinedOptions();
        var (grain, state, feed, transport, _, _, _) = Create(opts);
        feed.ThrowOnRead = new IOException("prime-boom");
        feed.Append(MakeEntry("k1", ticks: 1));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ConsecutiveFailures, Is.GreaterThan(0),
                "a prime-read throw on the pipelined path must apply backoff");
            Assert.That(SendAsyncCallCount(transport), Is.Zero,
                "no batch may ship when the drain tick could not be initialized");
        });
    }

    [Test]
    public async Task PumpPipelinedOnceAsync_wire_version_negotiation_failure_returns_without_shipping()
    {
        // CRDT-mode tree negotiating a below-current peer cannot be faithfully
        // down-encoded; the pipelined negotiation gate returns before the
        // window opens, mirroring the serial gate.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = 1,
            FramingCompression = LatticeCompression.None,
            FramingCompressionMinBatchBytes = 0,
            WireVersionNegotiationEnabled = true,
            MinimumSupportedWireVersion = 1,
            UnknownPeerWireVersionFloor = 4,
            ShipMaxInFlight = 2,
        };
        var (grain, _, feed, transport, _, _, _) =
            Create(opts, modeResolver: ModeResolver(LatticeMergeMode.OrSet));
        feed.Append(MakeEntry("k1", ticks: 1));

        using var counter = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.ShipWireVersionDownStampName);

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ShipCallCount(transport), Is.Zero,
                "the pipelined negotiation gate must pause rather than ship a mis-applyable frame");
            Assert.That(
                counter.Measurements.Single().Tags.Any(t =>
                    t.Key == LatticeReplicationMetrics.TagReason
                    && (string?)t.Value == LatticeReplicationMetrics.DownStampReasonBlockedCrdtMode),
                Is.True,
                "the counter must be tagged reason=blocked_crdt_mode");
        });
    }

    [Test]
    public async Task PumpPipelinedOnceAsync_merge_read_throw_backs_off_and_fails_the_tick()
    {
        // Prime succeeds (read #1); the mid-batch refill inside
        // MergeOneBatchAsync throws (read #2), driving the in-loop drain catch.
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            ReplogPartitions = 1,
            WireVersionNegotiationEnabled = false,
            ShipMaxInFlight = 2,
            ShipBatchSize = 2,
            ShipPartitionPageSize = 1,
        };
        var (grain, state, feed, transport, _, _, _) = Create(opts);
        feed.Append(MakeEntry("k1", ticks: 1));
        feed.Append(MakeEntry("k2", ticks: 2));
        feed.OnReadShipping = _ =>
        {
            if (feed.ReadCalls == 2)
            {
                throw new IOException("merge-refill-boom");
            }
            return Task.CompletedTask;
        };

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.ConsecutiveFailures, Is.GreaterThan(0),
                "a mid-tick merge throw must apply backoff");
            Assert.That(SendAsyncCallCount(transport), Is.Zero,
                "the tick must fail before any batch ships");
        });
    }

    [Test]
    public async Task PumpPipelinedOnceAsync_synchronous_send_throw_backs_off_and_stops_the_window()
    {
        var opts = PipelinedOptions();
        var (grain, state, feed, transport, _, _, _) = Create(opts);
        feed.Append(MakeEntry("k1", ticks: 1));
        transport
            .When(t => t.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>()))
            .Do(_ => throw new InvalidOperationException("sync-send-boom"));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.That(state.State.ConsecutiveFailures, Is.GreaterThan(0),
            "a synchronous SendAsync throw must be treated as a transport failure and back off");
    }

    [Test]
    public async Task PumpPipelinedOnceAsync_faulted_send_task_drains_to_failure_and_breaks_window()
    {
        // Two one-entry batches fill the window; draining the FIFO head awaits
        // a faulted send task, so DrainOneInFlightAsync returns false and the
        // producer loop breaks.
        var opts = PipelinedOptions(shipMaxInFlight: 2, shipBatchSize: 1);
        var (grain, state, feed, transport, _, _, _) = Create(opts);
        feed.Append(MakeEntry("k1", ticks: 1));
        feed.Append(MakeEntry("k2", ticks: 2));
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<ReplicationAck>(new InvalidOperationException("faulted-send")));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(SendAsyncCallCount(transport), Is.GreaterThanOrEqualTo(2),
                "both window slots must have launched a send before the drain observed the fault");
            Assert.That(state.State.ConsecutiveFailures, Is.GreaterThan(0),
                "a faulted in-flight send must back off");
        });
    }

    [Test]
    public async Task PumpPipelinedOnceAsync_finally_observes_still_pending_faulted_sends()
    {
        // Three one-entry batches with a window of three: draining the head
        // fails and breaks, leaving two faulted sends still queued, which the
        // finally block awaits and swallows.
        var opts = PipelinedOptions(shipMaxInFlight: 3, shipBatchSize: 1);
        var (grain, state, feed, transport, _, _, _) = Create(opts);
        feed.Append(MakeEntry("k1", ticks: 1));
        feed.Append(MakeEntry("k2", ticks: 2));
        feed.Append(MakeEntry("k3", ticks: 3));
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<ReplicationAck>(new InvalidOperationException("faulted-send")));

        await grain.PumpForTestingAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(SendAsyncCallCount(transport), Is.EqualTo(3),
                "all three window slots must have launched before the head drain failed the tick");
            Assert.That(state.State.ConsecutiveFailures, Is.GreaterThan(0),
                "the failed head drain must back off; the finally-block faults are swallowed");
            Assert.That(state.State.Cursor, Is.EqualTo(HybridLogicalClock.Zero),
                "no cursor may advance when the whole window faulted");
        });
    }
}
