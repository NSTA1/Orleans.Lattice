using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

public partial class ShardedReplogSinkTests
{
    // ------------------------------------------------------------------
    // Writer-side doorbell coalescing. A doorbell is an idempotent,
    // edge-triggered "there is work" wake, so a burst of per-commit ring
    // requests for the same (tree, peer) must collapse into at most one
    // in-flight ring plus one pending follow-up - bounding the doorbell
    // message rate the non-reentrant shipper activation sees to a small
    // constant regardless of write throughput.
    // ------------------------------------------------------------------

    [Test]
    public async Task WriteAsync_coalesces_a_burst_into_at_most_two_rings_per_peer()
    {
        var monitor = MonitorWithDoorbell();
        var topology = new FakeReplicationTopology(new[] { "site-b" });
        var factory = Substitute.For<IGrainFactory>();
        var shipper = Substitute.For<IReplicationShipperGrain>();

        // Gate the in-flight ring so every burst write lands while the
        // first ring is still running and must coalesce into pending.
        var rings = new CallCounter();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        shipper.OnDoorbellAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            rings.Increment();
            return gate.Task;
        });
        factory.GetGrain<IReplicationShipperGrain>("orders/site-b").Returns(shipper);

        var sink = new ShardedReplogSink(factory, monitor, topology, NullLogger<ShardedReplogSink>.Instance);

        using var coalesced = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.DoorbellCoalescedName);
        using var rung = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.DoorbellRungName);

        // Fire a large burst synchronously. The first WriteAsync starts the
        // ring loop (which parks on the gate); every subsequent WriteAsync
        // must fold into the single pending follow-up rather than enqueue
        // its own OnDoorbellAsync grain call.
        const int burst = 5000;
        for (var i = 0; i < burst; i++)
        {
            await sink.WriteAsync("orders", CancellationToken.None);
        }

        // Release the in-flight ring; the loop then fires exactly one
        // trailing ring for the coalesced pending request and settles.
        gate.SetResult();

        // Wait for the trailing ring to be dispatched, then prove the loop
        // really settled by watching for a third ring that must never come.
        // Both are observations of the ring loop itself rather than a fixed
        // sleep that assumed the loop had finished.
        await WaitUntilAsync(() => rings.Count >= 2, 5000);
        Assert.That(
            await WaitUntilAsync(() => rings.Count > 2, NoDispatchWindowMs),
            Is.False,
            "the coalesced burst must settle after the single trailing ring");

        var totalRung = rung.Measurements.Sum(m => m.Value);
        var totalCoalesced = coalesced.Measurements.Sum(m => m.Value);
        Assert.Multiple(() =>
        {
            // The storm-bounding guarantee: a burst of thousands of writes
            // dispatches at most two rings (one in-flight, one follow-up).
            Assert.That(totalRung, Is.GreaterThanOrEqualTo(1).And.LessThanOrEqualTo(2),
                "a burst must collapse to at most one in-flight ring plus one follow-up");
            // The overwhelming majority of the burst was elided.
            Assert.That(totalCoalesced, Is.GreaterThanOrEqualTo(burst - 2),
                "expected nearly the entire burst to be coalesced");
            Assert.That(
                coalesced.Measurements,
                Has.All.Matches<RecordedMeasurement<long>>(m =>
                    m.Tags.Any(t => t.Key == LatticeReplicationMetrics.TagTree && Equals(t.Value, "orders"))
                    && m.Tags.Any(t => t.Key == LatticeReplicationMetrics.TagPeer && Equals(t.Value, "site-b"))),
                "every coalesced measurement carries the (tree, peer) tags");
        });
    }

    [Test]
    public async Task WriteAsync_records_dispatched_rings_on_the_rung_counter()
    {
        var rings = new CallCounter();
        var monitor = MonitorWithDoorbell();
        var topology = new FakeReplicationTopology(new[] { "site-b" });
        var factory = Substitute.For<IGrainFactory>();
        var shipper = Substitute.For<IReplicationShipperGrain>();
        shipper.OnDoorbellAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            rings.Increment();
            return Task.CompletedTask;
        });
        factory.GetGrain<IReplicationShipperGrain>("orders/site-b").Returns(shipper);
        var sink = new ShardedReplogSink(factory, monitor, topology, NullLogger<ShardedReplogSink>.Instance);

        using var rung = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.DoorbellRungName);

        await sink.WriteAsync("orders", CancellationToken.None);
        // The counter is incremented immediately before the grain call, so
        // observing the call is a sound happens-after signal for the
        // measurement - unlike a fixed sleep.
        Assert.That(await WaitUntilAsync(() => rings.Count > 0), Is.True,
            "the fire-and-forget ring must reach the shipper");

        Assert.That(rung.Measurements.Sum(m => m.Value), Is.GreaterThanOrEqualTo(1),
            "a settled single write dispatches exactly one ring");
        Assert.That(
            rung.Measurements,
            Has.All.Matches<RecordedMeasurement<long>>(m =>
                m.Tags.Any(t => t.Key == LatticeReplicationMetrics.TagTree && Equals(t.Value, "orders"))
                && m.Tags.Any(t => t.Key == LatticeReplicationMetrics.TagPeer && Equals(t.Value, "site-b"))));
    }

    [Test]
    public async Task WriteAsync_re_rings_after_a_ring_completes_for_a_later_burst()
    {
        // A doorbell rung after the loop has already settled must start a
        // fresh ring loop rather than being silently dropped.
        var rings = new CallCounter();
        var monitor = MonitorWithDoorbell();
        var topology = new FakeReplicationTopology(new[] { "site-b" });
        var factory = Substitute.For<IGrainFactory>();
        var shipper = Substitute.For<IReplicationShipperGrain>();
        shipper.OnDoorbellAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            rings.Increment();
            return Task.CompletedTask;
        });
        factory.GetGrain<IReplicationShipperGrain>("orders/site-b").Returns(shipper);
        var sink = new ShardedReplogSink(factory, monitor, topology, NullLogger<ShardedReplogSink>.Instance);

        await sink.WriteAsync("orders", CancellationToken.None);
        // Wait for the first ring to have been dispatched before the second
        // write, so the second write is genuinely a later burst rather than
        // one that happened to land inside an unfinished sleep window.
        Assert.That(await WaitUntilAsync(() => rings.Count >= 1), Is.True,
            "the first write must dispatch its ring");

        await sink.WriteAsync("orders", CancellationToken.None);
        Assert.That(await WaitUntilAsync(() => rings.Count >= 2), Is.True,
            "a write after the ring loop settled must dispatch a fresh ring");

        await shipper.Received(2).OnDoorbellAsync(Arg.Any<CancellationToken>());
    }
}
