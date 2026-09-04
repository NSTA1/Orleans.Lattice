using Orleans.Lattice.BPlusTree.Grains;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public partial class ShardedReplogSinkTests
{
    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(string clusterId = "site-a")
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var options = new LatticeReplicationOptions
        {
            ClusterId = clusterId,
        };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    /// <summary>
    /// Thread-safe tally of doorbell rings. The sink dispatches its rings
    /// fire-and-forget from a ring loop, so assertions need a happens-after
    /// signal they can poll rather than a fixed sleep that merely hopes the
    /// loop got there first.
    /// </summary>
    private sealed class CallCounter
    {
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public void Increment() => Interlocked.Increment(ref _count);
    }

    /// <summary>
    /// Polls <paramref name="condition"/> until it holds or the timeout
    /// expires, returning whether it was ever observed to hold. Positive
    /// assertions wait only as long as they must (with a ceiling generous
    /// enough that a loaded CI agent cannot fail them); negative
    /// assertions are expressed as "the earliest observable evidence never
    /// appeared within a window far longer than the dispatch takes",
    /// which is a stronger claim than a single fixed sleep.
    /// </summary>
    private static async Task<bool> WaitUntilAsync(Func<bool> condition, int timeoutMs = 10000)
    {
        var deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (condition())
            {
                return true;
            }

            await Task.Delay(5);
        }

        return condition();
    }

    /// <summary>
    /// Window a negative assertion waits before concluding that a
    /// fire-and-forget dispatch never happened. An order of magnitude
    /// more generous than the fixed sleeps it replaced.
    /// </summary>
    private const int NoDispatchWindowMs = 250;

    // ------------------------------------------------------------------
    // The commit-time sink is a nudge, not a WAL writer. The leaf
    // commit-log writer is the single WAL appender; the log-tailing
    // shipper tails that same WAL. The sink must therefore never touch
    // an IWalShardGrain.
    // ------------------------------------------------------------------

    [Test]
    public async Task WriteAsync_does_not_append_to_any_wal_shard_grain()
    {
        var rings = new CallCounter();
        var factory = Substitute.For<IGrainFactory>();
        var shipper = Substitute.For<IReplicationShipperGrain>();
        shipper.OnDoorbellAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            rings.Increment();
            return Task.CompletedTask;
        });
        factory.GetGrain<IReplicationShipperGrain>(Arg.Any<string>()).Returns(shipper);
        var sink = new ShardedReplogSink(
            factory,
            Monitor(),
            new FakeReplicationTopology(new[] { "site-b" }),
            NullLogger<ShardedReplogSink>.Instance);

        await sink.WriteAsync("orders", CancellationToken.None);

        // The doorbell ring is the last thing the fire-and-forget path
        // does, so observing it is a real happens-after barrier for the
        // negative assertion below - a fixed sleep asserted against
        // whatever the ring loop happened to have reached.
        Assert.That(await WaitUntilAsync(() => rings.Count > 0), Is.True,
            "the sink must complete its fire-and-forget doorbell dispatch");

        factory.DidNotReceive().GetGrain<IWalShardGrain>(Arg.Any<string>());
    }

    [Test]
    public void WriteAsync_completes_synchronously_without_a_grain_round_trip()
    {
        var factory = Substitute.For<IGrainFactory>();
        var shipper = Substitute.For<IReplicationShipperGrain>();
        factory.GetGrain<IReplicationShipperGrain>(Arg.Any<string>()).Returns(shipper);
        var sink = new ShardedReplogSink(
            factory,
            Monitor(),
            new FakeReplicationTopology(peers: null),
            NullLogger<ShardedReplogSink>.Instance);

        // With no peers the commit-time path is pure local work (no
        // doorbell fan-out) and must not await any cross-grain call.
        var task = sink.WriteAsync("orders", CancellationToken.None);

        Assert.That(task.IsCompletedSuccessfully, Is.True);
    }

    // ------------------------------------------------------------------
    // Writer-side doorbell fan-out (production replication drivers)
    // ------------------------------------------------------------------

    private static IOptionsMonitor<LatticeReplicationOptions> MonitorWithDoorbell(
        bool doorbellEnabled = true)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var options = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ShipDoorbellEnabled = doorbellEnabled,
        };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    [Test]
    public async Task WriteAsync_rings_each_peer_doorbell_when_enabled()
    {
        var ringsB = new CallCounter();
        var ringsC = new CallCounter();
        var monitor = MonitorWithDoorbell();
        var topology = new FakeReplicationTopology(new[] { "site-b", "site-c" });
        var factory = Substitute.For<IGrainFactory>();
        var shipperB = Substitute.For<IReplicationShipperGrain>();
        var shipperC = Substitute.For<IReplicationShipperGrain>();
        shipperB.OnDoorbellAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            ringsB.Increment();
            return Task.CompletedTask;
        });
        shipperC.OnDoorbellAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            ringsC.Increment();
            return Task.CompletedTask;
        });
        factory.GetGrain<IReplicationShipperGrain>("orders/site-b").Returns(shipperB);
        factory.GetGrain<IReplicationShipperGrain>("orders/site-c").Returns(shipperC);
        var sink = new ShardedReplogSink(factory, monitor, topology, NullLogger<ShardedReplogSink>.Instance);

        await sink.WriteAsync("orders", CancellationToken.None);
        // Doorbell ring is fire-and-forget; wait for the continuations to
        // drain rather than assuming a fixed delay was long enough.
        Assert.That(await WaitUntilAsync(() => ringsB.Count > 0 && ringsC.Count > 0), Is.True,
            "both peers must have their doorbell rung");

        await shipperB.Received(1).OnDoorbellAsync(Arg.Any<CancellationToken>());
        await shipperC.Received(1).OnDoorbellAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task WriteAsync_skips_doorbell_when_disabled()
    {
        var rings = new CallCounter();
        var monitor = MonitorWithDoorbell(doorbellEnabled: false);
        var topology = new FakeReplicationTopology(new[] { "site-b" });
        var factory = Substitute.For<IGrainFactory>();
        var shipperB = Substitute.For<IReplicationShipperGrain>();
        shipperB.OnDoorbellAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            rings.Increment();
            return Task.CompletedTask;
        });
        factory.GetGrain<IReplicationShipperGrain>(Arg.Any<string>()).Returns(shipperB);
        var sink = new ShardedReplogSink(factory, monitor, topology, NullLogger<ShardedReplogSink>.Instance);

        await sink.WriteAsync("orders", CancellationToken.None);

        Assert.That(
            await WaitUntilAsync(() => rings.Count > 0, NoDispatchWindowMs),
            Is.False,
            "no doorbell may be dispatched while the doorbell is disabled");
        await shipperB.DidNotReceive().OnDoorbellAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task WriteAsync_skips_doorbell_when_topology_peers_empty()
    {
        var rings = new CallCounter();
        var monitor = MonitorWithDoorbell();
        var topology = new FakeReplicationTopology(peers: null);
        var factory = Substitute.For<IGrainFactory>();
        var shipper = Substitute.For<IReplicationShipperGrain>();
        shipper.OnDoorbellAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            rings.Increment();
            return Task.CompletedTask;
        });
        factory.GetGrain<IReplicationShipperGrain>(Arg.Any<string>()).Returns(shipper);
        var sink = new ShardedReplogSink(factory, monitor, topology, NullLogger<ShardedReplogSink>.Instance);

        await sink.WriteAsync("orders", CancellationToken.None);

        Assert.That(
            await WaitUntilAsync(() => rings.Count > 0, NoDispatchWindowMs),
            Is.False,
            "a null peer set must dispatch no doorbell");
        await shipper.DidNotReceive().OnDoorbellAsync(Arg.Any<CancellationToken>());

        // And again with an explicitly empty collection.
        var topology2 = new FakeReplicationTopology(Array.Empty<string>());
        var sink2 = new ShardedReplogSink(factory, monitor, topology2, NullLogger<ShardedReplogSink>.Instance);
        await sink2.WriteAsync("orders", CancellationToken.None);

        Assert.That(
            await WaitUntilAsync(() => rings.Count > 0, NoDispatchWindowMs),
            Is.False,
            "an empty peer set must dispatch no doorbell");
        await shipper.DidNotReceive().OnDoorbellAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task WriteAsync_swallows_doorbell_failures()
    {
        var rings = new CallCounter();
        var monitor = MonitorWithDoorbell();
        var topology = new FakeReplicationTopology(new[] { "site-b" });
        var factory = Substitute.For<IGrainFactory>();
        var shipper = Substitute.For<IReplicationShipperGrain>();
        shipper.OnDoorbellAsync(Arg.Any<CancellationToken>())
            .Returns<Task>(_ =>
            {
                rings.Increment();
                return Task.FromException(new InvalidOperationException("doorbell-failed"));
            });
        factory.GetGrain<IReplicationShipperGrain>(Arg.Any<string>()).Returns(shipper);
        var sink = new ShardedReplogSink(factory, monitor, topology, NullLogger<ShardedReplogSink>.Instance);

        // The producer-side commit path must never fault on a
        // doorbell ring failure; WriteAsync returns successfully even
        // when every doorbell ring throws.
        Assert.That(
            async () => await sink.WriteAsync("orders", CancellationToken.None),
            Throws.Nothing);

        // The swallow only means something once the failing ring has
        // actually been dispatched - otherwise the test could pass with
        // the fault path never entered at all.
        Assert.That(await WaitUntilAsync(() => rings.Count > 0), Is.True,
            "the failing doorbell ring must actually be dispatched, so the swallow path is exercised");
    }

    [Test]
    public async Task WriteAsync_skips_doorbell_for_null_or_empty_peer_entries()
    {
        var rings = new CallCounter();
        var monitor = MonitorWithDoorbell();
        // FakeReplicationTopology's ctor filters out null/whitespace
        // peers, so to exercise the sink's own inner skip-empty-peer
        // guard the topology is stubbed directly with malformed entries.
        var topology = Substitute.For<IReplicationTopology>();
        topology.CurrentPeers.Returns(new[] { "", null!, "" });
        var factory = Substitute.For<IGrainFactory>();
        var shipper = Substitute.For<IReplicationShipperGrain>();
        shipper.OnDoorbellAsync(Arg.Any<CancellationToken>()).Returns(_ =>
        {
            rings.Increment();
            return Task.CompletedTask;
        });
        factory.GetGrain<IReplicationShipperGrain>(Arg.Any<string>()).Returns(shipper);
        var sink = new ShardedReplogSink(factory, monitor, topology, NullLogger<ShardedReplogSink>.Instance);

        await sink.WriteAsync("orders", CancellationToken.None);

        Assert.That(
            await WaitUntilAsync(() => rings.Count > 0, NoDispatchWindowMs),
            Is.False,
            "malformed peer entries must be skipped rather than rung");
        await shipper.DidNotReceive().OnDoorbellAsync(Arg.Any<CancellationToken>());
    }
}
