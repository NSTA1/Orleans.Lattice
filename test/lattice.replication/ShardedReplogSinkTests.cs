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

    // ------------------------------------------------------------------
    // The commit-time sink is a nudge, not a WAL writer. The leaf
    // commit-log writer is the single WAL appender; the log-tailing
    // shipper tails that same WAL. The sink must therefore never touch
    // an IWalShardGrain.
    // ------------------------------------------------------------------

    [Test]
    public async Task WriteAsync_does_not_append_to_any_wal_shard_grain()
    {
        var factory = Substitute.For<IGrainFactory>();
        var shipper = Substitute.For<IReplicationShipperGrain>();
        factory.GetGrain<IReplicationShipperGrain>(Arg.Any<string>()).Returns(shipper);
        var sink = new ShardedReplogSink(
            factory,
            Monitor(),
            new FakeReplicationTopology(new[] { "site-b" }),
            NullLogger<ShardedReplogSink>.Instance);

        await sink.WriteAsync("orders", CancellationToken.None);
        await Task.Delay(20);

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
        var monitor = MonitorWithDoorbell();
        var topology = new FakeReplicationTopology(new[] { "site-b", "site-c" });
        var factory = Substitute.For<IGrainFactory>();
        var shipperB = Substitute.For<IReplicationShipperGrain>();
        var shipperC = Substitute.For<IReplicationShipperGrain>();
        factory.GetGrain<IReplicationShipperGrain>("orders/site-b").Returns(shipperB);
        factory.GetGrain<IReplicationShipperGrain>("orders/site-c").Returns(shipperC);
        var sink = new ShardedReplogSink(factory, monitor, topology, NullLogger<ShardedReplogSink>.Instance);

        await sink.WriteAsync("orders", CancellationToken.None);
        // Doorbell ring is fire-and-forget; let the continuations drain.
        await Task.Yield();
        await Task.Delay(20);

        await shipperB.Received(1).OnDoorbellAsync(Arg.Any<CancellationToken>());
        await shipperC.Received(1).OnDoorbellAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task WriteAsync_skips_doorbell_when_disabled()
    {
        var monitor = MonitorWithDoorbell(doorbellEnabled: false);
        var topology = new FakeReplicationTopology(new[] { "site-b" });
        var factory = Substitute.For<IGrainFactory>();
        var shipperB = Substitute.For<IReplicationShipperGrain>();
        factory.GetGrain<IReplicationShipperGrain>(Arg.Any<string>()).Returns(shipperB);
        var sink = new ShardedReplogSink(factory, monitor, topology, NullLogger<ShardedReplogSink>.Instance);

        await sink.WriteAsync("orders", CancellationToken.None);
        await Task.Delay(20);

        await shipperB.DidNotReceive().OnDoorbellAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task WriteAsync_skips_doorbell_when_topology_peers_empty()
    {
        var monitor = MonitorWithDoorbell();
        var topology = new FakeReplicationTopology(peers: null);
        var factory = Substitute.For<IGrainFactory>();
        var shipper = Substitute.For<IReplicationShipperGrain>();
        factory.GetGrain<IReplicationShipperGrain>(Arg.Any<string>()).Returns(shipper);
        var sink = new ShardedReplogSink(factory, monitor, topology, NullLogger<ShardedReplogSink>.Instance);

        await sink.WriteAsync("orders", CancellationToken.None);
        await Task.Delay(20);

        await shipper.DidNotReceive().OnDoorbellAsync(Arg.Any<CancellationToken>());

        // And again with an explicitly empty collection.
        var topology2 = new FakeReplicationTopology(Array.Empty<string>());
        var sink2 = new ShardedReplogSink(factory, monitor, topology2, NullLogger<ShardedReplogSink>.Instance);
        await sink2.WriteAsync("orders", CancellationToken.None);
        await Task.Delay(20);
        await shipper.DidNotReceive().OnDoorbellAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task WriteAsync_swallows_doorbell_failures()
    {
        var monitor = MonitorWithDoorbell();
        var topology = new FakeReplicationTopology(new[] { "site-b" });
        var factory = Substitute.For<IGrainFactory>();
        var shipper = Substitute.For<IReplicationShipperGrain>();
        shipper.OnDoorbellAsync(Arg.Any<CancellationToken>())
            .Returns<Task>(_ => Task.FromException(new InvalidOperationException("doorbell-failed")));
        factory.GetGrain<IReplicationShipperGrain>(Arg.Any<string>()).Returns(shipper);
        var sink = new ShardedReplogSink(factory, monitor, topology, NullLogger<ShardedReplogSink>.Instance);

        // The producer-side commit path must never fault on a
        // doorbell ring failure; WriteAsync returns successfully even
        // when every doorbell ring throws.
        Assert.That(
            async () => await sink.WriteAsync("orders", CancellationToken.None),
            Throws.Nothing);
        await Task.Delay(20);
    }

    [Test]
    public async Task WriteAsync_skips_doorbell_for_null_or_empty_peer_entries()
    {
        var monitor = MonitorWithDoorbell();
        // FakeReplicationTopology's ctor filters out null/whitespace
        // peers, so to exercise the sink's own inner skip-empty-peer
        // guard the topology is stubbed directly with malformed entries.
        var topology = Substitute.For<IReplicationTopology>();
        topology.CurrentPeers.Returns(new[] { "", null!, "" });
        var factory = Substitute.For<IGrainFactory>();
        var shipper = Substitute.For<IReplicationShipperGrain>();
        factory.GetGrain<IReplicationShipperGrain>(Arg.Any<string>()).Returns(shipper);
        var sink = new ShardedReplogSink(factory, monitor, topology, NullLogger<ShardedReplogSink>.Instance);

        await sink.WriteAsync("orders", CancellationToken.None);
        await Task.Delay(20);

        await shipper.DidNotReceive().OnDoorbellAsync(Arg.Any<CancellationToken>());
    }
}
