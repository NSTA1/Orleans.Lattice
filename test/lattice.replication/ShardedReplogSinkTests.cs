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
    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(int partitions, string clusterId = "site-a")
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var options = new LatticeReplicationOptions
        {
            ClusterId = clusterId,
            ReplogPartitions = partitions,
        };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static (ShardedReplogSink Sink, IGrainFactory Factory, IWalShardGrain DefaultGrain)
        CreateSink(int partitions = 1)
    {
        var factory = Substitute.For<IGrainFactory>();
        var defaultGrain = Substitute.For<IWalShardGrain>();
        defaultGrain.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(0L);
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(defaultGrain);
        var sink = new ShardedReplogSink(
            factory,
            Monitor(partitions),
            new FakeReplicationTopology(),
            new LocalVectorClockCache(factory),
            NullLogger<ShardedReplogSink>.Instance);
        return (sink, factory, defaultGrain);
    }

    private static WalRecord MakeEntry(string treeId, string key) => new()
    {
        TreeId = treeId,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        OriginClusterId = "site-a",
    };

    [Test]
    public async Task WriteAsync_routes_to_partition_zero_when_one_partition()
    {
        var (sink, factory, grain) = CreateSink(partitions: 1);
        var entry = MakeEntry("tree", "k");

        await sink.WriteAsync(entry, CancellationToken.None);

        factory.Received(1).GetGrain<IWalShardGrain>("tree/0");
        await grain.Received(1).AppendAsync(entry, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task WriteAsync_includes_tree_id_in_grain_key()
    {
        var (sink, factory, _) = CreateSink(partitions: 1);

        await sink.WriteAsync(MakeEntry("alpha", "x"), CancellationToken.None);
        await sink.WriteAsync(MakeEntry("beta", "x"), CancellationToken.None);

        factory.Received(1).GetGrain<IWalShardGrain>("alpha/0");
        factory.Received(1).GetGrain<IWalShardGrain>("beta/0");
    }

    [Test]
    public async Task WriteAsync_routes_distinct_keys_to_distinct_partitions_when_multiple()
    {
        const int partitions = 8;
        var (sink, factory, _) = CreateSink(partitions);

        var seenKeys = new HashSet<string>();
        for (var i = 0; i < 64; i++)
        {
            var key = $"k-{i}";
            var expected = WalPartitionHash.Compute(key, partitions);
            await sink.WriteAsync(MakeEntry("tree", key), CancellationToken.None);
            seenKeys.Add($"tree/{expected}");
        }

        Assert.That(seenKeys.Count, Is.GreaterThan(1));
        foreach (var grainKey in seenKeys)
        {
            factory.Received().GetGrain<IWalShardGrain>(grainKey);
        }
    }

    [Test]
    public async Task WriteAsync_routes_delete_range_using_start_key()
    {
        var (sink, factory, _) = CreateSink(partitions: 4);
        var entry = new WalRecord
        {
            TreeId = "rtree",
            Op = MutationKind.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            IsTombstone = true,
            Timestamp = HybridLogicalClock.Zero,
            OriginClusterId = "site-a",
        };
        var expected = WalPartitionHash.Compute("a", 4);

        await sink.WriteAsync(entry, CancellationToken.None);

        factory.Received(1).GetGrain<IWalShardGrain>($"rtree/{expected}");
    }

    [Test]
    public async Task WriteAsync_treats_null_entry_key_as_empty_string()
    {
        var (sink, factory, _) = CreateSink(partitions: 4);
        var entry = new WalRecord
        {
            TreeId = "tree",
            Op = MutationKind.Set,
            Key = null!,
            Timestamp = HybridLogicalClock.Zero,
            OriginClusterId = "site-a",
        };
        var expected = WalPartitionHash.Compute(string.Empty, 4);

        await sink.WriteAsync(entry, CancellationToken.None);

        factory.Received(1).GetGrain<IWalShardGrain>($"tree/{expected}");
    }

    [Test]
    public async Task WriteAsync_forwards_cancellation_token_to_grain()
    {
        var (sink, _, grain) = CreateSink(partitions: 1);
        using var cts = new CancellationTokenSource();

        await sink.WriteAsync(MakeEntry("tree", "k"), cts.Token);

        await grain.Received(1).AppendAsync(Arg.Any<WalRecord>(), cts.Token);
    }

    [Test]
    public async Task WriteAsync_uses_current_value_for_partition_count()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = "x", ReplogPartitions = 8 });
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(Substitute.For<IWalShardGrain>());
        var sink = new ShardedReplogSink(factory, monitor, new FakeReplicationTopology(), new LocalVectorClockCache(factory), NullLogger<ShardedReplogSink>.Instance);

        await sink.WriteAsync(MakeEntry("hot", "k"), CancellationToken.None);

        var hotPartition = WalPartitionHash.Compute("k", 8);
        factory.Received(1).GetGrain<IWalShardGrain>($"hot/{hotPartition}");
    }

    [Test]
    public void WriteAsync_propagates_grain_failures_to_caller()
    {
        var monitor = Monitor(partitions: 1);
        var factory = Substitute.For<IGrainFactory>();
        var grain = Substitute.For<IWalShardGrain>();
        grain.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns<long>(_ => throw new InvalidOperationException("boom"));
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(grain);
        var sink = new ShardedReplogSink(factory, monitor, new FakeReplicationTopology(), new LocalVectorClockCache(factory), NullLogger<ShardedReplogSink>.Instance);

        Assert.That(
            async () => await sink.WriteAsync(MakeEntry("tree", "k"), CancellationToken.None),
            Throws.InvalidOperationException.With.Message.EqualTo("boom"));
    }

    [Test]
    public async Task WriteAsync_increments_wal_entries_appended_counter_with_tree_tag()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.WalEntriesAppendedName);
        var (sink, _, _) = CreateSink(partitions: 1);

        await sink.WriteAsync(MakeEntry("orders", "k"), CancellationToken.None);

        Assert.That(collector.Measurements, Has.Count.EqualTo(1));
        var only = collector.Measurements.Single();
        Assert.That(only.Value, Is.EqualTo(1L));
        Assert.That(only.Tags, Has.Some.Matches<KeyValuePair<string, object?>>(t =>
            t.Key == "tree" && (string?)t.Value == "orders"));
    }

    [Test]
    public void WriteAsync_skips_wal_entries_appended_counter_when_grain_throws()
    {
        using var collector = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName,
            LatticeReplicationMetrics.WalEntriesAppendedName);
        var monitor = Monitor(partitions: 1);
        var factory = Substitute.For<IGrainFactory>();
        var grain = Substitute.For<IWalShardGrain>();
        grain.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>())
            .Returns<long>(_ => throw new InvalidOperationException("boom"));
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(grain);
        var sink = new ShardedReplogSink(factory, monitor, new FakeReplicationTopology(), new LocalVectorClockCache(factory), NullLogger<ShardedReplogSink>.Instance);

        Assert.That(
            async () => await sink.WriteAsync(MakeEntry("tree", "k"), CancellationToken.None),
            Throws.InvalidOperationException);

        // The counter must reflect committed entries only - a thrown
        // append does not contribute, so growth-rate vs ship-rate
        // operators are not misled by failed appends.
        Assert.That(collector.Measurements, Is.Empty);
    }

    // ------------------------------------------------------------------
    // Writer-side doorbell fan-out (production replication drivers)
    // ------------------------------------------------------------------

    private static IOptionsMonitor<LatticeReplicationOptions> MonitorWithDoorbell(
        bool doorbellEnabled = true,
        int partitions = 1)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        var options = new LatticeReplicationOptions
        {
            ClusterId = "site-a",
            ReplogPartitions = partitions,
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
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(0L);
        var shipperB = Substitute.For<IReplicationShipperGrain>();
        var shipperC = Substitute.For<IReplicationShipperGrain>();
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(shard);
        factory.GetGrain<IReplicationShipperGrain>("orders/site-b").Returns(shipperB);
        factory.GetGrain<IReplicationShipperGrain>("orders/site-c").Returns(shipperC);
        var sink = new ShardedReplogSink(factory, monitor, topology, new LocalVectorClockCache(factory), NullLogger<ShardedReplogSink>.Instance);

        await sink.WriteAsync(MakeEntry("orders", "k"), CancellationToken.None);
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
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(0L);
        var shipperB = Substitute.For<IReplicationShipperGrain>();
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(shard);
        factory.GetGrain<IReplicationShipperGrain>(Arg.Any<string>()).Returns(shipperB);
        var sink = new ShardedReplogSink(factory, monitor, topology, new LocalVectorClockCache(factory), NullLogger<ShardedReplogSink>.Instance);

        await sink.WriteAsync(MakeEntry("orders", "k"), CancellationToken.None);
        await Task.Delay(20);

        await shipperB.DidNotReceive().OnDoorbellAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task WriteAsync_skips_doorbell_when_topology_peers_empty()
    {
        var monitor = MonitorWithDoorbell();
        var topology = new FakeReplicationTopology(peers: null);
        var factory = Substitute.For<IGrainFactory>();
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(0L);
        var shipper = Substitute.For<IReplicationShipperGrain>();
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(shard);
        factory.GetGrain<IReplicationShipperGrain>(Arg.Any<string>()).Returns(shipper);
        var sink = new ShardedReplogSink(factory, monitor, topology, new LocalVectorClockCache(factory), NullLogger<ShardedReplogSink>.Instance);

        await sink.WriteAsync(MakeEntry("orders", "k"), CancellationToken.None);
        await Task.Delay(20);

        await shipper.DidNotReceive().OnDoorbellAsync(Arg.Any<CancellationToken>());

        // And again with an explicitly empty collection.
        var topology2 = new FakeReplicationTopology(Array.Empty<string>());
        var sink2 = new ShardedReplogSink(factory, monitor, topology2, new LocalVectorClockCache(factory), NullLogger<ShardedReplogSink>.Instance);
        await sink2.WriteAsync(MakeEntry("orders", "k"), CancellationToken.None);
        await Task.Delay(20);
        await shipper.DidNotReceive().OnDoorbellAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task WriteAsync_swallows_doorbell_failures()
    {
        var monitor = MonitorWithDoorbell();
        var topology = new FakeReplicationTopology(new[] { "site-b" });
        var factory = Substitute.For<IGrainFactory>();
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(0L);
        var shipper = Substitute.For<IReplicationShipperGrain>();
        shipper.OnDoorbellAsync(Arg.Any<CancellationToken>())
            .Returns<Task>(_ => Task.FromException(new InvalidOperationException("doorbell-failed")));
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(shard);
        factory.GetGrain<IReplicationShipperGrain>(Arg.Any<string>()).Returns(shipper);
        var sink = new ShardedReplogSink(factory, monitor, topology, new LocalVectorClockCache(factory), NullLogger<ShardedReplogSink>.Instance);

        // The producer-side commit path must never fault on a
        // doorbell ring failure; WriteAsync awaits the WAL append
        // and returns successfully even when every doorbell ring
        // throws.
        Assert.That(
            async () => await sink.WriteAsync(MakeEntry("orders", "k"), CancellationToken.None),
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
        var shard = Substitute.For<IWalShardGrain>();
        shard.AppendAsync(Arg.Any<WalRecord>(), Arg.Any<CancellationToken>()).Returns(0L);
        var shipper = Substitute.For<IReplicationShipperGrain>();
        factory.GetGrain<IWalShardGrain>(Arg.Any<string>()).Returns(shard);
        factory.GetGrain<IReplicationShipperGrain>(Arg.Any<string>()).Returns(shipper);
        var sink = new ShardedReplogSink(factory, monitor, topology, new LocalVectorClockCache(factory), NullLogger<ShardedReplogSink>.Instance);

        await sink.WriteAsync(MakeEntry("orders", "k"), CancellationToken.None);
        await Task.Delay(20);

        await shipper.DidNotReceive().OnDoorbellAsync(Arg.Any<CancellationToken>());
    }
}
