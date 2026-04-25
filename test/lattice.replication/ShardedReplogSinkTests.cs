using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class ShardedReplogSinkTests
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

    private static (ShardedReplogSink Sink, IGrainFactory Factory, IReplogShardGrain DefaultGrain)
        CreateSink(int partitions = 1)
    {
        var factory = Substitute.For<IGrainFactory>();
        var defaultGrain = Substitute.For<IReplogShardGrain>();
        defaultGrain.AppendAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>()).Returns(0L);
        factory.GetGrain<IReplogShardGrain>(Arg.Any<string>()).Returns(defaultGrain);
        var sink = new ShardedReplogSink(factory, Monitor(partitions));
        return (sink, factory, defaultGrain);
    }

    private static ReplogEntry MakeEntry(string treeId, string key) => new()
    {
        TreeId = treeId,
        Op = ReplogOp.Set,
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

        factory.Received(1).GetGrain<IReplogShardGrain>("tree/0");
        await grain.Received(1).AppendAsync(entry, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task WriteAsync_includes_tree_id_in_grain_key()
    {
        var (sink, factory, _) = CreateSink(partitions: 1);

        await sink.WriteAsync(MakeEntry("alpha", "x"), CancellationToken.None);
        await sink.WriteAsync(MakeEntry("beta", "x"), CancellationToken.None);

        factory.Received(1).GetGrain<IReplogShardGrain>("alpha/0");
        factory.Received(1).GetGrain<IReplogShardGrain>("beta/0");
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
            var expected = ReplogPartitionHash.Compute(key, partitions);
            await sink.WriteAsync(MakeEntry("tree", key), CancellationToken.None);
            seenKeys.Add($"tree/{expected}");
        }

        Assert.That(seenKeys.Count, Is.GreaterThan(1));
        foreach (var grainKey in seenKeys)
        {
            factory.Received().GetGrain<IReplogShardGrain>(grainKey);
        }
    }

    [Test]
    public async Task WriteAsync_routes_delete_range_using_start_key()
    {
        var (sink, factory, _) = CreateSink(partitions: 4);
        var entry = new ReplogEntry
        {
            TreeId = "rtree",
            Op = ReplogOp.DeleteRange,
            Key = "a",
            EndExclusiveKey = "z",
            IsTombstone = true,
            Timestamp = HybridLogicalClock.Zero,
            OriginClusterId = "site-a",
        };
        var expected = ReplogPartitionHash.Compute("a", 4);

        await sink.WriteAsync(entry, CancellationToken.None);

        factory.Received(1).GetGrain<IReplogShardGrain>($"rtree/{expected}");
    }

    [Test]
    public async Task WriteAsync_treats_null_entry_key_as_empty_string()
    {
        var (sink, factory, _) = CreateSink(partitions: 4);
        var entry = new ReplogEntry
        {
            TreeId = "tree",
            Op = ReplogOp.Set,
            Key = null!,
            Timestamp = HybridLogicalClock.Zero,
            OriginClusterId = "site-a",
        };
        var expected = ReplogPartitionHash.Compute(string.Empty, 4);

        await sink.WriteAsync(entry, CancellationToken.None);

        factory.Received(1).GetGrain<IReplogShardGrain>($"tree/{expected}");
    }

    [Test]
    public async Task WriteAsync_forwards_cancellation_token_to_grain()
    {
        var (sink, _, grain) = CreateSink(partitions: 1);
        using var cts = new CancellationTokenSource();

        await sink.WriteAsync(MakeEntry("tree", "k"), cts.Token);

        await grain.Received(1).AppendAsync(Arg.Any<ReplogEntry>(), cts.Token);
    }

    [Test]
    public async Task WriteAsync_uses_current_value_for_partition_count()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.CurrentValue.Returns(new LatticeReplicationOptions { ClusterId = "x", ReplogPartitions = 8 });
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IReplogShardGrain>(Arg.Any<string>()).Returns(Substitute.For<IReplogShardGrain>());
        var sink = new ShardedReplogSink(factory, monitor);

        await sink.WriteAsync(MakeEntry("hot", "k"), CancellationToken.None);

        var hotPartition = ReplogPartitionHash.Compute("k", 8);
        factory.Received(1).GetGrain<IReplogShardGrain>($"hot/{hotPartition}");
    }

    [Test]
    public void WriteAsync_propagates_grain_failures_to_caller()
    {
        var monitor = Monitor(partitions: 1);
        var factory = Substitute.For<IGrainFactory>();
        var grain = Substitute.For<IReplogShardGrain>();
        grain.AppendAsync(Arg.Any<ReplogEntry>(), Arg.Any<CancellationToken>())
            .Returns<long>(_ => throw new InvalidOperationException("boom"));
        factory.GetGrain<IReplogShardGrain>(Arg.Any<string>()).Returns(grain);
        var sink = new ShardedReplogSink(factory, monitor);

        Assert.That(
            async () => await sink.WriteAsync(MakeEntry("tree", "k"), CancellationToken.None),
            Throws.InvalidOperationException.With.Message.EqualTo("boom"));
    }
}
