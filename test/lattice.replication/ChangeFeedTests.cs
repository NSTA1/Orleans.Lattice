using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class ChangeFeedTests
{
    private const string Tree = "tree";
    private const string LocalCluster = "site-a";
    private const string RemoteCluster = "site-b";

    private static IOptionsMonitor<LatticeReplicationOptions> Monitor(int partitions, string clusterId = LocalCluster)
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

    private static ReplogEntry Entry(string key, HybridLogicalClock ts, string origin = LocalCluster) => new()
    {
        TreeId = Tree,
        Op = ReplogOp.Set,
        Key = key,
        Value = new byte[] { 1 },
        Timestamp = ts,
        OriginClusterId = origin,
    };

    private static IReplogShardGrain Grain(params ReplogEntry[] entries)
    {
        var grain = Substitute.For<IReplogShardGrain>();
        var sequenced = new ReplogShardEntry[entries.Length];
        for (var i = 0; i < entries.Length; i++)
        {
            sequenced[i] = new ReplogShardEntry { Sequence = i, Entry = entries[i] };
        }

        grain.ReadAsync(Arg.Any<long>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var from = (long)call[0];
                var max = (int)call[1];
                if (from >= sequenced.Length)
                {
                    return Task.FromResult(ReplogShardPage.Empty(from));
                }

                var available = sequenced.Length - (int)from;
                var take = Math.Min(max, available);
                var page = new ReplogShardEntry[take];
                Array.Copy(sequenced, (int)from, page, 0, take);
                return Task.FromResult(new ReplogShardPage
                {
                    Entries = page,
                    NextSequence = from + take,
                });
            });
        return grain;
    }

    private static (ChangeFeed Feed, IGrainFactory Factory) CreateFeed(int partitions, string clusterId = LocalCluster)
    {
        var factory = Substitute.For<IGrainFactory>();
        var feed = new ChangeFeed(factory, Monitor(partitions, clusterId));
        return (feed, factory);
    }

    private static async Task<List<ReplogEntry>> CollectAsync(IAsyncEnumerable<ReplogEntry> source)
    {
        var result = new List<ReplogEntry>();
        await foreach (var entry in source)
        {
            result.Add(entry);
        }
        return result;
    }

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    [Test]
    public void Subscribe_throws_when_tree_name_is_null()
    {
        var (feed, _) = CreateFeed(partitions: 1);

        Assert.That(
            async () => await CollectAsync(feed.Subscribe(null!, HybridLogicalClock.Zero)),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task Subscribe_returns_empty_stream_when_wal_is_empty()
    {
        var (feed, factory) = CreateFeed(partitions: 1);
        var empty = Grain();
        factory.GetGrain<IReplogShardGrain>(Arg.Any<string>()).Returns(empty);

        var entries = await CollectAsync(feed.Subscribe(Tree, HybridLogicalClock.Zero));

        Assert.That(entries, Is.Empty);
    }

    [Test]
    public async Task Subscribe_yields_entries_with_timestamp_strictly_greater_than_cursor()
    {
        var (feed, factory) = CreateFeed(partitions: 1);
        var grain = Grain(
            Entry("a", Hlc(1)),
            Entry("b", Hlc(5)),
            Entry("c", Hlc(10)));
        factory.GetGrain<IReplogShardGrain>($"{Tree}/0").Returns(grain);

        var entries = await CollectAsync(feed.Subscribe(Tree, Hlc(5)));

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "c" }));
    }

    [Test]
    public async Task Subscribe_emits_every_entry_when_cursor_is_zero()
    {
        var (feed, factory) = CreateFeed(partitions: 1);
        var grain = Grain(Entry("a", Hlc(1)), Entry("b", Hlc(2)));
        factory.GetGrain<IReplogShardGrain>($"{Tree}/0").Returns(grain);

        var entries = await CollectAsync(feed.Subscribe(Tree, HybridLogicalClock.Zero));

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task Subscribe_yields_in_hlc_ascending_order_across_partitions()
    {
        var (feed, factory) = CreateFeed(partitions: 2);
        var p0 = Grain(Entry("p0a", Hlc(1)), Entry("p0b", Hlc(4)));
        var p1 = Grain(Entry("p1a", Hlc(2)), Entry("p1b", Hlc(3)));
        factory.GetGrain<IReplogShardGrain>($"{Tree}/0").Returns(p0);
        factory.GetGrain<IReplogShardGrain>($"{Tree}/1").Returns(p1);

        var entries = await CollectAsync(feed.Subscribe(Tree, HybridLogicalClock.Zero));

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "p0a", "p1a", "p1b", "p0b" }));
    }

    [Test]
    public async Task Subscribe_walks_every_partition_indicated_by_options()
    {
        var (feed, factory) = CreateFeed(partitions: 4);
        var empty = Grain();
        factory.GetGrain<IReplogShardGrain>(Arg.Any<string>()).Returns(empty);

        await CollectAsync(feed.Subscribe(Tree, HybridLogicalClock.Zero));

        for (var p = 0; p < 4; p++)
        {
            factory.Received(1).GetGrain<IReplogShardGrain>($"{Tree}/{p}");
        }
    }

    [Test]
    public async Task Subscribe_excludes_local_origin_when_include_local_origin_is_false()
    {
        var (feed, factory) = CreateFeed(partitions: 1);
        var grain = Grain(
            Entry("local", Hlc(1), origin: LocalCluster),
            Entry("remote", Hlc(2), origin: RemoteCluster));
        factory.GetGrain<IReplogShardGrain>($"{Tree}/0").Returns(grain);

        var entries = await CollectAsync(
            feed.Subscribe(Tree, HybridLogicalClock.Zero, includeLocalOrigin: false));

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "remote" }));
    }

    [Test]
    public async Task Subscribe_includes_local_origin_by_default()
    {
        var (feed, factory) = CreateFeed(partitions: 1);
        var grain = Grain(
            Entry("local", Hlc(1), origin: LocalCluster),
            Entry("remote", Hlc(2), origin: RemoteCluster));
        factory.GetGrain<IReplogShardGrain>($"{Tree}/0").Returns(grain);

        var entries = await CollectAsync(feed.Subscribe(Tree, HybridLogicalClock.Zero));

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "local", "remote" }));
    }

    [Test]
    public async Task Subscribe_does_not_filter_remote_origin_when_include_local_origin_is_false()
    {
        var (feed, factory) = CreateFeed(partitions: 1);
        var grain = Grain(
            Entry("a", Hlc(1), origin: RemoteCluster),
            Entry("b", Hlc(2), origin: "site-c"));
        factory.GetGrain<IReplogShardGrain>($"{Tree}/0").Returns(grain);

        var entries = await CollectAsync(
            feed.Subscribe(Tree, HybridLogicalClock.Zero, includeLocalOrigin: false));

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public async Task Subscribe_keeps_entries_with_null_origin_when_include_local_origin_is_false()
    {
        var (feed, factory) = CreateFeed(partitions: 1);
        var grain = Grain(Entry("nullorigin", Hlc(1)) with { OriginClusterId = null });
        factory.GetGrain<IReplogShardGrain>($"{Tree}/0").Returns(grain);

        var entries = await CollectAsync(
            feed.Subscribe(Tree, HybridLogicalClock.Zero, includeLocalOrigin: false));

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "nullorigin" }));
    }

    [Test]
    public async Task Subscribe_pages_through_more_entries_than_page_size()
    {
        const int total = 600; // greater than the internal PageSize of 256.
        var entries = new ReplogEntry[total];
        for (var i = 0; i < total; i++)
        {
            entries[i] = Entry($"k{i:D4}", Hlc(i + 1));
        }

        var (feed, factory) = CreateFeed(partitions: 1);
        var grain = Grain(entries);
        factory.GetGrain<IReplogShardGrain>($"{Tree}/0").Returns(grain);

        var result = await CollectAsync(feed.Subscribe(Tree, HybridLogicalClock.Zero));

        Assert.That(result, Has.Count.EqualTo(total));
        Assert.That(result[0].Key, Is.EqualTo("k0000"));
        Assert.That(result[^1].Key, Is.EqualTo($"k{total - 1:D4}"));
    }

    [Test]
    public void Subscribe_observes_cancellation()
    {
        var (feed, factory) = CreateFeed(partitions: 1);
        var grain = Grain(Entry("a", Hlc(1)));
        factory.GetGrain<IReplogShardGrain>($"{Tree}/0").Returns(grain);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await CollectAsync(feed.Subscribe(Tree, HybridLogicalClock.Zero, cancellationToken: cts.Token)),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task Subscribe_does_not_read_partitions_outside_configured_count()
    {
        var (feed, factory) = CreateFeed(partitions: 1);
        var empty = Grain();
        factory.GetGrain<IReplogShardGrain>(Arg.Any<string>()).Returns(empty);

        await CollectAsync(feed.Subscribe(Tree, HybridLogicalClock.Zero));

        factory.Received(1).GetGrain<IReplogShardGrain>($"{Tree}/0");
        factory.DidNotReceive().GetGrain<IReplogShardGrain>($"{Tree}/1");
    }

    [Test]
    public async Task Subscribe_resolves_options_using_tree_name()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ReplogPartitions = 1,
        });
        var factory = Substitute.For<IGrainFactory>();
        var empty = Grain();
        factory.GetGrain<IReplogShardGrain>(Arg.Any<string>()).Returns(empty);
        var feed = new ChangeFeed(factory, monitor);

        await CollectAsync(feed.Subscribe("alpha", HybridLogicalClock.Zero));
        await CollectAsync(feed.Subscribe("beta", HybridLogicalClock.Zero));

        monitor.Received().Get("alpha");
        monitor.Received().Get("beta");
    }
}
