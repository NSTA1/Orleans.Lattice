using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public class LatticeStatsGrainTests
{
    private const string TreeId = "stats-test-tree";

    private static (LatticeStatsGrain grain, IGrainFactory factory, ILattice lattice, Dictionary<int, IShardRootGrain> shards)
        CreateGrain(int physicalShardCount = 2, int virtualShardCount = 16, LatticeOptions? options = null)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("stats", TreeId));

        var factory = Substitute.For<IGrainFactory>();
        options ??= new LatticeOptions();

        var lattice = Substitute.For<ILattice>();
        var map = ShardMap.CreateDefault(virtualShardCount, physicalShardCount);
        lattice.GetRoutingAsync(Arg.Any<CancellationToken>())
            .Returns(new RoutingInfo(TreeId, map));
        factory.GetGrain<ILattice>(TreeId).Returns(lattice);

        var shards = new Dictionary<int, IShardRootGrain>();
        for (var i = 0; i < physicalShardCount; i++)
        {
            var idx = i;
            var shard = Substitute.For<IShardRootGrain>();
            shard.GetDiagnosticsAsync(Arg.Any<bool>()).Returns(ci => new ShardDiagnosticReport
            {
                Depth = 1,
                RootIsLeaf = true,
                LiveKeys = 10,
                Tombstones = (bool)ci[0] ? 3 : 0,
                TombstoneRatio = (bool)ci[0] ? 3.0 / 13 : 0,
                Reads = 5,
                Writes = 7,
                HotnessWindow = TimeSpan.FromSeconds(30),
            });
            shards[idx] = shard;
            factory.GetGrain<IShardRootGrain>($"{TreeId}/{idx}").Returns(shard);
        }

        var resolver = TestOptionsResolver.ForFactory(factory, options);
        var logger = NullLogger<LatticeStatsGrain>.Instance;
        var grain = new LatticeStatsGrain(context, factory, resolver, logger);
        return (grain, factory, lattice, shards);
    }

    [Test]
    public async Task GetReportAsync_shallow_aggregates_live_keys_per_shard()
    {
        var (grain, _, _, _) = CreateGrain(physicalShardCount: 2);

        var report = await grain.GetReportAsync(deep: false, CancellationToken.None);

        Assert.That(report.TreeId, Is.EqualTo(TreeId));
        Assert.That(report.ShardCount, Is.EqualTo(2));
        Assert.That(report.TotalLiveKeys, Is.EqualTo(20));
        Assert.That(report.TotalTombstones, Is.EqualTo(0));
        Assert.That(report.Deep, Is.False);
        Assert.That(report.Shards, Has.Length.EqualTo(2));
        Assert.That(report.Shards.Select(s => s.ShardIndex), Is.EqualTo(new[] { 0, 1 }));
    }

    [Test]
    public async Task GetReportAsync_deep_aggregates_tombstones()
    {
        var (grain, _, _, _) = CreateGrain(physicalShardCount: 2);

        var report = await grain.GetReportAsync(deep: true, CancellationToken.None);

        Assert.That(report.Deep, Is.True);
        Assert.That(report.TotalLiveKeys, Is.EqualTo(20));
        Assert.That(report.TotalTombstones, Is.EqualTo(6));
    }

    [Test]
    public async Task GetReportAsync_caches_within_ttl()
    {
        var (grain, _, _, shards) = CreateGrain(options: new LatticeOptions { DiagnosticsCacheTtl = TimeSpan.FromMinutes(5) });

        var r1 = await grain.GetReportAsync(deep: false, CancellationToken.None);
        var r2 = await grain.GetReportAsync(deep: false, CancellationToken.None);

        Assert.That(r2.SampledAt, Is.EqualTo(r1.SampledAt));
        await shards[0].Received(1).GetDiagnosticsAsync(false);
    }

    [Test]
    public async Task GetReportAsync_does_not_cache_when_ttl_is_zero()
    {
        var (grain, _, _, shards) = CreateGrain(options: new LatticeOptions { DiagnosticsCacheTtl = TimeSpan.Zero });

        await grain.GetReportAsync(deep: false, CancellationToken.None);
        await grain.GetReportAsync(deep: false, CancellationToken.None);

        await shards[0].Received(2).GetDiagnosticsAsync(false);
    }

    [Test]
    public async Task GetReportAsync_shallow_and_deep_cached_independently()
    {
        var (grain, _, _, shards) = CreateGrain(options: new LatticeOptions { DiagnosticsCacheTtl = TimeSpan.FromMinutes(5) });

        await grain.GetReportAsync(deep: false, CancellationToken.None);
        await grain.GetReportAsync(deep: true, CancellationToken.None);

        await shards[0].Received(1).GetDiagnosticsAsync(false);
        await shards[0].Received(1).GetDiagnosticsAsync(true);
    }

    [Test]
    public async Task RecordSplitAsync_appends_entry_to_recent_splits()
    {
        var (grain, _, _, _) = CreateGrain();

        await grain.RecordSplitAsync(3, DateTime.UtcNow);
        var report = await grain.GetReportAsync(deep: false, CancellationToken.None);

        Assert.That(report.RecentSplits, Has.Length.EqualTo(1));
        Assert.That(report.RecentSplits[0].ShardIndex, Is.EqualTo(3));
    }

    [Test]
    public async Task RecordSplitAsync_caps_ring_buffer_at_capacity()
    {
        var (grain, _, _, _) = CreateGrain();

        for (var i = 0; i < LatticeStatsGrain.RecentSplitsCapacity + 10; i++)
        {
            await grain.RecordSplitAsync(i, DateTime.UtcNow);
        }
        var report = await grain.GetReportAsync(deep: false, CancellationToken.None);

        Assert.That(report.RecentSplits, Has.Length.EqualTo(LatticeStatsGrain.RecentSplitsCapacity));
        // Oldest entries evicted: the buffer should contain the most recent entries.
        Assert.That(report.RecentSplits[0].ShardIndex, Is.EqualTo(10));
        Assert.That(report.RecentSplits[^1].ShardIndex, Is.EqualTo(LatticeStatsGrain.RecentSplitsCapacity + 9));
    }

    [Test]
    public async Task RecordSplitAsync_invalidates_cached_reports()
    {
        var (grain, _, _, shards) = CreateGrain(options: new LatticeOptions { DiagnosticsCacheTtl = TimeSpan.FromMinutes(5) });

        await grain.GetReportAsync(deep: false, CancellationToken.None);
        await grain.RecordSplitAsync(1, DateTime.UtcNow);
        await grain.GetReportAsync(deep: false, CancellationToken.None);

        await shards[0].Received(2).GetDiagnosticsAsync(false);
    }

    [Test]
    public void GetReportAsync_precancelled_token_throws()
    {
        var (grain, _, _, _) = CreateGrain();
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await grain.GetReportAsync(deep: false, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task GetReportAsync_stamps_shard_index_per_shard()
    {
        var (grain, _, _, _) = CreateGrain(physicalShardCount: 3);

        var report = await grain.GetReportAsync(deep: false, CancellationToken.None);

        Assert.That(report.Shards.Select(s => s.ShardIndex), Is.EqualTo(new[] { 0, 1, 2 }));
    }
}

