using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public class DiagnosticsIntegrationTests
{
    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private async Task<ILattice> NewTreeAsync(string prefix)
    {
        return await _fixture.CreateTreeAsync($"{prefix}-{Guid.NewGuid():N}");
    }

    [Test]
    public async Task DiagnoseAsync_empty_tree_returns_shard_reports_with_zero_live_keys()
    {
        var tree = await NewTreeAsync("diag-empty");

        var report = await tree.DiagnoseAsync();

        Assert.That(report.ShardCount, Is.EqualTo(FourShardClusterFixture.TestShardCount));
        Assert.That(report.Shards.Length, Is.EqualTo(FourShardClusterFixture.TestShardCount));
        Assert.That(report.TotalLiveKeys, Is.EqualTo(0));
        Assert.That(report.TotalTombstones, Is.EqualTo(0));
        Assert.That(report.Deep, Is.False);
        Assert.That(report.VirtualShardCount, Is.GreaterThan(0));
        var shardIndices = report.Shards.Select(s => s.ShardIndex).ToArray();
        Assert.That(shardIndices, Is.Unique);
        Assert.That(shardIndices, Is.Ordered);
    }

    [Test]
    public async Task DiagnoseAsync_populated_tree_total_live_keys_matches_count()
    {
        var tree = await NewTreeAsync("diag-count");
        const int keyCount = 30;
        for (var i = 0; i < keyCount; i++)
        {
            await tree.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        // Use a fresh tree reference to avoid any cached diagnostics.
        var report = await tree.DiagnoseAsync();
        var count = await tree.CountAsync();

        Assert.That(report.TotalLiveKeys, Is.EqualTo(count));
        Assert.That(report.TotalLiveKeys, Is.EqualTo(keyCount));
        Assert.That(report.Shards.Sum(s => s.LiveKeys), Is.EqualTo(keyCount));
    }

    [Test]
    public async Task DiagnoseAsync_deep_mode_reports_tombstones()
    {
        var tree = await NewTreeAsync("diag-deep");
        for (var i = 0; i < 20; i++)
        {
            await tree.SetAsync($"k{i:D3}", Encoding.UTF8.GetBytes($"v{i}"));
        }
        for (var i = 0; i < 5; i++)
        {
            await tree.DeleteAsync($"k{i:D3}");
        }

        var shallow = await tree.DiagnoseAsync(deep: false);
        var deep = await tree.DiagnoseAsync(deep: true);

        Assert.That(shallow.Deep, Is.False);
        Assert.That(shallow.TotalTombstones, Is.EqualTo(0));
        Assert.That(deep.Deep, Is.True);
        Assert.That(deep.TotalTombstones, Is.EqualTo(5));
        Assert.That(deep.TotalLiveKeys, Is.EqualTo(15));
        var shardWithTombstones = deep.Shards.FirstOrDefault(s => s.Tombstones > 0);
        Assert.That(shardWithTombstones.TombstoneRatio, Is.GreaterThan(0));
    }

    [Test]
    public async Task DiagnoseAsync_depth_grows_after_enough_inserts_to_split_leaves()
    {
        var tree = await NewTreeAsync("diag-depth");
        // With MaxLeafKeys=4 and 4 shards, inserting many well-distributed keys
        // forces at least one shard to grow beyond a single leaf.
        for (var i = 0; i < 80; i++)
        {
            await tree.SetAsync($"key-{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));
        }

        var report = await tree.DiagnoseAsync();

        Assert.That(report.Shards.Any(s => s.Depth >= 2), Is.True, "Expected at least one shard to have depth>=2 after many inserts.");
    }

    [Test]
    public async Task DiagnoseAsync_cancellation_before_call_throws()
    {
        var tree = await NewTreeAsync("diag-cancel");
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await tree.DiagnoseAsync(deep: false, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    [Test]
    public async Task DiagnoseAsync_hotness_counters_reflect_recent_activity()
    {
        var tree = await NewTreeAsync("diag-hot");
        for (var i = 0; i < 10; i++)
        {
            await tree.SetAsync($"k{i}", Encoding.UTF8.GetBytes($"v{i}"));
            _ = await tree.GetAsync($"k{i}");
        }

        var report = await tree.DiagnoseAsync();

        var totalReads = report.Shards.Sum(s => s.Reads);
        var totalWrites = report.Shards.Sum(s => s.Writes);
        Assert.That(totalWrites, Is.GreaterThanOrEqualTo(10));
        Assert.That(totalReads, Is.GreaterThanOrEqualTo(10));
    }

    [Test]
    public async Task DiagnoseAsync_cached_within_ttl_returns_identical_sample_timestamp()
    {
        var tree = await NewTreeAsync("diag-cache");
        await tree.SetAsync("k", [1]);

        var r1 = await tree.DiagnoseAsync();
        var r2 = await tree.DiagnoseAsync();

        // Within the default 5s cache TTL, the same SampledAt should be returned.
        Assert.That(r2.SampledAt, Is.EqualTo(r1.SampledAt));
    }
}

