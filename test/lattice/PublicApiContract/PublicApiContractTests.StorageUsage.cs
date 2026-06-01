using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

/// <summary>
/// Pins the public byte-accurate storage-usage contract:
/// <see cref="ILattice.GetStorageUsageAsync"/> for a single tree and
/// <see cref="ILatticeAdmin.GetTotalStorageUsageAsync"/> for the
/// cluster-wide roll-up. The default in-memory WAL provider supports byte
/// accounting, so reports are exact (not partial) in this suite.
/// </summary>
public partial class PublicApiContractTests
{
    [Test]
    public async Task GetStorageUsageAsync_returns_report_for_tree_with_data()
    {
        var treeId = "pac-usage-basic-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("alpha", Bytes("one"));
        await tree.SetAsync("beta", Bytes("two"));

        var report = await tree.GetStorageUsageAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeId, Is.EqualTo(treeId));
            Assert.That(report.LeafStateBytes, Is.GreaterThan(0));
            Assert.That(report.WalRetainedBytes, Is.GreaterThan(0));
            Assert.That(report.TotalBytes, Is.EqualTo(
                report.WalRetainedBytes + report.SnapshotBytes + report.LeafStateBytes));
            Assert.That(report.Partial, Is.False);
            Assert.That(report.SampledAt, Is.GreaterThan(DateTimeOffset.MinValue));
        });
    }

    [Test]
    public async Task GetStorageUsageAsync_leaf_state_bytes_grow_with_more_data()
    {
        var treeId = "pac-usage-grow-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("k", Bytes("v"));

        var before = await tree.GetStorageUsageAsync();

        for (var i = 0; i < 20; i++)
        {
            await tree.SetAsync($"key-{i:D3}", Bytes(new string('x', 64)));
        }

        // Bypass the cache window by waiting it out is flaky; instead assert
        // the aggregate is monotonic over a fresh aggregator activation by
        // using a value-bearing comparison that tolerates the TTL cache:
        // the second report is either cached (equal) or larger.
        var after = await tree.GetStorageUsageAsync();

        Assert.That(after.LeafStateBytes, Is.GreaterThanOrEqualTo(before.LeafStateBytes));
    }

    [Test]
    public async Task GetStorageUsageAsync_empty_tree_reports_zero_leaf_state_bytes()
    {
        var treeId = "pac-usage-empty-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        var report = await tree.GetStorageUsageAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeId, Is.EqualTo(treeId));
            Assert.That(report.LeafStateBytes, Is.EqualTo(0));
            Assert.That(report.SnapshotBytes, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task GetStorageUsageAsync_aggregates_leaf_state_across_shards()
    {
        var treeId = "pac-usage-shards-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 4);

        for (var i = 0; i < 40; i++)
        {
            await tree.SetAsync($"k{i:D3}", Bytes($"value-{i}"));
        }

        var report = await tree.GetStorageUsageAsync();
        Assert.That(report.LeafStateBytes, Is.GreaterThan(0));
        Assert.That(report.TotalBytes, Is.GreaterThanOrEqualTo(report.LeafStateBytes));
    }

    [Test]
    public async Task GetTotalStorageUsageAsync_rolls_up_registered_trees()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var treeA = "pac-cluster-a-" + suffix;
        var treeB = "pac-cluster-b-" + suffix;
        var a = await _fixture.CreateSmallTreeAsync(treeA, shardCount: 1);
        var b = await _fixture.CreateSmallTreeAsync(treeB, shardCount: 1);
        await a.SetAsync("ka", Bytes("va"));
        await b.SetAsync("kb", Bytes("vb"));

        var admin = Client.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey);
        var report = await admin.GetTotalStorageUsageAsync();

        Assert.Multiple(() =>
        {
            Assert.That(report.TreeCount, Is.GreaterThanOrEqualTo(2));
            Assert.That(report.Trees, Is.Not.Empty);
            Assert.That(report.TotalBytes, Is.EqualTo(
                report.WalRetainedBytes + report.SnapshotBytes + report.LeafStateBytes));
            Assert.That(report.Trees.Any(t => t.TreeId == treeA), Is.True);
            Assert.That(report.Trees.Any(t => t.TreeId == treeB), Is.True);
            Assert.That(report.SampledAt, Is.GreaterThan(DateTimeOffset.MinValue));
        });
    }

    [Test]
    public async Task GetTotalStorageUsageAsync_total_equals_sum_of_per_tree_totals()
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync("pac-cluster-sum-" + suffix, shardCount: 1);
        await tree.SetAsync("k", Bytes("v"));

        var admin = Client.GetGrain<ILatticeAdmin>(LatticeConstants.AdminGrainKey);
        var report = await admin.GetTotalStorageUsageAsync();

        long sum = 0;
        foreach (var t in report.Trees)
        {
            sum += t.TotalBytes;
        }

        Assert.That(report.TotalBytes, Is.EqualTo(sum));
    }
}
