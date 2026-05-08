namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

public partial class PublicApiContractTests
{
    // ── DiagnoseAsync (shallow) ─────────────────────────────────────────

    [Test]
    public async Task DiagnoseAsync_shallow_returns_report_with_tree_id_and_shard_count()
    {
        var treeId = "pac-diag-shallow-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 4);
        await tree.SetAsync("k", Bytes("v"));

        var report = await tree.DiagnoseAsync(deep: false);

        Assert.That(report.TreeId, Is.EqualTo(treeId));
        Assert.That(report.ShardCount, Is.EqualTo(4));
        Assert.That(report.Shards, Is.Not.Empty);
        Assert.That(report.Deep, Is.False);
        Assert.That(report.SampledAt, Is.GreaterThan(DateTimeOffset.MinValue));
    }

    [Test]
    public async Task DiagnoseAsync_shallow_aggregates_live_keys_across_shards()
    {
        var treeId = "pac-diag-livekeys-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 2);

        for (var i = 0; i < 10; i++)
        {
            await tree.SetAsync($"k{i:D2}", Bytes($"v{i}"));
        }

        var report = await tree.DiagnoseAsync(deep: false);
        Assert.That(report.TotalLiveKeys, Is.EqualTo(10));
    }

    [Test]
    public async Task DiagnoseAsync_shallow_does_not_populate_tombstone_counts()
    {
        var treeId = "pac-diag-shallow-tomb-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("k", Bytes("v"));
        await tree.DeleteAsync("k");

        var report = await tree.DiagnoseAsync(deep: false);
        Assert.That(report.TotalTombstones, Is.EqualTo(0));
    }

    // ── DiagnoseAsync (deep) ───────────────────────────────────────────

    [Test]
    public async Task DiagnoseAsync_deep_populates_tombstone_counts()
    {
        var treeId = "pac-diag-deep-tomb-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("a", Bytes("1"));
        await tree.SetAsync("b", Bytes("2"));
        await tree.DeleteAsync("a");

        var report = await tree.DiagnoseAsync(deep: true);
        Assert.That(report.Deep, Is.True);
        Assert.That(report.TotalTombstones, Is.GreaterThanOrEqualTo(1));
    }

    [Test]
    public async Task DiagnoseAsync_returns_per_shard_diagnostics_in_index_order()
    {
        var treeId = "pac-diag-pershard-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 4);
        await tree.SetAsync("k", Bytes("v"));

        var report = await tree.DiagnoseAsync(deep: false);
        Assert.That(report.Shards.Length, Is.GreaterThanOrEqualTo(4));

        // Shard indexes should be monotonically non-decreasing.
        for (var i = 1; i < report.Shards.Length; i++)
        {
            Assert.That(report.Shards[i].ShardIndex, Is.GreaterThanOrEqualTo(report.Shards[i - 1].ShardIndex));
        }
    }

    [Test]
    public async Task DiagnoseAsync_repeated_call_returns_cached_result_or_fresh_one()
    {
        // The returned report carries a SampledAt timestamp captured by
        // whichever silo materialised it. A repeat call within the
        // configured DiagnosticsCacheTtl is cache-served, so SampledAt
        // is identical; outside the TTL it is fresh. We just assert
        // both calls succeed and return reports for the same tree id.
        var treeId = "pac-diag-cache-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("k", Bytes("v"));

        var first = await tree.DiagnoseAsync();
        var second = await tree.DiagnoseAsync();

        Assert.That(first.TreeId, Is.EqualTo(treeId));
        Assert.That(second.TreeId, Is.EqualTo(treeId));
    }
}
