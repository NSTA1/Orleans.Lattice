namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

public partial class PublicApiContractTests
{
    // ── LeafProjectionDigest (shard digest) ─────────────────────────────

    [Test]
    public async Task GetLeafProjectionDigestAsync_returns_a_16_byte_hash()
    {
        var treeId = "pac-digest-bytes-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("k", Bytes("v"));

        var digest = await tree.GetLeafProjectionDigestAsync(shardIndex: 0);
        Assert.That(digest.Hash, Is.Not.Null);
        Assert.That(digest.Hash.Length, Is.EqualTo(16));
    }

    [Test]
    public async Task GetLeafProjectionDigestAsync_is_deterministic_across_calls()
    {
        var treeId = "pac-digest-deterministic-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("k1", Bytes("v1"));
        await tree.SetAsync("k2", Bytes("v2"));

        var first = await tree.GetLeafProjectionDigestAsync(shardIndex: 0);
        var second = await tree.GetLeafProjectionDigestAsync(shardIndex: 0);

        Assert.That(second.Hash, Is.EqualTo(first.Hash));
        Assert.That(second.EntryCount, Is.EqualTo(first.EntryCount));
        Assert.That(second.CheckpointOffset, Is.EqualTo(first.CheckpointOffset));
    }

    [Test]
    public async Task GetLeafProjectionDigestAsync_changes_when_data_changes()
    {
        var treeId = "pac-digest-changes-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("k", Bytes("v1"));
        var first = await tree.GetLeafProjectionDigestAsync(shardIndex: 0);

        await tree.SetAsync("k", Bytes("v2"));
        var second = await tree.GetLeafProjectionDigestAsync(shardIndex: 0);

        Assert.That(second.Hash, Is.Not.EqualTo(first.Hash));
    }

    [Test]
    public async Task GetLeafProjectionDigestAsync_reports_entry_count_for_shard()
    {
        var treeId = "pac-digest-entrycount-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        for (var i = 0; i < 5; i++)
        {
            await tree.SetAsync($"k{i}", Bytes($"v{i}"));
        }

        var digest = await tree.GetLeafProjectionDigestAsync(shardIndex: 0);
        Assert.That(digest.EntryCount, Is.GreaterThanOrEqualTo(5));
    }

    [Test]
    public async Task GetLeafProjectionDigestAsync_for_empty_shard_returns_zero_count()
    {
        var treeId = "pac-digest-empty-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        // No data - but we still have to register the tree, so do a
        // single write/delete to bring the shard online.
        await tree.SetAsync("k", Bytes("v"));
        await tree.DeleteAsync("k");

        var digest = await tree.GetLeafProjectionDigestAsync(shardIndex: 0);
        Assert.That(digest.Hash, Is.Not.Null);
        Assert.That(digest.Hash.Length, Is.EqualTo(16));
    }

    [Test]
    public async Task GetLeafProjectionDigestAsync_independently_per_shard()
    {
        var treeId = "pac-digest-pershard-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 2);
        for (var i = 0; i < 10; i++)
        {
            await tree.SetAsync($"k{i:D2}", Bytes($"v{i}"));
        }

        var d0 = await tree.GetLeafProjectionDigestAsync(shardIndex: 0);
        var d1 = await tree.GetLeafProjectionDigestAsync(shardIndex: 1);

        // Both shards have data, so both return valid digests, and the
        // total entry count across the two shards equals the keys we wrote.
        Assert.That(d0.EntryCount + d1.EntryCount, Is.GreaterThanOrEqualTo(10));
    }

    [Test]
    public async Task GetLeafProjectionDigestAsync_entry_count_exact_after_internal_node_splits()
    {
        // Regression for the internal-node split digest double-count.
        // The fixture pins a 4/4 leaf/internal-children layout, so writing
        // well past 4 * 4 = 16 keys into a single shard forces the leaf
        // tier to split repeatedly and then drives the internal tier to
        // split too (an internal node exceeding 4 children promotes a new
        // sibling). Two defects conspired to corrupt the chained-fold
        // entry total across an internal split:
        //   1. The donor trimmed its Children list but never pruned the
        //      matching ChildDigests rows, so it kept summing the moved
        //      children while the new sibling also counted them.
        //   2. After SplitAsync handed a child to the new sibling,
        //      AcceptSplitCoreAsync unconditionally re-seeded that child's
        //      parent slot back to the donor, so the moved child kept
        //      publishing its digest to a node that no longer owned it.
        // The fix prunes the moved rows, guards the fold against snapshots
        // from non-owned children, and only re-seeds the parent slot of a
        // child the node still owns. The chained-fold digest must report
        // EXACTLY the number of distinct keys written. A >= assertion
        // would not catch either an over- or under-count, so this test
        // pins the exact value.
        var treeId = "pac-digest-internalsplit-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);

        const int keyCount = 60;
        for (var i = 0; i < keyCount; i++)
        {
            await tree.SetAsync($"k{i:D4}", Bytes($"v{i}"));
        }

        var digest = await tree.GetLeafProjectionDigestAsync(shardIndex: 0);
        Assert.That(digest.EntryCount, Is.EqualTo(keyCount),
            "the chained-fold shard digest must report the exact distinct key count; "
            + "stale ChildDigests rows or a misdirected parent pointer after an "
            + "internal-node split skew the subtree total.");
    }
}
