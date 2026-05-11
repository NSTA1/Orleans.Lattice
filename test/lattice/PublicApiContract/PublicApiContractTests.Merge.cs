namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

public partial class PublicApiContractTests
{
    // ── MergeAsync ──────────────────────────────────────────────────────

    [Test]
    public async Task MergeAsync_pulls_keys_from_source_tree()
    {
        var sourceId = "pac-merge-src-" + Guid.NewGuid().ToString("N")[..8];
        var targetId = "pac-merge-dst-" + Guid.NewGuid().ToString("N")[..8];
        var src = await _fixture.CreateSmallTreeAsync(sourceId, shardCount: 1);
        var dst = await _fixture.CreateSmallTreeAsync(targetId, shardCount: 1);

        await src.SetAsync("a", Bytes("from-src"));
        await src.SetAsync("b", Bytes("from-src-b"));
        await dst.SetAsync("c", Bytes("from-dst"));

        await dst.MergeAsync(sourceId);

        // Wait for merge coordinator to mark itself complete.
        await PollUntilAsync(async () => await dst.IsMergeCompleteAsync(), TimeSpan.FromSeconds(20));

        Assert.That(Str(await dst.GetAsync("a")), Is.EqualTo("from-src"));
        Assert.That(Str(await dst.GetAsync("b")), Is.EqualTo("from-src-b"));
        Assert.That(Str(await dst.GetAsync("c")), Is.EqualTo("from-dst"));
    }

    [Test]
    public async Task MergeAsync_does_not_modify_source_tree()
    {
        var sourceId = "pac-merge-src-immut-" + Guid.NewGuid().ToString("N")[..8];
        var targetId = "pac-merge-dst-immut-" + Guid.NewGuid().ToString("N")[..8];
        var src = await _fixture.CreateSmallTreeAsync(sourceId, shardCount: 1);
        var dst = await _fixture.CreateSmallTreeAsync(targetId, shardCount: 1);

        await src.SetAsync("k", Bytes("source-value"));

        await dst.MergeAsync(sourceId);
        await PollUntilAsync(async () => await dst.IsMergeCompleteAsync(), TimeSpan.FromSeconds(20));

        Assert.That(Str(await src.GetAsync("k")), Is.EqualTo("source-value"));
    }

    [Test]
    public async Task MergeAsync_higher_HLC_wins_LWW()
    {
        var sourceId = "pac-merge-lww-src-" + Guid.NewGuid().ToString("N")[..8];
        var targetId = "pac-merge-lww-dst-" + Guid.NewGuid().ToString("N")[..8];
        var src = await _fixture.CreateSmallTreeAsync(sourceId, shardCount: 1);
        var dst = await _fixture.CreateSmallTreeAsync(targetId, shardCount: 1);

        // Write to dst first; then write to src — src's HLC will be later.
        await dst.SetAsync("k", Bytes("dst-old"));
        await Task.Delay(20);
        await src.SetAsync("k", Bytes("src-newer"));

        await dst.MergeAsync(sourceId);
        await PollUntilAsync(async () => await dst.IsMergeCompleteAsync(), TimeSpan.FromSeconds(20));

        // The source write was later in HLC order so it should win.
        Assert.That(Str(await dst.GetAsync("k")), Is.EqualTo("src-newer"));
    }

    [Test]
    public async Task MergeAsync_propagates_tombstones_from_source()
    {
        var sourceId = "pac-merge-tomb-src-" + Guid.NewGuid().ToString("N")[..8];
        var targetId = "pac-merge-tomb-dst-" + Guid.NewGuid().ToString("N")[..8];
        var src = await _fixture.CreateSmallTreeAsync(sourceId, shardCount: 1);
        var dst = await _fixture.CreateSmallTreeAsync(targetId, shardCount: 1);

        // Both have "k". Then source deletes "k" — the tombstone is later.
        await dst.SetAsync("k", Bytes("dst-live"));
        await src.SetAsync("k", Bytes("src-live"));
        await Task.Delay(20);
        await src.DeleteAsync("k");

        await dst.MergeAsync(sourceId);
        await PollUntilAsync(async () => await dst.IsMergeCompleteAsync(), TimeSpan.FromSeconds(20));

        // Source tombstone wins LWW; dst should observe a deleted key.
        Assert.That(await dst.GetAsync("k"), Is.Null);
    }

    [Test]
    public async Task MergeAsync_with_different_shard_counts_rehashes_entries()
    {
        var sourceId = "pac-merge-rehash-src-" + Guid.NewGuid().ToString("N")[..8];
        var targetId = "pac-merge-rehash-dst-" + Guid.NewGuid().ToString("N")[..8];
        var src = await _fixture.CreateSmallTreeAsync(sourceId, shardCount: 1);
        var dst = await _fixture.CreateSmallTreeAsync(targetId, shardCount: 4);

        for (var i = 0; i < 20; i++)
        {
            await src.SetAsync($"k{i:D2}", Bytes($"v{i}"));
        }

        await dst.MergeAsync(sourceId);
        await PollUntilAsync(async () => await dst.IsMergeCompleteAsync(), TimeSpan.FromSeconds(30));

        // Every source key should be visible on dst, re-hashed across the
        // 4-shard layout.
        for (var i = 0; i < 20; i++)
        {
            Assert.That(Str(await dst.GetAsync($"k{i:D2}")), Is.EqualTo($"v{i}"));
        }
    }

    [Test]
    public async Task MergeAsync_from_self_throws()
    {
        var treeId = "pac-merge-self-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("k", Bytes("v"));

        Assert.That(
            async () => await tree.MergeAsync(treeId),
            Throws.InstanceOf<ArgumentException>().Or.InstanceOf<InvalidOperationException>());
    }

    // ── IsMergeCompleteAsync ────────────────────────────────────────────

    [Test]
    public async Task IsMergeCompleteAsync_returns_true_when_no_merge_in_progress()
    {
        var treeId = "pac-merge-noop-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("k", Bytes("v"));

        Assert.That(await tree.IsMergeCompleteAsync(), Is.True);
    }
}
