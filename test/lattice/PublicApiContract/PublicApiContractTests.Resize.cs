namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

public partial class PublicApiContractTests
{
    // ── ResizeAsync ─────────────────────────────────────────────────────

    [Test]
    public async Task ResizeAsync_changes_node_sizing_and_preserves_data()
    {
        var treeId = "pac-resize-data-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 2);

        // Seed a handful of keys at the small (4/4) sizing so a couple of
        // splits have already happened.
        for (var i = 0; i < 12; i++)
        {
            await tree.SetAsync($"k{i:D2}", Bytes($"v{i}"));
        }

        await tree.ResizeAsync(newMaxLeafKeys: 8, newMaxInternalChildren: 8);

        // Wait for the resize coordinator to mark itself complete.
        await PollUntilAsync(async () => await tree.IsResizeCompleteAsync(), TimeSpan.FromSeconds(20));

        // Every key written before the resize is still readable through the
        // (alias-preserving) logical tree id.
        for (var i = 0; i < 12; i++)
        {
            var v = await tree.GetAsync($"k{i:D2}");
            Assert.That(Str(v), Is.EqualTo($"v{i}"));
        }
    }

    [Test]
    public async Task ResizeAsync_to_invalid_leaf_keys_throws()
    {
        var treeId = "pac-resize-bad-leaf-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("k", Bytes("v"));

        Assert.That(
            async () => await tree.ResizeAsync(newMaxLeafKeys: 1, newMaxInternalChildren: 8),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task ResizeAsync_to_invalid_internal_children_throws()
    {
        var treeId = "pac-resize-bad-internal-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("k", Bytes("v"));

        Assert.That(
            async () => await tree.ResizeAsync(newMaxLeafKeys: 8, newMaxInternalChildren: 2),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    // ── UndoResizeAsync ─────────────────────────────────────────────────

    [Test]
    public async Task UndoResizeAsync_with_no_completed_resize_throws()
    {
        var treeId = "pac-undo-noresize-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("k", Bytes("v"));

        Assert.That(
            async () => await tree.UndoResizeAsync(),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task UndoResizeAsync_after_resize_restores_old_tree()
    {
        var treeId = "pac-undo-after-resize-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("k", Bytes("v1"));

        await tree.ResizeAsync(newMaxLeafKeys: 8, newMaxInternalChildren: 8);
        await PollUntilAsync(async () => await tree.IsResizeCompleteAsync(), TimeSpan.FromSeconds(20));

        // Write more to the resized tree.
        await tree.SetAsync("k", Bytes("v2"));

        await tree.UndoResizeAsync();

        // After undo the alias points at the original tree; that tree had
        // value "v1" at the moment of the resize.
        Assert.That(Str(await tree.GetAsync("k")), Is.EqualTo("v1"));
    }

    // ── ReshardAsync ────────────────────────────────────────────────────

    [Test]
    public async Task ReshardAsync_grows_shard_count_and_preserves_data()
    {
        var treeId = "pac-reshard-grow-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 2);

        for (var i = 0; i < 16; i++)
        {
            await tree.SetAsync($"k{i:D2}", Bytes($"v{i}"));
        }

        await tree.ReshardAsync(newShardCount: 4);
        await PollUntilAsync(async () => await tree.IsReshardCompleteAsync(), TimeSpan.FromSeconds(30));

        // Confirm every key is still readable.
        for (var i = 0; i < 16; i++)
        {
            Assert.That(Str(await tree.GetAsync($"k{i:D2}")), Is.EqualTo($"v{i}"));
        }

        // Shard count should be at least 4 now.
        var perShard = await tree.CountPerShardAsync();
        Assert.That(perShard.Count, Is.GreaterThanOrEqualTo(4));
    }

    [Test]
    public async Task ReshardAsync_to_smaller_count_throws()
    {
        var treeId = "pac-reshard-shrink-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 4);
        await tree.SetAsync("k", Bytes("v"));

        Assert.That(
            async () => await tree.ReshardAsync(newShardCount: 2),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task ReshardAsync_to_zero_throws()
    {
        var treeId = "pac-reshard-zero-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 2);
        await tree.SetAsync("k", Bytes("v"));

        Assert.That(
            async () => await tree.ReshardAsync(newShardCount: 0),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    // ── IsResizeCompleteAsync / IsReshardCompleteAsync ──────────────────

    [Test]
    public async Task IsResizeCompleteAsync_returns_true_when_no_resize_in_progress()
    {
        var treeId = "pac-resize-nooop-complete-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("k", Bytes("v"));

        Assert.That(await tree.IsResizeCompleteAsync(), Is.True);
    }

    [Test]
    public async Task IsReshardCompleteAsync_returns_true_when_no_reshard_in_progress()
    {
        var treeId = "pac-reshard-nooop-complete-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 2);
        await tree.SetAsync("k", Bytes("v"));

        Assert.That(await tree.IsReshardCompleteAsync(), Is.True);
    }

    private static async Task PollUntilAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
            {
                return;
            }
            await Task.Delay(100);
        }

        throw new TimeoutException($"Condition not met within {timeout}.");
    }
}
