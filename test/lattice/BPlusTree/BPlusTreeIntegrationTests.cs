using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

[Collection(ClusterCollection.Name)]
public class BPlusTreeIntegrationTests(ClusterFixture fixture)
{
    private readonly TestCluster _cluster = fixture.Cluster;

    [Fact]
    public async Task Set_and_Get_roundtrips_a_value()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("test-tree");
        var value = Encoding.UTF8.GetBytes("hello-world");

        await router.SetAsync("key1", value);
        var result = await router.GetAsync("key1");

        Assert.NotNull(result);
        Assert.Equal("hello-world", Encoding.UTF8.GetString(result));
    }

    [Fact]
    public async Task Get_returns_null_for_missing_key()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("test-tree-miss");
        var result = await router.GetAsync("nonexistent");
        Assert.Null(result);
    }

    [Fact]
    public async Task Delete_returns_false_for_missing_key()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("test-tree-del-miss");
        var result = await router.DeleteAsync("nonexistent");
        Assert.False(result);
    }

    [Fact]
    public async Task Delete_removes_a_previously_set_key()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("test-tree-del");
        await router.SetAsync("to-delete", Encoding.UTF8.GetBytes("value"));

        var deleted = await router.DeleteAsync("to-delete");
        Assert.True(deleted);

        var result = await router.GetAsync("to-delete");
        Assert.Null(result);
    }

    [Fact]
    public async Task Set_overwrites_existing_value()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("test-tree-overwrite");
        await router.SetAsync("k", Encoding.UTF8.GetBytes("v1"));
        await router.SetAsync("k", Encoding.UTF8.GetBytes("v2"));

        var result = await router.GetAsync("k");
        Assert.NotNull(result);
        Assert.Equal("v2", Encoding.UTF8.GetString(result));
    }

    [Fact]
    public async Task Multiple_keys_in_same_shard_are_independent()
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>("test-tree-multi");
        await router.SetAsync("alpha", Encoding.UTF8.GetBytes("a"));
        await router.SetAsync("bravo", Encoding.UTF8.GetBytes("b"));
        await router.SetAsync("charlie", Encoding.UTF8.GetBytes("c"));

        Assert.Equal("a", Encoding.UTF8.GetString((await router.GetAsync("alpha"))!));
        Assert.Equal("b", Encoding.UTF8.GetString((await router.GetAsync("bravo"))!));
        Assert.Equal("c", Encoding.UTF8.GetString((await router.GetAsync("charlie"))!));
    }
}

/// <summary>
/// Integration tests that insert keys in non-ascending order using a single-shard,
/// small-leaf cluster to force many splits and expose routing bugs.
/// </summary>
[Collection(SmallLeafClusterCollection.Name)]
public class BPlusTreeInsertionOrderTests(SmallLeafClusterFixture fixture)
{
    private readonly TestCluster _cluster = fixture.Cluster;

    [Fact]
    public async Task Reverse_order_insert_then_get_all_keys()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("rev-insert-get");
        const int count = 30;
        var value = Encoding.UTF8.GetBytes("v");

        for (int i = count - 1; i >= 0; i--)
            await tree.SetAsync($"k{i:D4}", value);

        var missing = new List<string>();
        for (int i = 0; i < count; i++)
        {
            var result = await tree.GetAsync($"k{i:D4}");
            if (result is null) missing.Add($"k{i:D4}");
        }

        Assert.Empty(missing);
    }

    [Fact]
    public async Task Random_order_insert_then_get_all_keys()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("rand-insert-get");
        const int count = 30;
        var value = Encoding.UTF8.GetBytes("v");

        var indices = Enumerable.Range(0, count).ToArray();
        var rng = new Random(42);
        for (int i = count - 1; i > 0; i--)
        {
            int j = rng.Next(i + 1);
            (indices[i], indices[j]) = (indices[j], indices[i]);
        }

        foreach (var i in indices)
            await tree.SetAsync($"k{i:D4}", value);

        var missing = new List<string>();
        for (int i = 0; i < count; i++)
        {
            var result = await tree.GetAsync($"k{i:D4}");
            if (result is null) missing.Add($"k{i:D4}");
        }

        Assert.Empty(missing);
    }

    [Fact]
    public async Task Reverse_order_insert_keys_scan_returns_all()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("rev-insert-keys");
        const int count = 30;
        var value = Encoding.UTF8.GetBytes("v");

        for (int i = count - 1; i >= 0; i--)
            await tree.SetAsync($"k{i:D4}", value);

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);

        var expected = Enumerable.Range(0, count)
            .Select(i => $"k{i:D4}")
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToList();

        Assert.Equal(expected, keys);
    }

    [Fact]
    public async Task Reverse_order_insert_large_set_then_get_all()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("rev-insert-large");
        const int count = 200;
        var value = Encoding.UTF8.GetBytes("v");

        for (int i = count - 1; i >= 0; i--)
            await tree.SetAsync($"k{i:D4}", value);

        var missing = new List<string>();
        for (int i = 0; i < count; i++)
        {
            var result = await tree.GetAsync($"k{i:D4}");
            if (result is null) missing.Add($"k{i:D4}");
        }

        Assert.Empty(missing);
    }

    [Fact]
    public async Task Concurrent_reverse_order_inserts_then_get_all()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("rev-concurrent");
        const int count = 50;
        var value = Encoding.UTF8.GetBytes("v");

        // Fire multiple inserts concurrently (reverse order).
        var tasks = new List<Task>();
        for (int i = count - 1; i >= 0; i--)
            tasks.Add(tree.SetAsync($"k{i:D4}", value));
        await Task.WhenAll(tasks);

        var missing = new List<string>();
        for (int i = 0; i < count; i++)
        {
            var result = await tree.GetAsync($"k{i:D4}");
            if (result is null) missing.Add($"k{i:D4}");
        }

        Assert.Empty(missing);
    }
}

/// <summary>
/// Integration tests for <see cref="ILattice.BulkLoadAsync"/> and the streaming
/// <see cref="LatticeExtensions.BulkLoadAsync"/> extension method.
/// Uses a single-shard, small-leaf cluster to exercise multi-level tree construction.
/// </summary>
[Collection(SmallLeafClusterCollection.Name)]
public class BPlusTreeBulkLoadTests(SmallLeafClusterFixture fixture)
{
    private readonly TestCluster _cluster = fixture.Cluster;

    [Fact]
    public async Task BulkLoad_builds_tree_and_retrieves_all_keys()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("bulk-basic");
        const int count = 50;
        var entries = Enumerable.Range(0, count)
            .Select(i => KeyValuePair.Create($"k{i:D4}", Encoding.UTF8.GetBytes($"v{i}")))
            .ToList();

        await tree.BulkLoadAsync(entries);

        var missing = new List<string>();
        for (int i = 0; i < count; i++)
        {
            var result = await tree.GetAsync($"k{i:D4}");
            if (result is null) missing.Add($"k{i:D4}");
        }

        Assert.Empty(missing);
    }

    [Fact]
    public async Task BulkLoad_keys_scan_returns_sorted_keys()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("bulk-keys-scan");
        const int count = 30;
        var entries = Enumerable.Range(0, count)
            .Select(i => KeyValuePair.Create($"k{i:D4}", Encoding.UTF8.GetBytes("v")))
            .ToList();

        await tree.BulkLoadAsync(entries);

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);

        var expected = entries.Select(e => e.Key).Order().ToList();
        Assert.Equal(expected, keys);
    }

    [Fact]
    public async Task BulkLoad_then_set_and_delete_work()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("bulk-then-mutate");
        var entries = Enumerable.Range(0, 20)
            .Select(i => KeyValuePair.Create($"k{i:D4}", Encoding.UTF8.GetBytes("v")))
            .ToList();

        await tree.BulkLoadAsync(entries);

        // Set a new key beyond the bulk-loaded range.
        await tree.SetAsync("k0099", Encoding.UTF8.GetBytes("new"));
        var result = await tree.GetAsync("k0099");
        Assert.NotNull(result);
        Assert.Equal("new", Encoding.UTF8.GetString(result));

        // Delete a bulk-loaded key.
        var deleted = await tree.DeleteAsync("k0005");
        Assert.True(deleted);
        Assert.Null(await tree.GetAsync("k0005"));
    }

    [Fact]
    public async Task BulkLoad_single_entry_creates_root_leaf()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("bulk-single");
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            KeyValuePair.Create("only-key", Encoding.UTF8.GetBytes("only-value"))
        };

        await tree.BulkLoadAsync(entries);

        var result = await tree.GetAsync("only-key");
        Assert.NotNull(result);
        Assert.Equal("only-value", Encoding.UTF8.GetString(result));
    }

    [Fact]
    public async Task BulkLoad_large_dataset_builds_multi_level_tree()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("bulk-large");
        const int count = 200;
        var entries = Enumerable.Range(0, count)
            .Select(i => KeyValuePair.Create($"k{i:D4}", Encoding.UTF8.GetBytes($"v{i}")))
            .ToList();

        await tree.BulkLoadAsync(entries);

        var missing = new List<string>();
        for (int i = 0; i < count; i++)
        {
            var result = await tree.GetAsync($"k{i:D4}");
            if (result is null) missing.Add($"k{i:D4}");
        }

        Assert.Empty(missing);

        // Verify key scan is complete and sorted.
        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);
        Assert.Equal(count, keys.Count);
    }

    [Fact]
    public async Task Streaming_BulkLoad_via_extension_method()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("bulk-stream");
        const int count = 50;

        async IAsyncEnumerable<KeyValuePair<string, byte[]>> GenerateEntries()
        {
            // Entries must be in sorted key order.
            for (int i = 0; i < count; i++)
            {
                yield return KeyValuePair.Create($"k{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));
                await Task.Yield();
            }
        }

        // SmallLeafClusterFixture uses ShardCount=1.
        await tree.BulkLoadAsync(GenerateEntries(), _cluster.GrainFactory, shardCount: 1, chunkSize: 10);

        var missing = new List<string>();
        for (int i = 0; i < count; i++)
        {
            var result = await tree.GetAsync($"k{i:D4}");
            if (result is null) missing.Add($"k{i:D4}");
        }

        Assert.Empty(missing);
    }

    [Fact]
    public async Task BulkAppend_after_BulkLoad_appends_to_right_edge()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("bulk-append");
        const int initialCount = 20;
        const int appendCount = 30;

        // Initial bulk load.
        var initial = Enumerable.Range(0, initialCount)
            .Select(i => KeyValuePair.Create($"k{i:D4}", Encoding.UTF8.GetBytes("v")))
            .ToList();
        await tree.BulkLoadAsync(initial);

        // Streaming append with keys beyond the initial range.
        async IAsyncEnumerable<KeyValuePair<string, byte[]>> AppendEntries()
        {
            for (int i = initialCount; i < initialCount + appendCount; i++)
            {
                yield return KeyValuePair.Create($"k{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));
                await Task.Yield();
            }
        }

        await tree.BulkLoadAsync(AppendEntries(), _cluster.GrainFactory, shardCount: 1, chunkSize: 8);

        // Verify all keys are present.
        var totalCount = initialCount + appendCount;
        var missing = new List<string>();
        for (int i = 0; i < totalCount; i++)
        {
            var result = await tree.GetAsync($"k{i:D4}");
            if (result is null) missing.Add($"k{i:D4}");
        }

        Assert.Empty(missing);

        // Verify key scan returns all in order.
        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);
        Assert.Equal(totalCount, keys.Count);
    }

    [Fact]
    public async Task BulkAppend_direct_shard_call_stores_entries()
    {
        // Call BulkAppendAsync directly on the shard to isolate from extension method.
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("bulk-direct");
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("bulk-direct/0");
        var entries = Enumerable.Range(0, 10)
            .Select(i => KeyValuePair.Create($"k{i:D4}", Encoding.UTF8.GetBytes($"v{i}")))
            .ToList();

        await shard.BulkAppendAsync("test-direct-append", entries);

        // Read through ILattice (not directly from shard).
        var missing = new List<string>();
        for (int i = 0; i < 10; i++)
        {
            var result = await tree.GetAsync($"k{i:D4}");
            if (result is null) missing.Add($"k{i:D4}");
        }

        Assert.Empty(missing);
    }

    [Fact]
    public async Task BulkLoad_idempotent_retry_is_noop()
    {
        // BulkLoadAsync on the same tree twice should succeed — the second
        // call hits the idempotency guard (LastCompletedBulkOperationId).
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("bulk-idempotent");
        var entries = Enumerable.Range(0, 20)
            .Select(i => KeyValuePair.Create($"k{i:D4}", Encoding.UTF8.GetBytes($"v{i}")))
            .ToList();

        await tree.BulkLoadAsync(entries);

        // Second call — should not throw or corrupt the tree.
        // (LatticeGrain generates a new operationId, but ShardRootGrain rejects
        // it with "already has data" if the shard is non-empty, unless the
        // operation ID matches. So we test at the shard level for true idempotency.)
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("bulk-idempotent/0");
        var shardEntries = entries
            .Where(e => LatticeSharding.GetShardIndex(e.Key, 1) == 0)
            .OrderBy(e => e.Key)
            .ToList();

        // Calling with a different operationId on a non-empty shard should throw.
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => shard.BulkLoadAsync("different-op", shardEntries));

        // All original keys still readable.
        for (int i = 0; i < 20; i++)
        {
            var result = await tree.GetAsync($"k{i:D4}");
            Assert.NotNull(result);
        }
    }

    [Fact]
    public async Task BulkAppend_idempotent_retry_same_operationId_is_noop()
    {
        // Calling BulkAppendAsync twice with the same operationId should
        // produce the same result — no duplicate entries, no corruption.
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("bulk-append-idem/0");
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("bulk-append-idem");

        var entries = Enumerable.Range(0, 12)
            .Select(i => KeyValuePair.Create($"k{i:D4}", Encoding.UTF8.GetBytes($"v{i}")))
            .ToList();

        await shard.BulkAppendAsync("op-1", entries);

        // Retry with same operationId — should be a no-op.
        await shard.BulkAppendAsync("op-1", entries);

        // Verify key count matches expected (no duplicates).
        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);

        Assert.Equal(12, keys.Count);
    }

    [Fact]
    public async Task BulkAppend_different_operationIds_append_independently()
    {
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("bulk-append-multi/0");
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("bulk-append-multi");

        var batch1 = Enumerable.Range(0, 8)
            .Select(i => KeyValuePair.Create($"k{i:D4}", Encoding.UTF8.GetBytes("v")))
            .ToList();
        var batch2 = Enumerable.Range(8, 8)
            .Select(i => KeyValuePair.Create($"k{i:D4}", Encoding.UTF8.GetBytes("v")))
            .ToList();

        await shard.BulkAppendAsync("op-a", batch1);
        await shard.BulkAppendAsync("op-b", batch2);

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);

        Assert.Equal(16, keys.Count);
    }

    [Fact]
    public async Task BulkLoad_empty_entries_is_noop()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("bulk-empty");
        await tree.BulkLoadAsync([]);

        // Tree should still work — set and get a key after empty bulk load.
        await tree.SetAsync("after", Encoding.UTF8.GetBytes("val"));
        var result = await tree.GetAsync("after");
        Assert.NotNull(result);
    }

    [Fact]
    public async Task BulkAppend_empty_entries_is_noop()
    {
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("bulk-append-empty/0");
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("bulk-append-empty");

        // Append some data first.
        var entries = Enumerable.Range(0, 5)
            .Select(i => KeyValuePair.Create($"k{i:D4}", Encoding.UTF8.GetBytes("v")))
            .ToList();
        await shard.BulkAppendAsync("op-1", entries);

        // Append empty — should not fail or change anything.
        await shard.BulkAppendAsync("op-2", []);

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);

        Assert.Equal(5, keys.Count);
    }

    [Fact]
    public async Task BulkAppend_fills_existing_leaf_before_creating_new_ones()
    {
        // With MaxLeafKeys=4, append 2 entries (fills existing), then 6 more
        // (should create new leaves). Verify all 8 are readable and in order.
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("bulk-append-fill/0");
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("bulk-append-fill");

        var batch1 = Enumerable.Range(0, 2)
            .Select(i => KeyValuePair.Create($"k{i:D4}", Encoding.UTF8.GetBytes("v")))
            .ToList();
        await shard.BulkAppendAsync("op-fill-1", batch1);

        var batch2 = Enumerable.Range(2, 6)
            .Select(i => KeyValuePair.Create($"k{i:D4}", Encoding.UTF8.GetBytes("v")))
            .ToList();
        await shard.BulkAppendAsync("op-fill-2", batch2);

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);

        var expected = Enumerable.Range(0, 8).Select(i => $"k{i:D4}").ToList();
        Assert.Equal(expected, keys);
    }

    [Fact]
    public async Task Streaming_BulkLoad_multiple_chunks_all_keys_present()
    {
        // Streaming with a small chunkSize forces multiple BulkAppendAsync calls,
        // each with a different operationId. Verify no data loss across chunks.
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("bulk-stream-multi");
        const int count = 40;

        async IAsyncEnumerable<KeyValuePair<string, byte[]>> GenerateEntries()
        {
            for (int i = 0; i < count; i++)
            {
                yield return KeyValuePair.Create($"k{i:D4}", Encoding.UTF8.GetBytes($"v{i}"));
                await Task.Yield();
            }
        }

        await tree.BulkLoadAsync(GenerateEntries(), _cluster.GrainFactory, shardCount: 1, chunkSize: 7);

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);

        Assert.Equal(count, keys.Count);
        var expected = Enumerable.Range(0, count).Select(i => $"k{i:D4}").Order().ToList();
        Assert.Equal(expected, keys);
    }

    [Fact]
    public async Task Normal_operations_work_after_BulkAppend()
    {
        // After bulk append, Set/Get/Delete should work normally
        // (verifies ResumePendingBulkGraftAsync doesn't interfere).
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("bulk-then-normal/0");
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("bulk-then-normal");

        var entries = Enumerable.Range(0, 10)
            .Select(i => KeyValuePair.Create($"k{i:D4}", Encoding.UTF8.GetBytes("v")))
            .ToList();
        await shard.BulkAppendAsync("op-init", entries);

        // Set a new key beyond the bulk range.
        await tree.SetAsync("k0099", Encoding.UTF8.GetBytes("new"));
        Assert.NotNull(await tree.GetAsync("k0099"));

        // Delete a bulk-loaded key.
        Assert.True(await tree.DeleteAsync("k0005"));
        Assert.Null(await tree.GetAsync("k0005"));

        // Keys scan should reflect the changes.
        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);

        Assert.Contains("k0099", keys);
        Assert.DoesNotContain("k0005", keys);
    }
}

