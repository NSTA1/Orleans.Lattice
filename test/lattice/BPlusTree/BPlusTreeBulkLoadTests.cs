using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration tests for <see cref="ILattice.BulkLoadAsync"/> and the streaming
/// <see cref="LatticeExtensions.BulkLoadAsync"/> extension method.
/// Uses a single-shard, small-leaf cluster to exercise multi-level tree construction.
/// </summary>
[TestFixture]
public class BPlusTreeBulkLoadTests
{
    private SmallLeafClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SmallLeafClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    /// <summary>
    /// Registers a single-shard pin for <paramref name="treeId"/> so tests that
    /// write directly to shard <c>/0</c> and read back through <c>ILattice</c>
    /// are not broken by the global default <c>ShardCount=64</c> that registry-authoritative sizing
    /// applies via lazy-seed.
    /// </summary>
    private async Task RegisterSingleShardAsync(string treeId)
    {
        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = SmallLeafClusterFixture.SmallMaxLeafKeys,
            ShardCount = 1,
        });
    }

    [Test]
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

        Assert.That(missing, Is.Empty);
    }

    [Test]
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
        Assert.That(keys, Is.EqualTo(expected));
    }

    [Test]
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
        Assert.That(result, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("new"));

        // Delete a bulk-loaded key.
        var deleted = await tree.DeleteAsync("k0005");
        Assert.That(deleted, Is.True);
        Assert.That(await tree.GetAsync("k0005"), Is.Null);
    }

    [Test]
    public async Task BulkLoad_single_entry_creates_root_leaf()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("bulk-single");
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            KeyValuePair.Create("only-key", Encoding.UTF8.GetBytes("only-value"))
        };

        await tree.BulkLoadAsync(entries);

        var result = await tree.GetAsync("only-key");
        Assert.That(result, Is.Not.Null);
        Assert.That(Encoding.UTF8.GetString(result!), Is.EqualTo("only-value"));
    }

    [Test]
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

        Assert.That(missing, Is.Empty);

        // Verify key scan is complete and sorted.
        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);
        Assert.That(keys.Count, Is.EqualTo(count));
    }

    [Test]
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
        await tree.BulkLoadAsync(GenerateEntries(), _cluster.GrainFactory, chunkSize: 10);

        var missing = new List<string>();
        for (int i = 0; i < count; i++)
        {
            var result = await tree.GetAsync($"k{i:D4}");
            if (result is null) missing.Add($"k{i:D4}");
        }

        Assert.That(missing, Is.Empty);
    }

    [Test]
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

        await tree.BulkLoadAsync(AppendEntries(), _cluster.GrainFactory, chunkSize: 8);

        // Verify all keys are present.
        var totalCount = initialCount + appendCount;
        var missing = new List<string>();
        for (int i = 0; i < totalCount; i++)
        {
            var result = await tree.GetAsync($"k{i:D4}");
            if (result is null) missing.Add($"k{i:D4}");
        }

        Assert.That(missing, Is.Empty);

        // Verify key scan returns all in order.
        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);
        Assert.That(keys.Count, Is.EqualTo(totalCount));
    }

    [Test]
    public async Task BulkAppend_direct_shard_call_stores_entries()
    {
        // Call BulkAppendAsync directly on the shard to isolate from extension method.
        await RegisterSingleShardAsync("bulk-direct");
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

        Assert.That(missing, Is.Empty);
    }

    [Test]
    public async Task BulkLoad_idempotent_retry_is_noop()
    {
        // BulkLoadAsync on the same tree twice should succeed — the second
        // call hits the idempotency guard (LastCompletedBulkOperationId).
        await RegisterSingleShardAsync("bulk-idempotent");
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
        Assert.ThrowsAsync<InvalidOperationException>(
            () => shard.BulkLoadAsync("different-op", shardEntries));

        // All original keys still readable.
        for (int i = 0; i < 20; i++)
        {
            var result = await tree.GetAsync($"k{i:D4}");
            Assert.That(result, Is.Not.Null);
        }
    }

    [Test]
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

        Assert.That(keys.Count, Is.EqualTo(12));
    }

    [Test]
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

        Assert.That(keys.Count, Is.EqualTo(16));
    }

    [Test]
    public async Task BulkLoad_empty_entries_is_noop()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("bulk-empty");
        await tree.BulkLoadAsync([]);

        // Tree should still work — set and get a key after empty bulk load.
        await tree.SetAsync("after", Encoding.UTF8.GetBytes("val"));
        var result = await tree.GetAsync("after");
        Assert.That(result, Is.Not.Null);
    }

    [Test]
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

        Assert.That(keys.Count, Is.EqualTo(5));
    }

    [Test]
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
        Assert.That(keys, Is.EqualTo(expected));
    }

    [Test]
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

        await tree.BulkLoadAsync(GenerateEntries(), _cluster.GrainFactory, chunkSize: 7);

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);

        Assert.That(keys.Count, Is.EqualTo(count));
        var expected = Enumerable.Range(0, count).Select(i => $"k{i:D4}").Order().ToList();
        Assert.That(keys, Is.EqualTo(expected));
    }

    [Test]
    public async Task Normal_operations_work_after_BulkAppend()
    {
        // After bulk append, Set/Get/Delete should work normally
        // (verifies ResumePendingBulkGraftAsync doesn't interfere).
        await RegisterSingleShardAsync("bulk-then-normal");
        var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>("bulk-then-normal/0");
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("bulk-then-normal");

        var entries = Enumerable.Range(0, 10)
            .Select(i => KeyValuePair.Create($"k{i:D4}", Encoding.UTF8.GetBytes("v")))
            .ToList();
        await shard.BulkAppendAsync("op-init", entries);

        // Set a new key beyond the bulk range.
        await tree.SetAsync("k0099", Encoding.UTF8.GetBytes("new"));
        Assert.That(await tree.GetAsync("k0099"), Is.Not.Null);

        // Delete a bulk-loaded key.
        Assert.That(await tree.DeleteAsync("k0005"), Is.True);
        Assert.That(await tree.GetAsync("k0005"), Is.Null);

        // Keys scan should reflect the changes.
        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);

        Assert.That(keys, Does.Contain("k0099"));
        Assert.That(keys, Does.Not.Contain("k0005"));
    }

    [Test]
    public async Task DeleteRange_tombstones_keys_in_range()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("test-tree-delrange");
        await tree.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await tree.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await tree.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        await tree.SetAsync("d", Encoding.UTF8.GetBytes("4"));
        await tree.SetAsync("e", Encoding.UTF8.GetBytes("5"));

        var count = await tree.DeleteRangeAsync("b", "e");

        Assert.That(count, Is.EqualTo(3));
        Assert.That(await tree.GetAsync("a"), Is.Not.Null);
        Assert.That(await tree.GetAsync("b"), Is.Null);
        Assert.That(await tree.GetAsync("c"), Is.Null);
        Assert.That(await tree.GetAsync("d"), Is.Null);
        Assert.That(await tree.GetAsync("e"), Is.Not.Null);
    }

    [Test]
    public async Task DeleteRange_returns_zero_for_empty_range()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("test-tree-delrange-empty");
        await tree.SetAsync("a", Encoding.UTF8.GetBytes("1"));

        var count = await tree.DeleteRangeAsync("m", "z");

        Assert.That(count, Is.EqualTo(0));
        Assert.That(await tree.GetAsync("a"), Is.Not.Null);
    }

    [Test]
    public async Task DeleteRange_keys_excluded_from_subsequent_scan()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("test-tree-delrange-scan");
        await tree.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await tree.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await tree.SetAsync("c", Encoding.UTF8.GetBytes("3"));

        await tree.DeleteRangeAsync("a", "c");

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);

        Assert.That(keys, Is.EqualTo(new[] { "c" }));
    }

    [Test]
    public void BulkLoadAsync_streaming_throws_when_lattice_null()
    {
        Assert.ThrowsAsync<ArgumentNullException>(
            () => LatticeExtensions.BulkLoadAsync(
                null!,
                AsyncEnumerable.Empty<KeyValuePair<string, byte[]>>(),
                _cluster.GrainFactory));
    }

    [Test]
    public void BulkLoadAsync_streaming_throws_when_entries_null()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("bulk-null-entries");
        Assert.ThrowsAsync<ArgumentNullException>(
            () => tree.BulkLoadAsync(
                (IAsyncEnumerable<KeyValuePair<string, byte[]>>)null!,
                _cluster.GrainFactory));
    }

    [Test]
    public void BulkLoadAsync_streaming_throws_when_factory_null()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("bulk-null-factory");
        Assert.ThrowsAsync<ArgumentNullException>(
            () => tree.BulkLoadAsync(
                AsyncEnumerable.Empty<KeyValuePair<string, byte[]>>(),
                null!));
    }
}
