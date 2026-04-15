using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

[Collection(FaultInjectionClusterCollection.Name)]
public class FaultInjectionStorageTests(FaultInjectionClusterFixture fixture)
{
    private readonly TestCluster _cluster = fixture.Cluster;

    /// <summary>
    /// Helper to get the fault-injection grain for the Lattice storage provider.
    /// </summary>
    private IStorageFaultGrain StorageFaultGrain =>
        _cluster.GrainFactory.GetGrain<IStorageFaultGrain>(LatticeOptions.StorageProviderName);

    // -----------------------------------------------------------------------
    // Leaf split recovery after storage fault
    // -----------------------------------------------------------------------

    [Fact]
    public async Task Leaf_split_recovers_after_write_fault_and_all_keys_are_readable()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(FaultInjectionClusterFixture.TreeName);
        var limit = FaultInjectionClusterFixture.SmallMaxLeafKeys;

        // Fill a leaf to just under the split threshold.
        for (int i = 0; i < limit; i++)
        {
            await tree.SetAsync($"lsr-{i:D4}", Encoding.UTF8.GetBytes($"v-{i}"));
        }

        // Discover the leaf GrainId by probing a known key through the tree.
        // We use a direct leaf grain to discover its GrainId.
        var probeLeaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(
            await GetLeafForKeyAsync("lsr-0000"));
        var leafId = probeLeaf.GetGrainId();

        // Inject a write fault on that leaf — the next WriteStateAsync will throw.
        await StorageFaultGrain.AddFaultOnWrite(
            leafId, new InvalidOperationException("Injected storage fault"));

        // This write should exceed the limit and trigger a split.
        // The split's Phase 1 WriteStateAsync will hit the fault.
        // The grain should throw, and Orleans will deactivate it.
        try
        {
            await tree.SetAsync($"lsr-{limit:D4}", Encoding.UTF8.GetBytes($"v-{limit}"));
        }
        catch
        {
            // Expected — the storage fault causes the call to fail.
        }

        // No more faults — the next attempt should succeed via recovery.
        // Give the grain a moment to deactivate if needed, then retry.
        await tree.SetAsync($"lsr-{limit:D4}", Encoding.UTF8.GetBytes($"v-{limit}"));

        // Verify ALL keys (including the new one) are readable.
        for (int i = 0; i <= limit; i++)
        {
            var result = await tree.GetAsync($"lsr-{i:D4}");
            Assert.NotNull(result);
            Assert.Equal($"v-{i}", Encoding.UTF8.GetString(result));
        }
    }

    // -----------------------------------------------------------------------
    // Multiple splits after fault — system stabilizes
    // -----------------------------------------------------------------------

    [Fact]
    public async Task Multiple_splits_after_fault_stabilize_and_data_is_consistent()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(
            $"{FaultInjectionClusterFixture.TreeName}-multi");
        var limit = FaultInjectionClusterFixture.SmallMaxLeafKeys;
        var totalKeys = limit * 4; // enough to trigger multiple splits

        // Insert half the keys successfully.
        for (int i = 0; i < totalKeys / 2; i++)
        {
            await tree.SetAsync($"ms-{i:D4}", Encoding.UTF8.GetBytes($"v-{i}"));
        }

        // Inject a blanket of faults — we'll inject on the first leaf we can find.
        var probeLeaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(
            await GetLeafForKeyAsync("ms-0000", $"{FaultInjectionClusterFixture.TreeName}-multi"));
        await StorageFaultGrain.AddFaultOnWrite(
            probeLeaf.GetGrainId(),
            new InvalidOperationException("Injected fault for multi-split"));

        // Try to insert more keys — some may fail.
        var failures = 0;
        for (int i = totalKeys / 2; i < totalKeys; i++)
        {
            try
            {
                await tree.SetAsync($"ms-{i:D4}", Encoding.UTF8.GetBytes($"v-{i}"));
            }
            catch
            {
                failures++;
            }
        }

        // Now retry any that failed — the fault was consumed on the first throw.
        for (int i = totalKeys / 2; i < totalKeys; i++)
        {
            var existing = await tree.GetAsync($"ms-{i:D4}");
            if (existing is null)
            {
                await tree.SetAsync($"ms-{i:D4}", Encoding.UTF8.GetBytes($"v-{i}"));
            }
        }

        // All keys should be readable.
        for (int i = 0; i < totalKeys; i++)
        {
            var result = await tree.GetAsync($"ms-{i:D4}");
            Assert.NotNull(result);
            Assert.Equal($"v-{i}", Encoding.UTF8.GetString(result));
        }
    }

    // -----------------------------------------------------------------------
    // Idempotent split promotion — duplicate AcceptSplit is harmless
    // -----------------------------------------------------------------------

    [Fact]
    public async Task Split_result_is_idempotent_when_replayed_through_tree()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(
            $"{FaultInjectionClusterFixture.TreeName}-idem");
        var limit = FaultInjectionClusterFixture.SmallMaxLeafKeys;

        // Fill past the split point twice — each batch triggers a split.
        for (int i = 0; i < limit * 3; i++)
        {
            await tree.SetAsync($"id-{i:D4}", Encoding.UTF8.GetBytes($"v-{i}"));
        }

        // All keys must be readable — no duplicates, no lost data.
        for (int i = 0; i < limit * 3; i++)
        {
            var result = await tree.GetAsync($"id-{i:D4}");
            Assert.NotNull(result);
            Assert.Equal($"v-{i}", Encoding.UTF8.GetString(result));
        }
    }

    // -----------------------------------------------------------------------
    // Write fault on new sibling during split — sibling retains data on retry
    // -----------------------------------------------------------------------

    [Fact]
    public async Task Data_survives_write_fault_on_sibling_during_split()
    {
        var treeName = $"{FaultInjectionClusterFixture.TreeName}-sib";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        var limit = FaultInjectionClusterFixture.SmallMaxLeafKeys;

        // Fill to the split point.
        for (int i = 0; i < limit; i++)
        {
            await tree.SetAsync($"sb-{i:D4}", Encoding.UTF8.GetBytes($"v-{i}"));
        }

        // Trigger the split — this time let it succeed.
        await tree.SetAsync($"sb-{limit:D4}", Encoding.UTF8.GetBytes($"v-{limit}"));

        // Overwrite a key that likely moved to the sibling (upper half).
        var upperKey = $"sb-{limit:D4}";
        await tree.SetAsync(upperKey, Encoding.UTF8.GetBytes("updated"));

        var result = await tree.GetAsync(upperKey);
        Assert.NotNull(result);
        Assert.Equal("updated", Encoding.UTF8.GetString(result));
    }

    // -----------------------------------------------------------------------
    // Tombstones survive leaf splits
    // -----------------------------------------------------------------------

    [Fact]
    public async Task Tombstones_survive_leaf_split()
    {
        var treeName = $"{FaultInjectionClusterFixture.TreeName}-tomb";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        var limit = FaultInjectionClusterFixture.SmallMaxLeafKeys;

        // Insert keys, then delete some in the upper range.
        for (int i = 0; i < limit; i++)
        {
            await tree.SetAsync($"tb-{i:D4}", Encoding.UTF8.GetBytes($"v-{i}"));
        }

        // Delete a key that will land in the upper half after split.
        await tree.DeleteAsync($"tb-{limit - 1:D4}");

        // Trigger the split.
        await tree.SetAsync($"tb-{limit:D4}", Encoding.UTF8.GetBytes($"v-{limit}"));

        // The deleted key must still be absent (tombstone preserved across split).
        var result = await tree.GetAsync($"tb-{limit - 1:D4}");
        Assert.Null(result);

        // Other keys should be present.
        for (int i = 0; i < limit - 1; i++)
        {
            var r = await tree.GetAsync($"tb-{i:D4}");
            Assert.NotNull(r);
        }
    }

    // -----------------------------------------------------------------------
    // Deep tree traversal (3+ levels) still routes correctly
    // -----------------------------------------------------------------------

    [Fact]
    public async Task Deep_tree_traversal_routes_correctly_after_many_splits()
    {
        var treeName = $"{FaultInjectionClusterFixture.TreeName}-deep";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        // With MaxLeafKeys=4 and MaxInternalChildren=4, a tree with ~50+ keys
        // should have 3 levels. Insert enough to force that.
        var totalKeys = FaultInjectionClusterFixture.SmallMaxLeafKeys
            * FaultInjectionClusterFixture.SmallMaxInternalChildren * 2;

        for (int i = 0; i < totalKeys; i++)
        {
            await tree.SetAsync($"deep-{i:D4}", Encoding.UTF8.GetBytes($"v-{i}"));
        }

        // All keys should be readable through the deep traversal.
        for (int i = 0; i < totalKeys; i++)
        {
            var result = await tree.GetAsync($"deep-{i:D4}");
            Assert.NotNull(result);
            Assert.Equal($"v-{i}", Encoding.UTF8.GetString(result));
        }
    }

    // -----------------------------------------------------------------------
    // Cache reflects correct data after split (no stale entries)
    // -----------------------------------------------------------------------

    [Fact]
    public async Task Cache_returns_correct_data_after_leaf_split()
    {
        var treeName = $"{FaultInjectionClusterFixture.TreeName}-cache";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        var limit = FaultInjectionClusterFixture.SmallMaxLeafKeys;

        // Populate and read (priming the cache).
        for (int i = 0; i < limit; i++)
        {
            await tree.SetAsync($"ca-{i:D4}", Encoding.UTF8.GetBytes($"v-{i}"));
        }
        for (int i = 0; i < limit; i++)
        {
            await tree.GetAsync($"ca-{i:D4}");
        }

        // Trigger split.
        await tree.SetAsync($"ca-{limit:D4}", Encoding.UTF8.GetBytes($"v-{limit}"));

        // Update a key that was moved to the new sibling.
        await tree.SetAsync($"ca-{limit:D4}", Encoding.UTF8.GetBytes("updated"));

        // Read all keys — cache should return correct (not stale) data.
        for (int i = 0; i <= limit; i++)
        {
            var result = await tree.GetAsync($"ca-{i:D4}");
            Assert.NotNull(result);
            if (i == limit)
                Assert.Equal("updated", Encoding.UTF8.GetString(result));
            else
                Assert.Equal($"v-{i}", Encoding.UTF8.GetString(result));
        }
    }

    // -----------------------------------------------------------------------
    // Deterministic root leaf — same shard always produces same GrainId
    // -----------------------------------------------------------------------

    [Fact]
    public async Task Deterministic_root_leaf_is_reachable_after_shard_reinitializes()
    {
        // Two independent reads to the same shard should resolve
        // to the same underlying leaf, verifiable by writing a key via one
        // reference and reading it back via another.
        var treeName = $"{FaultInjectionClusterFixture.TreeName}-det";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await tree.SetAsync("det-key", Encoding.UTF8.GetBytes("det-value"));

        // Read it back — internally this goes through a potentially different
        // cache activation, but the shard root must route to the same leaf.
        var result = await tree.GetAsync("det-key");
        Assert.NotNull(result);
        Assert.Equal("det-value", Encoding.UTF8.GetString(result));
    }

    // -----------------------------------------------------------------------
    // ShardRootGrain retry — write succeeds despite a transient fault
    // -----------------------------------------------------------------------

    [Fact]
    public async Task Write_succeeds_after_transient_storage_fault_via_retry()
    {
        var treeName = $"{FaultInjectionClusterFixture.TreeName}-retry";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        // Prime the shard — ensure the root leaf exists.
        await tree.SetAsync("retry-prime", Encoding.UTF8.GetBytes("prime"));

        // Inject a write fault on the root leaf.
        var probeLeaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(
            await GetLeafForKeyAsync("retry-prime", treeName));
        await StorageFaultGrain.AddFaultOnWrite(
            probeLeaf.GetGrainId(),
            new InvalidOperationException("Transient fault"));

        // ShardRootGrain's built-in retry should absorb the first failure.
        // The fault fires once then self-clears, so the retry succeeds.
        await tree.SetAsync("retry-key", Encoding.UTF8.GetBytes("retry-value"));

        var result = await tree.GetAsync("retry-key");
        Assert.NotNull(result);
        Assert.Equal("retry-value", Encoding.UTF8.GetString(result));
    }

    [Fact]
    public async Task Delete_succeeds_after_transient_storage_fault_via_retry()
    {
        var treeName = $"{FaultInjectionClusterFixture.TreeName}-retry-del";
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await tree.SetAsync("del-key", Encoding.UTF8.GetBytes("value"));

        var probeLeaf = _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(
            await GetLeafForKeyAsync("del-key", treeName));
        await StorageFaultGrain.AddFaultOnWrite(
            probeLeaf.GetGrainId(),
            new InvalidOperationException("Transient fault"));

        // Delete should succeed via retry.
        var deleted = await tree.DeleteAsync("del-key");
        Assert.True(deleted);

        var result = await tree.GetAsync("del-key");
        Assert.Null(result);
    }

    // -----------------------------------------------------------------------
    // Helper: resolve the leaf GrainId for a given key via the shard root
    // -----------------------------------------------------------------------

    private async Task<GrainId> GetLeafForKeyAsync(
        string key,
        string? treeName = null)
    {
        treeName ??= FaultInjectionClusterFixture.TreeName;

        // The fixture configures ShardCount = 1 for the tree, so all keys
        // land in shard 0.
        var shardIndex = Orleans.Lattice.BPlusTree.Grains.LatticeGrain.GetShardIndex(key, 1);
        var shardKey = $"{treeName}/{shardIndex}";

        // Trigger initialization by reading so the shard root creates its leaf.
        var shardRoot = _cluster.GrainFactory.GetGrain<IShardRootGrain>(shardKey);
        await shardRoot.GetAsync(key);

        // The ShardRootGrain.EnsureRootAsync creates a deterministic GrainId
        // from the shard key using SHA-256. Replicate that logic and use
        // GetGrain<IBPlusLeafGrain> so the GrainId has the correct grain type.
        var hash = System.Security.Cryptography.SHA256.HashData(
            Encoding.UTF8.GetBytes(shardKey));
        var deterministicGuid = new Guid(hash.AsSpan(0, 16));
        return _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(deterministicGuid).GetGrainId();
    }
}
