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

