using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration tests for <see cref="ILattice.KeysAsync"/> using the default cluster
/// (default shard count, default max leaf keys — no splits expected for small data sets).
/// </summary>
[Collection(ClusterCollection.Name)]
public class KeysDefaultClusterTests(ClusterFixture fixture)
{
    private readonly TestCluster _cluster = fixture.Cluster;

    [Fact]
    public async Task Keys_empty_tree_returns_nothing()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("keys-empty");
        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);

        Assert.Empty(keys);
    }

    [Fact]
    public async Task Keys_returns_all_keys_sorted()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("keys-sorted");
        var expected = new[] { "alpha", "bravo", "charlie", "delta", "echo" };
        foreach (var k in expected)
            await tree.SetAsync(k, Encoding.UTF8.GetBytes(k));

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);

        Assert.Equal(expected.OrderBy(k => k, StringComparer.Ordinal).ToList(), keys);
    }

    [Fact]
    public async Task Keys_reverse_returns_descending_order()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("keys-reverse");
        var items = new[] { "a", "b", "c", "d", "e" };
        foreach (var k in items)
            await tree.SetAsync(k, Encoding.UTF8.GetBytes(k));

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync(reverse: true))
            keys.Add(k);

        Assert.Equal(
            items.OrderByDescending(k => k, StringComparer.Ordinal).ToList(),
            keys);
    }

    [Fact]
    public async Task Keys_range_filters_correctly()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("keys-range");
        var items = new[] { "a", "b", "c", "d", "e", "f" };
        foreach (var k in items)
            await tree.SetAsync(k, Encoding.UTF8.GetBytes(k));

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync(startInclusive: "b", endExclusive: "e"))
            keys.Add(k);

        Assert.Equal(["b", "c", "d"], keys);
    }

    [Fact]
    public async Task Keys_range_reverse_returns_filtered_descending()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("keys-range-rev");
        var items = new[] { "a", "b", "c", "d", "e", "f" };
        foreach (var k in items)
            await tree.SetAsync(k, Encoding.UTF8.GetBytes(k));

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync(startInclusive: "b", endExclusive: "e", reverse: true))
            keys.Add(k);

        Assert.Equal(["d", "c", "b"], keys);
    }

    [Fact]
    public async Task Keys_excludes_deleted_keys()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("keys-deleted");
        await tree.SetAsync("keep", Encoding.UTF8.GetBytes("v"));
        await tree.SetAsync("remove", Encoding.UTF8.GetBytes("v"));
        await tree.DeleteAsync("remove");

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);

        Assert.Equal(["keep"], keys);
    }
}

/// <summary>
/// Integration tests for <see cref="ILattice.KeysAsync"/> using a single-shard cluster
/// with small leaf keys (max 4) to exercise leaf splits within a single shard.
/// </summary>
[Collection(SmallLeafClusterCollection.Name)]
public class KeysSingleShardSplitTests(SmallLeafClusterFixture fixture)
{
    private readonly TestCluster _cluster = fixture.Cluster;

    [Fact]
    public async Task Keys_after_splits_returns_all_keys_sorted()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("1s-keys-split");

        // Insert enough keys to force multiple leaf splits (max 4 keys/leaf).
        var expected = Enumerable.Range(0, 20)
            .Select(i => $"k-{i:D4}")
            .ToList();

        foreach (var k in expected)
            await tree.SetAsync(k, Encoding.UTF8.GetBytes(k));

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);

        expected.Sort(StringComparer.Ordinal);
        Assert.Equal(expected, keys);
    }

    [Fact]
    public async Task Keys_range_works_across_split_leaves()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("1s-keys-range");

        for (int i = 0; i < 20; i++)
            await tree.SetAsync($"k-{i:D4}", Encoding.UTF8.GetBytes($"v-{i}"));

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync(startInclusive: "k-0005", endExclusive: "k-0015"))
            keys.Add(k);

        var expected = Enumerable.Range(5, 10)
            .Select(i => $"k-{i:D4}")
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToList();

        Assert.Equal(expected, keys);
    }

    [Fact]
    public async Task Keys_reverse_works_across_split_leaves()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("1s-keys-rev");

        for (int i = 0; i < 20; i++)
            await tree.SetAsync($"k-{i:D4}", Encoding.UTF8.GetBytes($"v-{i}"));

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync(reverse: true))
            keys.Add(k);

        var expected = Enumerable.Range(0, 20)
            .Select(i => $"k-{i:D4}")
            .OrderByDescending(k => k, StringComparer.Ordinal)
            .ToList();

        Assert.Equal(expected, keys);
    }

    [Fact]
    public async Task Keys_reverse_range_across_split_leaves()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("1s-keys-rev-range");

        for (int i = 0; i < 20; i++)
            await tree.SetAsync($"k-{i:D4}", Encoding.UTF8.GetBytes($"v-{i}"));

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync(startInclusive: "k-0005", endExclusive: "k-0015", reverse: true))
            keys.Add(k);

        var expected = Enumerable.Range(5, 10)
            .Select(i => $"k-{i:D4}")
            .OrderByDescending(k => k, StringComparer.Ordinal)
            .ToList();

        Assert.Equal(expected, keys);
    }

    [Fact]
    public async Task Keys_forward_and_reverse_are_mirrors()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("1s-keys-mirror");

        for (int i = 0; i < 30; i++)
            await tree.SetAsync($"m-{i:D4}", Encoding.UTF8.GetBytes($"v-{i}"));

        var forward = new List<string>();
        await foreach (var k in tree.KeysAsync())
            forward.Add(k);

        var reverse = new List<string>();
        await foreach (var k in tree.KeysAsync(reverse: true))
            reverse.Add(k);

        forward.Reverse();
        Assert.Equal(forward, reverse);
    }
}

/// <summary>
/// Integration tests for <see cref="ILattice.KeysAsync"/> using a 4-shard cluster
/// with small leaf keys to exercise cross-shard merging and leaf splits.
/// </summary>
[Collection(FourShardClusterCollection.Name)]
public class KeysFourShardTests(FourShardClusterFixture fixture)
{
    private readonly TestCluster _cluster = fixture.Cluster;

    [Fact]
    public async Task Keys_merges_across_shards_sorted()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("4s-merge");

        var expected = Enumerable.Range(0, 50)
            .Select(i => $"item-{i:D4}")
            .ToList();

        foreach (var k in expected)
            await tree.SetAsync(k, Encoding.UTF8.GetBytes(k));

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);

        expected.Sort(StringComparer.Ordinal);
        Assert.Equal(expected, keys);
    }

    [Fact]
    public async Task Keys_range_across_shards()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("4s-range");

        for (int i = 0; i < 50; i++)
            await tree.SetAsync($"item-{i:D4}", Encoding.UTF8.GetBytes($"v-{i}"));

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync(startInclusive: "item-0010", endExclusive: "item-0030"))
            keys.Add(k);

        var expected = Enumerable.Range(10, 20)
            .Select(i => $"item-{i:D4}")
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToList();

        Assert.Equal(expected, keys);
    }

    [Fact]
    public async Task Keys_reverse_across_shards()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("4s-reverse");

        for (int i = 0; i < 50; i++)
            await tree.SetAsync($"item-{i:D4}", Encoding.UTF8.GetBytes($"v-{i}"));

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync(reverse: true))
            keys.Add(k);

        var expected = Enumerable.Range(0, 50)
            .Select(i => $"item-{i:D4}")
            .OrderByDescending(k => k, StringComparer.Ordinal)
            .ToList();

        Assert.Equal(expected, keys);
    }

    [Fact]
    public async Task Keys_many_keys_forces_pagination()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("4s-pagination");

        // 200 keys across 4 shards with max 4 leaf keys — forces many splits
        // and multiple pages per shard.
        var expected = Enumerable.Range(0, 200)
            .Select(i => $"pg-{i:D4}")
            .ToList();

        foreach (var k in expected)
            await tree.SetAsync(k, Encoding.UTF8.GetBytes(k));

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);

        expected.Sort(StringComparer.Ordinal);
        Assert.Equal(expected, keys);
    }

    [Fact]
    public async Task Keys_reverse_many_keys_forces_pagination()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("4s-rev-pagination");

        var expected = Enumerable.Range(0, 200)
            .Select(i => $"rp-{i:D4}")
            .ToList();

        foreach (var k in expected)
            await tree.SetAsync(k, Encoding.UTF8.GetBytes(k));

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync(reverse: true))
            keys.Add(k);

        expected.Sort(StringComparer.Ordinal);
        expected.Reverse();
        Assert.Equal(expected, keys);
    }

    [Fact]
    public async Task Keys_reverse_range_across_shards()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("4s-rev-range");

        for (int i = 0; i < 50; i++)
            await tree.SetAsync($"rr-{i:D4}", Encoding.UTF8.GetBytes($"v-{i}"));

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync(startInclusive: "rr-0010", endExclusive: "rr-0030", reverse: true))
            keys.Add(k);

        var expected = Enumerable.Range(10, 20)
            .Select(i => $"rr-{i:D4}")
            .OrderByDescending(k => k, StringComparer.Ordinal)
            .ToList();

        Assert.Equal(expected, keys);
    }
}
