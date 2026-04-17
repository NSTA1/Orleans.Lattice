using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration tests that insert keys in non-ascending order using a single-shard,
/// small-leaf cluster to force many splits and expose routing bugs.
/// </summary>
[TestFixture]
public class BPlusTreeInsertionOrderTests
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

    [Test]
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

        Assert.That(missing, Is.Empty);
    }

    [Test]
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

        Assert.That(missing, Is.Empty);
    }

    [Test]
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

        Assert.That(keys, Is.EqualTo(expected));
    }

    [Test]
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

        Assert.That(missing, Is.Empty);
    }

    [Test]
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

        Assert.That(missing, Is.Empty);
    }

    [Test]
    public async Task DeleteRange_spans_multiple_leaves()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("delrange-multi-leaf");
        var value = Encoding.UTF8.GetBytes("v");

        // Insert 20 keys into a tree with MaxLeafKeys=4 to force multiple splits.
        for (int i = 0; i < 20; i++)
            await tree.SetAsync($"k{i:D4}", value);

        // Delete a range that spans multiple leaves.
        var count = await tree.DeleteRangeAsync("k0005", "k0015");

        Assert.That(count, Is.EqualTo(10));

        // Verify keys outside the range are still live.
        for (int i = 0; i < 5; i++)
            Assert.That(await tree.GetAsync($"k{i:D4}"), Is.Not.Null, $"k{i:D4} should survive");

        for (int i = 15; i < 20; i++)
            Assert.That(await tree.GetAsync($"k{i:D4}"), Is.Not.Null, $"k{i:D4} should survive");

        // Verify keys inside the range are tombstoned.
        for (int i = 5; i < 15; i++)
            Assert.That(await tree.GetAsync($"k{i:D4}"), Is.Null, $"k{i:D4} should be deleted");

        // KeysAsync should exclude deleted keys.
        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
            keys.Add(k);

        Assert.That(keys, Has.Count.EqualTo(10));
        Assert.That(keys.Any(k =>
            string.Compare(k, "k0005", StringComparison.Ordinal) >= 0 &&
            string.Compare(k, "k0015", StringComparison.Ordinal) < 0), Is.False);
    }
}
