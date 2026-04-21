using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Integration tests for <see cref="ILattice.EntriesAsync"/> using the default cluster.
/// </summary>
[TestFixture]
public class EntriesDefaultClusterTests
{
    private ClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new ClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    [Test]
    public async Task Entries_empty_tree_returns_nothing()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("entries-empty");
        var entries = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in tree.EntriesAsync())
            entries.Add(e);

        Assert.That(entries, Is.Empty);
    }

    [Test]
    public async Task Entries_returns_all_entries_sorted_by_key()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("entries-sorted");
        var expected = new[] { "alpha", "bravo", "charlie", "delta", "echo" };
        foreach (var k in expected)
            await tree.SetAsync(k, Encoding.UTF8.GetBytes(k));

        var entries = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in tree.EntriesAsync())
            entries.Add(e);

        var keys = entries.Select(e => e.Key).ToList();
        Assert.That(keys, Is.EqualTo(expected.OrderBy(k => k, StringComparer.Ordinal).ToList()));

        // Verify values match keys.
        foreach (var e in entries)
            Assert.That(Encoding.UTF8.GetString(e.Value), Is.EqualTo(e.Key));
    }

    [Test]
    public async Task Entries_reverse_returns_descending_order()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("entries-reverse");
        var items = new[] { "a", "b", "c", "d", "e" };
        foreach (var k in items)
            await tree.SetAsync(k, Encoding.UTF8.GetBytes(k));

        var entries = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in tree.EntriesAsync(reverse: true))
            entries.Add(e);

        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(
            items.OrderByDescending(k => k, StringComparer.Ordinal).ToList()));
    }

    [Test]
    public async Task Entries_range_filters_correctly()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("entries-range");
        var items = new[] { "a", "b", "c", "d", "e", "f" };
        foreach (var k in items)
            await tree.SetAsync(k, Encoding.UTF8.GetBytes(k));

        var entries = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in tree.EntriesAsync(startInclusive: "b", endExclusive: "e"))
            entries.Add(e);

        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(new[] { "b", "c", "d" }));
    }

    [Test]
    public async Task Entries_excludes_tombstoned_keys()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("entries-tombstoned");
        await tree.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await tree.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await tree.SetAsync("c", Encoding.UTF8.GetBytes("3"));
        await tree.DeleteAsync("b");

        var entries = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in tree.EntriesAsync())
            entries.Add(e);

        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(new[] { "a", "c" }));
    }
}

/// <summary>
/// Integration tests for <see cref="ILattice.EntriesAsync"/> with a small-leaf cluster
/// to exercise multi-leaf pagination across splits.
/// </summary>
[TestFixture]
public class EntriesSmallLeafClusterTests
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
    public async Task Entries_across_multiple_leaves_returns_all_entries_sorted()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>(SmallLeafClusterFixture.TreeName);
        // Insert more keys than MaxLeafKeys (4) to force splits.
        var expected = Enumerable.Range(0, 20).Select(i => $"key-{i:D3}").ToList();
        foreach (var k in expected)
            await tree.SetAsync(k, Encoding.UTF8.GetBytes(k));

        var entries = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in tree.EntriesAsync())
            entries.Add(e);

        expected.Sort(StringComparer.Ordinal);
        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(expected));

        // Verify each value matches its key.
        foreach (var e in entries)
            Assert.That(Encoding.UTF8.GetString(e.Value), Is.EqualTo(e.Key));
    }

    [Test]
    public async Task Entries_reverse_across_multiple_leaves()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("entries-rev-multi");
        var items = Enumerable.Range(0, 12).Select(i => $"r-{i:D3}").ToList();
        foreach (var k in items)
            await tree.SetAsync(k, Encoding.UTF8.GetBytes(k));

        var entries = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in tree.EntriesAsync(reverse: true))
            entries.Add(e);

        items.Sort(StringComparer.Ordinal);
        items.Reverse();
        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(items));
    }

    [Test]
    public async Task Entries_range_across_multiple_leaves()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("entries-range-multi");
        var items = Enumerable.Range(0, 20).Select(i => $"e-{i:D3}").ToList();
        foreach (var k in items)
            await tree.SetAsync(k, Encoding.UTF8.GetBytes(k));

        var entries = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in tree.EntriesAsync(startInclusive: "e-005", endExclusive: "e-015"))
            entries.Add(e);

        var expected = items.Where(k =>
            string.Compare(k, "e-005", StringComparison.Ordinal) >= 0 &&
            string.Compare(k, "e-015", StringComparison.Ordinal) < 0)
            .OrderBy(k => k, StringComparer.Ordinal).ToList();
        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(expected));
    }
}

/// <summary>
/// Integration tests for <see cref="ILattice.EntriesAsync"/> with the <c>prefetch</c>
/// parameter enabled, using a 4-shard cluster with small leaves to force pagination.
/// </summary>
[TestFixture]
public class EntriesPrefetchTests
{
    private FourShardClusterFixture _fixture = null!;
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FourShardClusterFixture();
        await _fixture.InitializeAsync();
        _cluster = _fixture.Cluster;
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _fixture.DisposeAsync();
    }

    [Test]
    public async Task Entries_prefetch_returns_all_entries_sorted()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("epf-sorted");

        var keys = Enumerable.Range(0, 50)
            .Select(i => $"epf-{i:D4}")
            .ToList();

        foreach (var k in keys)
            await tree.SetAsync(k, Encoding.UTF8.GetBytes(k));

        var entries = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in tree.EntriesAsync(prefetch: true))
            entries.Add(e);

        keys.Sort(StringComparer.Ordinal);
        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(keys));
        foreach (var e in entries)
            Assert.That(Encoding.UTF8.GetString(e.Value), Is.EqualTo(e.Key));
    }

    [Test]
    public async Task Entries_prefetch_reverse_returns_descending()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("epf-reverse");

        var keys = Enumerable.Range(0, 50)
            .Select(i => $"epfr-{i:D4}")
            .ToList();

        foreach (var k in keys)
            await tree.SetAsync(k, Encoding.UTF8.GetBytes(k));

        var entries = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in tree.EntriesAsync(reverse: true, prefetch: true))
            entries.Add(e);

        keys.Sort(StringComparer.Ordinal);
        keys.Reverse();
        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(keys));
    }

    [Test]
    public async Task Entries_prefetch_range_filters_correctly()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("epf-range");

        for (int i = 0; i < 50; i++)
            await tree.SetAsync($"epfg-{i:D4}", Encoding.UTF8.GetBytes($"v-{i}"));

        var entries = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in tree.EntriesAsync(startInclusive: "epfg-0010", endExclusive: "epfg-0030", prefetch: true))
            entries.Add(e);

        var expected = Enumerable.Range(10, 20)
            .Select(i => $"epfg-{i:D4}")
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToList();

        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(expected));
    }

    [Test]
    public async Task Entries_prefetch_many_entries_forces_pagination()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("epf-pagination");

        var keys = Enumerable.Range(0, 200)
            .Select(i => $"epfp-{i:D4}")
            .ToList();

        foreach (var k in keys)
            await tree.SetAsync(k, Encoding.UTF8.GetBytes(k));

        var entries = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in tree.EntriesAsync(prefetch: true))
            entries.Add(e);

        keys.Sort(StringComparer.Ordinal);
        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(keys));
    }

    [Test]
    public async Task Entries_prefetch_false_disables_even_when_option_enabled()
    {
        // prefetch: false should work the same as default — verifies the parameter
        // override path doesn't break anything.
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("epf-disabled");

        var keys = Enumerable.Range(0, 30)
            .Select(i => $"epfd-{i:D4}")
            .ToList();

        foreach (var k in keys)
            await tree.SetAsync(k, Encoding.UTF8.GetBytes(k));

        var entries = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in tree.EntriesAsync(prefetch: false))
            entries.Add(e);

        keys.Sort(StringComparer.Ordinal);
        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(keys));
    }

    [Test]
    public async Task Entries_prefetch_empty_tree_returns_nothing()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("epf-empty");
        var entries = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in tree.EntriesAsync(prefetch: true))
            entries.Add(e);

        Assert.That(entries, Is.Empty);
    }

    [Test]
    public async Task Entries_prefetch_reverse_range_filters_correctly()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("epf-rev-range");

        for (int i = 0; i < 50; i++)
            await tree.SetAsync($"eprr-{i:D4}", Encoding.UTF8.GetBytes($"v-{i}"));

        var entries = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in tree.EntriesAsync(
            startInclusive: "eprr-0010", endExclusive: "eprr-0030",
            reverse: true, prefetch: true))
            entries.Add(e);

        var expected = Enumerable.Range(10, 20)
            .Select(i => $"eprr-{i:D4}")
            .OrderByDescending(k => k, StringComparer.Ordinal)
            .ToList();

        Assert.That(entries.Select(e => e.Key).ToList(), Is.EqualTo(expected));
    }

    [Test]
    public async Task Entries_prefetch_matches_non_prefetch_results()
    {
        var tree = _cluster.GrainFactory.GetGrain<ILattice>("epf-match");

        var keys = Enumerable.Range(0, 100)
            .Select(i => $"epm-{i:D4}")
            .ToList();

        foreach (var k in keys)
            await tree.SetAsync(k, Encoding.UTF8.GetBytes(k));

        var withoutPrefetch = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in tree.EntriesAsync(prefetch: false))
            withoutPrefetch.Add(e);

        var withPrefetch = new List<KeyValuePair<string, byte[]>>();
        await foreach (var e in tree.EntriesAsync(prefetch: true))
            withPrefetch.Add(e);

        Assert.That(withPrefetch.Select(e => e.Key).ToList(),
            Is.EqualTo(withoutPrefetch.Select(e => e.Key).ToList()));
        for (int i = 0; i < withPrefetch.Count; i++)
            Assert.That(withPrefetch[i].Value, Is.EqualTo(withoutPrefetch[i].Value));
    }
}
