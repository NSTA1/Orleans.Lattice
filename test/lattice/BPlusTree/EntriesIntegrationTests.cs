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
}
