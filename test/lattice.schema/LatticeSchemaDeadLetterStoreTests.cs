using NSubstitute;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaDeadLetterStore"/>: append, per-tree
/// prefix-scoped list and count, and argument validation, exercised against an
/// in-memory <see cref="ILattice"/> so no cluster is required. Confirms that
/// entries for one tree do not leak into another tree's contiguous prefix scan.
/// </summary>
[TestFixture]
public sealed class LatticeSchemaDeadLetterStoreTests
{
    private static LatticeSchemaDeadLetterStore CreateStore()
    {
        var backing = new SortedDictionary<string, byte[]>(StringComparer.Ordinal);
        var lattice = InMemoryLatticeFake.Create(backing);
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(SchemaConstants.DeadLetterTree).Returns(lattice);
        return new LatticeSchemaDeadLetterStore(grainFactory);
    }

    private static LatticeSchemaDeadLetterEntry Entry(string key, DateTimeOffset when) =>
        new(key, [1, 2, 3], 3, "invalid", LatticeSchemaDeadLetterSource.Replication, when);

    [Test]
    public async Task AppendAsync_then_ListAsync_returns_the_entry()
    {
        var store = CreateStore();
        var when = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        await store.AppendAsync("orders", Entry("k1", when));

        var listed = new List<LatticeSchemaDeadLetterEntry>();
        await foreach (var entry in store.ListAsync("orders"))
        {
            listed.Add(entry);
        }

        Assert.That(listed, Has.Count.EqualTo(1));
        Assert.That(listed[0].Key, Is.EqualTo("k1"));
        Assert.That(listed[0].Reason, Is.EqualTo("invalid"));
    }

    [Test]
    public async Task ListAsync_returns_entries_in_timestamp_order()
    {
        var store = CreateStore();
        var t1 = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var t2 = new DateTimeOffset(2026, 1, 2, 0, 0, 0, TimeSpan.Zero);

        await store.AppendAsync("orders", Entry("later", t2));
        await store.AppendAsync("orders", Entry("earlier", t1));

        var keys = new List<string>();
        await foreach (var entry in store.ListAsync("orders"))
        {
            keys.Add(entry.Key);
        }

        Assert.That(keys, Is.EqualTo(new[] { "earlier", "later" }));
    }

    [Test]
    public async Task ListAsync_scopes_to_the_requested_tree()
    {
        var store = CreateStore();
        var when = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        await store.AppendAsync("orders", Entry("o1", when));
        await store.AppendAsync("users", Entry("u1", when));

        var ordersKeys = new List<string>();
        await foreach (var entry in store.ListAsync("orders"))
        {
            ordersKeys.Add(entry.Key);
        }

        Assert.That(ordersKeys, Is.EqualTo(new[] { "o1" }));
    }

    [Test]
    public async Task CountAsync_counts_only_the_requested_tree()
    {
        var store = CreateStore();
        var when = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        await store.AppendAsync("orders", Entry("o1", when));
        await store.AppendAsync("orders", Entry("o2", when.AddSeconds(1)));
        await store.AppendAsync("users", Entry("u1", when));

        Assert.That(await store.CountAsync("orders"), Is.EqualTo(2));
        Assert.That(await store.CountAsync("users"), Is.EqualTo(1));
    }

    [Test]
    public async Task CountAsync_empty_tree_returns_zero()
    {
        var store = CreateStore();

        Assert.That(await store.CountAsync("orders"), Is.EqualTo(0));
    }

    [Test]
    public void AppendAsync_empty_tree_throws()
    {
        var store = CreateStore();

        Assert.That(
            () => store.AppendAsync("", Entry("k", DateTimeOffset.UnixEpoch)),
            Throws.ArgumentException);
    }

    [Test]
    public void AppendAsync_null_entry_throws()
    {
        var store = CreateStore();

        Assert.That(() => store.AppendAsync("orders", null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ListAsync_empty_tree_throws()
    {
        var store = CreateStore();

        Assert.That(
            async () =>
            {
                await foreach (var _ in store.ListAsync(""))
                {
                }
            },
            Throws.ArgumentException);
    }

    [Test]
    public void CountAsync_empty_tree_throws()
    {
        var store = CreateStore();

        Assert.That(() => store.CountAsync(""), Throws.ArgumentException);
    }
}
