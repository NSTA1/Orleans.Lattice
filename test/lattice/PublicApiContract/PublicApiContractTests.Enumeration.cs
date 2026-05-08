namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

public partial class PublicApiContractTests
{
    // ── KeysAsync ───────────────────────────────────────────────────────

    [Test]
    public async Task KeysAsync_returns_all_live_keys_in_lexicographic_order()
    {
        var tree = Tree("pac-enum-keys-all");
        await tree.SetManyAsync([Kvp("c", "3"), Kvp("a", "1"), Kvp("b", "2")]);

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
        {
            keys.Add(k);
        }

        Assert.That(keys, Is.EqualTo(new[] { "a", "b", "c" }));
    }

    [Test]
    public async Task KeysAsync_returns_keys_in_reverse_when_reverse_is_true()
    {
        var tree = Tree("pac-enum-keys-reverse");
        await tree.SetManyAsync([Kvp("a", "1"), Kvp("b", "2"), Kvp("c", "3")]);

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync(reverse: true))
        {
            keys.Add(k);
        }

        Assert.That(keys, Is.EqualTo(new[] { "c", "b", "a" }));
    }

    [Test]
    public async Task KeysAsync_filters_to_inclusive_lower_and_exclusive_upper_bound()
    {
        var tree = Tree("pac-enum-keys-range");
        await tree.SetManyAsync(
            [Kvp("a", "1"), Kvp("b", "2"), Kvp("c", "3"), Kvp("d", "4")]);

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync(startInclusive: "b", endExclusive: "d"))
        {
            keys.Add(k);
        }

        Assert.That(keys, Is.EqualTo(new[] { "b", "c" }));
    }

    [Test]
    public async Task KeysAsync_omits_tombstoned_keys()
    {
        var tree = Tree("pac-enum-keys-skiptombstones");
        await tree.SetManyAsync([Kvp("a", "1"), Kvp("b", "2"), Kvp("c", "3")]);
        await tree.DeleteAsync("b");

        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
        {
            keys.Add(k);
        }

        Assert.That(keys, Does.Not.Contain("b"));
    }

    [Test]
    public async Task KeysAsync_empty_tree_yields_nothing()
    {
        var tree = Tree("pac-enum-keys-empty");
        var keys = new List<string>();
        await foreach (var k in tree.KeysAsync())
        {
            keys.Add(k);
        }
        Assert.That(keys, Is.Empty);
    }

    // ── EntriesAsync ────────────────────────────────────────────────────

    [Test]
    public async Task EntriesAsync_returns_all_live_entries_in_lexicographic_order()
    {
        var tree = Tree("pac-enum-entries-all");
        await tree.SetManyAsync([Kvp("a", "1"), Kvp("b", "2"), Kvp("c", "3")]);

        var entries = new List<KeyValuePair<string, string>>();
        await foreach (var e in tree.EntriesAsync())
        {
            entries.Add(new(e.Key, Str(e.Value)));
        }

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "a", "b", "c" }));
        Assert.That(entries.Select(e => e.Value), Is.EqualTo(new[] { "1", "2", "3" }));
    }

    [Test]
    public async Task EntriesAsync_in_reverse_returns_descending_keys()
    {
        var tree = Tree("pac-enum-entries-reverse");
        await tree.SetManyAsync([Kvp("a", "1"), Kvp("b", "2"), Kvp("c", "3")]);

        var keys = new List<string>();
        await foreach (var e in tree.EntriesAsync(reverse: true))
        {
            keys.Add(e.Key);
        }

        Assert.That(keys, Is.EqualTo(new[] { "c", "b", "a" }));
    }

    [Test]
    public async Task EntriesAsync_filters_to_range()
    {
        var tree = Tree("pac-enum-entries-range");
        await tree.SetManyAsync(
            [Kvp("a", "1"), Kvp("b", "2"), Kvp("c", "3"), Kvp("d", "4")]);

        var keys = new List<string>();
        await foreach (var e in tree.EntriesAsync(startInclusive: "b", endExclusive: "d"))
        {
            keys.Add(e.Key);
        }

        Assert.That(keys, Is.EqualTo(new[] { "b", "c" }));
    }

    // ── ScanKeysAsync (resilient extension) ─────────────────────────────

    [Test]
    public async Task ScanKeysAsync_returns_all_live_keys_in_lexicographic_order()
    {
        var tree = Tree("pac-enum-scankeys-all");
        await tree.SetManyAsync([Kvp("c", "3"), Kvp("a", "1"), Kvp("b", "2")]);

        var keys = new List<string>();
        await foreach (var k in tree.ScanKeysAsync())
        {
            keys.Add(k);
        }

        Assert.That(keys, Is.EqualTo(new[] { "a", "b", "c" }));
    }

    [Test]
    public async Task ScanKeysAsync_in_reverse_returns_descending_keys()
    {
        var tree = Tree("pac-enum-scankeys-reverse");
        await tree.SetManyAsync([Kvp("a", "1"), Kvp("b", "2"), Kvp("c", "3")]);

        var keys = new List<string>();
        await foreach (var k in tree.ScanKeysAsync(reverse: true))
        {
            keys.Add(k);
        }

        Assert.That(keys, Is.EqualTo(new[] { "c", "b", "a" }));
    }

    [Test]
    public async Task ScanKeysAsync_filters_to_range()
    {
        var tree = Tree("pac-enum-scankeys-range");
        await tree.SetManyAsync(
            [Kvp("a", "1"), Kvp("b", "2"), Kvp("c", "3"), Kvp("d", "4")]);

        var keys = new List<string>();
        await foreach (var k in tree.ScanKeysAsync(startInclusive: "b", endExclusive: "d"))
        {
            keys.Add(k);
        }

        Assert.That(keys, Is.EqualTo(new[] { "b", "c" }));
    }

    [Test]
    public void ScanKeysAsync_throws_for_null_lattice()
    {
        ILattice? lattice = null;
        Assert.That(
            () => lattice!.ScanKeysAsync().GetAsyncEnumerator(),
            Throws.InstanceOf<ArgumentNullException>());
    }

    // ── ScanEntriesAsync (resilient extension) ──────────────────────────

    [Test]
    public async Task ScanEntriesAsync_returns_all_live_entries_in_lexicographic_order()
    {
        var tree = Tree("pac-enum-scanentries-all");
        await tree.SetManyAsync([Kvp("a", "1"), Kvp("b", "2"), Kvp("c", "3")]);

        var entries = new List<KeyValuePair<string, string>>();
        await foreach (var e in tree.ScanEntriesAsync())
        {
            entries.Add(new(e.Key, Str(e.Value)));
        }

        Assert.That(entries.Select(e => e.Key), Is.EqualTo(new[] { "a", "b", "c" }));
        Assert.That(entries.Select(e => e.Value), Is.EqualTo(new[] { "1", "2", "3" }));
    }

    [Test]
    public async Task ScanEntriesAsync_filters_to_range()
    {
        var tree = Tree("pac-enum-scanentries-range");
        await tree.SetManyAsync(
            [Kvp("a", "1"), Kvp("b", "2"), Kvp("c", "3"), Kvp("d", "4")]);

        var keys = new List<string>();
        await foreach (var e in tree.ScanEntriesAsync(startInclusive: "b", endExclusive: "d"))
        {
            keys.Add(e.Key);
        }

        Assert.That(keys, Is.EqualTo(new[] { "b", "c" }));
    }

    // ── Cancellation ────────────────────────────────────────────────────

    [Test]
    public void ScanKeysAsync_propagates_cancellation()
    {
        var tree = Tree("pac-enum-scankeys-cancel");
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.That(async () =>
        {
            await foreach (var _ in tree.ScanKeysAsync(cancellationToken: cts.Token))
            {
                /* drain */
            }
        }, Throws.InstanceOf<OperationCanceledException>());
    }
}
