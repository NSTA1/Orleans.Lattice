using NSubstitute;
using Orleans.Lattice.Vector.Persistence;
using Orleans.Lattice.Vector.Tests.Fakes;

namespace Orleans.Lattice.Vector.Tests.Persistence;

/// <summary>
/// The one type in the package that touches a Lattice tree. What matters here is
/// the translation: a prefix-shaped operation must become exactly the half-open
/// ordinal range that covers it, because a bound computed by hand is subtle at
/// the <c>U+FFFF</c> boundary and a wrong one silently scans nothing.
/// </summary>
[TestFixture]
public sealed class LatticeVectorIndexStoreTests
{
    private static (LatticeVectorIndexStore Store, ILattice Tree) Create()
    {
        var tree = Substitute.For<ILattice>();
        return (new LatticeVectorIndexStore(tree), tree);
    }

    private static async IAsyncEnumerable<KeyValuePair<string, byte[]>> Entries(
        params KeyValuePair<string, byte[]>[] entries)
    {
        foreach (var entry in entries)
        {
            yield return entry;
            await Task.CompletedTask.ConfigureAwait(false);
        }
    }

    private static async IAsyncEnumerable<string> Names(params string[] names)
    {
        foreach (var name in names)
        {
            yield return name;
            await Task.CompletedTask.ConfigureAwait(false);
        }
    }

    [Test]
    public void A_null_tree_is_refused()
    {
        Assert.That(() => new LatticeVectorIndexStore(null!), Throws.ArgumentNullException);
    }

    [Test]
    public async Task Reading_delegates_to_the_tree()
    {
        var (store, tree) = Create();
        tree.GetAsync("k", Arg.Any<CancellationToken>()).Returns(Task.FromResult<byte[]?>([1, 2]));

        Assert.That(await store.ReadAsync("k"), Is.EqualTo(new byte[] { 1, 2 }));
    }

    [Test]
    public async Task Reading_an_absent_key_reports_null_rather_than_an_empty_record()
    {
        var (store, tree) = Create();
        tree.GetAsync("k", Arg.Any<CancellationToken>()).Returns(Task.FromResult<byte[]?>(null));

        Assert.That(await store.ReadAsync("k"), Is.Null);
    }

    [Test]
    public async Task Reading_many_keys_costs_one_round_trip()
    {
        var (store, tree) = Create();
        tree.GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new Dictionary<string, byte[]> { ["a"] = [1] }));

        var found = await store.ReadManyAsync(["a", "b"]);

        Assert.Multiple(() =>
        {
            Assert.That(found.ContainsKey("a"), Is.True);
            Assert.That(found.ContainsKey("b"), Is.False, "An absent key is missing, not empty.");
        });

        await tree.Received(1).GetManyAsync(
            Arg.Is<List<string>>(keys => keys.Count == 2), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Reading_no_keys_does_not_touch_the_tree()
    {
        var (store, tree) = Create();

        Assert.That(await store.ReadManyAsync([]), Is.Empty);

        await tree.DidNotReceive().GetManyAsync(Arg.Any<List<string>>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Writing_batches_into_one_call()
    {
        var (store, tree) = Create();

        await store.WriteAsync([new("a", [1]), new("b", [2])]);

        await tree.Received(1).SetManyAsync(
            Arg.Is<List<KeyValuePair<string, byte[]>>>(entries => entries.Count == 2),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Writing_nothing_does_not_touch_the_tree()
    {
        var (store, tree) = Create();

        await store.WriteAsync([]);

        await tree.DidNotReceive().SetManyAsync(
            Arg.Any<List<KeyValuePair<string, byte[]>>>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Deleting_removes_every_key_it_was_given()
    {
        var (store, tree) = Create();
        tree.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(true));

        await store.DeleteAsync(["a", "b", "c"]);

        await tree.Received(1).DeleteAsync("a", Arg.Any<CancellationToken>());
        await tree.Received(1).DeleteAsync("b", Arg.Any<CancellationToken>());
        await tree.Received(1).DeleteAsync("c", Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_scan_is_bounded_by_the_shared_prefix_upper_bound()
    {
        var (store, tree) = Create();
        tree.EntriesAsync("vidx/g/", Arg.Any<string?>(), Arg.Any<bool>(), Arg.Any<bool?>(),
                Arg.Any<CancellationToken>())
            .Returns(Entries(new KeyValuePair<string, byte[]>("vidx/g/0", [1])));

        var seen = new List<string>();
        await foreach (var entry in store.ScanAsync("vidx/g/"))
        {
            seen.Add(entry.Key);
        }

        Assert.That(seen, Is.EqualTo(new[] { "vidx/g/0" }));

        tree.Received(1).EntriesAsync(
            "vidx/g/",
            LatticeKeyRange.PrefixUpperBound("vidx/g/"),
            Arg.Any<bool>(),
            Arg.Any<bool?>(),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_scan_of_a_prefix_with_no_finite_upper_bound_runs_to_the_end_of_the_keyspace()
    {
        // The helper returns null for a prefix that is entirely U+FFFF, which the
        // scan primitives take to mean "unbounded above". Passing an incremented
        // last code unit instead would wrap to U+0000 and sort below the prefix,
        // inverting the range so the scan captured nothing.
        var prefix = new string(char.MaxValue, 2);
        Assert.That(LatticeKeyRange.PrefixUpperBound(prefix), Is.Null);

        var (store, tree) = Create();
        tree.EntriesAsync(prefix, null, Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(Entries());

        await foreach (var _ in store.ScanAsync(prefix))
        {
            // Draining the enumerator is what issues the call.
        }

        tree.Received(1).EntriesAsync(
            prefix, null, Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Deleting_a_prefix_becomes_a_bounded_range_delete()
    {
        var (store, tree) = Create();
        tree.DeleteRangeAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(3));

        await store.DeletePrefixAsync("vidx/g/000/");

        await tree.Received(1).DeleteRangeAsync(
            "vidx/g/000/",
            LatticeKeyRange.PrefixUpperBound("vidx/g/000/")!,
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Deleting_a_prefix_with_no_finite_upper_bound_falls_back_to_enumerating_the_keys()
    {
        var prefix = new string(char.MaxValue, 2);
        var (store, tree) = Create();
        tree.KeysAsync(prefix, null, Arg.Any<bool>(), Arg.Any<bool?>(), Arg.Any<CancellationToken>())
            .Returns(Names(prefix + "a", prefix + "b"));
        tree.DeleteAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult(true));

        await store.DeletePrefixAsync(prefix);

        await tree.DidNotReceive().DeleteRangeAsync(
            Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>());
        await tree.Received(1).DeleteAsync(prefix + "a", Arg.Any<CancellationToken>());
        await tree.Received(1).DeleteAsync(prefix + "b", Arg.Any<CancellationToken>());
    }

    [Test]
    public void Every_operation_refuses_a_null_key()
    {
        var (store, _) = Create();

        Assert.Multiple(() =>
        {
            Assert.That(async () => await store.ReadAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await store.ReadManyAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await store.WriteAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await store.DeleteAsync(null!), Throws.ArgumentNullException);
            Assert.That(async () => await store.DeletePrefixAsync(null!), Throws.ArgumentNullException);
            Assert.That(
                async () =>
                {
                    await foreach (var _ in store.ScanAsync(null!))
                    {
                        // The guard fires when the enumerator is first drained.
                    }
                },
                Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task A_durable_index_round_trips_over_a_tree_backed_store()
    {
        // The adapter and the engine, exercised together over an in-memory tree
        // that behaves the way a Lattice tree does: ordinal key order and
        // half-open range semantics.
        var tree = OrdinalLatticeTree.Create();
        var store = new LatticeVectorIndexStore(tree);
        var source = DurableIndexHarness.Source(300);
        var options = DurableIndexHarness.Options();

        var built = await DurableVectorIndex.OpenAsync(store, source, options);
        await built.RunBuildAsync();

        var query = source[DurableIndexHarness.Id(4)];
        var before = DurableIndexHarness.SearchResults(built, query, 10);

        var reloaded = await DurableVectorIndex.OpenAsync(store, source, options);

        Assert.Multiple(() =>
        {
            Assert.That(reloaded.Count, Is.EqualTo(300));
            Assert.That(DurableIndexHarness.SearchResults(reloaded, query, 10), Is.EqualTo(before));
        });
    }
}
