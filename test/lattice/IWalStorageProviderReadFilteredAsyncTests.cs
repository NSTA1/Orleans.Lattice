using System.Runtime.CompilerServices;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Contract tests for the <see cref="IWalStorageProvider.ReadFilteredAsync"/>
/// default interface member - the compatibility promise the seam makes to a
/// provider that has not implemented the filtered read path of issue #3565.
/// <para>
/// Every provider shipped in this repository (the in-memory, file and Azure
/// Table backends) overrides the method so it can decide exclusion before it
/// materialises a payload, which is the whole point of the seam. That leaves the
/// default body reachable from no production call site at all: it exists purely
/// so a third-party <see cref="IWalStorageProvider"/> written against an earlier
/// version keeps working, at the cost of a full decode. A seam whose only
/// consumer is a provider nobody in this tree has written is exactly the kind of
/// promise that quietly stops being true, so it is pinned here directly against
/// a minimal foreign provider rather than through any shipped one.
/// </para>
/// <para>
/// What is pinned is the documented rule: the default body must produce the same
/// entries as a native override - every examined entry the filter does not
/// exclude in full, no excluded entry except a trailing one delivered
/// routing-only, the examined-entry budget bounding the work rather than the
/// result, and the window's inclusive upper bound honoured - plus the two
/// argument guards it applies eagerly.
/// </para>
/// </summary>
[TestFixture]
public class IWalStorageProviderReadFilteredAsyncTests
{
    private const string Tree = "tree";
    private const int Shard = 0;

    /// <summary>Owns the single-letter key space from "m" up to (but excluding) "n".</summary>
    private static readonly WalKeyFilter OwnsM = new("m", "n");

    private static WalEntry Entry(long offset, string key) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = key,
            Value = new byte[] { 1, 2, 3 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
            OriginClusterId = "site-a",
        },
    };

    private static async Task<List<WalEntry>> ReadAsync(
        IWalStorageProvider provider,
        long fromOffsetExclusive,
        long toOffsetInclusive,
        int maxEntries,
        WalKeyFilter filter,
        CancellationToken cancellationToken = default)
    {
        var read = new List<WalEntry>();
        await foreach (var entry in provider.ReadFilteredAsync(
            Tree, Shard, fromOffsetExclusive, toOffsetInclusive, maxEntries, filter, cancellationToken))
        {
            read.Add(entry);
        }

        return read;
    }

    private static StubWalStorageProvider Seeded(params (long Offset, string Key)[] entries)
    {
        var stub = new StubWalStorageProvider();
        stub.Seed(entries.Select(e => Entry(e.Offset, e.Key)));
        return stub;
    }

    [Test]
    public async Task ReadFilteredAsync_default_fallback_yields_only_owned_entries()
    {
        var stub = Seeded((0, "a"), (1, "m1"), (2, "z"), (3, "m2"));

        var read = await ReadAsync(stub, -1L, 3L, 1024, OwnsM);

        Assert.That(read.Select(e => e.Mutation.Key), Is.EqualTo(new[] { "m1", "m2" }));
        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 1L, 3L }));
        Assert.That(read[0].Mutation.Value, Is.EqualTo(new byte[] { 1, 2, 3 }),
            "an owned entry is delivered in full, not routing-only");
    }

    [Test]
    public async Task ReadFilteredAsync_default_fallback_delivers_a_trailing_excluded_entry_routing_only()
    {
        // The window's last examined entry is foreign, so it must still reach the
        // reader - offset, kind and key exact, every other field default - or a
        // reader advancing by the last offset it received would never move past it.
        var stub = Seeded((0, "m1"), (1, "z"));

        var read = await ReadAsync(stub, -1L, 1L, 1024, OwnsM);

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L }));
        var trailing = read[^1];
        Assert.Multiple(() =>
        {
            Assert.That(trailing.Mutation.Key, Is.EqualTo("z"), "the routing fields stay exact");
            Assert.That(trailing.Mutation.Kind, Is.EqualTo(MutationKind.Set));
            Assert.That(trailing.Mutation.Value, Is.Null, "every non-routing field is defaulted");
            Assert.That(trailing.Mutation.OriginClusterId, Is.Null);
        });
    }

    [Test]
    public async Task ReadFilteredAsync_default_fallback_does_not_deliver_a_trailing_entry_when_the_window_ends_owned()
    {
        var stub = Seeded((0, "z"), (1, "m1"));

        var read = await ReadAsync(stub, -1L, 1L, 1024, OwnsM);

        Assert.That(read.Select(e => e.Mutation.Key), Is.EqualTo(new[] { "m1" }));
    }

    [Test]
    public async Task ReadFilteredAsync_default_fallback_with_an_unbounded_filter_equals_the_unfiltered_window()
    {
        // The seam's documented equivalence: with an unbounded filter the result
        // is exactly ReadAsync over the window.
        var stub = Seeded((0, "a"), (1, "m1"), (2, "z"));
        Assert.That(default(WalKeyFilter).IsUnbounded, Is.True, "the premise of this test");

        var filtered = await ReadAsync(stub, -1L, 2L, 1024, default);

        var unfiltered = new List<WalEntry>();
        await foreach (var entry in stub.ReadAsync(Tree, Shard, -1L, 1024, CancellationToken.None))
        {
            unfiltered.Add(entry);
        }

        Assert.That(filtered.Select(e => e.Offset), Is.EqualTo(unfiltered.Select(e => e.Offset)));
        Assert.That(filtered.Select(e => e.Mutation.Key), Is.EqualTo(unfiltered.Select(e => e.Mutation.Key)));
    }

    [Test]
    public async Task ReadFilteredAsync_default_fallback_honours_the_inclusive_upper_bound()
    {
        var stub = Seeded((0, "m1"), (1, "m2"), (2, "m3"));

        var read = await ReadAsync(stub, -1L, 1L, 1024, OwnsM);

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 0L, 1L }),
            "no entry above the upper bound is even examined");
    }

    [Test]
    public async Task ReadFilteredAsync_default_fallback_honours_the_exclusive_lower_bound()
    {
        var stub = Seeded((0, "m1"), (1, "m2"), (2, "m3"));

        var read = await ReadAsync(stub, 0L, 2L, 1024, OwnsM);

        Assert.That(read.Select(e => e.Offset), Is.EqualTo(new[] { 1L, 2L }));
    }

    [Test]
    public async Task ReadFilteredAsync_default_fallback_bounds_entries_examined_not_entries_delivered()
    {
        // Three foreign entries and one owned one, with a budget of two: the read
        // must stop after examining two, delivering only the trailing routing-only
        // entry. Counting delivered entries instead would let one call scan the
        // whole partition for a reader that owns little of it.
        var stub = Seeded((0, "a"), (1, "b"), (2, "c"), (3, "m1"));

        var read = await ReadAsync(stub, -1L, 3L, 2, OwnsM);

        Assert.That(read, Has.Count.EqualTo(1));
        Assert.That(read[0].Offset, Is.EqualTo(1L), "the second examined entry is the trailing one");
        Assert.That(read[0].Mutation.Value, Is.Null, "it is delivered routing-only");
    }

    [Test]
    public async Task ReadFilteredAsync_default_fallback_is_empty_exactly_when_the_window_holds_no_entry()
    {
        var stub = Seeded((0, "a"), (1, "b"));

        var pastTheTail = await ReadAsync(stub, 1L, 10L, 1024, OwnsM);
        var emptyWindow = await ReadAsync(stub, 1L, 1L, 1024, OwnsM);
        var foreignOnly = await ReadAsync(stub, -1L, 1L, 1024, OwnsM);

        Assert.Multiple(() =>
        {
            Assert.That(pastTheTail, Is.Empty, "a window past the tail holds no entry");
            Assert.That(emptyWindow, Is.Empty, "an empty window holds no entry");
            Assert.That(foreignOnly, Is.Not.Empty,
                "a window holding only foreign entries is still non-empty, so an empty page means end-of-window");
        });
    }

    [Test]
    public async Task ReadFilteredAsync_default_fallback_yields_nothing_for_an_absent_shard()
    {
        var stub = Seeded((0, "m1"));

        var read = new List<WalEntry>();
        await foreach (var entry in ((IWalStorageProvider)stub).ReadFilteredAsync(
            Tree, Shard + 1, -1L, 10L, 1024, OwnsM, CancellationToken.None))
        {
            read.Add(entry);
        }

        Assert.That(read, Is.Empty);
    }

    [Test]
    public void ReadFilteredAsync_default_fallback_rejects_a_null_treeId()
    {
        var stub = (IWalStorageProvider)new StubWalStorageProvider();

        // The guard is eager: it fires on the call, not on first MoveNextAsync.
        Assert.That(
            () => stub.ReadFilteredAsync(null!, Shard, -1L, 10L, 1024, OwnsM, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public void ReadFilteredAsync_default_fallback_rejects_a_max_entries_below_one()
    {
        var stub = (IWalStorageProvider)new StubWalStorageProvider();

        Assert.Multiple(() =>
        {
            Assert.That(
                () => stub.ReadFilteredAsync(Tree, Shard, -1L, 10L, 0, OwnsM, CancellationToken.None),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(
                () => stub.ReadFilteredAsync(Tree, Shard, -1L, 10L, -1, OwnsM, CancellationToken.None),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public void ReadFilteredAsync_default_fallback_observes_a_pre_cancelled_token()
    {
        var stub = (IWalStorageProvider)Seeded((0, "m1"), (1, "m2"));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.That(
            async () => await ReadAsync(stub, -1L, 10L, 1024, OwnsM, cts.Token),
            Throws.InstanceOf<OperationCanceledException>());
    }

    /// <summary>
    /// A minimal foreign provider: it fills in the required members only and
    /// inherits the default <see cref="IWalStorageProvider.ReadFilteredAsync"/>.
    /// Deliberately not one of the shipped providers, all of which override the
    /// method, so this fixture cannot accidentally test an override instead.
    /// </summary>
    private sealed class StubWalStorageProvider : IWalStorageProvider
    {
        private readonly List<WalEntry> _entries = new();

        public void Seed(IEnumerable<WalEntry> entries) => _entries.AddRange(entries);

        public Task AppendBatchAsync(
            string treeId, int shardIndex, IReadOnlyList<WalEntry> entries, CancellationToken cancellationToken)
        {
            _entries.AddRange(entries);
            return Task.CompletedTask;
        }

        public async IAsyncEnumerable<WalEntry> ReadAsync(
            string treeId,
            int shardIndex,
            long fromOffsetExclusive,
            int maxEntries,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            if (shardIndex != Shard)
            {
                yield break;
            }

            var yielded = 0;
            foreach (var entry in _entries)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (entry.Offset <= fromOffsetExclusive)
                {
                    continue;
                }

                if (yielded >= maxEntries)
                {
                    yield break;
                }

                yield return entry;
                yielded++;
                await Task.Yield();
            }
        }

        public Task<long> GetHighestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => Task.FromResult(_entries.Count == 0 ? -1L : _entries[^1].Offset);

        public Task<long> GetLowestOffsetAsync(string treeId, int shardIndex, CancellationToken cancellationToken)
            => Task.FromResult(_entries.Count == 0 ? -1L : _entries[0].Offset);

        public Task TrimAsync(
            string treeId, int shardIndex, long throughOffsetInclusive, CancellationToken cancellationToken)
        {
            _entries.RemoveAll(e => e.Offset <= throughOffsetInclusive);
            return Task.CompletedTask;
        }
    }
}
