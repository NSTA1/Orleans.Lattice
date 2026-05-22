using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public sealed class LeafEntryCacheTests
{
    private static SortedDictionary<string, LwwValue<byte[]>> NewBackingStore()
        => new(StringComparer.Ordinal);

    private static LwwValue<byte[]> Row(byte[] value, long ticks = 1, bool tombstone = false)
        => new()
        {
            Value = value,
            Timestamp = new HybridLogicalClock { WallClockTicks = ticks, Counter = 0 },
            IsTombstone = tombstone,
        };

    [Test]
    public void Constructor_throws_on_null_backing_store()
    {
        Assert.That(() => new LeafEntryCache(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void TryGetRow_returns_false_for_missing_key()
    {
        var cache = new LeafEntryCache(NewBackingStore());

        Assert.That(cache.TryGetRow("missing", out _), Is.False);
    }

    [Test]
    public void TryGetRow_returns_existing_row()
    {
        var backing = NewBackingStore();
        var row = Row([1, 2, 3]);
        backing["k"] = row;
        var cache = new LeafEntryCache(backing);

        Assert.That(cache.TryGetRow("k", out var actual), Is.True);
        Assert.That(actual.Value, Is.EqualTo(new byte[] { 1, 2, 3 }));
    }

    [Test]
    public void TryGetRow_throws_on_null_key()
    {
        var cache = new LeafEntryCache(NewBackingStore());
        Assert.That(() => cache.TryGetRow(null!, out _), Throws.ArgumentNullException);
    }

    [Test]
    public void ContainsKey_reflects_backing_store()
    {
        var backing = NewBackingStore();
        backing["k"] = Row([0]);
        var cache = new LeafEntryCache(backing);

        Assert.That(cache.ContainsKey("k"), Is.True);
        Assert.That(cache.ContainsKey("other"), Is.False);
    }

    [Test]
    public void ContainsKey_throws_on_null_key()
    {
        var cache = new LeafEntryCache(NewBackingStore());
        Assert.That(() => cache.ContainsKey(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void StoreRow_inserts_and_replaces()
    {
        var backing = NewBackingStore();
        var cache = new LeafEntryCache(backing);

        cache.StoreRow("k", Row([1]));
        Assert.That(cache.Count, Is.EqualTo(1));
        Assert.That(backing["k"].Value, Is.EqualTo(new byte[] { 1 }));

        cache.StoreRow("k", Row([2], ticks: 2));
        Assert.That(cache.Count, Is.EqualTo(1));
        Assert.That(backing["k"].Value, Is.EqualTo(new byte[] { 2 }));
    }

    [Test]
    public void StoreRow_throws_on_null_key()
    {
        var cache = new LeafEntryCache(NewBackingStore());
        Assert.That(() => cache.StoreRow(null!, Row([0])), Throws.ArgumentNullException);
    }

    [Test]
    public void Remove_returns_true_when_present_and_false_when_absent()
    {
        var cache = new LeafEntryCache(NewBackingStore());
        cache.StoreRow("k", Row([1]));

        Assert.That(cache.Remove("k"), Is.True);
        Assert.That(cache.Remove("k"), Is.False);
        Assert.That(cache.Count, Is.EqualTo(0));
    }

    [Test]
    public void Remove_throws_on_null_key()
    {
        var cache = new LeafEntryCache(NewBackingStore());
        Assert.That(() => cache.Remove(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Clear_empties_the_cache()
    {
        var cache = new LeafEntryCache(NewBackingStore());
        cache.StoreRow("a", Row([1]));
        cache.StoreRow("b", Row([2]));

        cache.Clear();

        Assert.That(cache.Count, Is.EqualTo(0));
    }

    [Test]
    public void EnumerateRows_returns_entries_in_ordinal_sorted_order()
    {
        var cache = new LeafEntryCache(NewBackingStore());
        cache.StoreRow("c", Row([3]));
        cache.StoreRow("a", Row([1]));
        cache.StoreRow("b", Row([2]));

        var keys = cache.EnumerateRows().Select(p => p.Key).ToArray();

        Assert.That(keys, Is.EqualTo(new[] { "a", "b", "c" }));
    }

    [Test]
    public void Shim_is_live_view_over_backing_store()
    {
        var backing = NewBackingStore();
        var cache = new LeafEntryCache(backing);

        backing["external"] = Row([9]);

        Assert.That(cache.ContainsKey("external"), Is.True);
        Assert.That(cache.Count, Is.EqualTo(1));

        cache.StoreRow("via-cache", Row([8]));
        Assert.That(backing.ContainsKey("via-cache"), Is.True);
    }

    [Test]
    public void UnderlyingRows_exposes_backing_store_for_transitional_call_sites()
    {
        var backing = NewBackingStore();
        var cache = new LeafEntryCache(backing);

        Assert.That(cache.UnderlyingRows, Is.SameAs(backing));
    }
}
