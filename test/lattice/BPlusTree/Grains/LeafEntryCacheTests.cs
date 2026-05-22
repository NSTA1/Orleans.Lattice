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

    private sealed class FakeTypedState
    {
        public int Counter;
    }

    private sealed class OtherTypedState
    {
        public string Label = string.Empty;
    }

    [Test]
    public void StoreTyped_throws_on_null_key()
    {
        var cache = new LeafEntryCache(NewBackingStore());
        Assert.That(() => cache.StoreTyped<FakeTypedState>(null!, new FakeTypedState()), Throws.ArgumentNullException);
    }

    [Test]
    public void StoreTyped_throws_on_null_typed_instance()
    {
        var cache = new LeafEntryCache(NewBackingStore());
        Assert.That(() => cache.StoreTyped<FakeTypedState>("k", null!), Throws.ArgumentNullException);
    }

    [Test]
    public void TryGetTyped_throws_on_null_key()
    {
        var cache = new LeafEntryCache(NewBackingStore());
        Assert.That(() => cache.TryGetTyped<FakeTypedState>(null!, out _), Throws.ArgumentNullException);
    }

    [Test]
    public void TryGetTyped_returns_false_when_no_shadow_has_been_stored()
    {
        var cache = new LeafEntryCache(NewBackingStore());
        Assert.That(cache.TryGetTyped<FakeTypedState>("missing", out var typed), Is.False);
        Assert.That(typed, Is.Null);
    }

    [Test]
    public void StoreTyped_then_TryGetTyped_round_trips_the_same_instance()
    {
        var cache = new LeafEntryCache(NewBackingStore());
        var typed = new FakeTypedState { Counter = 7 };
        cache.StoreTyped("k", typed);

        Assert.That(cache.TryGetTyped<FakeTypedState>("k", out var actual), Is.True);
        Assert.That(actual, Is.SameAs(typed));
    }

    [Test]
    public void StoreTyped_replaces_existing_shadow_for_the_same_key()
    {
        var cache = new LeafEntryCache(NewBackingStore());
        var first = new FakeTypedState { Counter = 1 };
        var second = new FakeTypedState { Counter = 2 };

        cache.StoreTyped("k", first);
        cache.StoreTyped("k", second);

        Assert.That(cache.TryGetTyped<FakeTypedState>("k", out var actual), Is.True);
        Assert.That(actual, Is.SameAs(second));
    }

    [Test]
    public void TryGetTyped_returns_false_when_stored_type_is_not_assignable_to_requested_type()
    {
        var cache = new LeafEntryCache(NewBackingStore());
        cache.StoreTyped("k", new FakeTypedState());

        Assert.That(cache.TryGetTyped<OtherTypedState>("k", out var actual), Is.False);
        Assert.That(actual, Is.Null);
    }

    [Test]
    public void StoreRow_evicts_the_typed_shadow_for_the_same_key()
    {
        var cache = new LeafEntryCache(NewBackingStore());
        cache.StoreTyped("k", new FakeTypedState { Counter = 1 });
        cache.StoreRow("k", Row([1]));

        Assert.That(cache.TryGetTyped<FakeTypedState>("k", out _), Is.False);
    }

    [Test]
    public void StoreRow_does_not_evict_typed_shadows_for_other_keys()
    {
        var cache = new LeafEntryCache(NewBackingStore());
        cache.StoreTyped("k1", new FakeTypedState { Counter = 1 });
        cache.StoreTyped("k2", new FakeTypedState { Counter = 2 });

        cache.StoreRow("k1", Row([1]));

        Assert.That(cache.TryGetTyped<FakeTypedState>("k1", out _), Is.False);
        Assert.That(cache.TryGetTyped<FakeTypedState>("k2", out var k2), Is.True);
        Assert.That(k2.Counter, Is.EqualTo(2));
    }

    [Test]
    public void Remove_evicts_the_typed_shadow_for_the_same_key()
    {
        var cache = new LeafEntryCache(NewBackingStore());
        cache.StoreRow("k", Row([1]));
        cache.StoreTyped("k", new FakeTypedState { Counter = 1 });

        Assert.That(cache.Remove("k"), Is.True);
        Assert.That(cache.TryGetTyped<FakeTypedState>("k", out _), Is.False);
    }

    [Test]
    public void Clear_evicts_every_typed_shadow()
    {
        var cache = new LeafEntryCache(NewBackingStore());
        cache.StoreTyped("k1", new FakeTypedState { Counter = 1 });
        cache.StoreTyped("k2", new FakeTypedState { Counter = 2 });

        cache.Clear();

        Assert.That(cache.TryGetTyped<FakeTypedState>("k1", out _), Is.False);
        Assert.That(cache.TryGetTyped<FakeTypedState>("k2", out _), Is.False);
    }
}
