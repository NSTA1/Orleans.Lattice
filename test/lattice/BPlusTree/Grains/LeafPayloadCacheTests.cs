using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="LeafPayloadCache"/>, the read-through mirror
/// storage behind <see cref="LeafCacheGrain"/>. Cover the unbounded
/// zero-overhead default, the LRU value-payload budget, the payload-evicted
/// sentinel shape, recency ordering, and the row-removal / clear paths.
/// </summary>
[TestFixture]
public class LeafPayloadCacheTests
{
    private static long _clockCounter;

    private static LwwValue<byte[]> Val(string value)
    {
        // Distinct, strictly-increasing timestamps so LWW merges are well-defined.
        var clock = default(HybridLogicalClock);
        var ticks = Interlocked.Increment(ref _clockCounter);
        for (var i = 0; i < ticks; i++)
            clock = HybridLogicalClock.Tick(clock);
        return LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes(value), clock);
    }

    private static bool IsPayloadEvicted(in LwwValue<byte[]> v) => v.Value is null && !v.IsTombstone;

    [Test]
    public void Unbounded_store_round_trips_without_byte_accounting()
    {
        var cache = new LeafPayloadCache();

        cache.Set("a", Val("aaaa"));
        cache.Set("b", Val("bbbbbb"));

        Assert.That(cache.Count, Is.EqualTo(2));
        // Accounting is disabled while unbounded.
        Assert.That(cache.ResidentValueBytes, Is.EqualTo(0));
        Assert.That(cache.TryPeek("a", out var a), Is.True);
        Assert.That(Encoding.UTF8.GetString(a.Value!), Is.EqualTo("aaaa"));
    }

    [Test]
    public void Unbounded_store_never_evicts_however_large()
    {
        var cache = new LeafPayloadCache();
        for (var i = 0; i < 500; i++)
            cache.Set($"k{i}", Val(new string('x', 1000)));

        Assert.That(cache.Count, Is.EqualTo(500));
        foreach (var v in cache.Values)
            Assert.That(v.Value, Is.Not.Null, "no payload should be evicted when unbounded");
    }

    [Test]
    public void Bounded_store_within_budget_keeps_all_payloads_resident()
    {
        var cache = new LeafPayloadCache();
        cache.SetBudget(1000);

        cache.Set("a", Val("1234567890"));   // 10 bytes
        cache.Set("b", Val("1234567890"));   // 10 bytes

        Assert.That(cache.ResidentValueBytes, Is.EqualTo(20));
        Assert.That(cache.TryPeek("a", out var a) && a.Value is not null, Is.True);
        Assert.That(cache.TryPeek("b", out var b) && b.Value is not null, Is.True);
    }

    [Test]
    public void Bounded_store_evicts_lru_payload_when_budget_exceeded()
    {
        var cache = new LeafPayloadCache();
        cache.SetBudget(20); // room for two 10-byte payloads

        cache.Set("a", Val("aaaaaaaaaa")); // 10
        cache.Set("b", Val("bbbbbbbbbb")); // 10 -> total 20, at budget
        cache.Set("c", Val("cccccccccc")); // 10 -> 30, evicts LRU ("a")

        Assert.That(cache.ResidentValueBytes, Is.LessThanOrEqualTo(20));
        Assert.That(cache.Count, Is.EqualTo(3), "eviction drops payloads, not rows");

        Assert.That(cache.TryPeek("a", out var a), Is.True);
        Assert.That(IsPayloadEvicted(a), Is.True, "least-recently-used key's payload is evicted");

        Assert.That(cache.TryPeek("c", out var c), Is.True);
        Assert.That(c.Value, Is.Not.Null, "most-recently-used key stays resident");
    }

    [Test]
    public void Evicted_entry_retains_all_metadata()
    {
        var cache = new LeafPayloadCache();
        cache.SetBudget(10);

        var clock = HybridLogicalClock.Tick(default);
        var migrated = new LwwValue<byte[]>
        {
            Value = Encoding.UTF8.GetBytes("0123456789"),
            Timestamp = clock,
            IsMigrated = true,
            ExpiresAtTicks = 123456,
            OriginClusterId = "site-b",
        };
        cache.Set("m", migrated);
        cache.Set("n", Val("evict-trigger")); // pushes over budget, evicts "m"

        Assert.That(cache.TryPeek("m", out var m), Is.True);
        Assert.That(IsPayloadEvicted(m), Is.True);
        Assert.That(m.Timestamp, Is.EqualTo(clock));
        Assert.That(m.IsMigrated, Is.True);
        Assert.That(m.ExpiresAtTicks, Is.EqualTo(123456));
        Assert.That(m.OriginClusterId, Is.EqualTo("site-b"));
    }

    [Test]
    public void RecordHit_protects_key_from_eviction()
    {
        var cache = new LeafPayloadCache();
        cache.SetBudget(20);

        cache.Set("a", Val("aaaaaaaaaa")); // 10
        cache.Set("b", Val("bbbbbbbbbb")); // 10
        // Touch "a" so it becomes most-recently-used; "b" is now the LRU victim.
        cache.RecordHit("a");
        cache.Set("c", Val("cccccccccc")); // 10 -> evicts LRU, which is now "b"

        Assert.That(cache.TryPeek("a", out var a) && a.Value is not null, Is.True,
            "recently-hit key survives");
        Assert.That(cache.TryPeek("b", out var b) && IsPayloadEvicted(b), Is.True,
            "un-hit key is the eviction victim");
    }

    [Test]
    public void Re_setting_a_key_marks_it_most_recently_used()
    {
        var cache = new LeafPayloadCache();
        cache.SetBudget(20);

        cache.Set("a", Val("aaaaaaaaaa"));
        cache.Set("b", Val("bbbbbbbbbb"));
        cache.Set("a", Val("AAAAAAAAAA")); // re-set "a" -> MRU, "b" becomes LRU
        cache.Set("c", Val("cccccccccc")); // evicts LRU "b"

        Assert.That(cache.TryPeek("a", out var a) && a.Value is not null, Is.True);
        Assert.That(cache.TryPeek("b", out var b) && IsPayloadEvicted(b), Is.True);
    }

    [Test]
    public void Re_setting_an_evicted_key_with_a_payload_restores_residency()
    {
        var cache = new LeafPayloadCache();
        cache.SetBudget(20);

        cache.Set("a", Val("aaaaaaaaaa"));
        cache.Set("b", Val("bbbbbbbbbb"));
        cache.Set("c", Val("cccccccccc")); // evicts "a"
        Assert.That(cache.TryPeek("a", out var evicted) && IsPayloadEvicted(evicted), Is.True);

        // A fresh write for "a" (higher HLC) re-populates the payload.
        cache.Set("a", Val("AAAAAAAAAA"));
        Assert.That(cache.TryPeek("a", out var restored) && restored.Value is not null, Is.True);
    }

    [Test]
    public void Empty_value_is_never_evicted_into_the_sentinel()
    {
        var cache = new LeafPayloadCache();
        cache.SetBudget(1); // extremely tight

        var clock = HybridLogicalClock.Tick(default);
        cache.Set("empty", LwwValue<byte[]>.Create(Array.Empty<byte>(), clock));
        cache.Set("big", Val("payload-that-overflows"));

        Assert.That(cache.TryPeek("empty", out var e), Is.True);
        Assert.That(e.Value, Is.Not.Null, "a present empty value must stay non-null");
        Assert.That(e.Value!.Length, Is.EqualTo(0));
    }

    [Test]
    public void Tombstone_is_not_counted_and_not_evicted()
    {
        var cache = new LeafPayloadCache();
        cache.SetBudget(5);

        var clock = HybridLogicalClock.Tick(default);
        cache.Set("dead", LwwValue<byte[]>.Tombstone(clock));
        cache.Set("live", Val("abc"));

        Assert.That(cache.TryPeek("dead", out var dead), Is.True);
        Assert.That(dead.IsTombstone, Is.True, "tombstone row is retained as a tombstone, not a payload-evicted sentinel");
        Assert.That(IsPayloadEvicted(dead), Is.False);
    }

    [Test]
    public void Remove_drops_row_and_decrements_bytes()
    {
        var cache = new LeafPayloadCache();
        cache.SetBudget(1000);

        cache.Set("a", Val("aaaaa")); // 5
        cache.Set("b", Val("bbbbb")); // 5
        Assert.That(cache.ResidentValueBytes, Is.EqualTo(10));

        Assert.That(cache.Remove("a"), Is.True);
        Assert.That(cache.Remove("a"), Is.False, "removing a missing key returns false");
        Assert.That(cache.Count, Is.EqualTo(1));
        Assert.That(cache.ResidentValueBytes, Is.EqualTo(5));
    }

    [Test]
    public void Clear_resets_rows_and_accounting()
    {
        var cache = new LeafPayloadCache();
        cache.SetBudget(1000);
        cache.Set("a", Val("aaaaa"));
        cache.Set("b", Val("bbbbb"));

        cache.Clear();

        Assert.That(cache.Count, Is.EqualTo(0));
        Assert.That(cache.ResidentValueBytes, Is.EqualTo(0));
        Assert.That(cache.TryPeek("a", out _), Is.False);
    }

    [Test]
    public void KeysSnapshot_is_safe_to_enumerate_while_removing()
    {
        var cache = new LeafPayloadCache();
        cache.SetBudget(1000);
        for (var i = 0; i < 10; i++)
            cache.Set($"k{i}", Val("v"));

        // Simulate a prune pass: enumerate the snapshot, remove from the store.
        foreach (var key in cache.KeysSnapshot())
            cache.Remove(key);

        Assert.That(cache.Count, Is.EqualTo(0));
    }

    [Test]
    public void Lowering_budget_evicts_on_next_set()
    {
        var cache = new LeafPayloadCache();
        cache.SetBudget(1000);
        cache.Set("a", Val("aaaaaaaaaa")); // 10
        cache.Set("b", Val("bbbbbbbbbb")); // 10
        Assert.That(cache.ResidentValueBytes, Is.EqualTo(20));

        // Tighten the budget below the resident total; the next Set applies it.
        cache.SetBudget(10);
        cache.Set("c", Val("cccccccccc")); // 10

        Assert.That(cache.ResidentValueBytes, Is.LessThanOrEqualTo(10));
    }
}
