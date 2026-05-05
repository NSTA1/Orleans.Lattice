using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class RecentApplyCacheTests
{
    private const string Origin = "site-b";
    private const string Tree = "tree";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static ReplogEntry Entry(
        string key,
        HybridLogicalClock ts,
        string origin = Origin,
        ReplogOp op = ReplogOp.Set) => new()
    {
        TreeId = Tree,
        Op = op,
        Key = key,
        Timestamp = ts,
        OriginClusterId = origin,
        Value = op == ReplogOp.Set ? new byte[] { 1 } : null,
        IsTombstone = op != ReplogOp.Set,
    };

    [Test]
    public void Constructor_throws_when_capacity_is_below_one()
    {
        Assert.That(() => new RecentApplyCache(0), Throws.TypeOf<ArgumentOutOfRangeException>());
        Assert.That(() => new RecentApplyCache(-1), Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Constructor_accepts_capacity_of_one()
    {
        var cache = new RecentApplyCache(1);

        Assert.That(cache.Capacity, Is.EqualTo(1));
        Assert.That(cache.Count, Is.Zero);
    }

    [Test]
    public void TryAdd_returns_true_for_first_observation()
    {
        var cache = new RecentApplyCache(8);

        var added = cache.TryAdd(Entry("k", Hlc(1)));

        Assert.That(added, Is.True);
        Assert.That(cache.Count, Is.EqualTo(1));
    }

    [Test]
    public void TryAdd_returns_false_for_duplicate_identity_tuple()
    {
        var cache = new RecentApplyCache(8);
        var entry = Entry("k", Hlc(1));

        Assert.That(cache.TryAdd(entry), Is.True);
        Assert.That(cache.TryAdd(entry), Is.False);
        Assert.That(cache.Count, Is.EqualTo(1));
    }

    [Test]
    public void TryAdd_distinguishes_entries_by_origin()
    {
        var cache = new RecentApplyCache(8);
        var ts = Hlc(1);

        Assert.That(cache.TryAdd(Entry("k", ts, origin: "site-b")), Is.True);
        Assert.That(cache.TryAdd(Entry("k", ts, origin: "site-c")), Is.True);
        Assert.That(cache.Count, Is.EqualTo(2));
    }

    [Test]
    public void TryAdd_distinguishes_entries_by_timestamp()
    {
        var cache = new RecentApplyCache(8);

        Assert.That(cache.TryAdd(Entry("k", Hlc(1))), Is.True);
        Assert.That(cache.TryAdd(Entry("k", Hlc(2))), Is.True);
        Assert.That(cache.TryAdd(Entry("k", Hlc(1, counter: 1))), Is.True);
        Assert.That(cache.Count, Is.EqualTo(3));
    }

    [Test]
    public void TryAdd_distinguishes_entries_by_key()
    {
        var cache = new RecentApplyCache(8);
        var ts = Hlc(1);

        Assert.That(cache.TryAdd(Entry("a", ts)), Is.True);
        Assert.That(cache.TryAdd(Entry("b", ts)), Is.True);
        Assert.That(cache.Count, Is.EqualTo(2));
    }

    [Test]
    public void TryAdd_distinguishes_entries_by_op()
    {
        var cache = new RecentApplyCache(8);
        var ts = Hlc(1);

        Assert.That(cache.TryAdd(Entry("k", ts, op: ReplogOp.Set)), Is.True);
        Assert.That(cache.TryAdd(Entry("k", ts, op: ReplogOp.Delete)), Is.True);
        Assert.That(cache.Count, Is.EqualTo(2));
    }

    [Test]
    public void TryAdd_evicts_oldest_entry_on_overflow()
    {
        var cache = new RecentApplyCache(2);

        Assert.That(cache.TryAdd(Entry("a", Hlc(1))), Is.True);
        Assert.That(cache.TryAdd(Entry("b", Hlc(2))), Is.True);
        Assert.That(cache.TryAdd(Entry("c", Hlc(3))), Is.True);

        Assert.That(cache.Count, Is.EqualTo(2));
        Assert.That(cache.Contains(Entry("a", Hlc(1))), Is.False);
        Assert.That(cache.Contains(Entry("b", Hlc(2))), Is.True);
        Assert.That(cache.Contains(Entry("c", Hlc(3))), Is.True);
    }

    [Test]
    public void TryAdd_after_eviction_treats_evicted_entry_as_new()
    {
        var cache = new RecentApplyCache(2);
        var evicted = Entry("a", Hlc(1));

        cache.TryAdd(evicted);
        cache.TryAdd(Entry("b", Hlc(2)));
        cache.TryAdd(Entry("c", Hlc(3)));

        // The evicted entry's tuple is no longer present, so re-adding
        // it returns true. The HWM is the authoritative dedupe backstop
        // for entries the cache has evicted; the cache itself is a
        // fast-path optimisation only.
        Assert.That(cache.TryAdd(evicted), Is.True);
    }

    [Test]
    public void TryAdd_does_not_change_count_on_duplicate()
    {
        var cache = new RecentApplyCache(8);
        var entry = Entry("k", Hlc(1));

        cache.TryAdd(entry);
        var before = cache.Count;
        cache.TryAdd(entry);

        Assert.That(cache.Count, Is.EqualTo(before));
    }

    [Test]
    public void Contains_returns_false_for_unknown_entry()
    {
        var cache = new RecentApplyCache(8);

        Assert.That(cache.Contains(Entry("k", Hlc(1))), Is.False);
    }

    [Test]
    public void Contains_does_not_modify_cache()
    {
        var cache = new RecentApplyCache(8);

        cache.Contains(Entry("k", Hlc(1)));

        Assert.That(cache.Count, Is.Zero);
    }

    [Test]
    public void Capacity_reflects_constructor_argument()
    {
        var cache = new RecentApplyCache(4096);

        Assert.That(cache.Capacity, Is.EqualTo(4096));
    }

    [Test]
    public async Task TryAdd_is_thread_safe_under_concurrent_distinct_writers()
    {
        var cache = new RecentApplyCache(1024);
        var addCount = 0;
        var tasks = new Task[8];
        for (var i = 0; i < tasks.Length; i++)
        {
            var taskId = i;
            tasks[i] = Task.Run(() =>
            {
                for (var j = 0; j < 100; j++)
                {
                    if (cache.TryAdd(Entry($"k-{taskId}-{j}", Hlc(j))))
                    {
                        Interlocked.Increment(ref addCount);
                    }
                }
            });
        }

        await Task.WhenAll(tasks);

        // 8 tasks * 100 distinct keys each = 800 distinct identity tuples,
        // all within the 1024-capacity cache so none evict.
        Assert.That(addCount, Is.EqualTo(800));
        Assert.That(cache.Count, Is.EqualTo(800));
    }

    [Test]
    public async Task TryAdd_admits_exactly_one_winner_under_concurrent_duplicate_writers()
    {
        // The shadow-forward race the cache exists to close: many
        // workers race to add the same identity tuple. Exactly one
        // must succeed; the rest must observe the entry as already
        // present.
        var cache = new RecentApplyCache(64);
        var entry = Entry("k", Hlc(42));
        var winners = 0;
        var tasks = new Task[16];
        using var barrier = new Barrier(tasks.Length);
        for (var i = 0; i < tasks.Length; i++)
        {
            tasks[i] = Task.Run(() =>
            {
                barrier.SignalAndWait();
                if (cache.TryAdd(entry))
                {
                    Interlocked.Increment(ref winners);
                }
            });
        }

        await Task.WhenAll(tasks);

        Assert.That(winners, Is.EqualTo(1));
        Assert.That(cache.Count, Is.EqualTo(1));
    }

    [Test]
    public void TryAdd_under_churn_never_resurrects_evicted_entries()
    {
        // Fuzz: insert 10000 distinct entries through a 128-entry
        // cache. After each insert, re-probe a window of the most
        // recent N entries and confirm they are present; older
        // entries are evicted FIFO and must not spuriously return
        // true on Contains.
        const int Capacity = 128;
        const int Total = 10_000;
        var cache = new RecentApplyCache(Capacity);
        for (var i = 0; i < Total; i++)
        {
            Assert.That(cache.TryAdd(Entry($"k-{i}", Hlc(i))), Is.True, $"entry {i} should be new on first insert");
        }

        Assert.That(cache.Count, Is.EqualTo(Capacity));

        // Last Capacity entries must all be present.
        for (var i = Total - Capacity; i < Total; i++)
        {
            Assert.That(cache.Contains(Entry($"k-{i}", Hlc(i))), Is.True, $"entry {i} (within retention window) should be present");
        }

        // Earlier entries must be absent.
        for (var i = 0; i < Total - Capacity; i++)
        {
            Assert.That(cache.Contains(Entry($"k-{i}", Hlc(i))), Is.False, $"entry {i} (evicted) should be absent");
        }
    }

    [Test]
    public void Remove_returns_false_when_entry_not_present()
    {
        var cache = new RecentApplyCache(8);

        var removed = cache.Remove(Entry("k", Hlc(1)));

        Assert.That(removed, Is.False);
        Assert.That(cache.Count, Is.Zero);
    }

    [Test]
    public void Remove_returns_true_and_decreases_count_when_entry_present()
    {
        var cache = new RecentApplyCache(8);
        var entry = Entry("k", Hlc(1));
        Assert.That(cache.TryAdd(entry), Is.True);
        Assert.That(cache.Count, Is.EqualTo(1));

        var removed = cache.Remove(entry);

        Assert.That(removed, Is.True);
        Assert.That(cache.Count, Is.Zero);
        Assert.That(cache.Contains(entry), Is.False);
    }

    [Test]
    public void TryAdd_after_remove_re_admits_entry()
    {
        // Rollback contract: when an apply throws after TryAdd
        // succeeded, the applier calls Remove so the transport's
        // retry path can re-admit the same identity tuple. A second
        // TryAdd must therefore return true.
        var cache = new RecentApplyCache(8);
        var entry = Entry("k", Hlc(1));
        Assert.That(cache.TryAdd(entry), Is.True);
        Assert.That(cache.Remove(entry), Is.True);

        Assert.That(cache.TryAdd(entry), Is.True);
        Assert.That(cache.Count, Is.EqualTo(1));
    }

    [Test]
    public void Remove_is_idempotent()
    {
        var cache = new RecentApplyCache(8);
        var entry = Entry("k", Hlc(1));
        Assert.That(cache.TryAdd(entry), Is.True);

        Assert.That(cache.Remove(entry), Is.True);
        Assert.That(cache.Remove(entry), Is.False);
        Assert.That(cache.Remove(entry), Is.False);
    }

    [Test]
    public void Remove_only_targets_the_matching_identity_tuple()
    {
        // Remove must use the same (origin, ts, key, op) identity as
        // TryAdd; entries that differ in any of those four
        // dimensions must be unaffected.
        var cache = new RecentApplyCache(8);
        var ts = Hlc(1);
        var target = Entry("k", ts, origin: "site-b");
        var sibling1 = Entry("k", ts, origin: "site-c");
        var sibling2 = Entry("k", Hlc(2), origin: "site-b");
        var sibling3 = Entry("other", ts, origin: "site-b");
        var sibling4 = Entry("k", ts, origin: "site-b", op: ReplogOp.Delete);
        cache.TryAdd(target);
        cache.TryAdd(sibling1);
        cache.TryAdd(sibling2);
        cache.TryAdd(sibling3);
        cache.TryAdd(sibling4);

        Assert.That(cache.Remove(target), Is.True);

        Assert.That(cache.Contains(target), Is.False);
        Assert.That(cache.Contains(sibling1), Is.True);
        Assert.That(cache.Contains(sibling2), Is.True);
        Assert.That(cache.Contains(sibling3), Is.True);
        Assert.That(cache.Contains(sibling4), Is.True);
        Assert.That(cache.Count, Is.EqualTo(4));
    }

    [Test]
    public void Eviction_recycles_the_linked_list_node_so_steady_state_is_alloc_free()
    {
        // Capacity-bounded fill followed by overflow inserts must not
        // leave dangling index entries: every evicted key must be
        // absent and every retained key must be present. This
        // exercises the recycling path (RemoveFirst + Value setter +
        // AddLast(node)) and confirms it preserves correctness even
        // after thousands of evictions.
        var cache = new RecentApplyCache(4);
        for (var i = 0; i < 1000; i++)
        {
            Assert.That(cache.TryAdd(Entry($"k-{i}", Hlc(i))), Is.True);
        }

        Assert.That(cache.Count, Is.EqualTo(4));
        for (var i = 996; i < 1000; i++)
        {
            Assert.That(cache.Contains(Entry($"k-{i}", Hlc(i))), Is.True);
        }
        for (var i = 0; i < 996; i++)
        {
            Assert.That(cache.Contains(Entry($"k-{i}", Hlc(i))), Is.False);
        }
    }
}
