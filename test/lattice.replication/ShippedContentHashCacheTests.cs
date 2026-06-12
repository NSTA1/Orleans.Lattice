using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit coverage for the per-(tree, peer) shipped-content-hash dedup
/// measurement cache.
/// </summary>
[TestFixture]
public class ShippedContentHashCacheTests
{
    [Test]
    public void Constructor_throws_when_capacity_is_below_one()
    {
        Assert.That(() => new ShippedContentHashCache(0), Throws.TypeOf<ArgumentOutOfRangeException>());
        Assert.That(() => new ShippedContentHashCache(-1), Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Capacity_reflects_constructor_argument()
    {
        Assert.That(new ShippedContentHashCache(64).Capacity, Is.EqualTo(64));
    }

    [Test]
    public void Observe_returns_false_for_first_observation_of_a_key()
    {
        var cache = new ShippedContentHashCache(8);

        Assert.That(cache.Observe("k", 123UL), Is.False);
        Assert.That(cache.Count, Is.EqualTo(1));
    }

    [Test]
    public void Observe_returns_true_for_byte_identical_re_set()
    {
        var cache = new ShippedContentHashCache(8);

        Assert.That(cache.Observe("k", 123UL), Is.False);
        Assert.That(cache.Observe("k", 123UL), Is.True);
        Assert.That(cache.Count, Is.EqualTo(1));
    }

    [Test]
    public void Observe_returns_false_when_content_changes_for_same_key()
    {
        var cache = new ShippedContentHashCache(8);

        Assert.That(cache.Observe("k", 1UL), Is.False);
        Assert.That(cache.Observe("k", 2UL), Is.False);
        // The latest content is now recorded, so a re-send of the new
        // value is redundant while a re-send of the old value is not.
        Assert.That(cache.Observe("k", 2UL), Is.True);
        Assert.That(cache.Observe("k", 1UL), Is.False);
    }

    [Test]
    public void Observe_distinguishes_keys()
    {
        var cache = new ShippedContentHashCache(8);

        Assert.That(cache.Observe("a", 7UL), Is.False);
        Assert.That(cache.Observe("b", 7UL), Is.False);
        Assert.That(cache.Count, Is.EqualTo(2));
    }

    [Test]
    public void Observe_evicts_least_recently_used_key_on_overflow()
    {
        var cache = new ShippedContentHashCache(2);

        cache.Observe("a", 1UL);
        cache.Observe("b", 2UL);
        cache.Observe("c", 3UL); // evicts "a" (LRU)

        Assert.That(cache.Count, Is.EqualTo(2));
        Assert.That(cache.Contains("a"), Is.False);
        Assert.That(cache.Contains("b"), Is.True);
        Assert.That(cache.Contains("c"), Is.True);
    }

    [Test]
    public void Observe_promotes_touched_key_to_most_recently_used()
    {
        var cache = new ShippedContentHashCache(2);

        cache.Observe("a", 1UL);
        cache.Observe("b", 2UL);
        cache.Observe("a", 1UL); // touch "a" -> "b" is now LRU
        cache.Observe("c", 3UL); // evicts "b", not "a"

        Assert.That(cache.Contains("a"), Is.True);
        Assert.That(cache.Contains("b"), Is.False);
        Assert.That(cache.Contains("c"), Is.True);
    }

    [Test]
    public void Observe_after_eviction_treats_re_added_key_as_new()
    {
        var cache = new ShippedContentHashCache(2);

        cache.Observe("a", 1UL);
        cache.Observe("b", 2UL);
        cache.Observe("c", 3UL); // evicts "a"

        // "a" was evicted, so re-observing the same content is not
        // redundant - the cache no longer holds its prior digest.
        Assert.That(cache.Observe("a", 1UL), Is.False);
    }

    [Test]
    public void Observe_throws_when_key_is_null()
    {
        var cache = new ShippedContentHashCache(8);

        Assert.That(() => cache.Observe(null!, 1UL), Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Contains_throws_when_key_is_null()
    {
        var cache = new ShippedContentHashCache(8);

        Assert.That(() => cache.Contains(null!), Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Observe_is_thread_safe_under_concurrent_distinct_keys()
    {
        var cache = new ShippedContentHashCache(2048);
        var tasks = new Task[8];
        for (var i = 0; i < tasks.Length; i++)
        {
            var taskId = i;
            tasks[i] = Task.Run(() =>
            {
                for (var j = 0; j < 100; j++)
                {
                    cache.Observe($"k-{taskId}-{j}", (ulong)j);
                }
            });
        }

        Task.WaitAll(tasks);

        Assert.That(cache.Count, Is.EqualTo(800));
    }
}
