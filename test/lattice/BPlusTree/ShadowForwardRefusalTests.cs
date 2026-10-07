using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #4478: following a shadow-forward the resized copy refused because a
/// split moved the key's slot.
/// </summary>
[TestFixture]
public class ShadowForwardRefusalTests
{
    [Test]
    public void NextShard_follows_the_refusal_to_the_shard_it_names()
    {
        Assert.That(ShadowForwardRefusal.NextShard(new StaleShardRoutingException(0, 5, 7), 0, hopsTaken: 0), Is.EqualTo(5));
    }

    [Test]
    public void NextShard_stops_once_the_hops_run_out()
    {
        var refusal = new StaleShardRoutingException(0, 5, 7);

        Assert.Multiple(() =>
        {
            Assert.That(ShadowForwardRefusal.NextShard(refusal, 0, ShadowForwardRefusal.MaxHops - 1), Is.EqualTo(5));
            Assert.That(ShadowForwardRefusal.NextShard(refusal, 0, ShadowForwardRefusal.MaxHops), Is.Null);
        });
    }

    [TestCase(-1)]
    [TestCase(3)]
    public void NextShard_stops_on_a_refusal_naming_no_other_shard(int target)
    {
        Assert.That(ShadowForwardRefusal.NextShard(new StaleShardRoutingException(3, target, -1), 3, hopsTaken: 0), Is.Null);
    }

    [Test]
    public void NextShard_rejects_a_null_refusal()
    {
        Assert.Throws<ArgumentNullException>(() => ShadowForwardRefusal.NextShard(null!, 0, 0));
    }

    [Test]
    public void PerEntry_splits_a_set_many_batch_into_single_entry_batches_in_order()
    {
        List<KeyValuePair<string, byte[]>> entries = [new("a", [1]), new("b", [2])];

        var parts = ShadowForwardRefusal.PerEntry(entries);

        Assert.That(parts.Select(p => p.Single().Key), Is.EqualTo(new[] { "a", "b" }));
    }

    [Test]
    public void PerEntry_keeps_the_predicate_on_every_conditional_entry()
    {
        var predicate = LatticePredicateNode.Member("a");
        List<KeyValuePair<string, byte[]>> entries = [new("a", [1]), new("b", [2])];

        var parts = ShadowForwardRefusal.PerEntry((entries, predicate));

        Assert.Multiple(() =>
        {
            Assert.That(parts.Select(p => p.Entries.Single().Key), Is.EqualTo(new[] { "a", "b" }));
            Assert.That(parts.All(p => Equals(p.Predicate, predicate)), Is.True);
        });
    }

    [Test]
    public void PerEntry_splits_a_merge_batch_keeping_its_comparer()
    {
        var entries = new Dictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal)
        {
            ["a"] = LwwValue<byte[]>.Create([1], HybridLogicalClock.Zero),
            ["b"] = LwwValue<byte[]>.Create([2], HybridLogicalClock.Zero),
        };

        var parts = ShadowForwardRefusal.PerEntry(entries);

        Assert.Multiple(() =>
        {
            Assert.That(parts.Select(p => p.Single().Key).Order(), Is.EqualTo(new[] { "a", "b" }));
            Assert.That(parts.All(p => ReferenceEquals(p.Comparer, StringComparer.Ordinal)), Is.True);
        });
    }

    [Test]
    public void PerEntry_rejects_null_batches()
    {
        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => ShadowForwardRefusal.PerEntry((List<KeyValuePair<string, byte[]>>)null!));
            Assert.Throws<ArgumentNullException>(() => ShadowForwardRefusal.PerEntry(((List<KeyValuePair<string, byte[]>>)null!, LatticePredicateNode.Member("a"))));
            Assert.Throws<ArgumentNullException>(() => ShadowForwardRefusal.PerEntry((Dictionary<string, LwwValue<byte[]>>)null!));
        });
    }
}
