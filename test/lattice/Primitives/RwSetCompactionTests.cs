namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Convergence and bounded-state guards for dot-history compaction as applied to <see cref="RwSet"/>.
/// </summary>
[TestFixture]
public class RwSetCompactionTests
{
    private static readonly byte[] Element = "element"u8.ToArray();

    private static void AddAsAccessorWould(RwSet set, byte[] element, string replicaId)
        => set.Add(element, replicaId, NextCounter(set, Key(element), replicaId));

    private static void RemoveAsAccessorWould(RwSet set, byte[] element, string replicaId)
        => set.Remove(element, replicaId, NextCounter(set, Key(element), replicaId));

    [Test]
    public void Repeated_add_and_remove_from_one_replica_keep_exactly_one_dot_per_side()
    {
        var adds = new RwSet();
        for (var i = 0; i < 1000; i++) AddAsAccessorWould(adds, Element, "local");

        var removes = new RwSet();
        for (var i = 0; i < 1000; i++) RemoveAsAccessorWould(removes, Element, "local");

        Assert.Multiple(() =>
        {
            Assert.That(adds.Adds[Key(Element)], Has.Count.EqualTo(1),
                "A replica re-adding the same element holds one add dot, not one per assertion.");
            Assert.That(adds.Adds[Key(Element)][0].Counter, Is.EqualTo(1000));
            Assert.That(removes.Removes[Key(Element)], Has.Count.EqualTo(1),
                "A replica re-removing the same element holds one remove dot, not one per assertion.");
            Assert.That(removes.Removes[Key(Element)][0].Counter, Is.EqualTo(1000));
        });
    }

    [Test]
    public void State_stays_bounded_by_replica_count_not_assertion_count()
    {
        var set = new RwSet();
        for (var round = 0; round < 200; round++)
        {
            AddAsAccessorWould(set, Element, "A");
            AddAsAccessorWould(set, Element, "B");
            AddAsAccessorWould(set, Element, "C");
        }

        Assert.That(set.Adds[Key(Element)], Has.Count.EqualTo(3),
            "600 assertions across 3 replicas cost 3 dots: state is O(replicas), not O(assertions).");
    }

    [Test]
    public void Add_concurrent_with_remove_elsewhere_still_loses()
    {
        var remover = new RwSet();
        AddAsAccessorWould(remover, Element, "A");
        RemoveAsAccessorWould(remover, Element, "A");

        var adder = new RwSet();
        AddAsAccessorWould(adder, Element, "B");

        Assert.Multiple(() =>
        {
            Assert.That(RwSet.Merge(remover, adder).Contains(Element), Is.False,
                "A remove concurrent with an add it never observed wins: remove-wins is preserved.");
            Assert.That(RwSet.Merge(adder, remover).Contains(Element), Is.False,
                "The remove-wins tie-break is order-independent.");
        });
    }

    [Test]
    public void Add_after_a_compacted_remove_still_tombstones_peer_old_remove()
    {
        var author = new RwSet();
        RemoveAsAccessorWould(author, Element, "A");
        var peerHoldingOldRemove = author.Clone();

        RemoveAsAccessorWould(author, Element, "A");
        AddAsAccessorWould(author, Element, "A");

        var merged = peerHoldingOldRemove.Clone();
        merged.MergeFrom(author);

        Assert.Multiple(() =>
        {
            Assert.That(author.Contains(Element), Is.True);
            Assert.That(merged.Contains(Element), Is.True,
                "The add tombstone covers the peer's older remove from the same replica, so no stale remove suppresses it.");
        });
    }

    [Test]
    public void Merge_is_commutative_associative_and_idempotent()
    {
        var a = new RwSet();
        AddAsAccessorWould(a, Element, "A");
        AddAsAccessorWould(a, Element, "A");

        var b = new RwSet();
        AddAsAccessorWould(b, "other"u8.ToArray(), "B");
        RemoveAsAccessorWould(b, "other"u8.ToArray(), "B");

        var c = new RwSet();
        AddAsAccessorWould(c, Element, "C");

        var left = RwSet.Merge(RwSet.Merge(a, b), c);
        var right = RwSet.Merge(a, RwSet.Merge(b, c));
        var once = RwSet.Merge(a, b);
        var twice = RwSet.Merge(once, b);

        Assert.Multiple(() =>
        {
            Assert.That(left.Contains(Element), Is.EqualTo(right.Contains(Element)), "associative membership");
            Assert.That(RwSet.Merge(a, b).Contains(Element), Is.EqualTo(RwSet.Merge(b, a).Contains(Element)),
                "commutative membership");
            Assert.That(twice.Contains(Element), Is.EqualTo(once.Contains(Element)), "idempotent membership");
            Assert.That(twice.Adds[Key(Element)], Has.Count.EqualTo(once.Adds[Key(Element)].Count),
                "Re-merging the same state adds no dots.");
        });
    }

    [Test]
    public void Folding_a_replicated_delta_repeatedly_does_not_grow_state()
    {
        var set = new RwSet();
        var delta = new RwSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = Element, ReplicaId = "peer", Counter = 7 } },
            Removes = new[] { new OrSetDeltaDot { Element = Element, ReplicaId = "peer", Counter = 8 } },
            Tombstones = Array.Empty<OrSetDeltaDot>(),
        };

        for (var i = 0; i < 50; i++) set.MergeDelta(delta);

        Assert.Multiple(() =>
        {
            Assert.That(set.Adds[Key(Element)], Has.Count.EqualTo(1));
            Assert.That(set.Removes[Key(Element)], Has.Count.EqualTo(1));
            Assert.That(set.Contains(Element), Is.False, "The live remove still wins after duplicate delta folds.");
        });
    }

    [Test]
    public void An_already_bloated_history_heals_on_the_next_merge()
    {
        var legacy = new RwSet();
        var key = Key(Element);
        legacy.Adds[key] = [];
        for (var i = 1; i <= 500; i++)
        {
            legacy.Adds[key].Add(new OrSetDot { ReplicaId = "local", Counter = i });
        }

        Assume.That(legacy.Adds[key], Has.Count.EqualTo(500), "arranged as a pre-fix row");

        legacy.MergeFrom(new RwSet());

        Assert.Multiple(() =>
        {
            Assert.That(legacy.Adds[key], Has.Count.EqualTo(1),
                "A merge normalises an inherited unbounded history without an operator step.");
            Assert.That(legacy.Adds[key][0].Counter, Is.EqualTo(500));
            Assert.That(legacy.Contains(Element), Is.True, "Healing never changes the observable value.");
        });
    }

    private static string Key(byte[] element) => Convert.ToBase64String(element);

    private static long NextCounter(RwSet set, string key, string replicaId)
    {
        long max = 0;
        Max(set.Adds, key, replicaId, ref max);
        Max(set.Removes, key, replicaId, ref max);
        Max(set.Tombstones, key, replicaId, ref max);
        return max + 1;
    }

    private static void Max(Dictionary<string, List<OrSetDot>> map, string key, string replicaId, ref long max)
    {
        if (!map.TryGetValue(key, out var dots)) return;
        foreach (var dot in dots)
        {
            if (dot.ReplicaId == replicaId && dot.Counter > max) max = dot.Counter;
        }
    }
}
