using System.Text;

namespace Orleans.Lattice.Tests.Primitives;

[TestFixture]
public class RwSetTests
{
    private static byte[] E(string s) => Encoding.UTF8.GetBytes(s);

    [Test]
    public void New_set_is_empty_and_bottom()
    {
        var set = new RwSet();
        Assert.Multiple(() =>
        {
            Assert.That(set.IsEmpty, Is.True);
            Assert.That(set.IsBottom, Is.True);
            Assert.That(set.Count, Is.EqualTo(0));
            Assert.That(set.Adds, Is.Empty);
            Assert.That(set.Removes, Is.Empty);
            Assert.That(set.Tombstones, Is.Empty);
        });
    }

    [Test]
    public void Add_makes_element_a_member()
    {
        var set = new RwSet();
        set.Add(E("x"), "r1", 1);
        Assert.Multiple(() =>
        {
            Assert.That(set.Contains(E("x")), Is.True);
            Assert.That(set.IsEmpty, Is.False);
            Assert.That(set.IsBottom, Is.False);
            Assert.That(set.Count, Is.EqualTo(1));
        });
    }

    [Test]
    public void Add_throws_on_null_element()
    {
        var set = new RwSet();
        Assert.That(() => set.Add(null!, "r1", 1), Throws.ArgumentNullException);
    }

    [Test]
    public void Add_throws_on_empty_replica_id()
    {
        var set = new RwSet();
        Assert.Multiple(() =>
        {
            Assert.That(() => set.Add(E("x"), "", 1), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => set.Add(E("x"), null!, 1), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void Remove_throws_on_null_element()
    {
        var set = new RwSet();
        Assert.That(() => set.Remove(null!, "r1", 1), Throws.ArgumentNullException);
    }

    [Test]
    public void Remove_throws_on_empty_replica_id()
    {
        var set = new RwSet();
        Assert.Multiple(() =>
        {
            Assert.That(() => set.Remove(E("x"), "", 1), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => set.Remove(E("x"), null!, 1), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void Contains_throws_on_null_element()
    {
        var set = new RwSet();
        Assert.That(() => set.Contains(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Empty_array_is_a_valid_element()
    {
        var set = new RwSet();
        set.Add([], "r1", 1);
        Assert.Multiple(() =>
        {
            Assert.That(set.Contains([]), Is.True);
            Assert.That(set.Count, Is.EqualTo(1));
        });
    }

    [Test]
    public void Remove_after_add_removes_membership_and_returns_true()
    {
        var set = new RwSet();
        set.Add(E("x"), "r1", 1);
        var wasMember = set.Remove(E("x"), "r1", 2);
        Assert.Multiple(() =>
        {
            Assert.That(wasMember, Is.True);
            Assert.That(set.Contains(E("x")), Is.False);
            Assert.That(set.Count, Is.EqualTo(0));
        });
    }

    [Test]
    public void Remove_of_absent_element_returns_false()
    {
        var set = new RwSet();
        var wasMember = set.Remove(E("x"), "r1", 1);
        Assert.That(wasMember, Is.False);
    }

    [Test]
    public void Add_that_observes_remove_tombstones_it_and_readds()
    {
        var set = new RwSet();
        set.Add(E("x"), "r1", 1);
        set.Remove(E("x"), "r1", 2);
        Assert.That(set.Contains(E("x")), Is.False);

        set.Add(E("x"), "r1", 3);

        Assert.Multiple(() =>
        {
            Assert.That(set.Contains(E("x")), Is.True, "re-add after an observed remove restores membership");
            Assert.That(set.Tombstones, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void Add_with_no_observed_removes_tombstones_nothing()
    {
        var set = new RwSet();
        set.Add(E("x"), "r1", 1);
        Assert.That(set.Tombstones, Is.Empty);
    }

    [Test]
    public void Multiple_distinct_elements_are_tracked_independently()
    {
        var set = new RwSet();
        set.Add(E("a"), "r1", 1);
        set.Add(E("b"), "r1", 2);
        set.Remove(E("a"), "r1", 3);

        Assert.Multiple(() =>
        {
            Assert.That(set.Contains(E("a")), Is.False);
            Assert.That(set.Contains(E("b")), Is.True);
            Assert.That(set.Count, Is.EqualTo(1));
        });
    }

    [Test]
    public void Elements_enumerates_only_live_members()
    {
        var set = new RwSet();
        set.Add(E("a"), "r1", 1);
        set.Add(E("b"), "r1", 2);
        set.Add(E("c"), "r1", 3);
        set.Remove(E("b"), "r1", 4);

        var elements = set.Elements().Select(Encoding.UTF8.GetString).ToList();

        Assert.That(elements, Is.EquivalentTo(new[] { "a", "c" }));
    }

    [Test]
    public void Elements_is_empty_for_empty_set()
    {
        var set = new RwSet();
        Assert.That(set.Elements(), Is.Empty);
    }

    [Test]
    public void Merge_is_commutative()
    {
        var a = new RwSet();
        a.Add(E("a"), "A", 1);
        var b = new RwSet();
        b.Add(E("b"), "B", 1);
        b.Remove(E("b"), "B", 2);

        var ab = RwSet.Merge(a, b);
        var ba = RwSet.Merge(b, a);

        Assert.Multiple(() =>
        {
            Assert.That(ab.Contains(E("a")), Is.EqualTo(ba.Contains(E("a"))));
            Assert.That(ab.Contains(E("b")), Is.EqualTo(ba.Contains(E("b"))));
            Assert.That(ab.Count, Is.EqualTo(ba.Count));
        });
    }

    [Test]
    public void Merge_is_associative()
    {
        var a = new RwSet();
        a.Add(E("a"), "A", 1);
        var b = new RwSet();
        b.Add(E("b"), "B", 1);
        b.Remove(E("b"), "B", 2);
        var c = new RwSet();
        c.Add(E("c"), "C", 1);

        var left = RwSet.Merge(RwSet.Merge(a, b), c);
        var right = RwSet.Merge(a, RwSet.Merge(b, c));

        Assert.Multiple(() =>
        {
            Assert.That(left.Count, Is.EqualTo(right.Count));
            Assert.That(left.Elements().Select(Encoding.UTF8.GetString),
                Is.EquivalentTo(right.Elements().Select(Encoding.UTF8.GetString)));
        });
    }

    [Test]
    public void Merge_is_idempotent()
    {
        var a = new RwSet();
        a.Add(E("a"), "A", 1);
        a.Add(E("b"), "A", 2);

        var merged = RwSet.Merge(a, a);

        Assert.Multiple(() =>
        {
            Assert.That(merged.Count, Is.EqualTo(2));
            Assert.That(merged.Adds["YQ=="], Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void Merge_throws_on_null_operand()
    {
        var a = new RwSet();
        Assert.Multiple(() =>
        {
            Assert.That(() => RwSet.Merge(null!, a), Throws.ArgumentNullException);
            Assert.That(() => RwSet.Merge(a, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Concurrent_add_and_remove_converges_remove_wins()
    {
        // Replica A adds then removes x (observing only its own add dot).
        var a = new RwSet();
        a.Add(E("x"), "A", 1);
        a.Remove(E("x"), "A", 2);

        // Replica B concurrently adds x with a dot that observed neither A's
        // remove. After bidirectional merge both replicas agree x is absent:
        // A's remove dot is not tombstoned by B's add, so it survives and
        // suppresses the element - remove wins.
        var b = new RwSet();
        b.Add(E("x"), "B", 1);

        var ab = RwSet.Merge(a, b);
        var ba = RwSet.Merge(b, a);

        Assert.Multiple(() =>
        {
            Assert.That(ab.Contains(E("x")), Is.False);
            Assert.That(ba.Contains(E("x")), Is.False);
        });
    }

    [Test]
    public void Add_that_observes_all_removes_readds_after_merge()
    {
        var a = new RwSet();
        a.Add(E("x"), "A", 1);

        // Merge in B's remove, then add: A now observes the remove and
        // tombstones it, so x is a member again.
        var b = new RwSet();
        b.Add(E("x"), "B", 1);
        b.Remove(E("x"), "B", 2);
        a.MergeFrom(b);
        a.Add(E("x"), "A", 2);

        Assert.That(a.Contains(E("x")), Is.True);
    }

    [Test]
    public void Unobserved_concurrent_remove_keeps_element_out_after_readd()
    {
        // Model a re-add followed by a merge carrying a concurrent remove the
        // re-add never observed: the un-tombstoned remove keeps the element out.
        var set = new RwSet();
        set.Add(E("x"), "A", 1);
        set.Remove(E("x"), "A", 2);
        set.Add(E("x"), "A", 3); // observes and tombstones A's remove -> present
        // A second replica's remove that our re-add never observed arrives via
        // merge after the fact, so it is not tombstoned.
        set.Removes["eA=="].Add(new OrSetDot { ReplicaId = "B", Counter = 2 });

        Assert.That(set.Contains(E("x")), Is.False,
            "an un-tombstoned concurrent remove keeps the element out");
    }

    [Test]
    public void MergeFrom_unions_all_dot_maps()
    {
        var a = new RwSet();
        a.Add(E("a"), "A", 1);
        var b = new RwSet();
        b.Add(E("b"), "B", 1);
        b.Remove(E("b"), "B", 2);

        a.MergeFrom(b);

        Assert.Multiple(() =>
        {
            Assert.That(a.Adds, Has.Count.EqualTo(2));
            Assert.That(a.Removes, Has.Count.EqualTo(1));
            Assert.That(a.Contains(E("a")), Is.True);
            Assert.That(a.Contains(E("b")), Is.False);
        });
    }

    [Test]
    public void MergeFrom_throws_on_null()
    {
        var a = new RwSet();
        Assert.That(() => a.MergeFrom(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Clone_is_independent_of_source()
    {
        var a = new RwSet();
        a.Add(E("x"), "A", 1);
        var clone = a.Clone();
        a.Add(E("x"), "A", 2);

        Assert.Multiple(() =>
        {
            // Independence is that the clone did not observe the later add,
            // which the counters prove: both sides hold one dot per replica, but
            // the clone's is still the pre-clone one.
            Assert.That(clone.Adds["eA=="], Has.Count.EqualTo(1));
            Assert.That(clone.Adds["eA=="][0].Counter, Is.EqualTo(1));
            Assert.That(a.Adds["eA=="], Has.Count.EqualTo(1),
                "Repeated same-replica adds compact to the newest dot.");
            Assert.That(a.Adds["eA=="][0].Counter, Is.EqualTo(2));
        });
    }

    [Test]
    public void MergeDelta_unions_add_dots()
    {
        var set = new RwSet();
        var delta = new RwSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = E("x"), ReplicaId = "A", Counter = 1 } },
            Removes = Array.Empty<OrSetDeltaDot>(),
            Tombstones = Array.Empty<OrSetDeltaDot>(),
        };

        set.MergeDelta(delta);

        Assert.Multiple(() =>
        {
            Assert.That(set.Contains(E("x")), Is.True);
            Assert.That(set.Count, Is.EqualTo(1));
        });
    }

    [Test]
    public void MergeDelta_unions_remove_dots()
    {
        var set = new RwSet();
        set.Add(E("x"), "A", 1);
        var delta = new RwSetDelta
        {
            Adds = Array.Empty<OrSetDeltaDot>(),
            Removes = new[] { new OrSetDeltaDot { Element = E("x"), ReplicaId = "A", Counter = 2 } },
            Tombstones = Array.Empty<OrSetDeltaDot>(),
        };

        set.MergeDelta(delta);

        Assert.That(set.Contains(E("x")), Is.False);
    }

    [Test]
    public void MergeDelta_unions_tombstone_dots()
    {
        var set = new RwSet();
        set.Add(E("x"), "A", 1);
        set.Remove(E("x"), "A", 2);
        Assert.That(set.Contains(E("x")), Is.False);

        var delta = new RwSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = E("x"), ReplicaId = "A", Counter = 3 } },
            Removes = Array.Empty<OrSetDeltaDot>(),
            Tombstones = new[] { new OrSetDeltaDot { Element = E("x"), ReplicaId = "A", Counter = 2 } },
        };

        set.MergeDelta(delta);

        Assert.That(set.Contains(E("x")), Is.True);
    }

    [Test]
    public void MergeDelta_is_idempotent_under_duplicate_delivery()
    {
        var set = new RwSet();
        var delta = new RwSetDelta
        {
            Adds = new[] { new OrSetDeltaDot { Element = E("x"), ReplicaId = "A", Counter = 1 } },
            Removes = Array.Empty<OrSetDeltaDot>(),
            Tombstones = Array.Empty<OrSetDeltaDot>(),
        };

        set.MergeDelta(delta);
        set.MergeDelta(delta);

        Assert.That(set.Adds["eA=="], Has.Count.EqualTo(1));
    }

    [Test]
    public void MergeDelta_treats_null_collections_as_empty()
    {
        var set = new RwSet();
        set.Add(E("x"), "A", 1);

        set.MergeDelta(default);

        Assert.Multiple(() =>
        {
            Assert.That(set.Contains(E("x")), Is.True);
            Assert.That(set.Count, Is.EqualTo(1));
        });
    }
}
