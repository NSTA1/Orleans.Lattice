namespace Orleans.Lattice.Tests.Primitives;

[TestFixture]
public class RwFlagTests
{
    [Test]
    public void New_flag_is_not_enabled()
    {
        var flag = new RwFlag();
        Assert.Multiple(() =>
        {
            Assert.That(flag.IsEnabled, Is.False);
            Assert.That(flag.IsBottom, Is.True);
            Assert.That(flag.Enables, Is.Empty);
            Assert.That(flag.Disables, Is.Empty);
            Assert.That(flag.Tombstones, Is.Empty);
        });
    }

    [Test]
    public void Enable_makes_flag_enabled()
    {
        var flag = new RwFlag();
        flag.Enable("r1", 1);
        Assert.Multiple(() =>
        {
            Assert.That(flag.IsEnabled, Is.True);
            Assert.That(flag.IsBottom, Is.False);
            Assert.That(flag.Enables, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void Enable_throws_on_empty_replica_id()
    {
        var flag = new RwFlag();
        Assert.Multiple(() =>
        {
            Assert.That(() => flag.Enable("", 1), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => flag.Enable(null!, 1), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void Disable_throws_on_empty_replica_id()
    {
        var flag = new RwFlag();
        Assert.Multiple(() =>
        {
            Assert.That(() => flag.Disable("", 1), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => flag.Disable(null!, 1), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void Disable_after_enable_makes_flag_not_enabled()
    {
        var flag = new RwFlag();
        flag.Enable("r1", 1);
        flag.Disable("r1", 2);
        Assert.Multiple(() =>
        {
            Assert.That(flag.IsEnabled, Is.False);
            Assert.That(flag.Disables, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void Enable_tombstones_observed_disable_and_returns_true()
    {
        var flag = new RwFlag();
        flag.Enable("r1", 1);
        flag.Disable("r1", 2);
        var tombstoned = flag.Enable("r1", 3);
        Assert.Multiple(() =>
        {
            Assert.That(tombstoned, Is.True);
            Assert.That(flag.IsEnabled, Is.True);
            Assert.That(flag.Tombstones, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void Enable_on_flag_with_no_disables_tombstones_nothing()
    {
        var flag = new RwFlag();
        var tombstoned = flag.Enable("r1", 1);
        Assert.Multiple(() =>
        {
            Assert.That(tombstoned, Is.False);
            Assert.That(flag.Tombstones, Is.Empty);
        });
    }

    [Test]
    public void Reenable_after_disable_with_fresh_dot_makes_flag_enabled()
    {
        var flag = new RwFlag();
        flag.Enable("r1", 1);
        flag.Disable("r1", 2);
        flag.Enable("r1", 3);
        Assert.That(flag.IsEnabled, Is.True);
    }

    [Test]
    public void Merge_is_commutative()
    {
        var a = new RwFlag();
        a.Enable("A", 1);
        var b = new RwFlag();
        b.Enable("B", 1);
        b.Disable("B", 2);

        var ab = RwFlag.Merge(a, b);
        var ba = RwFlag.Merge(b, a);

        Assert.Multiple(() =>
        {
            Assert.That(ab.IsEnabled, Is.EqualTo(ba.IsEnabled));
            Assert.That(ab.Enables, Is.EquivalentTo(ba.Enables));
            Assert.That(ab.Disables, Is.EquivalentTo(ba.Disables));
            Assert.That(ab.Tombstones, Is.EquivalentTo(ba.Tombstones));
        });
    }

    [Test]
    public void Merge_is_associative()
    {
        var a = new RwFlag();
        a.Enable("A", 1);
        var b = new RwFlag();
        b.Enable("B", 1);
        b.Disable("B", 2);
        var c = new RwFlag();
        c.Enable("C", 1);

        var left = RwFlag.Merge(RwFlag.Merge(a, b), c);
        var right = RwFlag.Merge(a, RwFlag.Merge(b, c));

        Assert.Multiple(() =>
        {
            Assert.That(left.IsEnabled, Is.EqualTo(right.IsEnabled));
            Assert.That(left.Enables, Is.EquivalentTo(right.Enables));
            Assert.That(left.Disables, Is.EquivalentTo(right.Disables));
            Assert.That(left.Tombstones, Is.EquivalentTo(right.Tombstones));
        });
    }

    [Test]
    public void Merge_is_idempotent()
    {
        var a = new RwFlag();
        a.Enable("A", 1);
        a.Enable("B", 1);

        var merged = RwFlag.Merge(a, a);

        Assert.Multiple(() =>
        {
            Assert.That(merged.Enables, Has.Count.EqualTo(2));
            Assert.That(merged.IsEnabled, Is.True);
        });
    }

    [Test]
    public void Merge_throws_on_null_operand()
    {
        var a = new RwFlag();
        Assert.Multiple(() =>
        {
            Assert.That(() => RwFlag.Merge(null!, a), Throws.ArgumentNullException);
            Assert.That(() => RwFlag.Merge(a, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Concurrent_enable_and_disable_converges_remove_wins()
    {
        // Replica A enables then disables (observing only its own enable dot).
        var a = new RwFlag();
        a.Enable("A", 1);
        a.Disable("A", 2);

        // Replica B concurrently enables with a dot that observed neither A's
        // disable. After bidirectional merge both replicas agree the flag is
        // disabled: A's disable dot is not tombstoned by B's enable, so it
        // survives and suppresses the flag - remove wins.
        var b = new RwFlag();
        b.Enable("B", 1);

        var ab = RwFlag.Merge(a, b);
        var ba = RwFlag.Merge(b, a);

        Assert.Multiple(() =>
        {
            Assert.That(ab.IsEnabled, Is.False);
            Assert.That(ba.IsEnabled, Is.False);
        });
    }

    [Test]
    public void Enable_that_observes_all_disables_reenables_after_merge()
    {
        var a = new RwFlag();
        a.Enable("A", 1);

        // Merge in B's disable, then enable: A now observes the disable and
        // tombstones it, so the flag is enabled again.
        var b = new RwFlag();
        b.Enable("B", 1);
        b.Disable("B", 2);
        a.MergeFrom(b);
        a.Enable("A", 2);

        Assert.That(a.IsEnabled, Is.True);
    }

    [Test]
    public void Unobserved_concurrent_disable_keeps_flag_off_after_reenable()
    {
        // Two replicas disable concurrently; a re-enable that observes only
        // one disable leaves the other live, so the flag stays off.
        var a = new RwFlag();
        a.Enable("A", 1);
        a.Disable("A", 2);

        var b = new RwFlag();
        b.Enable("B", 1);
        b.Disable("B", 2);

        a.MergeFrom(b);
        // A re-enables but its tombstones only cover the disables it observed.
        a.Enable("A", 3);

        Assert.That(a.IsEnabled, Is.True, "A observed both disables before re-enabling, so both are tombstoned");

        // Now model a re-enable that observed only one disable: drop B's
        // tombstone to simulate the un-observed case.
        var c = new RwFlag();
        c.Enable("A", 1);
        c.Disable("A", 2);
        c.Disable("B", 2);
        c.Enable("A", 3);
        c.Tombstones.RemoveAll(d => d.ReplicaId == "B");
        Assert.That(c.IsEnabled, Is.False, "an un-tombstoned concurrent disable keeps the flag off");
    }

    [Test]
    public void MergeFrom_unions_all_dot_sets()
    {
        var a = new RwFlag();
        a.Enable("A", 1);
        var b = new RwFlag();
        b.Enable("B", 1);
        b.Disable("B", 2);

        a.MergeFrom(b);

        Assert.Multiple(() =>
        {
            Assert.That(a.Enables, Has.Count.EqualTo(2));
            Assert.That(a.Disables, Has.Count.EqualTo(1));
            Assert.That(a.IsEnabled, Is.False);
        });
    }

    [Test]
    public void MergeFrom_throws_on_null()
    {
        var a = new RwFlag();
        Assert.That(() => a.MergeFrom(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Clone_is_independent_of_source()
    {
        var a = new RwFlag();
        a.Enable("A", 1);
        var clone = a.Clone();
        a.Enable("A", 2);

        Assert.Multiple(() =>
        {
            // Independence is that the clone did not observe the later enable,
            // which the counters prove: both sides hold one dot per replica, but
            // the clone's is still the pre-clone one.
            Assert.That(clone.Enables, Has.Count.EqualTo(1));
            Assert.That(clone.Enables[0].Counter, Is.EqualTo(1));
            Assert.That(a.Enables, Has.Count.EqualTo(1),
                "Repeated same-replica enables compact to the newest dot.");
            Assert.That(a.Enables[0].Counter, Is.EqualTo(2));
        });
    }

    [Test]
    public void MergeDelta_unions_enable_dots()
    {
        var flag = new RwFlag();
        var delta = new RwFlagDelta
        {
            Enables = new[] { new OrSetDot { ReplicaId = "A", Counter = 1 } },
            Disables = Array.Empty<OrSetDot>(),
            Tombstones = Array.Empty<OrSetDot>(),
        };

        flag.MergeDelta(delta);

        Assert.Multiple(() =>
        {
            Assert.That(flag.IsEnabled, Is.True);
            Assert.That(flag.Enables, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void MergeDelta_unions_disable_dots()
    {
        var flag = new RwFlag();
        flag.Enable("A", 1);
        var delta = new RwFlagDelta
        {
            Enables = Array.Empty<OrSetDot>(),
            Disables = new[] { new OrSetDot { ReplicaId = "A", Counter = 2 } },
            Tombstones = Array.Empty<OrSetDot>(),
        };

        flag.MergeDelta(delta);

        Assert.That(flag.IsEnabled, Is.False);
    }

    [Test]
    public void MergeDelta_unions_tombstone_dots()
    {
        var flag = new RwFlag();
        flag.Enable("A", 1);
        flag.Disable("A", 2);
        Assert.That(flag.IsEnabled, Is.False);

        var delta = new RwFlagDelta
        {
            Enables = new[] { new OrSetDot { ReplicaId = "A", Counter = 3 } },
            Disables = Array.Empty<OrSetDot>(),
            Tombstones = new[] { new OrSetDot { ReplicaId = "A", Counter = 2 } },
        };

        flag.MergeDelta(delta);

        Assert.That(flag.IsEnabled, Is.True);
    }

    [Test]
    public void MergeDelta_is_idempotent_under_duplicate_delivery()
    {
        var flag = new RwFlag();
        var delta = new RwFlagDelta
        {
            Enables = new[] { new OrSetDot { ReplicaId = "A", Counter = 1 } },
            Disables = Array.Empty<OrSetDot>(),
            Tombstones = Array.Empty<OrSetDot>(),
        };

        flag.MergeDelta(delta);
        flag.MergeDelta(delta);

        Assert.That(flag.Enables, Has.Count.EqualTo(1));
    }

    [Test]
    public void MergeDelta_treats_null_collections_as_empty()
    {
        var flag = new RwFlag();
        flag.Enable("A", 1);

        flag.MergeDelta(default);

        Assert.Multiple(() =>
        {
            Assert.That(flag.IsEnabled, Is.True);
            Assert.That(flag.Enables, Has.Count.EqualTo(1));
        });
    }
}
