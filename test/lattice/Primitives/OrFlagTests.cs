namespace Orleans.Lattice.Tests.Primitives;

[TestFixture]
public class OrFlagTests
{
    [Test]
    public void New_flag_is_not_enabled()
    {
        var flag = new OrFlag();
        Assert.Multiple(() =>
        {
            Assert.That(flag.IsEnabled, Is.False);
            Assert.That(flag.IsBottom, Is.True);
            Assert.That(flag.Enables, Is.Empty);
            Assert.That(flag.Tombstones, Is.Empty);
        });
    }

    [Test]
    public void Enable_makes_flag_enabled()
    {
        var flag = new OrFlag();
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
        var flag = new OrFlag();
        Assert.Multiple(() =>
        {
            Assert.That(() => flag.Enable("", 1), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => flag.Enable(null!, 1), Throws.InstanceOf<ArgumentException>());
        });
    }

    [Test]
    public void Disable_after_enable_makes_flag_not_enabled()
    {
        var flag = new OrFlag();
        flag.Enable("r1", 1);
        var changed = flag.Disable();
        Assert.Multiple(() =>
        {
            Assert.That(changed, Is.True);
            Assert.That(flag.IsEnabled, Is.False);
            Assert.That(flag.Tombstones, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void Disable_on_empty_flag_is_no_op()
    {
        var flag = new OrFlag();
        var changed = flag.Disable();
        Assert.Multiple(() =>
        {
            Assert.That(changed, Is.False);
            Assert.That(flag.Tombstones, Is.Empty);
        });
    }

    [Test]
    public void Disable_is_idempotent_for_already_observed_dots()
    {
        var flag = new OrFlag();
        flag.Enable("r1", 1);
        flag.Disable();
        var changed = flag.Disable();
        Assert.Multiple(() =>
        {
            Assert.That(changed, Is.False);
            Assert.That(flag.Tombstones, Has.Count.EqualTo(1));
        });
    }

    [Test]
    public void Reenable_after_disable_with_fresh_dot_makes_flag_enabled()
    {
        var flag = new OrFlag();
        flag.Enable("r1", 1);
        flag.Disable();
        flag.Enable("r1", 2);
        Assert.That(flag.IsEnabled, Is.True);
    }

    [Test]
    public void Merge_is_commutative()
    {
        var a = new OrFlag();
        a.Enable("A", 1);
        var b = new OrFlag();
        b.Enable("B", 1);

        var ab = OrFlag.Merge(a, b);
        var ba = OrFlag.Merge(b, a);

        Assert.Multiple(() =>
        {
            Assert.That(ab.IsEnabled, Is.EqualTo(ba.IsEnabled));
            Assert.That(ab.Enables, Is.EquivalentTo(ba.Enables));
            Assert.That(ab.Tombstones, Is.EquivalentTo(ba.Tombstones));
        });
    }

    [Test]
    public void Merge_is_associative()
    {
        var a = new OrFlag();
        a.Enable("A", 1);
        var b = new OrFlag();
        b.Enable("B", 1);
        b.Disable();
        var c = new OrFlag();
        c.Enable("C", 1);

        var left = OrFlag.Merge(OrFlag.Merge(a, b), c);
        var right = OrFlag.Merge(a, OrFlag.Merge(b, c));

        Assert.Multiple(() =>
        {
            Assert.That(left.IsEnabled, Is.EqualTo(right.IsEnabled));
            Assert.That(left.Enables, Is.EquivalentTo(right.Enables));
            Assert.That(left.Tombstones, Is.EquivalentTo(right.Tombstones));
        });
    }

    [Test]
    public void Merge_is_idempotent()
    {
        var a = new OrFlag();
        a.Enable("A", 1);
        a.Enable("B", 1);

        var merged = OrFlag.Merge(a, a);

        Assert.Multiple(() =>
        {
            Assert.That(merged.Enables, Has.Count.EqualTo(2));
            Assert.That(merged.IsEnabled, Is.True);
        });
    }

    [Test]
    public void Merge_throws_on_null_operand()
    {
        var a = new OrFlag();
        Assert.Multiple(() =>
        {
            Assert.That(() => OrFlag.Merge(null!, a), Throws.ArgumentNullException);
            Assert.That(() => OrFlag.Merge(a, null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public void Concurrent_enable_and_disable_converges_enable_wins()
    {
        // Replica A enables and then disables (observing only its own dot).
        var a = new OrFlag();
        a.Enable("A", 1);
        a.Disable();

        // Replica B concurrently enables with a dot A never observed.
        var b = new OrFlag();
        b.Enable("B", 1);

        // After bidirectional merge both replicas agree the flag is enabled:
        // B's enable dot is not in A's tombstone set, so it survives.
        var ab = OrFlag.Merge(a, b);
        var ba = OrFlag.Merge(b, a);

        Assert.Multiple(() =>
        {
            Assert.That(ab.IsEnabled, Is.True);
            Assert.That(ba.IsEnabled, Is.True);
        });
    }

    [Test]
    public void Disable_only_tombstones_observed_enable_dots()
    {
        var a = new OrFlag();
        a.Enable("A", 1);

        // Merge in B's enable, then disable: A now observes both dots and
        // tombstones both, so the flag is fully disabled after the disable.
        var b = new OrFlag();
        b.Enable("B", 1);
        a.MergeFrom(b);
        a.Disable();

        Assert.That(a.IsEnabled, Is.False);
    }

    [Test]
    public void MergeFrom_unions_enable_and_tombstone_dots()
    {
        var a = new OrFlag();
        a.Enable("A", 1);
        var b = new OrFlag();
        b.Enable("B", 1);
        b.Disable();

        a.MergeFrom(b);

        Assert.Multiple(() =>
        {
            Assert.That(a.Enables, Has.Count.EqualTo(2));
            Assert.That(a.Tombstones, Has.Count.EqualTo(1));
            Assert.That(a.IsEnabled, Is.True);
        });
    }

    [Test]
    public void MergeFrom_throws_on_null()
    {
        var a = new OrFlag();
        Assert.That(() => a.MergeFrom(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void Clone_is_independent_of_source()
    {
        var a = new OrFlag();
        a.Enable("A", 1);
        var clone = a.Clone();
        a.Enable("A", 2);

        Assert.Multiple(() =>
        {
            // Independence is that the clone did not observe the later enable.
            // The source holds one dot per replica rather than both, because a
            // replica's later dot supersedes its own earlier one.
            Assert.That(clone.Enables, Has.Count.EqualTo(1));
            Assert.That(clone.Enables[0].Counter, Is.EqualTo(1));
            Assert.That(a.Enables, Has.Count.EqualTo(1));
            Assert.That(a.Enables[0].Counter, Is.EqualTo(2));
        });
    }

    [Test]
    public void MergeDelta_unions_enable_dots()
    {
        var flag = new OrFlag();
        var delta = new OrFlagDelta
        {
            Enables = new[] { new OrSetDot { ReplicaId = "A", Counter = 1 } },
            Disables = Array.Empty<OrSetDot>(),
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
        var flag = new OrFlag();
        flag.Enable("A", 1);
        var delta = new OrFlagDelta
        {
            Enables = Array.Empty<OrSetDot>(),
            Disables = new[] { new OrSetDot { ReplicaId = "A", Counter = 1 } },
        };

        flag.MergeDelta(delta);

        Assert.That(flag.IsEnabled, Is.False);
    }

    [Test]
    public void MergeDelta_is_idempotent_under_duplicate_delivery()
    {
        var flag = new OrFlag();
        var delta = new OrFlagDelta
        {
            Enables = new[] { new OrSetDot { ReplicaId = "A", Counter = 1 } },
            Disables = Array.Empty<OrSetDot>(),
        };

        flag.MergeDelta(delta);
        flag.MergeDelta(delta);

        Assert.That(flag.Enables, Has.Count.EqualTo(1));
    }

    [Test]
    public void MergeDelta_treats_null_collections_as_empty()
    {
        var flag = new OrFlag();
        flag.Enable("A", 1);

        flag.MergeDelta(default);

        Assert.Multiple(() =>
        {
            Assert.That(flag.IsEnabled, Is.True);
            Assert.That(flag.Enables, Has.Count.EqualTo(1));
        });
    }
}
