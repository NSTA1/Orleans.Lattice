using Orleans.Lattice;

namespace Orleans.Lattice.Tests.Primitives;

[TestFixture]
public class GCounterTests
{
    [Test]
    public void New_counter_has_zero_value()
    {
        var c = new GCounter();
        Assert.That(c.Value, Is.EqualTo(0));
    }

    [Test]
    public void New_counter_is_bottom()
    {
        var c = new GCounter();
        Assert.That(c.IsBottom, Is.True);
    }

    [Test]
    public void Increment_advances_replica_component()
    {
        var c = new GCounter();
        c.Increment("r1", 3);
        Assert.That(c.Value, Is.EqualTo(3));
        Assert.That(c.Increments["r1"], Is.EqualTo(3));
        Assert.That(c.IsBottom, Is.False);
    }

    [Test]
    public void Increment_accumulates_on_same_replica()
    {
        var c = new GCounter();
        c.Increment("r1", 3);
        c.Increment("r1", 4);
        Assert.That(c.Value, Is.EqualTo(7));
        Assert.That(c.Increments["r1"], Is.EqualTo(7));
    }

    [Test]
    public void Increment_default_amount_is_one()
    {
        var c = new GCounter();
        c.Increment("r1");
        c.Increment("r1");
        Assert.That(c.Value, Is.EqualTo(2));
    }

    [Test]
    public void Increment_with_zero_is_no_op()
    {
        var c = new GCounter();
        c.Increment("r1", 0);
        Assert.That(c.Increments, Is.Empty);
        Assert.That(c.IsBottom, Is.True);
    }

    [Test]
    public void Increment_throws_on_negative_amount()
    {
        var c = new GCounter();
        Assert.That(() => c.Increment("r1", -1), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Increment_throws_on_empty_replica_id()
    {
        var c = new GCounter();
        Assert.That(() => c.Increment("", 1), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => c.Increment(null!, 1), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Value_sums_all_replica_components()
    {
        var c = new GCounter();
        c.Increment("r1", 5);
        c.Increment("r2", 7);
        c.Increment("r3", 4);
        Assert.That(c.Value, Is.EqualTo(16));
    }

    [Test]
    public void Merge_takes_pointwise_max_per_replica()
    {
        var a = new GCounter();
        a.Increment("r1", 5);
        a.Increment("r2", 1);

        var b = new GCounter();
        b.Increment("r1", 3);
        b.Increment("r2", 4);

        var merged = GCounter.Merge(a, b);
        Assert.That(merged.Increments["r1"], Is.EqualTo(5));
        Assert.That(merged.Increments["r2"], Is.EqualTo(4));
        Assert.That(merged.Value, Is.EqualTo(9));
    }

    [Test]
    public void Merge_is_commutative()
    {
        var a = new GCounter(); a.Increment("r1", 5); a.Increment("r2", 1);
        var b = new GCounter(); b.Increment("r1", 3); b.Increment("r2", 4);

        var ab = GCounter.Merge(a, b);
        var ba = GCounter.Merge(b, a);
        Assert.That(ab.Value, Is.EqualTo(ba.Value));
        Assert.That(ab.Increments, Is.EquivalentTo(ba.Increments));
    }

    [Test]
    public void Merge_is_idempotent()
    {
        var a = new GCounter(); a.Increment("r1", 7); a.Increment("r2", 3);
        var twice = GCounter.Merge(a, a);
        Assert.That(twice.Value, Is.EqualTo(10));
        Assert.That(twice.Increments, Is.EquivalentTo(a.Increments));
    }

    [Test]
    public void Merge_is_associative()
    {
        var a = new GCounter(); a.Increment("r1", 3);
        var b = new GCounter(); b.Increment("r2", 2);
        var c = new GCounter(); c.Increment("r3", 5);

        var left = GCounter.Merge(GCounter.Merge(a, b), c);
        var right = GCounter.Merge(a, GCounter.Merge(b, c));
        Assert.That(left.Value, Is.EqualTo(right.Value));
        Assert.That(left.Increments, Is.EquivalentTo(right.Increments));
    }

    [Test]
    public void Merge_throws_on_null_left()
    {
        var c = new GCounter();
        Assert.That(() => GCounter.Merge(null!, c), Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Merge_throws_on_null_right()
    {
        var c = new GCounter();
        Assert.That(() => GCounter.Merge(c, null!), Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Merge_does_not_mutate_operands()
    {
        var a = new GCounter(); a.Increment("r1", 5);
        var b = new GCounter(); b.Increment("r1", 9);

        _ = GCounter.Merge(a, b);

        Assert.That(a.Increments["r1"], Is.EqualTo(5));
        Assert.That(b.Increments["r1"], Is.EqualTo(9));
    }

    [Test]
    public void MergeFrom_throws_on_null_other()
    {
        var c = new GCounter();
        Assert.That(() => c.MergeFrom(null!), Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void MergeFrom_matches_static_Merge()
    {
        var a = new GCounter();
        a.Increment("r1", 5);
        a.Increment("r2", 7);

        var b = new GCounter();
        b.Increment("r1", 3); // smaller than a's 5 - pointwise-max keeps a's
        b.Increment("r2", 9); // larger than a's 7 - pointwise-max keeps b's
        b.Increment("r3", 4);

        var staticResult = GCounter.Merge(a, b);
        var inPlace = a.Clone();
        inPlace.MergeFrom(b);

        Assert.That(inPlace.Value, Is.EqualTo(staticResult.Value));
        Assert.That(inPlace.Increments["r1"], Is.EqualTo(5));
        Assert.That(inPlace.Increments["r2"], Is.EqualTo(9));
        Assert.That(inPlace.Increments["r3"], Is.EqualTo(4));
    }

    [Test]
    public void Concurrent_increments_on_different_replicas_both_count()
    {
        var r1 = new GCounter(); r1.Increment("r1", 2);
        var r2 = new GCounter(); r2.Increment("r2", 3);
        var merged = GCounter.Merge(r1, r2);
        Assert.That(merged.Value, Is.EqualTo(5));
    }

    [Test]
    public void MergeDelta_applies_pointwise_max()
    {
        var c = new GCounter();
        c.Increment("r1", 5);
        c.Increment("r2", 2);

        c.MergeDelta(new GCounterDelta { Increments = new Dictionary<string, long> { ["r1"] = 3, ["r2"] = 8, ["r3"] = 4 } });

        Assert.That(c.Increments["r1"], Is.EqualTo(5)); // local larger kept
        Assert.That(c.Increments["r2"], Is.EqualTo(8)); // delta larger applied
        Assert.That(c.Increments["r3"], Is.EqualTo(4)); // new replica added
    }

    [Test]
    public void MergeDelta_is_idempotent()
    {
        var c = new GCounter();
        c.Increment("r1", 1);

        var delta = new GCounterDelta { Increments = new Dictionary<string, long> { ["r1"] = 5 } };
        c.MergeDelta(delta);
        c.MergeDelta(delta);

        Assert.That(c.Increments["r1"], Is.EqualTo(5));
    }

    [Test]
    public void MergeDelta_empty_is_no_op()
    {
        var c = new GCounter();
        c.Increment("r1", 3);

        c.MergeDelta(GCounterDelta.Empty);

        Assert.That(c.Increments["r1"], Is.EqualTo(3));
        Assert.That(c.Increments, Has.Count.EqualTo(1));
    }

    [Test]
    public void Clone_copies_component_value_for_value()
    {
        var original = new GCounter();
        original.Increment("r1", 5);
        original.Increment("r2", 7);

        var clone = original.Clone();

        Assert.That(clone.Value, Is.EqualTo(original.Value));
        Assert.That(clone.Increments, Is.EquivalentTo(original.Increments));
    }

    [Test]
    public void Clone_of_empty_counter_is_bottom()
    {
        var clone = new GCounter().Clone();

        Assert.That(clone.IsBottom, Is.True);
        Assert.That(clone.Value, Is.EqualTo(0));
    }

    [Test]
    public void Clone_is_a_deep_copy_mutating_clone_does_not_affect_original()
    {
        var original = new GCounter();
        original.Increment("r1", 5);

        var clone = original.Clone();
        clone.Increment("r1", 100);
        clone.Increment("r2", 50);

        Assert.That(original.Increments["r1"], Is.EqualTo(5));
        Assert.That(original.Increments.ContainsKey("r2"), Is.False);
        Assert.That(clone.Increments["r1"], Is.EqualTo(105));
        Assert.That(clone.Increments["r2"], Is.EqualTo(50));
    }

    [Test]
    public void Clone_does_not_share_dictionary_reference_with_original()
    {
        var original = new GCounter();
        original.Increment("r1", 1);

        var clone = original.Clone();

        Assert.That(clone.Increments, Is.Not.SameAs(original.Increments));
    }
}
