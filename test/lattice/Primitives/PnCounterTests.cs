using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.Primitives;

[TestFixture]
public class PnCounterTests
{
    [Test]
    public void New_counter_has_zero_value()
    {
        var c = new PnCounter();
        Assert.That(c.Value, Is.EqualTo(0));
    }

    [Test]
    public void Increment_advances_positive_component()
    {
        var c = new PnCounter();
        c.Increment("r1", 3);
        Assert.That(c.Value, Is.EqualTo(3));
        Assert.That(c.Increments["r1"], Is.EqualTo(3));
    }

    [Test]
    public void Decrement_advances_negative_component()
    {
        var c = new PnCounter();
        c.Decrement("r1", 2);
        Assert.That(c.Value, Is.EqualTo(-2));
        Assert.That(c.Decrements["r1"], Is.EqualTo(2));
    }

    [Test]
    public void Mixed_increment_and_decrement_sum_correctly()
    {
        var c = new PnCounter();
        c.Increment("r1", 5);
        c.Decrement("r1", 2);
        c.Increment("r2", 4);
        Assert.That(c.Value, Is.EqualTo(7));
    }

    [Test]
    public void Increment_default_amount_is_one()
    {
        var c = new PnCounter();
        c.Increment("r1");
        c.Increment("r1");
        Assert.That(c.Value, Is.EqualTo(2));
    }

    [Test]
    public void Decrement_default_amount_is_one()
    {
        var c = new PnCounter();
        c.Decrement("r1");
        Assert.That(c.Value, Is.EqualTo(-1));
    }

    [Test]
    public void Increment_with_zero_is_no_op()
    {
        var c = new PnCounter();
        c.Increment("r1", 0);
        Assert.That(c.Increments, Is.Empty);
    }

    [Test]
    public void Decrement_with_zero_is_no_op()
    {
        var c = new PnCounter();
        c.Decrement("r1", 0);
        Assert.That(c.Decrements, Is.Empty);
    }

    [Test]
    public void Increment_throws_on_negative_amount()
    {
        var c = new PnCounter();
        Assert.That(() => c.Increment("r1", -1), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Decrement_throws_on_negative_amount()
    {
        var c = new PnCounter();
        Assert.That(() => c.Decrement("r1", -1), Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Increment_throws_on_empty_replica_id()
    {
        var c = new PnCounter();
        Assert.That(() => c.Increment("", 1), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => c.Increment(null!, 1), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Decrement_throws_on_empty_replica_id()
    {
        var c = new PnCounter();
        Assert.That(() => c.Decrement("", 1), Throws.InstanceOf<ArgumentException>());
        Assert.That(() => c.Decrement(null!, 1), Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void Merge_takes_pointwise_max_per_replica()
    {
        var a = new PnCounter();
        a.Increment("r1", 5);
        a.Decrement("r2", 1);

        var b = new PnCounter();
        b.Increment("r1", 3);
        b.Decrement("r2", 4);

        var merged = PnCounter.Merge(a, b);
        Assert.That(merged.Increments["r1"], Is.EqualTo(5));
        Assert.That(merged.Decrements["r2"], Is.EqualTo(4));
    }

    [Test]
    public void Merge_is_commutative()
    {
        var a = new PnCounter(); a.Increment("r1", 5); a.Decrement("r2", 1);
        var b = new PnCounter(); b.Increment("r1", 3); b.Decrement("r2", 4);

        var ab = PnCounter.Merge(a, b);
        var ba = PnCounter.Merge(b, a);
        Assert.That(ab.Value, Is.EqualTo(ba.Value));
    }

    [Test]
    public void Merge_is_idempotent()
    {
        var a = new PnCounter(); a.Increment("r1", 7);
        var twice = PnCounter.Merge(a, a);
        Assert.That(twice.Value, Is.EqualTo(7));
    }

    [Test]
    public void Merge_is_associative()
    {
        var a = new PnCounter(); a.Increment("r1", 3);
        var b = new PnCounter(); b.Decrement("r2", 2);
        var c = new PnCounter(); c.Increment("r3", 5);

        var left = PnCounter.Merge(PnCounter.Merge(a, b), c);
        var right = PnCounter.Merge(a, PnCounter.Merge(b, c));
        Assert.That(left.Value, Is.EqualTo(right.Value));
    }

    [Test]
    public void Merge_throws_on_null_left()
    {
        var c = new PnCounter();
        Assert.That(() => PnCounter.Merge(null!, c), Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void Merge_throws_on_null_right()
    {
        var c = new PnCounter();
        Assert.That(() => PnCounter.Merge(c, null!), Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void MergeFrom_throws_on_null_other()
    {
        var c = new PnCounter();
        Assert.That(() => c.MergeFrom(null!), Throws.InstanceOf<ArgumentNullException>());
    }

    [Test]
    public void MergeFrom_matches_static_Merge()
    {
        var a = new PnCounter();
        a.Increment("r1", 5);
        a.Decrement("r1", 2);
        a.Increment("r2", 7);

        var b = new PnCounter();
        b.Increment("r1", 3); // smaller than a's 5 - pointwise-max keeps a's
        b.Increment("r2", 9); // larger than a's 7 - pointwise-max keeps b's
        b.Decrement("r3", 4);

        var staticResult = PnCounter.Merge(a, b);
        var inPlace = a.Clone();
        inPlace.MergeFrom(b);

        Assert.That(inPlace.Value, Is.EqualTo(staticResult.Value));
        Assert.That(inPlace.Increments["r1"], Is.EqualTo(5));
        Assert.That(inPlace.Increments["r2"], Is.EqualTo(9));
        Assert.That(inPlace.Decrements["r3"], Is.EqualTo(4));
    }

    [Test]
    public void Concurrent_increments_on_different_replicas_both_count()
    {
        var r1 = new PnCounter(); r1.Increment("r1", 2);
        var r2 = new PnCounter(); r2.Increment("r2", 3);
        var merged = PnCounter.Merge(r1, r2);
        Assert.That(merged.Value, Is.EqualTo(5));
    }

    [Test]
    public void Clone_copies_both_components_value_for_value()
    {
        var original = new PnCounter();
        original.Increment("r1", 5);
        original.Increment("r2", 7);
        original.Decrement("r1", 2);
        original.Decrement("r3", 4);

        var clone = original.Clone();

        Assert.That(clone.Value, Is.EqualTo(original.Value));
        Assert.That(clone.Increments, Is.EquivalentTo(original.Increments));
        Assert.That(clone.Decrements, Is.EquivalentTo(original.Decrements));
    }

    [Test]
    public void Clone_of_empty_counter_is_bottom()
    {
        var clone = new PnCounter().Clone();

        Assert.That(clone.IsBottom, Is.True);
        Assert.That(clone.Value, Is.EqualTo(0));
    }

    [Test]
    public void Clone_is_a_deep_copy_mutating_clone_does_not_affect_original()
    {
        var original = new PnCounter();
        original.Increment("r1", 5);
        original.Decrement("r1", 2);

        var clone = original.Clone();
        clone.Increment("r1", 100);
        clone.Decrement("r2", 50);

        Assert.That(original.Increments["r1"], Is.EqualTo(5));
        Assert.That(original.Decrements.ContainsKey("r2"), Is.False);
        Assert.That(clone.Increments["r1"], Is.EqualTo(105));
        Assert.That(clone.Decrements["r2"], Is.EqualTo(50));
    }

    [Test]
    public void Clone_does_not_share_dictionary_references_with_original()
    {
        var original = new PnCounter();
        original.Increment("r1", 1);

        var clone = original.Clone();

        Assert.That(clone.Increments, Is.Not.SameAs(original.Increments));
        Assert.That(clone.Decrements, Is.Not.SameAs(original.Decrements));
    }
}
