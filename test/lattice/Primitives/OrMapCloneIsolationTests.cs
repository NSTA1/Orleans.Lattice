namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Regression for the <see cref="ICrdt{TSelf}.Clone"/> deep-copy contract on
/// <see cref="OrMap{TKey, TValue}"/>. The static
/// <see cref="OrMap{TKey, TValue}.Merge"/> is
/// <c>left.Clone().MergeFrom(right)</c>, and
/// <see cref="OrMap{TKey, TValue}.MergeFrom"/> folds a same-dot value collision
/// in place via <c>existing.Value.MergeFrom(...)</c>. A shallow clone that
/// shared the entry objects (and their nested value CRDTs) with the source
/// would let that in-place fold mutate the merge's left operand, which the
/// ICrdt.Clone contract forbids: "mutating the returned value must never affect
/// the receiver".
/// </summary>
[TestFixture]
public class OrMapCloneIsolationTests
{
    // Both maps start empty and write under the same key with the same OrMap
    // replica id, so both mint the identical dot (r1, 1) but stamp a different
    // nested counter value under it - the "same dot, divergent value" case that
    // makes MergeFrom fold the nested value in place.
    private static OrMap<string, PnCounter> MapWith(string key, string replicaId, long increment)
    {
        var map = new OrMap<string, PnCounter>();
        var counter = new PnCounter();
        counter.Increment("author", increment);
        map.Set(key, replicaId, counter);
        return map;
    }

    [Test]
    public void Merge_does_not_mutate_left_operand_on_a_same_dot_value_collision()
    {
        var left = MapWith("k", "r1", 5);
        var right = MapWith("k", "r1", 9);
        var before = left.Get("k")!.Value;

        var merged = OrMap<string, PnCounter>.Merge(left, right);

        Assert.Multiple(() =>
        {
            Assert.That(before, Is.EqualTo(5), "precondition: the left operand starts at 5");
            Assert.That(left.Get("k")!.Value, Is.EqualTo(before),
                "Merge must be pure: the left operand's nested value must be unchanged");
            Assert.That(merged.Get("k")!.Value, Is.EqualTo(9),
                "the merged map still folds the same-dot collision to the max value");
        });
    }

    [Test]
    public void Clone_is_independent_so_merging_into_the_clone_leaves_the_source_unchanged()
    {
        var source = MapWith("k", "r1", 5);
        var other = MapWith("k", "r1", 9);

        var clone = source.Clone();
        clone.MergeFrom(other);

        Assert.Multiple(() =>
        {
            Assert.That(source.Get("k")!.Value, Is.EqualTo(5),
                "mutating the clone (via MergeFrom) must never affect the source");
            Assert.That(clone.Get("k")!.Value, Is.EqualTo(9),
                "the clone itself folds the collision in place, as expected");
        });
    }
}
