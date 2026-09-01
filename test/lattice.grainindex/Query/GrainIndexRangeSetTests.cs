using Orleans.Lattice.GrainIndex.Query;

namespace Orleans.Lattice.GrainIndex.Tests.Query;

/// <summary>
/// The interval algebra a clause's key ranges are built from. Negation depends
/// on the complement being exact, and a conjunction over one property depends on
/// the intersection being exact, so both are pinned directly rather than only
/// through the query surface.
/// </summary>
[TestFixture]
public sealed class GrainIndexRangeSetTests
{
    [Test]
    public void Intersect_of_two_overlapping_ranges_is_the_overlap()
    {
        var result = GrainIndexRangeSet.Intersect([Range("a", "m")], [Range("f", "z")]);

        Assert.That(result, Is.EqualTo(new[] { Range("f", "m") }));
    }

    [Test]
    public void Intersect_of_disjoint_ranges_is_empty()
    {
        var result = GrainIndexRangeSet.Intersect([Range("a", "c")], [Range("m", "z")]);

        Assert.That(result, Is.Empty);
    }

    [Test]
    public void Intersect_with_an_empty_set_is_empty()
    {
        Assert.Multiple(() =>
        {
            Assert.That(GrainIndexRangeSet.Intersect([], [Range("a", "z")]), Is.Empty);
            Assert.That(GrainIndexRangeSet.Intersect([Range("a", "z")], []), Is.Empty);
        });
    }

    [Test]
    public void Intersect_of_a_two_range_set_keeps_every_overlap()
    {
        var result = GrainIndexRangeSet.Intersect(
            [Range("a", "d"), Range("m", "z")],
            [Range("b", "p")]);

        Assert.That(result, Is.EqualTo(new[] { Range("b", "d"), Range("m", "p") }));
    }

    [Test]
    public void Intersect_of_touching_ranges_is_empty()
    {
        var result = GrainIndexRangeSet.Intersect([Range("a", "m")], [Range("m", "z")]);

        Assert.That(result, Is.Empty);
    }

    [Test]
    public void Complement_of_an_interior_range_is_the_two_gaps_around_it()
    {
        var result = GrainIndexRangeSet.Complement([Range("f", "m")], "a", "z");

        Assert.That(result, Is.EqualTo(new[] { Range("a", "f"), Range("m", "z") }));
    }

    [Test]
    public void Complement_of_the_whole_universe_is_empty()
    {
        var result = GrainIndexRangeSet.Complement([Range("a", "z")], "a", "z");

        Assert.That(result, Is.Empty);
    }

    [Test]
    public void Complement_of_nothing_is_the_whole_universe()
    {
        var result = GrainIndexRangeSet.Complement([], "a", "z");

        Assert.That(result, Is.EqualTo(new[] { Range("a", "z") }));
    }

    [Test]
    public void Complement_of_a_leading_range_is_the_remainder()
    {
        var result = GrainIndexRangeSet.Complement([Range("a", "f")], "a", "z");

        Assert.That(result, Is.EqualTo(new[] { Range("f", "z") }));
    }

    [Test]
    public void Complement_of_a_two_range_set_is_the_gap_between_them()
    {
        var result = GrainIndexRangeSet.Complement([Range("a", "f"), Range("m", "z")], "a", "z");

        Assert.That(result, Is.EqualTo(new[] { Range("f", "m") }));
    }

    [Test]
    public void Complement_is_an_involution_on_an_interior_range()
    {
        var once = GrainIndexRangeSet.Complement([Range("f", "m")], "a", "z");
        var twice = GrainIndexRangeSet.Complement(once, "a", "z");

        Assert.That(twice, Is.EqualTo(new[] { Range("f", "m") }));
    }

    [Test]
    public void Is_universe_recognises_only_the_exact_whole_range()
    {
        Assert.Multiple(() =>
        {
            Assert.That(GrainIndexRangeSet.IsUniverse([Range("a", "z")], "a", "z"), Is.True);
            Assert.That(GrainIndexRangeSet.IsUniverse([Range("b", "z")], "a", "z"), Is.False);
            Assert.That(GrainIndexRangeSet.IsUniverse([Range("a", "y")], "a", "z"), Is.False);
            Assert.That(GrainIndexRangeSet.IsUniverse([], "a", "z"), Is.False);
            Assert.That(GrainIndexRangeSet.IsUniverse([Range("a", "b"), Range("c", "z")], "a", "z"), Is.False);
        });
    }

    [Test]
    public void An_inverted_range_reports_itself_empty()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Range("m", "a").IsEmpty, Is.True);
            Assert.That(Range("a", "a").IsEmpty, Is.True);
            Assert.That(Range("a", "b").IsEmpty, Is.False);
        });
    }

    private static GrainIndexKeyRange Range(string start, string end) => new(start, end);
}
