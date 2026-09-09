using Orleans.Lattice.GrainIndex.Query;

namespace Orleans.Lattice.GrainIndex.Tests.Query;

/// <summary>
/// The relational half of the signed-zero contract. <c>-0.0</c> and <c>+0.0</c>
/// compare equal in C# but occupy adjacent, distinct slots in the key order, so
/// the builder treats the pair as one equivalence class - the "zero band" - and
/// every comparison against zero includes or excludes both slots together.
/// <para>
/// The sibling fixture pins <c>==</c>, <c>&gt;</c> and <c>&gt;=</c>; this one
/// pins the three arms that complete the operator set (<c>!=</c>, <c>&lt;</c>,
/// <c>&lt;=</c>) plus the unroutable-operator fallback. Each asserts the band
/// boundary explicitly rather than only the range count, because a range that
/// keyed on the query literal's own slot would still produce the right number of
/// ranges while silently dropping the other stored zero.
/// </para>
/// </summary>
public sealed partial class GrainIndexRangeBuilderTests
{
    /// <summary>The lower bound of the zero band: the -0.0 slot's start.</summary>
    private static string ZeroBandStartInclusive =>
        GrainIndexKeyEncoder.ValueRangeStartInclusive("Score", GrainIndexKeyEncoder.EncodeValue(-0.0));

    /// <summary>The upper bound of the zero band: the +0.0 slot's end.</summary>
    private static string ZeroBandEndExclusive =>
        GrainIndexKeyEncoder.ValueRangeEndExclusive("Score", GrainIndexKeyEncoder.EncodeValue(0.0));

    [Test]
    public void The_two_signed_zero_slots_really_are_distinct_and_adjacent()
    {
        // The premise the whole band exists for. If the encoder ever collapsed
        // the two zeros to one slot, every band assertion below would still pass
        // while testing nothing, so pin the premise separately.
        string negative = GrainIndexKeyEncoder.EncodeValue(-0.0);
        string positive = GrainIndexKeyEncoder.EncodeValue(0.0);

        Assert.Multiple(() =>
        {
            Assert.That(negative, Is.Not.EqualTo(positive));
            Assert.That(string.CompareOrdinal(negative, positive), Is.LessThan(0));
        });
    }

    [Test]
    public void Inequality_with_zero_excludes_both_signed_zero_slots()
    {
        // != 0.0 is true for every value except the two zeros, so the complement
        // is taken over the whole band rather than over the literal's own slot -
        // otherwise a stored -0.0 would be returned for `Score != 0.0`.
        bool built = GrainIndexRangeBuilder.TryBuild(
            Score, LatticeComparisonOperator.NotEqual, 0.0, out var ranges, out bool exact);

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.True);
            Assert.That(exact, Is.True);
            Assert.That(ranges, Has.Length.EqualTo(2));

            // The gap between the two ranges is exactly the zero band, and the
            // pair spans the whole property - including the null slot, because
            // in C# a null operand makes != true.
            Assert.That(ranges[0].StartInclusive, Is.EqualTo(Score.RangeStartInclusive));
            Assert.That(ranges[0].EndExclusive, Is.EqualTo(ZeroBandStartInclusive));
            Assert.That(ranges[1].StartInclusive, Is.EqualTo(ZeroBandEndExclusive));
            Assert.That(ranges[1].EndExclusive, Is.EqualTo(Score.RangeEndExclusive));
        });
    }

    [Test]
    public void Inequality_with_negative_zero_derives_the_same_ranges_as_positive_zero()
    {
        // Whichever zero literal the query happened to use, the band - and so
        // the derived range set - is identical.
        GrainIndexRangeBuilder.TryBuild(
            Score, LatticeComparisonOperator.NotEqual, 0.0, out var fromPositive, out _);
        GrainIndexRangeBuilder.TryBuild(
            Score, LatticeComparisonOperator.NotEqual, -0.0, out var fromNegative, out _);

        Assert.That(fromNegative, Is.EqualTo(fromPositive));
    }

    [Test]
    public void Less_than_zero_stops_below_both_signed_zero_slots()
    {
        // Neither zero is strictly less than zero, so the range must end at the
        // lower (negative) zero slot's start. It begins at the present flag, so
        // the null slot is excluded; the NaN slot it does over-include is what
        // makes the result inexact.
        bool built = GrainIndexRangeBuilder.TryBuild(
            Score, LatticeComparisonOperator.LessThan, 0.0, out var ranges, out bool exact);

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.True);
            Assert.That(exact, Is.False, "the retained predicate has to drop the over-included NaN slot");
            Assert.That(ranges, Has.Length.EqualTo(1));
            Assert.That(ranges[0].StartInclusive, Is.EqualTo(Score.PresentStartInclusive));
            Assert.That(ranges[0].EndExclusive, Is.EqualTo(ZeroBandStartInclusive));
        });
    }

    [Test]
    public void Less_than_or_equal_zero_includes_both_signed_zero_slots()
    {
        // A stored -0.0 satisfies `<= 0.0`, and so does a stored +0.0, so the
        // range has to run past the upper zero slot rather than stop at the
        // literal's own slot.
        bool built = GrainIndexRangeBuilder.TryBuild(
            Score, LatticeComparisonOperator.LessThanOrEqual, 0.0, out var ranges, out bool exact);

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.True);
            Assert.That(exact, Is.False, "the retained predicate has to drop the over-included NaN slot");
            Assert.That(ranges, Has.Length.EqualTo(1));
            Assert.That(ranges[0].StartInclusive, Is.EqualTo(Score.PresentStartInclusive));
            Assert.That(ranges[0].EndExclusive, Is.EqualTo(ZeroBandEndExclusive));
        });
    }

    [Test]
    public void Less_than_or_equal_zero_reaches_further_than_less_than_zero()
    {
        // The single line that separates the two arms: <= keeps the band, < drops
        // it. Asserted as a relation so it cannot be satisfied by both arms
        // returning the same bound.
        GrainIndexRangeBuilder.TryBuild(
            Score, LatticeComparisonOperator.LessThan, 0.0, out var strict, out _);
        GrainIndexRangeBuilder.TryBuild(
            Score, LatticeComparisonOperator.LessThanOrEqual, 0.0, out var orEqual, out _);

        Assert.That(
            string.CompareOrdinal(strict[0].EndExclusive, orEqual[0].EndExclusive),
            Is.LessThan(0));
    }

    [Test]
    public void An_unknown_operator_against_zero_derives_no_range()
    {
        // The signed-zero router has its own operator switch, so its unroutable
        // fallback is a different line from the general builder's.
        bool built = GrainIndexRangeBuilder.TryBuild(
            Score, (LatticeComparisonOperator)99, 0.0, out var ranges, out bool exact);

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.False);
            Assert.That(ranges, Is.Empty);
            Assert.That(exact, Is.False);
        });
    }

    [Test]
    public void The_signed_zero_band_is_derived_for_a_nullable_double_property()
    {
        // The band is built through the property's binder, so a nullable
        // floating-point property has to reach the same two slots as the
        // non-nullable one rather than falling back to a whole-property scan.
        var nullableScore = new GrainIndexQueryProperty(9, "NullableScore", typeof(double?));

        bool built = GrainIndexRangeBuilder.TryBuild(
            nullableScore, LatticeComparisonOperator.LessThanOrEqual, 0.0, out var ranges, out bool exact);

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.True);
            Assert.That(exact, Is.False);
            Assert.That(ranges[0].StartInclusive, Is.EqualTo(nullableScore.PresentStartInclusive));
            Assert.That(ranges[0].EndExclusive, Is.EqualTo(
                GrainIndexKeyEncoder.ValueRangeEndExclusive(
                    "NullableScore", GrainIndexKeyEncoder.EncodeValue<double?>(0.0))));
        });
    }
}
