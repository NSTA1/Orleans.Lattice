using Orleans.Lattice.GrainIndex.Query;

namespace Orleans.Lattice.GrainIndex.Tests.Query;

/// <summary>
/// The contract every routed comparison honours: the derived key ranges are a
/// superset of the comparison's true result set, and the exactness flag says
/// whether the residual predicate can be dropped. A range that under-includes
/// would lose rows silently, so the bounds are pinned against the encoder that
/// wrote them.
/// </summary>
[TestFixture]
public sealed class GrainIndexRangeBuilderTests
{
    private static readonly GrainIndexQueryProperty Age = new(0, "Age", typeof(int));
    private static readonly GrainIndexQueryProperty LastSeen = new(1, "LastSeen", typeof(DateTimeOffset?));
    private static readonly GrainIndexQueryProperty Score = new(2, "Score", typeof(double));
    private static readonly GrainIndexQueryProperty Status = new(3, "Status", typeof(TestStatus));
    private static readonly GrainIndexQueryProperty Country = new(4, "Country", typeof(string));

    [Test]
    public void Equality_resolves_to_the_exact_value_slot()
    {
        bool built = GrainIndexRangeBuilder.TryBuild(
            Age, LatticeComparisonOperator.Equal, 18, out var ranges, out bool exact);

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.True);
            Assert.That(exact, Is.True);
            Assert.That(ranges, Is.EqualTo(new[]
            {
                new GrainIndexKeyRange(
                    GrainIndexKeyEncoder.ValueRangeStartInclusive("Age", GrainIndexKeyEncoder.EncodeValue(18)),
                    GrainIndexKeyEncoder.ValueRangeEndExclusive("Age", GrainIndexKeyEncoder.EncodeValue(18))),
            }));
        });
    }

    [Test]
    public void Greater_than_or_equal_runs_from_the_value_to_the_end_of_the_property()
    {
        bool built = GrainIndexRangeBuilder.TryBuild(
            Age, LatticeComparisonOperator.GreaterThanOrEqual, 18, out var ranges, out bool exact);

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.True);
            Assert.That(exact, Is.True);
            Assert.That(ranges[0].StartInclusive, Is.EqualTo(
                GrainIndexKeyEncoder.ValueRangeStartInclusive("Age", GrainIndexKeyEncoder.EncodeValue(18))));
            Assert.That(ranges[0].EndExclusive, Is.EqualTo(Age.RangeEndExclusive));
        });
    }

    [Test]
    public void Less_than_starts_above_the_null_slot()
    {
        bool built = GrainIndexRangeBuilder.TryBuild(
            LastSeen, LatticeComparisonOperator.LessThan, DateTimeOffset.UnixEpoch, out var ranges, out bool exact);

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.True);
            Assert.That(exact, Is.True);
            Assert.That(ranges[0].StartInclusive, Is.EqualTo(LastSeen.PresentStartInclusive));
        });
    }

    [Test]
    public void Inequality_resolves_to_the_two_ranges_around_the_value()
    {
        bool built = GrainIndexRangeBuilder.TryBuild(
            Age, LatticeComparisonOperator.NotEqual, 18, out var ranges, out bool exact);

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.True);
            Assert.That(exact, Is.True);
            Assert.That(ranges, Has.Length.EqualTo(2));
            Assert.That(ranges[0].StartInclusive, Is.EqualTo(Age.RangeStartInclusive));
            Assert.That(ranges[1].EndExclusive, Is.EqualTo(Age.RangeEndExclusive));
        });
    }

    [Test]
    public void Equality_with_null_resolves_to_the_null_slot()
    {
        bool built = GrainIndexRangeBuilder.TryBuild(
            LastSeen, LatticeComparisonOperator.Equal, null, out var ranges, out bool exact);

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.True);
            Assert.That(exact, Is.True);
            Assert.That(ranges[0].StartInclusive, Is.EqualTo(
                GrainIndexKeyEncoder.ValueRangeStartInclusive("LastSeen", GrainIndexKeyEncoder.NullFlag.ToString())));
        });
    }

    [Test]
    public void Inequality_with_null_resolves_to_every_present_value()
    {
        bool built = GrainIndexRangeBuilder.TryBuild(
            LastSeen, LatticeComparisonOperator.NotEqual, null, out var ranges, out bool exact);

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.True);
            Assert.That(exact, Is.True);
            Assert.That(ranges, Is.EqualTo(new[]
            {
                new GrainIndexKeyRange(LastSeen.PresentStartInclusive, LastSeen.RangeEndExclusive),
            }));
        });
    }

    [Test]
    public void A_relational_comparison_with_null_matches_nothing()
    {
        bool built = GrainIndexRangeBuilder.TryBuild(
            LastSeen, LatticeComparisonOperator.GreaterThan, null, out var ranges, out bool exact);

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.True);
            Assert.That(exact, Is.True);
            Assert.That(ranges, Is.Empty);
        });
    }

    [Test]
    public void A_floating_point_less_than_keeps_its_predicate_because_of_not_a_number()
    {
        bool built = GrainIndexRangeBuilder.TryBuild(
            Score, LatticeComparisonOperator.LessThan, 5.0, out _, out bool exact);

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.True);
            Assert.That(exact, Is.False);
        });
    }

    [Test]
    public void A_floating_point_greater_than_is_exact_because_not_a_number_sorts_below()
    {
        bool built = GrainIndexRangeBuilder.TryBuild(
            Score, LatticeComparisonOperator.GreaterThan, 5.0, out _, out bool exact);

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.True);
            Assert.That(exact, Is.True);
        });
    }

    [Test]
    public void Equality_with_zero_spans_both_signed_zero_slots()
    {
        bool built = GrainIndexRangeBuilder.TryBuild(
            Score, LatticeComparisonOperator.Equal, 0.0, out var ranges, out bool exact);

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.True);
            Assert.That(exact, Is.True);
            Assert.That(ranges[0].StartInclusive, Is.EqualTo(
                GrainIndexKeyEncoder.ValueRangeStartInclusive("Score", GrainIndexKeyEncoder.EncodeValue(-0.0))));
            Assert.That(ranges[0].EndExclusive, Is.EqualTo(
                GrainIndexKeyEncoder.ValueRangeEndExclusive("Score", GrainIndexKeyEncoder.EncodeValue(0.0))));
        });
    }

    [Test]
    public void Equality_with_not_a_number_matches_nothing()
    {
        bool built = GrainIndexRangeBuilder.TryBuild(
            Score, LatticeComparisonOperator.Equal, double.NaN, out var ranges, out bool exact);

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.True);
            Assert.That(exact, Is.True);
            Assert.That(ranges, Is.Empty);
        });
    }

    [Test]
    public void Inequality_with_not_a_number_matches_the_whole_property()
    {
        bool built = GrainIndexRangeBuilder.TryBuild(
            Score, LatticeComparisonOperator.NotEqual, double.NaN, out var ranges, out bool exact);

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.True);
            Assert.That(exact, Is.True);
            Assert.That(ranges, Is.EqualTo(Score.FullRange));
        });
    }

    [Test]
    public void A_relational_comparison_with_not_a_number_matches_nothing()
    {
        bool built = GrainIndexRangeBuilder.TryBuild(
            Score, LatticeComparisonOperator.LessThan, double.NaN, out var ranges, out bool exact);

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.True);
            Assert.That(exact, Is.True);
            Assert.That(ranges, Is.Empty);
        });
    }

    [Test]
    public void A_property_with_no_ordered_key_derives_no_range()
    {
        bool built = GrainIndexRangeBuilder.TryBuild(
            Status, LatticeComparisonOperator.Equal, TestStatus.Active, out _, out _);

        Assert.That(built, Is.False);
    }

    [Test]
    public void An_unconvertible_bound_derives_no_range()
    {
        bool built = GrainIndexRangeBuilder.TryBuild(
            Age, LatticeComparisonOperator.Equal, Guid.NewGuid(), out _, out _);

        Assert.That(built, Is.False);
    }

    [Test]
    public void A_prefix_narrows_to_a_contiguous_range()
    {
        bool built = GrainIndexRangeBuilder.TryBuildPrefix(Country, "G", out var ranges);

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.True);
            Assert.That(ranges, Has.Length.EqualTo(1));
            Assert.That(ranges[0].StartInclusive, Is.EqualTo(
                Country.RangeStartInclusive + GrainIndexKeyEncoder.EncodeValue("G")));
            Assert.That(
                string.CompareOrdinal(
                    Country.RangeStartInclusive + GrainIndexKeyEncoder.EncodeValue("GB"),
                    ranges[0].EndExclusive),
                Is.LessThan(0));
        });
    }

    [Test]
    public void A_prefix_over_a_non_string_property_derives_no_range()
    {
        Assert.That(GrainIndexRangeBuilder.TryBuildPrefix(Age, "G", out _), Is.False);
    }

    [Test]
    public void An_unknown_operator_derives_no_range()
    {
        // An operator value outside the known set returns false (line 110).
        bool built = GrainIndexRangeBuilder.TryBuild(
            Age, (LatticeComparisonOperator)99, 18, out _, out _);

        Assert.That(built, Is.False);
    }

    [Test]
    public void A_prefix_whose_encoded_last_char_is_max_value_derives_no_range()
    {
        // A prefix whose encoded form ends with char.MaxValue has no successor
        // character to form the upper bound (line 135).
        bool built = GrainIndexRangeBuilder.TryBuildPrefix(Country, "\uFFFF", out _);

        Assert.That(built, Is.False);
    }

    [Test]
    public void Equality_with_null_on_a_non_nullable_property_matches_nothing()
    {
        // A non-nullable property has no null slot, so null == prop is empty (line 159).
        bool built = GrainIndexRangeBuilder.TryBuild(
            Age, LatticeComparisonOperator.Equal, null, out var ranges, out bool exact);

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.True);
            Assert.That(exact, Is.True);
            Assert.That(ranges, Is.Empty);
        });
    }

    [Test]
    public void A_float_bound_is_widened_to_double_for_a_double_property()
    {
        // A float constant is widened via the float branch of TryAsDouble (lines 244-245).
        bool built = GrainIndexRangeBuilder.TryBuild(
            Score, LatticeComparisonOperator.Equal, 1.0f, out _, out bool exact);

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.True);
            Assert.That(exact, Is.True);
        });
    }

    [Test]
    public void A_decimal_bound_converts_via_IConvertible()
    {
        // decimal implements IConvertible; ToDouble succeeds (lines 249-250).
        bool built = GrainIndexRangeBuilder.TryBuild(
            Score, LatticeComparisonOperator.Equal, 2.0m, out _, out bool exact);

        Assert.Multiple(() =>
        {
            Assert.That(built, Is.True);
            Assert.That(exact, Is.True);
        });
    }

    [Test]
    public void A_convertible_bound_that_cannot_convert_to_double_derives_no_range()
    {
        // DateTime implements IConvertible but ToDouble throws InvalidCastException
        // (lines 252-256 in the catch block of TryAsDouble).
        bool built = GrainIndexRangeBuilder.TryBuild(
            Score, LatticeComparisonOperator.Equal, DateTime.UtcNow, out _, out _);

        Assert.That(built, Is.False);
    }

    [Test]
    public void A_non_IConvertible_bound_on_a_double_property_derives_no_range()
    {
        // An object that implements neither double, float, nor IConvertible hits
        // the default branch of TryAsDouble (lines 260-261).
        bool built = GrainIndexRangeBuilder.TryBuild(
            Score, LatticeComparisonOperator.Equal, new object(), out _, out _);

        Assert.That(built, Is.False);
    }

    [Test]
    public void A_query_property_classifies_its_encoding_traits()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Age.IsOrderPreserving, Is.True);
            Assert.That(Age.IsFloatingPoint, Is.False);
            Assert.That(Age.IsTemporal, Is.False);
            Assert.That(Score.IsFloatingPoint, Is.True);
            Assert.That(LastSeen.IsTemporal, Is.True);
            Assert.That(Status.IsOrderPreserving, Is.False);
            Assert.That(Country.Ordinal, Is.EqualTo(4));
            Assert.That(Country.PropertyType, Is.EqualTo(typeof(string)));
            Assert.That(Age.PresentStartInclusive, Is.EqualTo(
                Age.RangeStartInclusive + GrainIndexKeyEncoder.PresentFlag));
        });
    }
}
