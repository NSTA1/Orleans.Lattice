using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Value-equality regression tests for <see cref="AggregationContribution"/>. Its
/// <see cref="AggregationContribution.Value"/> byte array was compared by reference
/// under the compiler-generated record-struct equality, so two structurally
/// identical fold contributions - including a contribution and its
/// post-serialization self - never compared equal.
/// </summary>
[TestFixture]
public sealed class AggregationContributionEqualityTests
{
    private static AggregationContribution Sample() => new()
    {
        Kind = AggregationContributionKind.Contribute,
        GroupKey = "g",
        SourceKey = "s",
        Numeric = 3.5,
        Member = "m",
        Timestamp = new HybridLogicalClock { WallClockTicks = 77, Counter = 4 },
        EndKey = null,
        Value = [1, 2, 3],
    };

    [Test]
    public void Equal_when_all_fields_and_value_bytes_match_across_distinct_arrays()
    {
        var a = Sample();
        var b = Sample();

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Value, b.Value), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_value_bytes_differ()
    {
        var a = Sample();
        var b = a with { Value = [9, 9] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_a_scalar_field_differs()
    {
        var a = Sample();

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(a with { Kind = AggregationContributionKind.Retract }), Is.False);
            Assert.That(a.Equals(a with { GroupKey = "other" }), Is.False);
            Assert.That(a.Equals(a with { SourceKey = "other" }), Is.False);
            Assert.That(a.Equals(a with { Numeric = 4.5 }), Is.False);
            Assert.That(a.Equals(a with { Member = "other" }), Is.False);
            Assert.That(a.Equals(a with { Timestamp = new HybridLogicalClock { WallClockTicks = 78, Counter = 4 } }), Is.False);
            Assert.That(a.Equals(a with { EndKey = "z" }), Is.False);
        });
    }

    [Test]
    public void Equal_when_value_is_null_on_both_sides()
    {
        var a = Sample() with { Value = null };
        var b = Sample() with { Value = null };

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_only_one_value_is_null()
    {
        var a = Sample();
        var b = a with { Value = null };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Serialization_round_trip_preserves_value_equality()
    {
        var contribution = Sample();

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<AggregationContribution>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(contribution));

        Assert.Multiple(() =>
        {
            Assert.That(decoded.Equals(contribution), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(contribution.GetHashCode()));
        });
    }
}
