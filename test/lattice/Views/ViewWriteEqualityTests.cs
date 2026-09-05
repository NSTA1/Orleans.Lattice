using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Value-equality regression tests for <see cref="ViewWrite"/>. Its
/// <see cref="ViewWrite.Value"/> byte array was compared by reference under the
/// compiler-generated record-struct equality, so two structurally identical
/// writes - including a write and its post-serialization self - never compared
/// equal.
/// </summary>
[TestFixture]
public sealed class ViewWriteEqualityTests
{
    private static ViewWrite Sample() => new()
    {
        Kind = ViewWriteKind.Upsert,
        Key = "view:k",
        Value = [1, 2, 3],
        ExpiresAtTicks = 42,
        Timestamp = new HybridLogicalClock { WallClockTicks = 77, Counter = 4 },
        EndKey = null,
        SourceKey = "k",
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
            Assert.That(a.Equals(a with { Key = "other" }), Is.False);
            Assert.That(a.Equals(a with { ExpiresAtTicks = 43 }), Is.False);
            Assert.That(a.Equals(a with { Kind = ViewWriteKind.Delete }), Is.False);
            Assert.That(a.Equals(a with { SourceKey = "other" }), Is.False);
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
        var write = Sample();

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<ViewWrite>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(write));

        Assert.Multiple(() =>
        {
            Assert.That(decoded.Equals(write), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(write.GetHashCode()));
        });
    }
}
