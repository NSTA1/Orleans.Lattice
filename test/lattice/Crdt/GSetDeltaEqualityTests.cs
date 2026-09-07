using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Crdt;

/// <summary>
/// Value-equality regression tests for <see cref="GSetDelta"/>. Its
/// <see cref="GSetDelta.Adds"/> collection of byte arrays was compared by
/// reference under the compiler-generated record-struct equality, so two
/// deltas built from independently allocated but byte-identical payloads -
/// including a delta and its post-serialization self - never compared equal.
/// </summary>
[TestFixture]
public sealed class GSetDeltaEqualityTests
{
    private static GSetDelta Sample(params byte[][] adds) => new() { Adds = adds };

    [Test]
    public void Equal_when_elements_match_across_distinct_arrays()
    {
        var a = Sample([7, 8], [9]);
        var b = Sample([7, 8], [9]);

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Adds, b.Adds), Is.False);
            Assert.That(ReferenceEquals(a.Adds[0], b.Adds[0]), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_an_element_byte_differs()
    {
        var a = Sample([1, 2], [3]);
        var b = Sample([1, 2], [4]);

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_element_count_differs()
    {
        var a = Sample([1, 2]);
        var b = Sample([1, 2], [3]);

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Empty_equals_a_freshly_constructed_empty_delta()
    {
        var fresh = Sample();

        Assert.Multiple(() =>
        {
            Assert.That(GSetDelta.Empty.Equals(fresh), Is.True);
            Assert.That(GSetDelta.Empty.GetHashCode(), Is.EqualTo(fresh.GetHashCode()));
        });
    }

    [Test]
    public void Default_instance_is_not_equal_to_the_empty_delta()
    {
        Assert.That(default(GSetDelta).Equals(GSetDelta.Empty), Is.False);
    }

    [Test]
    public void Serialization_round_trip_preserves_value_equality()
    {
        var value = Sample([1, 2], [3, 4, 5]);

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<GSetDelta>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(value));

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded.Adds, value.Adds), Is.False);
            Assert.That(decoded.Equals(value), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(value.GetHashCode()));
        });
    }
}
