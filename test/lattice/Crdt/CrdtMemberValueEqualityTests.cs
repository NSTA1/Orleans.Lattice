using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Crdt;

/// <summary>
/// Value-equality regression tests for <see cref="CrdtMemberValue"/>. Its
/// <see cref="CrdtMemberValue.Element"/> byte array was compared by reference
/// under the compiler-generated record-struct equality, so two members built
/// from independently allocated but byte-identical elements - including a member
/// and its post-serialization self - never compared equal.
/// </summary>
[TestFixture]
public sealed class CrdtMemberValueEqualityTests
{
    private static CrdtMemberValue Sample(byte[]? element = null) => new()
    {
        Element = element ?? [1, 2, 3],
        ReplicaId = "r1",
        Ordinal = 42,
    };

    [Test]
    public void Equal_when_all_fields_and_element_bytes_match_across_distinct_arrays()
    {
        var a = Sample([7, 8, 9]);
        var b = Sample([7, 8, 9]);

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Element, b.Element), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_element_bytes_differ()
    {
        var a = Sample([1, 2, 3]);
        var b = Sample([1, 2, 4]);

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_a_scalar_field_differs()
    {
        var a = Sample();

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(a with { ReplicaId = "r2" }), Is.False);
            Assert.That(a.Equals(a with { Ordinal = 43 }), Is.False);
        });
    }

    [Test]
    public void Equal_when_element_is_empty_on_both_sides()
    {
        var a = Sample([]);
        var b = Sample([]);

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_only_one_element_is_null()
    {
        var a = Sample([1, 2, 3]);
        var b = a with { Element = null! };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Serialization_round_trip_preserves_value_equality()
    {
        var value = Sample();

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<CrdtMemberValue>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(value));

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded.Element, value.Element), Is.False);
            Assert.That(decoded.Equals(value), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(value.GetHashCode()));
        });
    }
}
