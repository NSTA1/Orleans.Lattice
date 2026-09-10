using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Primitives;

/// <summary>
/// Value-equality regression tests for <see cref="MvRegisterEntry"/>. Its
/// <see cref="MvRegisterEntry.Value"/> byte array was compared by reference
/// under the compiler-generated record-struct equality, so two entries built
/// from independently allocated but byte-identical values - including an entry
/// and its post-serialization self - never compared equal.
/// </summary>
[TestFixture]
public sealed class MvRegisterEntryEqualityTests
{
    private static MvRegisterEntry Sample(byte[]? value = null) => new()
    {
        ReplicaId = "replica-a",
        Counter = 7L,
        Value = value ?? [1, 2, 3],
    };

    [Test]
    public void Equal_when_all_fields_and_value_bytes_match_across_distinct_arrays()
    {
        var a = Sample([7, 8, 9]);
        var b = Sample([7, 8, 9]);

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
            Assert.That(a.Equals(a with { ReplicaId = "replica-b" }), Is.False);
            Assert.That(a.Equals(a with { Counter = 8L }), Is.False);
        });
    }

    [Test]
    public void Equal_when_value_is_empty_on_both_sides()
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
    public void Serialization_round_trip_preserves_value_equality()
    {
        var value = Sample();

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<MvRegisterEntry>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(value));

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded.Value, value.Value), Is.False);
            Assert.That(decoded.Equals(value), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(value.GetHashCode()));
        });
    }
}
