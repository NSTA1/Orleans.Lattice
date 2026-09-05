using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.Data;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Value-equality regression tests for <see cref="DataEntry"/>, the key / value entry
/// used on both the write-batch and bounded range-read paths of
/// <c>ILatticeDataApi</c>. Its <see cref="DataEntry.Value"/> byte array was compared
/// by reference under the compiler-generated record equality, so two structurally
/// identical entries - including an entry and its post-serialization self - never
/// compared equal.
/// </summary>
[TestFixture]
public sealed class DataEntryEqualityTests
{
    private static DataEntry Sample(byte[]? value = null) => new()
    {
        Key = "k",
        Value = value ?? [1, 2, 3],
        MergeMode = LatticeMergeMode.OrSet,
        Raw = true,
    };

    [Test]
    public void Equal_across_distinct_arrays()
    {
        var a = Sample([1, 2, 3]);
        var b = Sample([1, 2, 3]);

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
            Assert.That(a.Equals(a with { MergeMode = null }), Is.False);
            Assert.That(a.Equals(a with { Raw = false }), Is.False);
        });
    }

    [Test]
    public void Equal_when_values_empty_on_both_sides()
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
        var entry = Sample();

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<DataEntry>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(entry));

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded.Value, entry.Value), Is.False);
            Assert.That(decoded.Equals(entry), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(entry.GetHashCode()));
        });
    }
}
