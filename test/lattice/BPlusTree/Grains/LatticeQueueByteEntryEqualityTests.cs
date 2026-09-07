using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Value-equality regression tests for <see cref="LatticeQueueByteEntry"/>. Its
/// <see cref="LatticeQueueByteEntry.Value"/> byte array was compared by
/// reference under the compiler-generated record-struct equality, so two
/// entries built from independently allocated but byte-identical payloads -
/// including an entry and its post-serialization self - never compared equal.
/// </summary>
[TestFixture]
public sealed class LatticeQueueByteEntryEqualityTests
{
    private static LatticeQueueByteEntry Sample(long entryId = 7L, byte[]? value = null) => new()
    {
        EntryId = entryId,
        Value = value ?? [1, 2, 3],
    };

    [Test]
    public void Equal_when_id_and_value_bytes_match_across_distinct_arrays()
    {
        var a = Sample(value: [7, 8, 9]);
        var b = Sample(value: [7, 8, 9]);

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
        var a = Sample(value: [1, 2, 3]);
        var b = Sample(value: [1, 2, 4]);

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_entry_id_differs()
    {
        var a = Sample(1L);
        var b = Sample(2L);

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Equal_when_value_is_empty_on_both_sides()
    {
        var a = Sample(value: []);
        var b = Sample(value: []);

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
        var serializer = services.GetRequiredService<Serializer<LatticeQueueByteEntry>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(value));

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded.Value, value.Value), Is.False);
            Assert.That(decoded.Equals(value), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(value.GetHashCode()));
        });
    }
}
