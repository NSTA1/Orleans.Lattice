using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Value-equality regression tests for <see cref="AtomicActionEntry"/>, a single
/// key/value entry in a built-in tree-write saga step. Its
/// <see cref="AtomicActionEntry.Value"/> byte array was compared by reference under
/// the compiler-generated record-struct equality, so two entries built from
/// independently allocated but byte-identical payloads - including an entry and its
/// post-serialization self - never compared equal. This mirrors the sibling
/// <c>WalShardShippingEntry</c> / <c>SnapshotEntry</c> fixes.
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class AtomicActionEntryEqualityTests
{
    private static AtomicActionEntry Sample(string key = "k", byte[]? value = null, bool delete = false) =>
        new(key, value ?? [1, 2, 3], delete);

    [Test]
    public void Equal_when_key_and_payload_bytes_match_across_distinct_arrays()
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
    public void Not_equal_when_payload_bytes_differ()
    {
        var a = Sample(value: [1, 2, 3]);
        var b = Sample(value: [1, 2, 4]);

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_key_differs()
    {
        Assert.That(Sample(key: "a").Equals(Sample(key: "b")), Is.False);
    }

    [Test]
    public void Not_equal_when_delete_flag_differs()
    {
        Assert.That(Sample(delete: false).Equals(Sample(delete: true)), Is.False);
    }

    [Test]
    public void Equal_when_payload_is_empty_on_both_sides()
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
        var serializer = services.GetRequiredService<Serializer<AtomicActionEntry>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(value));

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded.Value, value.Value), Is.False);
            Assert.That(decoded.Equals(value), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(value.GetHashCode()));
        });
    }
}
