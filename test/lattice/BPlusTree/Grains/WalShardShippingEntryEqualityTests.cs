using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Value-equality regression tests for <see cref="WalShardShippingEntry"/>. Its
/// <see cref="WalShardShippingEntry.EncodedPayload"/> byte array was compared by
/// reference under the compiler-generated record-struct equality, so two entries
/// built from independently allocated but byte-identical payloads - including an
/// entry and its post-serialization self - never compared equal.
/// </summary>
[TestFixture]
public sealed class WalShardShippingEntryEqualityTests
{
    private static WalShardShippingEntry Sample(long sequence = 7L, byte[]? payload = null) => new()
    {
        Sequence = sequence,
        EncodedPayload = payload ?? [1, 2, 3],
    };

    [Test]
    public void Equal_when_sequence_and_payload_bytes_match_across_distinct_arrays()
    {
        var a = Sample(payload: [7, 8, 9]);
        var b = Sample(payload: [7, 8, 9]);

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.EncodedPayload, b.EncodedPayload), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_payload_bytes_differ()
    {
        var a = Sample(payload: [1, 2, 3]);
        var b = Sample(payload: [1, 2, 4]);

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_sequence_differs()
    {
        var a = Sample(1L);
        var b = Sample(2L);

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Equal_when_payload_is_empty_on_both_sides()
    {
        var a = Sample(payload: []);
        var b = Sample(payload: []);

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
        var serializer = services.GetRequiredService<Serializer<WalShardShippingEntry>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(value));

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded.EncodedPayload, value.EncodedPayload), Is.False);
            Assert.That(decoded.Equals(value), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(value.GetHashCode()));
        });
    }
}
