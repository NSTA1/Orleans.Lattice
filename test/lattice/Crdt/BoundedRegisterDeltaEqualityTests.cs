using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Crdt;

/// <summary>
/// Value-equality regression tests for <see cref="BoundedRegisterDelta"/>. Its
/// <see cref="BoundedRegisterDelta.Value"/> and
/// <see cref="BoundedRegisterDelta.OrderKey"/> byte arrays were compared by
/// reference under the compiler-generated record-struct equality, so two deltas
/// built from independently allocated but byte-identical payloads - including a
/// delta and its post-serialization self - never compared equal.
/// </summary>
[TestFixture]
public sealed class BoundedRegisterDeltaEqualityTests
{
    private static BoundedRegisterDelta Sample(byte[]? value = null, byte[]? orderKey = null) => new()
    {
        Value = value ?? [1, 2, 3],
        OrderKey = orderKey ?? [4, 5, 6],
        HasValue = true,
    };

    [Test]
    public void Equal_when_all_fields_and_byte_payloads_match_across_distinct_arrays()
    {
        var a = Sample([7, 8, 9], [10, 11]);
        var b = Sample([7, 8, 9], [10, 11]);

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Value, b.Value), Is.False);
            Assert.That(ReferenceEquals(a.OrderKey, b.OrderKey), Is.False);
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
    public void Not_equal_when_order_key_bytes_differ()
    {
        var a = Sample(orderKey: [4, 5, 6]);
        var b = Sample(orderKey: [4, 5, 7]);

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_has_value_flag_differs()
    {
        var a = Sample();
        var b = a with { HasValue = false };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Equal_when_byte_payloads_are_empty_on_both_sides()
    {
        var a = Sample([], []);
        var b = Sample([], []);

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_only_one_value_is_null()
    {
        var a = Sample([1, 2, 3]);
        var b = a with { Value = null };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Empty_equals_default_no_op_delta()
    {
        Assert.Multiple(() =>
        {
            Assert.That(BoundedRegisterDelta.Empty.Equals(default), Is.True);
            Assert.That(
                BoundedRegisterDelta.Empty.GetHashCode(),
                Is.EqualTo(default(BoundedRegisterDelta).GetHashCode()));
        });
    }

    [Test]
    public void Serialization_round_trip_preserves_value_equality()
    {
        var value = Sample();

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<BoundedRegisterDelta>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(value));

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded.Value, value.Value), Is.False);
            Assert.That(decoded.Equals(value), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(value.GetHashCode()));
        });
    }
}
