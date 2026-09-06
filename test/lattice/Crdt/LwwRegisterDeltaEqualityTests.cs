using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Crdt;

/// <summary>
/// Value-equality regression tests for <see cref="LwwRegisterDelta"/>. Its
/// <see cref="LwwRegisterDelta.Value"/> byte array was compared by reference
/// under the compiler-generated record-struct equality, so two deltas built
/// from independently allocated but byte-identical payloads - including a delta
/// and its post-serialization self - never compared equal.
/// </summary>
[TestFixture]
public sealed class LwwRegisterDeltaEqualityTests
{
    private static readonly HybridLogicalClock SampleClock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);

    private static LwwRegisterDelta Sample(byte[]? value = null) => new()
    {
        Value = value ?? [1, 2, 3],
        Timestamp = SampleClock,
        IsTombstone = false,
        ExpiresAtTicks = 1234L,
        OriginClusterId = "site-a",
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
            Assert.That(a.Equals(a with { IsTombstone = true }), Is.False);
            Assert.That(a.Equals(a with { ExpiresAtTicks = 5678L }), Is.False);
            Assert.That(a.Equals(a with { OriginClusterId = "site-b" }), Is.False);
            Assert.That(a.Equals(a with { Timestamp = HybridLogicalClock.Tick(SampleClock) }), Is.False);
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
    public void Not_equal_when_only_one_value_is_null()
    {
        var a = Sample([1, 2, 3]);
        var b = a with { Value = null };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Serialization_round_trip_preserves_value_equality()
    {
        var value = Sample();

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<LwwRegisterDelta>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(value));

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded.Value, value.Value), Is.False);
            Assert.That(decoded.Equals(value), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(value.GetHashCode()));
        });
    }
}
