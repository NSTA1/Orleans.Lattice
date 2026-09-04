using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Crdt;

/// <summary>
/// Value-equality regression tests for <see cref="CrdtMemberChange"/>. Its
/// <see cref="CrdtMemberChange.Element"/> byte array was compared by reference
/// under the compiler-generated record-struct equality, so two changes built
/// from independently allocated but byte-identical elements - including a change
/// and its post-serialization self - never compared equal, contradicting the
/// type's documented by-content element identity.
/// </summary>
[TestFixture]
public sealed class CrdtMemberChangeEqualityTests
{
    private static CrdtMemberChange Sample(byte[]? element = null) => new()
    {
        Element = element ?? [1, 2, 3],
        Kind = CrdtMemberChangeKind.Added,
        ReplicaId = "r1",
        Ordinal = 42,
        WallClock = new HybridLogicalClock { WallClockTicks = 1234, Counter = 3 },
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
            Assert.That(a.Equals(a with { Kind = CrdtMemberChangeKind.Removed }), Is.False);
            Assert.That(a.Equals(a with { ReplicaId = "r2" }), Is.False);
            Assert.That(a.Equals(a with { Ordinal = 43 }), Is.False);
            Assert.That(a.Equals(a with { WallClock = null }), Is.False);
        });
    }

    [Test]
    public void Equal_when_element_is_null_on_both_sides()
    {
        var a = new CrdtMemberChange { Element = null!, ReplicaId = "r", Ordinal = 1 };
        var b = new CrdtMemberChange { Element = null!, ReplicaId = "r", Ordinal = 1 };

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
        var change = Sample();

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<CrdtMemberChange>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(change));

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded.Element, change.Element), Is.False);
            Assert.That(decoded.Equals(change), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(change.GetHashCode()));
        });
    }
}
