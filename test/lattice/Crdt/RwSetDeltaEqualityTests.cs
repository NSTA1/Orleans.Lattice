using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Crdt;

/// <summary>
/// Value-equality regression tests for <see cref="RwSetDelta"/>. Its
/// <see cref="RwSetDelta.Adds"/>, <see cref="RwSetDelta.Removes"/>, and
/// <see cref="RwSetDelta.Tombstones"/> lists were compared by reference under
/// the compiler-generated record-struct equality - even though the contained
/// <see cref="OrSetDeltaDot"/> already compares by value - so two deltas built
/// from independently allocated but structurally identical collections,
/// including a delta and its post-serialization self, never compared equal.
/// </summary>
[TestFixture]
public sealed class RwSetDeltaEqualityTests
{
    private static OrSetDeltaDot Dot(byte element, string replica, long counter) =>
        new() { Element = [element], ReplicaId = replica, Counter = counter };

    private static RwSetDelta Sample() => new()
    {
        Adds = [Dot(1, "r1", 1), Dot(2, "r1", 2)],
        Removes = [Dot(1, "r2", 7)],
        Tombstones = [Dot(1, "r2", 5)],
    };

    [Test]
    public void Equal_when_collections_match_across_distinct_instances()
    {
        var a = Sample();
        var b = Sample();

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Adds, b.Adds), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_an_added_element_differs()
    {
        var a = Sample();
        var b = a with { Adds = [Dot(1, "r1", 1), Dot(9, "r1", 2)] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_an_added_dot_counter_differs()
    {
        var a = Sample();
        var b = a with { Adds = [Dot(1, "r1", 1), Dot(2, "r1", 99)] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_a_removed_dot_differs()
    {
        var a = Sample();
        var b = a with { Removes = [Dot(1, "r2", 8)] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_a_tombstone_dot_differs()
    {
        var a = Sample();
        var b = a with { Tombstones = [Dot(1, "r2", 6)] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_add_counts_differ()
    {
        var a = Sample();
        var b = a with { Adds = [Dot(1, "r1", 1)] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Equal_when_both_are_empty()
    {
        var a = RwSetDelta.Empty;
        var b = new RwSetDelta { Adds = [], Removes = [], Tombstones = [] };

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Equal_when_both_are_the_default_instance()
    {
        var a = default(RwSetDelta);
        var b = default(RwSetDelta);

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
        var serializer = services.GetRequiredService<Serializer<RwSetDelta>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(value));

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded.Adds, value.Adds), Is.False);
            Assert.That(decoded.Equals(value), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(value.GetHashCode()));
        });
    }
}
