using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Crdt;

/// <summary>
/// Value-equality regression tests for <see cref="RgaDelta"/>. Its
/// <see cref="RgaDelta.Inserts"/> and <see cref="RgaDelta.Tombstones"/> lists
/// were compared by reference under the compiler-generated record-struct
/// equality - even though the contained <see cref="RgaDeltaNode"/> and
/// <see cref="OrSetDot"/> already compare by value - so two deltas built from
/// independently allocated but structurally identical collections, including a
/// delta and its post-serialization self, never compared equal.
/// </summary>
[TestFixture]
public sealed class RgaDeltaEqualityTests
{
    private static RgaDeltaNode Node(byte value, string replica, long counter, OrSetDot parent) =>
        new() { ReplicaId = replica, Counter = counter, ParentDot = parent, Value = [value] };

    private static RgaDelta Sample() => new()
    {
        Inserts =
        [
            Node(1, "r1", 1, Rga.Root),
            Node(2, "r1", 2, new OrSetDot { ReplicaId = "r1", Counter = 1 }),
        ],
        Tombstones = [new OrSetDot { ReplicaId = "r2", Counter = 9 }],
    };

    [Test]
    public void Equal_when_collections_match_across_distinct_instances()
    {
        var a = Sample();
        var b = Sample();

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Inserts, b.Inserts), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_an_insert_value_differs()
    {
        var a = Sample();
        var b = a with
        {
            Inserts =
            [
                Node(9, "r1", 1, Rga.Root),
                Node(2, "r1", 2, new OrSetDot { ReplicaId = "r1", Counter = 1 }),
            ],
        };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_an_insert_parent_dot_differs()
    {
        var a = Sample();
        var b = a with
        {
            Inserts =
            [
                Node(1, "r1", 1, Rga.Root),
                Node(2, "r1", 2, new OrSetDot { ReplicaId = "r9", Counter = 1 }),
            ],
        };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_a_tombstone_differs()
    {
        var a = Sample();
        var b = a with { Tombstones = [new OrSetDot { ReplicaId = "r2", Counter = 10 }] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_insert_counts_differ()
    {
        var a = Sample();
        var b = a with { Inserts = [Node(1, "r1", 1, Rga.Root)] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Equal_when_both_are_empty()
    {
        var a = RgaDelta.Empty;
        var b = new RgaDelta { Inserts = [], Tombstones = [] };

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Equal_when_both_are_the_default_instance()
    {
        var a = default(RgaDelta);
        var b = default(RgaDelta);

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
        var serializer = services.GetRequiredService<Serializer<RgaDelta>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(value));

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded.Inserts, value.Inserts), Is.False);
            Assert.That(decoded.Equals(value), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(value.GetHashCode()));
        });
    }
}
