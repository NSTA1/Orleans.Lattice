using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Crdt;

/// <summary>
/// Value-equality regression tests for <see cref="OrFlagDelta"/>. Its
/// <see cref="OrFlagDelta.Enables"/> and <see cref="OrFlagDelta.Disables"/>
/// lists were compared by reference under the compiler-generated record-struct
/// equality - even though the contained <see cref="OrSetDot"/> already compares
/// by value - so two deltas built from independently allocated but structurally
/// identical collections, including a delta and its post-serialization self,
/// never compared equal.
/// </summary>
[TestFixture]
public sealed class OrFlagDeltaEqualityTests
{
    private static OrSetDot Dot(string replica, long counter) =>
        new() { ReplicaId = replica, Counter = counter };

    private static OrFlagDelta Sample() => new()
    {
        Enables = [Dot("r1", 1), Dot("r1", 2)],
        Disables = [Dot("r2", 7)],
    };

    [Test]
    public void Equal_when_collections_match_across_distinct_instances()
    {
        var a = Sample();
        var b = Sample();

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Enables, b.Enables), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_an_enable_dot_counter_differs()
    {
        var a = Sample();
        var b = a with { Enables = [Dot("r1", 1), Dot("r1", 99)] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_an_enable_dot_replica_differs()
    {
        var a = Sample();
        var b = a with { Enables = [Dot("r1", 1), Dot("rX", 2)] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_a_disable_dot_differs()
    {
        var a = Sample();
        var b = a with { Disables = [Dot("r2", 8)] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_enable_counts_differ()
    {
        var a = Sample();
        var b = a with { Enables = [Dot("r1", 1)] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Equal_when_both_are_empty()
    {
        var a = OrFlagDelta.Empty;
        var b = new OrFlagDelta { Enables = [], Disables = [] };

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Equal_when_both_are_the_default_instance()
    {
        var a = default(OrFlagDelta);
        var b = default(OrFlagDelta);

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
        var serializer = services.GetRequiredService<Serializer<OrFlagDelta>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(value));

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded.Enables, value.Enables), Is.False);
            Assert.That(decoded.Equals(value), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(value.GetHashCode()));
        });
    }
}
