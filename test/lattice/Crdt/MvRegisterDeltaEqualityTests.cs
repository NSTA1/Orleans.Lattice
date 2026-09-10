using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.Crdt;

/// <summary>
/// Value-equality regression tests for <see cref="MvRegisterDelta"/>. Its
/// <see cref="MvRegisterDelta.Entries"/> list and <see cref="MvRegisterDelta.Context"/>
/// dictionary were compared by reference under the compiler-generated
/// record-struct equality - even though the contained
/// <see cref="MvRegisterEntry"/> already compares by value - so two deltas built
/// from independently allocated but structurally identical collections,
/// including a delta and its post-serialization self, never compared equal. The
/// context dictionary is compared order-independently.
/// </summary>
[TestFixture]
public sealed class MvRegisterDeltaEqualityTests
{
    private static MvRegisterEntry Entry(byte value, string replica, long counter) =>
        new() { Value = [value], ReplicaId = replica, Counter = counter };

    private static MvRegisterDelta Sample() => new()
    {
        Entries = [Entry(1, "r1", 3), Entry(2, "r2", 4)],
        Context = new Dictionary<string, long>(StringComparer.Ordinal) { ["r1"] = 3, ["r2"] = 4 },
    };

    [Test]
    public void Equal_when_collections_match_across_distinct_instances()
    {
        var a = Sample();
        var b = Sample();

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Entries, b.Entries), Is.False);
            Assert.That(ReferenceEquals(a.Context, b.Context), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Equal_when_context_key_order_differs()
    {
        var a = Sample();
        var b = a with
        {
            Context = new Dictionary<string, long>(StringComparer.Ordinal) { ["r2"] = 4, ["r1"] = 3 },
        };

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_an_entry_value_differs()
    {
        var a = Sample();
        var b = a with { Entries = [Entry(9, "r1", 3), Entry(2, "r2", 4)] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_a_context_value_differs()
    {
        var a = Sample();
        var b = a with
        {
            Context = new Dictionary<string, long>(StringComparer.Ordinal) { ["r1"] = 3, ["r2"] = 99 },
        };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_a_context_key_is_missing()
    {
        var a = Sample();
        var b = a with
        {
            Context = new Dictionary<string, long>(StringComparer.Ordinal) { ["r1"] = 3 },
        };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_entry_counts_differ()
    {
        var a = Sample();
        var b = a with { Entries = [Entry(1, "r1", 3)] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Equal_when_both_are_empty()
    {
        var a = MvRegisterDelta.Empty;
        var b = new MvRegisterDelta
        {
            Entries = [],
            Context = new Dictionary<string, long>(StringComparer.Ordinal),
        };

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Equal_when_both_are_the_default_instance()
    {
        var a = default(MvRegisterDelta);
        var b = default(MvRegisterDelta);

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
        var serializer = services.GetRequiredService<Serializer<MvRegisterDelta>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(value));

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(decoded.Entries, value.Entries), Is.False);
            Assert.That(decoded.Equals(value), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(value.GetHashCode()));
        });
    }
}
