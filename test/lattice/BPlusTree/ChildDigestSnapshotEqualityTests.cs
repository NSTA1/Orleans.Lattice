using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Value-equality regression tests for <see cref="ChildDigestSnapshot"/>. Its
/// <see cref="ChildDigestSnapshot.Hash"/> byte array was compared by reference
/// under the compiler-generated record-struct equality, so two structurally
/// identical snapshots - including a snapshot and its post-serialization self -
/// never compared equal, defeating the content-digest comparison this type
/// exists for.
/// </summary>
[TestFixture]
public sealed class ChildDigestSnapshotEqualityTests
{
    private static ChildDigestSnapshot Sample() => new()
    {
        Hash = [1, 2, 3, 4],
        EntryCount = 5,
        CheckpointOffset = 9,
        PublishSequence = 2,
        LowKeyInclusive = "a",
        HighKeyExclusive = "z",
        LiveCount = 4,
        TombstoneCount = 1,
        SubtreeDepth = 2,
        ChildFanout = 3,
    };

    [Test]
    public void Equal_when_all_fields_and_hash_bytes_match_across_distinct_arrays()
    {
        var a = Sample();
        var b = Sample();

        Assert.Multiple(() =>
        {
            Assert.That(ReferenceEquals(a.Hash, b.Hash), Is.False);
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a == b, Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_hash_bytes_differ()
    {
        var a = Sample();
        var b = a with { Hash = [1, 2, 3, 9] };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_a_scalar_field_differs()
    {
        var a = Sample();

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(a with { EntryCount = 6 }), Is.False);
            Assert.That(a.Equals(a with { CheckpointOffset = 10 }), Is.False);
            Assert.That(a.Equals(a with { PublishSequence = 3 }), Is.False);
            Assert.That(a.Equals(a with { LowKeyInclusive = "b" }), Is.False);
            Assert.That(a.Equals(a with { HighKeyExclusive = "y" }), Is.False);
            Assert.That(a.Equals(a with { LiveCount = 5 }), Is.False);
            Assert.That(a.Equals(a with { TombstoneCount = 2 }), Is.False);
            Assert.That(a.Equals(a with { SubtreeDepth = 3 }), Is.False);
            Assert.That(a.Equals(a with { ChildFanout = 4 }), Is.False);
        });
    }

    [Test]
    public void Equal_when_hash_is_null_on_both_sides()
    {
        var a = Sample() with { Hash = null };
        var b = Sample() with { Hash = null };

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(b), Is.True);
            Assert.That(a.GetHashCode(), Is.EqualTo(b.GetHashCode()));
        });
    }

    [Test]
    public void Not_equal_when_only_one_hash_is_null()
    {
        var a = Sample();
        var b = a with { Hash = null };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Default_snapshots_compare_equal()
    {
        Assert.That(default(ChildDigestSnapshot).Equals(default), Is.True);
    }

    [Test]
    public void Serialization_round_trip_preserves_value_equality()
    {
        var snapshot = Sample();

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<ChildDigestSnapshot>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(snapshot));

        Assert.Multiple(() =>
        {
            Assert.That(decoded.Equals(snapshot), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(snapshot.GetHashCode()));
        });
    }
}
