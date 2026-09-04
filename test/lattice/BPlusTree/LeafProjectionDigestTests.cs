using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit tests for the additive <see cref="LeafProjectionDigest.Version"/>
/// field. The contribution-function compatibility contract requires every
/// producer to stamp <see cref="LeafProjectionDigest.CurrentVersion"/> and
/// the default value to remain <c>0</c> so deserialized digests from
/// pre-versioning persisted state continue to compare correctly.
/// </summary>
[TestFixture]
public class LeafProjectionDigestTests
{
    [Test]
    public void CurrentVersion_is_zero_for_the_shipping_shape()
    {
        // v0 is the original (and at present only) shipping shape.
        // Bumping this constant in source must coincide with a deliberate
        // contribution-function change across leaf, internal, and shard
        // root producers; the test exists to fail loudly if someone
        // bumps it without that coordinated change.
        Assert.That(LeafProjectionDigest.CurrentVersion, Is.EqualTo(0));
    }

    [Test]
    public void Default_value_has_version_zero()
    {
        // Backwards compatibility: persisted-but-deserialised digests
        // from before the version field was added arrive with
        // Version = 0 (the int default), which must equal CurrentVersion
        // so legacy and current producers compare cleanly under v0.
        var d = default(LeafProjectionDigest);
        Assert.That(d.Version, Is.EqualTo(0));
        Assert.That(d.Version, Is.EqualTo(LeafProjectionDigest.CurrentVersion));
    }

    [Test]
    public void Equal_when_scalars_and_hash_bytes_match_across_distinct_arrays()
    {
        // The two Hash arrays are distinct instances with equal content, so a
        // reference comparison (the pre-fix compiler-generated equality) reports
        // unequal while value equality must report equal.
        var a = new LeafProjectionDigest { Hash = [1, 2, 3, 4], EntryCount = 5, CheckpointOffset = 9, Version = 0 };
        var b = new LeafProjectionDigest { Hash = [1, 2, 3, 4], EntryCount = 5, CheckpointOffset = 9, Version = 0 };

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
        var a = new LeafProjectionDigest { Hash = [1, 2, 3, 4], EntryCount = 5 };
        var b = new LeafProjectionDigest { Hash = [1, 2, 3, 9], EntryCount = 5 };

        Assert.That(a.Equals(b), Is.False);
    }

    [Test]
    public void Not_equal_when_a_scalar_field_differs()
    {
        var a = new LeafProjectionDigest { Hash = [1], EntryCount = 5, CheckpointOffset = 9, Version = 0 };

        Assert.Multiple(() =>
        {
            Assert.That(a.Equals(a with { EntryCount = 6 }), Is.False);
            Assert.That(a.Equals(a with { CheckpointOffset = 10 }), Is.False);
            Assert.That(a.Equals(a with { Version = 1 }), Is.False);
        });
    }

    [Test]
    public void Default_digests_compare_equal()
    {
        Assert.That(default(LeafProjectionDigest).Equals(default), Is.True);
    }

    [Test]
    public void Serialization_round_trip_preserves_value_equality()
    {
        var digest = new LeafProjectionDigest { Hash = [7, 8, 9], EntryCount = 3, CheckpointOffset = 11, Version = 0 };

        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<LeafProjectionDigest>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(digest));

        Assert.Multiple(() =>
        {
            Assert.That(decoded.Equals(digest), Is.True);
            Assert.That(decoded.GetHashCode(), Is.EqualTo(digest.GetHashCode()));
        });
    }
}

