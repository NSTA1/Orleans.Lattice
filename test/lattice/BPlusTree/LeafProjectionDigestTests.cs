using Orleans.Lattice;

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
}

