namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for <see cref="NullLatticeEnvelopeCodec"/>: the core no-op envelope
/// codec must be inert - never active, every value unversioned, and every strip an
/// identity - so the CRDT fold path stays byte-for-byte identical when no schema /
/// versioning add-on is registered.
/// </summary>
[TestFixture]
public class NullLatticeEnvelopeCodecTests
{
    private static readonly NullLatticeEnvelopeCodec Codec = new();

    [Test]
    public void IsActive_is_never_true()
    {
        Assert.That(Codec.IsActive("any-tree"), Is.False);
        Assert.That(Codec.IsActive(string.Empty), Is.False);
    }

    [Test]
    public void ReadVersion_is_always_zero()
    {
        Assert.That(Codec.ReadVersion(null), Is.EqualTo(0u));
        Assert.That(Codec.ReadVersion(new byte[] { 1, 2, 3 }), Is.EqualTo(0u));
    }

    [Test]
    public void StripForFold_returns_same_reference()
    {
        var delta = new byte[] { 1, 2, 3 };
        Assert.That(Codec.StripForFold(delta), Is.SameAs(delta));
    }
}
