using System.Text;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaEnvelopeCodec"/>: version reading from a
/// stamped merge input, the version-agnostic strip that recovers a producer's exact
/// body, and the determinism property that an apply-time strip and every later
/// replay strip yield byte-identical fold bodies.
/// </summary>
public sealed class LatticeSchemaEnvelopeCodecTests
{
    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    private static readonly LatticeSchemaEnvelopeCodec Codec = new();

    [Test]
    public void IsActive_is_true_for_every_tree()
    {
        Assert.That(Codec.IsActive("orders"), Is.True);
        Assert.That(Codec.IsActive("anything"), Is.True);
    }

    [Test]
    public void ReadVersion_null_value_is_zero()
    {
        Assert.That(Codec.ReadVersion(null), Is.EqualTo(0u));
    }

    [Test]
    public void ReadVersion_unenveloped_value_is_zero()
    {
        Assert.That(Codec.ReadVersion(Utf8("raw-delta")), Is.EqualTo(0u));
    }

    [Test]
    public void ReadVersion_enveloped_value_returns_stamped_version()
    {
        var enveloped = LatticeSchemaEnvelope.Encode(schemaId: 7, version: 4, Utf8("body"));
        Assert.That(Codec.ReadVersion(enveloped), Is.EqualTo(4u));
    }

    [Test]
    public void StripForFold_unenveloped_delta_returns_same_reference()
    {
        var raw = Utf8("raw-delta");
        Assert.That(Codec.StripForFold(raw), Is.SameAs(raw));
    }

    [Test]
    public void StripForFold_enveloped_delta_recovers_exact_body()
    {
        var body = Utf8("typed-crdt-body");
        var enveloped = LatticeSchemaEnvelope.Encode(schemaId: 1, version: 2, body);

        Assert.That(Codec.StripForFold(enveloped), Is.EqualTo(body));
    }

    [Test]
    public void StripForFold_is_version_agnostic_across_targets()
    {
        // The SACRED INVARIANT: a delta enveloped at v2 strips to the same body no
        // matter what the tree's current target is. The strip never upcasts, so an
        // apply-time fold and every later cold-replay fold see byte-identical bytes.
        var body = Utf8("delta-body");
        var envV2 = LatticeSchemaEnvelope.Encode(schemaId: 1, version: 2, body);
        var envV9 = LatticeSchemaEnvelope.Encode(schemaId: 1, version: 9, body);

        // Same body, different stamped version -> identical strip output.
        Assert.That(Codec.StripForFold(envV2), Is.EqualTo(body));
        Assert.That(Codec.StripForFold(envV9), Is.EqualTo(body));
        Assert.That(Codec.StripForFold(envV2), Is.EqualTo(Codec.StripForFold(envV2)));
    }

    [Test]
    public void StripForFold_null_delta_throws()
    {
        Assert.That(() => Codec.StripForFold(null!), Throws.ArgumentNullException);
    }
}
