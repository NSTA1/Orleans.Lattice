using System.Text;

namespace Orleans.Lattice.Schema.Tests;

/// <summary>
/// Unit tests for <see cref="LatticeSchemaEnvelope"/>: the frozen header layout,
/// round-trip encode / strip, self-describing detection of stamped vs un-stamped
/// values, and the short-buffer / null guards.
/// </summary>
public sealed class LatticeSchemaEnvelopeTests
{
    private static byte[] Utf8(string s) => Encoding.UTF8.GetBytes(s);

    [Test]
    public void Constants_are_frozen()
    {
        Assert.That(LatticeSchemaEnvelope.Magic, Is.EqualTo((byte)0xFE));
        Assert.That(LatticeSchemaEnvelope.FormatVersion, Is.EqualTo((byte)0x01));
        Assert.That(LatticeSchemaEnvelope.HeaderLength, Is.EqualTo(10));
    }

    [Test]
    public void Encode_prepends_ten_byte_big_endian_header()
    {
        var body = Utf8("{\"a\":1}");

        var enveloped = LatticeSchemaEnvelope.Encode(0x01020304u, 0x0A0B0C0Du, body);

        Assert.That(enveloped.Length, Is.EqualTo(LatticeSchemaEnvelope.HeaderLength + body.Length));
        Assert.That(enveloped[0], Is.EqualTo((byte)0xFE));
        Assert.That(enveloped[1], Is.EqualTo((byte)0x01));
        // schemaId 0x01020304 big-endian at offset 2..5.
        Assert.That(enveloped[2..6], Is.EqualTo(new byte[] { 0x01, 0x02, 0x03, 0x04 }));
        // version 0x0A0B0C0D big-endian at offset 6..9.
        Assert.That(enveloped[6..10], Is.EqualTo(new byte[] { 0x0A, 0x0B, 0x0C, 0x0D }));
        Assert.That(enveloped[10..], Is.EqualTo(body));
    }

    [Test]
    public void IsEnveloped_true_for_encoded_value()
    {
        var enveloped = LatticeSchemaEnvelope.Encode(1, 2, Utf8("x"));

        Assert.That(LatticeSchemaEnvelope.IsEnveloped(enveloped), Is.True);
    }

    [Test]
    public void IsEnveloped_false_for_plain_utf8_body()
    {
        // A valid UTF-8 / JSON body never begins with 0xFE.
        Assert.That(LatticeSchemaEnvelope.IsEnveloped(Utf8("{\"a\":1}")), Is.False);
    }

    [Test]
    public void IsEnveloped_false_for_short_buffer()
    {
        Assert.That(LatticeSchemaEnvelope.IsEnveloped(new byte[] { 0xFE, 0x01, 0x00 }), Is.False);
    }

    [Test]
    public void IsEnveloped_false_for_unknown_format_version()
    {
        var buffer = LatticeSchemaEnvelope.Encode(1, 1, Utf8("x"));
        buffer[1] = 0x02; // unrecognized envelope-format version

        Assert.That(LatticeSchemaEnvelope.IsEnveloped(buffer), Is.False);
    }

    [Test]
    public void TryReadHeader_reads_encoded_schema_and_version()
    {
        var enveloped = LatticeSchemaEnvelope.Encode(42u, 7u, Utf8("body"));

        var ok = LatticeSchemaEnvelope.TryReadHeader(enveloped, out var schemaId, out var version);

        Assert.That(ok, Is.True);
        Assert.That(schemaId, Is.EqualTo(42u));
        Assert.That(version, Is.EqualTo(7u));
    }

    [Test]
    public void TryReadHeader_returns_false_for_non_envelope()
    {
        var ok = LatticeSchemaEnvelope.TryReadHeader(Utf8("plain"), out var schemaId, out var version);

        Assert.That(ok, Is.False);
        Assert.That(schemaId, Is.EqualTo(0u));
        Assert.That(version, Is.EqualTo(0u));
    }

    [Test]
    public void StripToBody_returns_bytes_after_header()
    {
        var body = Utf8("{\"a\":1}");
        var enveloped = LatticeSchemaEnvelope.Encode(1, 1, body);

        Assert.That(LatticeSchemaEnvelope.StripToBody(enveloped), Is.EqualTo(body));
    }

    [Test]
    public void Encode_then_strip_round_trips_empty_body()
    {
        var enveloped = LatticeSchemaEnvelope.Encode(1, 1, ReadOnlySpan<byte>.Empty);

        Assert.That(enveloped.Length, Is.EqualTo(LatticeSchemaEnvelope.HeaderLength));
        Assert.That(LatticeSchemaEnvelope.StripToBody(enveloped), Is.Empty);
    }

    [Test]
    public void StripToBody_null_throws()
    {
        Assert.That(() => LatticeSchemaEnvelope.StripToBody(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void StripToBody_short_buffer_throws()
    {
        Assert.That(() => LatticeSchemaEnvelope.StripToBody(new byte[] { 0xFE, 0x01 }), Throws.ArgumentException);
    }
}
