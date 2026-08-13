namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Tests for <see cref="VectorCodec"/>: the little-endian encoding round-trips,
/// the content address is deterministic and content-sensitive, and the source
/// identifier is stable per key.
/// </summary>
[TestFixture]
public sealed class VectorCodecTests
{
    [Test]
    public void Encode_then_decode_round_trips_the_components()
    {
        var vector = new[] { 0.5f, -1.25f, 3.0f, 0f };

        var decoded = VectorCodec.Decode(VectorCodec.Encode(vector));

        Assert.That(decoded, Is.EqualTo(vector));
    }

    [Test]
    public void Encode_produces_four_bytes_per_component()
    {
        var bytes = VectorCodec.Encode(new[] { 1f, 2f, 3f });

        Assert.That(bytes, Has.Length.EqualTo(3 * sizeof(float)));
    }

    [Test]
    public void ContentAddress_is_stable_for_identical_payloads()
    {
        var left = VectorCodec.Encode(new[] { 1f, 2f, 3f });
        var right = VectorCodec.Encode(new[] { 1f, 2f, 3f });

        Assert.That(VectorCodec.ContentAddress(left), Is.EqualTo(VectorCodec.ContentAddress(right)));
    }

    [Test]
    public void ContentAddress_differs_for_different_payloads()
    {
        var left = VectorCodec.Encode(new[] { 1f, 2f, 3f });
        var right = VectorCodec.Encode(new[] { 1f, 2f, 3.5f });

        Assert.That(VectorCodec.ContentAddress(left), Is.Not.EqualTo(VectorCodec.ContentAddress(right)));
    }

    [Test]
    public void ContentAddress_is_lowercase_hex_of_sha256_length()
    {
        var address = VectorCodec.ContentAddress(VectorCodec.Encode(new[] { 1f }));

        Assert.Multiple(() =>
        {
            Assert.That(address, Has.Length.EqualTo(64));
            Assert.That(address, Does.Match("^[0-9a-f]+$"));
        });
    }

    [Test]
    public void SourceId_is_stable_and_sixteen_hex_chars()
    {
        var first = VectorCodec.SourceId("repo/r/file/src/Program.cs");
        var second = VectorCodec.SourceId("repo/r/file/src/Program.cs");

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo(second));
            Assert.That(first, Has.Length.EqualTo(16));
            Assert.That(first, Does.Match("^[0-9a-f]+$"));
        });
    }

    [Test]
    public void SourceId_differs_for_different_keys()
        => Assert.That(
            VectorCodec.SourceId("repo/r/file/a.cs"),
            Is.Not.EqualTo(VectorCodec.SourceId("repo/r/file/b.cs")));

    [Test]
    public void Decode_rejects_a_length_that_is_not_a_whole_number_of_components()
        => Assert.Throws<ArgumentException>(() => VectorCodec.Decode(new byte[] { 1, 2, 3 }));

    [Test]
    public void Decode_rejects_a_null_payload()
        => Assert.Throws<ArgumentNullException>(() => VectorCodec.Decode(null!));

    [Test]
    public void SourceId_rejects_a_null_key()
        => Assert.Throws<ArgumentNullException>(() => VectorCodec.SourceId(null!));
}
