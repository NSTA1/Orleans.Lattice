using Orleans.Lattice;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit coverage for <see cref="ZstdLatticeCompressor"/>. Validates
/// constructor argument checking, the algorithm identifier, and
/// round-trip behaviour over a realistic uncompressed-tail-shaped
/// byte block.
/// </summary>
[TestFixture]
public class ZstdLatticeCompressorTests
{
    [Test]
    public void Algorithm_returns_Zstd()
    {
        using var c = new ZstdLatticeCompressor(3);
        Assert.That(c.Algorithm, Is.EqualTo(LatticeCompression.Zstd));
    }

    [Test]
    public void Constructor_rejects_out_of_range_compression_level()
    {
        Assert.That(
            () => new ZstdLatticeCompressor(0),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
        Assert.That(
            () => new ZstdLatticeCompressor(99),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void Compress_of_empty_input_emits_a_decompressible_empty_frame()
    {
        using var c = new ZstdLatticeCompressor(3);
        // A Span destination is a struct - it cannot be null - so the
        // historical "null destination" guard is moot. Pin the boundary
        // case (empty source) instead: Zstd's empty frame still carries
        // header bytes, so the write is strictly positive, stays inside
        // the advertised bound, and round-trips back to nothing.
        var bound = c.GetMaxCompressedLength(0);
        var dest = new byte[bound];
        var written = c.Compress(ReadOnlySpan<byte>.Empty, dest);

        Assert.Multiple(() =>
        {
            Assert.That(written, Is.GreaterThan(0),
                "an empty payload still produces a framed Zstd block, not zero bytes");
            Assert.That(written, Is.LessThanOrEqualTo(bound),
                "GetMaxCompressedLength(0) must bound the empty frame it is sized for");
        });

        // The frame is a real frame: decompressing it yields the empty
        // payload rather than throwing, which is what makes the boundary
        // safe for a zero-length WAL/segment write.
        Assert.That(
            () => c.Decompress(dest.AsSpan(0, written), Span<byte>.Empty, 0),
            Throws.Nothing);
    }

    [Test]
    public void Compress_then_Decompress_round_trips_random_bytes()
    {
        using var c = new ZstdLatticeCompressor(3);

        // Highly compressible payload exercises the meaningful code
        // path; a 4 KiB run of the same byte compresses to a small
        // frame so the round-trip is non-trivial.
        var src = new byte[4096];
        for (var i = 0; i < src.Length; i++)
        {
            src[i] = (byte)(i % 7);
        }

        var bound = c.GetMaxCompressedLength(src.Length);
        var dest = new byte[bound];
        var written = c.Compress(src, dest);
        Assert.That(written, Is.GreaterThan(0));
        Assert.That(written, Is.LessThan(src.Length),
            "Zstd should compress a repeating 4KiB pattern below its raw length.");

        var inflated = new byte[src.Length];
        c.Decompress(dest.AsSpan(0, written), inflated, src.Length);
        Assert.That(inflated, Is.EqualTo(src));
    }

    [Test]
    public void Decompress_throws_when_destination_length_does_not_match()
    {
        using var c = new ZstdLatticeCompressor(3);
        var src = new byte[64];
        for (var i = 0; i < src.Length; i++) src[i] = (byte)i;
        var dest = new byte[c.GetMaxCompressedLength(src.Length)];
        var written = c.Compress(src, dest);

        var wrong = new byte[src.Length + 1];
        Assert.That(
            () => c.Decompress(dest.AsSpan(0, written), wrong, src.Length),
            Throws.ArgumentException);
    }

    [Test]
    public void Decompress_throws_on_corrupt_payload()
    {
        using var c = new ZstdLatticeCompressor(3);
        var bogus = new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 };
        Assert.That(
            () => c.Decompress(bogus, new byte[16], 16),
            Throws.ArgumentException);
    }

    [Test]
    public void Compress_and_Decompress_after_dispose_throw()
    {
        var c = new ZstdLatticeCompressor(3);
        c.Dispose();
        Assert.That(
            () => c.Compress(new byte[8], new byte[64]),
            Throws.InstanceOf<ObjectDisposedException>());
        Assert.That(
            () => c.Decompress(new byte[8], new byte[8], 8),
            Throws.InstanceOf<ObjectDisposedException>());
    }
}
