using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Replication;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Integration coverage for the compressed-tail framing path on
/// <see cref="OrleansBinaryReplicationBatchEncoder"/>: encode/decode
/// round-trips, pluggable compressor dispatch, registration validation
/// and unsupported-algorithm error surfacing.
/// </summary>
[TestFixture]
public class CompressedFramingRoundtripTests
{
    private ServiceProvider _services = null!;
    private Serializer<ReplicationBatchEnvelope> _serializer = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        _serializer = _services.GetRequiredService<Serializer<ReplicationBatchEnvelope>>();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private OrleansBinaryReplicationBatchEncoder NewEncoderWithZstd(int level = 3)
    {
        var compressor = new ZstdLatticeCompressor(level);
        return new OrleansBinaryReplicationBatchEncoder(_serializer, new ILatticeCompressor[] { compressor });
    }

    private OrleansBinaryReplicationBatchEncoder NewEncoderNoCompressors()
        => new(_serializer);

    private static EncodedBatchHeader Header(
        int entryCount,
        LatticeCompression compression = LatticeCompression.None,
        long batchSeq = 1L)
        => new()
        {
            Magic = EncodedBatchHeader.MagicValue,
            WireVersion = EncodedBatchHeader.CurrentWireVersion,
            OriginClusterIdHash = EncodedBatchHeader.HashClusterId("site-a"),
            EntryCount = entryCount,
            BatchSequence = batchSeq,
            AtomicBatchSpanCount = 0,
            Mode = LatticeMergeMode.LwwRegister,
            Compression = compression,
        };

    [Test]
    public void Encoder_constructor_rejects_duplicate_compressor_algorithm()
    {
        Assert.That(
            () => new OrleansBinaryReplicationBatchEncoder(
                _serializer,
                new ILatticeCompressor[]
                {
                    new ZstdLatticeCompressor(3),
                    new ZstdLatticeCompressor(5),
                }),
            Throws.ArgumentException);
    }

    [Test]
    public void Encoder_constructor_rejects_compressor_claiming_None()
    {
        Assert.That(
            () => new OrleansBinaryReplicationBatchEncoder(
                _serializer,
                new ILatticeCompressor[] { new NoneClaimingCompressor() }),
            Throws.ArgumentException);
    }

    [TestCase((byte)0x01)] // Defined core member (Zstd) - host type cannot squat on it
    [TestCase((byte)0x02)]
    [TestCase((byte)0x42)]
    [TestCase((byte)0x7F)]
    public void Encoder_constructor_rejects_compressor_claiming_core_reserved_tag(byte tag)
    {
        // Tags in [0x00, 0x7F] are core-reserved. Only types
        // declared in the core Orleans.Lattice assembly may claim
        // a tag in this range. A host type registering a
        // compressor whose Algorithm tag falls in the core range -
        // including a defined core member like Zstd (0x01) - must
        // be rejected at startup, because either (a) a future core
        // release would collide with it on the wire (undefined
        // tags), or (b) the host type would silently squat on the
        // wire identity of the canonical core implementation
        // (defined tags), producing receiver-side decode failures
        // against any peer running the core implementation. The
        // rejection is a meaningful ArgumentException - not a
        // convention-only constraint documented in compression.md.
        var rogue = new IdentityCompressor((LatticeCompression)tag);
        Assert.That(
            () => new OrleansBinaryReplicationBatchEncoder(
                _serializer,
                new ILatticeCompressor[] { rogue }),
            Throws.ArgumentException
                .With.Message.Contains("core-reserved"));
    }

    [Test]
    public void Encoder_constructor_accepts_compressor_claiming_defined_core_tag()
    {
        // Sanity: the canonical core ZstdLatticeCompressor is
        // declared in the Orleans.Lattice assembly and so is
        // exempt from the core-reserved-range rule - it can claim
        // its own LatticeCompression.Zstd tag without rejection.
        Assert.That(
            () => new OrleansBinaryReplicationBatchEncoder(
                _serializer,
                new ILatticeCompressor[] { new ZstdLatticeCompressor(3) }),
            Throws.Nothing);
    }

    [Test]
    public void EncodeFraming_with_Zstd_produces_payload_decodable_with_same_encoder()
    {
        var encoder = NewEncoderWithZstd();
        var writer = new ArrayBufferWriter<byte>();

        // Use a moderately compressible body so Zstd has something to
        // do; the round-trip must restore every byte verbatim.
        var bodyA = new byte[256];
        for (var i = 0; i < bodyA.Length; i++) bodyA[i] = (byte)(i % 9);
        var bodyB = new byte[64];
        for (var i = 0; i < bodyB.Length; i++) bodyB[i] = 0xAA;
        var entries = new[] { new ArraySegment<byte>(bodyA), new ArraySegment<byte>(bodyB) };
        var header = Header(entries.Length, LatticeCompression.Zstd);

        encoder.EncodeFraming(header, "tree-1", "site-a", entries, writer);

        var ok = encoder.TryDecodeFraming(
            writer.WrittenMemory,
            out var dh,
            out var tree,
            out var origin,
            out var decoded);

        Assert.That(ok, Is.True);
        Assert.That(dh.Compression, Is.EqualTo(LatticeCompression.Zstd));
        Assert.That(tree, Is.EqualTo("tree-1"));
        Assert.That(origin, Is.EqualTo("site-a"));
        Assert.That(decoded.Length, Is.EqualTo(2));
        Assert.That(decoded.Span[0].ToArray(), Is.EqualTo(bodyA));
        Assert.That(decoded.Span[1].ToArray(), Is.EqualTo(bodyB));
    }

    [Test]
    public void EncodeFraming_with_None_writes_canonical_uncompressed_layout()
    {
        var encoder = NewEncoderWithZstd();
        var writer = new ArrayBufferWriter<byte>();
        var entries = new[] { new ArraySegment<byte>(new byte[] { 1, 2, 3 }) };
        encoder.EncodeFraming(
            Header(1, LatticeCompression.None),
            "tree-1",
            "site-a",
            entries,
            writer);

        // Header (32) + treeName length-prefixed (4 + 6) + origin (4 + 6)
        // + entry length (4) + entry body (3) = 59 bytes.
        const int Expected = EncodedBatchHeader.WireSize + 4 + 6 + 4 + 6 + 4 + 3;
        Assert.That(writer.WrittenCount, Is.EqualTo(Expected));
    }

    [Test]
    public void TryDecodeFraming_throws_when_no_compressor_is_registered_for_algorithm()
    {
        // Producer uses a Zstd-aware encoder.
        var producer = NewEncoderWithZstd();
        var writer = new ArrayBufferWriter<byte>();
        var entries = new[] { new ArraySegment<byte>(new byte[] { 1, 2, 3, 4 }) };
        producer.EncodeFraming(
            Header(1, LatticeCompression.Zstd),
            "tree-1",
            "site-a",
            entries,
            writer);

        // Consumer encoder has no compressors registered; receiving
        // a Zstd batch must fail fast with NotSupportedException.
        var consumer = NewEncoderNoCompressors();
        Assert.That(
            () => consumer.TryDecodeFraming(
                writer.WrittenMemory,
                out _,
                out _,
                out _,
                out _),
            Throws.InstanceOf<NotSupportedException>());
    }

    [Test]
    public void EncodeFraming_throws_when_no_compressor_is_registered_for_algorithm()
    {
        var encoder = NewEncoderNoCompressors();
        var writer = new ArrayBufferWriter<byte>();
        Assert.That(
            () => encoder.EncodeFraming(
                Header(0, LatticeCompression.Zstd),
                "tree-1",
                "site-a",
                ReadOnlyMemory<ArraySegment<byte>>.Empty,
                writer),
            Throws.InstanceOf<NotSupportedException>());
    }

    [Test]
    public void EncodeFraming_with_host_reserved_byte_tag_round_trips_through_custom_compressor()
    {
        // A host can ship a custom ILatticeCompressor whose
        // Algorithm tag is a byte in the reserved [0x80, 0xFF]
        // range. The encoder must key its dispatch dictionary on
        // the raw byte (not the named LatticeCompression enum
        // members) so the custom tag round-trips through encode
        // and decode without any core enum churn.
        const byte CustomTag = 0xC3;
        var custom = new IdentityCompressor((LatticeCompression)CustomTag);
        var encoder = new OrleansBinaryReplicationBatchEncoder(
            _serializer,
            new ILatticeCompressor[] { custom });

        var body = new byte[] { 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 };
        var entries = new[] { new ArraySegment<byte>(body) };
        var header = Header(entries.Length, (LatticeCompression)CustomTag);

        var writer = new ArrayBufferWriter<byte>();
        encoder.EncodeFraming(header, "tree-1", "site-a", entries, writer);

        var ok = encoder.TryDecodeFraming(
            writer.WrittenMemory,
            out var dh,
            out var tree,
            out var origin,
            out var decoded);

        Assert.Multiple(() =>
        {
            Assert.That(ok, Is.True);
            Assert.That((byte)dh.Compression, Is.EqualTo(CustomTag));
            Assert.That(tree, Is.EqualTo("tree-1"));
            Assert.That(origin, Is.EqualTo("site-a"));
            Assert.That(decoded.Length, Is.EqualTo(1));
            Assert.That(decoded.Span[0].ToArray(), Is.EqualTo(body));
        });
    }

    private sealed class NoneClaimingCompressor : ILatticeCompressor
    {
        public LatticeCompression Algorithm => LatticeCompression.None;
        public int GetMaxCompressedLength(int uncompressedLength) => uncompressedLength;
        public int Compress(ReadOnlySpan<byte> source, Span<byte> destination) => throw new NotImplementedException();
        public void Decompress(ReadOnlySpan<byte> source, Span<byte> destination, int uncompressedLength) => throw new NotImplementedException();
    }

    /// <summary>
    /// Test-only no-op compressor that copies bytes through verbatim
    /// while reporting an arbitrary <see cref="LatticeCompression"/>
    /// tag. Used to pin the byte-keyed dispatch path against
    /// host-defined tag values.
    /// </summary>
    private sealed class IdentityCompressor(LatticeCompression algorithm) : ILatticeCompressor
    {
        public LatticeCompression Algorithm { get; } = algorithm;
        public int GetMaxCompressedLength(int uncompressedLength) => uncompressedLength;
        public int Compress(ReadOnlySpan<byte> source, Span<byte> destination)
        {
            source.CopyTo(destination);
            return source.Length;
        }
        public void Decompress(ReadOnlySpan<byte> source, Span<byte> destination, int uncompressedLength)
        {
            if (source.Length != uncompressedLength)
            {
                throw new ArgumentException("Identity compressor expects matching source and uncompressed lengths.");
            }
            source.CopyTo(destination);
        }
    }
}
