using System.Buffers;
using System.Buffers.Binary;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Unit tests for the corrupt- and hostile-payload guards in
/// <see cref="OrleansBinaryReplicationBatchEncoder"/>'s framing decoder, and for
/// the graceful local fallback its encoder applies when a requested
/// dictionary frame cannot be produced on this silo.
/// </summary>
/// <remarks>
/// <para>
/// These guards are security-load-bearing rather than merely defensive: the gRPC
/// transport decodes framing <b>before</b> the shared-secret auth interceptor
/// body runs, so every one of them is reachable pre-auth by an unauthenticated
/// peer. Each turns a forged wire field - a negative length, an entry count
/// larger than the payload can hold, a truncated prefix - into a cheap, catchable
/// <see cref="ArgumentException"/> instead of an out-of-range read or a
/// multi-gigabyte allocation. A guard that stopped firing would not fail any
/// round-trip test, which is exactly why each is asserted directly here.
/// </para>
/// <para>
/// The inflated-tail guards are reached through an identity "compressor"
/// registered under the Zstd tag: it makes the compressed body byte-identical to
/// the tail, so a test can hand-craft precisely the corrupt inflated tail it
/// wants to exercise. Deterministic - no cluster, no real compression.
/// </para>
/// </remarks>
[TestFixture]
public sealed class OrleansBinaryReplicationBatchEncoderFramingGuardTests
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

    private OrleansBinaryReplicationBatchEncoder Encoder(params ILatticeCompressor[] compressors)
        => compressors.Length == 0 ? new(_serializer) : new(_serializer, compressors);

    /// <summary>
    /// A host-defined compression tag. Core tags [0x00, 0x7F] may only be claimed
    /// by compressors declared in the core assembly, so the identity compressor
    /// these tests craft frames with must live in the host-reserved range.
    /// </summary>
    private const LatticeCompression HostTag = (LatticeCompression)0x80;

    private static EncodedBatchHeader Header(
        int entryCount,
        LatticeCompression compression = LatticeCompression.None,
        uint dictionaryId = 0u)
        => new()
        {
            Magic = EncodedBatchHeader.MagicValue,
            WireVersion = EncodedBatchHeader.CurrentWireVersion,
            OriginClusterIdHash = EncodedBatchHeader.HashClusterId("site-a"),
            EntryCount = entryCount,
            BatchSequence = 1L,
            AtomicBatchSpanCount = 0,
            Mode = LatticeMergeMode.LwwRegister,
            Compression = compression,
            DictionaryId = dictionaryId,
        };

    /// <summary>Header bytes followed by <paramref name="tail"/> verbatim, with no length prefixes.</summary>
    private static byte[] RawFrame(EncodedBatchHeader header, ReadOnlySpan<byte> tail)
    {
        var payload = new byte[EncodedBatchHeader.WireSize + tail.Length];
        header.WriteTo(payload);
        tail.CopyTo(payload.AsSpan(EncodedBatchHeader.WireSize));
        return payload;
    }

    /// <summary>
    /// A well-formed compressed frame whose body is <paramref name="inflatedTail"/>
    /// verbatim, for decoding by an encoder holding the identity compressor. The
    /// declared lengths are honest, so the decoder reaches the inflated-tail parse
    /// and the crafted corruption there is what is exercised.
    /// </summary>
    private static byte[] CompressedFrame(EncodedBatchHeader header, byte[] inflatedTail)
    {
        var payload = new byte[EncodedBatchHeader.WireSize + 8 + inflatedTail.Length];
        header.WriteTo(payload);
        BinaryPrimitives.WriteInt32LittleEndian(
            payload.AsSpan(EncodedBatchHeader.WireSize, 4), inflatedTail.Length);
        BinaryPrimitives.WriteInt32LittleEndian(
            payload.AsSpan(EncodedBatchHeader.WireSize + 4, 4), inflatedTail.Length);
        inflatedTail.CopyTo(payload.AsSpan(EncodedBatchHeader.WireSize + 8));
        return payload;
    }

    /// <summary>A tail carrying two empty length-prefixed routing strings, then <paramref name="rest"/>.</summary>
    private static byte[] TailWithEmptyRouting(params byte[] rest)
    {
        var tail = new byte[8 + rest.Length];
        rest.CopyTo(tail.AsSpan(8));
        return tail;
    }

    private static byte[] Int32Le(int value)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, value);
        return bytes;
    }

    /// <summary>The core dictionary compressor, holding exactly one dictionary id.</summary>
    private static ZstdDictionaryLatticeCompressor DictionaryCompressor(uint availableDictionaryId)
        => new(3, new SingleDictionaryProvider(availableDictionaryId, new byte[512]));

    // ---- uncompressed-path guards ------------------------------------------

    [Test]
    public void Decode_rejects_a_negative_entry_count()
    {
        var payload = RawFrame(Header(-1), ReadOnlySpan<byte>.Empty);

        Assert.That(
            () => Encoder().TryDecodeFraming(payload, out _, out _, out _, out _),
            Throws.ArgumentException.With.Message.Contains("negative entry count"),
            "A negative entry count is a forged header field and must be rejected before it sizes an array.");
    }

    [Test]
    public void Decode_rejects_an_entry_count_larger_than_the_payload_can_hold()
    {
        // Every entry contributes at least a 4-byte length prefix, so this count
        // is unsatisfiable and would otherwise size a multi-gigabyte array.
        var payload = RawFrame(Header(int.MaxValue), ReadOnlySpan<byte>.Empty);

        Assert.That(
            () => Encoder().TryDecodeFraming(payload, out _, out _, out _, out _),
            Throws.ArgumentException.With.Message.Contains("can hold at most"),
            "An unsatisfiable entry count must be rejected before the segment array is allocated.");
    }

    [Test]
    public void Decode_copies_a_payload_whose_memory_is_not_array_backed()
    {
        var writer = new ArrayBufferWriter<byte>();
        var entries = new[] { new ArraySegment<byte>(new byte[] { 1, 2, 3 }) };
        Encoder().EncodeFraming(Header(1), "tree-1", "site-a", entries, writer);

        using var manager = new NonArrayMemoryManager(writer.WrittenMemory.ToArray());

        var ok = Encoder().TryDecodeFraming(
            manager.Memory, out _, out var treeName, out var originClusterId, out var decoded);

        Assert.That(ok, Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(treeName, Is.EqualTo("tree-1"));
            Assert.That(originClusterId, Is.EqualTo("site-a"));
            Assert.That(decoded.Span[0].ToArray(), Is.EqualTo(new byte[] { 1, 2, 3 }),
                "A payload with no backing array is copied once so entries can still be sliced.");
        });
    }

    // ---- compressed-tail prefix guards -------------------------------------

    [Test]
    public void Decode_rejects_a_frame_truncated_at_the_compressed_tail_length_prefixes()
    {
        var payload = RawFrame(Header(1, HostTag), new byte[] { 0, 0, 0 });

        Assert.That(
            () => Encoder(new IdentityCompressor(HostTag))
                .TryDecodeFraming(payload, out _, out _, out _, out _),
            Throws.ArgumentException.With.Message.Contains("compressed-tail length prefixes"));
    }

    [Test]
    public void Decode_rejects_a_negative_declared_tail_length()
    {
        var payload = RawFrame(
            Header(1, HostTag),
            [.. Int32Le(-1), .. Int32Le(4)]);

        Assert.That(
            () => Encoder(new IdentityCompressor(HostTag))
                .TryDecodeFraming(payload, out _, out _, out _, out _),
            Throws.ArgumentException.With.Message.Contains("negative tail length"));
    }

    // ---- dictionary-frame guards -------------------------------------------

    [Test]
    public void Decode_rejects_a_dictionary_frame_truncated_at_the_dictionary_id_prefix()
    {
        var payload = RawFrame(
            Header(1, LatticeCompression.ZstdDictionary, dictionaryId: 7u),
            new byte[] { 0, 0 });

        using var compressor = DictionaryCompressor(availableDictionaryId: 7u);

        Assert.That(
            () => Encoder(compressor).TryDecodeFraming(payload, out _, out _, out _, out _),
            Throws.ArgumentException.With.Message.Contains("dictionary-id prefix"),
            "The 4-byte dictionary id is read before any length prefix, so a frame too short to carry it "
            + "must be rejected rather than read out of range.");
    }

    [Test]
    public void Decode_rejects_a_dictionary_frame_referencing_a_dictionary_this_receiver_lacks()
    {
        var payload = RawFrame(
            Header(1, LatticeCompression.ZstdDictionary, dictionaryId: 9u),
            [.. Int32Le(9), .. Int32Le(0), .. Int32Le(0)]);

        using var compressor = DictionaryCompressor(availableDictionaryId: 7u);

        Assert.That(
            () => Encoder(compressor).TryDecodeFraming(payload, out _, out _, out _, out _),
            Throws.InstanceOf<NotSupportedException>().With.Message.Contains("not available on this receiver"),
            "An unresolvable dictionary must fault loudly rather than silently mis-decode.");
    }

    // ---- inflated-tail guards ----------------------------------------------

    [Test]
    public void Decode_rejects_an_inflated_tail_truncated_at_a_routing_field_prefix()
    {
        var payload = CompressedFrame(Header(0, HostTag), new byte[] { 0, 0 });

        Assert.That(
            () => Encoder(new IdentityCompressor(HostTag))
                .TryDecodeFraming(payload, out _, out _, out _, out _),
            Throws.ArgumentException.With.Message.Contains("length prefix for treeName"));
    }

    [Test]
    public void Decode_rejects_an_inflated_tail_whose_routing_field_overruns_it()
    {
        var payload = CompressedFrame(Header(0, HostTag), Int32Le(int.MaxValue));

        Assert.That(
            () => Encoder(new IdentityCompressor(HostTag))
                .TryDecodeFraming(payload, out _, out _, out _, out _),
            Throws.ArgumentException.With.Message.Contains("would overrun the tail"),
            "The declared field length is widened to long before the bound check, so it cannot overflow past it.");
    }

    [Test]
    public void Decode_rejects_an_inflated_tail_entry_count_larger_than_the_tail_can_hold()
    {
        var payload = CompressedFrame(Header(1_000, HostTag), TailWithEmptyRouting());

        Assert.That(
            () => Encoder(new IdentityCompressor(HostTag))
                .TryDecodeFraming(payload, out _, out _, out _, out _),
            Throws.ArgumentException.With.Message.Contains("can hold at most"),
            "The inflated tail's entry count must be bounded before the segment array is allocated.");
    }

    [Test]
    public void Decode_rejects_an_inflated_tail_truncated_at_an_entry_length_prefix()
    {
        var payload = CompressedFrame(Header(1, HostTag), TailWithEmptyRouting());

        Assert.That(
            () => Encoder(new IdentityCompressor(HostTag))
                .TryDecodeFraming(payload, out _, out _, out _, out _),
            Throws.ArgumentException.With.Message.Contains("length prefix for entry 0"));
    }

    [Test]
    public void Decode_rejects_an_inflated_tail_whose_entry_body_overruns_it()
    {
        var payload = CompressedFrame(
            Header(1, HostTag),
            TailWithEmptyRouting(Int32Le(int.MaxValue)));

        Assert.That(
            () => Encoder(new IdentityCompressor(HostTag))
                .TryDecodeFraming(payload, out _, out _, out _, out _),
            Throws.ArgumentException.With.Message.Contains("would overrun the tail"));
    }

    [Test]
    public void Decode_reads_a_well_formed_inflated_tail()
    {
        var payload = CompressedFrame(
            Header(1, HostTag),
            TailWithEmptyRouting([.. Int32Le(2), 7, 8]));

        var ok = Encoder(new IdentityCompressor(HostTag))
            .TryDecodeFraming(payload, out _, out var treeName, out _, out var entries);

        Assert.That(ok, Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(treeName, Is.Empty, "An empty routing string round-trips as empty, not null.");
            Assert.That(entries.Span[0].ToArray(), Is.EqualTo(new byte[] { 7, 8 }));
        });
    }

    // ---- encoder-side graceful fallback ------------------------------------

    [Test]
    public void Encode_degrades_a_dictionary_frame_to_verbatim_when_no_compressor_can_serve_it()
    {
        var encoder = Encoder();
        var writer = new ArrayBufferWriter<byte>();
        var entries = new[] { new ArraySegment<byte>(new byte[] { 1, 2, 3 }) };

        encoder.EncodeFraming(
            Header(1, LatticeCompression.ZstdDictionary, dictionaryId: 7u),
            "tree-1", "site-a", entries, writer);

        var ok = encoder.TryDecodeFraming(
            writer.WrittenMemory, out var header, out var treeName, out _, out var decoded);

        Assert.That(ok, Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(header.Compression, Is.EqualTo(LatticeCompression.None),
                "With neither a dictionary-aware nor a plain Zstd compressor the frame degrades to verbatim, "
                + "so a peer without the dictionary can still decode it.");
            Assert.That(header.DictionaryId, Is.Zero,
                "The verbatim frame must not advertise a dictionary it did not use.");
            Assert.That(treeName, Is.EqualTo("tree-1"));
            Assert.That(decoded.Span[0].ToArray(), Is.EqualTo(new byte[] { 1, 2, 3 }));
        });
    }

    [Test]
    public void Encode_degrades_a_dictionary_frame_to_plain_compression_when_only_that_is_available()
    {
        using var zstd = new ZstdLatticeCompressor(3);
        var encoder = Encoder(zstd);
        var writer = new ArrayBufferWriter<byte>();
        var entries = new[] { new ArraySegment<byte>(new byte[] { 1, 2, 3 }) };

        encoder.EncodeFraming(
            Header(1, LatticeCompression.ZstdDictionary, dictionaryId: 7u),
            "tree-1", "site-a", entries, writer);

        var ok = encoder.TryDecodeFraming(
            writer.WrittenMemory, out var header, out _, out _, out var decoded);

        Assert.That(ok, Is.True);
        Assert.Multiple(() =>
        {
            Assert.That(header.Compression, Is.EqualTo(LatticeCompression.Zstd),
                "The dictionary tag degrades to plain compression, which any peer with the core "
                + "compressor can still decode.");
            Assert.That(header.DictionaryId, Is.Zero);
            Assert.That(decoded.Span[0].ToArray(), Is.EqualTo(new byte[] { 1, 2, 3 }));
        });
    }

    /// <summary>
    /// A verbatim "compressor": the compressed body is byte-identical to the
    /// source, so a test can hand-craft the exact inflated tail the decoder will
    /// parse.
    /// </summary>
    private sealed class IdentityCompressor(LatticeCompression algorithm) : ILatticeCompressor
    {
        public LatticeCompression Algorithm => algorithm;

        public int GetMaxCompressedLength(int uncompressedLength) => uncompressedLength;

        public int Compress(ReadOnlySpan<byte> source, Span<byte> destination)
        {
            source.CopyTo(destination);
            return source.Length;
        }

        public void Decompress(ReadOnlySpan<byte> source, Span<byte> destination, int uncompressedLength)
            => source[..uncompressedLength].CopyTo(destination);
    }

    /// <summary>A dictionary provider that resolves exactly one dictionary id.</summary>
    private sealed class SingleDictionaryProvider(uint availableDictionaryId, byte[] dictionary)
        : ILatticeCompressionDictionaryProvider
    {
        public bool TryGetDictionary(uint dictionaryId, out ReadOnlyMemory<byte> value)
        {
            if (dictionaryId == availableDictionaryId)
            {
                value = dictionary;
                return true;
            }

            value = default;
            return false;
        }
    }

    /// <summary>
    /// Memory whose backing store cannot be recovered as an array, so the decoder
    /// takes its one-copy path rather than slicing in place.
    /// </summary>
    private sealed class NonArrayMemoryManager(byte[] data) : MemoryManager<byte>
    {
        public override Span<byte> GetSpan() => data.AsSpan();

        public override MemoryHandle Pin(int elementIndex = 0) => default;

        public override void Unpin()
        {
        }

        protected override void Dispose(bool disposing)
        {
        }
    }
}
