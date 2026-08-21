using System.Buffers;
using System.Buffers.Binary;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Exercises the default <see cref="IReplicationBatchEncoder.EncodeFraming"/>
/// and <see cref="IReplicationBatchEncoder.TryDecodeFraming"/> interface
/// implementations directly. The canonical
/// <see cref="OrleansBinaryReplicationBatchEncoder"/> overrides both, so the
/// interface defaults are only reachable through an encoder that leaves them
/// inherited - modelled here by <see cref="DefaultsOnlyEncoder"/>, a minimal
/// implementation supplying only the abstract members.
/// </summary>
[TestFixture]
public class IReplicationBatchEncoderDefaultFramingTests
{
    private const string TreeName = "tree-1";
    private const string OriginClusterId = "site-a";

    private readonly IReplicationBatchEncoder _encoder = new DefaultsOnlyEncoder();

    private static EncodedBatchHeader MakeHeader(int entryCount, long batchSeq = 7L)
        => new()
        {
            Magic = EncodedBatchHeader.MagicValue,
            WireVersion = EncodedBatchHeader.CurrentWireVersion,
            OriginClusterIdHash = EncodedBatchHeader.HashClusterId(OriginClusterId),
            EntryCount = entryCount,
            BatchSequence = batchSeq,
            AtomicBatchSpanCount = 0,
            Mode = LatticeMergeMode.LwwRegister,
            Compression = LatticeCompression.None,
        };

    private static ArraySegment<byte> Seg(params byte[] bytes) => new(bytes);

    private static byte[] EncodeSample(
        IReplicationBatchEncoder encoder,
        EncodedBatchHeader header,
        string treeName,
        string originClusterId,
        params ArraySegment<byte>[] entries)
    {
        var writer = new ArrayBufferWriter<byte>();
        encoder.EncodeFraming(header, treeName, originClusterId, entries, writer);
        return writer.WrittenMemory.ToArray();
    }

    [Test]
    public void EncodeFraming_throws_on_null_writer()
    {
        Assert.That(
            () => _encoder.EncodeFraming(
                MakeHeader(0), TreeName, OriginClusterId,
                ReadOnlyMemory<ArraySegment<byte>>.Empty, null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void EncodeFraming_throws_on_null_treeName()
    {
        Assert.That(
            () => _encoder.EncodeFraming(
                MakeHeader(0), null!, OriginClusterId,
                ReadOnlyMemory<ArraySegment<byte>>.Empty, new ArrayBufferWriter<byte>()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void EncodeFraming_throws_on_null_originClusterId()
    {
        Assert.That(
            () => _encoder.EncodeFraming(
                MakeHeader(0), TreeName, null!,
                ReadOnlyMemory<ArraySegment<byte>>.Empty, new ArrayBufferWriter<byte>()),
            Throws.ArgumentNullException);
    }

    [Test]
    public void EncodeFraming_throws_on_empty_treeName()
    {
        Assert.That(
            () => _encoder.EncodeFraming(
                MakeHeader(0), string.Empty, OriginClusterId,
                ReadOnlyMemory<ArraySegment<byte>>.Empty, new ArrayBufferWriter<byte>()),
            Throws.ArgumentException);
    }

    [Test]
    public void EncodeFraming_throws_on_empty_originClusterId()
    {
        Assert.That(
            () => _encoder.EncodeFraming(
                MakeHeader(0), TreeName, string.Empty,
                ReadOnlyMemory<ArraySegment<byte>>.Empty, new ArrayBufferWriter<byte>()),
            Throws.ArgumentException);
    }

    [Test]
    public void EncodeFraming_throws_when_entryCount_disagrees_with_entries_length()
    {
        Assert.That(
            () => _encoder.EncodeFraming(
                MakeHeader(2), TreeName, OriginClusterId,
                new[] { Seg(1) }, new ArrayBufferWriter<byte>()),
            Throws.ArgumentException);
    }

    [Test]
    public void TryDecodeFraming_round_trips_header_routing_and_entries()
    {
        var header = MakeHeader(3, batchSeq: 99L);
        var payload = EncodeSample(
            _encoder, header, TreeName, OriginClusterId,
            Seg(1, 2, 3), Seg(), Seg(9, 8));

        var ok = _encoder.TryDecodeFraming(
            payload,
            out var decodedHeader,
            out var treeName,
            out var originClusterId,
            out var entries);

        Assert.That(ok, Is.True);
        Assert.That(decodedHeader.Magic, Is.EqualTo(EncodedBatchHeader.MagicValue));
        Assert.That(decodedHeader.WireVersion, Is.EqualTo(EncodedBatchHeader.CurrentWireVersion));
        Assert.That(decodedHeader.EntryCount, Is.EqualTo(3));
        Assert.That(decodedHeader.BatchSequence, Is.EqualTo(99L));
        Assert.That(treeName, Is.EqualTo(TreeName));
        Assert.That(originClusterId, Is.EqualTo(OriginClusterId));
        Assert.That(entries.Length, Is.EqualTo(3));
        Assert.That(entries.Span[0].ToArray(), Is.EqualTo(new byte[] { 1, 2, 3 }));
        Assert.That(entries.Span[1].Count, Is.EqualTo(0));
        Assert.That(entries.Span[2].ToArray(), Is.EqualTo(new byte[] { 9, 8 }));
    }

    [Test]
    public void TryDecodeFraming_round_trips_empty_batch()
    {
        var payload = EncodeSample(_encoder, MakeHeader(0), TreeName, OriginClusterId);

        var ok = _encoder.TryDecodeFraming(
            payload, out var header, out var treeName, out var originClusterId, out var entries);

        Assert.That(ok, Is.True);
        Assert.That(header.EntryCount, Is.EqualTo(0));
        Assert.That(entries.Length, Is.EqualTo(0));
        Assert.That(treeName, Is.EqualTo(TreeName));
        Assert.That(originClusterId, Is.EqualTo(OriginClusterId));
    }

    [Test]
    public void TryDecodeFraming_returns_false_when_payload_shorter_than_header()
    {
        var ok = _encoder.TryDecodeFraming(
            new byte[EncodedBatchHeader.WireSize - 1],
            out _, out _, out _, out var entries);

        Assert.That(ok, Is.False);
        Assert.That(entries.Length, Is.EqualTo(0));
    }

    [Test]
    public void TryDecodeFraming_returns_false_on_wrong_magic()
    {
        var payload = new byte[EncodedBatchHeader.WireSize];
        BinaryPrimitives.WriteUInt32LittleEndian(payload, 0xDEADBEEFu);

        var ok = _encoder.TryDecodeFraming(payload, out _, out _, out _, out _);

        Assert.That(ok, Is.False);
    }

    [Test]
    public void TryDecodeFraming_throws_NotSupported_on_newer_wire_version()
    {
        var header = MakeHeader(0) with { WireVersion = EncodedBatchHeader.CurrentWireVersion + 1 };
        var buffer = new byte[EncodedBatchHeader.WireSize];
        header.WriteTo(buffer);

        Assert.That(
            () => _encoder.TryDecodeFraming(buffer, out _, out _, out _, out _),
            Throws.TypeOf<NotSupportedException>());
    }

    [Test]
    public void TryDecodeFraming_throws_on_negative_entry_count()
    {
        var header = MakeHeader(-1);
        var buffer = new byte[EncodedBatchHeader.WireSize];
        header.WriteTo(buffer);

        Assert.That(
            () => _encoder.TryDecodeFraming(buffer, out _, out _, out _, out _),
            Throws.ArgumentException);
    }

    [Test]
    public void TryDecodeFraming_throws_when_entry_count_exceeds_payload_capacity()
    {
        var header = MakeHeader(int.MaxValue);
        var buffer = new byte[EncodedBatchHeader.WireSize];
        header.WriteTo(buffer);

        Assert.That(
            () => _encoder.TryDecodeFraming(buffer, out _, out _, out _, out _),
            Throws.ArgumentException);
    }

    [Test]
    public void TryDecodeFraming_throws_when_treeName_prefix_truncated()
    {
        // Valid header, but the payload ends right after the fixed header so
        // the treeName length prefix cannot be read.
        var header = MakeHeader(0);
        var buffer = new byte[EncodedBatchHeader.WireSize + 2];
        header.WriteTo(buffer);

        Assert.That(
            () => _encoder.TryDecodeFraming(buffer, out _, out _, out _, out _),
            Throws.ArgumentException);
    }

    [Test]
    public void TryDecodeFraming_throws_when_treeName_body_overruns_payload()
    {
        var buffer = new byte[EncodedBatchHeader.WireSize + 4];
        MakeHeader(0).WriteTo(buffer);
        // Declare a 100-byte treeName that the payload cannot satisfy.
        BinaryPrimitives.WriteInt32LittleEndian(
            buffer.AsSpan(EncodedBatchHeader.WireSize, 4), 100);

        Assert.That(
            () => _encoder.TryDecodeFraming(buffer, out _, out _, out _, out _),
            Throws.ArgumentException);
    }

    [Test]
    public void TryDecodeFraming_throws_when_entry_length_prefix_truncated()
    {
        // Header claims one entry; supply treeName + originClusterId but drop
        // the entry length prefix entirely.
        var header = MakeHeader(1);
        var writer = new ArrayBufferWriter<byte>();
        var headerSpan = writer.GetSpan(EncodedBatchHeader.WireSize);
        header.WriteTo(headerSpan);
        writer.Advance(EncodedBatchHeader.WireSize);
        WriteLengthPrefixedAscii(writer, TreeName);
        WriteLengthPrefixedAscii(writer, OriginClusterId);
        var payload = writer.WrittenMemory.ToArray();

        Assert.That(
            () => _encoder.TryDecodeFraming(payload, out _, out _, out _, out _),
            Throws.ArgumentException);
    }

    [Test]
    public void TryDecodeFraming_throws_when_entry_body_overruns_payload()
    {
        var header = MakeHeader(1);
        var writer = new ArrayBufferWriter<byte>();
        var headerSpan = writer.GetSpan(EncodedBatchHeader.WireSize);
        header.WriteTo(headerSpan);
        writer.Advance(EncodedBatchHeader.WireSize);
        WriteLengthPrefixedAscii(writer, TreeName);
        WriteLengthPrefixedAscii(writer, OriginClusterId);
        // Entry length prefix promises 50 bytes but none follow.
        var lenSpan = writer.GetSpan(4);
        BinaryPrimitives.WriteInt32LittleEndian(lenSpan, 50);
        writer.Advance(4);
        var payload = writer.WrittenMemory.ToArray();

        Assert.That(
            () => _encoder.TryDecodeFraming(payload, out _, out _, out _, out _),
            Throws.ArgumentException);
    }

    private static void WriteLengthPrefixedAscii(IBufferWriter<byte> writer, string value)
    {
        var body = System.Text.Encoding.UTF8.GetBytes(value);
        var span = writer.GetSpan(4 + body.Length);
        BinaryPrimitives.WriteInt32LittleEndian(span, body.Length);
        body.CopyTo(span[4..]);
        writer.Advance(4 + body.Length);
    }

    /// <summary>
    /// Minimal <see cref="IReplicationBatchEncoder"/> that supplies only the
    /// abstract members and inherits the default framing implementations, so
    /// the interface defaults (not the canonical encoder's overrides) are the
    /// code under test.
    /// </summary>
    private sealed class DefaultsOnlyEncoder : IReplicationBatchEncoder
    {
        public string ContentType => "application/x-lattice-test";

        public int CurrentWireVersion => EncodedBatchHeader.CurrentWireVersion;

        public void Encode(ReplicationBatchEnvelope envelope, IBufferWriter<byte> writer)
            => throw new NotSupportedException("Not exercised by framing tests.");

        public ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload)
            => throw new NotSupportedException("Not exercised by framing tests.");
    }
}
