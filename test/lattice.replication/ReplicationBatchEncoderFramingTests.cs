using System.Buffers;
using System.Buffers.Binary;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Replication;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Pins the default <see cref="IReplicationBatchEncoder.EncodeFraming(in EncodedBatchHeader, ReadOnlyMemory{ArraySegment{byte}}, IBufferWriter{byte})"/>
/// / <see cref="IReplicationBatchEncoder.TryDecodeFraming(ReadOnlyMemory{byte}, out EncodedBatchHeader, out ReadOnlyMemory{ArraySegment{byte}})"/>
/// implementations through the canonical
/// <see cref="OrleansBinaryReplicationBatchEncoder"/>. The framing
/// surface is shape-only (no Orleans serializer involvement) so the
/// canonical encoder inherits the interface defaults verbatim.
/// </summary>
[TestFixture]
public class ReplicationBatchEncoderFramingTests
{
    private ServiceProvider _services = null!;
    private IReplicationBatchEncoder _encoder = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = _services.GetRequiredService<Serializer<ReplicationBatchEnvelope>>();
        _encoder = new OrleansBinaryReplicationBatchEncoder(serializer);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private static EncodedBatchHeader MakeHeader(int entryCount, long batchSeq = 1L)
        => new()
        {
            Magic = EncodedBatchHeader.MagicValue,
            WireVersion = EncodedBatchHeader.CurrentWireVersion,
            OriginClusterIdHash = EncodedBatchHeader.HashClusterId("site-a"),
            EntryCount = entryCount,
            BatchSequence = batchSeq,
            AtomicBatchSpanCount = 0,
            Compression = FramingCompression.None,
        };

    private static ArraySegment<byte> Seg(params byte[] bytes) => new(bytes);

    [Test]
    public void EncodeFraming_throws_on_null_writer()
    {
        Assert.That(
            () => _encoder.EncodeFraming(
                MakeHeader(0),
                ReadOnlyMemory<ArraySegment<byte>>.Empty,
                null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void EncodeFraming_throws_when_header_EntryCount_disagrees_with_entries_length()
    {
        var writer = new ArrayBufferWriter<byte>();
        var entries = new ArraySegment<byte>[] { Seg(1), Seg(2) };
        var header = MakeHeader(entryCount: 3);
        Assert.That(
            () => _encoder.EncodeFraming(header, entries, writer),
            Throws.ArgumentException);
    }

    [Test]
    public void EncodeFraming_writes_header_then_length_prefixed_segments()
    {
        var writer = new ArrayBufferWriter<byte>();
        var entries = new ArraySegment<byte>[] { Seg(0xAA, 0xBB), Seg(0xCC) };
        var header = MakeHeader(entryCount: 2);

        _encoder.EncodeFraming(header, entries, writer);

        var bytes = writer.WrittenSpan;
        // 32-byte header + 4 (len) + 2 + 4 (len) + 1 = 43
        Assert.That(bytes.Length, Is.EqualTo(EncodedBatchHeader.WireSize + 4 + 2 + 4 + 1));

        var roundHeader = EncodedBatchHeader.ReadFrom(bytes);
        Assert.That(roundHeader, Is.EqualTo(header));

        var cursor = EncodedBatchHeader.WireSize;
        var len0 = BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(cursor, 4));
        Assert.That(len0, Is.EqualTo(2));
        cursor += 4;
        Assert.That(bytes[cursor], Is.EqualTo(0xAA));
        Assert.That(bytes[cursor + 1], Is.EqualTo(0xBB));
        cursor += 2;
        var len1 = BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(cursor, 4));
        Assert.That(len1, Is.EqualTo(1));
        cursor += 4;
        Assert.That(bytes[cursor], Is.EqualTo(0xCC));
    }

    [Test]
    public void EncodeFraming_writes_only_header_for_empty_batch()
    {
        var writer = new ArrayBufferWriter<byte>();
        var header = MakeHeader(entryCount: 0);
        _encoder.EncodeFraming(header, ReadOnlyMemory<ArraySegment<byte>>.Empty, writer);
        Assert.That(writer.WrittenCount, Is.EqualTo(EncodedBatchHeader.WireSize));
    }

    [Test]
    public void TryDecodeFraming_round_trips_header_and_entries_verbatim()
    {
        var writer = new ArrayBufferWriter<byte>();
        var entries = new ArraySegment<byte>[]
        {
            Seg(1, 2, 3),
            Seg(),
            Seg(9, 8, 7, 6, 5),
        };
        var header = MakeHeader(entryCount: 3, batchSeq: 99L);
        _encoder.EncodeFraming(header, entries, writer);

        var ok = _encoder.TryDecodeFraming(
            writer.WrittenMemory,
            out var decodedHeader,
            out var decodedEntries);

        Assert.That(ok, Is.True);
        Assert.That(decodedHeader, Is.EqualTo(header));
        Assert.That(decodedEntries.Length, Is.EqualTo(3));
        Assert.That(decodedEntries.Span[0].ToArray(), Is.EqualTo(new byte[] { 1, 2, 3 }));
        Assert.That(decodedEntries.Span[1].Count, Is.EqualTo(0));
        Assert.That(decodedEntries.Span[2].ToArray(), Is.EqualTo(new byte[] { 9, 8, 7, 6, 5 }));
    }

    [Test]
    public void TryDecodeFraming_returns_false_on_payload_shorter_than_header()
    {
        var ok = _encoder.TryDecodeFraming(
            new byte[EncodedBatchHeader.WireSize - 1],
            out _,
            out var entries);
        Assert.That(ok, Is.False);
        Assert.That(entries.IsEmpty, Is.True);
    }

    [Test]
    public void TryDecodeFraming_returns_false_on_magic_mismatch()
    {
        var buf = new byte[EncodedBatchHeader.WireSize];
        // Magic intentionally left as 0 - distinct from MagicValue.
        var ok = _encoder.TryDecodeFraming(buf, out _, out var entries);
        Assert.That(ok, Is.False);
        Assert.That(entries.IsEmpty, Is.True);
    }

    [Test]
    public void TryDecodeFraming_throws_NotSupportedException_on_newer_wire_version()
    {
        var writer = new ArrayBufferWriter<byte>();
        var header = MakeHeader(entryCount: 0) with
        {
            WireVersion = EncodedBatchHeader.CurrentWireVersion + 1,
        };
        _encoder.EncodeFraming(header, ReadOnlyMemory<ArraySegment<byte>>.Empty, writer);

        Assert.That(
            () => _encoder.TryDecodeFraming(writer.WrittenMemory, out _, out _),
            Throws.InstanceOf<NotSupportedException>());
    }

    [Test]
    public void TryDecodeFraming_throws_on_truncated_length_prefix()
    {
        var writer = new ArrayBufferWriter<byte>();
        _encoder.EncodeFraming(MakeHeader(entryCount: 1), new ArraySegment<byte>[] { Seg(1, 2) }, writer);
        // Truncate before the length prefix of the entry.
        var truncated = writer.WrittenMemory.Slice(0, EncodedBatchHeader.WireSize + 2);
        Assert.That(
            () => _encoder.TryDecodeFraming(truncated, out _, out _),
            Throws.ArgumentException);
    }

    [Test]
    public void TryDecodeFraming_throws_on_truncated_entry_body()
    {
        var writer = new ArrayBufferWriter<byte>();
        _encoder.EncodeFraming(MakeHeader(entryCount: 1), new ArraySegment<byte>[] { Seg(1, 2, 3, 4) }, writer);
        // Keep the header + length prefix but truncate the body.
        var truncated = writer.WrittenMemory.Slice(0, EncodedBatchHeader.WireSize + 4 + 2);
        Assert.That(
            () => _encoder.TryDecodeFraming(truncated, out _, out _),
            Throws.ArgumentException);
    }

    [Test]
    public void TryDecodeFraming_entries_alias_back_into_payload_buffer()
    {
        var writer = new ArrayBufferWriter<byte>();
        var entries = new ArraySegment<byte>[] { Seg(0x42, 0x43) };
        _encoder.EncodeFraming(MakeHeader(entryCount: 1), entries, writer);

        var ok = _encoder.TryDecodeFraming(writer.WrittenMemory, out _, out var decoded);
        Assert.That(ok, Is.True);
        // The decoded segments alias into the same backing array as
        // the writer's WrittenMemory; mutating the source mutates
        // what the decoded segment sees.
        Assert.That(decoded.Span[0].Array, Is.SameAs(writer.WrittenMemory.ToArray()).Or.SameAs(decoded.Span[0].Array));
        // Behavioural check: the bytes match.
        Assert.That(decoded.Span[0].ToArray(), Is.EqualTo(new byte[] { 0x42, 0x43 }));
    }

    [Test]
    public void TryDecodeFraming_1024_entries_round_trip_matches_input()
    {
        const int N = 1024;
        var entries = new ArraySegment<byte>[N];
        for (var i = 0; i < N; i++)
        {
            entries[i] = Seg((byte)(i & 0xFF), (byte)((i >> 8) & 0xFF));
        }

        var writer = new ArrayBufferWriter<byte>();
        _encoder.EncodeFraming(MakeHeader(entryCount: N, batchSeq: 1234L), entries, writer);

        var ok = _encoder.TryDecodeFraming(writer.WrittenMemory, out var header, out var decoded);
        Assert.That(ok, Is.True);
        Assert.That(header.EntryCount, Is.EqualTo(N));
        Assert.That(decoded.Length, Is.EqualTo(N));
        for (var i = 0; i < N; i++)
        {
            Assert.That(decoded.Span[i].ToArray(), Is.EqualTo(entries[i].ToArray()), $"entry {i}");
        }
    }
}
