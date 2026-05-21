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
            Compression = LatticeCompression.None,
        };

    private static ArraySegment<byte> Seg(params byte[] bytes) => new(bytes);

    private const string TreeName = "tree-1";
    private const string OriginClusterId = "site-a";

    [Test]
    public void EncodeFraming_throws_on_null_writer()
    {
        Assert.That(
            () => _encoder.EncodeFraming(
                MakeHeader(0),
                TreeName,
                OriginClusterId,
                ReadOnlyMemory<ArraySegment<byte>>.Empty,
                null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void EncodeFraming_throws_on_null_treeName()
    {
        var writer = new ArrayBufferWriter<byte>();
        Assert.That(
            () => _encoder.EncodeFraming(
                MakeHeader(0),
                null!,
                OriginClusterId,
                ReadOnlyMemory<ArraySegment<byte>>.Empty,
                writer),
            Throws.ArgumentNullException);
    }

    [Test]
    public void EncodeFraming_throws_on_null_originClusterId()
    {
        var writer = new ArrayBufferWriter<byte>();
        Assert.That(
            () => _encoder.EncodeFraming(
                MakeHeader(0),
                TreeName,
                null!,
                ReadOnlyMemory<ArraySegment<byte>>.Empty,
                writer),
            Throws.ArgumentNullException);
    }

    [Test]
    public void EncodeFraming_throws_on_empty_treeName()
    {
        var writer = new ArrayBufferWriter<byte>();
        Assert.That(
            () => _encoder.EncodeFraming(
                MakeHeader(0),
                string.Empty,
                OriginClusterId,
                ReadOnlyMemory<ArraySegment<byte>>.Empty,
                writer),
            Throws.ArgumentException);
    }

    [Test]
    public void EncodeFraming_throws_on_empty_originClusterId()
    {
        var writer = new ArrayBufferWriter<byte>();
        Assert.That(
            () => _encoder.EncodeFraming(
                MakeHeader(0),
                TreeName,
                string.Empty,
                ReadOnlyMemory<ArraySegment<byte>>.Empty,
                writer),
            Throws.ArgumentException);
    }

    [Test]
    public void EncodeFraming_throws_when_header_EntryCount_disagrees_with_entries_length()
    {
        var writer = new ArrayBufferWriter<byte>();
        var entries = new ArraySegment<byte>[] { Seg(1), Seg(2) };
        var header = MakeHeader(entryCount: 3);
        Assert.That(
            () => _encoder.EncodeFraming(header, TreeName, OriginClusterId, entries, writer),
            Throws.ArgumentException);
    }

    [Test]
    public void EncodeFraming_writes_header_then_routing_strings_then_length_prefixed_segments()
    {
        var writer = new ArrayBufferWriter<byte>();
        var entries = new ArraySegment<byte>[] { Seg(0xAA, 0xBB), Seg(0xCC) };
        var header = MakeHeader(entryCount: 2);

        _encoder.EncodeFraming(header, TreeName, OriginClusterId, entries, writer);

        var bytes = writer.WrittenSpan;
        var treeBytes = System.Text.Encoding.UTF8.GetByteCount(TreeName);
        var originBytes = System.Text.Encoding.UTF8.GetByteCount(OriginClusterId);
        // 32-byte header + 4 + treeBytes + 4 + originBytes + 4 (len) + 2 + 4 (len) + 1
        Assert.That(
            bytes.Length,
            Is.EqualTo(EncodedBatchHeader.WireSize + 4 + treeBytes + 4 + originBytes + 4 + 2 + 4 + 1));

        var roundHeader = EncodedBatchHeader.ReadFrom(bytes);
        Assert.That(roundHeader, Is.EqualTo(header));

        var cursor = EncodedBatchHeader.WireSize;
        var treeLen = BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(cursor, 4));
        Assert.That(treeLen, Is.EqualTo(treeBytes));
        cursor += 4;
        Assert.That(System.Text.Encoding.UTF8.GetString(bytes.Slice(cursor, treeLen)), Is.EqualTo(TreeName));
        cursor += treeLen;
        var originLen = BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(cursor, 4));
        Assert.That(originLen, Is.EqualTo(originBytes));
        cursor += 4;
        Assert.That(System.Text.Encoding.UTF8.GetString(bytes.Slice(cursor, originLen)), Is.EqualTo(OriginClusterId));
        cursor += originLen;

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
    public void EncodeFraming_writes_header_and_routing_strings_for_empty_batch()
    {
        var writer = new ArrayBufferWriter<byte>();
        var header = MakeHeader(entryCount: 0);
        _encoder.EncodeFraming(header, TreeName, OriginClusterId, ReadOnlyMemory<ArraySegment<byte>>.Empty, writer);
        var treeBytes = System.Text.Encoding.UTF8.GetByteCount(TreeName);
        var originBytes = System.Text.Encoding.UTF8.GetByteCount(OriginClusterId);
        Assert.That(
            writer.WrittenCount,
            Is.EqualTo(EncodedBatchHeader.WireSize + 4 + treeBytes + 4 + originBytes));
    }

    [Test]
    public void TryDecodeFraming_round_trips_header_routing_strings_and_entries_verbatim()
    {
        var writer = new ArrayBufferWriter<byte>();
        var entries = new ArraySegment<byte>[]
        {
            Seg(1, 2, 3),
            Seg(),
            Seg(9, 8, 7, 6, 5),
        };
        var header = MakeHeader(entryCount: 3, batchSeq: 99L);
        _encoder.EncodeFraming(header, TreeName, OriginClusterId, entries, writer);

        var ok = _encoder.TryDecodeFraming(
            writer.WrittenMemory,
            out var decodedHeader,
            out var decodedTree,
            out var decodedOrigin,
            out var decodedEntries);

        Assert.That(ok, Is.True);
        Assert.That(decodedHeader, Is.EqualTo(header));
        Assert.That(decodedTree, Is.EqualTo(TreeName));
        Assert.That(decodedOrigin, Is.EqualTo(OriginClusterId));
        Assert.That(decodedEntries.Length, Is.EqualTo(3));
        Assert.That(decodedEntries.Span[0].ToArray(), Is.EqualTo(new byte[] { 1, 2, 3 }));
        Assert.That(decodedEntries.Span[1].Count, Is.EqualTo(0));
        Assert.That(decodedEntries.Span[2].ToArray(), Is.EqualTo(new byte[] { 9, 8, 7, 6, 5 }));
    }

    [Test]
    public void TryDecodeFraming_round_trips_non_ascii_routing_strings()
    {
        var writer = new ArrayBufferWriter<byte>();
        var tree = "tr\u00e9e-\u4e2d";
        var origin = "site-\u00fc\u00f1";
        var header = MakeHeader(entryCount: 0);
        _encoder.EncodeFraming(header, tree, origin, ReadOnlyMemory<ArraySegment<byte>>.Empty, writer);

        var ok = _encoder.TryDecodeFraming(
            writer.WrittenMemory,
            out _,
            out var decodedTree,
            out var decodedOrigin,
            out _);

        Assert.That(ok, Is.True);
        Assert.That(decodedTree, Is.EqualTo(tree));
        Assert.That(decodedOrigin, Is.EqualTo(origin));
    }

    [Test]
    public void TryDecodeFraming_returns_false_on_payload_shorter_than_header()
    {
        var ok = _encoder.TryDecodeFraming(
            new byte[EncodedBatchHeader.WireSize - 1],
            out _,
            out _,
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
        var ok = _encoder.TryDecodeFraming(buf, out _, out _, out _, out var entries);
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
        _encoder.EncodeFraming(header, TreeName, OriginClusterId, ReadOnlyMemory<ArraySegment<byte>>.Empty, writer);

        Assert.That(
            () => _encoder.TryDecodeFraming(writer.WrittenMemory, out _, out _, out _, out _),
            Throws.InstanceOf<NotSupportedException>());
    }

    [Test]
    public void TryDecodeFraming_throws_on_truncated_treeName_length_prefix()
    {
        var writer = new ArrayBufferWriter<byte>();
        _encoder.EncodeFraming(MakeHeader(entryCount: 0), TreeName, OriginClusterId, ReadOnlyMemory<ArraySegment<byte>>.Empty, writer);
        // Keep only the fixed header.
        var truncated = writer.WrittenMemory.Slice(0, EncodedBatchHeader.WireSize + 2);
        Assert.That(
            () => _encoder.TryDecodeFraming(truncated, out _, out _, out _, out _),
            Throws.ArgumentException);
    }

    [Test]
    public void TryDecodeFraming_throws_on_truncated_treeName_body()
    {
        var writer = new ArrayBufferWriter<byte>();
        _encoder.EncodeFraming(MakeHeader(entryCount: 0), TreeName, OriginClusterId, ReadOnlyMemory<ArraySegment<byte>>.Empty, writer);
        // Keep header + treeName length prefix but cut into the body.
        var truncated = writer.WrittenMemory.Slice(0, EncodedBatchHeader.WireSize + 4 + 1);
        Assert.That(
            () => _encoder.TryDecodeFraming(truncated, out _, out _, out _, out _),
            Throws.ArgumentException);
    }

    [Test]
    public void TryDecodeFraming_throws_on_truncated_length_prefix()
    {
        var writer = new ArrayBufferWriter<byte>();
        _encoder.EncodeFraming(MakeHeader(entryCount: 1), TreeName, OriginClusterId, new ArraySegment<byte>[] { Seg(1, 2) }, writer);
        // Truncate at 2 bytes into the entry length prefix (after the routing strings).
        var prefixLength = EncodedBatchHeader.WireSize
            + 4 + System.Text.Encoding.UTF8.GetByteCount(TreeName)
            + 4 + System.Text.Encoding.UTF8.GetByteCount(OriginClusterId);
        var truncated = writer.WrittenMemory.Slice(0, prefixLength + 2);
        Assert.That(
            () => _encoder.TryDecodeFraming(truncated, out _, out _, out _, out _),
            Throws.ArgumentException);
    }

    [Test]
    public void TryDecodeFraming_throws_on_truncated_entry_body()
    {
        var writer = new ArrayBufferWriter<byte>();
        _encoder.EncodeFraming(MakeHeader(entryCount: 1), TreeName, OriginClusterId, new ArraySegment<byte>[] { Seg(1, 2, 3, 4) }, writer);
        // Keep the header + routing strings + length prefix but truncate the body.
        var prefixLength = EncodedBatchHeader.WireSize
            + 4 + System.Text.Encoding.UTF8.GetByteCount(TreeName)
            + 4 + System.Text.Encoding.UTF8.GetByteCount(OriginClusterId);
        var truncated = writer.WrittenMemory.Slice(0, prefixLength + 4 + 2);
        Assert.That(
            () => _encoder.TryDecodeFraming(truncated, out _, out _, out _, out _),
            Throws.ArgumentException);
    }

    [Test]
    public void TryDecodeFraming_entries_alias_back_into_payload_buffer()
    {
        var writer = new ArrayBufferWriter<byte>();
        var entries = new ArraySegment<byte>[] { Seg(0x42, 0x43) };
        _encoder.EncodeFraming(MakeHeader(entryCount: 1), TreeName, OriginClusterId, entries, writer);

        var ok = _encoder.TryDecodeFraming(writer.WrittenMemory, out _, out _, out _, out var decoded);
        Assert.That(ok, Is.True);
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
        _encoder.EncodeFraming(MakeHeader(entryCount: N, batchSeq: 1234L), TreeName, OriginClusterId, entries, writer);

        var ok = _encoder.TryDecodeFraming(writer.WrittenMemory, out var header, out var tree, out var origin, out var decoded);
        Assert.That(ok, Is.True);
        Assert.That(header.EntryCount, Is.EqualTo(N));
        Assert.That(tree, Is.EqualTo(TreeName));
        Assert.That(origin, Is.EqualTo(OriginClusterId));
        Assert.That(decoded.Length, Is.EqualTo(N));
        for (var i = 0; i < N; i++)
        {
            Assert.That(decoded.Span[i].ToArray(), Is.EqualTo(entries[i].ToArray()), $"entry {i}");
        }
    }
}
