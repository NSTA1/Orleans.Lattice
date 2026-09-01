using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the aggregation-view row codec's encode and decode paths on the
/// projection write / read path so their per-call byte and CPU deltas are
/// measurable in the clear with no Orleans cluster in the loop. Each lane pairs
/// the prior implementation (baseline) against the shipped one (optimized);
/// every optimized lane produces byte-for-byte identical output (encode) or the
/// identical decoded value (decode), so the deltas are pure overhead removed,
/// not a behaviour change.
/// <para>
/// Encode lanes (1) <c>AggregationRowCodec.EncodeMembership</c>, (2)
/// <c>AggregationRowCodec.EncodeInverse</c>, and (3)
/// <c>AggregationRowCodec.EncodeFoldInverse</c> each replace a per-encode
/// <see cref="MemoryStream"/> + <see cref="BinaryWriter"/> (which allocate a
/// growable backing buffer, a writer, and an encoder per call, then a final
/// <c>ToArray</c> copy) with a single sizing pass and a direct write into an
/// exact-size array, so each encode allocates only the returned <c>byte[]</c>.
/// </para>
/// <para>
/// Decode lanes (4) <c>AggregationRowCodec.DecodeMembership</c>, (5)
/// <c>AggregationRowCodec.DecodeInverse</c>, and (6)
/// <c>AggregationRowCodec.DecodeFoldInverse</c> are the symmetric inverse: each
/// replaces a per-decode <see cref="MemoryStream"/> + <see cref="BinaryReader"/>
/// (which allocate a stream, a reader, and an internal decode buffer per call)
/// with a forward <c>RowReader</c> walk straight over the row span, so the only
/// heap a decode retains is the value(s) it must return. Membership rows are
/// read on every source retraction; inverse rows on every min / max / set-union
/// group-shard update; fold-inverse rows on every folded (custom-reducer)
/// group-shard update.
/// </para>
/// <para>
/// The codec internals are <see langword="internal"/>, so each row shape is
/// reproduced here (as <c>HashingAllocationBenchmarks</c> and
/// <c>CrdtMergeFoldBenchmarks</c> reproduce their internal call shapes) rather
/// than called directly; the reproduced logic is byte-identical to production
/// and is guarded against drift by the byte-identity and round-trip tests in
/// <c>AggregationRowCodecTests</c>. Run it via
/// <c>BENCH_MICROBENCH_SUITE=rowcodec</c> (or <c>--suite rowcodec</c>); see
/// <c>Program.cs</c>. The suite has no Orleans silo dependency, so it is fast to
/// run at <c>BENCH_MICROBENCH_FIDELITY=full</c> for tight confidence intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class RowCodecBenchmarks
{
    private const string GroupKey = "tenant-42/orders/2026-08-29/region-emea";
    private const string Member = "customer-000123";
    private const double Numeric = 12345.6789;

    private Dictionary<string, (double Numeric, string? Member)> _inverse = null!;
    private Dictionary<string, (byte[] Value, long WallClockTicks, int Counter)> _foldInverse = null!;

    // Pre-encoded row payloads for the decode lanes, built once in Setup with a
    // reference BinaryWriter so the bytes are exactly what production persists.
    private byte[] _membershipEncoded = null!;
    private byte[] _inverseEncoded = null!;
    private byte[] _foldInverseEncoded = null!;

    /// <summary>Builds the per-run inputs shared by the encode lanes.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _inverse = new Dictionary<string, (double, string?)>(StringComparer.Ordinal);
        for (var i = 0; i < 32; i++)
        {
            _inverse["source-" + i.ToString("D6", CultureInfo.InvariantCulture)] =
                (i * 1.5, (i & 1) == 0 ? "member-" + i.ToString("D4", CultureInfo.InvariantCulture) : null);
        }

        _foldInverse = new Dictionary<string, (byte[], long, int)>(StringComparer.Ordinal);
        for (var i = 0; i < 32; i++)
        {
            var value = new byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(value, 1_000_000L + i);
            _foldInverse["source-" + i.ToString("D6", CultureInfo.InvariantCulture)] =
                (value, 638_000_000_000_000_000L + i, i);
        }

        // Encode each row family once (reference BinaryWriter layout) to feed the
        // decode lanes below.
        using var membershipStream = new MemoryStream();
        using (var w = new BinaryWriter(membershipStream, Encoding.UTF8, leaveOpen: true))
        {
            w.Write(GroupKey);
            w.Write(true);
            w.Write(Numeric);
            w.Write(Member);
        }

        _membershipEncoded = membershipStream.ToArray();

        using var inverseStream = new MemoryStream();
        using (var w = new BinaryWriter(inverseStream, Encoding.UTF8, leaveOpen: true))
        {
            w.Write(_inverse.Count);
            foreach (var (sourceKey, entry) in _inverse)
            {
                w.Write(sourceKey);
                w.Write(entry.Member is not null);
                w.Write(entry.Numeric);
                if (entry.Member is not null) w.Write(entry.Member);
            }
        }

        _inverseEncoded = inverseStream.ToArray();

        using var foldStream = new MemoryStream();
        using (var w = new BinaryWriter(foldStream, Encoding.UTF8, leaveOpen: true))
        {
            w.Write(_foldInverse.Count);
            foreach (var (sourceKey, entry) in _foldInverse)
            {
                w.Write(sourceKey);
                w.Write(entry.WallClockTicks);
                w.Write(entry.Counter);
                w.Write(entry.Value.Length);
                w.Write(entry.Value);
            }
        }

        _foldInverseEncoded = foldStream.ToArray();
    }

    // ------------------------------------------------------------------
    // (1) AggregationRowCodec.EncodeMembership
    // ------------------------------------------------------------------

    /// <summary>Baseline: MemoryStream + BinaryWriter per encode, then ToArray.</summary>
    [Benchmark(Description = "EncodeMembership: MemoryStream+BinaryWriter (baseline)")]
    public int Membership_Baseline()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, Encoding.UTF8);
        writer.Write(GroupKey);
        writer.Write(true);
        writer.Write(Numeric);
        writer.Write(Member);
        writer.Flush();
        return stream.ToArray().Length;
    }

    /// <summary>Optimized: a single sizing pass then a direct write into an exact-size array.</summary>
    [Benchmark(Description = "EncodeMembership: exact-size array (optimized)")]
    public int Membership_Optimized()
    {
        var size = Utf8Size(GroupKey) + sizeof(bool) + sizeof(double) + Utf8Size(Member);
        var buffer = new byte[size];
        var pos = 0;
        WriteString(buffer, ref pos, GroupKey);
        buffer[pos++] = 1;
        BinaryPrimitives.WriteDoubleLittleEndian(buffer.AsSpan(pos), Numeric);
        pos += sizeof(double);
        WriteString(buffer, ref pos, Member);
        return buffer.Length;
    }

    // ------------------------------------------------------------------
    // (2) AggregationRowCodec.EncodeInverse (multi-entry row)
    // ------------------------------------------------------------------

    /// <summary>Baseline: MemoryStream + BinaryWriter across every entry, then ToArray.</summary>
    [Benchmark(Description = "EncodeInverse x32: MemoryStream+BinaryWriter (baseline)")]
    public int Inverse_Baseline()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, Encoding.UTF8);
        writer.Write(_inverse.Count);
        foreach (var (sourceKey, entry) in _inverse)
        {
            writer.Write(sourceKey);
            writer.Write(entry.Member is not null);
            writer.Write(entry.Numeric);
            if (entry.Member is not null) writer.Write(entry.Member);
        }

        writer.Flush();
        return stream.ToArray().Length;
    }

    /// <summary>Optimized: one sizing pass then a direct write into an exact-size array.</summary>
    [Benchmark(Description = "EncodeInverse x32: exact-size array (optimized)")]
    public int Inverse_Optimized()
    {
        var size = sizeof(int);
        foreach (var (sourceKey, entry) in _inverse)
        {
            size += Utf8Size(sourceKey) + sizeof(bool) + sizeof(double)
                + (entry.Member is not null ? Utf8Size(entry.Member) : 0);
        }

        var buffer = new byte[size];
        var pos = 0;
        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(pos), _inverse.Count);
        pos += sizeof(int);
        foreach (var (sourceKey, entry) in _inverse)
        {
            WriteString(buffer, ref pos, sourceKey);
            var hasMember = entry.Member is not null;
            buffer[pos++] = hasMember ? (byte)1 : (byte)0;
            BinaryPrimitives.WriteDoubleLittleEndian(buffer.AsSpan(pos), entry.Numeric);
            pos += sizeof(double);
            if (hasMember) WriteString(buffer, ref pos, entry.Member!);
        }

        return buffer.Length;
    }

    // ------------------------------------------------------------------
    // (3) AggregationRowCodec.EncodeFoldInverse (HLC-stamped byte[] payloads)
    // ------------------------------------------------------------------

    /// <summary>Baseline: MemoryStream + BinaryWriter across every entry, then ToArray.</summary>
    [Benchmark(Description = "EncodeFoldInverse x32: MemoryStream+BinaryWriter (baseline)")]
    public int FoldInverse_Baseline()
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, Encoding.UTF8);
        writer.Write(_foldInverse.Count);
        foreach (var (sourceKey, entry) in _foldInverse)
        {
            writer.Write(sourceKey);
            writer.Write(entry.WallClockTicks);
            writer.Write(entry.Counter);
            writer.Write(entry.Value.Length);
            writer.Write(entry.Value);
        }

        writer.Flush();
        return stream.ToArray().Length;
    }

    /// <summary>Optimized: one sizing pass then a direct write into an exact-size array.</summary>
    [Benchmark(Description = "EncodeFoldInverse x32: exact-size array (optimized)")]
    public int FoldInverse_Optimized()
    {
        var size = sizeof(int);
        foreach (var (sourceKey, entry) in _foldInverse)
        {
            size += Utf8Size(sourceKey) + sizeof(long) + sizeof(int) + sizeof(int) + entry.Value.Length;
        }

        var buffer = new byte[size];
        var pos = 0;
        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(pos), _foldInverse.Count);
        pos += sizeof(int);
        foreach (var (sourceKey, entry) in _foldInverse)
        {
            WriteString(buffer, ref pos, sourceKey);
            BinaryPrimitives.WriteInt64LittleEndian(buffer.AsSpan(pos), entry.WallClockTicks);
            pos += sizeof(long);
            BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(pos), entry.Counter);
            pos += sizeof(int);
            BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(pos), entry.Value.Length);
            pos += sizeof(int);
            entry.Value.CopyTo(buffer.AsSpan(pos));
            pos += entry.Value.Length;
        }

        return buffer.Length;
    }

    // ------------------------------------------------------------------
    // (4) AggregationRowCodec.DecodeMembership
    // ------------------------------------------------------------------

    /// <summary>Baseline: MemoryStream + BinaryReader per decode (stream, reader, decode buffer).</summary>
    [Benchmark(Description = "DecodeMembership: MemoryStream+BinaryReader (baseline)")]
    public double DecodeMembership_Baseline()
    {
        using var stream = new MemoryStream(_membershipEncoded);
        using var reader = new BinaryReader(stream, Encoding.UTF8);
        var groupKey = reader.ReadString();
        var hasMember = reader.ReadBoolean();
        var numeric = reader.ReadDouble();
        string? member = hasMember ? reader.ReadString() : null;
        return groupKey.Length + numeric + (member?.Length ?? 0);
    }

    /// <summary>Optimized: a forward span read with no stream, reader, or decode buffer.</summary>
    [Benchmark(Description = "DecodeMembership: span RowReader (optimized)")]
    public double DecodeMembership_Optimized()
    {
        var span = (ReadOnlySpan<byte>)_membershipEncoded;
        var pos = 0;
        var groupKey = ReadString(span, ref pos);
        var hasMember = span[pos++] != 0;
        var numeric = BinaryPrimitives.ReadDoubleLittleEndian(span[pos..]);
        pos += sizeof(double);
        string? member = hasMember ? ReadString(span, ref pos) : null;
        return groupKey.Length + numeric + (member?.Length ?? 0);
    }

    // ------------------------------------------------------------------
    // (5) AggregationRowCodec.DecodeInverse (multi-entry row)
    // ------------------------------------------------------------------

    /// <summary>Baseline: MemoryStream + BinaryReader across every entry.</summary>
    [Benchmark(Description = "DecodeInverse x32: MemoryStream+BinaryReader (baseline)")]
    public int DecodeInverse_Baseline()
    {
        using var stream = new MemoryStream(_inverseEncoded);
        using var reader = new BinaryReader(stream, Encoding.UTF8);
        var count = reader.ReadInt32();
        var map = new Dictionary<string, (double, string?)>(count, StringComparer.Ordinal);
        for (var i = 0; i < count; i++)
        {
            var sourceKey = reader.ReadString();
            var hasMember = reader.ReadBoolean();
            var numeric = reader.ReadDouble();
            string? member = hasMember ? reader.ReadString() : null;
            map[sourceKey] = (numeric, member);
        }

        return map.Count;
    }

    /// <summary>Optimized: a forward span read into the same map, no stream or reader.</summary>
    [Benchmark(Description = "DecodeInverse x32: span RowReader (optimized)")]
    public int DecodeInverse_Optimized()
    {
        var span = (ReadOnlySpan<byte>)_inverseEncoded;
        var pos = 0;
        var count = BinaryPrimitives.ReadInt32LittleEndian(span[pos..]);
        pos += sizeof(int);
        var map = new Dictionary<string, (double, string?)>(count, StringComparer.Ordinal);
        for (var i = 0; i < count; i++)
        {
            var sourceKey = ReadString(span, ref pos);
            var hasMember = span[pos++] != 0;
            var numeric = BinaryPrimitives.ReadDoubleLittleEndian(span[pos..]);
            pos += sizeof(double);
            string? member = hasMember ? ReadString(span, ref pos) : null;
            map[sourceKey] = (numeric, member);
        }

        return map.Count;
    }

    // ------------------------------------------------------------------
    // (6) AggregationRowCodec.DecodeFoldInverse (HLC-stamped byte[] payloads)
    // ------------------------------------------------------------------

    /// <summary>Baseline: MemoryStream + BinaryReader across every entry.</summary>
    [Benchmark(Description = "DecodeFoldInverse x32: MemoryStream+BinaryReader (baseline)")]
    public int DecodeFoldInverse_Baseline()
    {
        using var stream = new MemoryStream(_foldInverseEncoded);
        using var reader = new BinaryReader(stream, Encoding.UTF8);
        var count = reader.ReadInt32();
        var map = new Dictionary<string, (byte[], long, int)>(count, StringComparer.Ordinal);
        for (var i = 0; i < count; i++)
        {
            var sourceKey = reader.ReadString();
            var ticks = reader.ReadInt64();
            var counter = reader.ReadInt32();
            var length = reader.ReadInt32();
            var value = reader.ReadBytes(length);
            map[sourceKey] = (value, ticks, counter);
        }

        return map.Count;
    }

    /// <summary>Optimized: a forward span read with an exact-length slice copy for the payload.</summary>
    [Benchmark(Description = "DecodeFoldInverse x32: span RowReader (optimized)")]
    public int DecodeFoldInverse_Optimized()
    {
        var span = (ReadOnlySpan<byte>)_foldInverseEncoded;
        var pos = 0;
        var count = BinaryPrimitives.ReadInt32LittleEndian(span[pos..]);
        pos += sizeof(int);
        var map = new Dictionary<string, (byte[], long, int)>(count, StringComparer.Ordinal);
        for (var i = 0; i < count; i++)
        {
            var sourceKey = ReadString(span, ref pos);
            var ticks = BinaryPrimitives.ReadInt64LittleEndian(span[pos..]);
            pos += sizeof(long);
            var counter = BinaryPrimitives.ReadInt32LittleEndian(span[pos..]);
            pos += sizeof(int);
            var length = BinaryPrimitives.ReadInt32LittleEndian(span[pos..]);
            pos += sizeof(int);
            var value = span.Slice(pos, length).ToArray();
            pos += length;
            map[sourceKey] = (value, ticks, counter);
        }

        return map.Count;
    }

    // ------------------------------------------------------------------
    // Shared helpers - reproduce the production RowWriter primitives.
    // ------------------------------------------------------------------

    private static int Utf8Size(string value)
    {
        var byteCount = Encoding.UTF8.GetByteCount(value);
        return SevenBitSize(byteCount) + byteCount;
    }

    private static int SevenBitSize(int value)
    {
        var v = (uint)value;
        var size = 1;
        while (v >= 0x80)
        {
            size++;
            v >>= 7;
        }

        return size;
    }

    private static void WriteString(byte[] buffer, ref int pos, string value)
    {
        var byteCount = Encoding.UTF8.GetByteCount(value);
        var v = (uint)byteCount;
        while (v >= 0x80)
        {
            buffer[pos++] = (byte)(v | 0x80);
            v >>= 7;
        }

        buffer[pos++] = (byte)v;
        Encoding.UTF8.GetBytes(value, buffer.AsSpan(pos));
        pos += byteCount;
    }

    // Reproduces the production RowReader.ReadString primitive: a 7-bit-encoded
    // byte-count prefix followed by that many UTF-8 bytes, decoded from the span.
    private static string ReadString(ReadOnlySpan<byte> buffer, ref int pos)
    {
        var byteCount = 0;
        var shift = 0;
        while (true)
        {
            var b = buffer[pos++];
            byteCount |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
                break;
            }

            shift += 7;
        }

        var value = Encoding.UTF8.GetString(buffer.Slice(pos, byteCount));
        pos += byteCount;
        return value;
    }
}
