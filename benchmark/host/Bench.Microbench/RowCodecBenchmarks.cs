using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three aggregation-view row encoders on the projection write path
/// so their per-call byte and CPU deltas are measurable in the clear with no
/// Orleans cluster in the loop. Each lane pairs the prior implementation
/// (baseline) against the shipped one (optimized); every optimized lane produces
/// byte-for-byte identical output, so the deltas are pure overhead removed, not a
/// behaviour change.
/// <para>
/// (1) <c>AggregationRowCodec.EncodeMembership</c>, (2)
/// <c>AggregationRowCodec.EncodeInverse</c>, and (3)
/// <c>AggregationRowCodec.EncodeFoldInverse</c> each replace a per-encode
/// <see cref="MemoryStream"/> + <see cref="BinaryWriter"/> (which allocate a
/// growable backing buffer, a writer, and an encoder per call, then a final
/// <c>ToArray</c> copy) with a single sizing pass and a direct write into an
/// exact-size array, so each encode allocates only the returned <c>byte[]</c>.
/// Membership rows are written on every source mutation feeding a group-by view;
/// inverse rows on every min / max / set-union group-shard update; fold-inverse
/// rows on every folded (custom-reducer) group-shard update.
/// </para>
/// <para>
/// The codec internals are <see langword="internal"/>, so each row shape is
/// reproduced here (as <c>HashingAllocationBenchmarks</c> and
/// <c>CrdtMergeFoldBenchmarks</c> reproduce their internal call shapes) rather
/// than called directly; the reproduced logic is byte-identical to production
/// and is guarded against drift by the byte-identity tests in
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
}
