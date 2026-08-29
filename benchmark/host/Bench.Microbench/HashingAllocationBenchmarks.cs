using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.IO.Hashing;
using System.Text;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three view-maintenance hashing allocation trims so their
/// per-call (and, for the digest, per-entry) byte deltas are measurable in the
/// clear with no Orleans cluster in the loop. Each of the three production edits
/// removes a per-call <c>Encoding.UTF8.GetBytes(...)</c> heap <c>byte[]</c> on a
/// view write / maintenance path by hashing from a stack (or pooled, for long
/// inputs) UTF-8 buffer - the same idiom
/// <c>Orleans.Lattice.BPlusTree.LatticeSharding</c> and
/// <c>ShardMap.GetVirtualSlot</c> already use for core shard routing.
/// <para>
/// The three pairs mirror the production edits verbatim:
/// (1) <c>AggregationRowCodec.Slot</c> - the per-contribution accumulator-shard
/// routing hash (<see cref="XxHash32"/>);
/// (2) <c>AggregationApplier.OperationId</c> - the per-contribution atomic-flip
/// idempotency-id hash (<see cref="XxHash64"/>). Both lanes build the identical
/// payload string, so the sole per-lane difference is the encode <c>byte[]</c>
/// the baseline allocates;
/// (3) <c>ViewMaintainerGrain.ComputeTreeDigestAsync</c> - the per-entry key
/// encode inside the order-independent view-tree drift digest
/// (<see cref="XxHash128"/>), where the optimized lane reuses one pooled buffer
/// across the whole scan so N per-entry allocations collapse to a single rental.
/// </para>
/// <para>
/// The view internals are <see langword="internal"/>, so each shape is
/// reproduced here (as <c>HotPathAllocationBenchmarks</c> and
/// <c>CrdtMergeFoldBenchmarks</c> reproduce their internal call shapes) rather
/// than called directly; the reproduced hash is byte-identical to production, so
/// the <c>Allocated</c> delta is precisely the heap the change removes.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=hashalloc</c> (or
/// <c>--suite hashalloc</c>); see <c>Program.cs</c>. The suite has no Orleans
/// silo dependency, so it is fast to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c> for tight confidence intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class HashingAllocationBenchmarks
{
    // A representative source / group key routed on the view-write path.
    private const string SourceKey = "tenant-42/orders/2026-08-29/customer-000123";
    private const int Fanout = 16;

    // OperationId inputs.
    private const long OperationEpoch = 7;
    private const long WallClockTicks = 638_000_000_000_000_000L;
    private const int ClockCounter = 3;

    // The digest scan corpus: N materialised view-row keys.
    private List<string> _digestKeys = null!;
    private byte[] _digestValue = null!;

    /// <summary>Builds the per-size inputs shared by the digest lanes.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _digestValue = new byte[32];
        for (var i = 0; i < _digestValue.Length; i++) _digestValue[i] = (byte)i;

        _digestKeys = new List<string>(1024);
        for (var i = 0; i < 1024; i++)
        {
            _digestKeys.Add("group-" + i.ToString("D8", CultureInfo.InvariantCulture));
        }
    }

    // ------------------------------------------------------------------
    // (1) AggregationRowCodec.Slot - accumulator-shard routing hash
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: <c>XxHash32.HashToUInt32(Encoding.UTF8.GetBytes(sourceKey))</c> -
    /// a fresh encode <c>byte[]</c> per routing call.
    /// </summary>
    [Benchmark(Description = "Slot route hash: GetBytes byte[] (baseline)")]
    public int Slot_Baseline()
    {
        var hash = XxHash32.HashToUInt32(Encoding.UTF8.GetBytes(SourceKey));
        return (int)(hash % (uint)Fanout);
    }

    /// <summary>Optimized: hash from a stack UTF-8 buffer, allocating nothing.</summary>
    [Benchmark(Description = "Slot route hash: stack buffer (optimized)")]
    public int Slot_Optimized()
    {
        var maxByteCount = Encoding.UTF8.GetMaxByteCount(SourceKey.Length);
        byte[]? rented = null;
        Span<byte> buffer = maxByteCount <= 256
            ? stackalloc byte[maxByteCount]
            : (rented = ArrayPool<byte>.Shared.Rent(maxByteCount));
        try
        {
            var written = Encoding.UTF8.GetBytes(SourceKey, buffer);
            var hash = XxHash32.HashToUInt32(buffer[..written]);
            return (int)(hash % (uint)Fanout);
        }
        finally
        {
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        }
    }

    // ------------------------------------------------------------------
    // (2) AggregationApplier.OperationId - idempotency-id hash
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: build the payload string, then
    /// <c>XxHash64.HashToUInt64(Encoding.UTF8.GetBytes(payload))</c> - a fresh
    /// encode <c>byte[]</c> per contribution on top of the shared payload string.
    /// </summary>
    [Benchmark(Description = "OperationId hash: GetBytes byte[] (baseline)")]
    public string OperationId_Baseline()
    {
        var payload = $"{OperationEpoch}\u0000{SourceKey}\u0000{WallClockTicks}\u0000{ClockCounter}";
        var hash = XxHash64.HashToUInt64(Encoding.UTF8.GetBytes(payload));
        return "agg-" + hash.ToString("x16");
    }

    /// <summary>
    /// Optimized: the identical payload string, hashed from a stack UTF-8 buffer
    /// so the only removed allocation is the encode <c>byte[]</c>.
    /// </summary>
    [Benchmark(Description = "OperationId hash: stack buffer (optimized)")]
    public string OperationId_Optimized()
    {
        var payload = $"{OperationEpoch}\u0000{SourceKey}\u0000{WallClockTicks}\u0000{ClockCounter}";
        var maxByteCount = Encoding.UTF8.GetMaxByteCount(payload.Length);
        byte[]? rented = null;
        Span<byte> buffer = maxByteCount <= 256
            ? stackalloc byte[maxByteCount]
            : (rented = ArrayPool<byte>.Shared.Rent(maxByteCount));
        try
        {
            var written = Encoding.UTF8.GetBytes(payload, buffer);
            var hash = XxHash64.HashToUInt64(buffer[..written]);
            return "agg-" + hash.ToString("x16");
        }
        finally
        {
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        }
    }

    // ------------------------------------------------------------------
    // (3) ComputeTreeDigestAsync - per-entry key encode in the digest scan
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the prior digest fold - a fresh <c>Encoding.UTF8.GetBytes(key)</c>
    /// per scanned entry, so an N-row view tree pays N encode allocations per
    /// digest.
    /// </summary>
    [Benchmark(Description = "Digest scan encode: byte[] per entry (baseline)")]
    [Arguments(64)]
    [Arguments(1024)]
    public long Digest_Baseline(int entryCount)
    {
        var accumulator = new byte[16];
        var entryHash = new byte[16];
        var lengthPrefix = new byte[4];
        var hasher = new XxHash128();
        long count = 0;

        for (var i = 0; i < entryCount; i++)
        {
            var keyBytes = Encoding.UTF8.GetBytes(_digestKeys[i]);
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(lengthPrefix, keyBytes.Length);
            hasher.Append(lengthPrefix);
            hasher.Append(keyBytes);
            hasher.Append(_digestValue);
            hasher.GetHashAndReset(entryHash);
            for (var j = 0; j < 16; j++) accumulator[j] ^= entryHash[j];
            count++;
        }

        return count + accumulator[0];
    }

    /// <summary>
    /// Optimized: the shipped digest fold - one pooled UTF-8 buffer reused across
    /// every entry, so the whole scan pays a single rental rather than N encodes.
    /// The appended bytes are identical, so the folded digest is unchanged.
    /// </summary>
    [Benchmark(Description = "Digest scan encode: reused pooled buffer (optimized)")]
    [Arguments(64)]
    [Arguments(1024)]
    public long Digest_Optimized(int entryCount)
    {
        var accumulator = new byte[16];
        var entryHash = new byte[16];
        var lengthPrefix = new byte[4];
        var hasher = new XxHash128();
        long count = 0;

        var keyBuffer = ArrayPool<byte>.Shared.Rent(256);
        try
        {
            for (var i = 0; i < entryCount; i++)
            {
                var key = _digestKeys[i];
                var maxByteCount = Encoding.UTF8.GetMaxByteCount(key.Length);
                if (maxByteCount > keyBuffer.Length)
                {
                    ArrayPool<byte>.Shared.Return(keyBuffer);
                    keyBuffer = ArrayPool<byte>.Shared.Rent(maxByteCount);
                }

                var written = Encoding.UTF8.GetBytes(key, keyBuffer);
                System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(lengthPrefix, written);
                hasher.Append(lengthPrefix);
                hasher.Append(keyBuffer.AsSpan(0, written));
                hasher.Append(_digestValue);
                hasher.GetHashAndReset(entryHash);
                for (var j = 0; j < 16; j++) accumulator[j] ^= entryHash[j];
                count++;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(keyBuffer);
        }

        return count + accumulator[0];
    }
}
