using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.IO.Hashing;
using System.Linq;
using System.Text;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three trims made to the aggregation view's per-contribution fold
/// path in <c>Orleans.Lattice.Views.AggregationApplier</c>. Every lane calls the
/// real production codec through <c>InternalsVisibleTo</c>, so the optimised
/// arms are the shipped code rather than a model of it, and each baseline arm is
/// a <b>verbatim</b> copy of the body it replaced - it therefore pays exactly the
/// same surrounding cost (the same rows, the same shard loop, the same reduction)
/// and the delta is the trim and nothing else.
/// <para>
/// (1) <b>Inverse materialise.</b> <c>MaterialiseInverseAsync</c> runs after every
/// contribute and retract. Per shard it called <c>DecodeInverse</c>, which builds
/// a <c>Dictionary&lt;string, MemberEntry&gt;</c> presized to N and decodes a
/// fresh string for every source key - then read only the numeric (min/max) or
/// the member (set-union) and dropped the rest. Source keys are never looked up
/// on that path. <c>InverseRowScan</c> walks the identical bytes and materialises
/// nothing. Two lanes, because min/max and set-union take different walks.
/// </para>
/// <para>
/// (2) <b>Fold materialise.</b> <c>MaterialiseFoldAsync</c> decoded each shard
/// into a dictionary purely to flatten it into a list that was never presized and
/// therefore doubled. <c>FoldInverseRowScan</c> plus one <c>EnsureCapacity</c>
/// from the row's own header count removes both.
/// </para>
/// <para>
/// (3) <b>Operation id.</b> Composes the hash input straight into the UTF-8
/// buffer instead of interpolating a payload string to transcode and discard, and
/// formats the id in one <c>string.Create</c> rather than three heap strings. The
/// stack/pool sizing is deliberately left exactly as it was, so this lane changes
/// one thing.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=aggfold</c> (or <c>--suite aggfold</c>).
/// No Orleans silo is involved, so it is cheap at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class AggregationMaterialiseBenchmarks
{
    private const int ShardCount = 8;
    private const int MembersPerShard = 16;

    private byte[][] _inverseNumericShards = null!;
    private byte[][] _inverseMemberShards = null!;
    private byte[][] _foldShards = null!;

    private string _operationEpoch = null!;
    private string _sourceKey = null!;
    private string _longSourceKey = null!;
    private HybridLogicalClock _timestamp;

    /// <summary>
    /// Builds the encoded rows each lane walks, then asserts every baseline and
    /// its optimised partner produce the same answer. A pair that disagrees is
    /// timing two different computations, so the whole run is worthless; fail the
    /// setup rather than publish it.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _operationEpoch = "epoch-0000000000000001";
        _sourceKey = "tenant-a/orders/2024/000123";
        _longSourceKey = new string('k', 400);
        _timestamp = new HybridLogicalClock { WallClockTicks = 638_000_000_000_000_000, Counter = 7 };

        _inverseNumericShards = new byte[ShardCount][];
        _inverseMemberShards = new byte[ShardCount][];
        _foldShards = new byte[ShardCount][];

        for (var s = 0; s < ShardCount; s++)
        {
            var numeric = new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal);
            var member = new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal);
            var fold = new Dictionary<string, AggregationRowCodec.FoldMember>(StringComparer.Ordinal);

            for (var m = 0; m < MembersPerShard; m++)
            {
                var key = string.Create(CultureInfo.InvariantCulture, $"tenant-a/orders/2024/shard-{s}/src-{m:D6}");
                numeric[key] = new AggregationRowCodec.MemberEntry((s * MembersPerShard) + m, null);
                member[key] = new AggregationRowCodec.MemberEntry(0.0, string.Create(CultureInfo.InvariantCulture, $"member-{s}-{m}"));
                fold[key] = new AggregationRowCodec.FoldMember(
                    [(byte)s, (byte)m, 0x7F],
                    new HybridLogicalClock { WallClockTicks = 1_000 + m, Counter = s });
            }

            _inverseNumericShards[s] = AggregationRowCodec.EncodeInverse(numeric);
            _inverseMemberShards[s] = AggregationRowCodec.EncodeInverse(member);
            _foldShards[s] = AggregationRowCodec.EncodeFoldInverse(fold);
        }

        AssertEquivalence();
    }

    private void AssertEquivalence()
    {
        var baselineMin = BaselineInverseMin();
        var scanMin = ScanInverseMin();
        if (baselineMin != scanMin)
        {
            throw new InvalidOperationException(
                $"min lanes disagree: baseline {baselineMin}, scan {scanMin}. The pair is timing different answers.");
        }

        var baselineMembers = BaselineInverseSetUnion();
        var scanMembers = ScanInverseSetUnion();
        if (baselineMembers.Count != scanMembers.Count || !baselineMembers.SetEquals(scanMembers))
        {
            throw new InvalidOperationException(
                $"set-union lanes disagree: baseline {baselineMembers.Count} members, scan {scanMembers.Count}.");
        }

        var baselineFold = BaselineFoldGather();
        var scanFold = ScanFoldGather();
        if (baselineFold.Count != scanFold.Count)
        {
            throw new InvalidOperationException(
                $"fold lanes disagree on member count: baseline {baselineFold.Count}, scan {scanFold.Count}.");
        }

        // The re-fold sorts by (HLC, sourceKey), so gather order is free - but the
        // gathered multiset must be identical or the folded value would differ.
        var baselineSorted = baselineFold.OrderBy(x => x.SourceKey, StringComparer.Ordinal).ToList();
        var scanSorted = scanFold.OrderBy(x => x.SourceKey, StringComparer.Ordinal).ToList();
        for (var i = 0; i < baselineSorted.Count; i++)
        {
            if (!string.Equals(baselineSorted[i].SourceKey, scanSorted[i].SourceKey, StringComparison.Ordinal)
                || baselineSorted[i].Member.Timestamp.CompareTo(scanSorted[i].Member.Timestamp) != 0
                || !baselineSorted[i].Member.Value.AsSpan().SequenceEqual(scanSorted[i].Member.Value))
            {
                throw new InvalidOperationException($"fold lanes disagree at gathered entry {i}.");
            }
        }

        foreach (var key in new[] { _sourceKey, string.Empty, _longSourceKey })
        {
            var baselineId = BaselineOperationId(_operationEpoch, key, _timestamp);
            var directId = DirectOperationId(_operationEpoch, key, _timestamp);
            if (!string.Equals(baselineId, directId, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"operation id lanes disagree for a {key.Length}-char source key: '{baselineId}' vs '{directId}'. "
                    + "The id is the saga dedup key, so a divergence here is a correctness defect, not a benchmark artefact.");
            }
        }
    }

    // ---- (1) inverse materialise, min/max walk ----

    /// <summary>Verbatim: decode each shard to a keyed map, read only the numeric.</summary>
    [Benchmark(Baseline = true, Description = "Inverse min: DecodeInverse -> Dictionary (baseline)")]
    public double BaselineInverseMin()
    {
        var extreme = double.PositiveInfinity;
        foreach (var bytes in _inverseNumericShards)
        {
            if (bytes is null || AggregationRowCodec.IsEmpty(bytes))
            {
                continue;
            }

            foreach (var (_, entry) in AggregationRowCodec.DecodeInverse(bytes))
            {
                extreme = Math.Min(extreme, entry.Numeric);
            }
        }

        return extreme;
    }

    /// <summary>Shipped: walk the row, materialising neither key nor member.</summary>
    [Benchmark(Description = "Inverse min: InverseRowScan (shipped)")]
    public double ScanInverseMin()
    {
        var extreme = double.PositiveInfinity;
        foreach (var bytes in _inverseNumericShards)
        {
            if (bytes is null || AggregationRowCodec.IsEmpty(bytes))
            {
                continue;
            }

            var scan = new AggregationRowCodec.InverseRowScan(bytes);
            while (scan.MoveNextNumeric())
            {
                extreme = Math.Min(extreme, scan.Numeric);
            }
        }

        return extreme;
    }

    // ---- (1b) inverse materialise, set-union walk ----

    /// <summary>
    /// Verbatim baseline for the set-union arm. It decodes source keys it never
    /// uses, but it does materialise the members - so this lane isolates the
    /// source-key saving alone, with member transcoding held constant.
    /// </summary>
    [Benchmark(Description = "Inverse set-union: DecodeInverse -> Dictionary (baseline)")]
    public HashSet<string> BaselineInverseSetUnion()
    {
        var members = new HashSet<string>(StringComparer.Ordinal);
        foreach (var bytes in _inverseMemberShards)
        {
            if (bytes is null || AggregationRowCodec.IsEmpty(bytes))
            {
                continue;
            }

            foreach (var (_, entry) in AggregationRowCodec.DecodeInverse(bytes))
            {
                if (entry.Member is not null)
                {
                    members.Add(entry.Member);
                }
            }
        }

        return members;
    }

    /// <summary>Shipped: same members, no source keys and no per-shard map.</summary>
    [Benchmark(Description = "Inverse set-union: InverseRowScan (shipped)")]
    public HashSet<string> ScanInverseSetUnion()
    {
        var members = new HashSet<string>(StringComparer.Ordinal);
        foreach (var bytes in _inverseMemberShards)
        {
            if (bytes is null || AggregationRowCodec.IsEmpty(bytes))
            {
                continue;
            }

            var scan = new AggregationRowCodec.InverseRowScan(bytes);
            while (scan.MoveNext())
            {
                if (scan.Member is not null)
                {
                    members.Add(scan.Member);
                }
            }
        }

        return members;
    }

    // ---- (2) fold materialise gather ----

    /// <summary>Verbatim: decode to a map per shard, then flatten into an unsized list.</summary>
    [Benchmark(Description = "Fold gather: DecodeFoldInverse -> Dictionary -> List (baseline)")]
    public int BaselineFoldGatherLane() => BaselineFoldGather().Count;

    /// <summary>Shipped: walk straight into the list, grown once to the row's own count.</summary>
    [Benchmark(Description = "Fold gather: FoldInverseRowScan + EnsureCapacity (shipped)")]
    public int ScanFoldGatherLane() => ScanFoldGather().Count;

    // The gather helpers return an internal element type, so they cannot be the
    // public benchmark methods themselves; the lanes above build the identical
    // list and report its count.
    internal List<(string SourceKey, AggregationRowCodec.FoldMember Member)> BaselineFoldGather()
    {
        var members = new List<(string SourceKey, AggregationRowCodec.FoldMember Member)>();
        foreach (var bytes in _foldShards)
        {
            if (bytes is null || AggregationRowCodec.IsEmpty(bytes))
            {
                continue;
            }

            foreach (var (sourceKey, member) in AggregationRowCodec.DecodeFoldInverse(bytes))
            {
                members.Add((sourceKey, member));
            }
        }

        return members;
    }

    internal List<(string SourceKey, AggregationRowCodec.FoldMember Member)> ScanFoldGather()
    {
        var members = new List<(string SourceKey, AggregationRowCodec.FoldMember Member)>();
        foreach (var bytes in _foldShards)
        {
            if (bytes is null || AggregationRowCodec.IsEmpty(bytes))
            {
                continue;
            }

            var scan = new AggregationRowCodec.FoldInverseRowScan(bytes);
            members.EnsureCapacity(members.Count + scan.Remaining);
            while (scan.MoveNext())
            {
                members.Add((scan.SourceKey, scan.Member));
            }
        }

        return members;
    }

    // ---- (3) operation id ----

    /// <summary>Verbatim copy of the replaced body, stack/pool sizing included.</summary>
    private static string BaselineOperationId(string epoch, string sourceKey, HybridLogicalClock timestamp)
    {
        var payload = $"{epoch}\u0000{sourceKey}\u0000{timestamp.WallClockTicks}\u0000{timestamp.Counter}";

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
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    /// <summary>Mirror of the shipped body; the sizing decision is unchanged.</summary>
    private static string DirectOperationId(string epoch, string sourceKey, HybridLogicalClock timestamp)
    {
        var maxByteCount = Encoding.UTF8.GetMaxByteCount(epoch.Length)
            + Encoding.UTF8.GetMaxByteCount(sourceKey.Length)
            + 20 + 11 + 3;

        byte[]? rented = null;
        Span<byte> buffer = maxByteCount <= 256
            ? stackalloc byte[maxByteCount]
            : (rented = ArrayPool<byte>.Shared.Rent(maxByteCount));
        try
        {
            var written = 0;
            written += Encoding.UTF8.GetBytes(epoch, buffer);
            buffer[written++] = 0x00;
            written += Encoding.UTF8.GetBytes(sourceKey, buffer[written..]);
            buffer[written++] = 0x00;
            timestamp.WallClockTicks.TryFormat(buffer[written..], out var ticksWritten, default, CultureInfo.InvariantCulture);
            written += ticksWritten;
            buffer[written++] = 0x00;
            timestamp.Counter.TryFormat(buffer[written..], out var counterWritten, default, CultureInfo.InvariantCulture);
            written += counterWritten;

            var hash = XxHash64.HashToUInt64(buffer[..written]);
            return string.Create(
                "agg-".Length + 16,
                hash,
                static (destination, value) =>
                {
                    "agg-".CopyTo(destination);
                    value.TryFormat(destination["agg-".Length..], out _, "x16", CultureInfo.InvariantCulture);
                });
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    [Benchmark(Description = "OperationId: interpolated payload (baseline)")]
    public string BaselineOperationIdLane() => BaselineOperationId(_operationEpoch, _sourceKey, _timestamp);

    [Benchmark(Description = "OperationId: direct UTF-8 compose (shipped)")]
    public string DirectOperationIdLane() => DirectOperationId(_operationEpoch, _sourceKey, _timestamp);

    /// <summary>
    /// Above-threshold control: a 400-char source key pushes both lanes onto the
    /// pooled arm, so the trim is shown not to depend on the stack path.
    /// </summary>
    [Benchmark(Description = "OperationId pooled: interpolated payload (baseline)")]
    public string BaselineOperationIdPooledLane() => BaselineOperationId(_operationEpoch, _longSourceKey, _timestamp);

    [Benchmark(Description = "OperationId pooled: direct UTF-8 compose (shipped)")]
    public string DirectOperationIdPooledLane() => DirectOperationId(_operationEpoch, _longSourceKey, _timestamp);
}
