using System;
using System.Collections.Generic;
using System.Globalization;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;
using Orleans.Serialization;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three trims made to the view maintainer's drain path: the fused
/// same-group re-contribution in
/// <c>Orleans.Lattice.Views.AggregationApplier</c> (inverse and fold), and the
/// elided re-encode in the durable-history reshaping.
/// <para>
/// (1) <b>Fused inverse re-contribution.</b> <c>ContributeInverseAsync</c>
/// retracts a source key's prior contribution and then adds the new one. When
/// the key keeps its group - the steady state, since a group only changes when
/// the grouped column does - both mutations address the <b>same</b> shard row,
/// so the applier read it, spliced it, wrote it, read it back, spliced it again
/// and wrote it again, all to change one entry. <c>SpliceInverse</c> now takes a
/// <c>moveToEnd</c> mode that elides the old entry and appends the new one in a
/// single pass, which is byte-for-byte what the pair produced.
/// </para>
/// <para>
/// (2) <b>Fused fold re-contribution.</b> The identical shape in
/// <c>ContributeFoldAsync</c>, saving strictly more because every redundant walk
/// also carried each member's opaque value payload through the row.
/// </para>
/// <para>
/// (3) <b>No-op history reshape.</b> The history projection is a pure function of
/// one mutation and never stamps <c>RetentionShape</c>, so every row it emits
/// arrives carrying the enum default, <c>MetadataOnly</c> - which is also the
/// default retention policy. Under that policy a delete, a range-tombstone
/// marker and a CRDT delta are already in their stored shape, yet the maintainer
/// re-serialised each one to reproduce bytes it was already holding.
/// <c>Shape</c> now reports whether it altered the row and the maintainer keeps
/// the original bytes when it did not.
/// </para>
/// <para>
/// Controls, because a trim must be shown not to have taxed the case it cannot
/// help: a <b>re-group</b> contribution, whose two mutations address different
/// shards and therefore must not fuse; a <b>single-entry</b> shard, where the
/// fused splice has the least to elide and the unfused pair gets to skip its
/// second read by deleting the row; and a <b>FullValue</b> retention policy,
/// under which no row is ever already in shape so the change test can only cost.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=aggfused</c> (or
/// <c>--suite aggfused</c>). No Orleans silo is involved, so it is cheap at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class AggregationFusedContributionBenchmarks
{
    private const int MembersPerShard = 16;

    private byte[] _inverseShard = null!;
    private byte[] _inverseSingleton = null!;
    private byte[] _foldShard = null!;
    private byte[] _foldSingleton = null!;

    private string _presentKey = null!;
    private string _singletonKey = null!;
    private AggregationRowCodec.MemberEntry _entry;
    private AggregationRowCodec.FoldMember _foldEntry;

    private HistoryRowCodec _historyCodec = null!;
    private byte[] _deltaRow = null!;
    private byte[] _deleteRow = null!;
    private byte[] _setRow = null!;
    private HistoryRetentionPolicy _metadataOnly;
    private HistoryRetentionPolicy _fullValue;
    private long _nowTicks;

    /// <summary>
    /// Builds the rows every lane walks, then asserts that each fused lane is
    /// <b>byte-identical</b> to the unfused pair it replaces and that the elided
    /// re-encode reproduces the bytes the re-encode produced. A pair that
    /// disagrees is timing two different computations, so fail the setup rather
    /// than publish the run.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _presentKey = "tenant-a/orders/2024/src-000007";
        _singletonKey = "tenant-a/orders/2024/src-000000";
        _entry = new AggregationRowCodec.MemberEntry(4242.5, "member-spliced");
        _foldEntry = new AggregationRowCodec.FoldMember(
            [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
            new HybridLogicalClock { WallClockTicks = 638_000_000_000_000_000, Counter = 11 });

        var inverse = new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal);
        var fold = new Dictionary<string, AggregationRowCodec.FoldMember>(StringComparer.Ordinal);
        for (var m = 0; m < MembersPerShard; m++)
        {
            var key = string.Create(CultureInfo.InvariantCulture, $"tenant-a/orders/2024/src-{m:D6}");
            inverse[key] = new AggregationRowCodec.MemberEntry(m * 1.5, string.Create(CultureInfo.InvariantCulture, $"member-{m}"));
            fold[key] = new AggregationRowCodec.FoldMember(
                [(byte)m, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70],
                new HybridLogicalClock { WallClockTicks = 1_000 + m, Counter = m });
        }

        _inverseShard = AggregationRowCodec.EncodeInverse(inverse);
        _foldShard = AggregationRowCodec.EncodeFoldInverse(fold);
        _inverseSingleton = AggregationRowCodec.EncodeInverse(
            new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal)
            {
                [_singletonKey] = inverse[_singletonKey],
            });
        _foldSingleton = AggregationRowCodec.EncodeFoldInverse(
            new Dictionary<string, AggregationRowCodec.FoldMember>(StringComparer.Ordinal)
            {
                [_singletonKey] = fold[_singletonKey],
            });

        _historyCodec = new HistoryRowCodec(
            (Orleans.Serialization.Serializer<HistoryRow>)new Microsoft.Extensions.DependencyInjection.ServiceCollection()
                .AddSerializer()
                .BuildServiceProvider()
                .GetService(typeof(Orleans.Serialization.Serializer<HistoryRow>))!);
        _nowTicks = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;
        _metadataOnly = new HistoryRetentionPolicy(HistoryRetentionMode.MetadataOnly, TimeSpan.Zero, TimeSpan.Zero);
        _fullValue = new HistoryRetentionPolicy(HistoryRetentionMode.FullValue, TimeSpan.Zero, TimeSpan.Zero);

        var payload = new byte[512];
        for (var i = 0; i < payload.Length; i++)
        {
            payload[i] = (byte)i;
        }

        var stamp = new HybridLogicalClock { WallClockTicks = _nowTicks - 1_000, Counter = 3 };
        _deltaRow = _historyCodec.Encode(new HistoryRow
        {
            Timestamp = stamp,
            Kind = HistoryRowKind.CrdtDelta,
            SourceKey = "tenant-a/orders/2024/src-000007",
            Delta = payload,
        });
        _deleteRow = _historyCodec.Encode(new HistoryRow
        {
            Timestamp = stamp,
            Kind = HistoryRowKind.Delete,
            SourceKey = "tenant-a/orders/2024/src-000007",
        });
        _setRow = _historyCodec.Encode(new HistoryRow
        {
            Timestamp = stamp,
            Kind = HistoryRowKind.Set,
            SourceKey = "tenant-a/orders/2024/src-000007",
            Value = payload,
            ValueLength = payload.Length,
        });

        AssertEquivalence();
    }

    private void AssertEquivalence()
    {
        AssertSameRow("inverse re-contribute", UnfusedInverse(_inverseShard, _presentKey), FusedInverse(_inverseShard, _presentKey));
        AssertSameRow("inverse re-contribute, 1-entry", UnfusedInverse(_inverseSingleton, _singletonKey), FusedInverse(_inverseSingleton, _singletonKey));
        AssertSameRow("fold re-contribute", UnfusedFold(_foldShard, _presentKey), FusedFold(_foldShard, _presentKey));
        AssertSameRow("fold re-contribute, 1-entry", UnfusedFold(_foldSingleton, _singletonKey), FusedFold(_foldSingleton, _singletonKey));

        // A key absent from the shard must land identically either way: the
        // unfused removal is a no-op and both append.
        const string Absent = "tenant-a/orders/2024/src-999999";
        AssertSameRow("inverse re-contribute, absent", UnfusedInverse(_inverseShard, Absent), FusedInverse(_inverseShard, Absent));
        AssertSameRow("fold re-contribute, absent", UnfusedFold(_foldShard, Absent), FusedFold(_foldShard, Absent));

        // The elided re-encode must reproduce the re-encoded bytes exactly, for
        // every kind, under the policy where shaping is a no-op.
        foreach (var (lane, row) in new[] { ("delta", _deltaRow), ("delete", _deleteRow), ("set", _setRow) })
        {
            foreach (var (name, policy) in new[] { ("metadata-only", _metadataOnly), ("full-value", _fullValue) })
            {
                AssertSameRow($"history {lane} under {name}", BaselineShape(row, policy), TrimmedShape(row, policy));
            }
        }
    }

    private static void AssertSameRow(string lane, byte[]? expected, byte[]? actual)
    {
        if (expected is null || actual is null)
        {
            if (expected is null && actual is null)
            {
                return;
            }

            throw new InvalidOperationException(
                $"{lane}: one lane produced a deleted row and the other did not (baseline null={expected is null}).");
        }

        if (!expected.AsSpan().SequenceEqual(actual))
        {
            throw new InvalidOperationException(
                $"{lane}: the trimmed row differs from the baseline row ({expected.Length} vs {actual.Length} bytes).");
        }
    }

    // ---- (1) fused inverse re-contribution ----

    /// <summary>
    /// Verbatim: the retract splice, the delete-or-store decision the applier
    /// makes on its result, then the add splice against whatever that left. The
    /// baseline pays both walks, exactly as the applier's two store round trips
    /// did.
    /// </summary>
    private byte[]? UnfusedInverse(byte[] row, string sourceKey)
    {
        var retracted = AggregationRowCodec.SpliceInverse(row, sourceKey, add: null);
        var next = retracted is null
            ? AggregationRowCodec.EmptyEntryRow
            : retracted.AsSpan();
        return AggregationRowCodec.SpliceInverse(next, sourceKey, _entry);
    }

    private byte[]? FusedInverse(byte[] row, string sourceKey) =>
        AggregationRowCodec.SpliceInverse(row, sourceKey, _entry, moveToEnd: true);

    [Benchmark(Baseline = true, Description = "Inverse re-contribute, same group: retract splice + add splice (baseline)")]
    public int UnfusedInverseLane() => UnfusedInverse(_inverseShard, _presentKey)!.Length;

    [Benchmark(Description = "Inverse re-contribute, same group: one fused splice (shipped)")]
    public int FusedInverseLane() => FusedInverse(_inverseShard, _presentKey)!.Length;

    /// <summary>
    /// Control: a one-entry shard, where the unfused pair gets to skip its second
    /// walk (the retract drains the row, so the add starts from the four-byte
    /// empty row). If the fusion loses anywhere it is here.
    /// </summary>
    [Benchmark(Description = "Inverse re-contribute, 1-entry shard: retract + add (baseline control)")]
    public int UnfusedInverseSingletonLane() => UnfusedInverse(_inverseSingleton, _singletonKey)!.Length;

    [Benchmark(Description = "Inverse re-contribute, 1-entry shard: one fused splice (control)")]
    public int FusedInverseSingletonLane() => FusedInverse(_inverseSingleton, _singletonKey)!.Length;

    /// <summary>
    /// Control: a re-grouping contribution addresses two different shards, so the
    /// applier does not fuse it. The lane exists to show the unfused path is
    /// still the unfused path - the trim adds a string comparison to it and
    /// nothing else.
    /// </summary>
    [Benchmark(Description = "Inverse re-group: two shards, never fused (control)")]
    public int RegroupInverseLane()
    {
        var removed = AggregationRowCodec.SpliceInverse(_inverseShard, _presentKey, add: null);
        var added = AggregationRowCodec.SpliceInverse(AggregationRowCodec.EmptyEntryRow, _presentKey, _entry);
        return removed!.Length + added!.Length;
    }

    // ---- (2) fused fold re-contribution ----

    private byte[]? UnfusedFold(byte[] row, string sourceKey)
    {
        var retracted = AggregationRowCodec.SpliceFoldInverse(row, sourceKey, add: null);
        var next = retracted is null
            ? AggregationRowCodec.EmptyEntryRow
            : retracted.AsSpan();
        return AggregationRowCodec.SpliceFoldInverse(next, sourceKey, _foldEntry);
    }

    private byte[]? FusedFold(byte[] row, string sourceKey) =>
        AggregationRowCodec.SpliceFoldInverse(row, sourceKey, _foldEntry, moveToEnd: true);

    [Benchmark(Description = "Fold re-contribute, same group: retract splice + add splice (baseline)")]
    public int UnfusedFoldLane() => UnfusedFold(_foldShard, _presentKey)!.Length;

    [Benchmark(Description = "Fold re-contribute, same group: one fused splice (shipped)")]
    public int FusedFoldLane() => FusedFold(_foldShard, _presentKey)!.Length;

    /// <summary>Control: the same one-entry check for the fold row.</summary>
    [Benchmark(Description = "Fold re-contribute, 1-entry shard: retract + add (baseline control)")]
    public int UnfusedFoldSingletonLane() => UnfusedFold(_foldSingleton, _singletonKey)!.Length;

    [Benchmark(Description = "Fold re-contribute, 1-entry shard: one fused splice (control)")]
    public int FusedFoldSingletonLane() => FusedFold(_foldSingleton, _singletonKey)!.Length;

    // ---- (3) no-op history reshape ----

    /// <summary>Verbatim: decode, shape, re-encode unconditionally.</summary>
    private byte[] BaselineShape(byte[] value, HistoryRetentionPolicy policy)
    {
        var row = _historyCodec.Decode(value);
        var (shaped, _) = HistoryRetentionShaper.Shape(row, policy, _nowTicks);
        return _historyCodec.Encode(shaped);
    }

    private byte[] TrimmedShape(byte[] value, HistoryRetentionPolicy policy)
    {
        var row = _historyCodec.Decode(value);
        var (shaped, _) = HistoryRetentionShaper.Shape(row, policy, _nowTicks, out var changed);
        return changed ? _historyCodec.Encode(shaped) : value;
    }

    [Benchmark(Description = "History reshape, CRDT delta row: decode + shape + re-encode (baseline)")]
    public int BaselineShapeDeltaLane() => BaselineShape(_deltaRow, _metadataOnly).Length;

    [Benchmark(Description = "History reshape, CRDT delta row: re-encode elided (shipped)")]
    public int TrimmedShapeDeltaLane() => TrimmedShape(_deltaRow, _metadataOnly).Length;

    [Benchmark(Description = "History reshape, delete row: decode + shape + re-encode (baseline)")]
    public int BaselineShapeDeleteLane() => BaselineShape(_deleteRow, _metadataOnly).Length;

    [Benchmark(Description = "History reshape, delete row: re-encode elided (shipped)")]
    public int TrimmedShapeDeleteLane() => TrimmedShape(_deleteRow, _metadataOnly).Length;

    /// <summary>
    /// Control: an LWW Set row under metadata-only retention genuinely changes -
    /// its value bytes are stripped - so the re-encode still happens and the
    /// change test is pure overhead here.
    /// </summary>
    [Benchmark(Description = "History reshape, LWW set row (value stripped): baseline control")]
    public int BaselineShapeSetLane() => BaselineShape(_setRow, _metadataOnly).Length;

    [Benchmark(Description = "History reshape, LWW set row (value stripped): change test (control)")]
    public int TrimmedShapeSetLane() => TrimmedShape(_setRow, _metadataOnly).Length;

    /// <summary>
    /// Control: under full-value retention no row arrives already in shape, so
    /// every kind re-encodes and the change test can only cost.
    /// </summary>
    [Benchmark(Description = "History reshape, CRDT delta under full-value retention: baseline control")]
    public int BaselineShapeDeltaFullValueLane() => BaselineShape(_deltaRow, _fullValue).Length;

    [Benchmark(Description = "History reshape, CRDT delta under full-value retention: change test (control)")]
    public int TrimmedShapeDeltaFullValueLane() => TrimmedShape(_deltaRow, _fullValue).Length;
}
