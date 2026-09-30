using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three trims made to the aggregation view's per-contribution
/// read-modify-write path in <c>Orleans.Lattice.Views.AggregationApplier</c>.
/// Every lane drives the real production codec through
/// <c>InternalsVisibleTo</c>, and each baseline lane is a <b>verbatim</b> copy of
/// the body it replaced, so the pair differs by the trim and nothing else.
/// <para>
/// (1) <b>Inverse splice.</b> <c>MutateInverseAsync</c> changes exactly one entry
/// of one group shard - a min / max / set-union contribution assigns its source
/// key's entry, and a retract removes it. It did so by decoding the whole shard
/// row into a <c>Dictionary&lt;string, MemberEntry&gt;</c> (a fresh string and a
/// hash insert per entry), mutating one key, and re-encoding every entry
/// (transcoding each of those same keys back to UTF-8). Both halves scale with
/// the shard; the change does not. <c>SpliceInverse</c> walks the encoded bytes,
/// transcodes only the key being spliced, and copies the untouched entries
/// through as raw bytes.
/// </para>
/// <para>
/// (2) <b>Fold splice.</b> <c>MutateFoldAsync</c> has the identical shape and a
/// strictly larger waste: <c>DecodeFoldInverse</c> also copies every member's
/// opaque value payload onto the heap, only for the re-encode to copy it straight
/// back out. <c>SpliceFoldInverse</c> copies those payload bytes once, row to
/// row.
/// </para>
/// <para>
/// (3) <b>Membership head.</b> Every contribute and every retract reads the
/// source key's membership row to retract it, and every one of those six callers
/// uses exactly two fields - the group to decrement and the numeric to subtract.
/// The member was decoded into a string that no caller has ever read.
/// <c>DecodeMembershipHead</c> steps over it, still reading and still bounding
/// its length prefix so the row is validated as strictly.
/// </para>
/// <para>
/// Controls, because a trim that adds a fixed cost must be shown not to have
/// added one to the case it does not help: a <b>single-entry</b> shard row for
/// both splices (the splice's two walks and its key transcode must not lose to a
/// dictionary of one), and a <b>member-free</b> membership row (the count / sum /
/// min / max kinds, whose rows carry no member for the head read to skip).
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=aggsplice</c> (or
/// <c>--suite aggsplice</c>). No Orleans silo is involved, so it is cheap at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class AggregationSpliceTrimBenchmarks
{
    private const int MembersPerShard = 16;

    private byte[] _inverseShard = null!;
    private byte[] _inverseSingleton = null!;
    private byte[] _foldShard = null!;
    private byte[] _foldSingleton = null!;
    private byte[] _membershipWithMember = null!;
    private byte[] _membershipNumeric = null!;

    private string _presentKey = null!;
    private string _absentKey = null!;
    private string _singletonKey = null!;
    private AggregationRowCodec.MemberEntry _entry;
    private AggregationRowCodec.FoldMember _foldEntry;

    /// <summary>
    /// Builds the encoded rows each lane walks, then asserts that every baseline
    /// and its spliced partner produce <b>byte-identical</b> rows, and that a
    /// malformed row is rejected identically by both. A pair that disagrees is
    /// timing two different computations, so the whole run is worthless; fail the
    /// setup rather than publish it.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _presentKey = "tenant-a/orders/2024/src-000007";
        _absentKey = "tenant-a/orders/2024/src-999999";
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

        var singleInverse = new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal)
        {
            [_singletonKey] = inverse[_singletonKey],
        };
        var singleFold = new Dictionary<string, AggregationRowCodec.FoldMember>(StringComparer.Ordinal)
        {
            [_singletonKey] = fold[_singletonKey],
        };
        _inverseSingleton = AggregationRowCodec.EncodeInverse(singleInverse);
        _foldSingleton = AggregationRowCodec.EncodeFoldInverse(singleFold);

        _membershipWithMember = AggregationRowCodec.EncodeMembership(
            new AggregationRowCodec.MembershipRow("group/eu-west", 91.5, "member-eu-west"));
        _membershipNumeric = AggregationRowCodec.EncodeMembership(
            new AggregationRowCodec.MembershipRow("group/eu-west", 91.5, null));

        AssertEquivalence();
    }

    private void AssertEquivalence()
    {
        // The spliced row must be byte-identical to the decode / mutate /
        // re-encode it replaces, for all four mutation shapes: replacing a key
        // that is present, appending one that is absent, removing a present key,
        // and removing an absent one (a no-op the baseline still re-encodes).
        AssertSameRow("inverse replace", BaselineInverseUpsert(_inverseShard, _presentKey), SpliceInverseUpsert(_inverseShard, _presentKey));
        AssertSameRow("inverse append", BaselineInverseUpsert(_inverseShard, _absentKey), SpliceInverseUpsert(_inverseShard, _absentKey));
        AssertSameRow("inverse remove", BaselineInverseRemove(_inverseShard, _presentKey), SpliceInverseRemove(_inverseShard, _presentKey));
        AssertSameRow("inverse remove absent", BaselineInverseRemove(_inverseShard, _absentKey), SpliceInverseRemove(_inverseShard, _absentKey));
        AssertSameRow("inverse singleton replace", BaselineInverseUpsert(_inverseSingleton, _singletonKey), SpliceInverseUpsert(_inverseSingleton, _singletonKey));

        // Removing the last entry must collapse to "delete the row" on both
        // sides, not to a zero-entry row that a later read would resurrect.
        AssertSameRow("inverse drain", BaselineInverseRemove(_inverseSingleton, _singletonKey), SpliceInverseRemove(_inverseSingleton, _singletonKey));

        AssertSameRow("fold replace", BaselineFoldUpsert(_foldShard, _presentKey), SpliceFoldUpsert(_foldShard, _presentKey));
        AssertSameRow("fold append", BaselineFoldUpsert(_foldShard, _absentKey), SpliceFoldUpsert(_foldShard, _absentKey));
        AssertSameRow("fold remove", BaselineFoldRemove(_foldShard, _presentKey), SpliceFoldRemove(_foldShard, _presentKey));
        AssertSameRow("fold remove absent", BaselineFoldRemove(_foldShard, _absentKey), SpliceFoldRemove(_foldShard, _absentKey));
        AssertSameRow("fold singleton replace", BaselineFoldUpsert(_foldSingleton, _singletonKey), SpliceFoldUpsert(_foldSingleton, _singletonKey));
        AssertSameRow("fold drain", BaselineFoldRemove(_foldSingleton, _singletonKey), SpliceFoldRemove(_foldSingleton, _singletonKey));

        // A new shard's first entry goes through the same splice against the
        // zero-entry row, so it must match encoding a one-key map outright.
        var seeded = AggregationRowCodec.EncodeInverse(new Dictionary<string, AggregationRowCodec.MemberEntry>(StringComparer.Ordinal) { [_presentKey] = _entry });
        AssertSameRow("inverse seed", seeded, AggregationRowCodec.SpliceInverse(AggregationRowCodec.EmptyEntryRow, _presentKey, _entry));

        foreach (var bytes in new[] { _membershipWithMember, _membershipNumeric })
        {
            var full = AggregationRowCodec.DecodeMembership(bytes);
            var head = AggregationRowCodec.DecodeMembershipHead(bytes);
            if (!string.Equals(full.GroupKey, head.GroupKey, StringComparison.Ordinal)
                || full.Numeric.CompareTo(head.Numeric) != 0)
            {
                throw new InvalidOperationException(
                    $"membership lanes disagree: '{full.GroupKey}'/{full.Numeric} vs '{head.GroupKey}'/{head.Numeric}.");
            }
        }

        AssertHostileParity();
    }

    /// <summary>
    /// A row can arrive from a remote peer under <c>ShipView</c> replication, so
    /// a splice that validated less than the decode it replaces would have moved
    /// a validation boundary rather than removed work. Feed both sides the same
    /// truncated rows and require both to reject.
    /// </summary>
    private void AssertHostileParity()
    {
        for (var cut = 1; cut < _inverseShard.Length; cut++)
        {
            var truncated = _inverseShard[..cut];
            var baselineRejected = Rejects(() => BaselineInverseRemove(truncated, _presentKey));
            var spliceRejected = Rejects(() => SpliceInverseRemove(truncated, _presentKey));
            if (baselineRejected != spliceRejected)
            {
                throw new InvalidOperationException(
                    $"inverse row truncated to {cut} byte(s): baseline rejected={baselineRejected}, splice rejected={spliceRejected}.");
            }
        }

        for (var cut = 1; cut < _foldShard.Length; cut++)
        {
            var truncated = _foldShard[..cut];
            var baselineRejected = Rejects(() => BaselineFoldRemove(truncated, _presentKey));
            var spliceRejected = Rejects(() => SpliceFoldRemove(truncated, _presentKey));
            if (baselineRejected != spliceRejected)
            {
                throw new InvalidOperationException(
                    $"fold row truncated to {cut} byte(s): baseline rejected={baselineRejected}, splice rejected={spliceRejected}.");
            }
        }

        for (var cut = 1; cut < _membershipWithMember.Length; cut++)
        {
            var truncated = _membershipWithMember[..cut];
            var baselineRejected = Rejects(() => AggregationRowCodec.DecodeMembership(truncated));
            var headRejected = Rejects(() => AggregationRowCodec.DecodeMembershipHead(truncated));
            if (baselineRejected != headRejected)
            {
                throw new InvalidOperationException(
                    $"membership row truncated to {cut} byte(s): decode rejected={baselineRejected}, head rejected={headRejected}.");
            }
        }
    }

    private static bool Rejects(Action action)
    {
        try
        {
            action();
            return false;
        }
        catch (InvalidDataException)
        {
            return true;
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
                $"{lane}: the spliced row differs from the decode/re-encode row ({expected.Length} vs {actual.Length} bytes).");
        }
    }

    // ---- (1) inverse row splice ----

    /// <summary>Verbatim: decode the shard to a keyed map, assign one key, re-encode.</summary>
    internal static byte[]? BaselineInverseUpsert(byte[] row, string sourceKey)
    {
        var map = AggregationRowCodec.DecodeInverse(row);
        map[sourceKey] = new AggregationRowCodec.MemberEntry(4242.5, "member-spliced");
        return map.Count == 0 ? null : AggregationRowCodec.EncodeInverse(map);
    }

    internal static byte[]? BaselineInverseRemove(byte[] row, string sourceKey)
    {
        var map = AggregationRowCodec.DecodeInverse(row);
        map.Remove(sourceKey);
        return map.Count == 0 ? null : AggregationRowCodec.EncodeInverse(map);
    }

    internal byte[]? SpliceInverseUpsert(byte[] row, string sourceKey) =>
        AggregationRowCodec.SpliceInverse(row, sourceKey, _entry);

    internal static byte[]? SpliceInverseRemove(byte[] row, string sourceKey) =>
        AggregationRowCodec.SpliceInverse(row, sourceKey, null);

    [Benchmark(Baseline = true, Description = "Inverse upsert: DecodeInverse -> Dictionary -> EncodeInverse (baseline)")]
    public int BaselineInverseUpsertLane() => BaselineInverseUpsert(_inverseShard, _presentKey)!.Length;

    [Benchmark(Description = "Inverse upsert: SpliceInverse (shipped)")]
    public int SpliceInverseUpsertLane() => SpliceInverseUpsert(_inverseShard, _presentKey)!.Length;

    [Benchmark(Description = "Inverse remove: DecodeInverse -> Dictionary -> EncodeInverse (baseline)")]
    public int BaselineInverseRemoveLane() => BaselineInverseRemove(_inverseShard, _presentKey)!.Length;

    [Benchmark(Description = "Inverse remove: SpliceInverse (shipped)")]
    public int SpliceInverseRemoveLane() => SpliceInverseRemove(_inverseShard, _presentKey)!.Length;

    /// <summary>
    /// Control: a one-entry shard. The splice pays two walks and a key transcode
    /// where the dictionary pays one insert, so this is the case it could lose.
    /// </summary>
    [Benchmark(Description = "Inverse upsert, 1-entry shard: Dictionary (baseline control)")]
    public int BaselineInverseSingletonLane() => BaselineInverseUpsert(_inverseSingleton, _singletonKey)!.Length;

    [Benchmark(Description = "Inverse upsert, 1-entry shard: SpliceInverse (control)")]
    public int SpliceInverseSingletonLane() => SpliceInverseUpsert(_inverseSingleton, _singletonKey)!.Length;

    // ---- (2) fold-inverse row splice ----

    internal static byte[]? BaselineFoldUpsert(byte[] row, string sourceKey)
    {
        var map = AggregationRowCodec.DecodeFoldInverse(row);
        map[sourceKey] = new AggregationRowCodec.FoldMember(
            [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
            new HybridLogicalClock { WallClockTicks = 638_000_000_000_000_000, Counter = 11 });
        return map.Count == 0 ? null : AggregationRowCodec.EncodeFoldInverse(map);
    }

    internal static byte[]? BaselineFoldRemove(byte[] row, string sourceKey)
    {
        var map = AggregationRowCodec.DecodeFoldInverse(row);
        map.Remove(sourceKey);
        return map.Count == 0 ? null : AggregationRowCodec.EncodeFoldInverse(map);
    }

    internal byte[]? SpliceFoldUpsert(byte[] row, string sourceKey) =>
        AggregationRowCodec.SpliceFoldInverse(row, sourceKey, _foldEntry);

    internal static byte[]? SpliceFoldRemove(byte[] row, string sourceKey) =>
        AggregationRowCodec.SpliceFoldInverse(row, sourceKey, null);

    [Benchmark(Description = "Fold upsert: DecodeFoldInverse -> Dictionary -> EncodeFoldInverse (baseline)")]
    public int BaselineFoldUpsertLane() => BaselineFoldUpsert(_foldShard, _presentKey)!.Length;

    [Benchmark(Description = "Fold upsert: SpliceFoldInverse (shipped)")]
    public int SpliceFoldUpsertLane() => SpliceFoldUpsert(_foldShard, _presentKey)!.Length;

    [Benchmark(Description = "Fold remove: DecodeFoldInverse -> Dictionary -> EncodeFoldInverse (baseline)")]
    public int BaselineFoldRemoveLane() => BaselineFoldRemove(_foldShard, _presentKey)!.Length;

    [Benchmark(Description = "Fold remove: SpliceFoldInverse (shipped)")]
    public int SpliceFoldRemoveLane() => SpliceFoldRemove(_foldShard, _presentKey)!.Length;

    /// <summary>Control: the same one-entry check for the fold row.</summary>
    [Benchmark(Description = "Fold upsert, 1-entry shard: Dictionary (baseline control)")]
    public int BaselineFoldSingletonLane() => BaselineFoldUpsert(_foldSingleton, _singletonKey)!.Length;

    [Benchmark(Description = "Fold upsert, 1-entry shard: SpliceFoldInverse (control)")]
    public int SpliceFoldSingletonLane() => SpliceFoldUpsert(_foldSingleton, _singletonKey)!.Length;

    // ---- (3) membership head read ----

    [Benchmark(Description = "Membership read, set-union row: DecodeMembership (baseline)")]
    public double BaselineMembershipLane()
    {
        var row = AggregationRowCodec.DecodeMembership(_membershipWithMember);
        return row.GroupKey.Length + row.Numeric;
    }

    [Benchmark(Description = "Membership read, set-union row: DecodeMembershipHead (shipped)")]
    public double HeadMembershipLane()
    {
        var head = AggregationRowCodec.DecodeMembershipHead(_membershipWithMember);
        return head.GroupKey.Length + head.Numeric;
    }

    /// <summary>
    /// Control: a count / sum / min / max membership row carries no member, so
    /// the head read has nothing to skip. It must not be slower than the decode
    /// it replaces on the kinds the trim cannot help.
    /// </summary>
    [Benchmark(Description = "Membership read, member-free row: DecodeMembership (baseline control)")]
    public double BaselineMembershipNumericLane()
    {
        var row = AggregationRowCodec.DecodeMembership(_membershipNumeric);
        return row.GroupKey.Length + row.Numeric;
    }

    [Benchmark(Description = "Membership read, member-free row: DecodeMembershipHead (control)")]
    public double HeadMembershipNumericLane()
    {
        var head = AggregationRowCodec.DecodeMembershipHead(_membershipNumeric);
        return head.GroupKey.Length + head.Numeric;
    }
}
