using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Globalization;
using System.IO.Hashing;
using System.Text;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates three independent trims on the leaf projection-digest write path and
/// the shared pooled-return idiom, so each one's per-call time and byte delta is
/// measurable in the clear with no Orleans cluster in the loop.
/// <para>
/// Every lane pair builds the identical corpus and pays the identical dispatch, so
/// the sole per-lane difference is the work under test. Both the baseline and the
/// optimized body are reproduced <em>here</em>, verbatim, rather than one of them
/// being called through to production: that keeps the recorded numbers meaningful
/// after the production edit lands, and it is the convention the sibling suites
/// (<see cref="PooledReturnTrimBenchmarks"/>, <see cref="LeafDigestScanTrimBenchmarks"/>)
/// already follow.
/// </para>
/// <para>
/// The pairs mirror the shipped edits:
/// (1) <b>Fixed-width field coalescing.</b> A row contribution appended the LWW
/// timestamp's ticks (8), its counter (4), the tombstone flag (1) and the
/// expiry ticks (8) as four separate <c>XxHash128.Append</c> calls. Each call is a
/// virtual dispatch into the streaming state machine with its own residual-buffer
/// bookkeeping, and the four parts are contiguous in a scratch buffer that is
/// already on the stack, so staging all 21 bytes and appending once emits the
/// identical byte stream for a quarter of the calls. The same shape recurs in the
/// vector-clock feed, where each replica appended 8 then 4 bytes;
/// (2) <b>Length-prefix and body coalescing.</b> A length-prefixed UTF-8 field
/// appended the 4-byte prefix and then the transcoded body as two calls. Reserving
/// the prefix at the head of the same staging buffer the transcode already writes
/// into makes it one call, again for an identical byte stream. The long-key lane is
/// the control: its worst case overflows the stack budget and keeps the two-pass
/// count shape, so the coalesce must still not regress it;
/// (3) <b>Vector-clock paired-array sort.</b> The multi-replica feed copied the
/// dictionary's keys into one pooled array, sorted it ordinally, and then performed
/// one <em>dictionary lookup per replica</em> to recover each clock in sorted order -
/// a string hash plus an equality compare that the enumeration it had already done
/// could have avoided. The shipped lane copies key and clock into two pooled arrays
/// in that same single enumeration and sorts them together with the paired
/// <c>Array.Sort(keys, items, ...)</c> overload, so the lookups disappear. The
/// single-replica lane is the control: it is already fast-pathed and must not
/// regress;
/// (4) <b>Pooled return over-clear.</b> <c>ArrayPool&lt;T&gt;.Shared.Return(rented,
/// clearArray: true)</c> memsets the entire rounded-up rental rather than the prefix
/// the caller wrote. Clearing <c>AsSpan(0, written)</c> and returning the array
/// unflagged drops the same references for the pool's purposes while touching only
/// what was written. Lanes are carried at a small written count (where the power-of-two
/// round-up dominates) and a large one (where it does not), because the trim's whole
/// character is that it scales with the rental rather than with a fixed constant.
/// </para>
/// <para>
/// <see cref="Setup"/> asserts that each pair's two lanes produce byte-identical
/// hashes over the same corpus - and, for pair (4), identical post-return array
/// contents - so a lane that stopped computing the same thing fails the run rather
/// than reporting a cheaper number. The vector-clock equivalence is additionally
/// asserted over a corpus whose dictionary insertion order is deliberately
/// <em>not</em> sorted, which is the precondition the ordinal sort exists to
/// normalise.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=digestappendfolds</c> (or
/// <c>--suite digestappendfolds</c>); see <c>Program.cs</c>. The suite has no
/// Orleans silo dependency, so it is fast to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class DigestAppendFoldBenchmarks
{
    /// <summary>Mirrors the private <c>BPlusLeafGrain.DigestScratchBytes</c>.</summary>
    private const int DigestScratchBytes = 256;

    /// <summary>Width of the length prefix every string field carries.</summary>
    private const int LengthPrefixBytes = 4;

    /// <summary>Rows each digest lane folds, so one lane is not dominated by call overhead.</summary>
    private const int RowCount = 256;

    /// <summary>Small written count: the pool rounds a 4-element rent up to 16.</summary>
    private const int SmallWritten = 4;

    /// <summary>Large written count: the pool rounds a 200-element rent up to 256.</summary>
    private const int LargeWritten = 200;

    /// <summary>
    /// Rental bound for the sparse lane: a buffer rented at 4096 slots and filled
    /// only to <see cref="LargeWritten"/>, which is the written-to-rented ratio the
    /// converted production sites run at.
    /// </summary>
    private const int SparseRent = 4096;

    private string[] _shortKeys = [];
    private string[] _longKeys = [];
    private RowFields[] _rows = [];
    private VersionVector[] _singleReplicaClocks = [];
    private VersionVector[] _fourReplicaClocks = [];
    private VersionVector[] _sixteenReplicaClocks = [];
    private string[] _poolPayloadSmall = [];
    private string[] _poolPayloadLarge = [];

    /// <summary>The fixed-width slice of a row contribution, as the funnel sees it.</summary>
    private readonly record struct RowFields(long Ticks, int Counter, bool IsTombstone, long ExpiresAtTicks);

    /// <summary>
    /// Builds the per-shape corpora and asserts that both lanes of all four pairs
    /// agree exactly, so the measured lanes are known to compute the same function
    /// before any timing is reported.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _shortKeys = new string[RowCount];
        _longKeys = new string[RowCount];
        _rows = new RowFields[RowCount];
        for (var i = 0; i < RowCount; i++)
        {
            _shortKeys[i] = string.Create(
                CultureInfo.InvariantCulture,
                $"tenant/alpha/orders/{i:D6}");
            _longKeys[i] = string.Create(
                CultureInfo.InvariantCulture,
                $"tenant/alpha/orders/{i:D6}/").PadRight(140, 'k');
            _rows[i] = new RowFields(
                Ticks: 638_000_000_000_000_000L + i,
                Counter: i % 97,
                IsTombstone: (i % 11) == 0,
                ExpiresAtTicks: (i % 5) == 0 ? 0L : 638_900_000_000_000_000L + i);
        }

        _singleReplicaClocks = BuildClocks(replicas: 1);
        _fourReplicaClocks = BuildClocks(replicas: 4);
        _sixteenReplicaClocks = BuildClocks(replicas: 16);

        _poolPayloadSmall = BuildPayload(SmallWritten);
        _poolPayloadLarge = BuildPayload(LargeWritten);

        AssertEqual(
            "fixed-width fields",
            () => FoldFields(_rows, coalesced: false),
            () => FoldFields(_rows, coalesced: true));

        AssertEqual(
            "feed-string short",
            () => FoldStrings(_shortKeys, mode: 0),
            () => FoldStrings(_shortKeys, mode: 1));

        AssertEqual(
            "feed-string short (const-size staging)",
            () => FoldStrings(_shortKeys, mode: 0),
            () => FoldStrings(_shortKeys, mode: 2));

        AssertEqual(
            "feed-string long",
            () => FoldStrings(_longKeys, mode: 0),
            () => FoldStrings(_longKeys, mode: 1));

        AssertEqual(
            "feed-string long (const-size staging)",
            () => FoldStrings(_longKeys, mode: 0),
            () => FoldStrings(_longKeys, mode: 2));

        AssertEqual(
            "feed-string empty and multi-byte",
            () => FoldStrings(["", "\u00e9\u00e8\u00ea", "\u4e2d\u6587\u30ad\u30fc"], mode: 0),
            () => FoldStrings(["", "\u00e9\u00e8\u00ea", "\u4e2d\u6587\u30ad\u30fc"], mode: 1));

        AssertEqual(
            "feed-string empty and multi-byte (const-size staging)",
            () => FoldStrings(["", "\u00e9\u00e8\u00ea", "\u4e2d\u6587\u30ad\u30fc"], mode: 0),
            () => FoldStrings(["", "\u00e9\u00e8\u00ea", "\u4e2d\u6587\u30ad\u30fc"], mode: 2));

        AssertEqual(
            "vector clock x1",
            () => FoldClocks(_singleReplicaClocks, paired: false),
            () => FoldClocks(_singleReplicaClocks, paired: true));

        AssertEqual(
            "vector clock x4",
            () => FoldClocks(_fourReplicaClocks, paired: false),
            () => FoldClocks(_fourReplicaClocks, paired: true));

        AssertEqual(
            "vector clock x16",
            () => FoldClocks(_sixteenReplicaClocks, paired: false),
            () => FoldClocks(_sixteenReplicaClocks, paired: true));

        // The ordinal sort exists precisely so insertion order cannot reach the
        // digest. Assert the paired lane over a corpus that violates sorted
        // insertion order outright, not merely over the happy shape.
        var shuffled = BuildClocks(replicas: 8, reverseInsertionOrder: true);
        var sorted = BuildClocks(replicas: 8, reverseInsertionOrder: false);
        AssertEqual(
            "vector clock insertion-order independence",
            () => FoldClocks(shuffled, paired: false),
            () => FoldClocks(sorted, paired: true));
        AssertEqual(
            "vector clock insertion-order independence (paired both sides)",
            () => FoldClocks(shuffled, paired: true),
            () => FoldClocks(sorted, paired: true));

        AssertEqual(
            "end-to-end row contribution",
            () => FoldRows(coalesced: false),
            () => FoldRows(coalesced: true));

        AssertPoolEquivalence(_poolPayloadSmall);
        AssertPoolEquivalence(_poolPayloadLarge);
        AssertPoolEquivalence(_poolPayloadLarge, SparseRent);
    }

    // ---------------------------------------------------------------------
    // (1) Fixed-width field coalescing.
    // ---------------------------------------------------------------------

    /// <summary>Pre-trim: the four fixed-width fields as four separate appends.</summary>
    [Benchmark]
    [BenchmarkCategory("fields")]
    public int Fields_Baseline() => FoldFields(_rows, coalesced: false).Length;

    /// <summary>Shipped: one 21-byte append staged in the scratch already on the stack.</summary>
    [Benchmark]
    [BenchmarkCategory("fields")]
    public int Fields_Optimized() => FoldFields(_rows, coalesced: true).Length;

    // ---------------------------------------------------------------------
    // (2) Length-prefix and body coalescing.
    // ---------------------------------------------------------------------

    /// <summary>Pre-trim: prefix and body appended separately, over ordinary short keys.</summary>
    [Benchmark]
    [BenchmarkCategory("string")]
    public int FeedStringShort_Baseline() => FoldStrings(_shortKeys, mode: 0).Length;

    /// <summary>Candidate: prefix reserved at the head of a worst-case-sized transcode buffer.</summary>
    [Benchmark]
    [BenchmarkCategory("string")]
    public int FeedStringShort_Optimized() => FoldStrings(_shortKeys, mode: 1).Length;

    /// <summary>Candidate: prefix reserved at the head of a constant-size transcode buffer.</summary>
    [Benchmark]
    [BenchmarkCategory("string")]
    public int FeedStringShort_ConstStack() => FoldStrings(_shortKeys, mode: 2).Length;

    /// <summary>Control: keys whose worst case overflows the stack budget.</summary>
    [Benchmark]
    [BenchmarkCategory("string")]
    public int FeedStringLong_Baseline() => FoldStrings(_longKeys, mode: 0).Length;

    /// <summary>Control counterpart to <see cref="FeedStringLong_Baseline"/>.</summary>
    [Benchmark]
    [BenchmarkCategory("string")]
    public int FeedStringLong_Optimized() => FoldStrings(_longKeys, mode: 1).Length;

    /// <summary>Control counterpart for the constant-size staging candidate.</summary>
    [Benchmark]
    [BenchmarkCategory("string")]
    public int FeedStringLong_ConstStack() => FoldStrings(_longKeys, mode: 2).Length;

    // ---------------------------------------------------------------------
    // (3) Vector-clock paired-array sort.
    // ---------------------------------------------------------------------

    /// <summary>Control: the single-replica fast path, which neither lane changes materially.</summary>
    [Benchmark]
    [BenchmarkCategory("vclock")]
    public int VectorClock1_Baseline() => FoldClocks(_singleReplicaClocks, paired: false).Length;

    /// <summary>Control counterpart to <see cref="VectorClock1_Baseline"/>.</summary>
    [Benchmark]
    [BenchmarkCategory("vclock")]
    public int VectorClock1_Optimized() => FoldClocks(_singleReplicaClocks, paired: true).Length;

    /// <summary>Pre-trim: keys-only rental plus one dictionary lookup per replica.</summary>
    [Benchmark]
    [BenchmarkCategory("vclock")]
    public int VectorClock4_Baseline() => FoldClocks(_fourReplicaClocks, paired: false).Length;

    /// <summary>Shipped: paired rentals sorted together, no lookups.</summary>
    [Benchmark]
    [BenchmarkCategory("vclock")]
    public int VectorClock4_Optimized() => FoldClocks(_fourReplicaClocks, paired: true).Length;

    /// <summary>Pre-trim at the width where the sort, not the lookups, dominates.</summary>
    [Benchmark]
    [BenchmarkCategory("vclock")]
    public int VectorClock16_Baseline() => FoldClocks(_sixteenReplicaClocks, paired: false).Length;

    /// <summary>Shipped counterpart to <see cref="VectorClock16_Baseline"/>.</summary>
    [Benchmark]
    [BenchmarkCategory("vclock")]
    public int VectorClock16_Optimized() => FoldClocks(_sixteenReplicaClocks, paired: true).Length;

    // ---------------------------------------------------------------------
    // End-to-end: the whole per-row contribution, all three trims together.
    // ---------------------------------------------------------------------

    /// <summary>Pre-trim whole-row contribution, which is what the funnel actually calls.</summary>
    [Benchmark]
    [BenchmarkCategory("row")]
    public int EntryHash_Baseline() => FoldRows(coalesced: false).Length;

    /// <summary>Shipped whole-row contribution.</summary>
    [Benchmark]
    [BenchmarkCategory("row")]
    public int EntryHash_Optimized() => FoldRows(coalesced: true).Length;

    // ---------------------------------------------------------------------
    // (4) Pooled return over-clear.
    // ---------------------------------------------------------------------

    /// <summary>Pre-trim: a 4-element write returned with the whole 16-slot bucket wiped.</summary>
    [Benchmark]
    [BenchmarkCategory("pool")]
    public int PoolSmall_Baseline() => RentFillReturn(_poolPayloadSmall, clearWholeRental: true);

    /// <summary>Shipped: only the written prefix is cleared.</summary>
    [Benchmark]
    [BenchmarkCategory("pool")]
    public int PoolSmall_Optimized() => RentFillReturn(_poolPayloadSmall, clearWholeRental: false);

    /// <summary>Control: a 200-element write, where the round-up slack is proportionally small.</summary>
    [Benchmark]
    [BenchmarkCategory("pool")]
    public int PoolLarge_Baseline() => RentFillReturn(_poolPayloadLarge, clearWholeRental: true);

    /// <summary>Control counterpart to <see cref="PoolLarge_Baseline"/>.</summary>
    [Benchmark]
    [BenchmarkCategory("pool")]
    public int PoolLarge_Optimized() => RentFillReturn(_poolPayloadLarge, clearWholeRental: false);

    /// <summary>
    /// Pre-trim, representative shape: a 200-element write into a rental bounded at
    /// 4096. This is the ratio the converted sites actually run at - each rents a
    /// worst-case bound and fills a small prefix of it - so this pair, not the
    /// rent-exactly <see cref="PoolLarge_Baseline"/> pair, is the one that measures
    /// the trim.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("pool")]
    public int PoolSparse_Baseline() => RentFillReturn(_poolPayloadLarge, clearWholeRental: true, SparseRent);

    /// <summary>Shipped counterpart to <see cref="PoolSparse_Baseline"/>.</summary>
    [Benchmark]
    [BenchmarkCategory("pool")]
    public int PoolSparse_Optimized() => RentFillReturn(_poolPayloadLarge, clearWholeRental: false, SparseRent);

    // ---------------------------------------------------------------------
    // Verbatim pre-trim bodies, and the shipped shapes reproduced.
    // ---------------------------------------------------------------------

    private static byte[] FoldFields(RowFields[] rows, bool coalesced)
    {
        var hasher = new XxHash128();
        Span<byte> scratch = stackalloc byte[24];
        foreach (var row in rows)
        {
            if (coalesced)
            {
                BinaryPrimitives.WriteInt64LittleEndian(scratch, row.Ticks);
                BinaryPrimitives.WriteInt32LittleEndian(scratch[8..12], row.Counter);
                scratch[12] = row.IsTombstone ? (byte)1 : (byte)0;
                BinaryPrimitives.WriteInt64LittleEndian(scratch[13..21], row.ExpiresAtTicks);
                hasher.Append(scratch[..21]);
            }
            else
            {
                BinaryPrimitives.WriteInt64LittleEndian(scratch, row.Ticks);
                hasher.Append(scratch[..8]);
                BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], row.Counter);
                hasher.Append(scratch[..4]);
                scratch[0] = row.IsTombstone ? (byte)1 : (byte)0;
                hasher.Append(scratch[..1]);
                BinaryPrimitives.WriteInt64LittleEndian(scratch, row.ExpiresAtTicks);
                hasher.Append(scratch[..8]);
            }
        }

        return hasher.GetCurrentHash();
    }

    /// <summary>
    /// Folds <paramref name="values"/> with one of the three candidate bodies.
    /// Mode 0 is the verbatim pre-trim split-append shape, mode 1 stages the
    /// prefix into a worst-case-sized buffer whose length still varies with the
    /// string (so it emits a real localloc), and mode 2 stages into a buffer of
    /// constant size (a fixed frame slot and an unrolled memset, at the cost of
    /// zero-filling the whole budget even for a short key).
    /// </summary>
    private static byte[] FoldStrings(string[] values, int mode)
    {
        var hasher = new XxHash128();
        Span<byte> scratch = stackalloc byte[24];
        foreach (var value in values)
        {
            switch (mode)
            {
                case 0: SplitFeedString(hasher, value, scratch); break;
                case 1: CoalescedFeedString(hasher, value, scratch); break;
                default: ConstStackFeedString(hasher, value, scratch); break;
            }
        }

        return hasher.GetCurrentHash();
    }

    /// <summary>
    /// Candidate body: one constant-size staging buffer serves both the short
    /// and the medium branch, so the method emits a fixed stack frame slot
    /// instead of a variable localloc with stack probing.
    /// </summary>
    private static void ConstStackFeedString(XxHash128 hasher, string value, Span<byte> scratch)
    {
        if (value.Length == 0)
        {
            BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], 0);
            hasher.Append(scratch[..4]);
            return;
        }

        if (Encoding.UTF8.GetMaxByteCount(value.Length) <= DigestScratchBytes)
        {
            Span<byte> staged = stackalloc byte[LengthPrefixBytes + DigestScratchBytes];
            var encoded = Encoding.UTF8.GetBytes(value, staged[LengthPrefixBytes..]);
            BinaryPrimitives.WriteInt32LittleEndian(staged[..LengthPrefixBytes], encoded);
            hasher.Append(staged[..(LengthPrefixBytes + encoded)]);
            return;
        }

        var byteCount = Encoding.UTF8.GetByteCount(value);
        if (byteCount <= DigestScratchBytes)
        {
            Span<byte> buf = stackalloc byte[LengthPrefixBytes + DigestScratchBytes];
            var written = Encoding.UTF8.GetBytes(value, buf[LengthPrefixBytes..]);
            BinaryPrimitives.WriteInt32LittleEndian(buf[..LengthPrefixBytes], written);
            hasher.Append(buf[..(LengthPrefixBytes + written)]);
            return;
        }

        var rented = ArrayPool<byte>.Shared.Rent(LengthPrefixBytes + byteCount);
        try
        {
            var written = Encoding.UTF8.GetBytes(value, rented.AsSpan(LengthPrefixBytes));
            BinaryPrimitives.WriteInt32LittleEndian(rented.AsSpan(0, LengthPrefixBytes), written);
            hasher.Append(rented.AsSpan(0, LengthPrefixBytes + written));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Verbatim pre-trim body: the prefix and the body are two appends.</summary>
    private static void SplitFeedString(XxHash128 hasher, string value, Span<byte> scratch)
    {
        if (value.Length == 0)
        {
            BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], 0);
            hasher.Append(scratch[..4]);
            return;
        }

        var maxByteCount = Encoding.UTF8.GetMaxByteCount(value.Length);
        if (maxByteCount <= DigestScratchBytes)
        {
            Span<byte> tight = stackalloc byte[maxByteCount];
            var encoded = Encoding.UTF8.GetBytes(value, tight);
            BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], encoded);
            hasher.Append(scratch[..4]);
            hasher.Append(tight[..encoded]);
            return;
        }

        var byteCount = Encoding.UTF8.GetByteCount(value);
        BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], byteCount);
        hasher.Append(scratch[..4]);
        if (byteCount <= DigestScratchBytes)
        {
            Span<byte> buf = stackalloc byte[DigestScratchBytes];
            var written = Encoding.UTF8.GetBytes(value, buf);
            hasher.Append(buf[..written]);
        }
        else
        {
            var rented = ArrayPool<byte>.Shared.Rent(byteCount);
            try
            {
                var written = Encoding.UTF8.GetBytes(value, rented);
                hasher.Append(rented.AsSpan(0, written));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    /// <summary>Shipped body: the prefix is reserved at the head of the transcode buffer.</summary>
    private static void CoalescedFeedString(XxHash128 hasher, string value, Span<byte> scratch)
    {
        if (value.Length == 0)
        {
            BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], 0);
            hasher.Append(scratch[..4]);
            return;
        }

        var maxByteCount = Encoding.UTF8.GetMaxByteCount(value.Length);
        if (maxByteCount <= DigestScratchBytes)
        {
            Span<byte> staged = stackalloc byte[LengthPrefixBytes + maxByteCount];
            var encoded = Encoding.UTF8.GetBytes(value, staged[LengthPrefixBytes..]);
            BinaryPrimitives.WriteInt32LittleEndian(staged[..LengthPrefixBytes], encoded);
            hasher.Append(staged[..(LengthPrefixBytes + encoded)]);
            return;
        }

        var byteCount = Encoding.UTF8.GetByteCount(value);
        if (byteCount <= DigestScratchBytes)
        {
            Span<byte> buf = stackalloc byte[LengthPrefixBytes + DigestScratchBytes];
            var written = Encoding.UTF8.GetBytes(value, buf[LengthPrefixBytes..]);
            BinaryPrimitives.WriteInt32LittleEndian(buf[..LengthPrefixBytes], written);
            hasher.Append(buf[..(LengthPrefixBytes + written)]);
            return;
        }

        var rented = ArrayPool<byte>.Shared.Rent(LengthPrefixBytes + byteCount);
        try
        {
            var written = Encoding.UTF8.GetBytes(value, rented.AsSpan(LengthPrefixBytes));
            BinaryPrimitives.WriteInt32LittleEndian(rented.AsSpan(0, LengthPrefixBytes), written);
            hasher.Append(rented.AsSpan(0, LengthPrefixBytes + written));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static byte[] FoldClocks(VersionVector[] clocks, bool paired)
    {
        var hasher = new XxHash128();
        Span<byte> scratch = stackalloc byte[24];
        foreach (var clock in clocks)
        {
            if (paired)
            {
                PairedFeedVectorClock(hasher, clock, scratch);
            }
            else
            {
                LookupFeedVectorClock(hasher, clock, scratch);
            }
        }

        return hasher.GetCurrentHash();
    }

    /// <summary>Verbatim pre-trim body: keys-only rental, one dictionary lookup per replica.</summary>
    private static void LookupFeedVectorClock(XxHash128 hasher, VersionVector? vc, Span<byte> scratch)
    {
        if (vc is null || vc.Entries.Count == 0)
        {
            BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], -1);
            hasher.Append(scratch[..4]);
            return;
        }

        var count = vc.Entries.Count;
        if (count == 1)
        {
            BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], 1);
            hasher.Append(scratch[..4]);
            foreach (var (replica, clock) in vc.Entries)
            {
                SplitFeedString(hasher, replica, scratch);
                BinaryPrimitives.WriteInt64LittleEndian(scratch, clock.WallClockTicks);
                hasher.Append(scratch[..8]);
                BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], clock.Counter);
                hasher.Append(scratch[..4]);
            }

            return;
        }

        var replicas = ArrayPool<string>.Shared.Rent(count);
        try
        {
            var i = 0;
            foreach (var k in vc.Entries.Keys) replicas[i++] = k;
            Array.Sort(replicas, 0, count, StringComparer.Ordinal);

            BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], count);
            hasher.Append(scratch[..4]);

            for (var j = 0; j < count; j++)
            {
                var replica = replicas[j];
                SplitFeedString(hasher, replica, scratch);
                var clock = vc.Entries[replica];
                BinaryPrimitives.WriteInt64LittleEndian(scratch, clock.WallClockTicks);
                hasher.Append(scratch[..8]);
                BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], clock.Counter);
                hasher.Append(scratch[..4]);
            }
        }
        finally
        {
            ArrayPool<string>.Shared.Return(replicas, clearArray: true);
        }
    }

    /// <summary>Shipped body: paired rentals sorted together, prefix-only clear on return.</summary>
    private static void PairedFeedVectorClock(XxHash128 hasher, VersionVector? vc, Span<byte> scratch)
    {
        if (vc is null || vc.Entries.Count == 0)
        {
            BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], -1);
            hasher.Append(scratch[..4]);
            return;
        }

        var count = vc.Entries.Count;
        if (count == 1)
        {
            BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], 1);
            hasher.Append(scratch[..4]);
            foreach (var (replica, clock) in vc.Entries)
            {
                CoalescedFeedString(hasher, replica, scratch);
                BinaryPrimitives.WriteInt64LittleEndian(scratch, clock.WallClockTicks);
                BinaryPrimitives.WriteInt32LittleEndian(scratch[8..12], clock.Counter);
                hasher.Append(scratch[..12]);
            }

            return;
        }

        var replicas = ArrayPool<string>.Shared.Rent(count);
        var clocks = ArrayPool<HybridLogicalClock>.Shared.Rent(count);
        var written = 0;
        try
        {
            foreach (var (k, c) in vc.Entries)
            {
                replicas[written] = k;
                clocks[written] = c;
                written++;
            }

            Array.Sort(replicas, clocks, 0, count, StringComparer.Ordinal);

            BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], count);
            hasher.Append(scratch[..4]);

            for (var j = 0; j < count; j++)
            {
                CoalescedFeedString(hasher, replicas[j], scratch);
                var clock = clocks[j];
                BinaryPrimitives.WriteInt64LittleEndian(scratch, clock.WallClockTicks);
                BinaryPrimitives.WriteInt32LittleEndian(scratch[8..12], clock.Counter);
                hasher.Append(scratch[..12]);
            }
        }
        finally
        {
            replicas.AsSpan(0, written).Clear();
            ArrayPool<string>.Shared.Return(replicas);
            ArrayPool<HybridLogicalClock>.Shared.Return(clocks);
        }
    }

    /// <summary>
    /// The whole per-row contribution, as the write funnel calls it: key, timestamp,
    /// tombstone flag, expiry, origin id, vector clock and value length.
    /// </summary>
    private byte[] FoldRows(bool coalesced)
    {
        var hasher = new XxHash128();
        Span<byte> scratch = stackalloc byte[24];
        var value = new byte[64];
        for (var i = 0; i < RowCount; i++)
        {
            var row = _rows[i];
            if (coalesced)
            {
                CoalescedFeedString(hasher, _shortKeys[i], scratch);
                BinaryPrimitives.WriteInt64LittleEndian(scratch, row.Ticks);
                BinaryPrimitives.WriteInt32LittleEndian(scratch[8..12], row.Counter);
                scratch[12] = row.IsTombstone ? (byte)1 : (byte)0;
                BinaryPrimitives.WriteInt64LittleEndian(scratch[13..21], row.ExpiresAtTicks);
                hasher.Append(scratch[..21]);
                CoalescedFeedString(hasher, "cluster-alpha", scratch);
                PairedFeedVectorClock(hasher, _fourReplicaClocks[i], scratch);
            }
            else
            {
                SplitFeedString(hasher, _shortKeys[i], scratch);
                BinaryPrimitives.WriteInt64LittleEndian(scratch, row.Ticks);
                hasher.Append(scratch[..8]);
                BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], row.Counter);
                hasher.Append(scratch[..4]);
                scratch[0] = row.IsTombstone ? (byte)1 : (byte)0;
                hasher.Append(scratch[..1]);
                BinaryPrimitives.WriteInt64LittleEndian(scratch, row.ExpiresAtTicks);
                hasher.Append(scratch[..8]);
                SplitFeedString(hasher, "cluster-alpha", scratch);
                LookupFeedVectorClock(hasher, _fourReplicaClocks[i], scratch);
            }

            if (!row.IsTombstone)
            {
                BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], value.Length);
                hasher.Append(scratch[..4]);
                hasher.Append(value);
            }
            else
            {
                BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], -1);
                hasher.Append(scratch[..4]);
            }
        }

        return hasher.GetCurrentHash();
    }

    /// <summary>
    /// Rents a reference-typed buffer, writes <paramref name="payload"/> into its
    /// prefix, and returns it either with the whole rounded-up rental wiped or with
    /// only the written prefix cleared. Repeated so the per-call cost is not lost in
    /// the loop frame.
    /// <para>
    /// <paramref name="rentSize"/> decides the rental the prefix is written into,
    /// independently of how much is written. That ratio is the whole effect under
    /// test: the saving is proportional to the slack between the rental and the
    /// written prefix, so a lane that rents exactly what it writes measures almost
    /// nothing. The production sites this models rent a bound and write far less
    /// than it (a probe array rented for every candidate but filled only for the
    /// ones actually dispatched, a growable slice buffer rented at 64 and filled
    /// with a handful), so the sparse lane is the representative one and the
    /// rent-exactly lane is its near-worst case.
    /// </para>
    /// </summary>
    private static int RentFillReturn(string[] payload, bool clearWholeRental, int rentSize = 0)
    {
        var total = 0;
        var rent = rentSize <= 0 ? payload.Length : rentSize;
        for (var round = 0; round < 64; round++)
        {
            var rented = ArrayPool<string>.Shared.Rent(rent);
            var written = 0;
            try
            {
                for (var i = 0; i < payload.Length; i++)
                {
                    rented[written++] = payload[i];
                }

                total += written;
            }
            finally
            {
                if (clearWholeRental)
                {
                    ArrayPool<string>.Shared.Return(rented, clearArray: true);
                }
                else
                {
                    rented.AsSpan(0, written).Clear();
                    ArrayPool<string>.Shared.Return(rented);
                }
            }
        }

        return total;
    }

    // ---------------------------------------------------------------------
    // Corpus construction and equivalence assertions.
    // ---------------------------------------------------------------------

    private static VersionVector[] BuildClocks(int replicas, bool reverseInsertionOrder = false)
    {
        var clocks = new VersionVector[RowCount];
        for (var i = 0; i < RowCount; i++)
        {
            var vc = new VersionVector();
            for (var r = 0; r < replicas; r++)
            {
                var index = reverseInsertionOrder ? replicas - 1 - r : r;
                var replicaId = string.Create(CultureInfo.InvariantCulture, $"region-{index:D2}-silo");
                vc.Entries[replicaId] = new HybridLogicalClock
                {
                    WallClockTicks = 638_000_000_000_000_000L + (i * 31) + index,
                    Counter = (i + index) % 53,
                };
            }

            clocks[i] = vc;
        }

        return clocks;
    }

    private static string[] BuildPayload(int count)
    {
        var payload = new string[count];
        for (var i = 0; i < count; i++)
        {
            payload[i] = string.Create(CultureInfo.InvariantCulture, $"region-{i:D3}-silo");
        }

        return payload;
    }

    private static void AssertEqual(string what, Func<byte[]> baseline, Func<byte[]> optimized)
    {
        var left = baseline();
        var right = optimized();
        if (!left.AsSpan().SequenceEqual(right))
        {
            throw new InvalidOperationException(
                $"Lane pair '{what}' disagreed: the two bodies no longer compute the same digest.");
        }
    }

    /// <summary>
    /// Asserts the pooled-return lanes leave the pool in the same observable state:
    /// every slot the caller wrote is null again by the time the array is returned,
    /// which is the only property <c>clearArray: true</c> was there to guarantee.
    /// </summary>
    private static void AssertPoolEquivalence(string[] payload, int rentSize = 0)
    {
        var rented = ArrayPool<string>.Shared.Rent(rentSize <= 0 ? payload.Length : rentSize);
        var written = 0;
        for (var i = 0; i < payload.Length; i++)
        {
            rented[written++] = payload[i];
        }

        rented.AsSpan(0, written).Clear();
        for (var i = 0; i < written; i++)
        {
            if (rented[i] is not null)
            {
                throw new InvalidOperationException(
                    "Prefix-only clear left a written slot populated; the trim is not equivalent.");
            }
        }

        ArrayPool<string>.Shared.Return(rented);
    }
}
