using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.IO.Hashing;
using System.Buffers;
using System.Buffers.Binary;
using System.Text;

using BenchmarkDotNet.Attributes;

using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates three per-element trims on the leaf read, digest and bisect hot
/// paths, so their time and byte deltas are measurable in the clear rather
/// than buried under a silo, a grain call and a transport.
/// <para>
/// (1) <b>The leaf range enumerator retested a bound it had already
/// satisfied.</b> <c>LeafEntryCache.RangeRows.Enumerator.MoveNext</c> compared
/// every candidate row against <c>startInclusive</c>, including the rows
/// already inside the window. The backing dictionary is ordinally sorted, so
/// once one key sorts at or above the lower bound every later key does too:
/// the comparison can no longer change its answer, and the in-range span is
/// exactly the part of a range read the caller pays for. The lower bound is
/// now retired the first time it is satisfied.
/// </para>
/// <para>
/// (2) <b>The digest fed each string field by scanning it twice.</b>
/// <c>BPlusLeafGrain.FeedString</c> called <c>Encoding.UTF8.GetByteCount</c>
/// to write a length prefix and then <c>GetBytes</c> to write the payload, and
/// transcoded into an unconditional 256-byte <c>stackalloc</c> that C#
/// zero-fills whether or not the key is ten bytes long. When the worst-case
/// encoding fits the stack budget the exact count is not needed in advance, so
/// the string is now transcoded once and the written length becomes the
/// prefix, into a buffer sized to that worst case. This runs once per key,
/// once per origin id and once per vector-clock replica on every folded row.
/// </para>
/// <para>
/// (3) <b>The digest rented and sorted a one-element array per row.</b>
/// <c>BPlusLeafGrain.FeedVectorClock</c> rented a <c>string[]</c> from the
/// shared pool, copied the replica ids into it, called <c>Array.Sort</c> and
/// returned the rental with <c>clearArray: true</c> - for every folded row,
/// including the single-replica clock a non-replicated or single-region tree
/// always has, which is already sorted. <c>ArrayPool</c> rounds a one-element
/// rent up to its smallest bucket and the cleared return then wipes that whole
/// bucket, so the overhead is not proportional to the one entry it protects.
/// A single-replica clock is now fed straight through.
/// </para>
/// <para>
/// Read (1) and (3) for <b>time</b> and (3) additionally for <b>pool
/// traffic</b>: none of the three changes managed heap traffic - (2) narrows a
/// stack buffer and (3) removes an <c>ArrayPool</c> rental, neither of which
/// BenchmarkDotNet bills as allocation - so a claimed byte win would be noise,
/// and the <c>Allocated</c> column is reported precisely so that can be
/// checked rather than asserted.
/// </para>
/// <para>
/// Every group carries a control lane where the trim is expected to buy
/// nothing: an unbounded scan whose lower bound was never tested, a window
/// admitting a single row so the prefix skip is the whole walk, a string whose
/// worst-case encoding overflows the stack budget so the two-pass shape is
/// retained deliberately, and a four-replica clock that still rents and sorts.
/// A trim that taxes the case it cannot help is not a trim. Group (1)
/// additionally reports an isolated lane (<c>ScanWindow_*_KeysOnly</c>) beside
/// the end-to-end one, because a range read's per-row cost is dominated by
/// work the trim does not touch.
/// </para>
/// <para>
/// Every baseline lane is a verbatim copy of the code the trim replaced, so it
/// pays exactly the overhead its shipped counterpart pays, and each is
/// asserted in <see cref="Setup"/> to produce exactly the answer its
/// counterpart produces - over corpora that include an empty window, a window
/// admitting one row, a window below every key and one above every key, ASCII
/// and multi-byte-UTF-8 and surrogate-pair strings, the empty string, and
/// lengths either side of both the stack budget and the rent threshold. A lane
/// that answers differently is measuring different work, and the comparison
/// would be void.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=leafdigestscantrims</c> (or
/// <c>--suite leafdigestscantrims</c>); see <c>Program.cs</c>. No Orleans
/// silo is involved, so it runs cheaply at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class LeafDigestScanTrimBenchmarks
{
    /// <summary>Rows in the simulated leaf. A leaf splits well below this, so
    /// it is a generous upper bound on a real per-leaf scan.</summary>
    private const int RowCount = 4096;

    /// <summary>
    /// Mirrors the private <c>BPlusLeafGrain.DigestScratchBytes</c> and
    /// <c>LeafEntryCache.StackKeyBytes</c> so the baseline lanes apply the
    /// same stack budget the shipped code does.
    /// </summary>
    private const int BaselineStackBytes = 256;

    private SortedDictionary<string, LwwValue<byte[]>> _rows = null!;

    private string _windowStart = null!;
    private string _windowEnd = null!;
    private string _singleRowStart = null!;
    private string _singleRowEnd = null!;

    private string[] _shortKeys = null!;
    private string[] _longKeys = null!;

    private VersionVector[] _singleReplicaClocks = null!;
    private VersionVector[] _multiReplicaClocks = null!;

    /// <summary>
    /// Builds the corpora and asserts every optimized lane answers exactly
    /// what its baseline answers, over inputs that include the cases where the
    /// trims are expected to buy nothing.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _rows = new SortedDictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal);
        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        for (var i = 0; i < RowCount; i++)
        {
            var key = "key:" + i.ToString("D6", CultureInfo.InvariantCulture);
            _rows[key] = LwwValue<byte[]>.Create(new byte[32], clock) with
            {
                OriginClusterId = "cluster-" + (i % 4).ToString(CultureInfo.InvariantCulture),
            };
        }

        // A window over the middle half of the leaf: the shape a paged range
        // read produces, where most admitted rows follow the first one.
        _windowStart = "key:" + (RowCount / 4).ToString("D6", CultureInfo.InvariantCulture);
        _windowEnd = "key:" + (3 * RowCount / 4).ToString("D6", CultureInfo.InvariantCulture);

        // A window admitting exactly one row: the control shape for group (1),
        // where the prefix skip is the whole walk and the hoist buys nothing.
        _singleRowStart = "key:" + (RowCount / 2).ToString("D6", CultureInfo.InvariantCulture);
        _singleRowEnd = "key:" + ((RowCount / 2) + 1).ToString("D6", CultureInfo.InvariantCulture);

        _shortKeys = BuildShortKeys();
        _longKeys = BuildLongKeys();

        (_singleReplicaClocks, _multiReplicaClocks) = BuildClocks();

        AssertScanEquivalence();
        AssertDigestEquivalence();
        AssertVectorClockEquivalence();
    }

    private static string[] BuildShortKeys()
    {
        var keys = new string[512];
        for (var i = 0; i < keys.Length; i++)
        {
            keys[i] = (i % 4) switch
            {
                // Plain ASCII, the overwhelmingly common shape.
                0 => "tenant/orders/" + i.ToString("D6", CultureInfo.InvariantCulture),
                // Two-byte and three-byte UTF-8, so the written length differs
                // from the char count and a wrong prefix would be caught.
                1 => "clie\u00f1te/" + i.ToString(CultureInfo.InvariantCulture),
                2 => "\u65e5\u672c\u8a9e/" + i.ToString(CultureInfo.InvariantCulture),
                // A surrogate pair, encoding to four bytes from two chars.
                _ => "emoji/\U0001F600/" + i.ToString(CultureInfo.InvariantCulture),
            };
        }

        return keys;
    }

    private static string[] BuildLongKeys()
    {
        // 120 chars of ASCII: 120 bytes written, but a worst case of 363, so
        // this is the control shape where the single-pass arm cannot be taken
        // and the deliberately-retained two-pass shape must not regress.
        var keys = new string[256];
        for (var i = 0; i < keys.Length; i++)
        {
            keys[i] = new string('k', 112) + i.ToString("D8", CultureInfo.InvariantCulture);
        }

        return keys;
    }

    /// <summary>
    /// Builds the vector-clock corpora: the single-replica clock a
    /// non-replicated or single-region tree always carries, and the
    /// four-replica clock that is the control, where the rent and the sort are
    /// still required and the trim must cost nothing.
    /// </summary>
    private static (VersionVector[] Single, VersionVector[] Multi) BuildClocks()
    {
        var single = new VersionVector[512];
        for (var i = 0; i < single.Length; i++)
        {
            single[i] = new VersionVector();
            single[i].Entries["replica-" + (i % 3).ToString(CultureInfo.InvariantCulture)]
                = new HybridLogicalClock { WallClockTicks = 1_000L + i, Counter = i & 0xFF };
        }

        var multi = new VersionVector[512];
        for (var i = 0; i < multi.Length; i++)
        {
            multi[i] = new VersionVector();
            for (var r = 0; r < 4; r++)
            {
                // Inserted out of ordinal order so the sort is load-bearing.
                var replica = "replica-" + ((3 - r) + (i % 2) * 10).ToString(CultureInfo.InvariantCulture);
                multi[i].Entries[replica]
                    = new HybridLogicalClock { WallClockTicks = 2_000L + i + r, Counter = r };
            }
        }

        return (single, multi);
    }
    private void AssertScanEquivalence()
    {
        (string? Start, string? End)[] windows =
        [
            (_windowStart, _windowEnd),
            (null, null),
            (_singleRowStart, _singleRowEnd),
            // An empty window, one whose lower bound precedes every row, and
            // one whose lower bound follows every row.
            ("key:999998", "key:999999"),
            ("aaa", "key:000010"),
            ("zzz", null),
            (null, "aaa"),
        ];

        foreach (var (start, end) in windows)
        {
            var baseline = new List<string>();
            foreach (var row in new BaselineRangeRows(_rows, start, end))
            {
                baseline.Add(row.Key);
            }

            var optimized = new List<string>();
            foreach (var row in new LeafEntryCache.RangeRows(_rows, start, end))
            {
                optimized.Add(row.Key);
            }

            if (baseline.Count != optimized.Count)
            {
                throw new InvalidOperationException(
                    $"Range scan lanes disagree on count for window [{start}, {end}): "
                    + $"{baseline.Count} vs {optimized.Count}.");
            }

            for (var i = 0; i < baseline.Count; i++)
            {
                if (!string.Equals(baseline[i], optimized[i], StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(
                        $"Range scan lanes disagree at index {i} for window [{start}, {end}): "
                        + $"'{baseline[i]}' vs '{optimized[i]}'.");
                }
            }
        }
    }

    private void AssertDigestEquivalence()
    {
        var corpus = new List<string>(_shortKeys.Length + _longKeys.Length + 16);
        corpus.AddRange(_shortKeys);
        corpus.AddRange(_longKeys);
        corpus.Add(string.Empty);
        // Either side of the point at which the worst-case encoding stops
        // fitting the stack budget, and either side of the rent threshold.
        corpus.Add(new string('a', 84));
        corpus.Add(new string('a', 85));
        corpus.Add(new string('a', 256));
        corpus.Add(new string('a', 257));
        corpus.Add(new string('\u00e9', 200));
        corpus.Add(new string('\u65e5', 200));
        corpus.Add(string.Concat(Enumerable.Repeat("\U0001F600", 150)));

        Span<byte> scratch = stackalloc byte[16];
        foreach (var value in corpus)
        {
            var baselineHasher = new XxHash128();
            BaselineFeedString(baselineHasher, value, scratch);
            var baseline = baselineHasher.GetCurrentHash();

            var optimizedHasher = new XxHash128();
            BPlusLeafGrain.FeedString(optimizedHasher, value, scratch);
            var optimized = optimizedHasher.GetCurrentHash();

            if (!baseline.AsSpan().SequenceEqual(optimized))
            {
                throw new InvalidOperationException(
                    $"Digest feed lanes disagree for a {value.Length}-char value.");
            }

            var constScratchHasher = new XxHash128();
            ConstScratchFeedString(constScratchHasher, value, scratch);
            if (!baseline.AsSpan().SequenceEqual(constScratchHasher.GetCurrentHash()))
            {
                throw new InvalidOperationException(
                    $"Const-scratch digest arm disagrees for a {value.Length}-char value.");
            }
        }
    }

    private void AssertVectorClockEquivalence()
    {
        // Both corpora, plus a null clock and an empty one: the trim adds a
        // branch above the existing empty-clock guard, so that guard has to be
        // shown still to fire.
        var corpora = new[] { _singleReplicaClocks, _multiReplicaClocks };
        Span<byte> scratch = stackalloc byte[16];
        foreach (var corpus in corpora)
        {
            foreach (var clock in corpus)
            {
                var baselineHasher = new XxHash128();
                BaselineFeedVectorClock(baselineHasher, clock, scratch);
                var baseline = baselineHasher.GetCurrentHash();

                var optimizedHasher = new XxHash128();
                BPlusLeafGrain.FeedVectorClock(optimizedHasher, clock, scratch);
                var optimized = optimizedHasher.GetCurrentHash();

                if (!baseline.AsSpan().SequenceEqual(optimized))
                {
                    throw new InvalidOperationException(
                        $"Vector-clock lanes disagree for a {clock.Entries.Count}-replica clock.");
                }
            }
        }

        foreach (var degenerate in new VersionVector?[] { null, new VersionVector() })
        {
            var baselineHasher = new XxHash128();
            BaselineFeedVectorClock(baselineHasher, degenerate, scratch);

            var optimizedHasher = new XxHash128();
            BPlusLeafGrain.FeedVectorClock(optimizedHasher, degenerate, scratch);

            if (!baselineHasher.GetCurrentHash().AsSpan().SequenceEqual(optimizedHasher.GetCurrentHash()))
            {
                throw new InvalidOperationException("Vector-clock lanes disagree for a degenerate clock.");
            }
        }
    }
    // ---------------------------------------------------------------------
    // (1) Leaf range scan.
    // ---------------------------------------------------------------------

    /// <summary>End-to-end mid-leaf window walk on the pre-trim enumerator.</summary>
    [Benchmark]
    [BenchmarkCategory("scan")]
    public long ScanWindow_Baseline()
    {
        long total = 0;
        foreach (var row in new BaselineRangeRows(_rows, _windowStart, _windowEnd))
        {
            total += row.Value.Value!.Length;
        }

        return total;
    }

    /// <summary>End-to-end mid-leaf window walk on the shipped enumerator.</summary>
    [Benchmark]
    [BenchmarkCategory("scan")]
    public long ScanWindow_Optimized()
    {
        long total = 0;
        foreach (var row in new LeafEntryCache.RangeRows(_rows, _windowStart, _windowEnd))
        {
            total += row.Value.Value!.Length;
        }

        return total;
    }

    /// <summary>
    /// The same window, touching only the key. A range read's per-row cost is
    /// dominated by work the trim does not touch, so this lane isolates the
    /// removed comparison.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("scan")]
    public int ScanWindow_Baseline_KeysOnly()
    {
        var total = 0;
        foreach (var row in new BaselineRangeRows(_rows, _windowStart, _windowEnd))
        {
            total += row.Key.Length;
        }

        return total;
    }

    /// <summary>Isolated counterpart to <see cref="ScanWindow_Baseline_KeysOnly"/>.</summary>
    [Benchmark]
    [BenchmarkCategory("scan")]
    public int ScanWindow_Optimized_KeysOnly()
    {
        var total = 0;
        foreach (var row in new LeafEntryCache.RangeRows(_rows, _windowStart, _windowEnd))
        {
            total += row.Key.Length;
        }

        return total;
    }

    /// <summary>
    /// Control: a window admitting one row, so the prefix skip is the entire
    /// walk and the retired bound can buy nothing.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("scan")]
    public int ScanSingleRow_Baseline()
    {
        var total = 0;
        foreach (var row in new BaselineRangeRows(_rows, _singleRowStart, _singleRowEnd))
        {
            total += row.Key.Length;
        }

        return total;
    }

    /// <summary>Control counterpart to <see cref="ScanSingleRow_Baseline"/>.</summary>
    [Benchmark]
    [BenchmarkCategory("scan")]
    public int ScanSingleRow_Optimized()
    {
        var total = 0;
        foreach (var row in new LeafEntryCache.RangeRows(_rows, _singleRowStart, _singleRowEnd))
        {
            total += row.Key.Length;
        }

        return total;
    }

    /// <summary>
    /// Control: an unbounded scan. The pre-trim enumerator already skipped the
    /// lower-bound comparison when the bound was null, so both lanes run the
    /// same shape and must measure as a wash.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("scan")]
    public int ScanUnbounded_Baseline()
    {
        var total = 0;
        foreach (var row in new BaselineRangeRows(_rows, null, null))
        {
            total += row.Key.Length;
        }

        return total;
    }

    /// <summary>Control counterpart to <see cref="ScanUnbounded_Baseline"/>.</summary>
    [Benchmark]
    [BenchmarkCategory("scan")]
    public int ScanUnbounded_Optimized()
    {
        var total = 0;
        foreach (var row in new LeafEntryCache.RangeRows(_rows, null, null))
        {
            total += row.Key.Length;
        }

        return total;
    }

    // ---------------------------------------------------------------------
    // (2) Digest string feed.
    // ---------------------------------------------------------------------

    /// <summary>Pre-trim two-pass transcode over ordinary short keys.</summary>
    [Benchmark]
    [BenchmarkCategory("digest")]
    public int DigestShortKeys_Baseline()
    {
        Span<byte> scratch = stackalloc byte[16];
        var hasher = new XxHash128();
        foreach (var key in _shortKeys)
        {
            BaselineFeedString(hasher, key, scratch);
        }

        return hasher.GetCurrentHash().Length;
    }

    /// <summary>Shipped single-pass transcode over the same keys.</summary>
    [Benchmark]
    [BenchmarkCategory("digest")]
    public int DigestShortKeys_Optimized()
    {
        Span<byte> scratch = stackalloc byte[16];
        var hasher = new XxHash128();
        foreach (var key in _shortKeys)
        {
            BPlusLeafGrain.FeedString(hasher, key, scratch);
        }

        return hasher.GetCurrentHash().Length;
    }

    /// <summary>
    /// Third arm: single-pass transcode into a constant-size scratch. Isolates
    /// the transcode saving from the stack-narrowing, which are independent.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("digest")]
    public int DigestShortKeys_OptimizedConstScratch()
    {
        Span<byte> scratch = stackalloc byte[16];
        var hasher = new XxHash128();
        foreach (var key in _shortKeys)
        {
            ConstScratchFeedString(hasher, key, scratch);
        }

        return hasher.GetCurrentHash().Length;
    }

    /// <summary>
    /// Control: strings whose worst-case encoding overflows the stack budget,
    /// so the shipped code deliberately keeps the two-pass shape.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("digest")]
    public int DigestLongKeys_Baseline()
    {
        Span<byte> scratch = stackalloc byte[16];
        var hasher = new XxHash128();
        foreach (var key in _longKeys)
        {
            BaselineFeedString(hasher, key, scratch);
        }

        return hasher.GetCurrentHash().Length;
    }

    /// <summary>Control counterpart to <see cref="DigestLongKeys_Baseline"/>.</summary>
    [Benchmark]
    [BenchmarkCategory("digest")]
    public int DigestLongKeys_Optimized()
    {
        Span<byte> scratch = stackalloc byte[16];
        var hasher = new XxHash128();
        foreach (var key in _longKeys)
        {
            BPlusLeafGrain.FeedString(hasher, key, scratch);
        }

        return hasher.GetCurrentHash().Length;
    }

    // ---------------------------------------------------------------------
    // (3) Vector-clock feed.
    // ---------------------------------------------------------------------

    /// <summary>
    /// Pre-trim feed over single-replica clocks: rents, copies, sorts one
    /// element and returns the rental cleared, per row.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("vclock")]
    public int VectorClockSingleReplica_Baseline()
    {
        Span<byte> scratch = stackalloc byte[16];
        var hasher = new XxHash128();
        foreach (var clock in _singleReplicaClocks)
        {
            BaselineFeedVectorClock(hasher, clock, scratch);
        }

        return hasher.GetCurrentHash().Length;
    }

    /// <summary>Shipped feed: the single entry goes straight through.</summary>
    [Benchmark]
    [BenchmarkCategory("vclock")]
    public int VectorClockSingleReplica_Optimized()
    {
        Span<byte> scratch = stackalloc byte[16];
        var hasher = new XxHash128();
        foreach (var clock in _singleReplicaClocks)
        {
            BPlusLeafGrain.FeedVectorClock(hasher, clock, scratch);
        }

        return hasher.GetCurrentHash().Length;
    }

    /// <summary>
    /// Control: four-replica clocks, where the rent and the ordinal sort are
    /// load-bearing and the trim can only add a compare.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("vclock")]
    public int VectorClockMultiReplica_Baseline()
    {
        Span<byte> scratch = stackalloc byte[16];
        var hasher = new XxHash128();
        foreach (var clock in _multiReplicaClocks)
        {
            BaselineFeedVectorClock(hasher, clock, scratch);
        }

        return hasher.GetCurrentHash().Length;
    }

    /// <summary>Control counterpart to <see cref="VectorClockMultiReplica_Baseline"/>.</summary>
    [Benchmark]
    [BenchmarkCategory("vclock")]
    public int VectorClockMultiReplica_Optimized()
    {
        Span<byte> scratch = stackalloc byte[16];
        var hasher = new XxHash128();
        foreach (var clock in _multiReplicaClocks)
        {
            BPlusLeafGrain.FeedVectorClock(hasher, clock, scratch);
        }

        return hasher.GetCurrentHash().Length;
    }
    // ---------------------------------------------------------------------
    // Verbatim pre-trim bodies, and the shipped shape reproduced.
    // ---------------------------------------------------------------------

    /// <summary>
    /// Verbatim copy of <c>LeafEntryCache.RangeRows</c> as it stood before the
    /// trim, so the baseline lane pays exactly the enumerator overhead its
    /// shipped counterpart pays.
    /// </summary>
    private readonly struct BaselineRangeRows(
        SortedDictionary<string, LwwValue<byte[]>> rows, string? startInclusive, string? endExclusive)
    {
        public Enumerator GetEnumerator() => new(rows, startInclusive, endExclusive);

        public struct Enumerator(
            SortedDictionary<string, LwwValue<byte[]>> rows, string? startInclusive, string? endExclusive)
        {
            private SortedDictionary<string, LwwValue<byte[]>>.Enumerator _inner = rows.GetEnumerator();

            public KeyValuePair<string, LwwValue<byte[]>> Current { get; private set; }

            public bool MoveNext()
            {
                while (_inner.MoveNext())
                {
                    var candidate = _inner.Current;
                    if (startInclusive is not null
                        && string.CompareOrdinal(candidate.Key, startInclusive) < 0)
                    {
                        continue;
                    }

                    if (endExclusive is not null
                        && string.CompareOrdinal(candidate.Key, endExclusive) >= 0)
                    {
                        return false;
                    }

                    Current = candidate;
                    return true;
                }

                return false;
            }
        }
    }

    /// <summary>
    /// Verbatim copy of <c>BPlusLeafGrain.FeedString</c> as it stood before the
    /// trim, with the private stack budget mirrored as
    /// <see cref="BaselineStackBytes"/>.
    /// </summary>
    private static void BaselineFeedString(XxHash128 hasher, string value, Span<byte> scratch)
    {
        var byteCount = Encoding.UTF8.GetByteCount(value);
        BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], byteCount);
        hasher.Append(scratch[..4]);
        if (byteCount == 0) return;

        if (byteCount <= BaselineStackBytes)
        {
            Span<byte> buf = stackalloc byte[BaselineStackBytes];
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

    /// <summary>
    /// Third arm for group (2): the same single-pass transcode the shipped
    /// trim performs, but writing into a <em>constant-size</em>
    /// <c>stackalloc</c> instead of one sized to the individual string's
    /// worst case.
    /// <para>
    /// This arm exists because the two candidates it separates are
    /// independent, and only one of them is the actual mechanism. A
    /// constant-size <c>stackalloc</c> lowers to a fixed stack adjustment and
    /// an unrolled zeroing sequence, whereas a variable-size one needs a
    /// runtime stack probe and a variable-length zeroing loop - so narrowing
    /// the buffer can cost more than the bytes it saves zeroing. Running all
    /// three arms attributes the win rather than assuming it.
    /// </para>
    /// </summary>
    private static void ConstScratchFeedString(XxHash128 hasher, string value, Span<byte> scratch)
    {
        if (value.Length == 0)
        {
            BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], 0);
            hasher.Append(scratch[..4]);
            return;
        }

        if (Encoding.UTF8.GetMaxByteCount(value.Length) <= BaselineStackBytes)
        {
            Span<byte> buf = stackalloc byte[BaselineStackBytes];
            var encoded = Encoding.UTF8.GetBytes(value, buf);
            BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], encoded);
            hasher.Append(scratch[..4]);
            hasher.Append(buf[..encoded]);
            return;
        }

        BaselineFeedString(hasher, value, scratch);
    }

    /// <summary>
    /// Verbatim copy of <c>BPlusLeafGrain.FeedVectorClock</c> as it stood
    /// before the trim. It calls the shipped <c>FeedString</c>, so this lane
    /// and its counterpart differ only in the rent-and-sort and group (2)
    /// cannot leak into group (3)'s reading.
    /// </summary>
    private static void BaselineFeedVectorClock(XxHash128 hasher, VersionVector? vc, Span<byte> scratch)
    {
        if (vc is null || vc.Entries.Count == 0)
        {
            BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], -1);
            hasher.Append(scratch[..4]);
            return;
        }

        var count = vc.Entries.Count;
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
                BPlusLeafGrain.FeedString(hasher, replica, scratch);
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
}
