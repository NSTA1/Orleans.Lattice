using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using System.IO.Hashing;
using System.Runtime.InteropServices;
using System.Text;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.GrainIndex;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Arbitrates three independent read-path trims. Each group carries a baseline
/// lane holding a verbatim copy of the replaced body, an optimized lane running
/// the shipped body, and a control lane on the input shape where the trim is
/// expected to buy nothing - so a win that is really measurement noise has
/// somewhere to show itself.
/// <para>
/// <b>(1) bounds - the leaf range scan re-tested bounds the window already
/// enforced.</b> <c>CountAsync</c>, <c>GetKeysAsync</c> and
/// <c>GetEntriesAsync</c> fold every range bound into a half-open
/// <c>[from, to)</c> window and then hand that window to
/// <c>LeafEntryCache.EnumerateRange</c>, which yields only rows inside it. Each
/// then re-tested <c>endExclusive</c>, <c>beforeExclusive</c>, the in-progress
/// split key and <c>startInclusive</c> per admitted row - up to five ordinal
/// string comparisons whose answers the window has already decided. Only
/// <c>afterExclusive</c> survives: a lower bound is inclusive, so the one row
/// equal to it is admitted by the range and must still be rejected.
/// </para>
/// <para>
/// <b>(2) sort - the terminal re-sort the scan had already earned.</b> The
/// windowed scan emits in ascending ordinal order (the windows are contiguous,
/// half-open and ascending, and each walks the ordinally sorted backing
/// dictionary), so the only way a key range read's result can be out of order
/// is the tail that appends fresh committed pending keys. That tail is empty on
/// every read that carries no prepared write, which is the overwhelming
/// majority. The sort is now conditional on the tail having appended.
/// </para>
/// <para>
/// <b>(3) dotspan - provenance-decoder dot scans walked by list indexer.</b>
/// <c>OrSetDot</c> is a struct, so <c>list[i]</c> copies it, re-reads the
/// mutable <c>Count</c> and bounds-checks every access.
/// <c>OrSetProvenanceDecoder.ContainsExact</c> is the inner scan of
/// <c>DecodeState</c>'s per-element add-by-tombstone loop, so that cost is paid
/// quadratically; <c>SingleReplica</c> and the tombstone max-counter walk run
/// only on lists already longer than the decoder's index threshold. The bodies
/// are read-only, so the list length cannot change while a span over it is
/// alive.
/// </para>
/// <para>
/// <b>How to read the columns.</b> Time is the arbiter; allocation is reported
/// because none of the three trims is allowed to move it. Group (1) and (2)
/// lanes fill one pre-sized reused sink, so both arms of a pair allocate
/// identically and any Gen0 delta is a defect, not a result. Group (3)'s
/// <c>DecodeState</c> lanes do allocate - they build the event list - and both
/// arms must allocate the same.
/// </para>
/// <para>
/// <b>Controls.</b> <c>Bounds_*_Unbounded</c> is the whole-leaf read where every
/// bound is null, so the baseline's re-tests are null checks and the trim can
/// only remove branches that never fire. <c>Sort_*_WithPending</c> is the read
/// that does carry a prepared write, where the tail appends and both arms must
/// still sort. <c>SingleReplica_*_Short</c> is a three-dot list, below the
/// length at which materialising a span has been measured to repay (a prior run
/// cost +4.4% on a short list), so a regression there is the honest result and
/// is why the shipped callers gate the span walks behind a length threshold.
/// </para>
/// <para>
/// Run with <c>--suite leafboundhoistdotspan</c> (or
/// <c>BENCH_MICROBENCH_SUITE=leafboundhoistdotspan</c>). This machine is noisy:
/// treat smoke fidelity as an equivalence and direction check only and take
/// <c>BENCH_MICROBENCH_FIDELITY=full</c> as the arbiter.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class LeafBoundHoistAndDotSpanBenchmarks
{
    private const int RowCount = 4096;

    /// <summary>
    /// Mirrors the private <c>OrSetProvenanceDecoder.DotIndexThreshold</c>, so
    /// the long-list lanes sit where the shipped callers actually take the span
    /// walk and the short-list control sits well below it.
    /// </summary>
    private const int BaselineDotIndexThreshold = 8;

    private const string ReplicaA = "replica-a";
    private const string ReplicaB = "replica-b";

    // Group 1 - leaf range scan.
    private SortedDictionary<string, LwwValue<byte[]>> _rows = null!;
    private string _windowStart = null!;
    private string _windowEnd = null!;

    // Group 2 - terminal re-sort.
    private string[] _orderedKeys = null!;
    private string[] _pendingTail = null!;

    // Shared sink for groups 1 and 2, pre-sized so no lane pays for growth.
    private List<string> _sink = null!;

    // Group 3 - provenance decoder dot scans.
    private List<OrSetDot> _longAddDots = null!;
    private List<OrSetDot> _longSingleReplicaDots = null!;
    private List<OrSetDot> _shortSingleReplicaDots = null!;
    private OrSetDot _absentDot;
    private OrSet _churnedSet = null!;

    // Group 4 - grain-index fingerprint string feed.
    private const string KeyCodecId = "Orleans.Lattice.GrainIndex.StringGrainKeyCodec/v1";
    private GrainIndexDescriptor _descriptor = null!;

    /// <summary>
    /// Builds every corpus and asserts each optimized lane answers exactly what
    /// its baseline answers - including on the inputs where the trims must buy
    /// nothing, and on the one input the bound hoist must still reject.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _rows = new SortedDictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal);
        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        for (var i = 0; i < RowCount; i++)
        {
            _rows["key:" + i.ToString("D6", CultureInfo.InvariantCulture)]
                = LwwValue<byte[]>.Create(new byte[32], clock);
        }

        // The middle half of the leaf: the shape a paged range read produces,
        // where nearly every admitted row follows the first.
        _windowStart = "key:" + (RowCount / 4).ToString("D6", CultureInfo.InvariantCulture);
        _windowEnd = "key:" + (3 * RowCount / 4).ToString("D6", CultureInfo.InvariantCulture);

        _sink = new List<string>(RowCount + 8);

        _orderedKeys = new string[RowCount / 2];
        for (var i = 0; i < _orderedKeys.Length; i++)
        {
            _orderedKeys[i] = "key:" + ((RowCount / 4) + i).ToString("D6", CultureInfo.InvariantCulture);
        }

        // Four fresh committed pending keys, appended in dictionary order, so
        // the control lane's tail genuinely disorders the result.
        _pendingTail =
        [
            "key:000900", "key:003900", "key:001500", "key:002700",
        ];

        _longAddDots = MakeDots(ReplicaA, 32, baseCounter: 1);
        _longSingleReplicaDots = MakeDots(ReplicaA, 32, baseCounter: 1);
        _shortSingleReplicaDots = MakeDots(ReplicaA, 3, baseCounter: 1);
        _absentDot = new OrSetDot { ReplicaId = ReplicaB, Counter = 9_999 };
        _churnedSet = MakeChurnedOrSet(elements: 64, addsPerElement: 4, tombstonesPerElement: 12);

        // A realistic declaration: ordinary .NET type names, every one of them
        // short enough that its worst-case UTF-8 encoding fits the stack budget,
        // which is the path the trim changes.
        _descriptor = new GrainIndexDescriptor(
            name: "orders-by-customer",
            treeName: "orders",
            grainInterfaceTypeName: "Contoso.Ordering.Grains.IOrderGrain",
            stateTypeName: "Contoso.Ordering.Grains.OrderState",
            properties:
            [
                new GrainIndexPropertyDescriptor("CustomerId", "System.String"),
                new GrainIndexPropertyDescriptor("PlacedAtUtc", "System.DateTimeOffset"),
                new GrainIndexPropertyDescriptor("Status", "Contoso.Ordering.OrderStatus"),
                new GrainIndexPropertyDescriptor("TotalMinorUnits", "System.Int64"),
                new GrainIndexPropertyDescriptor("Region", "System.String"),
                new GrainIndexPropertyDescriptor("IsExpedited", "System.Boolean"),
            ],
            allowReplication: false);

        AssertBoundsEquivalence();
        AssertSortEquivalence();
        AssertDotScanEquivalence();
        AssertFingerprintEquivalence();
    }

    /// <summary>
    /// The fingerprint is persisted and compared across restarts, so the trim is
    /// only admissible if it emits byte-identical digests. Asserted over the
    /// benchmark descriptor and over the two shapes the feed special-cases: an
    /// empty name, and a name whose worst-case encoding overflows the stack
    /// budget so the two-pass path still runs.
    /// </summary>
    private void AssertFingerprintEquivalence()
    {
        GrainIndexDescriptor[] cases =
        [
            _descriptor,
            new GrainIndexDescriptor("n", string.Empty, string.Empty, string.Empty, [], false),
            new GrainIndexDescriptor(
                "long",
                new string('t', 512),
                new string('\u00e9', 200),
                "S",
                [new GrainIndexPropertyDescriptor(new string('p', 400), string.Empty)],
                false),
        ];

        foreach (var descriptor in cases)
        {
            var baseline = BaselineComputeFingerprint(descriptor, KeyCodecId);
            var optimized = GrainIndexFingerprint.Compute(descriptor, KeyCodecId).Value;
            if (!string.Equals(baseline, optimized, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Fingerprint trim changed the digest: baseline {baseline}, optimized {optimized}.");
            }
        }
    }

    // ---------------------------------------------------------------------
    // Equivalence.
    // ---------------------------------------------------------------------

    private void AssertBoundsEquivalence()
    {
        (string? Start, string? End, string? After)[] cases =
        [
            (_windowStart, _windowEnd, null),
            (null, null, null),
            // afterExclusive exactly equal to an admitted key: the single case
            // the window cannot express and the hoist must still reject.
            (_windowStart, _windowEnd, _windowStart),
            // afterExclusive below the window, so it can never fire.
            (_windowStart, _windowEnd, "aaa"),
            // Degenerate windows either side of the corpus.
            ("key:999998", "key:999999", null),
            (null, "key:000010", null),
            ("key:004000", null, null),
        ];

        foreach (var (start, end, after) in cases)
        {
            var baseline = BoundsBaseline(start, end, after);
            var optimized = BoundsOptimized(start, end, after);
            AssertSameKeys(baseline, optimized, $"bounds [{start}, {end}) after '{after}'");
        }
    }

    private void AssertSortEquivalence()
    {
        AssertSameKeys(SortBaseline(appendTail: false), SortOptimized(appendTail: false), "sort, no pending tail");
        AssertSameKeys(SortBaseline(appendTail: true), SortOptimized(appendTail: true), "sort, pending tail");
    }

    private void AssertDotScanEquivalence()
    {
        List<OrSetDot>[] corpora =
        [
            _longAddDots,
            _longSingleReplicaDots,
            _shortSingleReplicaDots,
            // Empty, single-dot, and a list whose last dot alone breaks the
            // shared-replica precondition - the case a counter-only test would
            // get wrong.
            [],
            [new OrSetDot { ReplicaId = ReplicaA, Counter = 5 }],
            [.. MakeDots(ReplicaA, 16, baseCounter: 1), new OrSetDot { ReplicaId = ReplicaB, Counter = 5 }],
        ];

        foreach (var dots in corpora)
        {
            if (!string.Equals(BaselineSingleReplica(dots), OrSetProvenanceDecoder.SingleReplica(dots), StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"SingleReplica lanes disagree on a {dots.Count}-dot list.");
            }

            // Probe a hit at each end, a counter-collision miss across replicas,
            // and an absent dot, so both the early-exit and the full scan are
            // covered.
            var probes = new List<OrSetDot>(4) { _absentDot, new OrSetDot { ReplicaId = ReplicaB, Counter = 1 } };
            if (dots.Count > 0)
            {
                probes.Add(dots[0]);
                probes.Add(dots[^1]);
            }

            foreach (var probe in probes)
            {
                if (BaselineContainsExact(dots, in probe) != OrSetProvenanceDecoder.ContainsExact(dots, in probe))
                {
                    throw new InvalidOperationException(
                        $"ContainsExact lanes disagree on a {dots.Count}-dot list for dot "
                        + $"({probe.ReplicaId}, {probe.Counter}).");
                }
            }
        }

        var baselineEvents = BaselineDecodeState(_churnedSet);
        var optimizedEvents = OrSetProvenanceDecoder.Instance.DecodeState(_churnedSet);
        if (baselineEvents.Count != optimizedEvents.Count)
        {
            throw new InvalidOperationException(
                $"DecodeState lanes disagree on count: {baselineEvents.Count} vs {optimizedEvents.Count}.");
        }

        for (var i = 0; i < baselineEvents.Count; i++)
        {
            var a = baselineEvents[i];
            var b = optimizedEvents[i];
            if (a.Kind != b.Kind
                || a.Ordinal != b.Ordinal
                || !string.Equals(a.ReplicaId, b.ReplicaId, StringComparison.Ordinal)
                || !a.Element.AsSpan().SequenceEqual(b.Element))
            {
                throw new InvalidOperationException($"DecodeState lanes disagree at index {i}.");
            }
        }
    }

    private static void AssertSameKeys(List<string> baseline, List<string> optimized, string what)
    {
        if (baseline.Count != optimized.Count)
        {
            throw new InvalidOperationException(
                $"Lanes disagree on count for {what}: {baseline.Count} vs {optimized.Count}.");
        }

        for (var i = 0; i < baseline.Count; i++)
        {
            if (!string.Equals(baseline[i], optimized[i], StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Lanes disagree at index {i} for {what}: '{baseline[i]}' vs '{optimized[i]}'.");
            }
        }
    }

    // ---------------------------------------------------------------------
    // (1) Leaf range scan bound re-tests.
    // ---------------------------------------------------------------------

    /// <summary>The mid-leaf window walk, re-testing every bound per row.</summary>
    [Benchmark]
    [BenchmarkCategory("bounds")]
    public int Bounds_Baseline_Window() => BoundsBaseline(_windowStart, _windowEnd, null).Count;

    /// <summary>The same walk carrying its bounds in the window alone.</summary>
    [Benchmark]
    [BenchmarkCategory("bounds")]
    public int Bounds_Optimized_Window() => BoundsOptimized(_windowStart, _windowEnd, null).Count;

    /// <summary>
    /// Control: the whole-leaf read, where every bound is null and the removed
    /// re-tests were only null checks.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("bounds")]
    public int Bounds_Baseline_Unbounded() => BoundsBaseline(null, null, null).Count;

    /// <summary>Optimized counterpart to <see cref="Bounds_Baseline_Unbounded"/>.</summary>
    [Benchmark]
    [BenchmarkCategory("bounds")]
    public int Bounds_Optimized_Unbounded() => BoundsOptimized(null, null, null).Count;

    /// <summary>Verbatim copy of the pre-trim per-row bound chain.</summary>
    private List<string> BoundsBaseline(string? startInclusive, string? endExclusive, string? afterExclusive)
    {
        // The window the production code hands to EnumerateRange, already
        // clipped: MaxOrdinal(startInclusive, afterExclusive) as the lower bound
        // and MinOrdinal(endExclusive, beforeExclusive, splitKey) as the upper.
        var from = MaxOrdinal(startInclusive, afterExclusive);
        var to = endExclusive;
        string? beforeExclusive = null;
        string? splitKey = null;
        const bool SplitInProgress = false;

        var sink = _sink;
        sink.Clear();
        foreach (var row in new LeafEntryCache.RangeRows(_rows, from, to))
        {
            var key = row.Key;
            if (endExclusive is not null && string.Compare(key, endExclusive, StringComparison.Ordinal) >= 0)
                break;

            if (beforeExclusive is not null && string.Compare(key, beforeExclusive, StringComparison.Ordinal) >= 0)
                break;

            if (SplitInProgress && splitKey is not null &&
                string.Compare(key, splitKey, StringComparison.Ordinal) >= 0)
                break;

            if (startInclusive is not null && string.Compare(key, startInclusive, StringComparison.Ordinal) < 0)
                continue;

            if (afterExclusive is not null && string.Compare(key, afterExclusive, StringComparison.Ordinal) <= 0)
                continue;

            if (row.Value.IsTombstone) continue;
            sink.Add(key);
        }

        return sink;
    }

    /// <summary>Verbatim copy of the shipped per-row bound chain.</summary>
    private List<string> BoundsOptimized(string? startInclusive, string? endExclusive, string? afterExclusive)
    {
        var from = MaxOrdinal(startInclusive, afterExclusive);
        var to = endExclusive;

        var sink = _sink;
        sink.Clear();
        foreach (var row in new LeafEntryCache.RangeRows(_rows, from, to))
        {
            var key = row.Key;
            if (afterExclusive is not null && string.CompareOrdinal(key, afterExclusive) <= 0)
                continue;

            if (row.Value.IsTombstone) continue;
            sink.Add(key);
        }

        return sink;
    }

    private static string? MaxOrdinal(string? a, string? b)
    {
        if (a is null) return b;
        if (b is null) return a;
        return string.CompareOrdinal(a, b) >= 0 ? a : b;
    }

    /// <summary>
    /// The bound chain in isolation, over a pre-materialised ordered key array
    /// rather than through <c>RangeRows</c>.
    /// <para>
    /// The windowed lanes above walk a <see cref="SortedDictionary{TKey,TValue}"/>
    /// red-black tree and resolve each row's visibility, and that work dwarfs
    /// the comparisons the trim removes - which is exactly why those lanes read
    /// as a wash. This pair strips the enumeration away and leaves only the
    /// chain, so the removed work is measured rather than inferred. Read it as
    /// an upper bound on what the trim can recover end-to-end, not as a claim
    /// about the read path.
    /// </para>
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("bounds")]
    public int BoundsChain_Baseline_Isolated()
    {
        var keys = _orderedKeys;
        string? startInclusive = _windowStart;
        string? endExclusive = _windowEnd;
        string? afterExclusive = null;
        string? beforeExclusive = null;
        string? splitKey = null;
        const bool SplitInProgress = false;

        var admitted = 0;
        for (var i = 0; i < keys.Length; i++)
        {
            var key = keys[i];
            if (endExclusive is not null && string.Compare(key, endExclusive, StringComparison.Ordinal) >= 0)
                break;

            if (beforeExclusive is not null && string.Compare(key, beforeExclusive, StringComparison.Ordinal) >= 0)
                break;

            if (SplitInProgress && splitKey is not null &&
                string.Compare(key, splitKey, StringComparison.Ordinal) >= 0)
                break;

            if (startInclusive is not null && string.Compare(key, startInclusive, StringComparison.Ordinal) < 0)
                continue;

            if (afterExclusive is not null && string.Compare(key, afterExclusive, StringComparison.Ordinal) <= 0)
                continue;

            admitted++;
        }

        return admitted;
    }

    /// <summary>The shipped chain over the same array.</summary>
    [Benchmark]
    [BenchmarkCategory("bounds")]
    public int BoundsChain_Optimized_Isolated()
    {
        var keys = _orderedKeys;
        string? afterExclusive = null;

        var admitted = 0;
        for (var i = 0; i < keys.Length; i++)
        {
            var key = keys[i];
            if (afterExclusive is not null && string.CompareOrdinal(key, afterExclusive) <= 0)
                continue;

            admitted++;
        }

        return admitted;
    }

    // ---------------------------------------------------------------------
    // (4) Grain-index fingerprint string feed.
    // ---------------------------------------------------------------------

    /// <summary>
    /// The pre-trim fingerprint: every type and property name walked twice, once
    /// to count its UTF-8 bytes and once to encode them, and a fixed 256-byte
    /// stack buffer zero-filled per name however short the name is.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("fingerprint")]
    public int Fingerprint_Baseline() => BaselineComputeFingerprint(_descriptor, KeyCodecId).Length;

    /// <summary>
    /// The shipped fingerprint: one transcode per name, and only the name's own
    /// worst case zero-filled.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("fingerprint")]
    public int Fingerprint_Optimized() => GrainIndexFingerprint.Compute(_descriptor, KeyCodecId).Value.Length;

    /// <summary>Verbatim copy of the pre-trim <c>Compute</c> and its feed.</summary>
    private static string BaselineComputeFingerprint(GrainIndexDescriptor descriptor, string keyCodecId)
    {
        var hasher = new XxHash128();
        Span<byte> scratch = stackalloc byte[4];

        BinaryPrimitives.WriteInt32LittleEndian(scratch, GrainIndexFingerprint.CurrentVersion);
        hasher.Append(scratch);

        BaselineFeedString(hasher, descriptor.TreeName, scratch);
        BaselineFeedString(hasher, descriptor.GrainInterfaceTypeName, scratch);
        BaselineFeedString(hasher, descriptor.StateTypeName, scratch);
        BaselineFeedString(hasher, keyCodecId, scratch);

        var properties = descriptor.Properties;
        BinaryPrimitives.WriteInt32LittleEndian(scratch, properties.Count);
        hasher.Append(scratch);

        for (var i = 0; i < properties.Count; i++)
        {
            var property = properties[i];
            BaselineFeedString(hasher, property.Name, scratch);
            BaselineFeedString(hasher, property.PropertyTypeName, scratch);
        }

        Span<byte> digest = stackalloc byte[16];
        hasher.TryGetHashAndReset(digest, out _);
        return Convert.ToHexString(digest);
    }

    /// <summary>Verbatim copy of the pre-trim two-pass feed.</summary>
    private static void BaselineFeedString(XxHash128 hasher, string value, Span<byte> scratch)
    {
        const int StackFeedLimit = 256;

        var byteCount = Encoding.UTF8.GetByteCount(value);
        BinaryPrimitives.WriteInt32LittleEndian(scratch, byteCount);
        hasher.Append(scratch);
        if (byteCount == 0)
        {
            return;
        }

        if (byteCount <= StackFeedLimit)
        {
            Span<byte> buffer = stackalloc byte[StackFeedLimit];
            var written = Encoding.UTF8.GetBytes(value, buffer);
            hasher.Append(buffer[..written]);
            return;
        }

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

    // ---------------------------------------------------------------------
    // (2) Terminal re-sort.
    // ---------------------------------------------------------------------

    /// <summary>The ordinary read - no prepared write - sorting unconditionally.</summary>
    [Benchmark]
    [BenchmarkCategory("sort")]
    public int Sort_Baseline_NoPending() => SortBaseline(appendTail: false).Count;

    /// <summary>The same read, skipping the sort the scan already earned.</summary>
    [Benchmark]
    [BenchmarkCategory("sort")]
    public int Sort_Optimized_NoPending() => SortOptimized(appendTail: false).Count;

    /// <summary>
    /// Control: a read over a prepared write, where the tail appends out of
    /// order and both arms must still sort.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("sort")]
    public int Sort_Baseline_WithPending() => SortBaseline(appendTail: true).Count;

    /// <summary>Optimized counterpart to <see cref="Sort_Baseline_WithPending"/>.</summary>
    [Benchmark]
    [BenchmarkCategory("sort")]
    public int Sort_Optimized_WithPending() => SortOptimized(appendTail: true).Count;

    private List<string> SortBaseline(bool appendTail)
    {
        var sink = _sink;
        sink.Clear();
        var ordered = _orderedKeys;
        for (var i = 0; i < ordered.Length; i++) sink.Add(ordered[i]);
        if (appendTail)
        {
            var tail = _pendingTail;
            for (var i = 0; i < tail.Length; i++) sink.Add(tail[i]);
        }

        sink.Sort(StringComparer.Ordinal);
        return sink;
    }

    private List<string> SortOptimized(bool appendTail)
    {
        var sink = _sink;
        sink.Clear();
        var ordered = _orderedKeys;
        for (var i = 0; i < ordered.Length; i++) sink.Add(ordered[i]);
        var orderedPrefix = sink.Count;
        if (appendTail)
        {
            var tail = _pendingTail;
            for (var i = 0; i < tail.Length; i++) sink.Add(tail[i]);
        }

        if (sink.Count != orderedPrefix) sink.Sort(StringComparer.Ordinal);
        return sink;
    }

    // ---------------------------------------------------------------------
    // (3) Provenance-decoder dot scans.
    // ---------------------------------------------------------------------

    /// <summary>Full-scan miss on a 32-dot add list, walked by list indexer.</summary>
    [Benchmark]
    [BenchmarkCategory("dotspan")]
    public bool ContainsExact_Baseline_Miss() => BaselineContainsExact(_longAddDots, in _absentDot);

    /// <summary>The shipped span walk over the same list.</summary>
    [Benchmark]
    [BenchmarkCategory("dotspan")]
    public bool ContainsExact_Optimized_Miss() => OrSetProvenanceDecoder.ContainsExact(_longAddDots, in _absentDot);

    /// <summary>The shared-replica precondition over a 32-dot list, by indexer.</summary>
    [Benchmark]
    [BenchmarkCategory("dotspan")]
    public string? SingleReplica_Baseline_Long() => BaselineSingleReplica(_longSingleReplicaDots);

    /// <summary>The shipped span walk over the same list.</summary>
    [Benchmark]
    [BenchmarkCategory("dotspan")]
    public string? SingleReplica_Optimized_Long() => OrSetProvenanceDecoder.SingleReplica(_longSingleReplicaDots);

    /// <summary>
    /// Control: a three-dot list, below the length at which materialising a
    /// span repays. The shipped callers gate the walk behind a threshold, so a
    /// regression here is expected and unreached in production.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dotspan")]
    public string? SingleReplica_Baseline_Short() => BaselineSingleReplica(_shortSingleReplicaDots);

    /// <summary>Optimized counterpart to <see cref="SingleReplica_Baseline_Short"/>.</summary>
    [Benchmark]
    [BenchmarkCategory("dotspan")]
    public string? SingleReplica_Optimized_Short() => OrSetProvenanceDecoder.SingleReplica(_shortSingleReplicaDots);

    /// <summary>
    /// End-to-end state decode of a churned OR-Set on the pre-span containment
    /// scan - the shape that pays the inner scan quadratically.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("dotspan")]
    public int DecodeState_Baseline_Churned() => BaselineDecodeState(_churnedSet).Count;

    /// <summary>The shipped decode of the same set.</summary>
    [Benchmark]
    [BenchmarkCategory("dotspan")]
    public int DecodeState_Optimized_Churned() => OrSetProvenanceDecoder.Instance.DecodeState(_churnedSet).Count;

    // ---------------------------------------------------------------------
    // Baseline bodies, verbatim as replaced.
    // ---------------------------------------------------------------------

    private static string? BaselineSingleReplica(List<OrSetDot> dots)
    {
        if (dots.Count == 0) return null;
        var first = dots[0].ReplicaId;
        for (var i = 1; i < dots.Count; i++)
        {
            var candidate = dots[i].ReplicaId;
            if (!ReferenceEquals(candidate, first)
                && !string.Equals(candidate, first, StringComparison.Ordinal))
            {
                return null;
            }
        }

        return first;
    }

    private static bool BaselineContainsExact(List<OrSetDot>? dots, in OrSetDot dot)
    {
        if (dots is null) return false;
        for (var i = 0; i < dots.Count; i++)
        {
            var candidate = dots[i];
            if (candidate.Counter == dot.Counter
                && string.Equals(candidate.ReplicaId, dot.ReplicaId, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Verbatim copy of <c>OrSetProvenanceDecoder.DecodeState</c> as it stood
    /// before the span walk, so the end-to-end lane pays every cost the shipped
    /// lane pays except the trim itself.
    /// </summary>
    private static List<CrdtMemberChange> BaselineDecodeState(OrSet set)
    {
        var adds = set.Adds;
        var tombstones = set.Tombstones;

        var keys = new List<string>(adds.Count + tombstones.Count);
        var total = 0;
        foreach (var (key, dots) in adds)
        {
            keys.Add(key);
            total += dots.Count;
        }

        foreach (var (key, dots) in tombstones)
        {
            total += dots.Count;
            if (!adds.ContainsKey(key)) keys.Add(key);
        }

        if (total == 0) return [];

        keys.Sort(StringComparer.Ordinal);

        var result = new List<CrdtMemberChange>(total);
        foreach (var key in keys)
        {
            var element = Convert.FromBase64String(key);
            var start = result.Count;

            if (adds.TryGetValue(key, out var addDots))
            {
                for (var i = 0; i < addDots.Count; i++)
                {
                    var dot = addDots[i];
                    result.Add(new CrdtMemberChange
                    {
                        Element = element,
                        Kind = CrdtMemberChangeKind.Added,
                        ReplicaId = dot.ReplicaId,
                        Ordinal = dot.Counter,
                        WallClock = null,
                    });
                }
            }

            if (tombstones.TryGetValue(key, out var tombDots))
            {
                for (var i = 0; i < tombDots.Count; i++)
                {
                    var dot = tombDots[i];
                    if (addDots is not null && !BaselineContainsExact(addDots, in dot))
                    {
                        result.Add(new CrdtMemberChange
                        {
                            Element = element,
                            Kind = CrdtMemberChangeKind.Added,
                            ReplicaId = dot.ReplicaId,
                            Ordinal = dot.Counter,
                            WallClock = null,
                        });
                    }

                    result.Add(new CrdtMemberChange
                    {
                        Element = element,
                        Kind = CrdtMemberChangeKind.Removed,
                        ReplicaId = dot.ReplicaId,
                        Ordinal = dot.Counter,
                        WallClock = null,
                    });
                }
            }

            result.Sort(start, result.Count - start, BaselineCausalOrderComparer.Instance);
        }

        return result;
    }

    private sealed class BaselineCausalOrderComparer : IComparer<CrdtMemberChange>
    {
        public static BaselineCausalOrderComparer Instance { get; } = new();

        public int Compare(CrdtMemberChange x, CrdtMemberChange y)
        {
            var byOrdinal = x.Ordinal.CompareTo(y.Ordinal);
            if (byOrdinal != 0) return byOrdinal;
            var byReplica = string.CompareOrdinal(x.ReplicaId, y.ReplicaId);
            if (byReplica != 0) return byReplica;
            return ((int)x.Kind).CompareTo((int)y.Kind);
        }
    }

    // ---------------------------------------------------------------------
    // Corpora.
    // ---------------------------------------------------------------------

    private static List<OrSetDot> MakeDots(string replica, int count, long baseCounter)
    {
        var dots = new List<OrSetDot>(count);
        for (var i = 0; i < count; i++)
        {
            dots.Add(new OrSetDot { ReplicaId = replica, Counter = baseCounter + i });
        }

        return dots;
    }

    /// <summary>
    /// A set whose elements each carry a compacted add list and a long
    /// tombstone list, so every tombstone dot drives a full containment scan -
    /// the quadratic shape the span walk targets. The tombstone lists sit above
    /// <see cref="BaselineDotIndexThreshold"/> so the shipped gated walks are
    /// reached.
    /// </summary>
    private static OrSet MakeChurnedOrSet(int elements, int addsPerElement, int tombstonesPerElement)
    {
        if (tombstonesPerElement <= BaselineDotIndexThreshold)
        {
            throw new ArgumentOutOfRangeException(
                nameof(tombstonesPerElement),
                "The churned corpus must clear the decoder's dot-index threshold.");
        }

        var set = new OrSet();
        for (var e = 0; e < elements; e++)
        {
            var key = Convert.ToBase64String(BitConverter.GetBytes(e));
            set.Adds[key] = MakeDots(ReplicaA, addsPerElement, baseCounter: 100);
            set.Tombstones[key] = MakeDots(ReplicaA, tombstonesPerElement, baseCounter: 1);
        }

        return set;
    }
}
