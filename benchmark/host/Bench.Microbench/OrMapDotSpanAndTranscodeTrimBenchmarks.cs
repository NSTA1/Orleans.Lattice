using System.Buffers;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Arbitrates three independent read-path trims. Every group carries a baseline
/// lane holding a verbatim copy of the replaced body, an optimized lane running
/// the shipped body, and a control lane on the input shape where the trim is
/// expected to buy nothing - so a win that is really measurement noise has
/// somewhere to show itself.
/// <para>
/// <b>(1) ormapspan - the OR-map provenance decoder's dot scans walked by list
/// indexer.</b> <see cref="OrSetDot"/> is a struct, so <c>list[i]</c> copies it,
/// bounds-checks the access and re-reads the mutable <c>Count</c> on every
/// iteration. <c>OrMapProvenanceDecoder.IsEntryTombstoned</c> is the inner scan
/// of the per-key membership test, so a key that misses the counter index pays
/// it once per add dot - quadratically in the churned shape.
/// <c>SingleReplica</c> and the counter-index fill run only on lists already
/// longer than the decoder's tombstone threshold. Each body only reads, so no
/// scanned list's length can change while a span over it is alive.
/// </para>
/// <para>
/// <b>(2) setspan - the same technique on the OR-set and RW-set decoders'
/// residual emit loops.</b> The containment scans in both decoders were spanned
/// earlier; their state-decode emit loops (per element, once per add dot and
/// once per remove or tombstone dot) were not, and they are the loops that run
/// on every element of every decode rather than only above a threshold. They
/// append to a different list than the one they scan, so the span is safe.
/// </para>
/// <para>
/// <b>(3) transcode - two remaining double transcodes of the same string.</b>
/// <c>WalMaterialiserPinRouting.StableHash</c> and the blob cache's key map each
/// ran <c>Encoding.UTF8.GetByteCount</c> purely to size or select a buffer and
/// then <c>GetBytes</c> to fill it - two full scans of the same string on a hot
/// routing path. When the string's own worst case already fits the stack budget
/// the count is not needed at all: encode once and let the encoder's written
/// count bound the fold, which is by definition the number the count pass would
/// have returned. A longer string keeps the two-pass shape, because renting for
/// a long-but-mostly-ASCII string costs more than the scan it saves. The WAL
/// site additionally drops a per-call <c>new byte[]</c> for a pooled rent on its
/// oversized path.
/// </para>
/// <para>
/// <b>How to read the columns.</b> Time is the arbiter; allocation is reported
/// because only one trim is allowed to move it. Groups (1) and (2) must allocate
/// identically in both arms - they build the same event lists - so a Gen0 delta
/// there is a defect, not a result. In group (3) the WAL oversized lane is the
/// single place bytes may legitimately fall, because that is the
/// <c>new byte[]</c> the pooled rent replaced.
/// </para>
/// <para>
/// <b>Controls.</b> <c>SingleReplica_*_Short</c> is a three-dot list, far below
/// the length at which materialising a span could repay.
/// <c>OrSetDecodeState_*_Flat</c> is a set of single-dot elements, where every
/// spanned loop runs exactly one iteration and the span setup is pure overhead.
/// <c>BlobName_*_LongKey</c> is a key whose worst case overflows the stack
/// budget, so both arms take the identical two-pass path.
/// </para>
/// <para>
/// <b><c>StableHash_*_LongAscii</c> is not a control, despite reading like
/// one.</b> It was written as one - a string whose worst case overflows the
/// stack budget, so both arms were expected to take the identical two-pass
/// path - but the two passes are not identical. Both arms measure the exact
/// byte count and both land inside the budget, yet the baseline then
/// stack-allocates <i>that count</i> while the shipped body stack-allocates the
/// fixed <see cref="BaselineStackTranscodeBytes"/>. A constant-size
/// <c>stackalloc</c> lowers to a fixed stack adjustment with unrolled zeroing;
/// a variable-size one needs a runtime stack probe. So the lane measures a real
/// second effect rather than noise, and its delta must be read as a result.
/// </para>
/// <para>
/// <b>Why the spanned loops do not all read through <c>ref readonly</c>.</b>
/// The first revision of this suite arbitrated a version that did, and the
/// pure-scan lanes won handsomely while every lane whose loop body makes a call
/// regressed - the end-to-end OR-map decode and the flat OR-set control most
/// visibly. A byref into the span that is live across a call is an interior
/// pointer the JIT must report to the GC, so it is pinned to a tracked stack
/// slot rather than enregistered, and that costs more than the 16-byte struct
/// copy it saves. The shipped bodies therefore read through <c>ref readonly</c>
/// only in loops that call nothing, and copy the element everywhere else. Both
/// shapes still take the span, so both still drop the <c>Count</c> re-read.
/// </para>
/// <para>
/// Run with <c>--suite ormapdotspantranscode</c> (or
/// <c>BENCH_MICROBENCH_SUITE=ormapdotspantranscode</c>). This machine is noisy:
/// treat smoke fidelity as an equivalence and direction check only and take
/// <c>BENCH_MICROBENCH_FIDELITY=full</c> as the arbiter.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class OrMapDotSpanAndTranscodeTrimBenchmarks
{
    /// <summary>
    /// Mirrors the private <c>OrMapProvenanceDecoder.TombstoneIndexThreshold</c>,
    /// so the long-list lanes sit where the shipped callers actually take the
    /// gated walks and the short-list control sits well below it.
    /// </summary>
    private const int BaselineTombstoneIndexThreshold = 8;

    /// <summary>
    /// Mirrors the private <c>WalMaterialiserPinRouting.StackTranscodeBytes</c>.
    /// </summary>
    private const int BaselineStackTranscodeBytes = 256;

    /// <summary>
    /// Mirrors the private <c>BlobCacheKeyMap.StackHashThresholdBytes</c>. The
    /// blob cache package is not referenced by this host, so both arms of the
    /// blob group are mirrors of the two bodies rather than one shipped call;
    /// they are copied byte for byte from the package and the equivalence
    /// assertion below pins the mirrors to the same digest.
    /// </summary>
    private const int BaselineStackHashThresholdBytes = 512;

    private const int BaselineStackHashThresholdChars = BaselineStackHashThresholdBytes / 3;

    private const string ReplicaA = "replica-a";
    private const string ReplicaB = "replica-b";

    // Group 1 - OR-map decoder dot scans.
    private List<OrSetDot> _longTombstones = null!;
    private List<OrSetDot> _shortTombstones = null!;
    private List<OrSetDot> _mixedReplicaTombstones = null!;
    private OrMap<string, OrFlag> _churnedMap = null!;

    // Group 2 - OR-set and RW-set decoder emit loops.
    private OrSet _churnedSet = null!;
    private OrSet _flatSet = null!;
    private RwSet _churnedRwSet = null!;

    // Group 3 - transcode.
    private string _shortConsumerId = null!;
    private string _multibyteConsumerId = null!;
    private string _longAsciiConsumerId = null!;
    private string _shortCacheKey = null!;
    private string _longCacheKey = null!;

    /// <summary>
    /// Builds every corpus and asserts each optimized lane answers exactly what
    /// its baseline answers - on the ordinary shapes, on the shapes where the
    /// trims must buy nothing, on inputs that violate each gated trim's
    /// precondition, and on the degenerate empty and multi-byte strings where a
    /// transcode trim is most likely to disagree.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _longTombstones = MakeDots(ReplicaA, 32, 1);
        _shortTombstones = MakeDots(ReplicaA, 3, 1);

        // Violates the shared-replica precondition the counter index is gated
        // on, so SingleReplica must return null on both arms.
        _mixedReplicaTombstones = MakeDots(ReplicaA, 16, 1);
        _mixedReplicaTombstones.AddRange(MakeDots(ReplicaB, 16, 1));

        _churnedMap = MakeChurnedOrMap(elements: 24, churnPerElement: 20);

        _churnedSet = MakeChurnedOrSet(elements: 24, addsPerElement: 2, tombstonesPerElement: 16);
        _flatSet = MakeFlatOrSet(elements: 128);
        _churnedRwSet = MakeChurnedRwSet(elements: 24, addsPerElement: 4, removesPerElement: 4);

        _shortConsumerId = "materialiser:tree-alpha:view-index:consumer-07";
        _multibyteConsumerId = "materialiser:\u00e9\u00e8\u00ea-tr\u00e9e:\u4e2d\u6587\u7d22\u5f15:07";
        // 220 ASCII chars: the exact count fits the 256-byte stack budget but
        // the worst case (3 bytes per char) does not, so this is the shape the
        // trim deliberately leaves on the two-pass path.
        _longAsciiConsumerId = new string('c', 220);

        _shortCacheKey = "tenant:acme/region:westeurope/entity:order/id:8f2c1d40";
        _longCacheKey = new string('k', 400);

        AssertOrMapEquivalence();
        AssertSetEquivalence();
        AssertTranscodeEquivalence();
    }

    private void AssertOrMapEquivalence()
    {
        AssertEqual(
            BaselineSingleReplica(_longTombstones),
            OrMapProvenanceDecoder.SingleReplica(_longTombstones),
            "OrMap SingleReplica (long, single replica)");
        AssertEqual(
            BaselineSingleReplica(_shortTombstones),
            OrMapProvenanceDecoder.SingleReplica(_shortTombstones),
            "OrMap SingleReplica (short)");
        AssertEqual(
            BaselineSingleReplica(_mixedReplicaTombstones),
            OrMapProvenanceDecoder.SingleReplica(_mixedReplicaTombstones),
            "OrMap SingleReplica (precondition violated)");
        AssertEqual(
            BaselineSingleReplica([]),
            OrMapProvenanceDecoder.SingleReplica([]),
            "OrMap SingleReplica (empty)");

        // Hit, miss, wrong-counter and wrong-replica: the four outcomes the
        // membership scan has to keep apart.
        AssertEqual(
            BaselineIsEntryTombstoned(_longTombstones, ReplicaA, 17),
            OrMapProvenanceDecoder.IsEntryTombstoned(_longTombstones, ReplicaA, 17),
            "OrMap IsEntryTombstoned (hit)");
        AssertEqual(
            BaselineIsEntryTombstoned(_longTombstones, ReplicaA, 9999),
            OrMapProvenanceDecoder.IsEntryTombstoned(_longTombstones, ReplicaA, 9999),
            "OrMap IsEntryTombstoned (counter miss)");
        AssertEqual(
            BaselineIsEntryTombstoned(_longTombstones, ReplicaB, 17),
            OrMapProvenanceDecoder.IsEntryTombstoned(_longTombstones, ReplicaB, 17),
            "OrMap IsEntryTombstoned (replica miss)");
        AssertEqual(
            BaselineIsEntryTombstoned(null, ReplicaA, 1),
            OrMapProvenanceDecoder.IsEntryTombstoned(null, ReplicaA, 1),
            "OrMap IsEntryTombstoned (null list)");

        AssertSameMembers(
            BaselineDecodeCurrentValue(_churnedMap),
            OrMapProvenanceDecoder.Instance.DecodeCurrentValue(_churnedMap),
            "OrMap DecodeCurrentValue (churned)");
        AssertSameMembers(
            BaselineDecodeCurrentValue(new OrMap<string, OrFlag>()),
            OrMapProvenanceDecoder.Instance.DecodeCurrentValue(new OrMap<string, OrFlag>()),
            "OrMap DecodeCurrentValue (empty)");
    }

    private void AssertSetEquivalence()
    {
        AssertSameChanges(
            BaselineOrSetDecodeState(_churnedSet),
            OrSetProvenanceDecoder.Instance.DecodeState(_churnedSet),
            "OrSet DecodeState (churned)");
        AssertSameChanges(
            BaselineOrSetDecodeState(_flatSet),
            OrSetProvenanceDecoder.Instance.DecodeState(_flatSet),
            "OrSet DecodeState (flat)");
        AssertSameChanges(
            BaselineOrSetDecodeState(new OrSet()),
            OrSetProvenanceDecoder.Instance.DecodeState(new OrSet()),
            "OrSet DecodeState (empty)");

        AssertSameChanges(
            BaselineRwSetDecodeState(_churnedRwSet),
            RwSetProvenanceDecoder.Instance.DecodeState(_churnedRwSet),
            "RwSet DecodeState (churned)");
        AssertSameChanges(
            BaselineRwSetDecodeState(new RwSet()),
            RwSetProvenanceDecoder.Instance.DecodeState(new RwSet()),
            "RwSet DecodeState (empty)");

        // The RW-set current value exercises the remove-scan the trim also
        // touches, and is the projection a reader actually sees.
        AssertSameMembers(
            RwSetProvenanceDecoder.Instance.DecodeCurrentValue(_churnedRwSet),
            RwSetProvenanceDecoder.Instance.DecodeCurrentValue(_churnedRwSet.Clone()),
            "RwSet DecodeCurrentValue (stable across clone)");
    }

    private void AssertTranscodeEquivalence()
    {
        string[] hashCorpus =
        [
            string.Empty,
            "a",
            _shortConsumerId,
            _multibyteConsumerId,
            _longAsciiConsumerId,
            new string('x', 512),
            "\U0001F600 surrogate pair leading the id",
        ];

        foreach (var value in hashCorpus)
        {
            AssertEqual(
                BaselineStableHash(value),
                WalMaterialiserPinRouting.StableHash(value),
                "StableHash(\"" + Describe(value) + "\")");
            AssertEqual(
                BaselineToBlobName("cache/", value),
                OptimizedToBlobName("cache/", value),
                "ToBlobName(\"" + Describe(value) + "\")");
        }

        AssertEqual(
            BaselineToBlobName(string.Empty, _shortCacheKey),
            OptimizedToBlobName(string.Empty, _shortCacheKey),
            "ToBlobName (no prefix)");
        AssertEqual(
            BaselineToBlobName("cache/", _longCacheKey),
            OptimizedToBlobName("cache/", _longCacheKey),
            "ToBlobName (long key)");
    }

    private static string Describe(string value) =>
        value.Length == 0 ? "<empty>" : value.Length + " chars";

    private static void AssertEqual<T>(T baseline, T optimized, string what)
    {
        if (!EqualityComparer<T>.Default.Equals(baseline, optimized))
        {
            throw new InvalidOperationException(
                $"{what}: optimized returned '{optimized}' but baseline returned '{baseline}'.");
        }
    }

    private static void AssertSameChanges(
        IReadOnlyList<CrdtMemberChange> baseline,
        IReadOnlyList<CrdtMemberChange> optimized,
        string what)
    {
        if (baseline.Count != optimized.Count)
        {
            throw new InvalidOperationException(
                $"{what}: {optimized.Count} events but baseline produced {baseline.Count}.");
        }

        for (var i = 0; i < baseline.Count; i++)
        {
            var b = baseline[i];
            var o = optimized[i];
            if (b.Kind != o.Kind
                || b.Ordinal != o.Ordinal
                || !string.Equals(b.ReplicaId, o.ReplicaId, StringComparison.Ordinal)
                || !b.Element.AsSpan().SequenceEqual(o.Element))
            {
                throw new InvalidOperationException($"{what}: event {i} differs from baseline.");
            }
        }
    }

    private static void AssertSameMembers(
        IReadOnlyList<CrdtMemberValue> baseline,
        IReadOnlyList<CrdtMemberValue> optimized,
        string what)
    {
        if (baseline.Count != optimized.Count)
        {
            throw new InvalidOperationException(
                $"{what}: {optimized.Count} members but baseline produced {baseline.Count}.");
        }

        for (var i = 0; i < baseline.Count; i++)
        {
            var b = baseline[i];
            var o = optimized[i];
            if (b.Ordinal != o.Ordinal
                || !string.Equals(b.ReplicaId, o.ReplicaId, StringComparison.Ordinal)
                || !b.Element.AsSpan().SequenceEqual(o.Element))
            {
                throw new InvalidOperationException($"{what}: member {i} differs from baseline.");
            }
        }
    }

    // ---------------------------------------------------------------------
    // (1) OR-map decoder dot scans.
    // ---------------------------------------------------------------------

    /// <summary>Full-scan miss on a 32-dot tombstone list, walked by indexer.</summary>
    [Benchmark]
    [BenchmarkCategory("ormapspan")]
    public bool IsEntryTombstoned_Baseline_Miss()
        => BaselineIsEntryTombstoned(_longTombstones, ReplicaA, 9999);

    /// <summary>The shipped span walk over the same list.</summary>
    [Benchmark]
    [BenchmarkCategory("ormapspan")]
    public bool IsEntryTombstoned_Optimized_Miss()
        => OrMapProvenanceDecoder.IsEntryTombstoned(_longTombstones, ReplicaA, 9999);

    /// <summary>The shared-replica precondition over a 32-dot list, by indexer.</summary>
    [Benchmark]
    [BenchmarkCategory("ormapspan")]
    public string? SingleReplica_Baseline_Long() => BaselineSingleReplica(_longTombstones);

    /// <summary>The shipped span walk over the same list.</summary>
    [Benchmark]
    [BenchmarkCategory("ormapspan")]
    public string? SingleReplica_Optimized_Long() => OrMapProvenanceDecoder.SingleReplica(_longTombstones);

    /// <summary>
    /// Control: a three-dot list, far below the tombstone index threshold the
    /// shipped callers gate this walk behind, so the span setup has almost
    /// nothing to amortise over.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("ormapspan")]
    public string? SingleReplica_Baseline_Short() => BaselineSingleReplica(_shortTombstones);

    /// <summary>Optimized counterpart to <see cref="SingleReplica_Baseline_Short"/>.</summary>
    [Benchmark]
    [BenchmarkCategory("ormapspan")]
    public string? SingleReplica_Optimized_Short() => OrMapProvenanceDecoder.SingleReplica(_shortTombstones);

    /// <summary>
    /// End-to-end current-value projection of a churned OR-map on the pre-span
    /// bodies - the shape that pays the membership scan per add dot.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("ormapspan")]
    public int DecodeCurrentValue_Baseline_Churned() => BaselineDecodeCurrentValue(_churnedMap).Count;

    /// <summary>The shipped projection of the same map.</summary>
    [Benchmark]
    [BenchmarkCategory("ormapspan")]
    public int DecodeCurrentValue_Optimized_Churned()
        => OrMapProvenanceDecoder.Instance.DecodeCurrentValue(_churnedMap).Count;

    // ---------------------------------------------------------------------
    // (2) OR-set and RW-set decoder emit loops.
    // ---------------------------------------------------------------------

    /// <summary>State decode of a churned OR-set on the pre-span emit loops.</summary>
    [Benchmark]
    [BenchmarkCategory("setspan")]
    public int OrSetDecodeState_Baseline_Churned() => BaselineOrSetDecodeState(_churnedSet).Count;

    /// <summary>The shipped decode of the same set.</summary>
    [Benchmark]
    [BenchmarkCategory("setspan")]
    public int OrSetDecodeState_Optimized_Churned()
        => OrSetProvenanceDecoder.Instance.DecodeState(_churnedSet).Count;

    /// <summary>
    /// Control: 128 elements each holding exactly one add dot, so every spanned
    /// loop runs a single iteration and the span setup is pure overhead.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("setspan")]
    public int OrSetDecodeState_Baseline_Flat() => BaselineOrSetDecodeState(_flatSet).Count;

    /// <summary>Optimized counterpart to <see cref="OrSetDecodeState_Baseline_Flat"/>.</summary>
    [Benchmark]
    [BenchmarkCategory("setspan")]
    public int OrSetDecodeState_Optimized_Flat()
        => OrSetProvenanceDecoder.Instance.DecodeState(_flatSet).Count;

    /// <summary>State decode of a churned RW-set on the pre-span emit loops.</summary>
    [Benchmark]
    [BenchmarkCategory("setspan")]
    public int RwSetDecodeState_Baseline_Churned() => BaselineRwSetDecodeState(_churnedRwSet).Count;

    /// <summary>The shipped decode of the same set.</summary>
    [Benchmark]
    [BenchmarkCategory("setspan")]
    public int RwSetDecodeState_Optimized_Churned()
        => RwSetProvenanceDecoder.Instance.DecodeState(_churnedRwSet).Count;

    // ---------------------------------------------------------------------
    // (3) Transcode.
    // ---------------------------------------------------------------------

    /// <summary>A typical consumer id, double-transcoded.</summary>
    [Benchmark]
    [BenchmarkCategory("transcode")]
    public uint StableHash_Baseline_Short() => BaselineStableHash(_shortConsumerId);

    /// <summary>The shipped single-pass transcode of the same id.</summary>
    [Benchmark]
    [BenchmarkCategory("transcode")]
    public uint StableHash_Optimized_Short() => WalMaterialiserPinRouting.StableHash(_shortConsumerId);

    /// <summary>A consumer id carrying multi-byte scalars, double-transcoded.</summary>
    [Benchmark]
    [BenchmarkCategory("transcode")]
    public uint StableHash_Baseline_Multibyte() => BaselineStableHash(_multibyteConsumerId);

    /// <summary>The shipped single-pass transcode of the same id.</summary>
    [Benchmark]
    [BenchmarkCategory("transcode")]
    public uint StableHash_Optimized_Multibyte() => WalMaterialiserPinRouting.StableHash(_multibyteConsumerId);

    /// <summary>
    /// Control: 220 ASCII chars, whose worst case overflows the 256-byte stack
    /// budget, so the shipped body deliberately keeps the two-pass shape and
    /// both arms should be indistinguishable.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("transcode")]
    public uint StableHash_Baseline_LongAscii() => BaselineStableHash(_longAsciiConsumerId);

    /// <summary>Optimized counterpart to <see cref="StableHash_Baseline_LongAscii"/>.</summary>
    [Benchmark]
    [BenchmarkCategory("transcode")]
    public uint StableHash_Optimized_LongAscii() => WalMaterialiserPinRouting.StableHash(_longAsciiConsumerId);

    /// <summary>
    /// The oversized path: 512 ASCII chars, where the baseline allocates a
    /// fresh <c>byte[]</c> per call and the shipped body rents from the pool.
    /// This is the one lane in the suite where allocation is allowed to fall.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("transcode")]
    public uint StableHash_Baseline_Oversized() => BaselineStableHash(_longCacheKey);

    /// <summary>Optimized counterpart to <see cref="StableHash_Baseline_Oversized"/>.</summary>
    [Benchmark]
    [BenchmarkCategory("transcode")]
    public uint StableHash_Optimized_Oversized() => WalMaterialiserPinRouting.StableHash(_longCacheKey);

    /// <summary>A typical cache key, with the buffer-selecting count pass.</summary>
    [Benchmark]
    [BenchmarkCategory("transcode")]
    public string BlobName_Baseline_Short() => BaselineToBlobName("cache/", _shortCacheKey);

    /// <summary>The same key with the count pass dropped.</summary>
    [Benchmark]
    [BenchmarkCategory("transcode")]
    public string BlobName_Optimized_Short() => OptimizedToBlobName("cache/", _shortCacheKey);

    /// <summary>
    /// Control: a 400-char key, past the char threshold at which the shipped
    /// body still has to take the count, so both arms run the same two passes.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("transcode")]
    public string BlobName_Baseline_LongKey() => BaselineToBlobName("cache/", _longCacheKey);

    /// <summary>Optimized counterpart to <see cref="BlobName_Baseline_LongKey"/>.</summary>
    [Benchmark]
    [BenchmarkCategory("transcode")]
    public string BlobName_Optimized_LongKey() => OptimizedToBlobName("cache/", _longCacheKey);

    // ---------------------------------------------------------------------
    // Baseline bodies, verbatim as replaced.
    // ---------------------------------------------------------------------

    private static string? BaselineSingleReplica(List<OrSetDot> tombstones)
    {
        if (tombstones.Count == 0) return null;
        var first = tombstones[0].ReplicaId;
        for (var i = 1; i < tombstones.Count; i++)
        {
            var candidate = tombstones[i].ReplicaId;
            if (!ReferenceEquals(candidate, first)
                && !string.Equals(candidate, first, StringComparison.Ordinal))
            {
                return null;
            }
        }

        return first;
    }

    private static bool BaselineIsEntryTombstoned(List<OrSetDot>? tombstones, string replicaId, long counter)
    {
        if (tombstones is null) return false;
        for (var i = 0; i < tombstones.Count; i++)
        {
            var t = tombstones[i];
            if (t.Counter == counter && string.Equals(t.ReplicaId, replicaId, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Verbatim copy of <c>OrMapProvenanceDecoder.EmitCurrentValueTyped</c> as
    /// it stood before the span walks, closed over the corpus's own type
    /// arguments (the shipped path reaches it through a cached delegate, so the
    /// reflection is not part of either arm's measured work) and wrapped in the
    /// public entry point's sink allocation and terminal sort.
    /// </summary>
    private static List<CrdtMemberValue> BaselineDecodeCurrentValue(OrMap<string, OrFlag> map)
    {
        var sink = new List<CrdtMemberValue>();
        sink.EnsureCapacity(sink.Count + map.Adds.Count);

        long[]? rented = null;

        foreach (var (key, entries) in map.Adds)
        {
            if (entries.Count == 0) continue;
            map.Tombstones.TryGetValue(key, out var tomb);

            string? sharedReplica = null;
            var counters = Span<long>.Empty;
            if (tomb is not null
                && tomb.Count > BaselineTombstoneIndexThreshold
                && entries.Count > 1)
            {
                sharedReplica = BaselineSingleReplica(tomb);
                if (sharedReplica is not null)
                {
                    if (rented is null || rented.Length < tomb.Count)
                    {
                        if (rented is not null) ArrayPool<long>.Shared.Return(rented);
                        rented = ArrayPool<long>.Shared.Rent(tomb.Count);
                    }

                    counters = rented.AsSpan(0, tomb.Count);
                    for (var i = 0; i < tomb.Count; i++) counters[i] = tomb[i].Counter;
                    counters.Sort();
                }
            }

            var hasLive = false;
            var bestReplica = string.Empty;
            var bestCounter = long.MinValue;
            for (var i = 0; i < entries.Count; i++)
            {
                var entry = entries[i];
                var tombstoned = sharedReplica is not null
                    ? string.Equals(entry.ReplicaId, sharedReplica, StringComparison.Ordinal)
                        && counters.BinarySearch(entry.Counter) >= 0
                    : BaselineIsEntryTombstoned(tomb, entry.ReplicaId, entry.Counter);
                if (tombstoned) continue;
                if (!hasLive
                    || entry.Counter > bestCounter
                    || (entry.Counter == bestCounter && string.CompareOrdinal(entry.ReplicaId, bestReplica) > 0))
                {
                    hasLive = true;
                    bestReplica = entry.ReplicaId;
                    bestCounter = entry.Counter;
                }
            }

            if (!hasLive) continue;
            sink.Add(new CrdtMemberValue
            {
                Element = key.Length == 0 ? [] : Encoding.UTF8.GetBytes(key),
                ReplicaId = bestReplica,
                Ordinal = bestCounter,
            });
        }

        if (rented is not null) ArrayPool<long>.Shared.Return(rented);

        if (sink.Count == 0) return sink;
        sink.Sort(static (x, y) => BaselineCompareElementBytes(x.Element, y.Element));
        return sink;
    }

    private static int BaselineCompareElementBytes(byte[] a, byte[] b)
    {
        var min = Math.Min(a.Length, b.Length);
        for (var i = 0; i < min; i++)
        {
            var c = a[i].CompareTo(b[i]);
            if (c != 0) return c;
        }

        return a.Length.CompareTo(b.Length);
    }

    /// <summary>
    /// Verbatim copy of <c>OrSetProvenanceDecoder.DecodeState</c> as it stood
    /// before the emit loops were spanned. It calls the shipped
    /// <c>ContainsExact</c>, which was spanned by an earlier change, so this
    /// lane isolates exactly the loops this change touched.
    /// </summary>
    private static List<CrdtMemberChange> BaselineOrSetDecodeState(OrSet set)
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
                    if (addDots is not null && !OrSetProvenanceDecoder.ContainsExact(addDots, in dot))
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

            result.Sort(start, result.Count - start, BaselineCausalComparer.Instance);
        }

        return result;
    }

    /// <summary>
    /// Verbatim copy of <c>RwSetProvenanceDecoder.DecodeState</c> as it stood
    /// before the emit loops were spanned.
    /// </summary>
    private static List<CrdtMemberChange> BaselineRwSetDecodeState(RwSet set)
    {
        var adds = set.Adds;
        var removes = set.Removes;

        var keys = new List<string>(adds.Count + removes.Count);
        var total = 0;
        foreach (var (key, dots) in adds)
        {
            keys.Add(key);
            total += dots.Count;
        }

        foreach (var (key, dots) in removes)
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

            if (removes.TryGetValue(key, out var removeDots))
            {
                for (var i = 0; i < removeDots.Count; i++)
                {
                    var dot = removeDots[i];
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

            result.Sort(start, result.Count - start, BaselineCausalComparer.Instance);
        }

        return result;
    }

    private sealed class BaselineCausalComparer : IComparer<CrdtMemberChange>
    {
        public static BaselineCausalComparer Instance { get; } = new();

        public int Compare(CrdtMemberChange x, CrdtMemberChange y)
        {
            var byOrdinal = x.Ordinal.CompareTo(y.Ordinal);
            if (byOrdinal != 0) return byOrdinal;
            var byReplica = string.CompareOrdinal(x.ReplicaId, y.ReplicaId);
            if (byReplica != 0) return byReplica;
            return ((int)x.Kind).CompareTo((int)y.Kind);
        }
    }

    /// <summary>
    /// Verbatim copy of <c>WalMaterialiserPinRouting.StableHash</c> as it stood
    /// before the single-pass transcode, including the per-call <c>byte[]</c>
    /// its oversized path allocated.
    /// </summary>
    private static uint BaselineStableHash(string value)
    {
        const uint offsetBasis = 2166136261;
        const uint prime = 16777619;

        var hash = offsetBasis;
        var byteCount = Encoding.UTF8.GetByteCount(value);
        Span<byte> buffer = byteCount <= BaselineStackTranscodeBytes
            ? stackalloc byte[byteCount]
            : new byte[byteCount];
        Encoding.UTF8.GetBytes(value, buffer);
        for (var i = 0; i < buffer.Length; i++)
        {
            hash ^= buffer[i];
            hash *= prime;
        }

        return hash;
    }

    /// <summary>
    /// Verbatim copy of <c>BlobCacheKeyMap.ToBlobName</c> as it stood before the
    /// count pass was dropped. The blob cache package is not referenced by this
    /// host, so both arms of this pair are mirrors; the equivalence assertion
    /// pins them to the same digest for every corpus string.
    /// </summary>
    private static string BaselineToBlobName(string keyPrefix, string key)
    {
        Span<byte> digest = stackalloc byte[SHA256.HashSizeInBytes];
        var byteCount = Encoding.UTF8.GetByteCount(key);

        byte[]? rented = null;
        try
        {
            Span<byte> buffer = byteCount <= BaselineStackHashThresholdBytes
                ? stackalloc byte[BaselineStackHashThresholdBytes]
                : (rented = ArrayPool<byte>.Shared.Rent(byteCount));

            var written = Encoding.UTF8.GetBytes(key, buffer);
            SHA256.HashData(buffer[..written], digest);
        }
        finally
        {
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        }

        var hex = Convert.ToHexStringLower(digest);
        return keyPrefix.Length == 0 ? hex : string.Concat(keyPrefix, hex);
    }

    /// <summary>Mirror of the shipped <c>BlobCacheKeyMap.ToBlobName</c>.</summary>
    private static string OptimizedToBlobName(string keyPrefix, string key)
    {
        Span<byte> digest = stackalloc byte[SHA256.HashSizeInBytes];

        var byteCount = key.Length <= BaselineStackHashThresholdChars
            ? -1
            : Encoding.UTF8.GetByteCount(key);

        byte[]? rented = null;
        try
        {
            Span<byte> buffer = byteCount <= BaselineStackHashThresholdBytes
                ? stackalloc byte[BaselineStackHashThresholdBytes]
                : (rented = ArrayPool<byte>.Shared.Rent(byteCount));

            var written = Encoding.UTF8.GetBytes(key, buffer);
            SHA256.HashData(buffer[..written], digest);
        }
        finally
        {
            if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
        }

        var hex = Convert.ToHexStringLower(digest);
        return keyPrefix.Length == 0 ? hex : string.Concat(keyPrefix, hex);
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
    /// A map whose keys have each been set and removed repeatedly, so every key
    /// carries a tombstone list above the decoder's index threshold and a live
    /// add from a second replica - the shape that drives the membership scan and
    /// the gated counter-index fill.
    /// </summary>
    private static OrMap<string, OrFlag> MakeChurnedOrMap(int elements, int churnPerElement)
    {
        if (churnPerElement <= BaselineTombstoneIndexThreshold)
        {
            throw new ArgumentOutOfRangeException(
                nameof(churnPerElement),
                churnPerElement,
                "The corpus must churn past the tombstone index threshold, or the gated walks are never reached.");
        }

        var left = new OrMap<string, OrFlag>();
        var right = new OrMap<string, OrFlag>();
        for (var e = 0; e < elements; e++)
        {
            var key = "key:" + e.ToString("D4", CultureInfo.InvariantCulture);
            for (var i = 0; i < churnPerElement; i++)
            {
                left.Set(key, ReplicaA, new OrFlag());
            }

            left.Remove(key);
            for (var i = 0; i < 3; i++) right.Set(key, ReplicaB, new OrFlag());
        }

        return OrMap<string, OrFlag>.Merge(left, right);
    }

    /// <summary>
    /// A set whose elements each carry a compacted add list and a long tombstone
    /// list, so both emit loops run many iterations per element.
    /// </summary>
    private static OrSet MakeChurnedOrSet(int elements, int addsPerElement, int tombstonesPerElement)
    {
        var set = new OrSet();
        var counter = 1L;
        for (var e = 0; e < elements; e++)
        {
            var element = Encoding.UTF8.GetBytes("element:" + e.ToString("D4", CultureInfo.InvariantCulture));
            for (var i = 0; i < tombstonesPerElement; i++)
            {
                set.Add(element, ReplicaA, counter++);
                set.Remove(element);
            }

            for (var i = 0; i < addsPerElement; i++) set.Add(element, ReplicaB, counter++);
        }

        return set;
    }

    /// <summary>
    /// The control corpus: many elements, one add dot each, so every spanned
    /// loop runs exactly one iteration.
    /// </summary>
    private static OrSet MakeFlatOrSet(int elements)
    {
        var set = new OrSet();
        for (var e = 0; e < elements; e++)
        {
            var element = Encoding.UTF8.GetBytes("flat:" + e.ToString("D4", CultureInfo.InvariantCulture));
            set.Add(element, ReplicaA, e + 1);
        }

        return set;
    }

    private static RwSet MakeChurnedRwSet(int elements, int addsPerElement, int removesPerElement)
    {
        var set = new RwSet();
        var counter = 1L;
        for (var e = 0; e < elements; e++)
        {
            var element = Encoding.UTF8.GetBytes("element:" + e.ToString("D4", CultureInfo.InvariantCulture));
            for (var i = 0; i < addsPerElement; i++) set.Add(element, ReplicaA, counter++);
            for (var i = 0; i < removesPerElement; i++) set.Remove(element, ReplicaB, counter++);
        }

        return set;
    }
}
