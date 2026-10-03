using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

using BenchmarkDotNet.Attributes;

using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates three trims on the CRDT liveness paths and the leaf bisect planner,
/// so their time and byte deltas are measurable in the clear rather than buried
/// under a silo, a grain call and a transport.
/// <para>
/// (1) <b><c>LeafEntryCache</c> sizes its bisect result lists from a closed
/// form.</b> Both frame-backed planners walk a fixed stride across the snapshot
/// and append one entry per step, so the number of entries is known before the
/// walk starts - it is a function of the row count and the stride alone. Both
/// nevertheless started from a default-capacity <c>List&lt;T&gt;</c> and
/// doubled their way there, allocating and then discarding every intermediate
/// backing array. A leaf wide enough to bisect into a few hundred batches grew
/// through eight arrays to reach one. <c>GetTransferBatchBoundariesWithoutHydrating</c>
/// also now returns early when the first boundary would already be past the
/// last row, where it previously allocated a list to return it empty.
/// </para>
/// <para>
/// (2) <b><c>OrMap.ListContainsDot</c> probes through a span.</b> The shape it
/// replaced indexed the list <em>twice</em> per iteration - once for the
/// counter and once for the replica id - so every non-matching element was
/// copied out of the list in full twice and bounds-checked twice, and
/// <c>List&lt;T&gt;.Count</c> is a mutable field the JIT cannot hoist out of
/// the loop condition. It now reads each element once through
/// <c>ref readonly</c>. It is the inner loop of <c>OrMap.Remove</c>'s tombstone
/// dedup and of <c>LiveEntryCount</c>, which backs <c>Count</c>,
/// <c>IsEmpty</c>, <c>Contains</c> and <c>Keys</c>.
/// </para>
/// <para>
/// (3) <b><c>OrSetDotCompaction.CountLive</c> / <c>AnyLive</c> resolve the
/// cover span once per walk.</b> <c>Covers</c> took a <c>List&lt;OrSetDot&gt;</c>
/// and called <c>CollectionsMarshal.AsSpan</c> on it, so testing <c>n</c>
/// candidate dots against one cancelling list re-derived the same span
/// <c>n</c> times. A span-typed overload lets the two walks that hold the cover
/// for their whole duration resolve it once. It is deliberately <em>not</em>
/// applied to the four merge-time call sites that append to the very list they
/// test against (<c>OrSet.MergeDelta</c>, <c>RwSet.UnionDeltaDots</c>,
/// <c>OrFlag</c> and <c>RwFlag.MergeDelta</c>): a hoisted span there would go
/// stale on the next growth, so those keep the list-typed overload, which
/// re-resolves per call.
/// </para>
/// <para>
/// Read group 1 for <b>bytes</b> - it is an allocation thesis, and its time
/// column is dominated by the per-boundary key decode both arms pay. Read
/// groups 2 and 3 for <b>time</b> only: neither changes heap traffic, and the
/// <c>Allocated</c> column is reported precisely so that can be checked rather
/// than asserted. Every group carries a control lane where the trim is expected
/// to buy little or nothing - a narrow leaf that bisects into a handful of
/// batches, a first-element hit, and a single-dot slot - because a trim that
/// taxes the degenerate case to help the common one is not a trim. Each group
/// also carries a lane isolating just the work the trim removed, so a small
/// end-to-end delta can be attributed rather than guessed at.
/// </para>
/// <para>
/// Every baseline lane is a verbatim copy of the code the trim replaced, with
/// the private thresholds and helpers it reads mirrored here, so it pays
/// exactly the dispatch its shipped counterpart pays. Each is asserted in
/// <see cref="Setup"/> to produce exactly the sequence its counterpart
/// produces, over corpora that include a wide leaf, a narrow leaf, multi-replica
/// dot lists, a counter collision across replicas and an empty collection. A
/// lane that answers differently is measuring different work and the comparison
/// would be void.
/// </para>
/// <para>
/// The group 3 corpora keep every cover list at or below the mirrored
/// <c>CoverCollapseThreshold</c>, so both arms take the linear tail walk this
/// trim touches rather than the collapsed counter test, which is unchanged and
/// is already covered by <c>CrdtDotCoverageCollapseBenchmarks</c>. The
/// pre-existing <c>CrdtDotScanTrimsBenchmarks</c> measures the <em>inside</em>
/// of <c>Covers</c> (list indexer versus span); this suite measures the
/// <em>outside</em> (how many times that span is resolved), which is a
/// different quantity and was not previously measured. The pre-existing
/// <c>DetachedLeafTransferBenchmarks</c> drives the <em>resident</em> boundary
/// planner, which has no closed form and is deliberately left unsized; group 1
/// drives the frame-backed planner, which does.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=crdtmergelivenesstrims</c> (or
/// <c>--suite crdtmergelivenesstrims</c>); see <c>Program.cs</c>. No Orleans
/// silo is involved, so it runs cheaply at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class CrdtMergeAndLivenessTrimBenchmarks
{
    private const string ReplicaA = "replica-a";
    private const string ReplicaB = "replica-b";

    /// <summary>
    /// Mirrors the private <c>OrSetDotCompaction.CoverCollapseThreshold</c>. The
    /// group 3 corpora stay at or below it so both lanes take the linear tail
    /// walk the trim changes, not the collapsed counter test it leaves alone.
    /// </summary>
    private const int BaselineCoverCollapseThreshold = 8;

    /// <summary>
    /// Mirrors the private <c>OrMap.LinearDedupThreshold</c>. The group 2
    /// corpora stay at or below it so the probe under test is the one the
    /// shipped dedup actually reaches.
    /// </summary>
    private const int BaselineLinearDedupThreshold = 16;

    // A 1 KiB row against a 16 KiB batch target yields the floor stride of one
    // hydration block, which is what a leaf of large rows actually plans at.
    private const int RowValueBytes = 1024;
    private const long TargetBatchBytes = 16L * 1024;
    private const long ResidentBudgetBytes = 16L * 1024;
    private const int WideRowCount = 8192;
    private const int NarrowRowCount = 96;
    private const int SlotCount = 256;

    // Group 1 - leaf bisect result presizing.
    private LeafEntryCache _wideCache = null!;
    private LeafEntryCache _narrowCache = null!;
    private LeafSnapshotHydrationSource _wideSource = null!;
    private LeafSnapshotHydrationSource _narrowSource = null!;
    private string _wideStartKey = null!;
    private string _narrowStartKey = null!;
    private string[] _growthProbeKeys = null!;

    // Group 2 - OrMap.ListContainsDot.
    private List<OrSetDot>[] _dedupLists = null!;
    private OrSetDot[] _dedupMissProbes = null!;
    private List<OrSetDot>[] _firstHitLists = null!;
    private OrSetDot[] _firstHitProbes = null!;

    // Group 3 - cover span hoist.
    private List<OrSetDot>[] _livenessDots = null!;
    private List<OrSetDot>[] _livenessCovers = null!;
    private List<OrSetDot>[] _singleDotSlots = null!;

    [GlobalSetup]
    public void Setup()
    {
        (_wideCache, _wideSource, _wideStartKey) = MakeFrameBackedLeaf(WideRowCount);
        (_narrowCache, _narrowSource, _narrowStartKey) = MakeFrameBackedLeaf(NarrowRowCount);
        _growthProbeKeys = BoundariesBaselineGrowing(_wideSource, _wideStartKey, TargetBatchBytes).ToArray();

        _dedupLists = new List<OrSetDot>[SlotCount];
        _dedupMissProbes = new OrSetDot[SlotCount];
        _firstHitLists = new List<OrSetDot>[SlotCount];
        _firstHitProbes = new OrSetDot[SlotCount];
        _livenessDots = new List<OrSetDot>[SlotCount];
        _livenessCovers = new List<OrSetDot>[SlotCount];
        _singleDotSlots = new List<OrSetDot>[SlotCount];
        for (var i = 0; i < SlotCount; i++)
        {
            _dedupLists[i] = MakeMultiReplicaSlot(BaselineLinearDedupThreshold - 4, baseCounter: 10);
            // Misses every element, so both arms walk the whole list - the shape
            // the dedup actually pays, since a hit short-circuits the append.
            _dedupMissProbes[i] = new OrSetDot { ReplicaId = "replica-z", Counter = 9999 };
            _firstHitLists[i] = MakeMultiReplicaSlot(BaselineLinearDedupThreshold - 4, baseCounter: 10);
            _firstHitProbes[i] = _firstHitLists[i][0];
            _livenessDots[i] = MakeMultiReplicaSlot(8, baseCounter: 20);
            _livenessCovers[i] = MakeMultiReplicaSlot(BaselineCoverCollapseThreshold, baseCounter: 1);
            _singleDotSlots[i] = [new OrSetDot { ReplicaId = ReplicaA, Counter = 30 }];
        }

        // --- Group 1 equivalence, wide and narrow, both planners. ---
        AssertBoundariesEquivalent(_wideCache, _wideSource, _wideStartKey, "wide leaf");
        AssertBoundariesEquivalent(_narrowCache, _narrowSource, _narrowStartKey, "narrow leaf");
        AssertWindowsEquivalent(_wideCache, _wideSource, "wide leaf");
        AssertWindowsEquivalent(_narrowCache, _narrowSource, "narrow leaf");
        // The start key past the last row is the branch the early return added:
        // the walk has no steps, so both arms must still answer empty.
        AssertBoundariesEquivalent(_wideCache, _wideSource, "zzzz-past-the-end", "exhausted range");

        // --- Group 2 equivalence. ---
        AssertContainsDotEquivalent(_dedupLists[0], _dedupMissProbes[0], "miss");
        AssertContainsDotEquivalent(_firstHitLists[0], _firstHitProbes[0], "first-element hit");
        AssertContainsDotEquivalent(_dedupLists[0], _dedupLists[0][^1], "last-element hit");
        AssertContainsDotEquivalent([], new OrSetDot { ReplicaId = ReplicaA, Counter = 1 }, "empty list");
        AssertContainsDotEquivalent(
            [new OrSetDot { ReplicaId = ReplicaB, Counter = 5 }],
            new OrSetDot { ReplicaId = ReplicaA, Counter = 5 },
            "counter collision across replicas");

        // --- Group 3 equivalence. ---
        AssertLivenessEquivalent(_livenessDots[0], _livenessCovers[0], "multi-replica");
        AssertLivenessEquivalent(_singleDotSlots[0], _livenessCovers[0], "single dot");
        AssertLivenessEquivalent([], _livenessCovers[0], "empty dots");
        AssertLivenessEquivalent(_livenessDots[0], [], "empty cover");
        AssertLivenessEquivalent(
            [new OrSetDot { ReplicaId = ReplicaB, Counter = 5 }],
            [new OrSetDot { ReplicaId = ReplicaA, Counter = 5 }],
            "counter collision across replicas");
    }

    // ---------------------------------------------------------------------
    // Group 1 - leaf bisect result presizing.
    // ---------------------------------------------------------------------

    /// <summary>
    /// A wide leaf planning its transfer boundaries: a few hundred appends, so
    /// the default-capacity list grows through every intermediate array.
    /// </summary>
    [Benchmark]
    public int TransferBoundariesWide_Baseline_GrowingList()
        => BoundariesBaselineGrowing(_wideSource, _wideStartKey, TargetBatchBytes).Count;

    [Benchmark]
    public int TransferBoundariesWide_Optimized_Presized()
        => _wideCache.GetTransferBatchBoundariesWithoutHydrating(_wideStartKey, TargetBatchBytes).Count;

    /// <summary>
    /// The control: a narrow leaf bisects into a handful of batches, where the
    /// default capacity was already enough and the trim must not cost anything.
    /// </summary>
    [Benchmark]
    public int TransferBoundariesNarrow_Baseline_GrowingList()
        => BoundariesBaselineGrowing(_narrowSource, _narrowStartKey, TargetBatchBytes).Count;

    [Benchmark]
    public int TransferBoundariesNarrow_Optimized_Presized()
        => _narrowCache.GetTransferBatchBoundariesWithoutHydrating(_narrowStartKey, TargetBatchBytes).Count;

    /// <summary>
    /// The full-scan window planner on the same wide leaf. Its entries are
    /// 16-byte tuples rather than references, so the discarded intermediate
    /// arrays are twice the size of the boundary planner's.
    /// </summary>
    [Benchmark]
    public int FullScanWindowsWide_Baseline_GrowingList()
        => WindowsBaselineGrowing(_wideSource, ResidentBudgetBytes).Count;

    [Benchmark]
    public int FullScanWindowsWide_Optimized_Presized()
        => _wideCache.GetFullScanWindowsWithoutHydrating().Count;

    /// <summary>The narrow-leaf control for the window planner.</summary>
    [Benchmark]
    public int FullScanWindowsNarrow_Baseline_GrowingList()
        => WindowsBaselineGrowing(_narrowSource, ResidentBudgetBytes).Count;

    [Benchmark]
    public int FullScanWindowsNarrow_Optimized_Presized()
        => _narrowCache.GetFullScanWindowsWithoutHydrating().Count;

    /// <summary>
    /// Isolates exactly the work the trim removed: the list growth itself, with
    /// the frame seek and the per-boundary key decode left out of both arms, so
    /// the byte delta is attributable rather than inferred.
    /// </summary>
    [Benchmark]
    public int ListGrowth_Baseline_DefaultCapacity()
    {
        var boundaries = new List<string>();
        for (var i = 0; i < _growthProbeKeys.Length; i++)
        {
            boundaries.Add(_growthProbeKeys[i]);
        }

        return boundaries.Count;
    }

    [Benchmark]
    public int ListGrowth_Optimized_Presized()
    {
        var boundaries = new List<string>(_growthProbeKeys.Length);
        for (var i = 0; i < _growthProbeKeys.Length; i++)
        {
            boundaries.Add(_growthProbeKeys[i]);
        }

        return boundaries.Count;
    }

    // ---------------------------------------------------------------------
    // Group 2 - OrMap.ListContainsDot double-index removal.
    // ---------------------------------------------------------------------

    /// <summary>
    /// The full-scan shape, which is what the tombstone dedup pays whenever the
    /// dot is genuinely new and the append goes ahead.
    /// </summary>
    [Benchmark]
    public int ListContainsDotMiss_Baseline_DoubleIndex()
    {
        var hits = 0;
        for (var i = 0; i < _dedupLists.Length; i++)
        {
            if (ListContainsDotByIndexer(_dedupLists[i], _dedupMissProbes[i])) hits++;
        }

        return hits;
    }

    [Benchmark]
    public int ListContainsDotMiss_Optimized_Span()
    {
        var hits = 0;
        for (var i = 0; i < _dedupLists.Length; i++)
        {
            if (ListContainsDotBySpan(_dedupLists[i], _dedupMissProbes[i])) hits++;
        }

        return hits;
    }

    /// <summary>
    /// The control: a hit on the first element reads one element either way, so
    /// the trim has nothing to remove and must not cost anything.
    /// </summary>
    [Benchmark]
    public int ListContainsDotFirstHit_Baseline_DoubleIndex()
    {
        var hits = 0;
        for (var i = 0; i < _firstHitLists.Length; i++)
        {
            if (ListContainsDotByIndexer(_firstHitLists[i], _firstHitProbes[i])) hits++;
        }

        return hits;
    }

    [Benchmark]
    public int ListContainsDotFirstHit_Optimized_Span()
    {
        var hits = 0;
        for (var i = 0; i < _firstHitLists.Length; i++)
        {
            if (ListContainsDotBySpan(_firstHitLists[i], _firstHitProbes[i])) hits++;
        }

        return hits;
    }

    /// <summary>
    /// Isolates the removed work: the second indexer read and the
    /// per-iteration <c>Count</c> reload, with the string comparison that
    /// dominates a matching element left out of both arms.
    /// </summary>
    [Benchmark]
    public int ElementRead_Baseline_DoubleIndex()
    {
        var total = 0L;
        for (var i = 0; i < _dedupLists.Length; i++)
        {
            var list = _dedupLists[i];
            for (var j = 0; j < list.Count; j++)
            {
                total += list[j].Counter;
                total += list[j].ReplicaId.Length;
            }
        }

        return (int)total;
    }

    [Benchmark]
    public int ElementRead_Optimized_SingleSpanRead()
    {
        var total = 0L;
        for (var i = 0; i < _dedupLists.Length; i++)
        {
            var span = CollectionsMarshal.AsSpan(_dedupLists[i]);
            for (var j = 0; j < span.Length; j++)
            {
                ref readonly var dot = ref span[j];
                total += dot.Counter;
                total += dot.ReplicaId.Length;
            }
        }

        return (int)total;
    }

    // ---------------------------------------------------------------------
    // Group 3 - cover span resolved once per walk, not once per dot.
    // ---------------------------------------------------------------------

    [Benchmark]
    public int CountLive_Baseline_CoverPerDot()
    {
        var live = 0;
        for (var i = 0; i < _livenessDots.Length; i++)
        {
            live += CountLiveByCoverPerDot(_livenessDots[i], _livenessCovers[i]);
        }

        return live;
    }

    [Benchmark]
    public int CountLive_Optimized_CoverHoisted()
    {
        var live = 0;
        for (var i = 0; i < _livenessDots.Length; i++)
        {
            live += OrSetDotCompaction.CountLive(_livenessDots[i], _livenessCovers[i]);
        }

        return live;
    }

    [Benchmark]
    public int AnyLive_Baseline_CoverPerDot()
    {
        var live = 0;
        for (var i = 0; i < _livenessDots.Length; i++)
        {
            if (AnyLiveByCoverPerDot(_livenessDots[i], _livenessCovers[i])) live++;
        }

        return live;
    }

    [Benchmark]
    public int AnyLive_Optimized_CoverHoisted()
    {
        var live = 0;
        for (var i = 0; i < _livenessDots.Length; i++)
        {
            if (OrSetDotCompaction.AnyLive(_livenessDots[i], _livenessCovers[i])) live++;
        }

        return live;
    }

    /// <summary>
    /// The control: one candidate dot means one <c>Covers</c> call, so the
    /// cover span is resolved once either way.
    /// </summary>
    [Benchmark]
    public int CountLiveSingleDot_Baseline_CoverPerDot()
    {
        var live = 0;
        for (var i = 0; i < _singleDotSlots.Length; i++)
        {
            live += CountLiveByCoverPerDot(_singleDotSlots[i], _livenessCovers[i]);
        }

        return live;
    }

    [Benchmark]
    public int CountLiveSingleDot_Optimized_CoverHoisted()
    {
        var live = 0;
        for (var i = 0; i < _singleDotSlots.Length; i++)
        {
            live += OrSetDotCompaction.CountLive(_singleDotSlots[i], _livenessCovers[i]);
        }

        return live;
    }

    /// <summary>
    /// Isolates the removed work: the repeated <c>CollectionsMarshal.AsSpan</c>
    /// resolution, with the cancellation predicate left out of both arms.
    /// </summary>
    [Benchmark]
    public int CoverResolve_Baseline_PerDot()
    {
        var total = 0;
        for (var i = 0; i < _livenessDots.Length; i++)
        {
            var dots = _livenessDots[i];
            var cover = _livenessCovers[i];
            for (var d = 0; d < dots.Count; d++)
            {
                total += CollectionsMarshal.AsSpan(cover).Length;
            }
        }

        return total;
    }

    [Benchmark]
    public int CoverResolve_Optimized_PerWalk()
    {
        var total = 0;
        for (var i = 0; i < _livenessDots.Length; i++)
        {
            var dots = _livenessDots[i];
            var coverSpan = CollectionsMarshal.AsSpan(_livenessCovers[i]);
            for (var d = 0; d < dots.Count; d++)
            {
                total += coverSpan.Length;
            }
        }

        return total;
    }

    // ---------------------------------------------------------------------
    // Baselines - verbatim copies of the code each trim replaced.
    // ---------------------------------------------------------------------

    /// <summary>
    /// The prior <c>GetTransferBatchBoundariesWithoutHydrating</c> frame branch,
    /// reading the same internal seek surface the shipped planner reads.
    /// </summary>
    private static IReadOnlyList<string> BoundariesBaselineGrowing(
        LeafSnapshotHydrationSource source,
        string startInclusive,
        long targetBatchBytes)
    {
        if (targetBatchBytes <= 0)
        {
            return [];
        }

        var rowCount = source.RowCount;
        if (rowCount == 0)
        {
            return [];
        }

        var averageRowBytes = Math.Max(1L, source.TotalStateBytes / rowCount);
        var rowsPerBatch = Math.Max(
            LeafSnapshotHydrationSource.BlockRows,
            targetBatchBytes / averageRowBytes);
        rowsPerBatch = ((rowsPerBatch + LeafSnapshotHydrationSource.BlockRows - 1)
            / LeafSnapshotHydrationSource.BlockRows) * LeafSnapshotHydrationSource.BlockRows;

        if (rowsPerBatch >= rowCount)
        {
            return [];
        }

        var keyUtf8 = Encoding.UTF8.GetBytes(startInclusive);
        if (!source.TryFindLowerBound(keyUtf8, out var lowerBound))
        {
            return [];
        }

        var stride = (int)rowsPerBatch;
        var boundaries = new List<string>();
        for (var index = lowerBound + stride; index < rowCount; index += stride)
        {
            if (source.TryReadRowKeyAt(index, out var boundary))
            {
                boundaries.Add(boundary);
            }
        }

        return boundaries;
    }

    /// <summary>The prior <c>GetFullScanWindowsWithoutHydrating</c> frame branch.</summary>
    private static IReadOnlyList<(string? StartInclusive, string? EndExclusive)> WindowsBaselineGrowing(
        LeafSnapshotHydrationSource source,
        long residentBudgetBytes)
    {
        if (residentBudgetBytes <= 0)
        {
            return [(null, null)];
        }

        var rowCount = source.RowCount;
        var rowsPerWindow = ComputeRowsPerBatchBaseline(source, residentBudgetBytes);
        if (rowsPerWindow <= 0 || rowsPerWindow >= rowCount)
        {
            return [(null, null)];
        }

        var windows = new List<(string?, string?)>();
        string? start = null;
        for (var index = (int)rowsPerWindow; index < rowCount; index += (int)rowsPerWindow)
        {
            if (!source.TryReadRowKeyAt(index, out var boundary))
            {
                break;
            }

            windows.Add((start, boundary));
            start = boundary;
        }

        windows.Add((start, null));
        return windows;
    }

    /// <summary>Mirrors the private <c>LeafEntryCache.ComputeRowsPerBatch</c>.</summary>
    private static long ComputeRowsPerBatchBaseline(LeafSnapshotHydrationSource source, long targetBatchBytes)
    {
        var rowCount = source.RowCount;
        if (rowCount == 0 || targetBatchBytes <= 0)
        {
            return 0;
        }

        var averageRowBytes = Math.Max(1L, source.TotalStateBytes / rowCount);
        var rowsPerBatch = Math.Max(
            LeafSnapshotHydrationSource.BlockRows,
            targetBatchBytes / averageRowBytes);

        return ((rowsPerBatch + LeafSnapshotHydrationSource.BlockRows - 1)
            / LeafSnapshotHydrationSource.BlockRows) * LeafSnapshotHydrationSource.BlockRows;
    }

    /// <summary>The prior <c>OrMap.ListContainsDot</c> body.</summary>
    private static bool ListContainsDotByIndexer(List<OrSetDot> list, OrSetDot dot)
    {
        for (var i = 0; i < list.Count; i++)
        {
            if (list[i].Counter == dot.Counter && string.Equals(list[i].ReplicaId, dot.ReplicaId, StringComparison.Ordinal)) return true;
        }
        return false;
    }

    /// <summary>
    /// The shipped <c>OrMap.ListContainsDot</c> body. It is private, so both
    /// arms are local copies and pay identical static dispatch; the only
    /// difference between them is the trim under test.
    /// </summary>
    private static bool ListContainsDotBySpan(List<OrSetDot> list, OrSetDot dot)
    {
        var span = CollectionsMarshal.AsSpan(list);
        for (var i = 0; i < span.Length; i++)
        {
            ref readonly var candidate = ref span[i];
            if (candidate.Counter == dot.Counter
                && string.Equals(candidate.ReplicaId, dot.ReplicaId, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// The prior <c>OrSetDotCompaction.CountLive</c> tail walk, which resolved
    /// the cover span inside <c>Covers</c> once per candidate dot. The corpora
    /// keep every cover at or below
    /// <see cref="BaselineCoverCollapseThreshold"/>, so neither this nor the
    /// shipped counterpart reaches the collapse branch and the mirrored test
    /// below is the only dispatch either arm pays for it.
    /// </summary>
    private static int CountLiveByCoverPerDot(List<OrSetDot> dots, List<OrSetDot> cover)
    {
        if (dots.Count == 0)
        {
            return 0;
        }

        if (cover.Count == 0)
        {
            return dots.Count;
        }

        if (cover.Count > BaselineCoverCollapseThreshold && dots.Count > 1)
        {
            throw new InvalidOperationException("Corpus reached the collapse branch; the comparison would be void.");
        }

        var live = 0;
        var span = CollectionsMarshal.AsSpan(dots);
        for (var i = 0; i < span.Length; i++)
        {
            if (!OrSetDotCompaction.Covers(cover, in span[i]))
            {
                live++;
            }
        }

        return live;
    }

    /// <summary>The prior <c>OrSetDotCompaction.AnyLive</c> tail walk.</summary>
    private static bool AnyLiveByCoverPerDot(List<OrSetDot> dots, List<OrSetDot> cover)
    {
        if (dots.Count == 0)
        {
            return false;
        }

        if (cover.Count == 0)
        {
            return true;
        }

        if (cover.Count > BaselineCoverCollapseThreshold && dots.Count > 1)
        {
            throw new InvalidOperationException("Corpus reached the collapse branch; the comparison would be void.");
        }

        var span = CollectionsMarshal.AsSpan(dots);
        for (var i = 0; i < span.Length; i++)
        {
            if (!OrSetDotCompaction.Covers(cover, in span[i]))
            {
                return true;
            }
        }

        return false;
    }

    // ---------------------------------------------------------------------
    // Corpora and equivalence assertions.
    // ---------------------------------------------------------------------

    /// <summary>
    /// Builds a frame-backed leaf: an encoded snapshot attached to a cache with
    /// a resident budget, plus the same frame opened directly so the baselines
    /// read exactly the seek surface the shipped planners read.
    /// </summary>
    private static (LeafEntryCache Cache, LeafSnapshotHydrationSource Source, string StartKey) MakeFrameBackedLeaf(
        int rowCount)
    {
        var rows = new LeafSnapshotRow[rowCount];
        for (var i = 0; i < rowCount; i++)
        {
            var payload = new byte[RowValueBytes];
            for (var b = 0; b < 32; b++)
            {
                payload[b] = (byte)((i * 31) + b);
            }

            rows[i] = new LeafSnapshotRow(
                string.Create(CultureInfo.InvariantCulture, $"k{i:D7}"),
                LwwValue<byte[]>.Create(payload, new HybridLogicalClock { WallClockTicks = 100L + i }));
        }

        var frame = LeafSnapshotCodec.Encode(rows);
        var cache = new LeafEntryCache(new(StringComparer.Ordinal));
        if (!cache.TryAttachSnapshot(frame, ResidentBudgetBytes))
        {
            throw new InvalidOperationException("The encoded frame could not back a seek.");
        }

        if (!LeafSnapshotHydrationSource.TryCreate(frame, out var source))
        {
            throw new InvalidOperationException("The encoded frame could not be opened directly.");
        }

        return (cache, source, rows[0].Key);
    }

    private static void AssertBoundariesEquivalent(
        LeafEntryCache cache,
        LeafSnapshotHydrationSource source,
        string startKey,
        string label)
    {
        var baseline = BoundariesBaselineGrowing(source, startKey, TargetBatchBytes);
        var shipped = cache.GetTransferBatchBoundariesWithoutHydrating(startKey, TargetBatchBytes);
        if (baseline.Count != shipped.Count || !baseline.SequenceEqual(shipped, StringComparer.Ordinal))
        {
            throw new InvalidOperationException(
                $"Transfer boundary lanes disagree ({label}): {baseline.Count} vs {shipped.Count}.");
        }
    }

    private static void AssertWindowsEquivalent(
        LeafEntryCache cache,
        LeafSnapshotHydrationSource source,
        string label)
    {
        var baseline = WindowsBaselineGrowing(source, ResidentBudgetBytes);
        var shipped = cache.GetFullScanWindowsWithoutHydrating();
        if (baseline.Count != shipped.Count)
        {
            throw new InvalidOperationException(
                $"Full-scan window lanes disagree ({label}): {baseline.Count} vs {shipped.Count}.");
        }

        for (var i = 0; i < baseline.Count; i++)
        {
            if (!string.Equals(baseline[i].StartInclusive, shipped[i].StartInclusive, StringComparison.Ordinal)
                || !string.Equals(baseline[i].EndExclusive, shipped[i].EndExclusive, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Full-scan window lanes disagree ({label}) at window {i}.");
            }
        }
    }

    private static List<OrSetDot> MakeMultiReplicaSlot(int count, int baseCounter)
    {
        var dots = new List<OrSetDot>(count);
        for (var i = 0; i < count; i++)
        {
            dots.Add(new OrSetDot
            {
                ReplicaId = string.Create(CultureInfo.InvariantCulture, $"replica-{i % 3}"),
                Counter = baseCounter + i,
            });
        }

        return dots;
    }

    private static void AssertContainsDotEquivalent(List<OrSetDot> list, OrSetDot probe, string label)
    {
        var baseline = ListContainsDotByIndexer(list, probe);
        var shipped = ListContainsDotBySpan(list, probe);
        if (baseline != shipped)
        {
            throw new InvalidOperationException(
                $"ListContainsDot lanes disagree on '{label}': baseline {baseline}, shipped {shipped}.");
        }
    }

    private static void AssertLivenessEquivalent(List<OrSetDot> dots, List<OrSetDot> cover, string label)
    {
        var baselineCount = CountLiveByCoverPerDot(dots, cover);
        var shippedCount = OrSetDotCompaction.CountLive(dots, cover);
        if (baselineCount != shippedCount)
        {
            throw new InvalidOperationException(
                $"CountLive lanes disagree on '{label}': baseline {baselineCount}, shipped {shippedCount}.");
        }

        var baselineAny = AnyLiveByCoverPerDot(dots, cover);
        var shippedAny = OrSetDotCompaction.AnyLive(dots, cover);
        if (baselineAny != shippedAny)
        {
            throw new InvalidOperationException(
                $"AnyLive lanes disagree on '{label}': baseline {baselineAny}, shipped {shippedAny}.");
        }
    }
}
