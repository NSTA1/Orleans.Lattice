using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three observed-remove dot-reconciliation allocation trims made
/// to the <c>Orleans.Lattice</c> CRDT primitives so their per-call byte deltas
/// are measurable in the clear, with no Orleans cluster in the loop. Each pair
/// below reproduces exactly one production method's prior shape against its
/// optimized shape over identical inputs, so the <c>Allocated</c> column is
/// deterministic and the baseline-vs-optimized delta is precisely the heap the
/// production change removes.
/// <para>
/// All three sites shared the same gap: when an element/key had accumulated a
/// long observed-remove (tombstone) history, the reconciliation built a
/// <c>HashSet&lt;OrSetDot&gt;</c> sized to that history on <em>every</em> call,
/// even though the <em>live</em> side (the element's surviving add dots, or a
/// key's live entries) is overwhelmingly 1-2 in practice. Building the set is
/// wasted when the live side is tiny: a linear membership scan of the few live
/// dots against the tombstone list is <c>O(liveCount * tombCount)</c> - the same
/// asymptotic as building the tomb-sized set when <c>liveCount</c> is a small
/// constant - but allocates nothing. The optimized shape diverts to the linear
/// scan whenever <em>either</em> side is small (the prior guard checked only the
/// tombstone side), so a heavily-tombstoned key with a single live dot no longer
/// allocates on the read (<c>IsEmpty</c> / <c>Count</c> / <c>Elements</c> /
/// <c>Contains</c>) and remove hot paths.
/// </para>
/// <para>
/// The three pairs mirror the production edits verbatim:
/// (1) <see cref="OrSet"/>'s private <c>LiveDotCount</c> - the per-key live-dot
/// tally behind every OR-Set read;
/// (2) <see cref="OrSet"/>'s <c>Remove</c> tombstone-dedup reconciliation;
/// (3) <see cref="OrMap{TKey, TValue}"/>'s private <c>LiveEntryCount</c> - the
/// per-key live-entry tally behind every OR-Map read.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=ordedup</c> (or <c>--suite ordedup</c>);
/// see <c>Program.cs</c>. The suite has no Orleans silo dependency, so it is
/// fast to run at <c>BENCH_MICROBENCH_FIDELITY=full</c> for tight confidence
/// intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class OrCrdtReconcileBenchmarks
{
    // OrSet's DotLinearScanThreshold (4): below this many dots on the small
    // side a linear scan beats allocating a HashSet.
    private const int OrSetLinearThreshold = 4;

    // OrMap's LinearDedupThreshold (16): the equivalent crossover for OR-Map
    // entry reconciliation.
    private const int OrMapLinearThreshold = 16;

    // A single live add dot (the dominant real case) against a long tombstone
    // history. Kept at 2 live dots to sit clearly inside both thresholds.
    private const int LiveDots = 2;

    private List<OrSetDot> _liveDots = null!;
    private List<OrSetDot> _tombstones = null!;
    private List<OrMapEntry<GCounter>> _liveEntries = null!;

    /// <summary>Builds the (few live, many tombstoned) inputs shared by all three pairs.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _liveDots = new List<OrSetDot>(LiveDots);
        for (var i = 0; i < LiveDots; i++)
        {
            _liveDots.Add(new OrSetDot { ReplicaId = "live", Counter = i });
        }

        _liveEntries = new List<OrMapEntry<GCounter>>(LiveDots);
        for (var i = 0; i < LiveDots; i++)
        {
            _liveEntries.Add(new OrMapEntry<GCounter>("live", i, new GCounter()));
        }
    }

    // ------------------------------------------------------------------
    // (1) OrSet.LiveDotCount - per-key live-dot tally behind every OR-Set read
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the prior <c>LiveDotCount</c> shape - once the tombstone list
    /// crosses the linear threshold it builds a <c>HashSet&lt;OrSetDot&gt;</c>
    /// sized to the whole tombstone history on every call, regardless of how
    /// few live dots are being counted.
    /// </summary>
    [Benchmark(Description = "OrSet live-dot count: HashSet (baseline)")]
    [Arguments(16)]
    [Arguments(64)]
    [Arguments(256)]
    public int OrSetLiveDotCount_Baseline(int tombstoneCount)
    {
        EnsureTombstones(tombstoneCount);
        return LiveDotCount_Baseline(_liveDots, _tombstones);
    }

    /// <summary>
    /// Optimized: the shipped <c>LiveDotCount</c> shape - it diverts to the
    /// allocation-free linear scan whenever the live-dot list is small, so the
    /// few-live/many-tombstoned read no longer allocates.
    /// </summary>
    [Benchmark(Description = "OrSet live-dot count: small-side linear (optimized)")]
    [Arguments(16)]
    [Arguments(64)]
    [Arguments(256)]
    public int OrSetLiveDotCount_Optimized(int tombstoneCount)
    {
        EnsureTombstones(tombstoneCount);
        return LiveDotCount_Optimized(_liveDots, _tombstones);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int LiveDotCount_Baseline(List<OrSetDot> dots, List<OrSetDot> tomb)
    {
        if (tomb.Count == 0) return dots.Count;
        if (tomb.Count <= OrSetLinearThreshold)
        {
            var live = 0;
            foreach (var d in dots)
            {
                if (!tomb.Contains(d)) live++;
            }
            return live;
        }
        var tombSet = new HashSet<OrSetDot>(tomb);
        var n = 0;
        foreach (var d in dots)
        {
            if (!tombSet.Contains(d)) n++;
        }
        return n;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int LiveDotCount_Optimized(List<OrSetDot> dots, List<OrSetDot> tomb)
    {
        if (tomb.Count == 0) return dots.Count;
        if (tomb.Count <= OrSetLinearThreshold || dots.Count <= OrSetLinearThreshold)
        {
            var live = 0;
            foreach (var d in dots)
            {
                if (!tomb.Contains(d)) live++;
            }
            return live;
        }
        var tombSet = new HashSet<OrSetDot>(tomb);
        var n = 0;
        foreach (var d in dots)
        {
            if (!tombSet.Contains(d)) n++;
        }
        return n;
    }

    // ------------------------------------------------------------------
    // (2) OrSet.Remove - tombstone-dedup reconciliation on the remove path
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the prior <c>Remove</c> tombstone-dedup shape - once the
    /// tombstone list is large it seeds a <c>HashSet&lt;OrSetDot&gt;</c> from
    /// the whole history to dedup the few observed add dots being tombstoned.
    /// </summary>
    [Benchmark(Description = "OrSet remove dedup: HashSet (baseline)")]
    [Arguments(16)]
    [Arguments(64)]
    [Arguments(256)]
    public int OrSetRemoveDedup_Baseline(int tombstoneCount)
    {
        EnsureTombstones(tombstoneCount);
        return RemoveDedup_Baseline(_liveDots, _tombstones);
    }

    /// <summary>
    /// Optimized: the shipped <c>Remove</c> shape - it diverts to the
    /// allocation-free linear dedup whenever the observed add-dot list is
    /// small, the common case for an element removed after a handful of adds.
    /// </summary>
    [Benchmark(Description = "OrSet remove dedup: small-side linear (optimized)")]
    [Arguments(16)]
    [Arguments(64)]
    [Arguments(256)]
    public int OrSetRemoveDedup_Optimized(int tombstoneCount)
    {
        EnsureTombstones(tombstoneCount);
        return RemoveDedup_Optimized(_liveDots, _tombstones);
    }

    // Non-mutating mirror of the Remove membership decision: returns the number
    // of observed dots that are not already tombstoned (the production method
    // then appends exactly those to the tombstone list - an O(1)-amortized add
    // identical in both shapes, excluded here so the sole per-lane difference
    // is the HashSet allocation the baseline pays).
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int RemoveDedup_Baseline(List<OrSetDot> dots, List<OrSetDot> tomb)
    {
        var added = 0;
        if (tomb.Count <= OrSetLinearThreshold)
        {
            foreach (var dot in dots)
            {
                if (!tomb.Contains(dot)) added++;
            }
            return added;
        }
        var tombSet = new HashSet<OrSetDot>(tomb);
        foreach (var dot in dots)
        {
            if (tombSet.Add(dot)) added++;
        }
        return added;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int RemoveDedup_Optimized(List<OrSetDot> dots, List<OrSetDot> tomb)
    {
        var added = 0;
        if (tomb.Count <= OrSetLinearThreshold || dots.Count <= OrSetLinearThreshold)
        {
            foreach (var dot in dots)
            {
                if (!tomb.Contains(dot)) added++;
            }
            return added;
        }
        var tombSet = new HashSet<OrSetDot>(tomb);
        foreach (var dot in dots)
        {
            if (tombSet.Add(dot)) added++;
        }
        return added;
    }

    // ------------------------------------------------------------------
    // (3) OrMap.LiveEntryCount - per-key live-entry tally behind every OR-Map read
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: the prior <c>LiveEntryCount</c> shape - once the tombstone
    /// list crosses the dedup threshold it builds a
    /// <c>HashSet&lt;OrSetDot&gt;</c> sized to the whole tombstone history on
    /// every call, regardless of how few live entries are being counted.
    /// </summary>
    [Benchmark(Description = "OrMap live-entry count: HashSet (baseline)")]
    [Arguments(32)]
    [Arguments(128)]
    [Arguments(512)]
    public int OrMapLiveEntryCount_Baseline(int tombstoneCount)
    {
        EnsureTombstones(tombstoneCount);
        return LiveEntryCount_Baseline(_liveEntries, _tombstones);
    }

    /// <summary>
    /// Optimized: the shipped <c>LiveEntryCount</c> shape - it diverts to the
    /// allocation-free linear scan whenever the live-entry list is small, so a
    /// heavily-tombstoned key with a single live value no longer allocates on
    /// the read path.
    /// </summary>
    [Benchmark(Description = "OrMap live-entry count: small-side linear (optimized)")]
    [Arguments(32)]
    [Arguments(128)]
    [Arguments(512)]
    public int OrMapLiveEntryCount_Optimized(int tombstoneCount)
    {
        EnsureTombstones(tombstoneCount);
        return LiveEntryCount_Optimized(_liveEntries, _tombstones);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int LiveEntryCount_Baseline(List<OrMapEntry<GCounter>> entries, List<OrSetDot> tomb)
    {
        if (tomb.Count == 0) return entries.Count;
        if (tomb.Count <= OrMapLinearThreshold)
        {
            var n = 0;
            foreach (var e in entries)
            {
                var dot = new OrSetDot { ReplicaId = e.ReplicaId, Counter = e.Counter };
                if (!ListContainsDot(tomb, dot)) n++;
            }
            return n;
        }
        var tombSet = new HashSet<OrSetDot>(tomb.Count);
        foreach (var d in tomb) tombSet.Add(d);
        var live = 0;
        foreach (var e in entries)
        {
            if (!tombSet.Contains(new OrSetDot { ReplicaId = e.ReplicaId, Counter = e.Counter })) live++;
        }
        return live;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int LiveEntryCount_Optimized(List<OrMapEntry<GCounter>> entries, List<OrSetDot> tomb)
    {
        if (tomb.Count == 0) return entries.Count;
        if (tomb.Count <= OrMapLinearThreshold || entries.Count <= OrMapLinearThreshold)
        {
            var n = 0;
            foreach (var e in entries)
            {
                var dot = new OrSetDot { ReplicaId = e.ReplicaId, Counter = e.Counter };
                if (!ListContainsDot(tomb, dot)) n++;
            }
            return n;
        }
        var tombSet = new HashSet<OrSetDot>(tomb.Count);
        foreach (var d in tomb) tombSet.Add(d);
        var live = 0;
        foreach (var e in entries)
        {
            if (!tombSet.Contains(new OrSetDot { ReplicaId = e.ReplicaId, Counter = e.Counter })) live++;
        }
        return live;
    }

    private static bool ListContainsDot(List<OrSetDot> list, OrSetDot dot)
    {
        for (var i = 0; i < list.Count; i++)
        {
            if (list[i].Counter == dot.Counter && string.Equals(list[i].ReplicaId, dot.ReplicaId, System.StringComparison.Ordinal))
            {
                return true;
            }
        }
        return false;
    }

    // Lazily (re)builds the shared tombstone list to the requested length. The
    // tombstone dots use a distinct replica id so none of them match a live
    // dot/entry - every live dot survives, isolating the reconciliation cost
    // rather than the count result.
    private void EnsureTombstones(int tombstoneCount)
    {
        if (_tombstones is not null && _tombstones.Count == tombstoneCount) return;
        var tomb = new List<OrSetDot>(tombstoneCount);
        for (var i = 0; i < tombstoneCount; i++)
        {
            tomb.Add(new OrSetDot { ReplicaId = "tomb-" + i.ToString("D6", CultureInfo.InvariantCulture), Counter = i });
        }
        _tombstones = tomb;
    }
}
