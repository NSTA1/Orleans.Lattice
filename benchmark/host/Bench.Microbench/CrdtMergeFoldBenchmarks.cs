using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the steady-state merge-fold allocation trim shared by every
/// OR-family union helper, so its per-call byte delta is measurable in the
/// clear with no Orleans cluster in the loop. It is the merge-path companion to
/// <see cref="OrCrdtReconcileBenchmarks"/>, which covers the read and
/// remove-side reconciliation instead.
/// <para>
/// The site: <see cref="OrSet"/> / <see cref="RwSet"/> <c>MergeMap</c> and the
/// two flags' (<see cref="OrFlag"/> / <see cref="RwFlag"/>) <c>UnionInto</c> /
/// <c>UnionDots</c> all fold an incoming dot list into an accumulated one. They
/// took the allocation-free linear path only when <em>both</em> sides were at or
/// below the linear-scan threshold (4), so folding the 1-2-dot delta that
/// dominates replication into a key or flag with a long accumulated dot history
/// seeded a <c>HashSet&lt;OrSetDot&gt;</c> from the whole accumulated list on
/// every merge. The guard now diverts on the incoming side alone.
/// </para>
/// <para>
/// Both lanes reproduce one production shape over identical inputs, so the
/// <c>Allocated</c> delta is precisely the heap the change removes. The baseline
/// mirrors the set-building shape actually shipped on <c>main</c> - presized to
/// the target plus the incoming count and filled through the list's struct
/// enumerator, the shape <c>OrSetDotSet.Build</c> encapsulates (it is
/// <c>internal</c>, so it is reproduced rather than called). Seeding a
/// <c>HashSet</c> through its collection constructor instead is a further ~3x
/// slower, and crediting that separately-landed win to this change would
/// overstate it.
/// </para>
/// <para>
/// The largest argument deliberately runs well past any realistic dot history:
/// the linear branch appends at most <c>DotLinearScanThreshold</c> dots, so it
/// stays <c>O(delta * accumulated)</c> - bounded by <c>O(4 * accumulated)</c>,
/// the same asymptotic as building the accumulated-sized set - and the lane
/// confirms it is still the faster shape there, not only at small histories.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=mergefold</c> (or
/// <c>--suite mergefold</c>); see <c>Program.cs</c>. The suite has no Orleans
/// silo dependency, so it is fast to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c> for tight confidence intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class CrdtMergeFoldBenchmarks
{
    // The DotLinearScanThreshold (4) shared by OrSet, RwSet, OrFlag and RwFlag:
    // at or below this many dots on the incoming side a linear scan beats
    // allocating a HashSet.
    private const int LinearThreshold = 4;

    // A small incoming delta (the dominant real case on the replication /
    // merge path). Kept at 2 to sit clearly inside the threshold.
    private const int IncomingDots = 2;

    private List<OrSetDot> _incoming = null!;
    private List<OrSetDot> _accumulated = null!;

    /// <summary>Builds the small incoming delta shared by both lanes.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _incoming = new List<OrSetDot>(IncomingDots);
        for (var i = 0; i < IncomingDots; i++)
        {
            _incoming.Add(new OrSetDot { ReplicaId = "delta", Counter = i });
        }
    }

    /// <summary>
    /// Baseline: the prior fold guard - the linear path required <em>both</em>
    /// the accumulated target and the incoming delta to be small, so a small
    /// delta folded into a long accumulated list seeded a
    /// <c>HashSet&lt;OrSetDot&gt;</c> from the whole target on every merge.
    /// </summary>
    /// <param name="accumulatedCount">Length of the accumulated dot list to fold into.</param>
    /// <returns>The number of incoming dots that were not already present.</returns>
    [Benchmark(Description = "Merge fold: HashSet (baseline)")]
    [Arguments(16)]
    [Arguments(64)]
    [Arguments(256)]
    [Arguments(1024)]
    public int MergeFold_Baseline(int accumulatedCount)
    {
        EnsureAccumulated(accumulatedCount);
        return FoldBaseline(_accumulated, _incoming);
    }

    /// <summary>
    /// Optimized: the shipped fold guard - it diverts to the allocation-free
    /// linear path whenever the incoming delta is small, independent of the
    /// accumulated target size, so the steady-state replication fold no longer
    /// allocates.
    /// </summary>
    /// <param name="accumulatedCount">Length of the accumulated dot list to fold into.</param>
    /// <returns>The number of incoming dots that were not already present.</returns>
    [Benchmark(Description = "Merge fold: small-incoming linear (optimized)")]
    [Arguments(16)]
    [Arguments(64)]
    [Arguments(256)]
    [Arguments(1024)]
    public int MergeFold_Optimized(int accumulatedCount)
    {
        EnsureAccumulated(accumulatedCount);
        return FoldOptimized(_accumulated, _incoming);
    }

    // Non-mutating mirror of the union membership decision: returns the number
    // of incoming dots not already present in the target (the production helper
    // then appends exactly those - an O(1)-amortized add identical in both
    // shapes, excluded here so the sole per-lane difference is the HashSet the
    // baseline builds).
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int FoldBaseline(List<OrSetDot> target, List<OrSetDot> source)
    {
        var added = 0;
        if (target.Count <= LinearThreshold && source.Count <= LinearThreshold)
        {
            foreach (var dot in source)
            {
                if (!target.Contains(dot)) added++;
            }
            return added;
        }
        var seen = BuildDotSet(target, source.Count);
        foreach (var dot in source)
        {
            if (seen.Add(dot)) added++;
        }
        return added;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int FoldOptimized(List<OrSetDot> target, List<OrSetDot> source)
    {
        var added = 0;
        if (source.Count <= LinearThreshold)
        {
            foreach (var dot in source)
            {
                if (!target.Contains(dot)) added++;
            }
            return added;
        }
        var seen = BuildDotSet(target, source.Count);
        foreach (var dot in source)
        {
            if (seen.Add(dot)) added++;
        }
        return added;
    }

    // Mirrors the internal OrSetDotSet.Build the production fold calls: presize
    // to the target plus the additions the caller will make, then fill through
    // the List<T> struct enumerator.
    private static HashSet<OrSetDot> BuildDotSet(List<OrSetDot> dots, int extraCapacity)
    {
        var set = new HashSet<OrSetDot>(dots.Count + extraCapacity);
        foreach (var dot in dots) set.Add(dot);
        return set;
    }

    // Lazily (re)builds the accumulated dot list to the requested length. Its
    // dots use a distinct replica id so no incoming dot matches - every incoming
    // dot is novel, isolating the membership-decision cost rather than the
    // result.
    private void EnsureAccumulated(int accumulatedCount)
    {
        if (_accumulated is not null && _accumulated.Count == accumulatedCount) return;
        var acc = new List<OrSetDot>(accumulatedCount);
        for (var i = 0; i < accumulatedCount; i++)
        {
            acc.Add(new OrSetDot { ReplicaId = "acc-" + i.ToString("D6", CultureInfo.InvariantCulture), Counter = i });
        }
        _accumulated = acc;
    }
}
