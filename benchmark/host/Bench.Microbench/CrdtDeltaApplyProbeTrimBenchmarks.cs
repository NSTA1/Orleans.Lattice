using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.InteropServices;

using BenchmarkDotNet.Attributes;

using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates three per-call trims on the CRDT delta-apply path, so their time
/// and byte deltas are measurable in the clear rather than buried under a silo,
/// a grain call and a transport. Each is a different <b>shape</b>, which is why
/// they are measured together: one removes an allocation, one removes struct
/// copies and an interface dispatch, one removes a list enumerator.
/// <para>
/// (1) <b>A capturing adapter lambda in the delta-run fold.</b> The N-ary union
/// helpers in <c>CrdtShapeDeltaRunFold</c> sized their accumulator with
/// <c>SumCounts(run, o =&gt; select(o)?.Count ?? 0)</c>. That reads as free, but
/// the lambda <b>captures <c>select</c></b>, so Roslyn cannot cache it in a
/// static singleton: every call heap-allocates a display class <i>and</i> a
/// <see cref="Func{T, TResult}"/> closed over it, and every element then pays a
/// second delegate hop on top of the one it already owes. The folds that size
/// this way run it two to three times apiece per applied delta run.
/// </para>
/// <para>
/// (2) <b>A <c>Nullable&lt;T&gt;</c> probe result in the MV-register merge.</b>
/// The duplicate-dot probe returned <c>MvRegisterEntry?</c>, so every hit
/// copied the 24-byte entry into the nullable wrapper and the caller's
/// <c>is { } dup</c> copied it straight back out - two copies to answer a
/// question whose miss path needs no entry at all. Both merge loops run the
/// probe once per local entry, so the copies scale with the <b>product</b> of
/// the two entry counts. The probe now returns an index and the caller reads
/// the match only on the hit path.
/// </para>
/// <para>
/// <b>Group (2) is measured twice, because the two shipped call sites bound
/// different overloads and only one of them had an enumerator.</b>
/// <c>MergeFrom</c> folds <c>MvRegister.Entries</c>, a <c>List&lt;T&gt;</c>, so
/// it bound an overload that walked with <c>foreach</c> - a struct enumerator,
/// no allocation, but a 24-byte copy and a version re-check per element.
/// <c>MergeDelta</c> folds <c>MvRegisterDelta.Entries</c>, an
/// <c>IReadOnlyList&lt;T&gt;</c>, so it bound an overload that walked with the
/// indexer - an <b>interface dispatch</b> per element. Neither allocated, so
/// read both for Mean only; a claimed byte win in group (2) would be an
/// unfaithful baseline rather than a result.
/// </para>
/// <para>
/// (3) <b>A list enumerator in the flag dot union.</b> <c>OrFlag</c> and
/// <c>RwFlag</c> folded an incoming dot list into an accumulated one through
/// <c>foreach</c>. <c>OrSetDot</c> is a multi-field struct, so the enumerator's
/// <c>Current</c> copies it once before the <c>Contains</c>/<c>Add</c> call
/// copies it again, and the enumerator re-checks the list version on every
/// <c>MoveNext</c>. The loops now resolve the span once and read through
/// <c>ref readonly</c>.
/// </para>
/// <para>
/// <b>What group (3) adds over the existing <c>crdtdotscantrims</c> suite.</b>
/// That suite already evidences the generic span-walk shape on
/// <c>OrSetDotCompaction</c>, and this lane does not re-claim it. What is new
/// here is that the union loop <b>appends to the target while walking the
/// source's span</b>. Aliasing the two lists would therefore let an append
/// resize the array out from under a live span - the one precondition the
/// generic result does not carry - so the shipped code short-circuits a
/// self-union, and <c>[GlobalSetup]</c> asserts that guard holds.
/// </para>
/// <para>
/// <b>Group (3) is also measured twice, for the reason rule 6 exists.</b> The
/// end-to-end union lane has to materialise a fresh target per operation, and
/// at these widths that copy and the dedup set dominate the arm: the walk under
/// test is a few per-element nanoseconds inside a much larger block of
/// untouched work, which is a floor on the resolvable delta rather than a
/// measurement of it. The <c>Scan</c> lane therefore drives the identical walk
/// and the identical <c>Contains</c> probe but accumulates a count instead of
/// appending, so the arms differ <b>only</b> in enumerator versus span. Read
/// the <c>Scan</c> pair for the size of the trim and the <c>Union</c> pair for
/// what it is worth in situ.
/// </para>
/// <para>
/// <b>Baseline fidelity.</b> Every <c>*Baseline</c> lane is a verbatim copy of
/// the body it replaced - loop form included, which is the part that is easy to
/// get wrong and is what makes or breaks the result - together with the private
/// <c>DotLinearScanThreshold</c> mirrored from the flag primitives, so the arms
/// differ only in the trim under test. <c>[GlobalSetup]</c> asserts each pair
/// agrees before any timing is taken, so a lane that drifted from its baseline
/// fails the run rather than reporting a win.
/// </para>
/// <para>
/// <b>Read group (1) for Allocated and groups (2) and (3) for Mean.</b> Group
/// (1) removes a fixed two-object allocation per call, so its byte delta is
/// <b>flat in the input width</b>: it does not grow with the run, and the lane
/// must not be read as a per-element saving. Groups (2) and (3) move no heap
/// traffic at all. Nothing here starts a silo, so the suite is cheap enough to
/// run at <c>BENCH_MICROBENCH_FIDELITY=full</c>, which is the fidelity these
/// deltas should be judged at.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=crdtapplyprobetrims</c> (or
/// <c>--suite crdtapplyprobetrims</c>); see <c>Program.cs</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class CrdtDeltaApplyProbeTrimBenchmarks
{
    /// <summary>
    /// Mirrors the private constant of the same name in <c>OrFlag</c> and
    /// <c>RwFlag</c>, so the baseline arm takes the same branch the shipped arm
    /// takes at every width below.
    /// </summary>
    private const int DotLinearScanThreshold = 4;

    /// <summary>
    /// Width of the collections each group folds. <c>2</c> sits below
    /// <see cref="DotLinearScanThreshold"/> and is the control lane for group
    /// (3): it takes the linear branch, where span materialisation is a cost
    /// rather than a saving and the trim is expected to be flat or slightly
    /// negative. <c>8</c> and <c>32</c> take the hashed branch.
    /// </summary>
    [Params(2, 8, 32)]
    public int Width { get; set; }

    private List<object> _run = [];
    private Func<object, IReadOnlyList<OrSetDot>?> _select = static _ => null;

    private List<MvRegisterEntry> _localEntries = [];
    private List<MvRegisterEntry> _listRemote = [];
    private IReadOnlyList<MvRegisterEntry> _readOnlyRemote = Array.Empty<MvRegisterEntry>();

    private List<OrSetDot> _unionTarget = [];
    private List<OrSetDot> _unionSource = [];

    /// <summary>A delta-run element carrying one dot list, as the fold sees it.</summary>
    private sealed class RunElement(IReadOnlyList<OrSetDot> dots)
    {
        /// <summary>The element's dot list, as the selector resolves it.</summary>
        public IReadOnlyList<OrSetDot> Dots { get; } = dots;
    }

    /// <summary>
    /// Builds the three corpora and asserts each optimized lane agrees with its
    /// baseline before any timing is taken.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _run = [];
        for (var i = 0; i < Width; i++)
        {
            var dots = new List<OrSetDot>(Width);
            for (var j = 0; j < Width; j++)
            {
                dots.Add(new OrSetDot
                {
                    ReplicaId = "replica-" + i.ToString(CultureInfo.InvariantCulture),
                    Counter = j,
                });
            }

            _run.Add(new RunElement(dots));
        }

        _select = static o => ((RunElement)o).Dots;

        // Half the remote entries duplicate a local dot, so both the hit and
        // the miss path of the probe are exercised at every width.
        _localEntries = [];
        _listRemote = [];
        for (var i = 0; i < Width; i++)
        {
            _localEntries.Add(new MvRegisterEntry
            {
                ReplicaId = "replica-" + (i % 3).ToString(CultureInfo.InvariantCulture),
                Counter = i,
                Value = [(byte)i],
            });
            _listRemote.Add(new MvRegisterEntry
            {
                ReplicaId = "replica-" + (i % 3).ToString(CultureInfo.InvariantCulture),
                Counter = i % 2 == 0 ? i : i + Width,
                Value = [(byte)i],
            });
        }

        _readOnlyRemote = _listRemote;

        _unionTarget = [];
        _unionSource = [];
        for (var i = 0; i < Width; i++)
        {
            _unionTarget.Add(new OrSetDot { ReplicaId = "target", Counter = i });

            // Overlap the first half so the dedup branch is taken, not skipped.
            _unionSource.Add(new OrSetDot
            {
                ReplicaId = i < Width / 2 ? "target" : "source",
                Counter = i,
            });
        }

        AssertEquivalence();
    }

    private void AssertEquivalence()
    {
        if (SumSelectedCountsBaseline(_run, _select) != SumSelectedCounts(_run, _select))
        {
            throw new InvalidOperationException("Run-fold sizing arms disagree.");
        }

        if (MergeFromProbeFoldBaseline(_localEntries, _listRemote)
            != MergeFromProbeFold(_localEntries, _listRemote))
        {
            throw new InvalidOperationException("MergeFrom probe arms disagree.");
        }

        if (MergeDeltaProbeFoldBaseline(_localEntries, _readOnlyRemote)
            != MergeDeltaProbeFold(_localEntries, _readOnlyRemote))
        {
            throw new InvalidOperationException("MergeDelta probe arms disagree.");
        }

        if (UnionScanBaseline(_unionTarget, _unionSource) != UnionScan(_unionTarget, _unionSource))
        {
            throw new InvalidOperationException("Flag union scan arms disagree.");
        }

        var baselineUnion = new List<OrSetDot>(_unionTarget);
        UnionIntoBaseline(baselineUnion, _unionSource);
        var trimmedUnion = new List<OrSetDot>(_unionTarget);
        UnionInto(trimmedUnion, _unionSource);

        if (baselineUnion.Count != trimmedUnion.Count)
        {
            throw new InvalidOperationException("Flag union arms disagree on width.");
        }

        for (var i = 0; i < baselineUnion.Count; i++)
        {
            if (!baselineUnion[i].Equals(trimmedUnion[i]))
            {
                throw new InvalidOperationException("Flag union arms disagree on content.");
            }
        }

        // The trimmed union resolves the source's backing span before
        // appending, so a self-union would be a use-after-resize rather than
        // merely wasted work. Assert the shipped guard holds: the identity
        // union must leave the list untouched.
        var aliased = new List<OrSetDot>(_unionTarget);
        UnionInto(aliased, aliased);
        if (aliased.Count != _unionTarget.Count)
        {
            throw new InvalidOperationException("Self-union guard did not hold.");
        }
    }

    // --- Group 1: run-fold sizing ------------------------------------------

    /// <summary>Verbatim copy of the replaced sizing call, adapter lambda included.</summary>
    /// <returns>The summed dot count, so the lane cannot be elided.</returns>
    [Benchmark(Baseline = true, Description = "RunFoldSizing: capturing adapter lambda (baseline)")]
    public int RunFoldSizingBaseline() => SumSelectedCountsBaseline(_run, _select);

    /// <summary>Drives the already-constructed selector directly.</summary>
    /// <returns>The summed dot count, so the lane cannot be elided.</returns>
    [Benchmark(Description = "RunFoldSizing: selector driven directly")]
    public int RunFoldSizingTrimmed() => SumSelectedCounts(_run, _select);

    private static int SumCountsBaseline(IReadOnlyList<object> run, Func<object, int> count)
    {
        var total = 0;
        for (var i = 0; i < run.Count; i++)
        {
            total += count(run[i]);
        }

        return total;
    }

    private static int SumSelectedCountsBaseline<T>(
        IReadOnlyList<object> run,
        Func<object, IReadOnlyList<T>?> select) =>
        SumCountsBaseline(run, o => select(o)?.Count ?? 0);

    private static int SumSelectedCounts<T>(
        IReadOnlyList<object> run,
        Func<object, IReadOnlyList<T>?> select)
    {
        var total = 0;
        for (var i = 0; i < run.Count; i++)
        {
            total += select(run[i])?.Count ?? 0;
        }

        return total;
    }

    // --- Group 2a: MergeFrom probe (List<T> overload, foreach baseline) ----

    /// <summary>Verbatim copy of the replaced nullable probe and its caller shape.</summary>
    /// <returns>The folded hit payload length, so the lane cannot be elided.</returns>
    [Benchmark(Description = "MvProbe/MergeFrom: nullable result over foreach (baseline)")]
    public int MvProbeMergeFromBaseline() => MergeFromProbeFoldBaseline(_localEntries, _listRemote);

    /// <summary>Index-returning probe over a resolved span.</summary>
    /// <returns>The folded hit payload length, so the lane cannot be elided.</returns>
    [Benchmark(Description = "MvProbe/MergeFrom: index result over span")]
    public int MvProbeMergeFromTrimmed() => MergeFromProbeFold(_localEntries, _listRemote);

    private static MvRegisterEntry? FindDotBaseline(
        List<MvRegisterEntry> entries, string replicaId, long counter)
    {
        foreach (var entry in entries)
        {
            if (entry.Counter == counter && entry.ReplicaId == replicaId) return entry;
        }

        return null;
    }

    private static int MergeFromProbeFoldBaseline(
        List<MvRegisterEntry> local, List<MvRegisterEntry> remote)
    {
        var hits = 0;
        var localCount = local.Count;
        for (var i = 0; i < localCount; i++)
        {
            var entry = local[i];
            if (FindDotBaseline(remote, entry.ReplicaId, entry.Counter) is { } dup)
            {
                hits += dup.Value.Length;
            }
        }

        return hits;
    }

    private static int IndexOfDot(List<MvRegisterEntry> entries, string replicaId, long counter)
    {
        var span = CollectionsMarshal.AsSpan(entries);
        for (var i = 0; i < span.Length; i++)
        {
            ref readonly var entry = ref span[i];
            if (entry.Counter == counter && entry.ReplicaId == replicaId) return i;
        }

        return -1;
    }

    private static int MergeFromProbeFold(
        List<MvRegisterEntry> local, List<MvRegisterEntry> remote)
    {
        var hits = 0;
        var localCount = local.Count;
        for (var i = 0; i < localCount; i++)
        {
            var entry = local[i];
            var dupIndex = IndexOfDot(remote, entry.ReplicaId, entry.Counter);
            if (dupIndex >= 0)
            {
                hits += remote[dupIndex].Value.Length;
            }
        }

        return hits;
    }

    // --- Group 2b: MergeDelta probe (IReadOnlyList<T> overload, indexer) ---

    /// <summary>Verbatim copy of the replaced nullable probe over the interface indexer.</summary>
    /// <returns>The folded hit payload length, so the lane cannot be elided.</returns>
    [Benchmark(Description = "MvProbe/MergeDelta: nullable result over indexer (baseline)")]
    public int MvProbeMergeDeltaBaseline() =>
        MergeDeltaProbeFoldBaseline(_localEntries, _readOnlyRemote);

    /// <summary>Index-returning probe that resolves the backing span where it can.</summary>
    /// <returns>The folded hit payload length, so the lane cannot be elided.</returns>
    [Benchmark(Description = "MvProbe/MergeDelta: index result over resolved span")]
    public int MvProbeMergeDeltaTrimmed() => MergeDeltaProbeFold(_localEntries, _readOnlyRemote);

    private static MvRegisterEntry? FindDotBaseline(
        IReadOnlyList<MvRegisterEntry> entries, string replicaId, long counter)
    {
        for (var i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            if (entry.Counter == counter && entry.ReplicaId == replicaId) return entry;
        }

        return null;
    }

    private static int MergeDeltaProbeFoldBaseline(
        List<MvRegisterEntry> local, IReadOnlyList<MvRegisterEntry> remote)
    {
        var hits = 0;
        var localCount = local.Count;
        for (var i = 0; i < localCount; i++)
        {
            var entry = local[i];
            var dup = FindDotBaseline(remote, entry.ReplicaId, entry.Counter);
            if (dup is { } match)
            {
                hits += match.Value.Length;
            }
        }

        return hits;
    }

    /// <summary>
    /// Mirrors the shipped <c>IReadOnlyList</c> probe: resolve the backing span
    /// where the runtime type allows and fall back to the indexer otherwise.
    /// </summary>
    private static int IndexOfDot(
        IReadOnlyList<MvRegisterEntry> entries, string replicaId, long counter)
    {
        if (TryGetSpan(entries, out var span))
        {
            for (var i = 0; i < span.Length; i++)
            {
                ref readonly var entry = ref span[i];
                if (entry.Counter == counter && entry.ReplicaId == replicaId) return i;
            }

            return -1;
        }

        for (var i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            if (entry.Counter == counter && entry.ReplicaId == replicaId) return i;
        }

        return -1;
    }

    private static bool TryGetSpan<T>(IReadOnlyList<T>? list, out ReadOnlySpan<T> span)
    {
        switch (list)
        {
            case null:
                span = default;
                return true;
            case T[] array:
                span = array;
                return true;
            case List<T> concrete:
                span = CollectionsMarshal.AsSpan(concrete);
                return true;
            default:
                span = default;
                return false;
        }
    }

    private static int MergeDeltaProbeFold(
        List<MvRegisterEntry> local, IReadOnlyList<MvRegisterEntry> remote)
    {
        var hits = 0;
        var localCount = local.Count;
        for (var i = 0; i < localCount; i++)
        {
            var entry = local[i];
            var dupIndex = IndexOfDot(remote, entry.ReplicaId, entry.Counter);
            if (dupIndex >= 0)
            {
                hits += remote[dupIndex].Value.Length;
            }
        }

        return hits;
    }

    // --- Group 3a: flag dot union, isolated walk (rule 6) -------------------

    /// <summary>
    /// Drives the baseline walk and probe without the append, so the arms
    /// differ only in enumerator versus span.
    /// </summary>
    /// <returns>The count of already-present dots, so the lane cannot be elided.</returns>
    [Benchmark(Description = "FlagUnionScan: list enumerator, no append (baseline)")]
    public int FlagUnionScanBaseline() => UnionScanBaseline(_unionTarget, _unionSource);

    /// <summary>The same walk through a resolved span and <c>ref readonly</c>.</summary>
    /// <returns>The count of already-present dots, so the lane cannot be elided.</returns>
    [Benchmark(Description = "FlagUnionScan: span walk, no append")]
    public int FlagUnionScanTrimmed() => UnionScan(_unionTarget, _unionSource);

    private static int UnionScanBaseline(List<OrSetDot> target, List<OrSetDot> source)
    {
        var present = 0;
        foreach (var dot in source)
        {
            if (target.Contains(dot)) present++;
        }

        return present;
    }

    private static int UnionScan(List<OrSetDot> target, List<OrSetDot> source)
    {
        var present = 0;
        var span = CollectionsMarshal.AsSpan(source);
        for (var i = 0; i < span.Length; i++)
        {
            ref readonly var dot = ref span[i];
            if (target.Contains(dot)) present++;
        }

        return present;
    }

    // --- Group 3b: flag dot union, end to end ------------------------------

    /// <summary>Verbatim copy of the replaced union, enumerator and threshold gate included.</summary>
    /// <returns>The unioned width, so the lane cannot be elided.</returns>
    [Benchmark(Description = "FlagUnion: list enumerator, end to end (baseline)")]
    public int FlagUnionBaseline()
    {
        var target = new List<OrSetDot>(_unionTarget);
        UnionIntoBaseline(target, _unionSource);
        return target.Count;
    }

    /// <summary>Span walk through <c>ref readonly</c>, with the self-union guard.</summary>
    /// <returns>The unioned width, so the lane cannot be elided.</returns>
    [Benchmark(Description = "FlagUnion: span walk, end to end")]
    public int FlagUnionTrimmed()
    {
        var target = new List<OrSetDot>(_unionTarget);
        UnionInto(target, _unionSource);
        return target.Count;
    }

    private static void UnionIntoBaseline(List<OrSetDot> target, List<OrSetDot> source)
    {
        if (source.Count == 0) return;
        if (source.Count <= DotLinearScanThreshold)
        {
            foreach (var dot in source)
            {
                if (!target.Contains(dot)) target.Add(dot);
            }

            return;
        }

        var seen = OrSetDotSet.Build(target, source.Count);
        foreach (var dot in source)
        {
            if (seen.Add(dot)) target.Add(dot);
        }
    }

    private static void UnionInto(List<OrSetDot> target, List<OrSetDot> source)
    {
        if (source.Count == 0) return;
        if (ReferenceEquals(target, source)) return;

        var span = CollectionsMarshal.AsSpan(source);
        if (source.Count <= DotLinearScanThreshold)
        {
            for (var i = 0; i < span.Length; i++)
            {
                ref readonly var dot = ref span[i];
                if (!target.Contains(dot)) target.Add(dot);
            }

            return;
        }

        var seen = OrSetDotSet.Build(target, source.Count);
        for (var i = 0; i < span.Length; i++)
        {
            ref readonly var dot = ref span[i];
            if (seen.Add(dot)) target.Add(dot);
        }
    }
}
