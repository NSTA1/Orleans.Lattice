using System;
using System.Buffers;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;

using BenchmarkDotNet.Attributes;

using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates two trims on the CRDT <b>delta</b>-apply path - the path an inbound
/// replication delta takes - so their time and byte deltas are measurable in the
/// clear rather than buried under a silo, a grain call and a transport.
/// <para>
/// (1) <b>An interface-typed walk in the flag delta dot union.</b> The state
/// path (<c>UnionInto</c>) was span-walked some time ago; its delta twin
/// (<c>UnionDots</c>) was not, because the parameter is declared
/// <see cref="IReadOnlyList{T}"/> - it is serialised public surface and cannot
/// be narrowed. The loop that shipped before therefore paid an interface call
/// for the indexer <b>and</b> a second one for the re-read of <c>Count</c> in
/// the loop condition, on every dot, with <c>OrSetDot</c> - a multi-field
/// struct - returned whole by value from the indexer. <c>OrFlag</c> drives this
/// twice and <c>RwFlag</c> three times per applied delta. The trim resolves the
/// backing span where the runtime shape allows it and reads through
/// <c>ref readonly</c>, and splits the two width strategies into sibling
/// methods behind a thin dispatcher rather than fusing them into one body.
/// </para>
/// <para>
/// (2) <b>A pooled rental taken per element in the delta dot keying loops.</b>
/// <c>OrSet.MergeDelta</c> and <c>RwSet.UnionDeltaDots</c> key each incoming
/// element by its base64 encoding, through a stack buffer when it fits and a
/// pooled rental when it does not. The shape that shipped before opened an
/// exception-handling region <b>per element</b> and rented and returned per
/// oversized element, so a delta carrying n large elements paid n rent/return
/// round trips where one suffices. The trim hoists a single monotonically
/// growing rental, and its guarding region, to the whole walk.
/// </para>
/// <para>
/// <b>Both groups are measured twice, for the reason rule 6 exists.</b> An
/// end-to-end union lane has to materialise a fresh target per operation, and
/// at these widths that copy - and, in group (2), the key strings - dominate the
/// arm: the work under test is a few per-element nanoseconds inside a much
/// larger block of untouched work, which is a floor on the resolvable delta
/// rather than a measurement of it. Each <c>Scan</c> lane therefore drives the
/// identical walk and the identical probe but accumulates a count instead of
/// appending, so the arms differ <b>only</b> in the trim. Read the <c>Scan</c>
/// pair for the size of the trim and the end-to-end pair for what it is worth
/// in situ.
/// </para>
/// <para>
/// <b>Group (2) is measured at two element sizes, and they answer different
/// questions.</b> <c>Small</c> elements encode inside the 256-char stack
/// scratch, so <b>no rental is ever taken in either arm</b> and the lane
/// reports what the walk trim is worth with the rental arm dormant - the
/// removal of the per-element exception-handling region, plus the same span
/// resolution group (1) measures, since the shipped change does both in one
/// body. <c>Large</c> elements exceed it, so the baseline rents and returns
/// once per element while the trim rents once for the walk; the difference
/// between the two lanes is what the rental hoist itself is worth. Reporting
/// only the large lane would overstate the trim on a realistic corpus, and
/// reporting only the small lane would hide the rental hoist entirely.
/// </para>
/// <para>
/// <b>Rule 5: every threshold-gated lane carries a below-threshold control.</b>
/// <c>Width=4</c> is at <c>DotLinearScanThreshold</c>, so group (1) takes the
/// linear-probe arm; <c>Width=64</c> takes the indexed arm. Both widths are
/// reported, so a win claimed on one arm cannot be read as a win on the other.
/// </para>
/// <para>
/// <b>Rule 21: the baselines match the loop form, not merely the operations.</b>
/// Every <c>*Baseline</c> lane is a verbatim copy of the body it replaced -
/// indexer walk, <c>Count</c> re-read in the loop condition, per-element
/// <c>try</c>/<c>finally</c> and all - together with the private
/// <c>DotLinearScanThreshold</c> and <c>MaxStackBase64Chars</c> constants
/// mirrored from the primitives, so the arms differ only in the trim under
/// test. <c>[GlobalSetup]</c> asserts each pair agrees before any timing is
/// taken, so a lane that drifted from its baseline fails the run rather than
/// reporting a win.
/// </para>
/// <para>
/// <b>An unspannable control lane is carried for group (1).</b> A delta whose
/// collection is neither an array nor a <c>List&lt;T&gt;</c> is what
/// <c>CrdtDeltaListSpan.TryGetSpan</c> reports unspannable, and the trim must
/// not make that case slower. The control drives the trimmed dispatcher over a
/// <see cref="ReadOnlyCollection{T}"/> so the fallback arm is measured rather
/// than assumed.
/// </para>
/// <para>
/// <b>Read group (1) for Mean and group (2) for Mean.</b> Neither trim moves
/// heap traffic on the spannable path - no allocation is removed and none is
/// added - so a byte delta on any lane here is a lane bug, not a result.
/// Nothing in this suite starts a silo, so it is cheap enough to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>, which is the fidelity these deltas
/// should be judged at.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=crdtdeltauniontrims</c> (or
/// <c>--suite crdtdeltauniontrims</c>); see <c>Program.cs</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class CrdtDeltaUnionTrimBenchmarks
{
    /// <summary>
    /// Mirrors the private constant of the same name in <c>OrFlag</c> and
    /// <c>RwFlag</c>, so the baseline arm takes the same branch the shipped arm
    /// takes at every width below.
    /// </summary>
    private const int DotLinearScanThreshold = 4;

    /// <summary>
    /// Mirrors the private constant of the same name in <c>OrSet</c> and
    /// <c>RwSet</c>: elements whose base64 encoding fits in this many chars are
    /// keyed through a stack buffer, larger ones through a pooled rental.
    /// </summary>
    private const int MaxStackBase64Chars = 256;

    /// <summary>Element byte length that encodes inside the stack scratch.</summary>
    private const int SmallElementBytes = 48;

    /// <summary>Element byte length that forces a pooled rental.</summary>
    private const int LargeElementBytes = 512;

    /// <summary>
    /// Incoming dot count. <c>4</c> is the below-threshold control (rule 5) and
    /// takes the linear-probe arm; <c>64</c> takes the indexed arm.
    /// </summary>
    [Params(4, 64)]
    public int Width { get; set; }

    private OrSetDot[] _deltaDots = [];
    private ReadOnlyCollection<OrSetDot> _unspannableDeltaDots = null!;
    private List<OrSetDot> _unionTarget = [];

    private OrSetDeltaDot[] _smallDeltaDots = [];
    private OrSetDeltaDot[] _largeDeltaDots = [];
    private Dictionary<string, List<OrSetDot>> _smallTarget = [];
    private Dictionary<string, List<OrSetDot>> _largeTarget = [];

    /// <summary>
    /// Builds the four corpora and asserts each optimised lane agrees with its
    /// baseline - including on the unspannable corpus, which is the shape the
    /// trim's fallback arm exists for - before any timing is taken.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        // Half the incoming dots already sit in the accumulated list, so both
        // the hit and the miss path of the probe are exercised at every width.
        _unionTarget = [];
        var dots = new OrSetDot[Width];
        for (var i = 0; i < Width; i++)
        {
            dots[i] = new OrSetDot
            {
                ReplicaId = "replica-" + (i % 4).ToString(CultureInfo.InvariantCulture),
                Counter = i,
            };
            if (i % 2 == 0) _unionTarget.Add(dots[i]);
        }

        _deltaDots = dots;
        _unspannableDeltaDots = new ReadOnlyCollection<OrSetDot>(dots);

        _smallDeltaDots = BuildDeltaDots(Width, SmallElementBytes);
        _largeDeltaDots = BuildDeltaDots(Width, LargeElementBytes);

        // The keying lanes re-apply an already-applied delta: redelivery is the
        // documented idempotency case, it keeps the operation repeatable, and it
        // keeps the key strings out of the measurement so the rental trim is not
        // buried under allocation the trim does not touch.
        _smallTarget = [];
        UnionDeltaDotsTrimmed(_smallTarget, _smallDeltaDots);
        _largeTarget = [];
        UnionDeltaDotsTrimmed(_largeTarget, _largeDeltaDots);

        AssertUnionAgrees(_deltaDots);
        AssertUnionAgrees(_unspannableDeltaDots);
        AssertKeyingAgrees(_smallDeltaDots);
        AssertKeyingAgrees(_largeDeltaDots);
    }

    private static OrSetDeltaDot[] BuildDeltaDots(int width, int elementBytes)
    {
        var dots = new OrSetDeltaDot[width];
        for (var i = 0; i < width; i++)
        {
            var element = new byte[elementBytes];
            for (var b = 0; b < elementBytes; b++) element[b] = (byte)((i * 31) + b);
            dots[i] = new OrSetDeltaDot
            {
                Element = element,
                ReplicaId = "replica-" + (i % 4).ToString(CultureInfo.InvariantCulture),
                Counter = i,
            };
        }

        return dots;
    }

    private void AssertUnionAgrees(IReadOnlyList<OrSetDot> source)
    {
        var baseline = new List<OrSetDot>(_unionTarget);
        UnionDotsBaseline(baseline, source);
        var trimmed = new List<OrSetDot>(_unionTarget);
        UnionDotsTrimmed(trimmed, source);
        if (baseline.Count != trimmed.Count)
        {
            throw new InvalidOperationException(
                $"Union arms disagree on width: {baseline.Count} vs {trimmed.Count}.");
        }

        for (var i = 0; i < baseline.Count; i++)
        {
            if (!baseline[i].Equals(trimmed[i]))
            {
                throw new InvalidOperationException($"Union arms disagree at index {i}.");
            }
        }
    }

    private static void AssertKeyingAgrees(IReadOnlyList<OrSetDeltaDot> source)
    {
        Dictionary<string, List<OrSetDot>> baseline = [];
        UnionDeltaDotsBaseline(baseline, source);
        Dictionary<string, List<OrSetDot>> trimmed = [];
        UnionDeltaDotsTrimmed(trimmed, source);
        if (baseline.Count != trimmed.Count)
        {
            throw new InvalidOperationException(
                $"Keying arms disagree on key count: {baseline.Count} vs {trimmed.Count}.");
        }

        foreach (var (key, baselineDots) in baseline)
        {
            if (!trimmed.TryGetValue(key, out var trimmedDots) || trimmedDots.Count != baselineDots.Count)
            {
                throw new InvalidOperationException($"Keying arms disagree on key {key}.");
            }

            for (var i = 0; i < baselineDots.Count; i++)
            {
                if (!baselineDots[i].Equals(trimmedDots[i]))
                {
                    throw new InvalidOperationException($"Keying arms disagree on key {key} at index {i}.");
                }
            }
        }
    }

    // --- Group 1a: flag delta dot union, isolated walk (rule 6) -------------

    /// <summary>
    /// Drives the baseline walk and probe without the append, so the arms differ
    /// only in interface indexer versus resolved span.
    /// </summary>
    /// <returns>The count of already-present dots, so the lane cannot be elided.</returns>
    [Benchmark(Description = "DeltaDotScan: interface indexer, no append (baseline)")]
    public int DeltaDotScanBaseline()
    {
        var present = 0;
        var source = (IReadOnlyList<OrSetDot>)_deltaDots;
        for (var i = 0; i < source.Count; i++)
        {
            var dot = source[i];
            if (_unionTarget.Contains(dot)) present++;
        }

        return present;
    }

    /// <summary>The same walk through a resolved span and <c>ref readonly</c>.</summary>
    /// <returns>The count of already-present dots, so the lane cannot be elided.</returns>
    [Benchmark(Description = "DeltaDotScan: resolved span, no append")]
    public int DeltaDotScanTrimmed()
    {
        var present = 0;
        if (!CrdtDeltaListSpan.TryGetSpan(_deltaDots, out var span)) return -1;
        for (var i = 0; i < span.Length; i++)
        {
            ref readonly var dot = ref span[i];
            if (_unionTarget.Contains(dot)) present++;
        }

        return present;
    }

    // --- Group 1b: flag delta dot union, end to end ------------------------

    /// <summary>Verbatim copy of the replaced union, indexer and threshold gate included.</summary>
    /// <returns>The unioned width, so the lane cannot be elided.</returns>
    [Benchmark(Description = "DeltaDotUnion: interface indexer, end to end (baseline)")]
    public int DeltaDotUnionBaseline()
    {
        var target = new List<OrSetDot>(_unionTarget);
        UnionDotsBaseline(target, _deltaDots);
        return target.Count;
    }

    /// <summary>Resolved span and split width strategies, as shipped.</summary>
    /// <returns>The unioned width, so the lane cannot be elided.</returns>
    [Benchmark(Description = "DeltaDotUnion: resolved span + split, end to end")]
    public int DeltaDotUnionTrimmed()
    {
        var target = new List<OrSetDot>(_unionTarget);
        UnionDotsTrimmed(target, _deltaDots);
        return target.Count;
    }

    /// <summary>
    /// Control: the trimmed dispatcher over a collection <c>TryGetSpan</c>
    /// reports unspannable, so the fallback arm is measured rather than assumed.
    /// </summary>
    /// <returns>The unioned width, so the lane cannot be elided.</returns>
    [Benchmark(Description = "DeltaDotUnion: unspannable collection, fallback arm (control)")]
    public int DeltaDotUnionUnspannable()
    {
        var target = new List<OrSetDot>(_unionTarget);
        UnionDotsTrimmed(target, _unspannableDeltaDots);
        return target.Count;
    }

    /// <summary>
    /// Verbatim copy of the <c>UnionDots</c> body that shipped before this
    /// change: the interface indexer, and <c>Count</c> re-read in the loop
    /// condition, in both width arms of one fused body.
    /// </summary>
    private static void UnionDotsBaseline(List<OrSetDot> target, IReadOnlyList<OrSetDot>? source)
    {
        if (source is not { Count: > 0 }) return;
        if (source.Count <= DotLinearScanThreshold)
        {
            for (var i = 0; i < source.Count; i++)
            {
                var dot = source[i];
                if (!target.Contains(dot)) target.Add(dot);
            }
            return;
        }
        var seen = new HashSet<OrSetDot>(target);
        for (var i = 0; i < source.Count; i++)
        {
            var dot = source[i];
            if (seen.Add(dot)) target.Add(dot);
        }
    }

    /// <summary>Mirrors the shipped dispatcher and its two sibling width arms.</summary>
    private static void UnionDotsTrimmed(List<OrSetDot> target, IReadOnlyList<OrSetDot>? source)
    {
        if (source is not { Count: > 0 }) return;
        if (ReferenceEquals(target, source)) return;
        if (!CrdtDeltaListSpan.TryGetSpan(source, out var span))
        {
            UnionDotsByIndex(target, source);
            return;
        }

        if (span.Length <= DotLinearScanThreshold)
        {
            UnionDotsNarrow(target, span);
            return;
        }

        UnionDotsWide(target, span);
    }

    private static void UnionDotsNarrow(List<OrSetDot> target, ReadOnlySpan<OrSetDot> source)
    {
        for (var i = 0; i < source.Length; i++)
        {
            ref readonly var dot = ref source[i];
            if (!target.Contains(dot)) target.Add(dot);
        }
    }

    private static void UnionDotsWide(List<OrSetDot> target, ReadOnlySpan<OrSetDot> source)
    {
        var seen = new HashSet<OrSetDot>(target);
        for (var i = 0; i < source.Length; i++)
        {
            ref readonly var dot = ref source[i];
            if (seen.Add(dot)) target.Add(dot);
        }
    }

    private static void UnionDotsByIndex(List<OrSetDot> target, IReadOnlyList<OrSetDot> source)
    {
        var count = source.Count;
        if (count <= DotLinearScanThreshold)
        {
            for (var i = 0; i < count; i++)
            {
                var dot = source[i];
                if (!target.Contains(dot)) target.Add(dot);
            }
            return;
        }
        var seen = new HashSet<OrSetDot>(target);
        for (var i = 0; i < count; i++)
        {
            var dot = source[i];
            if (seen.Add(dot)) target.Add(dot);
        }
    }

    // --- Group 2: delta dot keying, pooled rental --------------------------

    /// <summary>
    /// Small elements: no rental is taken in either arm, so this lane reports
    /// the walk and region trim with the rental arm dormant.
    /// </summary>
    /// <returns>The key count, so the lane cannot be elided.</returns>
    [Benchmark(Description = "DeltaKeySmall: region per element (baseline)")]
    public int DeltaKeySmallBaseline()
    {
        UnionDeltaDotsBaseline(_smallTarget, _smallDeltaDots);
        return _smallTarget.Count;
    }

    /// <summary>The same walk with the rental and its guarding region hoisted.</summary>
    /// <returns>The key count, so the lane cannot be elided.</returns>
    [Benchmark(Description = "DeltaKeySmall: hoisted rental")]
    public int DeltaKeySmallTrimmed()
    {
        UnionDeltaDotsTrimmed(_smallTarget, _smallDeltaDots);
        return _smallTarget.Count;
    }

    /// <summary>
    /// Large elements: the baseline rents and returns once per element while the
    /// trim rents once for the whole walk. Read against the small lane for what
    /// the rental hoist alone is worth.
    /// </summary>
    /// <returns>The key count, so the lane cannot be elided.</returns>
    [Benchmark(Description = "DeltaKeyLarge: rent per element (baseline)")]
    public int DeltaKeyLargeBaseline()
    {
        UnionDeltaDotsBaseline(_largeTarget, _largeDeltaDots);
        return _largeTarget.Count;
    }

    /// <summary>The same walk with one monotonically growing rental for the walk.</summary>
    /// <returns>The key count, so the lane cannot be elided.</returns>
    [Benchmark(Description = "DeltaKeyLarge: hoisted rental")]
    public int DeltaKeyLargeTrimmed()
    {
        UnionDeltaDotsTrimmed(_largeTarget, _largeDeltaDots);
        return _largeTarget.Count;
    }

    private static int Base64CharCount(int byteCount) => checked((byteCount + 2) / 3 * 4);

    /// <summary>
    /// Verbatim copy of the <c>UnionDeltaDots</c> body that shipped before this
    /// change: the interface indexer, and a pooled rental taken and returned
    /// inside a <c>try</c>/<c>finally</c> opened per element.
    /// </summary>
    private static void UnionDeltaDotsBaseline(
        Dictionary<string, List<OrSetDot>> target,
        IReadOnlyList<OrSetDeltaDot>? source)
    {
        if (source is not { Count: > 0 }) return;
        Span<char> scratch = stackalloc char[MaxStackBase64Chars];
        var lookup = target.GetAlternateLookup<ReadOnlySpan<char>>();
        for (var i = 0; i < source.Count; i++)
        {
            var dot = source[i];
            var element = dot.Element;
            if (element is null) continue;
            var charCount = Base64CharCount(element.Length);
            char[]? rented = charCount > scratch.Length ? ArrayPool<char>.Shared.Rent(charCount) : null;
            Span<char> buffer = rented ?? scratch;
            try
            {
                Convert.TryToBase64Chars(element, buffer, out var written);
                var key = buffer[..written];
                if (!lookup.TryGetValue(key, out var dots))
                {
                    dots = [];
                    target[new string(key)] = dots;
                }
                var entry = new OrSetDot { ReplicaId = dot.ReplicaId, Counter = dot.Counter };
                if (!dots.Contains(entry)) dots.Add(entry);
            }
            finally
            {
                if (rented is not null) ArrayPool<char>.Shared.Return(rented);
            }
        }
    }

    /// <summary>Mirrors the shipped split walk and its hoisted rental.</summary>
    private static void UnionDeltaDotsTrimmed(
        Dictionary<string, List<OrSetDot>> target,
        IReadOnlyList<OrSetDeltaDot>? source)
    {
        if (source is not { Count: > 0 }) return;
        Span<char> scratch = stackalloc char[MaxStackBase64Chars];
        var lookup = target.GetAlternateLookup<ReadOnlySpan<char>>();
        char[]? rented = null;
        try
        {
            if (CrdtDeltaListSpan.TryGetSpan(source, out var span))
            {
                for (var i = 0; i < span.Length; i++)
                {
                    ref readonly var dot = ref span[i];
                    AddDeltaDot(target, lookup, dot.Element, dot.ReplicaId, dot.Counter, scratch, ref rented);
                }
            }
            else
            {
                var count = source.Count;
                for (var i = 0; i < count; i++)
                {
                    var dot = source[i];
                    AddDeltaDot(target, lookup, dot.Element, dot.ReplicaId, dot.Counter, scratch, ref rented);
                }
            }
        }
        finally
        {
            if (rented is not null) ArrayPool<char>.Shared.Return(rented);
        }
    }

    private static void AddDeltaDot(
        Dictionary<string, List<OrSetDot>> target,
        Dictionary<string, List<OrSetDot>>.AlternateLookup<ReadOnlySpan<char>> lookup,
        byte[]? element,
        string replicaId,
        long counter,
        Span<char> scratch,
        ref char[]? rented)
    {
        if (element is null) return;
        var charCount = Base64CharCount(element.Length);
        Span<char> buffer;
        if (charCount > scratch.Length)
        {
            if (rented is null || rented.Length < charCount)
            {
                if (rented is not null) ArrayPool<char>.Shared.Return(rented);
                rented = ArrayPool<char>.Shared.Rent(charCount);
            }
            buffer = rented;
        }
        else
        {
            buffer = scratch;
        }
        Convert.TryToBase64Chars(element, buffer, out var written);
        var key = buffer[..written];
        if (!lookup.TryGetValue(key, out var dots))
        {
            dots = [];
            target[new string(key)] = dots;
        }
        var entry = new OrSetDot { ReplicaId = replicaId, Counter = counter };
        if (!dots.Contains(entry)) dots.Add(entry);
    }
}
