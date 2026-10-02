using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.InteropServices;

using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates three trims on the CRDT <c>MergeDelta</c> apply path, each a
/// different shape: one removes a heap allocation per walk, one removes a
/// per-element dictionary lookup construction, one removes the growth
/// reallocations a known-size batch of appends pays.
/// <para>
/// (1) <b>A boxed enumerator per delta walk.</b> The delta DTOs declare their
/// collections as <see cref="IReadOnlyList{T}"/> because they are serialised
/// public surface and cannot name a concrete container. A <c>foreach</c> over
/// that static type binds <see cref="IEnumerable{T}"/>'s
/// <c>GetEnumerator()</c>, which <b>heap-allocates a boxed enumerator</b> and
/// then dispatches <c>MoveNext</c>/<c>Current</c> through an interface on every
/// element, returning each struct element by value. <c>CrdtDeltaListSpan</c>
/// exists in the assembly precisely to resolve these to a span, and its own
/// documentation names this call shape - but six walks were never converted:
/// <c>OrSet.MergeDelta</c> (adds and removes), <c>OrMap.MergeDelta</c> (adds
/// and tombstones) and <c>Rga.MergeDelta</c> (inserts and tombstones).
/// </para>
/// <para>
/// <b>What group (1) adds over the existing <c>crdtapplyprobetrims</c> suite.</b>
/// Rule 19 is about shape, not site. That suite's group (3) measures
/// <c>foreach</c> over a <b>concrete <see cref="List{T}"/></b> - a struct
/// enumerator, which allocates nothing - against a span walk, so it evidences
/// only the copy and version-check saving. Its group (2) measures an
/// <see cref="IReadOnlyList{T}"/> walked through the <b>indexer</b>, which also
/// allocates nothing. Neither measures a <c>foreach</c> over
/// <see cref="IReadOnlyList{T}"/>, which is the only one of the three that
/// <b>allocates</b>. That is the shape here and it is new.
/// </para>
/// <para>
/// (2) <b>A dictionary alternate lookup constructed per dot.</b>
/// <c>OrSet.MergeDelta</c> called
/// <c>Adds.GetAlternateLookup&lt;ReadOnlySpan&lt;char&gt;&gt;()</c> <i>inside</i>
/// its per-dot loop, so every dot paid the lookup's comparer type-check and
/// struct construction afresh. Its own sibling <c>RwSet.UnionDeltaDots</c>
/// already hoists the identical call out of the identical loop, so this is the
/// one set primitive left behind rather than a new idea - and the sibling is
/// also the proof the hoist is sound, because both loops add keys to the very
/// dictionary the lookup is taken over.
/// </para>
/// <para>
/// (3) <b>Growth reallocation on a known-size insert batch.</b>
/// <c>Rga.MergeDelta</c> reads <c>delta.Inserts.Count</c> up front (it already
/// gates its dot index on that count) and then appends that many nodes to
/// <c>Nodes</c> one at a time, letting <see cref="List{T}"/> double its backing
/// array as it goes, and sizes its <c>dot -&gt; node</c> dictionary to the
/// <i>pre-merge</i> node count even though it is about to insert into it. Both
/// counts are known before the loop starts.
/// </para>
/// <para>
/// <b>Baseline fidelity.</b> Every <c>*Baseline</c> lane is a verbatim copy of
/// the body it replaces - loop form included, which is the part that decides
/// the result - with <c>OrSet</c>'s <c>MaxStackBase64Chars</c> and
/// <c>Base64CharCount</c> reached through <c>InternalsVisibleTo</c> rather than
/// re-spelled, and <c>Rga</c>'s private <c>MergeLinearScanThreshold</c>
/// mirrored below. <c>[GlobalSetup]</c> asserts each pair agrees before any
/// timing is taken, so a lane that drifted from its baseline fails the run
/// rather than reporting a win.
/// </para>
/// <para>
/// <b>How to read it.</b> Group (1) removes <b>one</b> boxed enumerator per
/// walk, so its byte delta is <b>flat in <see cref="Width"/></b> - it is a
/// per-call saving, not a per-element one, and must not be read as the latter
/// (rule 20); its <i>time</i> saving is per-element, because the interface
/// dispatch it removes is paid per element. Group (2) moves no heap traffic at
/// all: read it for Mean only, and read the <c>Isolated</c> lane for the size
/// of the trim and the <c>MergeDelta</c> pair for what it is worth in situ
/// (rule 6), where the base64 transcode and the dictionary probe it sits
/// between are untouched work that dominates the arm. Group (3) removes
/// reallocation, so its byte saving <b>grows</b> with <see cref="Width"/>.
/// </para>
/// <para>
/// <b>Controls.</b> <c>WalkUnspannableFallback</c> drives a collection that is
/// neither an array nor a <see cref="List{T}"/>, which is the case
/// <c>CrdtDeltaListSpan.TryGetSpan</c> reports as unspannable: it evidences
/// that the shipped code keeps the interface walk, and keeps its old cost,
/// rather than regressing on a container the trim cannot help. Group (3)'s
/// <see cref="Width"/> of 4 sits at <see cref="MergeLinearScanThreshold"/> and
/// is the below-threshold control, where the dictionary is never built.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=crdtapplywalktrims</c> (or
/// <c>--suite crdtapplywalktrims</c>); see <c>Program.cs</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class CrdtDeltaApplyWalkTrimBenchmarks
{
    /// <summary>
    /// Mirrors the private constant of the same name in <c>Rga</c>, so the
    /// group (3) arms take the same branch the shipped code takes at every
    /// width below.
    /// </summary>
    private const int MergeLinearScanThreshold = 4;

    /// <summary>
    /// Number of dots or nodes each delta carries. <c>4</c> sits at
    /// <see cref="MergeLinearScanThreshold"/> and is the below-threshold
    /// control for group (3). <c>16</c> and <c>64</c> cross it.
    /// </summary>
    [Params(4, 16, 64)]
    public int Width { get; set; }

    private OrSetDeltaDot[] _deltaDots = Array.Empty<OrSetDeltaDot>();
    private IReadOnlyList<OrSetDeltaDot> _spannableDots = Array.Empty<OrSetDeltaDot>();
    private IReadOnlyList<OrSetDeltaDot> _unspannableDots = Array.Empty<OrSetDeltaDot>();
    private OrSetDelta _orSetDelta;

    private RgaNode[] _seedNodes = Array.Empty<RgaNode>();
    private RgaDeltaNode[] _rgaInserts = Array.Empty<RgaDeltaNode>();

    /// <summary>
    /// A read-only list that is neither <c>T[]</c> nor <see cref="List{T}"/>,
    /// so <c>CrdtDeltaListSpan.TryGetSpan</c> reports it unspannable and the
    /// shipped code keeps its interface walk.
    /// </summary>
    private sealed class UnspannableList<T>(T[] items) : IReadOnlyList<T>
    {
        /// <summary>The element at <paramref name="index"/>.</summary>
        public T this[int index] => items[index];

        /// <summary>The element count.</summary>
        public int Count => items.Length;

        /// <summary>Enumerates the elements.</summary>
        public IEnumerator<T> GetEnumerator() => ((IEnumerable<T>)items).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => items.GetEnumerator();
    }

    /// <summary>
    /// Builds the corpora and asserts each optimized lane agrees with its
    /// baseline before any timing is taken.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _deltaDots = new OrSetDeltaDot[Width];
        for (var i = 0; i < Width; i++)
        {
            // Distinct elements, so every dot takes the miss path of the
            // dictionary probe and the loop body is the same work in both arms.
            _deltaDots[i] = new OrSetDeltaDot
            {
                Element = [(byte)i, (byte)(i >> 8), 0x5A],
                ReplicaId = "replica-" + (i % 3).ToString(CultureInfo.InvariantCulture),
                Counter = i,
            };
        }

        _spannableDots = _deltaDots;
        _unspannableDots = new UnspannableList<OrSetDeltaDot>(_deltaDots);
        _orSetDelta = new OrSetDelta { Adds = _deltaDots, Removes = Array.Empty<OrSetDeltaDot>() };

        _seedNodes = new RgaNode[Width];
        for (var i = 0; i < Width; i++)
        {
            _seedNodes[i] = new RgaNode
            {
                ReplicaId = "local",
                Counter = i,
                ParentDot = default,
                Value = [(byte)i],
                IsTombstone = false,
            };
        }

        _rgaInserts = new RgaDeltaNode[Width];
        for (var i = 0; i < Width; i++)
        {
            _rgaInserts[i] = new RgaDeltaNode
            {
                ReplicaId = "remote",
                Counter = i,
                ParentDot = default,
                Value = [(byte)i],
            };
        }

        AssertEquivalence();
    }

    private void AssertEquivalence()
    {
        if (WalkInterfaceForeachBaseline() != WalkSpan() || WalkSpan() != WalkUnspannableFallback())
        {
            throw new InvalidOperationException("Delta walk arms disagree.");
        }

        var baseline = OrSetMergeDeltaBaseline();
        var trimmed = OrSetMergeDelta();
        if (!SetsAgree(baseline, trimmed))
        {
            throw new InvalidOperationException("OrSet.MergeDelta arms disagree.");
        }

        // The trim must also hold on a delta whose collection is unspannable,
        // which is the precondition the span walk cannot assume away.
        var fallbackDelta = new OrSetDelta
        {
            Adds = new UnspannableList<OrSetDeltaDot>(_deltaDots),
            Removes = Array.Empty<OrSetDeltaDot>(),
        };
        var fallback = new OrSet();
        fallback.MergeDelta(fallbackDelta);
        if (!SetsAgree(baseline, fallback))
        {
            throw new InvalidOperationException("OrSet.MergeDelta disagrees on an unspannable delta.");
        }

        var growthBaseline = RgaInsertGrowthBaseline();
        var growthTrimmed = RgaInsertGrowth();
        var growthListOnly = RgaInsertGrowthListOnly();
        if (growthBaseline.Count != growthTrimmed.Count || growthBaseline.Count != growthListOnly.Count)
        {
            throw new InvalidOperationException("Rga insert arms disagree on node count.");
        }

        for (var i = 0; i < growthBaseline.Count; i++)
        {
            if (growthBaseline[i].ReplicaId != growthTrimmed[i].ReplicaId
                || growthBaseline[i].Counter != growthTrimmed[i].Counter)
            {
                throw new InvalidOperationException("Rga insert arms disagree on node order.");
            }
        }
    }

    private static bool SetsAgree(OrSet left, OrSet right)
    {
        if (left.Adds.Count != right.Adds.Count) return false;
        foreach (var (key, dots) in left.Adds)
        {
            if (!right.Adds.TryGetValue(key, out var other) || other.Count != dots.Count) return false;
            for (var i = 0; i < dots.Count; i++)
            {
                if (!other.Contains(dots[i])) return false;
            }
        }

        return true;
    }

    // ---------------------------------------------------------------------
    // Group (1): the delta walk itself, isolated from the loop body.
    // ---------------------------------------------------------------------

    /// <summary>
    /// Walks the delta's dots the way the shipped <c>MergeDelta</c> bodies did:
    /// <c>foreach</c> over the <see cref="IReadOnlyList{T}"/> static type, which
    /// boxes the enumerator onto the heap and dispatches every element through
    /// an interface.
    /// </summary>
    [Benchmark(Baseline = true, Description = "Walk: foreach over IReadOnlyList, boxed enumerator (baseline)")]
    public long WalkInterfaceForeachBaseline()
    {
        long sum = 0;
        foreach (var dot in _spannableDots)
        {
            sum += dot.Counter;
        }

        return sum;
    }

    /// <summary>
    /// Walks the same dots through <c>CrdtDeltaListSpan.TryGetSpan</c>: no
    /// enumerator is allocated and each element is read by reference.
    /// </summary>
    [Benchmark(Description = "Walk: resolved span, ref readonly")]
    public long WalkSpan()
    {
        long sum = 0;
        if (CrdtDeltaListSpan.TryGetSpan(_spannableDots, out var span))
        {
            for (var i = 0; i < span.Length; i++)
            {
                ref readonly var dot = ref span[i];
                sum += dot.Counter;
            }

            return sum;
        }

        for (var i = 0; i < _spannableDots.Count; i++)
        {
            sum += _spannableDots[i].Counter;
        }

        return sum;
    }

    /// <summary>
    /// Control lane: the same walk over a collection <c>TryGetSpan</c> reports
    /// unspannable, so it takes the indexer fallback the shipped code keeps.
    /// </summary>
    [Benchmark(Description = "Walk: unspannable collection, indexer fallback (control)")]
    public long WalkUnspannableFallback()
    {
        long sum = 0;
        if (CrdtDeltaListSpan.TryGetSpan(_unspannableDots, out var span))
        {
            for (var i = 0; i < span.Length; i++)
            {
                ref readonly var dot = ref span[i];
                sum += dot.Counter;
            }

            return sum;
        }

        for (var i = 0; i < _unspannableDots.Count; i++)
        {
            sum += _unspannableDots[i].Counter;
        }

        return sum;
    }

    // ---------------------------------------------------------------------
    // Group (2): the trims in situ on OrSet.MergeDelta.
    // ---------------------------------------------------------------------

    /// <summary>
    /// A verbatim copy of the shipped <c>OrSet.MergeDelta</c> adds loop before
    /// the trim: <c>foreach</c> over the interface, and the alternate lookup
    /// constructed inside the loop on every dot.
    /// </summary>
    [Benchmark(Description = "OrSetMergeDelta: boxed enumerator + per-dot lookup (baseline)")]
    public OrSet OrSetMergeDeltaBaseline()
    {
        var set = new OrSet();
        var adds = _orSetDelta.Adds;
        if (adds is { Count: > 0 })
        {
            Span<char> scratch = stackalloc char[OrSet.MaxStackBase64Chars];
            foreach (var dot in adds)
            {
                if (dot.Element is null) continue;
                var element = dot.Element;
                var charCount = OrSet.Base64CharCount(element.Length);
                char[]? rented = charCount > scratch.Length ? ArrayPool<char>.Shared.Rent(charCount) : null;
                Span<char> buffer = rented ?? scratch;
                try
                {
                    Convert.TryToBase64Chars(element, buffer, out var written);
                    var key = buffer[..written];
                    var addLookup = set.Adds.GetAlternateLookup<ReadOnlySpan<char>>();
                    if (!addLookup.TryGetValue(key, out var dots))
                    {
                        dots = [];
                        set.Adds[new string(key)] = dots;
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

        // The shipped method ends with Compact(), so the baseline pays it too -
        // otherwise this lane would be measuring strictly less work than the
        // arm it is compared against.
        set.Compact();
        return set;
    }

    /// <summary>
    /// The shipped <c>OrSet.MergeDelta</c>, carrying both trims: the span walk
    /// and the hoisted alternate lookup.
    /// </summary>
    [Benchmark(Description = "OrSetMergeDelta: span walk + hoisted lookup, end to end")]
    public OrSet OrSetMergeDelta()
    {
        var set = new OrSet();
        set.MergeDelta(_orSetDelta);
        return set;
    }

    /// <summary>
    /// Isolates trim (2) from the base64 transcode and dictionary probe it sits
    /// between: the same per-dot lookup construction the baseline paid, with no
    /// other work in the loop.
    /// </summary>
    [Benchmark(Description = "Lookup: alternate lookup built per dot (baseline)")]
    public int IsolatedLookupPerDotBaseline()
    {
        var set = new OrSet();
        var count = 0;
        for (var i = 0; i < Width; i++)
        {
            var lookup = set.Adds.GetAlternateLookup<ReadOnlySpan<char>>();
            if (lookup.TryGetValue("absent", out _)) count++;
        }

        return count;
    }

    /// <summary>
    /// The hoisted form: the lookup is constructed once for the whole walk.
    /// </summary>
    [Benchmark(Description = "Lookup: alternate lookup hoisted out of the walk")]
    public int IsolatedLookupHoisted()
    {
        var set = new OrSet();
        var count = 0;
        var lookup = set.Adds.GetAlternateLookup<ReadOnlySpan<char>>();
        for (var i = 0; i < Width; i++)
        {
            if (lookup.TryGetValue("absent", out _)) count++;
        }

        return count;
    }

    // ---------------------------------------------------------------------
    // Group (3): the Rga insert batch.
    // ---------------------------------------------------------------------

    /// <summary>
    /// A verbatim copy of the shipped <c>Rga.MergeDelta</c> insert growth
    /// before the trim: the node list grows by doubling as the batch is
    /// appended, and the dot index is sized to the pre-merge node count even
    /// though the batch is about to be inserted into it.
    /// </summary>
    [Benchmark(Description = "RgaInsert: growth by doubling, index sized pre-merge (baseline)")]
    public List<RgaNode> RgaInsertGrowthBaseline()
    {
        var nodes = new List<RgaNode>(_seedNodes);
        Dictionary<OrSetDot, RgaNode>? byDot = null;
        if (_rgaInserts.Length > MergeLinearScanThreshold)
        {
            byDot = new Dictionary<OrSetDot, RgaNode>(nodes.Count);
            foreach (var n in nodes) byDot[n.Dot] = n;
        }

        foreach (var ins in _rgaInserts)
        {
            var node = new RgaNode
            {
                ReplicaId = ins.ReplicaId,
                Counter = ins.Counter,
                ParentDot = ins.ParentDot,
                Value = ins.Value.AsSpan().ToArray(),
                IsTombstone = false,
            };
            nodes.Add(node);
            byDot?[ins.Dot] = node;
        }

        return nodes;
    }

    /// <summary>
    /// Splits the trim in half: the node list is sized once to the known batch
    /// count, but the dot index keeps its pre-merge sizing. Isolates the list
    /// growth saving from the index sizing change so each half can be judged on
    /// its own evidence.
    /// </summary>
    [Benchmark(Description = "RgaInsert: node list sized, index left pre-merge")]
    public List<RgaNode> RgaInsertGrowthListOnly()
    {
        var nodes = new List<RgaNode>(_seedNodes);
        var insertCount = _rgaInserts.Length;
        Dictionary<OrSetDot, RgaNode>? byDot = null;
        if (insertCount > MergeLinearScanThreshold)
        {
            byDot = new Dictionary<OrSetDot, RgaNode>(nodes.Count);
            foreach (var n in nodes) byDot[n.Dot] = n;
        }

        nodes.EnsureCapacity(nodes.Count + insertCount);
        foreach (var ins in _rgaInserts)
        {
            var node = new RgaNode
            {
                ReplicaId = ins.ReplicaId,
                Counter = ins.Counter,
                ParentDot = ins.ParentDot,
                Value = ins.Value.AsSpan().ToArray(),
                IsTombstone = false,
            };
            nodes.Add(node);
            byDot?[ins.Dot] = node;
        }

        return nodes;
    }

    /// <summary>
    /// The trimmed form: both the node list and the dot index are sized once to
    /// the count the batch is already known to carry.
    /// </summary>
    [Benchmark(Description = "RgaInsert: both sized to the known batch count")]
    public List<RgaNode> RgaInsertGrowth()
    {
        var nodes = new List<RgaNode>(_seedNodes);
        var insertCount = _rgaInserts.Length;
        Dictionary<OrSetDot, RgaNode>? byDot = null;
        if (insertCount > MergeLinearScanThreshold)
        {
            byDot = new Dictionary<OrSetDot, RgaNode>(nodes.Count + insertCount);
            foreach (var n in nodes) byDot[n.Dot] = n;
        }

        nodes.EnsureCapacity(nodes.Count + insertCount);
        foreach (var ins in _rgaInserts)
        {
            var node = new RgaNode
            {
                ReplicaId = ins.ReplicaId,
                Counter = ins.Counter,
                ParentDot = ins.ParentDot,
                Value = ins.Value.AsSpan().ToArray(),
                IsTombstone = false,
            };
            nodes.Add(node);
            byDot?[ins.Dot] = node;
        }

        return nodes;
    }
}
