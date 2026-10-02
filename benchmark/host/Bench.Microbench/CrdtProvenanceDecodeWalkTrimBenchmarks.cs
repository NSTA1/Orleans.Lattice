using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates three trims on the CRDT <b>provenance decode</b> read path - the
/// projection <c>LatticeStateQuery</c> runs over a folded state or an author
/// delta batch to answer a provenance query. Each trim is a different shape.
/// <para>
/// (1) <b>A dictionary re-probe per element on the ordered current-value
/// walk.</b> <c>VersionVectorProvenanceDecoder.DecodeCurrentValue</c> needs its
/// members emitted in ordinal replica order, and got there by copying the
/// <b>keys alone</b> into a list, sorting that, and then <b>re-probing the
/// dictionary once per key</b> to fetch the clock the first pass had already
/// walked past. Every re-probe is a full string hash of the key, a bucket walk,
/// and an ordinal compare to confirm the hit. Collecting
/// <see cref="KeyValuePair{TKey,TValue}"/> into a <b>pooled</b> scratch window
/// instead keeps key and value together, so the sorted walk reads the clock
/// straight off the pair, the hashes disappear, and the key list the baseline
/// allocated per call disappears with them.
/// </para>
/// <para>
/// (2) <b>An interface-dispatched indexer per dot on the delta walk.</b> The
/// delta DTOs declare their dot collections as <see cref="IReadOnlyList{T}"/>
/// because they are serialised public surface and cannot name a concrete
/// container, so every <c>list[j]</c> in a decoder's inner loop is an interface
/// call that returns the element by value. <c>CrdtDeltaListSpan</c> exists in
/// the assembly precisely to resolve these to a span and its own documentation
/// names this call shape, but <b>no provenance decoder used it</b>: five inner
/// walks across <c>OrSet</c> (adds, removes), <c>Sequence</c> (inserts,
/// tombstones) and <c>GSet</c> (adds) kept the interface indexer.
/// </para>
/// <para>
/// (3) <b>A doubly-indirect indexer per element on <c>Rga.ToList</c>.</b> The
/// public copy-out walks the cached projection through
/// <see cref="IReadOnlyList{T}"/>, whose runtime type is a
/// <see cref="System.Collections.ObjectModel.ReadOnlyCollection{T}"/> wrapper -
/// so each element costs <b>two</b> virtual calls (wrapper then inner list) and
/// returns a 24-byte tuple by value. Caching the wrapper's own backing list in
/// a private field lets the copy-out walk a span instead, while the only shape
/// any caller can still obtain is the read-only wrapper.
/// </para>
/// <para>
/// <b>What this adds over the existing <c>crdtapplyprobetrims</c> suite.</b>
/// Rule 19 is about shape, not site. That suite's group (2) measures an
/// <see cref="IReadOnlyList{T}"/> walked through the indexer against a span
/// walk <i>in isolation</i>, which is group (2)'s shape here - so this suite
/// deliberately does <b>not</b> re-measure it in isolation and carries an
/// in-situ <c>DecodeDeltas</c> pair instead (rule 6), which is what says
/// whether the shape is worth anything where the decoder actually spends its
/// time. Groups (1) and (3) are shapes neither suite has measured.
/// </para>
/// <para>
/// <b>Baseline fidelity.</b> Every <c>*Baseline</c> lane is a verbatim copy of
/// the body it replaces, loop form included, with
/// <c>OrdinalStringOrder.Comparison</c> reached through
/// <c>InternalsVisibleTo</c> rather than re-spelled so the sort order is
/// provably the same one. <c>[GlobalSetup]</c> asserts each pair decodes to
/// identical output before any timing is taken, so a lane that drifted from its
/// baseline fails the run rather than reporting a win.
/// </para>
/// <para>
/// <b>How to read it.</b> Group (1) is a <b>time</b> trim that <i>costs</i>
/// bytes: a <c>KeyValuePair</c> array is 8 bytes per entry wider than a key
/// list on 64-bit, so its allocated column must <b>rise</b> with
/// <see cref="Width"/> and that is the trade, not a regression (rule 3). Read
/// its <c>Isolated</c> pair for the size of the trim and its
/// <c>VersionVector</c> pair for what it is worth in situ, where the per-entry
/// <c>Encoding.UTF8.GetBytes</c> is untouched work that dominates the arm
/// (rule 6). Group (2) moves <b>no</b> heap traffic at all - an indexer walk
/// boxes nothing (rule 21) - so read it for Mean only. Group (3)'s isolated
/// pair strips the per-element <c>ToArray</c> that dominates the in-situ lane,
/// for the same reason.
/// </para>
/// <para>
/// <b>Controls.</b> <c>DecodeDeltasUnspannable</c> drives the shipped decoder
/// over a collection that is neither an array nor a <see cref="List{T}"/>,
/// which is the case <c>CrdtDeltaListSpan.TryGetSpan</c> reports as
/// unspannable: it evidences that the shipped code keeps its interface walk and
/// its old cost on a container the trim cannot help, rather than regressing.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=crdtdecodewalktrims</c> (or
/// <c>--suite crdtdecodewalktrims</c>); see <c>Program.cs</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class CrdtProvenanceDecodeWalkTrimBenchmarks
{
    private const string ReplicaA = "replica-a";
    private const int ValueBytes = 24;

    /// <summary>
    /// Number of replicas in the frontier, dots in the delta, or nodes in the
    /// sequence. Group (1)'s saving is a hash per element, so it must scale;
    /// <c>4</c> is the narrow case where a re-probe is cheap relative to the
    /// sort and the trim should be close to flat.
    /// </summary>
    [Params(4, 64, 512)]
    public int Width { get; set; }

    private VersionVector _vector = null!;
    private Dictionary<string, long> _probeSource = null!;

    private CrdtProvenanceDelta[] _orSetDeltas = null!;
    private CrdtProvenanceDelta[] _unspannableOrSetDeltas = null!;

    private Rga _sequence = null!;
    private IReadOnlyList<(OrSetDot Dot, byte[] Value)> _materialized = null!;

    /// <summary>
    /// The same projection as <see cref="_materialized"/> held as the concrete
    /// list the shipped <c>Rga</c> now caches alongside its read-only wrapper,
    /// so the isolated group (3) pair differs only in how it is walked.
    /// </summary>
    private List<(OrSetDot Dot, byte[] Value)> _materializedList = null!;

    /// <summary>Builds the decode inputs the lanes project, and asserts each pair agrees.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _vector = new VersionVector();
        _probeSource = new Dictionary<string, long>(Width, StringComparer.Ordinal);
        for (var i = 0; i < Width; i++)
        {
            // Descending ids, so neither the sort nor the dictionary's own
            // enumeration order can be mistaken for the emitted order.
            var replicaId = $"replica-{Width - i:D5}";
            _vector.Tick(replicaId);
            _probeSource[replicaId] = i;
        }

        var dots = new OrSetDeltaDot[Width];
        var unspannable = new OrSetDeltaDot[Width];
        for (var i = 0; i < Width; i++)
        {
            var dot = new OrSetDeltaDot
            {
                Element = MakeValue(i),
                ReplicaId = ReplicaA,
                Counter = i + 1,
            };
            dots[i] = dot;
            unspannable[i] = dot;
        }

        _orSetDeltas =
        [
            new CrdtProvenanceDelta(
                new OrSetDelta { Adds = dots, Removes = Array.Empty<OrSetDeltaDot>() },
                _vector.GetClock(ReplicaA)),
        ];
        _unspannableOrSetDeltas =
        [
            new CrdtProvenanceDelta(
                new OrSetDelta
                {
                    Adds = new UnspannableList<OrSetDeltaDot>(unspannable),
                    Removes = Array.Empty<OrSetDeltaDot>(),
                },
                _vector.GetClock(ReplicaA)),
        ];

        _sequence = new Rga();
        var parent = Rga.Root;
        for (var i = 0; i < Width; i++)
        {
            parent = _sequence.InsertAfter(parent, ReplicaA, MakeValue(i));
        }
        _materialized = _sequence.MaterializeShared();
        _materializedList = new List<(OrSetDot Dot, byte[] Value)>(_materialized);

        // A baseline lane is only honest evidence if it decodes to exactly what
        // the shipped lane decodes to. Assert that here rather than trusting
        // the reproduction by eye.
        AssertSameValues(
            VersionVectorCurrentValueBaseline(),
            VersionVectorProvenanceDecoder.Instance.DecodeCurrentValue(_vector),
            "version-vector current value");
        AssertSameChanges(
            OrSetDecodeDeltasBaseline(_orSetDeltas),
            OrSetProvenanceDecoder.Instance.DecodeDeltas(_orSetDeltas),
            "or-set deltas (spannable)");
        AssertSameChanges(
            OrSetDecodeDeltasBaseline(_unspannableOrSetDeltas),
            OrSetProvenanceDecoder.Instance.DecodeDeltas(_unspannableOrSetDeltas),
            "or-set deltas (unspannable control)");
        AssertSameSequence(RgaToListBaseline(), _sequence.ToList(), "rga to-list");

        // The control must actually be unspannable, or it is not a control.
        if (CrdtDeltaListSpan.TryGetSpan(
            ((OrSetDelta)_unspannableOrSetDeltas[0].Delta).Adds, out _))
        {
            throw new InvalidOperationException(
                "The unspannable control resolved to a span, so it does not exercise the fallback.");
        }

        // ...and the primary corpus must be spannable, or the trim lane is void.
        if (!CrdtDeltaListSpan.TryGetSpan(((OrSetDelta)_orSetDeltas[0].Delta).Adds, out _))
        {
            throw new InvalidOperationException(
                "The primary delta corpus is not spannable, so the trim lane measures nothing.");
        }

        if (ProbeKeyListBaseline() != ProbePairSortShipped())
        {
            throw new InvalidOperationException("The isolated group (1) lanes disagree.");
        }
    }

    // ========================================================================
    // (1) sorted-key re-probe collapse
    // ========================================================================

    /// <summary>
    /// The prior shape in isolation: copy the keys out, sort them, then re-probe
    /// the dictionary once per key to fetch the value. Reproduced here because
    /// the production methods no longer contain it.
    /// </summary>
    [Benchmark(Description = "(1) sorted re-probe: key list + per-key probe [isolated baseline]")]
    public long ProbeKeyListBaseline()
    {
        var source = _probeSource;
        var keys = new List<string>(source.Count);
        foreach (var key in source.Keys) keys.Add(key);
        keys.Sort(OrdinalStringOrder.Comparison);

        var total = 0L;
        foreach (var key in keys)
        {
            total += source[key];
        }

        return total;
    }

    /// <summary>
    /// The shipped shape in isolation: rent a scratch pair window, take key and
    /// value together in the one pass, sort the window, read the value off the
    /// pair. Removes one string hash per entry, and - because the window is
    /// rented rather than allocated - removes the baseline's key-list array too.
    /// </summary>
    [Benchmark(Description = "(1) sorted re-probe: pooled pair window + sort [isolated shipped]")]
    public long ProbePairSortShipped()
    {
        var source = _probeSource;
        var count = source.Count;
        var pairs = ArrayPool<KeyValuePair<string, long>>.Shared.Rent(count);
        try
        {
            var next = 0;
            foreach (var pair in source) pairs[next++] = pair;

            var window = pairs.AsSpan(0, count);
            window.Sort(static (x, y) => OrdinalStringOrder.Comparison(x.Key, y.Key));

            var total = 0L;
            foreach (var (_, value) in window)
            {
                total += value;
            }

            return total;
        }
        finally
        {
            Array.Clear(pairs, 0, count);
            ArrayPool<KeyValuePair<string, long>>.Shared.Return(pairs);
        }
    }

    /// <summary>
    /// Verbatim copy of the pre-trim <c>VersionVectorProvenanceDecoder</c>
    /// current-value body, so the in-situ comparison pays the same untouched
    /// per-entry <c>Encoding.UTF8.GetBytes</c> the shipped lane pays.
    /// </summary>
    [Benchmark(Description = "(1) version-vector current value [in-situ baseline]")]
    public IReadOnlyList<CrdtMemberValue> VersionVectorCurrentValueBaseline()
    {
        var entries = _vector.Entries;
        if (entries.Count == 0) return Array.Empty<CrdtMemberValue>();

        var replicas = new List<string>(entries.Count);
        foreach (var replicaId in entries.Keys) replicas.Add(replicaId);
        replicas.Sort(OrdinalStringOrder.Comparison);

        var result = new List<CrdtMemberValue>(replicas.Count);
        foreach (var replicaId in replicas)
        {
            var clock = entries[replicaId];
            result.Add(new CrdtMemberValue
            {
                Element = Encoding.UTF8.GetBytes(replicaId),
                ReplicaId = replicaId,
                Ordinal = clock.Counter,
            });
        }

        return result;
    }

    /// <summary>The shipped decoder over the same frontier.</summary>
    [Benchmark(Description = "(1) version-vector current value [in-situ shipped]")]
    public IReadOnlyList<CrdtMemberValue> VersionVectorCurrentValueShipped() =>
        VersionVectorProvenanceDecoder.Instance.DecodeCurrentValue(_vector);

    // ========================================================================
    // (2) decoder inner dot walk
    // ========================================================================

    /// <summary>
    /// Verbatim copy of the pre-trim <c>OrSetProvenanceDecoder.DecodeDeltas</c>
    /// body: both inner dot walks dispatch the indexer through
    /// <see cref="IReadOnlyList{T}"/>.
    /// </summary>
    [Benchmark(Description = "(2) or-set DecodeDeltas: interface indexer [in-situ baseline]")]
    public IReadOnlyList<CrdtMemberChange> OrSetDecodeDeltasBaselineLane() =>
        OrSetDecodeDeltasBaseline(_orSetDeltas);

    /// <summary>The shipped decoder, which resolves both walks to a span.</summary>
    [Benchmark(Description = "(2) or-set DecodeDeltas: span walk [in-situ shipped]")]
    public IReadOnlyList<CrdtMemberChange> OrSetDecodeDeltasShipped() =>
        OrSetProvenanceDecoder.Instance.DecodeDeltas(_orSetDeltas);

    /// <summary>
    /// Control: the shipped decoder over an unspannable container, which keeps
    /// the interface walk. It must land on the baseline, not below it.
    /// </summary>
    [Benchmark(Description = "(2) or-set DecodeDeltas: unspannable [control]")]
    public IReadOnlyList<CrdtMemberChange> OrSetDecodeDeltasUnspannable() =>
        OrSetProvenanceDecoder.Instance.DecodeDeltas(_unspannableOrSetDeltas);

    // ========================================================================
    // (3) Rga.ToList copy-out walk
    // ========================================================================

    /// <summary>
    /// Verbatim copy of the pre-trim <c>Rga.ToList</c> body: the cached
    /// projection is walked through <see cref="IReadOnlyList{T}"/>, so every
    /// element pays two virtual calls into the read-only wrapper.
    /// </summary>
    [Benchmark(Description = "(3) Rga.ToList: interface walk [in-situ baseline]")]
    public IReadOnlyList<(OrSetDot Dot, byte[] Value)> RgaToListBaseline()
    {
        var shared = _sequence.MaterializeShared();
        var count = shared.Count;
        if (count == 0) return shared;

        var copy = new (OrSetDot, byte[])[count];
        for (var i = 0; i < count; i++)
        {
            var (dot, value) = shared[i];
            copy[i] = (dot, value.AsSpan().ToArray());
        }

        return copy;
    }

    /// <summary>The shipped copy-out, which walks the cache's backing list as a span.</summary>
    [Benchmark(Description = "(3) Rga.ToList: span walk [in-situ shipped]")]
    public IReadOnlyList<(OrSetDot Dot, byte[] Value)> RgaToListShipped() => _sequence.ToList();

    /// <summary>
    /// The walk alone, with the per-element <c>ToArray</c> that dominates the
    /// in-situ pair removed, so the trim's own size is visible (rule 6).
    /// </summary>
    [Benchmark(Description = "(3) Rga copy-out walk: interface indexer [isolated baseline]")]
    public long RgaWalkInterfaceBaseline()
    {
        var shared = _materialized;
        var count = shared.Count;
        var total = 0L;
        for (var i = 0; i < count; i++)
        {
            var (dot, value) = shared[i];
            total += dot.Counter + value.Length;
        }

        return total;
    }

    /// <summary>The same walk over an equivalent backing list's span.</summary>
    [Benchmark(Description = "(3) Rga copy-out walk: span [isolated shipped]")]
    public long RgaWalkSpanShipped()
    {
        var span = CollectionsMarshal.AsSpan(_materializedList);
        var total = 0L;
        for (var i = 0; i < span.Length; i++)
        {
            ref readonly var entry = ref span[i];
            total += entry.Dot.Counter + entry.Value.Length;
        }

        return total;
    }

    // ========================================================================
    // helpers
    // ========================================================================

    /// <summary>
    /// Verbatim copy of the pre-trim <c>OrSetProvenanceDecoder.DecodeDeltas</c>
    /// body, shared by the lane and the setup equivalence assertion.
    /// </summary>
    private static IReadOnlyList<CrdtMemberChange> OrSetDecodeDeltasBaseline(
        IReadOnlyList<CrdtProvenanceDelta> deltas)
    {
        if (deltas.Count == 0) return Array.Empty<CrdtMemberChange>();

        var total = 0;
        for (var i = 0; i < deltas.Count; i++)
        {
            var delta = (OrSetDelta)deltas[i].Delta;
            if (delta.Adds is { Count: > 0 } adds) total += adds.Count;
            if (delta.Removes is { Count: > 0 } removes) total += removes.Count;
        }
        if (total == 0) return Array.Empty<CrdtMemberChange>();

        var result = new List<CrdtMemberChange>(total);
        for (var i = 0; i < deltas.Count; i++)
        {
            var entry = deltas[i];
            var delta = (OrSetDelta)entry.Delta;
            var wallClock = entry.WallClock;

            var adds = delta.Adds;
            if (adds is { Count: > 0 })
            {
                for (var j = 0; j < adds.Count; j++)
                {
                    var dot = adds[j];
                    if (dot.Element is null) continue;
                    result.Add(new CrdtMemberChange
                    {
                        Element = dot.Element,
                        Kind = CrdtMemberChangeKind.Added,
                        ReplicaId = dot.ReplicaId,
                        Ordinal = dot.Counter,
                        WallClock = wallClock,
                    });
                }
            }

            var removes = delta.Removes;
            if (removes is { Count: > 0 })
            {
                for (var j = 0; j < removes.Count; j++)
                {
                    var dot = removes[j];
                    if (dot.Element is null) continue;
                    result.Add(new CrdtMemberChange
                    {
                        Element = dot.Element,
                        Kind = CrdtMemberChangeKind.Removed,
                        ReplicaId = dot.ReplicaId,
                        Ordinal = dot.Counter,
                        WallClock = wallClock,
                    });
                }
            }
        }

        return result;
    }

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

        /// <summary>An enumerator over the elements.</summary>
        public IEnumerator<T> GetEnumerator() => ((IEnumerable<T>)items).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => items.GetEnumerator();
    }

    private static byte[] MakeValue(int index)
    {
        var value = new byte[ValueBytes];
        value[0] = (byte)(index & 0xFF);
        value[1] = (byte)((index >> 8) & 0xFF);
        for (var b = 2; b < ValueBytes; b++)
        {
            value[b] = (byte)(b + index);
        }

        return value;
    }

    private static void AssertSameValues(
        IReadOnlyList<CrdtMemberValue> baseline,
        IReadOnlyList<CrdtMemberValue> shipped,
        string lane)
    {
        if (baseline.Count != shipped.Count)
        {
            throw new InvalidOperationException(
                $"[{lane}] baseline emitted {baseline.Count} members, shipped emitted {shipped.Count}.");
        }

        for (var i = 0; i < baseline.Count; i++)
        {
            var a = baseline[i];
            var b = shipped[i];
            if (a.Ordinal != b.Ordinal
                || !string.Equals(a.ReplicaId, b.ReplicaId, StringComparison.Ordinal)
                || !a.Element.AsSpan().SequenceEqual(b.Element))
            {
                throw new InvalidOperationException($"[{lane}] baseline and shipped diverge at member {i}.");
            }
        }
    }

    private static void AssertSameChanges(
        IReadOnlyList<CrdtMemberChange> baseline,
        IReadOnlyList<CrdtMemberChange> shipped,
        string lane)
    {
        if (baseline.Count != shipped.Count)
        {
            throw new InvalidOperationException(
                $"[{lane}] baseline emitted {baseline.Count} events, shipped emitted {shipped.Count}.");
        }

        for (var i = 0; i < baseline.Count; i++)
        {
            var a = baseline[i];
            var b = shipped[i];
            if (a.Kind != b.Kind
                || a.Ordinal != b.Ordinal
                || !string.Equals(a.ReplicaId, b.ReplicaId, StringComparison.Ordinal)
                || !a.Element.AsSpan().SequenceEqual(b.Element))
            {
                throw new InvalidOperationException($"[{lane}] baseline and shipped diverge at event {i}.");
            }
        }
    }

    private static void AssertSameSequence(
        IReadOnlyList<(OrSetDot Dot, byte[] Value)> baseline,
        IReadOnlyList<(OrSetDot Dot, byte[] Value)> shipped,
        string lane)
    {
        if (baseline.Count != shipped.Count)
        {
            throw new InvalidOperationException(
                $"[{lane}] baseline emitted {baseline.Count} entries, shipped emitted {shipped.Count}.");
        }

        for (var i = 0; i < baseline.Count; i++)
        {
            var a = baseline[i];
            var b = shipped[i];
            if (a.Dot.Counter != b.Dot.Counter
                || !string.Equals(a.Dot.ReplicaId, b.Dot.ReplicaId, StringComparison.Ordinal)
                || !a.Value.AsSpan().SequenceEqual(b.Value))
            {
                throw new InvalidOperationException($"[{lane}] baseline and shipped diverge at entry {i}.");
            }
        }
    }
}
