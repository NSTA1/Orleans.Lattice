using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

using BenchmarkDotNet.Attributes;

using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates three CRDT provenance-decode paths an entry read runs so their time
/// and byte deltas are measurable in the clear rather than buried under a silo,
/// a grain call, and a transport. <c>LatticeStateQuery.DecodeMemberChanges</c>
/// drives one of these for <em>every</em> retained revision on every page of an
/// entry history, so the per-call work below is multiplied by the page size.
/// <para>
/// (1) <b>Multi-value register delta decode.</b> Two trims on the one method.
/// The result list was the last <c>DecodeDeltas</c> in the decoder family still
/// built with no capacity, so it doubled its way up from 4 while the exact bound
/// was already free in hand - each delta's <c>Entries.Count</c> and
/// <c>Context.Count</c> are counts of collections that are already
/// materialised, not a re-scan of their contents, which is the distinction that
/// decides whether a presize pays for itself at all. And <c>Emit</c> built a
/// <c>HashSet&lt;string&gt;</c> of live replicas on every call - three
/// allocations - to answer a membership question that, on a register holding one
/// or two concurrent values, a linear scan of the same tiny entry list answers
/// without allocating. The set is now built only above a threshold the steady
/// state never reaches.
/// </para>
/// <para>
/// (2) <b>Multi-value register current-value projection.</b> The projection
/// copied the entry list into a fresh <c>List</c> and sorted it before emitting,
/// unconditionally. A register holds one value except while a concurrent write
/// is unresolved, and a one-element sequence is already sorted, so the copy and
/// the sort were pure overhead on the shape that dominates. The single-valued
/// case now emits straight into an exactly-sized result.
/// </para>
/// <para>
/// (3) <b>OR-map folded-state decode and key projection.</b> Both emitters
/// appended into a sink left at capacity zero, so each climbed a doubling chain
/// of array allocations and copies before the first useful append. The map's
/// own dictionary counts bound both - exactly for the key projection, which
/// emits at most one member per key, and from below for the state decode, where
/// every non-empty key contributes at least one event - and a dictionary
/// <c>Count</c> is an O(1) field read, so the bound is free. Both sinks are now
/// sized from it up front. The delta emitter beside them already did this; these
/// two were the outliers.
/// </para>
/// <para>
/// Read every group for <b>bytes</b> first and time second: each trim removes
/// heap traffic (a doubling chain, a per-call hash set, a defensive copy)
/// rather than arithmetic, so the byte column is where the change is
/// unambiguous and the time column follows it through reduced GC pressure.
/// Each baseline lane is asserted in <see cref="Setup"/> to decode to exactly
/// the sequence its shipped counterpart decodes to - a lane that emits
/// something different is measuring different work, and the comparison would be
/// void.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=crdtprovenancedecode</c> (or
/// <c>--suite crdtprovenancedecode</c>); see <c>Program.cs</c>. No Orleans silo
/// is involved, so it runs cheaply at <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class CrdtProvenanceDecodeTrimBenchmarks
{
    // A history page's worth of register deltas: DecodeDeltas is called once per
    // revision with a single delta, so the per-revision lane below is the shape
    // the state query actually drives and the batched one its amortised bound.
    private const int DeltaCount = 64;
    private const int ContextReplicas = 4;

    // Representative of one retained revision's key population on an OR-map
    // backed index or membership map, large enough that the doubling chain the
    // presize removes is separable from the fixed cost of the decode.
    private const int MapKeyCount = 256;
    private const int ValueBytes = 24;

    private const string ReplicaA = "replica-a";
    private const string ReplicaB = "replica-b";

    private CrdtProvenanceDelta[] _mvDeltas = null!;
    private MvRegister _singleValued = null!;
    private OrMap<string, GCounter> _map = null!;
    private OrSet _compactedSet = null!;
    private OrSet _uncompactedSet = null!;

    /// <summary>Builds the decode inputs the lanes project.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _mvDeltas = new CrdtProvenanceDelta[DeltaCount];
        for (var i = 0; i < DeltaCount; i++)
        {
            _mvDeltas[i] = new CrdtProvenanceDelta(MakeRegisterDelta(i));
        }

        _singleValued = new MvRegister();
        _singleValued.Set(ReplicaA, MakeValue(1));

        _map = new OrMap<string, GCounter>();
        for (var i = 0; i < MapKeyCount; i++)
        {
            _map.Set($"key-{i:D4}", ReplicaA, new GCounter());
        }

        // A realistic retained revision is not pristine: remove a slice so the
        // state decode has genuine tombstone rows to emit alongside the adds.
        for (var i = 0; i < MapKeyCount; i += 4)
        {
            _map.Remove($"key-{i:D4}");
        }

        // A baseline lane is only honest evidence if it decodes to exactly what
        // the shipped lane decodes to. Assert that here rather than trusting the
        // reproduction by eye.
        AssertSameChanges(
            MvRegisterDeltasBaseline(),
            MvRegisterProvenanceDecoder.Instance.DecodeDeltas(_mvDeltas),
            "mv-register deltas");
        AssertSameValues(
            MvRegisterCurrentValueBaseline(),
            MvRegisterProvenanceDecoder.Instance.DecodeCurrentValue(_singleValued),
            "mv-register current value");
        AssertSameChanges(
            OrMapStateBaseline(),
            OrMapProvenanceDecoder.Instance.DecodeState(_map),
            "or-map state");
        AssertSameValues(
            OrMapCurrentValueBaseline(),
            OrMapProvenanceDecoder.Instance.DecodeCurrentValue(_map),
            "or-map current value");

        _compactedSet = BuildOrSet(compacted: true);
        _uncompactedSet = BuildOrSet(compacted: false);

        AssertSameChanges(
            OrSetStateBaseline(_compactedSet),
            OrSetProvenanceDecoder.Instance.DecodeState(_compactedSet),
            "or-set state (compacted)");
        AssertSameChanges(
            OrSetStateBaseline(_uncompactedSet),
            OrSetProvenanceDecoder.Instance.DecodeState(_uncompactedSet),
            "or-set state (uncompacted)");

        // The compacted corpus must actually SYNTHESIZE - that is the only path
        // the trim touches. A corpus that quietly stopped overflowing its
        // presize would make the primary lane a duplicate of the control and
        // the comparison would measure nothing.
        var compactedCount = OrSetProvenanceDecoder.Instance.DecodeState(_compactedSet).Count;
        var compactedDots = OrSetDotTotal(_compactedSet);
        if (compactedCount <= compactedDots)
        {
            throw new InvalidOperationException(
                "The compacted OrSet corpus does not overflow its presize, so the trim lane is void.");
        }

        // ...and the control corpus must NOT, or it is not a control.
        var uncompactedCount = OrSetProvenanceDecoder.Instance.DecodeState(_uncompactedSet).Count;
        if (uncompactedCount != OrSetDotTotal(_uncompactedSet))
        {
            throw new InvalidOperationException(
                "The control OrSet corpus synthesizes events, so it is not a below-threshold control.");
        }
    }

    private static int OrSetDotTotal(OrSet set)
    {
        var total = 0;
        foreach (var (_, dots) in set.Adds) total += dots.Count;
        foreach (var (_, dots) in set.Tombstones) total += dots.Count;
        return total;
    }

    /// <summary>
    /// Builds an OrSet whose decode either overflows its presize or does not.
    /// <para>
    /// Compacted: tombstones carry a DIFFERENT replica id from the adds, so no
    /// tombstone dot is present in the add list and every one synthesizes its
    /// Added half - which is what pushes the result past the dot total.
    /// Uncompacted: tombstones mirror add dots exactly, so nothing is
    /// synthesized and the presize is already exact.
    /// </para>
    /// </summary>
    private static OrSet BuildOrSet(bool compacted)
    {
        const int elements = 64;
        const int addsPerElement = 32;
        const int tombsPerElement = 4;

        var set = new OrSet();
        for (var e = 0; e < elements; e++)
        {
            var key = Convert.ToBase64String(Encoding.UTF8.GetBytes($"element-{e:D4}"));

            var adds = new List<OrSetDot>(addsPerElement);
            for (var i = 1; i <= addsPerElement; i++)
            {
                adds.Add(new OrSetDot { ReplicaId = ReplicaA, Counter = i });
            }

            var tombs = new List<OrSetDot>(tombsPerElement);
            for (var i = 1; i <= tombsPerElement; i++)
            {
                tombs.Add(new OrSetDot
                {
                    ReplicaId = compacted ? ReplicaB : ReplicaA,
                    Counter = i,
                });
            }

            set.Adds[key] = adds;
            set.Tombstones[key] = tombs;
        }

        return set;
    }

    // ========================================================================
    // (1) multi-value register delta decode
    // ========================================================================

    /// <summary>
    /// The prior shape: an uncapacitied result list, and a live-replica
    /// <c>HashSet</c> built on every emitted delta. Reproduced here because the
    /// production method no longer contains it.
    /// </summary>
    [Benchmark]
    public int MvRegisterDeltas_Baseline_UnsizedAndHashSetPerDelta() => MvRegisterDeltasBaseline().Count;

    /// <summary>
    /// The shipped shape: the <b>real production</b>
    /// <c>MvRegisterProvenanceDecoder.DecodeDeltas</c> - result presized from
    /// the free per-delta bound, live-replica set elided for a small register.
    /// </summary>
    [Benchmark]
    public int MvRegisterDeltas_Optimized_RealDecodeDeltas()
        => MvRegisterProvenanceDecoder.Instance.DecodeDeltas(_mvDeltas).Count;

    /// <summary>
    /// The single-delta call an entry-history read actually issues - one
    /// revision, one delta - so the per-call fixed cost the trims removed is
    /// visible without the batched lane's amortisation.
    /// </summary>
    [Benchmark]
    public int MvRegisterDeltas_Optimized_RealDecodePerRevision()
    {
        var total = 0;
        for (var i = 0; i < _mvDeltas.Length; i++)
        {
            total += MvRegisterProvenanceDecoder.Instance.DecodeDeltas(new[] { _mvDeltas[i] }).Count;
        }

        return total;
    }

    private List<CrdtMemberChange> MvRegisterDeltasBaseline()
    {
        var result = new List<CrdtMemberChange>();
        for (var i = 0; i < _mvDeltas.Length; i++)
        {
            var entry = _mvDeltas[i];
            var delta = (MvRegisterDelta)entry.Delta;
            var start = result.Count;

            HashSet<string>? liveReplicas = null;
            var entries = delta.Entries;
            if (entries is { Count: > 0 })
            {
                liveReplicas = new HashSet<string>(StringComparer.Ordinal);
                for (var j = 0; j < entries.Count; j++)
                {
                    var e = entries[j];
                    liveReplicas.Add(e.ReplicaId);
                    result.Add(new CrdtMemberChange
                    {
                        Element = e.Value ?? Array.Empty<byte>(),
                        Kind = CrdtMemberChangeKind.Added,
                        ReplicaId = e.ReplicaId,
                        Ordinal = e.Counter,
                        WallClock = entry.WallClock,
                    });
                }
            }

            if (delta.Context is { Count: > 0 } context)
            {
                foreach (var (replicaId, counter) in context)
                {
                    if (liveReplicas is not null && liveReplicas.Contains(replicaId)) continue;
                    result.Add(new CrdtMemberChange
                    {
                        Element = Array.Empty<byte>(),
                        Kind = CrdtMemberChangeKind.Removed,
                        ReplicaId = replicaId,
                        Ordinal = counter,
                        WallClock = entry.WallClock,
                    });
                }
            }

            result.Sort(start, result.Count - start, BaselineReplicaFirstOrder.Instance);
        }

        return result;
    }

    // ========================================================================
    // (2) multi-value register current-value projection
    // ========================================================================

    /// <summary>
    /// The prior shape: copy the entry list and sort it before emitting, even
    /// when it holds the single value the steady state holds. Reproduced here
    /// because the production method now short-circuits that case.
    /// </summary>
    [Benchmark]
    public int MvRegisterCurrentValue_Baseline_AlwaysCopyAndSort() => MvRegisterCurrentValueBaseline().Count;

    /// <summary>
    /// The shipped shape: the <b>real production</b>
    /// <c>MvRegisterProvenanceDecoder.DecodeCurrentValue</c>, which emits a
    /// single-valued register straight into an exactly-sized result.
    /// </summary>
    [Benchmark]
    public int MvRegisterCurrentValue_Optimized_RealDecodeCurrentValue()
        => MvRegisterProvenanceDecoder.Instance.DecodeCurrentValue(_singleValued).Count;

    private List<CrdtMemberValue> MvRegisterCurrentValueBaseline()
    {
        var entries = _singleValued.Entries;
        var ordered = new List<MvRegisterEntry>(entries);
        ordered.Sort(static (a, b) =>
        {
            var byReplica = string.CompareOrdinal(a.ReplicaId, b.ReplicaId);
            return byReplica != 0 ? byReplica : a.Counter.CompareTo(b.Counter);
        });

        var result = new List<CrdtMemberValue>(ordered.Count);
        for (var i = 0; i < ordered.Count; i++)
        {
            var e = ordered[i];
            result.Add(new CrdtMemberValue
            {
                Element = e.Value is null ? Array.Empty<byte>() : e.Value.AsSpan().ToArray(),
                ReplicaId = e.ReplicaId,
                Ordinal = e.Counter,
            });
        }

        return result;
    }

    // ========================================================================
    // (3) OR-map folded-state decode and key projection
    // ========================================================================

    /// <summary>
    /// The prior shape: append into a sink left at capacity zero, climbing a
    /// doubling chain. Reproduced here because the production emitter now sizes
    /// the sink from the map's free dictionary counts.
    /// </summary>
    [Benchmark]
    public int OrMapState_Baseline_UnsizedSink() => OrMapStateBaseline().Count;

    /// <summary>
    /// The shipped shape: the <b>real production</b>
    /// <c>OrMapProvenanceDecoder.DecodeState</c>.
    /// </summary>
    [Benchmark]
    public int OrMapState_Optimized_RealDecodeState()
        => OrMapProvenanceDecoder.Instance.DecodeState(_map).Count;

    /// <summary>The same unsized-sink shape at the key-projection seam.</summary>
    [Benchmark]
    public int OrMapCurrentValue_Baseline_UnsizedSink() => OrMapCurrentValueBaseline().Count;

    /// <summary>
    /// The shipped shape: the <b>real production</b>
    /// <c>OrMapProvenanceDecoder.DecodeCurrentValue</c>.
    /// </summary>
    [Benchmark]
    public int OrMapCurrentValue_Optimized_RealDecodeCurrentValue()
        => OrMapProvenanceDecoder.Instance.DecodeCurrentValue(_map).Count;

    // ========================================================================
    // (4) or-set state decode - compacted buffer growth
    // ========================================================================
    //
    // DecodeState presizes its result to the dot total, which is exact for an
    // uncompacted set. A COMPACTED set synthesizes an extra Added event per
    // tombstone dot that no longer survives in the add list, so the buffer
    // overflows and grows. It grows exactly once either way - the ceiling,
    // total + tombstoneDots, never exceeds 2 * total - so the saving is not in
    // the number of reallocations but in the SIZE of the one that happens:
    // List<T> doubles to 2 * total, where the provable ceiling is smaller by
    // the whole add-dot count.

    /// <summary>The prior shape: presize to the dot total and let the overflow double.</summary>
    [Benchmark]
    public int OrSetState_Baseline_DoublingGrowth() => OrSetStateBaseline(_compactedSet).Count;

    /// <summary>The shipped shape: the <b>real production</b> decoder, widening to the exact ceiling.</summary>
    [Benchmark]
    public int OrSetState_Optimized_RealDecodeState()
        => OrSetProvenanceDecoder.Instance.DecodeState(_compactedSet).Count;

    /// <summary>
    /// Control: an uncompacted set, where nothing is synthesized and the presize
    /// is already exact. Neither lane may grow at all, so the pair must show no
    /// separation - if it does, the primary delta is not the growth.
    /// </summary>
    [Benchmark]
    public int OrSetState_Control_Uncompacted_Baseline() => OrSetStateBaseline(_uncompactedSet).Count;

    /// <summary>Control: the shipped decoder over the same uncompacted set.</summary>
    [Benchmark]
    public int OrSetState_Control_Uncompacted_Optimized()
        => OrSetProvenanceDecoder.Instance.DecodeState(_uncompactedSet).Count;

    /// <summary>
    /// Verbatim copy of <c>OrSetProvenanceDecoder.DecodeState</c> as it stood
    /// before the trim: identical but for the exact-size widening, so the
    /// overflow is served by List&lt;T&gt;'s own doubling.
    /// </summary>
    private static List<CrdtMemberChange> OrSetStateBaseline(OrSet set)
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
        if (total == 0) return new List<CrdtMemberChange>();

        keys.Sort(StringComparer.Ordinal);

        var result = new List<CrdtMemberChange>(total);
        foreach (var key in keys)
        {
            var element = Convert.FromBase64String(key);
            var start = result.Count;

            adds.TryGetValue(key, out var addDots);
            if (addDots is not null)
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

            result.Sort(start, result.Count - start, BaselineCausalOrderComparer.Instance);
        }

        return result;
    }

    /// <summary>
    /// Verbatim copy of the decoder's private causal-order comparer, so the
    /// baseline lane sorts identically to the shipped one.
    /// </summary>
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

    private List<CrdtMemberChange> OrMapStateBaseline()
    {
        var result = new List<CrdtMemberChange>();

        foreach (var (key, entries) in _map.Adds)
        {
            if (entries.Count == 0) continue;
            var element = KeyToBytes(key);
            for (var i = 0; i < entries.Count; i++)
            {
                var e = entries[i];
                result.Add(new CrdtMemberChange
                {
                    Element = element,
                    Kind = CrdtMemberChangeKind.Added,
                    ReplicaId = e.ReplicaId,
                    Ordinal = e.Counter,
                    WallClock = null,
                });
            }
        }

        foreach (var (key, dots) in _map.Tombstones)
        {
            if (dots.Count == 0) continue;
            var element = KeyToBytes(key);
            for (var i = 0; i < dots.Count; i++)
            {
                var dot = dots[i];
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

        if (result.Count == 0) return result;
        result.Sort(BaselineOrMapElementOrder.Instance);
        return result;
    }

    private List<CrdtMemberValue> OrMapCurrentValueBaseline()
    {
        var result = new List<CrdtMemberValue>();

        foreach (var (key, entries) in _map.Adds)
        {
            if (entries.Count == 0) continue;
            _map.Tombstones.TryGetValue(key, out var tomb);

            var hasLive = false;
            var bestReplica = string.Empty;
            var bestCounter = long.MinValue;
            for (var i = 0; i < entries.Count; i++)
            {
                var entry = entries[i];
                if (IsEntryTombstoned(tomb, entry.ReplicaId, entry.Counter)) continue;
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
            result.Add(new CrdtMemberValue
            {
                Element = KeyToBytes(key),
                ReplicaId = bestReplica,
                Ordinal = bestCounter,
            });
        }

        if (result.Count == 0) return result;
        result.Sort(static (x, y) => CompareElementBytes(x.Element, y.Element));
        return result;
    }

    // ========================================================================
    // shared helpers
    // ========================================================================

    private static MvRegisterDelta MakeRegisterDelta(int index)
    {
        // The steady-state shape: one live dot-tagged value, plus a dot context
        // naming the replicas whose earlier values it observed and superseded.
        var entries = new MvRegisterEntry[]
        {
            new() { ReplicaId = ReplicaA, Counter = index + 1, Value = MakeValue(index) },
        };

        var context = new Dictionary<string, long>(ContextReplicas, StringComparer.Ordinal)
        {
            [ReplicaA] = index + 1,
            [ReplicaB] = index,
        };
        for (var r = 2; r < ContextReplicas; r++)
        {
            context[$"replica-{(char)('a' + r)}"] = index;
        }

        return new MvRegisterDelta { Entries = entries, Context = context };
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

    private static byte[] KeyToBytes<TKey>(TKey key)
    {
        var text = key as string ?? Convert.ToString(key, CultureInfo.InvariantCulture) ?? string.Empty;
        return text.Length == 0 ? Array.Empty<byte>() : Encoding.UTF8.GetBytes(text);
    }

    private static bool IsEntryTombstoned(List<OrSetDot>? tombstones, string replicaId, long counter)
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

    private static int CompareElementBytes(byte[] a, byte[] b)
    {
        var min = Math.Min(a.Length, b.Length);
        for (var i = 0; i < min; i++)
        {
            var c = a[i].CompareTo(b[i]);
            if (c != 0) return c;
        }

        return a.Length.CompareTo(b.Length);
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

    /// <summary>
    /// The register decoder's per-delta slice order (the internal
    /// <c>CrdtMemberChangeCausalComparer</c>), which leads on replica rather
    /// than ordinal. Reproduced so the baseline lane does the same sorting work
    /// as the shipped one and the measured delta is the allocation change alone.
    /// </summary>
    private sealed class BaselineReplicaFirstOrder : IComparer<CrdtMemberChange>
    {
        public static readonly BaselineReplicaFirstOrder Instance = new();

        public int Compare(CrdtMemberChange x, CrdtMemberChange y)
        {
            var byReplica = string.CompareOrdinal(x.ReplicaId, y.ReplicaId);
            if (byReplica != 0) return byReplica;
            var byOrdinal = x.Ordinal.CompareTo(y.Ordinal);
            if (byOrdinal != 0) return byOrdinal;
            return ((int)x.Kind).CompareTo((int)y.Kind);
        }
    }

    /// <summary>
    /// The OR-map decoder's private folded-state order: key surrogate bytes
    /// first, then replica, ordinal, and kind.
    /// </summary>
    private sealed class BaselineOrMapElementOrder : IComparer<CrdtMemberChange>
    {
        public static readonly BaselineOrMapElementOrder Instance = new();

        public int Compare(CrdtMemberChange x, CrdtMemberChange y)
        {
            var byElement = CompareElementBytes(x.Element, y.Element);
            if (byElement != 0) return byElement;
            var byReplica = string.CompareOrdinal(x.ReplicaId, y.ReplicaId);
            if (byReplica != 0) return byReplica;
            var byOrdinal = x.Ordinal.CompareTo(y.Ordinal);
            if (byOrdinal != 0) return byOrdinal;
            return ((int)x.Kind).CompareTo((int)y.Kind);
        }
    }
}
