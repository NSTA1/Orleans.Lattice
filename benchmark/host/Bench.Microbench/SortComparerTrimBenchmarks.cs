using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates one defect that recurs across three unrelated call-path families in
/// the library: a deterministic sort reached through an
/// <see cref="IComparer{T}"/> singleton allocates a comparison delegate on
/// <b>every call</b>.
/// <para>
/// The shape reads as allocation-free, and two production XML docs asserted
/// exactly that - <c>MvRegister.EntryOrdering</c> ("a cached singleton so the
/// multi-value sort path allocates no per-call comparer") and
/// <c>CrdtMemberChangeCausalComparer</c> ("shared as a single stateless
/// instance so the folded-state decoders ... never allocate a comparison
/// delegate"). Both claims are true of the <em>comparer object</em> and false
/// of the sort. Every <c>Sort</c> overload taking an
/// <see cref="IComparer{T}"/> - <see cref="Array.Sort{T}(T[], IComparer{T})"/>,
/// its <c>(index, length)</c> form, <see cref="List{T}.Sort(IComparer{T})"/>,
/// and <c>List&lt;T&gt;.Sort(int, int, IComparer&lt;T&gt;)</c> - funnels into
/// <c>ArraySortHelper&lt;T&gt;.Sort(Span&lt;T&gt;, IComparer&lt;T&gt;)</c>, whose body is
/// <c>IntrospectiveSort(keys, comparer.Compare)</c>. That argument is a
/// method-group conversion, so a fresh <see cref="Comparison{T}"/> is minted
/// per call and never cached.
/// </para>
/// <para>
/// Each group pairs a <c>_Comparer</c> lane (a verbatim copy of the shipped
/// shape before the change) against a <c>_Comparison</c> lane (the shape after
/// it). Both lanes run the identical introsort over the identical input, so the
/// orders are equal to the byte - asserted in <see cref="Setup"/> for every
/// pair, over a corpus that includes equal-key runs and an already-sorted
/// prefix so a comparison-order difference could not hide.
/// </para>
/// <para>
/// (1) <b>Ordinal string key sorts.</b> The deterministic key projection behind
/// <c>GSet.Values</c>/<c>SnapshotValues</c>, <c>OrSet</c>/<c>RwSet</c> live-set
/// reads, <c>CanonicalStringSet</c>, and the atomic-write key fingerprint.
/// Isolating lanes measure the sort alone at the production call shape; the
/// <c>GSetSnapshot</c> pair measures it in situ against the real shipped
/// method.
/// </para>
/// <para>
/// (2) <b>Causal member-change sorts.</b> The whole-list sort seven CRDT
/// provenance decoders run on their result, and the <b>sub-range</b> sort
/// <c>OrMapProvenanceDecoder.SortGroup</c> runs once per key group.
/// <c>List&lt;T&gt;</c> has no <c>Comparison</c> overload for a sub-range at all, so
/// that one converts through <c>CollectionsMarshal.AsSpan(list).Slice(..)</c> -
/// a different conversion, measured separately.
/// </para>
/// <para>
/// (3) <b>Struct array sorts.</b> <c>MvRegister.ValuesShared</c>'s multi-value
/// path (measured against the real shipped method) and the sub-range
/// <c>Array.Sort(keys, 0, keyCount, KeyRefComparer.Instance)</c> in the OrMap
/// provenance decoder, whose conversion is <c>AsSpan(0, n).Sort(..)</c>
/// because the <see cref="Comparison{T}"/> family has no <c>(index, length)</c>
/// overload.
/// </para>
/// <para>
/// Read this suite for <b>bytes</b>. The delegate is a flat 64 bytes per sort
/// call regardless of element count, so the win is exact, deterministic, and
/// independent of corpus - the microbench csproj forces workstation
/// non-concurrent GC, so BenchmarkDotNet's <c>Allocated</c> column is exact.
/// The isolating lanes repeat the sort <see cref="SortRepeats"/> times per
/// operation so the delegate's contribution clears timer resolution; the
/// end-to-end lanes sort once, which is the honest in-situ ratio and is
/// deliberately the smaller delta.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=sortcomparertrims</c> (or
/// <c>--suite sortcomparertrims</c>); see <c>Program.cs</c>. No Orleans silo is
/// involved, so it runs cheaply at <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class SortComparerTrimBenchmarks
{
    /// <summary>
    /// How many times an isolating lane repeats its sort per operation. The
    /// delegate is 64 bytes per call and a sort of a few hundred elements is a
    /// few microseconds, so a single call sits under timer resolution; the
    /// repeat count lifts both lanes equally into a measurable band without
    /// changing the per-call ratio being measured.
    /// </summary>
    private const int SortRepeats = 32;

    /// <summary>
    /// Element count for every corpus. 16 is a small live set or a short
    /// provenance group - the common steady-state case, and the one where a
    /// flat 64-byte delegate is the largest share of the sort's own cost. 256
    /// is a wide tag set or a decoder result after a burst, where the sort
    /// itself dominates and the delegate should fade. A trim whose two
    /// parameter siblings disagree in sign is the lane's fault, not the
    /// change's.
    /// </summary>
    [Params(16, 256)]
    public int ElementCount { get; set; }

    private string[] _keys = null!;
    private string[] _keyScratch = null!;

    private CrdtMemberChange[] _changes = null!;
    private List<CrdtMemberChange> _changeScratch = null!;

    private MvRegisterEntry[] _entries = null!;
    private MvRegister _register = null!;
    private GSet _gset = null!;

    private KeyGroup[] _groups = null!;
    private KeyGroup[] _groupScratch = null!;

    [GlobalSetup]
    public void Setup()
    {
        // Deterministic, and deliberately adversarial for an order-equivalence
        // claim: an already-sorted prefix (so a stable/unstable difference
        // would surface), a reversed middle, and a run of equal keys.
        _keys = new string[ElementCount];
        for (var i = 0; i < ElementCount; i++)
        {
            _keys[i] = i < ElementCount / 3 ? $"key-{i:D6}"
                : i < 2 * ElementCount / 3 ? $"key-{ElementCount - i:D6}"
                : "key-equal";
        }
        _keyScratch = new string[ElementCount];

        _changes = new CrdtMemberChange[ElementCount];
        for (var i = 0; i < ElementCount; i++)
        {
            _changes[i] = new CrdtMemberChange
            {
                Element = BitConverter.GetBytes(i),
                // Equal (ReplicaId, Ordinal) pairs differing only in Kind, so
                // the comparer's third tier is actually exercised.
                Kind = (i & 1) == 0 ? CrdtMemberChangeKind.Added : CrdtMemberChangeKind.Removed,
                ReplicaId = $"replica-{i % 4}",
                Ordinal = i / 2,
                WallClock = null,
            };
        }
        _changeScratch = new List<CrdtMemberChange>(ElementCount);

        _entries = new MvRegisterEntry[ElementCount];
        for (var i = 0; i < ElementCount; i++)
        {
            _entries[i] = new MvRegisterEntry
            {
                ReplicaId = $"replica-{i % 4}",
                Counter = ElementCount - i,
                Value = BitConverter.GetBytes(i),
            };
        }

        // A register whose entry list is multi-valued, so ValuesShared takes
        // the sorting path rather than the cached single-value snapshot.
        _register = new MvRegister { Entries = new List<MvRegisterEntry>(_entries) };

        _gset = new GSet();
        for (var i = 0; i < ElementCount; i++)
        {
            _gset.Add(BitConverter.GetBytes(i));
        }

        _groups = new KeyGroup[ElementCount];
        for (var i = 0; i < ElementCount; i++)
        {
            // Duplicate surrogates on purpose: the production comparer groups
            // by element bytes and distinct keys can share a surrogate.
            _groups[i] = new KeyGroup(BitConverter.GetBytes(i % (ElementCount / 2 + 1)));
        }
        _groupScratch = new KeyGroup[ElementCount];

        AssertEquivalence();
    }

    /// <summary>
    /// Proves every baseline/optimized pair in this file produces a
    /// byte-identical ordering before any timing is attributed to it.
    /// </summary>
    private void AssertEquivalence()
    {
        var viaComparer = (string[])_keys.Clone();
        var viaComparison = (string[])_keys.Clone();
        Array.Sort(viaComparer, StringComparer.Ordinal);
        Array.Sort(viaComparison, OrdinalStringOrder.Comparison);
        for (var i = 0; i < ElementCount; i++)
        {
            if (!string.Equals(viaComparer[i], viaComparison[i], StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"Ordinal key order diverged at {i}.");
            }
        }

        var listViaComparer = new List<string>(_keys);
        var listViaComparison = new List<string>(_keys);
        listViaComparer.Sort(StringComparer.Ordinal);
        listViaComparison.Sort(OrdinalStringOrder.Comparison);
        for (var i = 0; i < ElementCount; i++)
        {
            if (!string.Equals(listViaComparer[i], listViaComparison[i], StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"Ordinal list order diverged at {i}.");
            }
        }

        var changesViaComparer = new List<CrdtMemberChange>(_changes);
        var changesViaComparison = new List<CrdtMemberChange>(_changes);
        changesViaComparer.Sort(CrdtMemberChangeCausalComparer.Instance);
        changesViaComparison.Sort(CrdtMemberChangeCausalComparer.Comparison);
        for (var i = 0; i < ElementCount; i++)
        {
            if (CrdtMemberChangeCausalComparer.Instance.Compare(changesViaComparer[i], changesViaComparison[i]) != 0)
            {
                throw new InvalidOperationException($"Causal member order diverged at {i}.");
            }
        }

        // Sub-range: a List sorted in place through its own (index, count)
        // overload against the same list sorted through a span slice.
        var rangeViaComparer = new List<CrdtMemberChange>(_changes);
        var rangeViaSpan = new List<CrdtMemberChange>(_changes);
        var start = ElementCount / 4;
        var length = ElementCount - start;
        rangeViaComparer.Sort(start, length, CrdtMemberChangeCausalComparer.Instance);
        CollectionsMarshal.AsSpan(rangeViaSpan).Slice(start, length).Sort(CrdtMemberChangeCausalComparer.Comparison);
        for (var i = 0; i < ElementCount; i++)
        {
            if (CrdtMemberChangeCausalComparer.Instance.Compare(rangeViaComparer[i], rangeViaSpan[i]) != 0)
            {
                throw new InvalidOperationException($"Causal sub-range order diverged at {i}.");
            }
        }

        var entriesViaComparer = (MvRegisterEntry[])_entries.Clone();
        var entriesViaComparison = (MvRegisterEntry[])_entries.Clone();
        Array.Sort(entriesViaComparer, BaselineEntryOrdering.Instance);
        Array.Sort(entriesViaComparison, (Comparison<MvRegisterEntry>)BaselineEntryOrdering.Instance.Compare);
        for (var i = 0; i < ElementCount; i++)
        {
            if (BaselineEntryOrdering.Instance.Compare(entriesViaComparer[i], entriesViaComparison[i]) != 0)
            {
                throw new InvalidOperationException($"MvRegister entry order diverged at {i}.");
            }
        }

        // The shipped MvRegister.ValuesShared against the verbatim prior body.
        var liveValues = _register.ValuesShared();
        var baselineValues = BaselineMvRegisterValuesShared(_register);
        if (liveValues.Count != baselineValues.Count)
        {
            throw new InvalidOperationException("MvRegister value count diverged.");
        }
        for (var i = 0; i < liveValues.Count; i++)
        {
            if (!liveValues[i].AsSpan().SequenceEqual(baselineValues[i]))
            {
                throw new InvalidOperationException($"MvRegister value order diverged at {i}.");
            }
        }

        // The shipped GSet.SnapshotValues against the verbatim prior body.
        var liveSnapshot = _gset.SnapshotValues();
        var baselineSnapshot = BaselineGSetSnapshotValues(_gset);
        if (liveSnapshot.Length != baselineSnapshot.Length)
        {
            throw new InvalidOperationException("GSet snapshot count diverged.");
        }
        for (var i = 0; i < liveSnapshot.Length; i++)
        {
            if (!liveSnapshot[i].AsSpan().SequenceEqual(baselineSnapshot[i]))
            {
                throw new InvalidOperationException($"GSet snapshot order diverged at {i}.");
            }
        }

        var groupsViaComparer = (KeyGroup[])_groups.Clone();
        var groupsViaSpan = (KeyGroup[])_groups.Clone();
        var groupCount = ElementCount - 1;
        Array.Sort(groupsViaComparer, 0, groupCount, KeyGroupComparer.Instance);
        groupsViaSpan.AsSpan(0, groupCount).Sort(KeyGroupComparer.Comparison);
        for (var i = 0; i < ElementCount; i++)
        {
            if (KeyGroupComparer.Instance.Compare(groupsViaComparer[i], groupsViaSpan[i]) != 0)
            {
                throw new InvalidOperationException($"Key group order diverged at {i}.");
            }
        }
    }

    // ---------------------------------------------------------------------
    // (1) Ordinal string key sorts.
    // ---------------------------------------------------------------------

    /// <summary>The shipped shape before this change: an ordinal key array sorted through the comparer singleton.</summary>
    [Benchmark(Baseline = true)]
    public string OrdinalArraySort_Comparer()
    {
        for (var r = 0; r < SortRepeats; r++)
        {
            Array.Copy(_keys, _keyScratch, ElementCount);
            Array.Sort(_keyScratch, StringComparer.Ordinal);
        }
        return _keyScratch[0];
    }

    /// <summary>The shape after this change: the same ordering through a comparison constructed once.</summary>
    [Benchmark]
    public string OrdinalArraySort_Comparison()
    {
        for (var r = 0; r < SortRepeats; r++)
        {
            Array.Copy(_keys, _keyScratch, ElementCount);
            Array.Sort(_keyScratch, OrdinalStringOrder.Comparison);
        }
        return _keyScratch[0];
    }

    /// <summary>A verbatim copy of <c>GSet.SnapshotValues</c> before this change.</summary>
    [Benchmark]
    public int GSetSnapshot_Comparer() => BaselineGSetSnapshotValues(_gset).Length;

    /// <summary>The real shipped <c>GSet.SnapshotValues</c>, in situ.</summary>
    [Benchmark]
    public int GSetSnapshot_Live() => _gset.SnapshotValues().Length;

    // ---------------------------------------------------------------------
    // (2) Causal member-change sorts.
    // ---------------------------------------------------------------------

    /// <summary>The shipped shape before this change: a decoder result sorted through the comparer singleton.</summary>
    [Benchmark]
    public int MemberListSort_Comparer()
    {
        for (var r = 0; r < SortRepeats; r++)
        {
            _changeScratch.Clear();
            _changeScratch.AddRange(_changes);
            _changeScratch.Sort(CrdtMemberChangeCausalComparer.Instance);
        }
        return _changeScratch.Count;
    }

    /// <summary>The shape after this change: the same ordering through a comparison constructed once.</summary>
    [Benchmark]
    public int MemberListSort_Comparison()
    {
        for (var r = 0; r < SortRepeats; r++)
        {
            _changeScratch.Clear();
            _changeScratch.AddRange(_changes);
            _changeScratch.Sort(CrdtMemberChangeCausalComparer.Comparison);
        }
        return _changeScratch.Count;
    }

    /// <summary>
    /// The shipped shape before this change for <c>OrMapProvenanceDecoder.SortGroup</c>:
    /// a <b>sub-range</b> of the sink sorted through the comparer singleton.
    /// </summary>
    [Benchmark]
    public int MemberRangeSort_Comparer()
    {
        var start = ElementCount / 4;
        var length = ElementCount - start;
        for (var r = 0; r < SortRepeats; r++)
        {
            _changeScratch.Clear();
            _changeScratch.AddRange(_changes);
            _changeScratch.Sort(start, length, CrdtMemberChangeCausalComparer.Instance);
        }
        return _changeScratch.Count;
    }

    /// <summary>
    /// The shape after this change. <see cref="List{T}"/> has no
    /// <see cref="Comparison{T}"/> overload for a sub-range, so the conversion
    /// goes through the list's backing span instead.
    /// </summary>
    [Benchmark]
    public int MemberRangeSort_SpanComparison()
    {
        var start = ElementCount / 4;
        var length = ElementCount - start;
        for (var r = 0; r < SortRepeats; r++)
        {
            _changeScratch.Clear();
            _changeScratch.AddRange(_changes);
            CollectionsMarshal.AsSpan(_changeScratch).Slice(start, length).Sort(CrdtMemberChangeCausalComparer.Comparison);
        }
        return _changeScratch.Count;
    }

    // ---------------------------------------------------------------------
    // (3) Struct array sorts.
    // ---------------------------------------------------------------------

    /// <summary>A verbatim copy of <c>MvRegister.ValuesShared</c>'s multi-value path before this change.</summary>
    [Benchmark]
    public int MvRegisterValues_Comparer() => BaselineMvRegisterValuesShared(_register).Count;

    /// <summary>The real shipped <c>MvRegister.ValuesShared</c>, in situ.</summary>
    [Benchmark]
    public int MvRegisterValues_Live() => _register.ValuesShared().Count;

    /// <summary>
    /// The shipped shape before this change for the OrMap provenance decoder's
    /// key-group sort: an <c>(index, length)</c> array sort through a comparer
    /// singleton.
    /// </summary>
    [Benchmark]
    public int KeyGroupRangeSort_Comparer()
    {
        var count = ElementCount - 1;
        for (var r = 0; r < SortRepeats; r++)
        {
            Array.Copy(_groups, _groupScratch, ElementCount);
            Array.Sort(_groupScratch, 0, count, KeyGroupComparer.Instance);
        }
        return count;
    }

    /// <summary>
    /// The shape after this change. The <see cref="Comparison{T}"/> family has
    /// no <c>(index, length)</c> overload, so the sub-range converts through a
    /// span slice.
    /// </summary>
    [Benchmark]
    public int KeyGroupRangeSort_SpanComparison()
    {
        var count = ElementCount - 1;
        for (var r = 0; r < SortRepeats; r++)
        {
            Array.Copy(_groups, _groupScratch, ElementCount);
            _groupScratch.AsSpan(0, count).Sort(KeyGroupComparer.Comparison);
        }
        return count;
    }

    // ---------------------------------------------------------------------
    // Verbatim copies of the replaced bodies, so each baseline lane pays
    // exactly the overhead its optimized counterpart pays and nothing else.
    // ---------------------------------------------------------------------

    private static byte[][] BaselineGSetSnapshotValues(GSet set)
    {
        var count = set.Elements.Count;
        if (count == 0) return Array.Empty<byte[]>();

        var keys = new string[count];
        set.Elements.CopyTo(keys);
        Array.Sort(keys, StringComparer.Ordinal);

        var values = new byte[count][];
        for (var i = 0; i < count; i++)
        {
            values[i] = Convert.FromBase64String(keys[i]);
        }
        return values;
    }

    private static IReadOnlyList<byte[]> BaselineMvRegisterValuesShared(MvRegister register)
    {
        var count = register.Entries.Count;
        if (count == 0) return Array.Empty<byte[]>();
        if (count == 1) return new[] { register.Entries[0].Value };

        var entries = new MvRegisterEntry[count];
        register.Entries.CopyTo(entries);
        Array.Sort(entries, BaselineEntryOrdering.Instance);

        var ordered = new byte[count][];
        for (var i = 0; i < count; i++) ordered[i] = entries[i].Value;
        return ordered;
    }

    /// <summary>A verbatim copy of <c>MvRegister.EntryOrdering</c>.</summary>
    private sealed class BaselineEntryOrdering : IComparer<MvRegisterEntry>
    {
        public static readonly BaselineEntryOrdering Instance = new();

        public int Compare(MvRegisterEntry x, MvRegisterEntry y)
        {
            var byReplica = string.CompareOrdinal(x.ReplicaId, y.ReplicaId);
            return byReplica != 0 ? byReplica : x.Counter.CompareTo(y.Counter);
        }
    }

    /// <summary>
    /// Mirrors <c>OrMapProvenanceDecoder.KeyRef</c>: a key's surrogate bytes,
    /// sorted by an ordinal byte comparison. The production type is private to
    /// its decoder, so the call shape is reproduced rather than imported.
    /// </summary>
    private readonly struct KeyGroup(byte[] element)
    {
        public byte[] Element { get; } = element;
    }

    private sealed class KeyGroupComparer : IComparer<KeyGroup>
    {
        public static KeyGroupComparer Instance { get; } = new();

        public static readonly Comparison<KeyGroup> Comparison = Instance.Compare;

        public int Compare(KeyGroup x, KeyGroup y)
            => x.Element.AsSpan().SequenceCompareTo(y.Element.AsSpan());
    }
}
