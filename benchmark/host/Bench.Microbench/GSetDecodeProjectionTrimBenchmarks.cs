using System;
using System.Collections.Generic;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the grow-only-set read-path trim applied to
/// <see cref="GSetProvenanceDecoder"/> and to <see cref="GSet.Values"/>.
/// <para>
/// <b>What was there.</b> <c>GSet.Values()</c> is a <c>yield return</c>
/// iterator: it copied the element keys into a <c>List&lt;string&gt;</c>,
/// sorted that, and yielded one decoded <c>byte[]</c> per element. Both
/// decoder entry points then walked it with a <c>foreach</c> and appended to a
/// <c>List</c>. That is three layers of bookkeeping over one exactly-known
/// count: an iterator state machine with an interface-dispatched
/// <c>MoveNext</c>/<c>Current</c> per element, a <c>List&lt;string&gt;</c>
/// wrapper around the key window, and a <c>List.Add</c> per result that pays a
/// bounds check and a version bump the caller never reads. Every other
/// accessor in the tree had already been moved to the eager
/// <c>GSet.SnapshotValues()</c> for exactly this reason; these two call sites
/// were the last in-tree users of the iterator.
/// </para>
/// <para>
/// <b>What ships.</b> Both decoder methods copy the keys into an
/// exactly-sized <c>string[]</c>, sort it with the same cached
/// <c>OrdinalStringOrder.Comparison</c>, and project straight into an
/// exactly-sized result array written by index - the result shape
/// <c>SequenceProvenanceDecoder</c> already uses. <c>GSet.Values()</c> keeps
/// its signature, its laziness, and its order, and swaps only its internal
/// <c>List&lt;string&gt;</c> for the same exactly-sized array, which also
/// drops the per-element version check its <c>foreach</c> paid.
/// </para>
/// <para>
/// <b>No pooling.</b> Every key window here is a fresh gen0 array that dies in
/// its own method. Rented windows were measured for this shape and rejected -
/// see the pooled-window rule in the microbench candidate register: a rented
/// buffer is long-lived, so holding it across the allocating projection turns
/// every stored reference into a cross-generational root and costs more than
/// the allocation it removes.
/// </para>
/// <para>
/// <b>Baselines are verbatim.</b> <see cref="Baseline"/> carries a byte-for-byte
/// copy of the pre-trim <c>Values()</c> iterator and of both pre-trim decoder
/// bodies, so the only difference measured is the projection shape. The
/// baseline decoders walk the baseline iterator, not the shipped one, so the
/// trim is not partly credited to itself.
/// </para>
/// <para>
/// <b>Controls.</b> <see cref="Elements"/> starts at <b>1</b>, where the
/// per-element bookkeeping cannot amortise and the two lanes should be close to
/// parity, and the decode of each element's bytes dominates. The per-element
/// savings can only separate from the noise as the width grows, which is what
/// the 64 and 512 points are for. The element payload is deliberately small, so
/// <c>Convert.FromBase64String</c> does not swamp the bookkeeping being
/// measured; at larger payloads the trim is unchanged in absolute terms but a
/// smaller share of the total.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=gsetdecodetrims</c> (or
/// <c>--suite gsetdecodetrims</c>). No Orleans silo is involved, so it is cheap
/// at <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class GSetDecodeProjectionTrimBenchmarks
{
    /// <summary>The number of elements the decoded set carries.</summary>
    [Params(1, 64, 512)]
    public int Elements { get; set; }

    private GSet _set = new();

    [GlobalSetup]
    public void Setup()
    {
        _set = new GSet();
        for (var i = 0; i < Elements; i++)
        {
            // Vary the leading bytes so the ordinal key sort is doing real
            // work rather than walking an already-ordered window.
            var element = new byte[12];
            element[0] = (byte)(i * 31);
            element[1] = (byte)(i >> 3);
            element[2] = (byte)(i >> 11);
            element[11] = (byte)i;
            _set.Add(element);
        }

        AssertEquivalence();
    }

    /// <summary>
    /// Proves the projected arrays are indistinguishable from the lists the
    /// decoder built: the same members, in the same order, with the same bytes
    /// and the same empty provenance. A trim that changed the sort, dropped an
    /// element, or reordered the window would fail here rather than surface as
    /// a silent change in decoded provenance.
    /// </summary>
    private void AssertEquivalence()
    {
        var legacyValues = new List<byte[]>(Baseline.Values(_set));
        var shippedValues = new List<byte[]>(_set.Values());
        AssertSameElements(legacyValues, shippedValues, "GSet.Values");

        var legacyState = Baseline.DecodeState(_set);
        var shippedState = GSetProvenanceDecoder.Instance.DecodeState(_set);
        if (legacyState.Count != shippedState.Count)
        {
            throw new InvalidOperationException("GSet DecodeState trim changed the event count.");
        }

        for (var i = 0; i < legacyState.Count; i++)
        {
            var legacy = legacyState[i];
            var shipped = shippedState[i];
            if (!legacy.Element.AsSpan().SequenceEqual(shipped.Element)
                || legacy.Kind != shipped.Kind
                || !string.Equals(legacy.ReplicaId, shipped.ReplicaId, StringComparison.Ordinal)
                || legacy.Ordinal != shipped.Ordinal
                || legacy.WallClock != shipped.WallClock)
            {
                throw new InvalidOperationException($"GSet DecodeState trim changed the event at index {i}.");
            }
        }

        var legacyCurrent = Baseline.DecodeCurrentValue(_set);
        var shippedCurrent = GSetProvenanceDecoder.Instance.DecodeCurrentValue(_set);
        if (legacyCurrent.Count != shippedCurrent.Count)
        {
            throw new InvalidOperationException("GSet DecodeCurrentValue trim changed the member count.");
        }

        for (var i = 0; i < legacyCurrent.Count; i++)
        {
            var legacy = legacyCurrent[i];
            var shipped = shippedCurrent[i];
            if (!legacy.Element.AsSpan().SequenceEqual(shipped.Element)
                || !string.Equals(legacy.ReplicaId, shipped.ReplicaId, StringComparison.Ordinal)
                || legacy.Ordinal != shipped.Ordinal)
            {
                throw new InvalidOperationException($"GSet DecodeCurrentValue trim changed the member at index {i}.");
            }
        }
    }

    private static void AssertSameElements(List<byte[]> legacy, List<byte[]> shipped, string what)
    {
        if (legacy.Count != shipped.Count)
        {
            throw new InvalidOperationException($"{what} trim changed the element count.");
        }

        for (var i = 0; i < legacy.Count; i++)
        {
            if (!legacy[i].AsSpan().SequenceEqual(shipped[i]))
            {
                throw new InvalidOperationException($"{what} trim changed the element at index {i}.");
            }
        }
    }

    /// <summary>The pre-trim <c>GSet.Values()</c> iterator, fully drained.</summary>
    [Benchmark(Description = "GSet.Values walk (baseline)")]
    public int ValuesBaseline()
    {
        var total = 0;
        foreach (var element in Baseline.Values(_set))
        {
            total += element.Length;
        }

        return total;
    }

    /// <summary>The shipped <c>GSet.Values()</c> iterator, fully drained.</summary>
    [Benchmark(Description = "GSet.Values walk (shipped)")]
    public int ValuesShipped()
    {
        var total = 0;
        foreach (var element in _set.Values())
        {
            total += element.Length;
        }

        return total;
    }

    /// <summary>The pre-trim state decode: the baseline iterator into a List.</summary>
    [Benchmark(Description = "GSet DecodeState (baseline)")]
    public IReadOnlyList<CrdtMemberChange> DecodeStateBaseline() => Baseline.DecodeState(_set);

    /// <summary>The shipped state decode, called through the real decoder.</summary>
    [Benchmark(Description = "GSet DecodeState (shipped)")]
    public IReadOnlyList<CrdtMemberChange> DecodeStateShipped() => GSetProvenanceDecoder.Instance.DecodeState(_set);

    /// <summary>The pre-trim current-value projection: the baseline iterator into a List.</summary>
    [Benchmark(Description = "GSet DecodeCurrentValue (baseline)")]
    public IReadOnlyList<CrdtMemberValue> DecodeCurrentValueBaseline() => Baseline.DecodeCurrentValue(_set);

    /// <summary>The shipped current-value projection, called through the real decoder.</summary>
    [Benchmark(Description = "GSet DecodeCurrentValue (shipped)")]
    public IReadOnlyList<CrdtMemberValue> DecodeCurrentValueShipped() => GSetProvenanceDecoder.Instance.DecodeCurrentValue(_set);

    /// <summary>
    /// The verbatim pre-trim read path: the <c>yield return</c> iterator over a
    /// sorted <c>List&lt;string&gt;</c>, and the two decoder bodies that walked
    /// it with a <c>foreach</c> and appended to a <c>List</c>.
    /// </summary>
    private static class Baseline
    {
        internal static IEnumerable<byte[]> Values(GSet set)
        {
            if (set.Elements.Count == 0) yield break;

            var keys = new List<string>(set.Elements);
            keys.Sort(OrdinalStringOrder.Comparison);
            foreach (var key in keys)
            {
                yield return Convert.FromBase64String(key);
            }
        }

        internal static IReadOnlyList<CrdtMemberChange> DecodeState(GSet set)
        {
            if (set.Count == 0) return Array.Empty<CrdtMemberChange>();

            var result = new List<CrdtMemberChange>(set.Count);
            foreach (var element in Values(set))
            {
                result.Add(new CrdtMemberChange
                {
                    Element = element,
                    Kind = CrdtMemberChangeKind.Added,
                    ReplicaId = string.Empty,
                    Ordinal = 0,
                    WallClock = null,
                });
            }

            return result;
        }

        internal static IReadOnlyList<CrdtMemberValue> DecodeCurrentValue(GSet set)
        {
            if (set.Count == 0) return Array.Empty<CrdtMemberValue>();

            var result = new List<CrdtMemberValue>(set.Count);
            foreach (var element in Values(set))
            {
                result.Add(new CrdtMemberValue
                {
                    Element = element,
                    ReplicaId = string.Empty,
                    Ordinal = 0,
                });
            }

            return result;
        }
    }
}
