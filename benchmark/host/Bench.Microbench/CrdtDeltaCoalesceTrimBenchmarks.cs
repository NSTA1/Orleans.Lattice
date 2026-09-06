using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three allocation reductions made to the CRDT <b>pre-ship delta
/// coalescing</b> path - the fold the replication shipper runs over its drain
/// buffer before every ship, collapsing each key's run of pending deltas into
/// one wire delta.
/// <para>
/// The shape that makes this path worth trimming is that
/// <c>CrdtShape.CombineDeltas</c> is a <b>pairwise running left fold</b>:
/// <c>ReplicationShipperGrain.CoalesceCrdtDrainBuffer</c> folds a key's run with
/// <c>combined = shape.CombineDeltas(combined, next)</c>, so union number k
/// re-walks every element accumulated by unions 1..k-1. A per-element allocation
/// inside the union is therefore not linear in the run length, it is quadratic,
/// and a run of a few dozen deltas is ordinary on a busy tree.
/// </para>
/// <para>
/// Judge the suite on <b>Allocated</b>. These are allocation trims, the
/// allocated column reproduces bit-for-bit across rounds, and Mean on a shared
/// developer host does not. Nothing here starts a silo, so the suite is cheap
/// enough to run at <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// <para>
/// <b>Shell fidelity.</b> The helpers under test are private statics on
/// <see cref="CrdtShape"/>, so the A/B arms reproduce their bodies here. Both
/// arms of every pair are driven by the <b>same</b> fold shell over the
/// <b>same</b> pre-built deltas, take the same <c>IReadOnlyList&lt;T&gt;</c>
/// parameter types production takes, and differ only in the body under test - a
/// baseline arm that skips part of the optimized arm's shell fabricates a
/// regression. Each lane additionally ships a <c>_Production</c> arm that folds
/// the same estate through the <b>real shipped</b>
/// <c>CrdtShape.ForXxx().CombineDeltas</c>, pinning the copied shells to
/// reality: its Allocated should track the optimized arm.
/// </para>
/// <para>
/// The three edits under test:
/// (1) the grow-only element union and the OR-Set/RW-Set dot union both deduped
/// on a throwaway <c>Convert.ToBase64String(element)</c> surrogate. Base64 is
/// injective over byte sequences, so sequence equality over the raw bytes states
/// exactly the same equivalence - the surrogate bought nothing but a string per
/// element, per union, per fold step. Both now dedup on the element array itself
/// through a structural comparer;
/// (2) every dedup accumulator in those helpers grew from empty even though the
/// union can never exceed the two sources' combined width, which is known before
/// the first insert. All of them now presize from that bound, so a fold no
/// longer walks the prime doubling chain rehashing what it already holds;
/// (3) the OR-Map add combine kept a <c>Dictionary&lt;dot, entry&gt;</c>
/// alongside a <c>List&lt;dot&gt;</c> of wide dot tuples recording
/// first-observation order, then ran a third pass that re-hashed every dot to
/// materialise the ordered array. It now maps each dot to its <b>slot</b> in the
/// result list, so order is carried by the list itself, the whole re-probe pass
/// is gone, and the remaining probe is folded to one with
/// <see cref="CollectionsMarshal.GetValueRefOrAddDefault{TKey, TValue}"/>.
/// </para>
/// <para>
/// <b>Contrast arm.</b> <see cref="GSetFold_Contrast_Base64Presized"/> takes
/// only the cheap half of edit (1)+(2) - it keeps the base64 surrogate and
/// merely presizes the set - so the report can show whether the structural
/// comparer earns its complexity or whether presizing alone would have done.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=crdtcoalescetrims</c> (or
/// <c>--suite crdtcoalescetrims</c>); see <c>Program.cs</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class CrdtDeltaCoalesceTrimBenchmarks
{
    // A drain-buffer run shaped like a busy tree's: a few dozen deltas for one
    // key, each carrying a handful of elements drawn from an overlapping pool so
    // the union hits both the add-new and the skip-duplicate branch, and so the
    // accumulated set keeps growing across the fold the way a real run does.
    private const int FoldWidth = 24;
    private const int ElementsPerDelta = 16;
    private const int ElementPoolSize = 256;
    private const int ElementBytes = 24;
    private const int ReplicaCount = 4;

    private static readonly CrdtShape GSetShape = CrdtShape.ForGSet();
    private static readonly CrdtShape OrSetShape = CrdtShape.ForOrSet();
    private static readonly CrdtShape OrMapShape = CrdtShape.ForOrMap<string, PnCounter>();

    private byte[][] _pool = [];
    private GSetDelta[] _gsetRun = [];
    private OrSetDelta[] _orsetRun = [];
    private OrMapDelta<string, PnCounter>[] _ormapRun = [];

    [GlobalSetup]
    public void Setup()
    {
        var rng = new Random(20260214);

        _pool = new byte[ElementPoolSize][];
        for (var i = 0; i < ElementPoolSize; i++)
        {
            var element = new byte[ElementBytes];
            rng.NextBytes(element);
            _pool[i] = element;
        }

        _gsetRun = new GSetDelta[FoldWidth];
        _orsetRun = new OrSetDelta[FoldWidth];
        _ormapRun = new OrMapDelta<string, PnCounter>[FoldWidth];

        for (var d = 0; d < FoldWidth; d++)
        {
            var adds = new byte[ElementsPerDelta][];
            var dots = new OrSetDeltaDot[ElementsPerDelta];
            var removes = new OrSetDeltaDot[ElementsPerDelta / 4];
            var entries = new OrMapDeltaEntry<string, PnCounter>[ElementsPerDelta];
            var tombstones = new OrMapDeltaTombstone<string>[ElementsPerDelta / 4];

            for (var e = 0; e < ElementsPerDelta; e++)
            {
                // Deliberate overlap: the pool index walks forward but wraps
                // inside a window narrower than the fold's total draw, so later
                // deltas re-present elements earlier ones already contributed.
                var element = _pool[((d * ElementsPerDelta) + e) % ElementPoolSize];
                var replica = "replica-" + (e % ReplicaCount).ToString();
                adds[e] = element;
                dots[e] = new OrSetDeltaDot
                {
                    Element = element,
                    ReplicaId = replica,
                    Counter = d + 1,
                };

                var value = new PnCounter();
                value.Increment(replica, e + 1);
                entries[e] = new OrMapDeltaEntry<string, PnCounter>
                {
                    // Keys repeat across the fold so the same dot is genuinely
                    // re-observed and the value-CRDT merge branch is exercised.
                    Key = "k" + (((d * ElementsPerDelta) + e) % (ElementPoolSize / 2)).ToString(),
                    ReplicaId = replica,
                    Counter = d + 1,
                    Value = value,
                };

                if (e < removes.Length)
                {
                    removes[e] = new OrSetDeltaDot
                    {
                        Element = _pool[(d + e) % ElementPoolSize],
                        ReplicaId = replica,
                        Counter = d + 1,
                    };
                    tombstones[e] = new OrMapDeltaTombstone<string>
                    {
                        Key = "k" + ((d + e) % (ElementPoolSize / 2)).ToString(),
                        ReplicaId = replica,
                        Counter = d + 1,
                    };
                }
            }

            _gsetRun[d] = new GSetDelta { Adds = adds };
            _orsetRun[d] = new OrSetDelta { Adds = dots, Removes = removes };
            _ormapRun[d] = new OrMapDelta<string, PnCounter>
            {
                Adds = entries,
                Tombstones = tombstones,
            };
        }

        // Fail loudly rather than publishing a report where the arms are not
        // computing the same thing: every lane's baseline, optimized, and
        // production arm must agree on the folded result's shape.
        AssertAgree(nameof(GSetFold_Baseline), GSetFold_Baseline(), GSetFold_Optimized(), GSetFold_Production(), GSetFold_Contrast_Base64Presized());
        AssertAgree(nameof(OrSetFold_Baseline), OrSetFold_Baseline(), OrSetFold_Optimized(), OrSetFold_Production());
        AssertAgree(nameof(OrMapFold_Baseline), OrMapFold_Baseline(), OrMapFold_Optimized(), OrMapFold_Production());
    }

    private static void AssertAgree(string lane, params int[] results)
    {
        foreach (var result in results)
        {
            if (result != results[0])
            {
                throw new InvalidOperationException(
                    $"lane {lane} arms disagree: [{string.Join(", ", results)}]");
            }
        }
    }

    // ---------------------------------------------------------------
    // Lane 1: grow-only element union - base64 surrogate key elision.
    // ---------------------------------------------------------------

    [Benchmark]
    public int GSetFold_Baseline()
    {
        var combined = _gsetRun[0];
        for (var i = 1; i < _gsetRun.Length; i++)
        {
            combined = new GSetDelta { Adds = BaselineUnionGSetElements(combined.Adds, _gsetRun[i].Adds) };
        }
        return combined.Adds.Count;
    }

    [Benchmark]
    public int GSetFold_Contrast_Base64Presized()
    {
        var combined = _gsetRun[0];
        for (var i = 1; i < _gsetRun.Length; i++)
        {
            combined = new GSetDelta { Adds = ContrastUnionGSetElements(combined.Adds, _gsetRun[i].Adds) };
        }
        return combined.Adds.Count;
    }

    [Benchmark]
    public int GSetFold_Optimized()
    {
        var combined = _gsetRun[0];
        for (var i = 1; i < _gsetRun.Length; i++)
        {
            combined = new GSetDelta { Adds = OptimizedUnionGSetElements(combined.Adds, _gsetRun[i].Adds) };
        }
        return combined.Adds.Count;
    }

    [Benchmark]
    public int GSetFold_Production()
    {
        object combined = _gsetRun[0];
        for (var i = 1; i < _gsetRun.Length; i++)
        {
            combined = GSetShape.CombineDeltas!(combined, _gsetRun[i]);
        }
        return ((GSetDelta)combined).Adds.Count;
    }

    // ---------------------------------------------------------------
    // Lane 2: OR-Set delta-dot union - same elision on the dot key.
    // ---------------------------------------------------------------

    [Benchmark]
    public int OrSetFold_Baseline()
    {
        var combined = _orsetRun[0];
        for (var i = 1; i < _orsetRun.Length; i++)
        {
            combined = new OrSetDelta
            {
                Adds = BaselineUnionOrSetDeltaDots(combined.Adds, _orsetRun[i].Adds),
                Removes = BaselineUnionOrSetDeltaDots(combined.Removes, _orsetRun[i].Removes),
            };
        }
        return combined.Adds.Count + combined.Removes.Count;
    }

    [Benchmark]
    public int OrSetFold_Optimized()
    {
        var combined = _orsetRun[0];
        for (var i = 1; i < _orsetRun.Length; i++)
        {
            combined = new OrSetDelta
            {
                Adds = OptimizedUnionOrSetDeltaDots(combined.Adds, _orsetRun[i].Adds),
                Removes = OptimizedUnionOrSetDeltaDots(combined.Removes, _orsetRun[i].Removes),
            };
        }
        return combined.Adds.Count + combined.Removes.Count;
    }

    [Benchmark]
    public int OrSetFold_Production()
    {
        object combined = _orsetRun[0];
        for (var i = 1; i < _orsetRun.Length; i++)
        {
            combined = OrSetShape.CombineDeltas!(combined, _orsetRun[i]);
        }
        var result = (OrSetDelta)combined;
        return result.Adds.Count + result.Removes.Count;
    }

    // ---------------------------------------------------------------
    // Lane 3: OR-Map add combine - slot map, single probe, no re-probe pass.
    // ---------------------------------------------------------------

    [Benchmark]
    public int OrMapFold_Baseline()
    {
        var combined = _ormapRun[0];
        for (var i = 1; i < _ormapRun.Length; i++)
        {
            combined = BaselineCombineOrMapDelta(combined, _ormapRun[i]);
        }
        return combined.Adds.Count + combined.Tombstones.Count;
    }

    [Benchmark]
    public int OrMapFold_Optimized()
    {
        var combined = _ormapRun[0];
        for (var i = 1; i < _ormapRun.Length; i++)
        {
            combined = OptimizedCombineOrMapDelta(combined, _ormapRun[i]);
        }
        return combined.Adds.Count + combined.Tombstones.Count;
    }

    [Benchmark]
    public int OrMapFold_Production()
    {
        object combined = _ormapRun[0];
        for (var i = 1; i < _ormapRun.Length; i++)
        {
            combined = OrMapShape.CombineDeltas!(combined, _ormapRun[i]);
        }
        var result = (OrMapDelta<string, PnCounter>)combined;
        return result.Adds.Count + result.Tombstones.Count;
    }

    // ---------------------------------------------------------------
    // Copied bodies - baseline (pre-change) and optimized (as shipped).
    // ---------------------------------------------------------------

    /// <summary>Pre-change element union: dedup set grown from empty, keyed on a per-element base64 surrogate.</summary>
    private static IReadOnlyList<byte[]> BaselineUnionGSetElements(
        IReadOnlyList<byte[]>? a,
        IReadOnlyList<byte[]>? b)
    {
        var result = new List<byte[]>((a?.Count ?? 0) + (b?.Count ?? 0));
        var seen = new HashSet<string>(StringComparer.Ordinal);
        BaselineAppendGSetElements(a, result, seen);
        BaselineAppendGSetElements(b, result, seen);
        return result;
    }

    private static void BaselineAppendGSetElements(
        IReadOnlyList<byte[]>? source,
        List<byte[]> result,
        HashSet<string> seen)
    {
        if (source is null)
        {
            return;
        }
        foreach (var element in source)
        {
            if (element is null)
            {
                continue;
            }
            if (seen.Add(Convert.ToBase64String(element)))
            {
                result.Add(element);
            }
        }
    }

    /// <summary>Cheap half only: keeps the base64 surrogate, presizes the dedup set.</summary>
    private static IReadOnlyList<byte[]> ContrastUnionGSetElements(
        IReadOnlyList<byte[]>? a,
        IReadOnlyList<byte[]>? b)
    {
        var bound = (a?.Count ?? 0) + (b?.Count ?? 0);
        var result = new List<byte[]>(bound);
        var seen = new HashSet<string>(bound, StringComparer.Ordinal);
        BaselineAppendGSetElements(a, result, seen);
        BaselineAppendGSetElements(b, result, seen);
        return result;
    }

    /// <summary>As shipped: presized structural dedup over the raw element bytes.</summary>
    private static IReadOnlyList<byte[]> OptimizedUnionGSetElements(
        IReadOnlyList<byte[]>? a,
        IReadOnlyList<byte[]>? b)
    {
        var bound = (a?.Count ?? 0) + (b?.Count ?? 0);
        var result = new List<byte[]>(bound);
        var seen = new HashSet<byte[]>(bound, BenchElementBytesComparer.Instance);
        OptimizedAppendGSetElements(a, result, seen);
        OptimizedAppendGSetElements(b, result, seen);
        return result;
    }

    private static void OptimizedAppendGSetElements(
        IReadOnlyList<byte[]>? source,
        List<byte[]> result,
        HashSet<byte[]> seen)
    {
        if (source is null)
        {
            return;
        }
        foreach (var element in source)
        {
            if (element is null)
            {
                continue;
            }
            if (seen.Add(element))
            {
                result.Add(element);
            }
        }
    }

    private static IReadOnlyList<OrSetDeltaDot> BaselineUnionOrSetDeltaDots(
        IReadOnlyList<OrSetDeltaDot>? a,
        IReadOnlyList<OrSetDeltaDot>? b)
    {
        var result = new List<OrSetDeltaDot>((a?.Count ?? 0) + (b?.Count ?? 0));
        var seen = new HashSet<(string ReplicaId, long Counter, string Element)>();
        BaselineAppendOrSetDeltaDots(a, result, seen);
        BaselineAppendOrSetDeltaDots(b, result, seen);
        return result;
    }

    private static void BaselineAppendOrSetDeltaDots(
        IReadOnlyList<OrSetDeltaDot>? source,
        List<OrSetDeltaDot> result,
        HashSet<(string ReplicaId, long Counter, string Element)> seen)
    {
        if (source is null)
        {
            return;
        }
        foreach (var dot in source)
        {
            if (dot.Element is null)
            {
                continue;
            }
            var element = Convert.ToBase64String(dot.Element);
            if (seen.Add((dot.ReplicaId ?? string.Empty, dot.Counter, element)))
            {
                result.Add(dot);
            }
        }
    }

    private static IReadOnlyList<OrSetDeltaDot> OptimizedUnionOrSetDeltaDots(
        IReadOnlyList<OrSetDeltaDot>? a,
        IReadOnlyList<OrSetDeltaDot>? b)
    {
        var bound = (a?.Count ?? 0) + (b?.Count ?? 0);
        var result = new List<OrSetDeltaDot>(bound);
        var seen = new HashSet<(string ReplicaId, long Counter, byte[] Element)>(
            bound, BenchElementDotComparer.Instance);
        OptimizedAppendOrSetDeltaDots(a, result, seen);
        OptimizedAppendOrSetDeltaDots(b, result, seen);
        return result;
    }

    private static void OptimizedAppendOrSetDeltaDots(
        IReadOnlyList<OrSetDeltaDot>? source,
        List<OrSetDeltaDot> result,
        HashSet<(string ReplicaId, long Counter, byte[] Element)> seen)
    {
        if (source is null)
        {
            return;
        }
        foreach (var dot in source)
        {
            if (dot.Element is null)
            {
                continue;
            }
            if (seen.Add((dot.ReplicaId ?? string.Empty, dot.Counter, dot.Element)))
            {
                result.Add(dot);
            }
        }
    }

    /// <summary>Pre-change OR-Map combine: entry dictionary plus a wide dot-tuple order list plus a re-probe materialisation pass.</summary>
    private static OrMapDelta<string, PnCounter> BaselineCombineOrMapDelta(
        OrMapDelta<string, PnCounter> a,
        OrMapDelta<string, PnCounter> b)
    {
        var addsByDot = new Dictionary<(string Key, string ReplicaId, long Counter), OrMapDeltaEntry<string, PnCounter>>();
        var addsOrder = new List<(string Key, string ReplicaId, long Counter)>();
        BaselineAppendOrMapAdds(a.Adds, addsByDot, addsOrder);
        BaselineAppendOrMapAdds(b.Adds, addsByDot, addsOrder);

        var adds = addsOrder.Count == 0
            ? Array.Empty<OrMapDeltaEntry<string, PnCounter>>()
            : BaselineBuildOrderedAdds(addsByDot, addsOrder);

        var tombstoneSeen = new HashSet<(string Key, string ReplicaId, long Counter)>();
        var tombstones = new List<OrMapDeltaTombstone<string>>(
            (a.Tombstones?.Count ?? 0) + (b.Tombstones?.Count ?? 0));
        AppendOrMapTombstones(a.Tombstones, tombstones, tombstoneSeen);
        AppendOrMapTombstones(b.Tombstones, tombstones, tombstoneSeen);

        return new OrMapDelta<string, PnCounter>
        {
            Adds = adds,
            Tombstones = tombstones.Count == 0
                ? Array.Empty<OrMapDeltaTombstone<string>>()
                : tombstones,
        };
    }

    private static void BaselineAppendOrMapAdds(
        IReadOnlyList<OrMapDeltaEntry<string, PnCounter>>? source,
        Dictionary<(string Key, string ReplicaId, long Counter), OrMapDeltaEntry<string, PnCounter>> byDot,
        List<(string Key, string ReplicaId, long Counter)> order)
    {
        if (source is null)
        {
            return;
        }
        foreach (var entry in source)
        {
            var replicaId = entry.ReplicaId ?? string.Empty;
            var dot = (entry.Key, replicaId, entry.Counter);
            if (byDot.TryGetValue(dot, out var stored))
            {
                stored.Value.MergeFrom(entry.Value);
            }
            else
            {
                var clone = new PnCounter();
                clone.MergeFrom(entry.Value);
                byDot[dot] = new OrMapDeltaEntry<string, PnCounter>
                {
                    Key = entry.Key,
                    ReplicaId = replicaId,
                    Counter = entry.Counter,
                    Value = clone,
                };
                order.Add(dot);
            }
        }
    }

    private static OrMapDeltaEntry<string, PnCounter>[] BaselineBuildOrderedAdds(
        Dictionary<(string Key, string ReplicaId, long Counter), OrMapDeltaEntry<string, PnCounter>> byDot,
        List<(string Key, string ReplicaId, long Counter)> order)
    {
        var adds = new OrMapDeltaEntry<string, PnCounter>[order.Count];
        for (var i = 0; i < order.Count; i++)
        {
            adds[i] = byDot[order[i]];
        }
        return adds;
    }

    /// <summary>As shipped: dot-to-slot map, single probe, order carried by the result list itself.</summary>
    private static OrMapDelta<string, PnCounter> OptimizedCombineOrMapDelta(
        OrMapDelta<string, PnCounter> a,
        OrMapDelta<string, PnCounter> b)
    {
        var addBound = (a.Adds?.Count ?? 0) + (b.Adds?.Count ?? 0);
        var addSlots = new Dictionary<(string Key, string ReplicaId, long Counter), int>(addBound);
        var addList = new List<OrMapDeltaEntry<string, PnCounter>>(addBound);
        OptimizedAppendOrMapAdds(a.Adds, addSlots, addList);
        OptimizedAppendOrMapAdds(b.Adds, addSlots, addList);

        var tombBound = (a.Tombstones?.Count ?? 0) + (b.Tombstones?.Count ?? 0);
        var tombstoneSeen = new HashSet<(string Key, string ReplicaId, long Counter)>(tombBound);
        var tombstones = new List<OrMapDeltaTombstone<string>>(tombBound);
        AppendOrMapTombstones(a.Tombstones, tombstones, tombstoneSeen);
        AppendOrMapTombstones(b.Tombstones, tombstones, tombstoneSeen);

        return new OrMapDelta<string, PnCounter>
        {
            Adds = addList.Count == 0
                ? Array.Empty<OrMapDeltaEntry<string, PnCounter>>()
                : addList,
            Tombstones = tombstones.Count == 0
                ? Array.Empty<OrMapDeltaTombstone<string>>()
                : tombstones,
        };
    }

    private static void OptimizedAppendOrMapAdds(
        IReadOnlyList<OrMapDeltaEntry<string, PnCounter>>? source,
        Dictionary<(string Key, string ReplicaId, long Counter), int> slots,
        List<OrMapDeltaEntry<string, PnCounter>> adds)
    {
        if (source is null)
        {
            return;
        }
        foreach (var entry in source)
        {
            var replicaId = entry.ReplicaId ?? string.Empty;
            var dot = (entry.Key, replicaId, entry.Counter);
            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(slots, dot, out var existed);
            if (existed)
            {
                CollectionsMarshal.AsSpan(adds)[slot].Value.MergeFrom(entry.Value);
                continue;
            }

            var clone = new PnCounter();
            clone.MergeFrom(entry.Value);
            slot = adds.Count;
            adds.Add(new OrMapDeltaEntry<string, PnCounter>
            {
                Key = entry.Key,
                ReplicaId = replicaId,
                Counter = entry.Counter,
                Value = clone,
            });
        }
    }

    /// <summary>Shared by both OR-Map arms: the tombstone pass is unchanged apart from its presize, which lane (2) covers.</summary>
    private static void AppendOrMapTombstones(
        IReadOnlyList<OrMapDeltaTombstone<string>>? source,
        List<OrMapDeltaTombstone<string>> result,
        HashSet<(string Key, string ReplicaId, long Counter)> seen)
    {
        if (source is null)
        {
            return;
        }
        foreach (var tombstone in source)
        {
            var replicaId = tombstone.ReplicaId ?? string.Empty;
            if (seen.Add((tombstone.Key, replicaId, tombstone.Counter)))
            {
                result.Add(tombstone);
            }
        }
    }

    private sealed class BenchElementBytesComparer : IEqualityComparer<byte[]>
    {
        public static BenchElementBytesComparer Instance { get; } = new();

        public bool Equals(byte[]? x, byte[]? y) =>
            ReferenceEquals(x, y)
            || (x is not null && y is not null && x.AsSpan().SequenceEqual(y));

        public int GetHashCode(byte[] obj)
        {
            var hash = new HashCode();
            hash.AddBytes(obj);
            return hash.ToHashCode();
        }
    }

    private sealed class BenchElementDotComparer : IEqualityComparer<(string ReplicaId, long Counter, byte[] Element)>
    {
        public static BenchElementDotComparer Instance { get; } = new();

        public bool Equals(
            (string ReplicaId, long Counter, byte[] Element) x,
            (string ReplicaId, long Counter, byte[] Element) y) =>
            x.Counter == y.Counter
            && string.Equals(x.ReplicaId, y.ReplicaId, StringComparison.Ordinal)
            && (ReferenceEquals(x.Element, y.Element) || x.Element.AsSpan().SequenceEqual(y.Element));

        public int GetHashCode((string ReplicaId, long Counter, byte[] Element) obj)
        {
            var hash = new HashCode();
            hash.Add(obj.ReplicaId, StringComparer.Ordinal);
            hash.Add(obj.Counter);
            hash.AddBytes(obj.Element);
            return hash.ToHashCode();
        }
    }
}
