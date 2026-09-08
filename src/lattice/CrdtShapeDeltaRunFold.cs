using System.Runtime.InteropServices;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice;

/// <summary>
/// Linear (single-pass) delta-run folds backing
/// <see cref="CrdtShape.CombineDeltaRun"/>.
/// <para>
/// Every combine on this type is a join over one or more grow-only
/// collections, so folding a run of <c>N</c> deltas pairwise re-materialises
/// the whole accumulated union on each of the <c>N-1</c> steps: the element
/// walk and the intermediate collections both grow with <c>N^2</c>. Each
/// helper here instead allocates the accumulator once, sized from the run's
/// own combined width, and appends every source into it exactly once. The
/// result is bit-identical to the pairwise fold because each shape's join is
/// commutative, associative, and idempotent, and because the per-source
/// append helpers are the same ones the pairwise combine calls.
/// </para>
/// </summary>
public sealed partial class CrdtShape
{
    private static OrSetDelta CombineOrSetDeltaRun(IReadOnlyList<object> run) => new()
    {
        Adds = UnionOrSetDeltaDotsRun(run, static d => ((OrSetDelta)d).Adds),
        Removes = UnionOrSetDeltaDotsRun(run, static d => ((OrSetDelta)d).Removes),
    };

    private static RwSetDelta CombineRwSetDeltaRun(IReadOnlyList<object> run) => new()
    {
        Adds = UnionOrSetDeltaDotsRun(run, static d => ((RwSetDelta)d).Adds),
        Removes = UnionOrSetDeltaDotsRun(run, static d => ((RwSetDelta)d).Removes),
        Tombstones = UnionOrSetDeltaDotsRun(run, static d => ((RwSetDelta)d).Tombstones),
    };

    private static OrFlagDelta CombineOrFlagDeltaRun(IReadOnlyList<object> run) => new()
    {
        Enables = UnionOrSetDotsRun(run, static d => ((OrFlagDelta)d).Enables),
        Disables = UnionOrSetDotsRun(run, static d => ((OrFlagDelta)d).Disables),
    };

    private static RwFlagDelta CombineRwFlagDeltaRun(IReadOnlyList<object> run) => new()
    {
        Enables = UnionOrSetDotsRun(run, static d => ((RwFlagDelta)d).Enables),
        Disables = UnionOrSetDotsRun(run, static d => ((RwFlagDelta)d).Disables),
        Tombstones = UnionOrSetDotsRun(run, static d => ((RwFlagDelta)d).Tombstones),
    };

    private static GSetDelta CombineGSetDeltaRun(IReadOnlyList<object> run)
    {
        var bound = SumCounts(run, static d => ((GSetDelta)d).Adds?.Count ?? 0);
        var result = new List<byte[]>(bound);
        var seen = new HashSet<byte[]>(bound, ElementBytesComparer.Instance);
        for (var i = 0; i < run.Count; i++)
        {
            AppendGSetElements(((GSetDelta)run[i]).Adds, result, seen);
        }

        return new GSetDelta { Adds = result };
    }

    private static RgaDelta CombineRgaDeltaRun(IReadOnlyList<object> run)
    {
        var insertBound = SumCounts(run, static d => ((RgaDelta)d).Inserts?.Count ?? 0);
        var inserts = new List<RgaDeltaNode>(insertBound);
        var insertSeen = new HashSet<OrSetDot>(insertBound);
        for (var i = 0; i < run.Count; i++)
        {
            AppendRgaInserts(((RgaDelta)run[i]).Inserts, inserts, insertSeen);
        }

        return new RgaDelta
        {
            Inserts = inserts,
            Tombstones = UnionOrSetDotsRun(run, static d => ((RgaDelta)d).Tombstones),
        };
    }

    private static PnCounterDelta CombinePnCounterDeltaRun(IReadOnlyList<object> run) => new()
    {
        Increments = PointwiseMaxLongRun(run, static d => ((PnCounterDelta)d).Increments),
        Decrements = PointwiseMaxLongRun(run, static d => ((PnCounterDelta)d).Decrements),
    };

    private static GCounterDelta CombineGCounterDeltaRun(IReadOnlyList<object> run) => new()
    {
        Increments = PointwiseMaxLongRun(run, static d => ((GCounterDelta)d).Increments),
    };

    private static VersionVectorDelta CombineVersionVectorDeltaRun(IReadOnlyList<object> run) => new()
    {
        Entries = PointwiseMaxHlcRun(run, static d => ((VersionVectorDelta)d).Entries),
    };

    private static MvRegisterDelta CombineMvRegisterDeltaRun(IReadOnlyList<object> run)
    {
        // The multi-value register resolves dot dominance rather than unioning,
        // so the fold runs through the primitive's own MergeFrom. Folding into
        // ONE transient accumulator visits each source register once; the
        // pairwise form rebuilt both operands and copied the whole dot context
        // out to a fresh dictionary on every step.
        var accumulator = ToMvRegister((MvRegisterDelta)run[0]);
        for (var i = 1; i < run.Count; i++)
        {
            accumulator.MergeFrom(ToMvRegister((MvRegisterDelta)run[i]));
        }

        return new MvRegisterDelta
        {
            // The accumulator is owned by this method and never touched again,
            // so its live entry list can back the combined delta directly.
            Entries = accumulator.Entries,
            Context = new Dictionary<string, long>(accumulator.Context, StringComparer.Ordinal),
        };
    }

    private static BoundedRegisterDelta CombineBoundedRegisterDeltaRun(IReadOnlyList<object> run, bool isMin)
    {
        // A directional register keeps a single winner, so the pairwise combine
        // is already O(1) per step and the run fold is a plain linear scan.
        var winner = (BoundedRegisterDelta)run[0];
        for (var i = 1; i < run.Count; i++)
        {
            winner = CombineBoundedRegisterDelta(winner, (BoundedRegisterDelta)run[i], isMin);
        }

        return winner;
    }

    private static OrMapDelta<TKey, TValue> CombineOrMapDeltaRun<TKey, TValue>(IReadOnlyList<object> run)
        where TKey : notnull
        where TValue : ICrdt<TValue>, new()
    {
        var addBound = SumCounts(run, static d => ((OrMapDelta<TKey, TValue>)d).Adds?.Count ?? 0);
        var addSlots = new Dictionary<(TKey Key, string ReplicaId, long Counter), int>(addBound);
        var addList = new List<OrMapDeltaEntry<TKey, TValue>>(addBound);

        var tombBound = SumCounts(run, static d => ((OrMapDelta<TKey, TValue>)d).Tombstones?.Count ?? 0);
        var tombstoneSeen = new HashSet<(TKey Key, string ReplicaId, long Counter)>(tombBound);
        var tombstones = new List<OrMapDeltaTombstone<TKey>>(tombBound);

        for (var i = 0; i < run.Count; i++)
        {
            var delta = (OrMapDelta<TKey, TValue>)run[i];
            AppendOrMapAdds(delta.Adds, addSlots, addList);
            AppendOrMapTombstones(delta.Tombstones, tombstones, tombstoneSeen);
        }

        return new OrMapDelta<TKey, TValue>
        {
            Adds = addList.Count == 0
                ? System.Array.Empty<OrMapDeltaEntry<TKey, TValue>>()
                : addList,
            Tombstones = tombstones.Count == 0
                ? System.Array.Empty<OrMapDeltaTombstone<TKey>>()
                : tombstones,
        };
    }

    // --- Shared N-ary union primitives ----------------------------------------
    //
    // Each sizes its accumulator from the SUM of the run's per-source widths,
    // which is a genuine UPPER bound on a dedup-union's result. A lower-bound
    // hint (the widest single source) would buy the cost of presizing without
    // buying the rehash chain it is meant to remove.

    private static int SumCounts(IReadOnlyList<object> run, Func<object, int> count)
    {
        var total = 0;
        for (var i = 0; i < run.Count; i++)
        {
            total += count(run[i]);
        }

        return total;
    }

    private static IReadOnlyList<OrSetDeltaDot> UnionOrSetDeltaDotsRun(
        IReadOnlyList<object> run,
        Func<object, IReadOnlyList<OrSetDeltaDot>?> select)
    {
        var bound = SumCounts(run, o => select(o)?.Count ?? 0);
        var result = new List<OrSetDeltaDot>(bound);
        var seen = new HashSet<(string ReplicaId, long Counter, byte[] Element)>(
            bound, ElementDotComparer.Instance);
        for (var i = 0; i < run.Count; i++)
        {
            AppendOrSetDeltaDots(select(run[i]), result, seen);
        }

        return result;
    }

    private static IReadOnlyList<OrSetDot> UnionOrSetDotsRun(
        IReadOnlyList<object> run,
        Func<object, IReadOnlyList<OrSetDot>?> select)
    {
        var bound = SumCounts(run, o => select(o)?.Count ?? 0);
        var result = new List<OrSetDot>(bound);
        var seen = new HashSet<OrSetDot>(bound);
        for (var i = 0; i < run.Count; i++)
        {
            var source = select(run[i]);
            if (source is null)
            {
                continue;
            }

            foreach (var dot in source)
            {
                if (seen.Add(dot))
                {
                    result.Add(dot);
                }
            }
        }

        return result;
    }

    private static Dictionary<string, long> PointwiseMaxLongRun(
        IReadOnlyList<object> run,
        Func<object, Dictionary<string, long>?> select)
    {
        // Seeded from the first non-empty source rather than from the summed
        // width: replica-keyed component maps repeat the SAME replica set on
        // every delta of a run, so the sum over-counts the union by the run
        // length while the first source is already the right order of magnitude.
        Dictionary<string, long>? result = null;
        for (var i = 0; i < run.Count; i++)
        {
            var source = select(run[i]);
            if (source is null)
            {
                continue;
            }

            if (result is null)
            {
                result = new Dictionary<string, long>(source, StringComparer.Ordinal);
                continue;
            }

            foreach (var (key, value) in source)
            {
                ref var existing = ref CollectionsMarshal.GetValueRefOrAddDefault(
                    result, key, out var existed);
                if (!existed || value > existing)
                {
                    existing = value;
                }
            }
        }

        return result ?? new Dictionary<string, long>(StringComparer.Ordinal);
    }

    private static Dictionary<string, HybridLogicalClock> PointwiseMaxHlcRun(
        IReadOnlyList<object> run,
        Func<object, Dictionary<string, HybridLogicalClock>?> select)
    {
        Dictionary<string, HybridLogicalClock>? result = null;
        for (var i = 0; i < run.Count; i++)
        {
            var source = select(run[i]);
            if (source is null)
            {
                continue;
            }

            if (result is null)
            {
                result = new Dictionary<string, HybridLogicalClock>(source, StringComparer.Ordinal);
                continue;
            }

            foreach (var (key, value) in source)
            {
                ref var existing = ref CollectionsMarshal.GetValueRefOrAddDefault(
                    result, key, out var existed);
                if (!existed || value.CompareTo(existing) > 0)
                {
                    existing = value;
                }
            }
        }

        return result ?? new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);
    }
}
