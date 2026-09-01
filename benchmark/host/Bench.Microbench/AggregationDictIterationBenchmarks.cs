using System;
using System.Collections.Generic;
using System.Globalization;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three fresh-dictionary direct-iteration trims made to
/// <c>Orleans.Lattice.Views.AggregationApplier</c> so their per-operation byte
/// delta is measurable in the clear. The aggregation applier reduces every view
/// contribution against freshly-materialised dictionaries - the per-contribution
/// accumulator slot map, and the per-materialise inverse/fold-inverse shard maps
/// decoded from the store. Each of these is a <b>fresh</b> dictionary, so a
/// <c>.Keys</c> / <c>.Values</c> access on it allocates a throwaway
/// <c>KeyCollection</c> / <c>ValueCollection</c> wrapper on first touch (a
/// long-lived dictionary caches the wrapper in its <c>_keys</c>/<c>_values</c>
/// field and pays nothing after the first access, but these maps live for a
/// single call). Walking the dictionary through its struct enumerator instead
/// removes that wrapper allocation while visiting exactly the same entries.
/// <para>
/// The pairs mirror the production edits verbatim, completing the direct-iteration
/// pass that <c>WorstKey</c>/<c>LargestSourceKey</c> already started:
/// (1) <c>ContributeNumericAsync</c> - the opportunistic cleanup loop walked
/// <c>slots.Keys</c> over the fresh per-contribution accumulator map;
/// (2) <c>MaterialiseInverseAsync</c> - the shard-gather walked
/// <c>shards.Values</c> over the fresh GetMany result and, nested inside,
/// <c>DecodeInverse(bytes).Values</c> over each fresh per-shard decode, so it
/// dropped <b>one wrapper for the outer map plus one per shard</b>;
/// (3) <c>MaterialiseFoldAsync</c> - the shard-gather walked <c>shards.Values</c>
/// over the fresh GetMany result.
/// </para>
/// <para>
/// Each lane builds the fresh dictionaries inside the benchmark (as production
/// does per call) so the wrapper allocation is charged, and both lanes in a pair
/// build them identically - the only difference is <c>.Keys</c>/<c>.Values</c>
/// versus the struct enumerator - so the measured <c>Allocated</c> delta is
/// precisely the collection-wrapper heap the production change removes. The maps
/// are keyed <see cref="StringComparer.Ordinal"/> exactly as the applier's are.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=aggiter</c> (or <c>--suite aggiter</c>);
/// see <c>Program.cs</c>. The suite has no Orleans silo dependency, so it is fast
/// to run at <c>BENCH_MICROBENCH_FIDELITY=full</c> for tight confidence intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class AggregationDictIterationBenchmarks
{
    // A minimal stand-in for the applier's AccumulatorRow / MemberEntry value
    // types. Only its presence as the dictionary's TValue matters; the wrapper
    // allocation the trims remove is independent of the value type.
    private readonly record struct Entry(long Count, double Numeric);

    // ---- (1) numeric-contribution cleanup: a fresh 1-2 entry slots map ----
    private string[] _slotKeys = null!;

    // ---- (2)/(3) materialise shard-gather: fresh outer + per-shard maps ----
    private string[] _shardKeys = null!;
    private string[][] _memberKeys = null!;

    /// <summary>Builds the key inputs the fresh maps are rebuilt from per op.</summary>
    [GlobalSetup]
    public void Setup()
    {
        // A same-group overwrite touches two accumulator slots (old + new); a
        // fresh-group contribution touches one. Two is the steady-state upper
        // bound and the shape the cleanup loop walks.
        _slotKeys = new[] { "grp-a\0s0", "grp-b\0s0" };

        // A group's inverse/fold-inverse rows are sharded by _fanout; a handful
        // of shards is the common case. Each shard decodes to a small member map.
        const int shardCount = 8;
        const int membersPerShard = 8;
        _shardKeys = new string[shardCount];
        _memberKeys = new string[shardCount][];
        for (var s = 0; s < shardCount; s++)
        {
            _shardKeys[s] = "inv\0grp\0" + s.ToString("D2", CultureInfo.InvariantCulture);
            var members = new string[membersPerShard];
            for (var m = 0; m < membersPerShard; m++)
            {
                members[m] = "src-" + (s * membersPerShard + m).ToString("D4", CultureInfo.InvariantCulture);
            }

            _memberKeys[s] = members;
        }
    }

    // ------------------------------------------------------------------
    // (1) Numeric-contribution cleanup loop
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: <c>foreach (var key in slots.Keys)</c> over the fresh
    /// per-contribution accumulator map allocates a throwaway
    /// <c>KeyCollection</c> on first access.
    /// </summary>
    [Benchmark(Baseline = true, Description = "Numeric cleanup: slots.Keys (baseline)")]
    public int NumericCleanup_Keys()
    {
        var slots = BuildSlots();
        var acc = 0;
        foreach (var key in slots.Keys)
        {
            acc += key.Length;
        }

        return acc;
    }

    /// <summary>
    /// Optimized: iterating the fresh map directly uses only the struct
    /// enumerator, allocating no <c>KeyCollection</c>.
    /// </summary>
    [Benchmark(Description = "Numeric cleanup: direct (optimized)")]
    public int NumericCleanup_Direct()
    {
        var slots = BuildSlots();
        var acc = 0;
        foreach (var (key, _) in slots)
        {
            acc += key.Length;
        }

        return acc;
    }

    private Dictionary<string, Entry> BuildSlots()
    {
        var slots = new Dictionary<string, Entry>(StringComparer.Ordinal);
        for (var i = 0; i < _slotKeys.Length; i++)
        {
            slots[_slotKeys[i]] = new Entry(i + 1, i);
        }

        return slots;
    }

    // ------------------------------------------------------------------
    // (2) Inverse-materialise shard gather (nested .Values)
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: <c>foreach (var v in shards.Values)</c> over the fresh GetMany
    /// result plus <c>foreach (var e in DecodeInverse(bytes).Values)</c> over each
    /// fresh per-shard decode allocates one <c>ValueCollection</c> for the outer
    /// map and one for every shard.
    /// </summary>
    [Benchmark(Description = "Inverse materialise: nested .Values (baseline)")]
    public double InverseMaterialise_Values()
    {
        var shards = BuildShards();
        var extreme = double.NegativeInfinity;
        foreach (var inner in shards.Values)
        {
            foreach (var entry in inner.Values)
            {
                extreme = Math.Max(extreme, entry.Numeric);
            }
        }

        return extreme;
    }

    /// <summary>
    /// Optimized: walking both fresh maps through their struct enumerators
    /// allocates neither <c>ValueCollection</c>.
    /// </summary>
    [Benchmark(Description = "Inverse materialise: nested direct (optimized)")]
    public double InverseMaterialise_Direct()
    {
        var shards = BuildShards();
        var extreme = double.NegativeInfinity;
        foreach (var (_, inner) in shards)
        {
            foreach (var (_, entry) in inner)
            {
                extreme = Math.Max(extreme, entry.Numeric);
            }
        }

        return extreme;
    }

    // ------------------------------------------------------------------
    // (3) Fold-materialise shard gather (outer .Values)
    // ------------------------------------------------------------------

    /// <summary>
    /// Baseline: <c>foreach (var v in shards.Values)</c> over the fresh GetMany
    /// result allocates a throwaway <c>ValueCollection</c> on first access; the
    /// inner map is already walked directly.
    /// </summary>
    [Benchmark(Description = "Fold materialise: shards.Values (baseline)")]
    public int FoldMaterialise_Values()
    {
        var shards = BuildShards();
        var acc = 0;
        foreach (var inner in shards.Values)
        {
            foreach (var (sourceKey, _) in inner)
            {
                acc += sourceKey.Length;
            }
        }

        return acc;
    }

    /// <summary>
    /// Optimized: walking the fresh outer map through its struct enumerator
    /// allocates no <c>ValueCollection</c>.
    /// </summary>
    [Benchmark(Description = "Fold materialise: direct (optimized)")]
    public int FoldMaterialise_Direct()
    {
        var shards = BuildShards();
        var acc = 0;
        foreach (var (_, inner) in shards)
        {
            foreach (var (sourceKey, _) in inner)
            {
                acc += sourceKey.Length;
            }
        }

        return acc;
    }

    private Dictionary<string, Dictionary<string, Entry>> BuildShards()
    {
        var shards = new Dictionary<string, Dictionary<string, Entry>>(_shardKeys.Length, StringComparer.Ordinal);
        for (var s = 0; s < _shardKeys.Length; s++)
        {
            var members = _memberKeys[s];
            var inner = new Dictionary<string, Entry>(members.Length, StringComparer.Ordinal);
            for (var m = 0; m < members.Length; m++)
            {
                inner[members[m]] = new Entry(1, m);
            }

            shards[_shardKeys[s]] = inner;
        }

        return shards;
    }
}
