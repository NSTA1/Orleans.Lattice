using System;
using System.Collections.Generic;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the vector-clock presize added to the leaf snapshot row decoder in
/// <c>Orleans.Lattice.BPlusTree.State.LeafSnapshotCodec</c>.
/// <para>
/// <b>What was there.</b> <c>TryReadRowCore</c> reads a row's replica count and
/// bounds-checks it against the remaining frame <i>before</i> it reads a single
/// entry, then built the destination with <c>new VersionVector()</c> - a
/// default-capacity dictionary. Every entry was then inserted into a store that
/// had to grow its way up to a size the decoder already knew exactly, rehashing
/// and reinserting everything placed so far at each step (3 -> 7 -> 17).
/// </para>
/// <para>
/// <b>What ships.</b> An <c>internal static VersionVector.WithCapacity(int)</c>
/// factory hands the already-bounded count straight to the dictionary, so the
/// decode fills one correctly sized store and never rehashes. The capacity comes
/// from a count the caller has already rejected if it could not fit the frame,
/// so it is not a wire-controlled allocation. The type's public surface is
/// unchanged - the factory is internal and the existing private direct-assign
/// constructor is what it reuses.
/// </para>
/// <para>
/// <b>Baselines are verbatim.</b> <see cref="Baseline"/> fills a
/// <c>new VersionVector()</c> exactly as the decoder's loop did, and the shipped
/// lane fills a <c>WithCapacity</c> vector with the identical loop, so the only
/// difference measured is the growth the presize removes.
/// </para>
/// <para>
/// <b>Controls.</b> <see cref="Replicas"/> starts at <b>1</b>, which is the
/// control the trim is honestly bounded by: <c>Dictionary</c> rounds any
/// capacity of 1-3 to the same three buckets the default constructor allocates
/// on first insert, so at one replica the two lanes are the same allocation and
/// must show parity. The win can only appear from four replicas up, where the
/// baseline pays its first resize, and grows at eight where it pays two.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=vvpresize</c> (or
/// <c>--suite vvpresize</c>). No Orleans silo is involved, so it is cheap at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class VersionVectorPresizeBenchmarks
{
    /// <summary>The number of replicas the decoded row carries a clock for.</summary>
    [Params(1, 4, 16)]
    public int Replicas { get; set; }

    private string[] _replicaIds = [];
    private HybridLogicalClock[] _clocks = [];

    [GlobalSetup]
    public void Setup()
    {
        _replicaIds = new string[Replicas];
        _clocks = new HybridLogicalClock[Replicas];
        for (var i = 0; i < Replicas; i++)
        {
            _replicaIds[i] = $"silo-{i:D3}.lattice.internal/shard-{i}";
            _clocks[i] = new HybridLogicalClock { WallClockTicks = 638_000_000_000_000_000L + i, Counter = i };
        }

        AssertEquivalence();
    }

    /// <summary>
    /// Proves the presized vector is indistinguishable from the default-capacity
    /// one the decoder built: the same replica set, mapped to the same clocks,
    /// with the same bottom-ness. A presize that changed the comparer or dropped
    /// an entry would fail here rather than surface as a silent decode change.
    /// </summary>
    private void AssertEquivalence()
    {
        var legacy = Baseline.Fill(_replicaIds, _clocks);
        var shipped = Shipped(_replicaIds, _clocks);

        if (legacy.Entries.Count != shipped.Entries.Count || legacy.IsBottom != shipped.IsBottom)
        {
            throw new InvalidOperationException("Version-vector presize changed the decoded entry count.");
        }

        foreach (var (replica, clock) in legacy.Entries)
        {
            if (!shipped.Entries.TryGetValue(replica, out var other) || other != clock)
            {
                throw new InvalidOperationException($"Version-vector presize changed the clock for '{replica}'.");
            }
        }

        // The comparer must stay the default ordinal one: a presize that supplied
        // a different comparer would still round-trip its own keys but would
        // change how a caller's lookup behaves.
        if (!shipped.Entries.ContainsKey(_replicaIds[0]) || shipped.Entries.ContainsKey(_replicaIds[0].ToUpperInvariant()))
        {
            throw new InvalidOperationException("Version-vector presize changed the key comparer.");
        }
    }

    private static VersionVector Shipped(string[] replicas, HybridLogicalClock[] clocks)
    {
        var vector = VersionVector.WithCapacity(replicas.Length);
        for (var i = 0; i < replicas.Length; i++)
        {
            vector.Entries[replicas[i]] = clocks[i];
        }

        return vector;
    }

    /// <summary>The pre-change decode fill, into a default-capacity vector.</summary>
    [Benchmark(Baseline = true, Description = "Decode vector clock (baseline)")]
    public VersionVector FillLegacy() => Baseline.Fill(_replicaIds, _clocks);

    /// <summary>The shipped decode fill, into a vector presized to the bounded entry count.</summary>
    [Benchmark(Description = "Decode vector clock (shipped)")]
    public VersionVector FillShipped() => Shipped(_replicaIds, _clocks);

    /// <summary>
    /// The verbatim pre-change fill: a default-capacity <see cref="VersionVector"/>
    /// grown one insert at a time, exactly as <c>TryReadRowCore</c> did.
    /// </summary>
    private static class Baseline
    {
        internal static VersionVector Fill(string[] replicas, HybridLogicalClock[] clocks)
        {
            var vector = new VersionVector();
            for (var i = 0; i < replicas.Length; i++)
            {
                vector.Entries[replicas[i]] = clocks[i];
            }

            return vector;
        }
    }
}
