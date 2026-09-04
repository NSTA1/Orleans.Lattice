using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three hash-probe reductions made to <c>Orleans.Lattice</c>'s
/// slot-routing, view-maintainer staging, and CRDT delta-combine paths, so the
/// per-operation time and byte deltas are measurable in the clear.
/// <para>
/// These are CPU wins first and allocation wins second, so the column to read
/// is <c>Mean</c>, not just <c>Allocated</c>. The end-to-end cluster benchmarks
/// route every operation through Orleans serialization, persistence and task
/// machinery, which buries a per-slot or per-mutation probe fold below their
/// run-to-run noise floor; each pair below runs the prior shape against its
/// replacement with no cluster in the loop, so the delta is precisely the work
/// the production change removes.
/// </para>
/// <para>
/// The pairs mirror the production edits:
/// (1) <see cref="LatticeGrain.BuildOwnedSlotMap"/>, which partitions the
/// virtual slot space by owning physical shard for every <c>CountAsync</c> /
/// <c>CountPerShardAsync</c> fan-out. The prior form hashed each virtual slot
/// five times (a counting <c>TryGetValue</c> plus indexer set, then a cursor
/// read, bucket read and cursor write on the fill pass) across two intermediate
/// <c>Dictionary&lt;int, int&gt;</c> instances. Physical shard indices are
/// small, dense and non-negative while the slot array is sized to the virtual
/// count (4096 by default), so the replacement buckets into owner-indexed
/// arrays and hashes nothing per slot. This lane calls the <b>real production
/// code</b> on the optimized side;
/// (2) the view maintainer's per-mutation staging accumulators -
/// <c>StagedTransaction.NoteOffset</c> (a min-fold), <c>StagePrepare</c>
/// (a read-then-overwrite) and <c>RecordOrdinaryOverStagedKey</c> (a max-fold) -
/// each a <c>TryGetValue</c> followed by an indexer set on the same key, so two
/// hash probes per staged entry, collapsed to one via
/// <see cref="CollectionsMarshal.GetValueRefOrAddDefault{TKey, TValue}"/>. The
/// staging types are private to the grain, so this lane reproduces the two
/// shapes exactly rather than calling through a silo;
/// (3) <c>CrdtShape</c>'s pointwise-max delta combine (<c>PointwiseMaxLong</c> /
/// <c>PointwiseMaxHlc</c>), which backs the PnCounter, GCounter and
/// VersionVector shipping-side delta coalesce. Same double-probe fold, same
/// single-probe replacement. The optimized side runs the <b>real production
/// code</b> through the public <c>CrdtShape.CombineDeltas</c> delegate.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=slotfolds</c> (or
/// <c>--suite slotfolds</c>); see <c>Program.cs</c>. The suite has no Orleans
/// silo dependency, so it is fast to run at <c>BENCH_MICROBENCH_FIDELITY=full</c>
/// for tight confidence intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class SlotPartitionAndProbeFoldBenchmarks
{
    // ---- (1) the production routing shape: 4096 virtual slots over 16
    //      physical shards, as ShardMap.CreateDefault emits ----
    private ShardMap _shardMap = null!;

    // ---- (2) one atomic batch's worth of staged mutations, with the
    //      re-stage and ordinary-supersede traffic the drain actually sees ----
    private LatticeMutation[] _prepares = null!;
    private LatticeMutation[] _ordinary = null!;
    private int[] _partitions = null!;
    private long[] _offsets = null!;

    // ---- (3) two PnCounter deltas over a realistic replica set, overlapping
    //      so the fold's raise-the-max branch is the common case ----
    private PnCounterDelta _deltaA;
    private PnCounterDelta _deltaB;
    private CrdtShape _pnShape = null!;

    /// <summary>Builds the inputs shared by the benchmark pairs.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _shardMap = ShardMap.CreateDefault(4096, 16);

        const int batch = 256;
        const int distinctKeys = 64;
        const int partitions = 8;

        _prepares = new LatticeMutation[batch];
        _ordinary = new LatticeMutation[batch];
        _partitions = new int[batch];
        _offsets = new long[batch];
        for (var i = 0; i < batch; i++)
        {
            var key = "source-key-" + (i % distinctKeys).ToString("D4");
            _prepares[i] = new LatticeMutation
            {
                TreeId = "tree",
                Key = key,
                Value = [(byte)i],
                Timestamp = new HybridLogicalClock { WallClockTicks = i, Counter = 0 },
                AtomicBatchSize = distinctKeys,
                AtomicBatchIndex = i % distinctKeys,
                IsPrepared = true,
            };
            _ordinary[i] = new LatticeMutation
            {
                TreeId = "tree",
                Key = key,
                Value = [(byte)i],
                Timestamp = new HybridLogicalClock { WallClockTicks = batch + i, Counter = 0 },
            };
            _partitions[i] = i % partitions;
            // Descending within a partition so the min-fold's update branch,
            // not its no-op branch, is what is measured.
            _offsets[i] = batch - i;
        }

        const int replicas = 32;
        var incA = new Dictionary<string, long>(replicas, StringComparer.Ordinal);
        var decA = new Dictionary<string, long>(replicas, StringComparer.Ordinal);
        var incB = new Dictionary<string, long>(replicas, StringComparer.Ordinal);
        var decB = new Dictionary<string, long>(replicas, StringComparer.Ordinal);
        for (var i = 0; i < replicas; i++)
        {
            var replica = "replica-" + i.ToString("D3");
            incA[replica] = i;
            decA[replica] = i;
            // B carries the higher count for every replica, so each entry
            // raises the running max - the branch the fold exists to serve.
            incB[replica] = i + replicas;
            decB[replica] = i + replicas;
        }

        _deltaA = new PnCounterDelta { Increments = incA, Decrements = decA };
        _deltaB = new PnCounterDelta { Increments = incB, Decrements = decB };
        _pnShape = CrdtShape.ForPnCounter();
    }

    // ========================================================================
    // (1) virtual-slot -> owning-shard partitioning
    // ========================================================================

    /// <summary>
    /// The prior shape: two intermediate hashed dictionaries and five hash
    /// probes per virtual slot.
    /// </summary>
    [Benchmark]
    public int BuildOwnedSlotMap_Baseline_HashedDictionaries()
    {
        var slots = _shardMap.Slots;
        var counts = new Dictionary<int, int>();
        for (var s = 0; s < slots.Length; s++)
        {
            var owner = slots[s];
            counts.TryGetValue(owner, out var c);
            counts[owner] = c + 1;
        }

        var result = new Dictionary<int, int[]>(counts.Count);
        var cursors = new Dictionary<int, int>(counts.Count);
        foreach (var kv in counts)
        {
            result[kv.Key] = new int[kv.Value];
            cursors[kv.Key] = 0;
        }

        for (var s = 0; s < slots.Length; s++)
        {
            var owner = slots[s];
            var pos = cursors[owner];
            result[owner][pos] = s;
            cursors[owner] = pos + 1;
        }
        return result.Count;
    }

    /// <summary>
    /// The shipped shape, calling the real production partitioner: dense
    /// owner-indexed buckets, no per-slot hashing.
    /// </summary>
    [Benchmark]
    public int BuildOwnedSlotMap_Optimized_DenseBuckets()
        => LatticeGrain.BuildOwnedSlotMap(_shardMap).Count;

    // ========================================================================
    // (2) view-maintainer per-mutation staging accumulators
    // ========================================================================

    /// <summary>
    /// The prior shape of the three staging folds: a <c>TryGetValue</c>
    /// followed by an indexer set on the same key, so two hash probes each.
    /// </summary>
    [Benchmark]
    public long StagingFolds_Baseline_DoubleProbe()
    {
        var preparesByIndex = new Dictionary<int, LatticeMutation>();
        var minOffsetByPartition = new Dictionary<int, long>();
        var ordinaryHlc = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);
        long stagedBytes = 0;

        for (var i = 0; i < _prepares.Length; i++)
        {
            // NoteOffset: min-fold.
            var partition = _partitions[i];
            var offset = _offsets[i];
            if (!minOffsetByPartition.TryGetValue(partition, out var cur) || offset < cur)
            {
                minOffsetByPartition[partition] = offset;
            }

            // StagePrepare: read the superseded entry, then overwrite.
            var mutation = _prepares[i];
            var bytes = (long)(mutation.Key?.Length ?? 0) + (mutation.Value?.Length ?? 0);
            if (preparesByIndex.TryGetValue(mutation.AtomicBatchIndex, out var prev))
            {
                stagedBytes -= (long)(prev.Key?.Length ?? 0) + (prev.Value?.Length ?? 0);
            }
            preparesByIndex[mutation.AtomicBatchIndex] = mutation;
            stagedBytes += bytes;

            // RecordOrdinaryOverStagedKey: max-fold.
            var ordinary = _ordinary[i];
            if (!ordinaryHlc.TryGetValue(ordinary.Key, out var prevHlc)
                || ordinary.Timestamp.CompareTo(prevHlc) > 0)
            {
                ordinaryHlc[ordinary.Key] = ordinary.Timestamp;
            }
        }

        return stagedBytes + minOffsetByPartition.Count + ordinaryHlc.Count;
    }

    /// <summary>
    /// The shipped shape: each fold collapsed to a single hash probe via
    /// <see cref="CollectionsMarshal.GetValueRefOrAddDefault{TKey, TValue}"/>.
    /// </summary>
    [Benchmark]
    public long StagingFolds_Optimized_SingleProbe()
    {
        var preparesByIndex = new Dictionary<int, LatticeMutation>();
        var minOffsetByPartition = new Dictionary<int, long>();
        var ordinaryHlc = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);
        long stagedBytes = 0;

        for (var i = 0; i < _prepares.Length; i++)
        {
            var partition = _partitions[i];
            var offset = _offsets[i];
            ref var cur = ref CollectionsMarshal.GetValueRefOrAddDefault(
                minOffsetByPartition, partition, out var hadOffset);
            if (!hadOffset || offset < cur)
            {
                cur = offset;
            }

            var mutation = _prepares[i];
            var bytes = (long)(mutation.Key?.Length ?? 0) + (mutation.Value?.Length ?? 0);
            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(
                preparesByIndex, mutation.AtomicBatchIndex, out var hadPrepare);
            if (hadPrepare)
            {
                stagedBytes -= (long)(slot.Key?.Length ?? 0) + (slot.Value?.Length ?? 0);
            }
            slot = mutation;
            stagedBytes += bytes;

            var ordinary = _ordinary[i];
            ref var prevHlc = ref CollectionsMarshal.GetValueRefOrAddDefault(
                ordinaryHlc, ordinary.Key, out var hadHlc);
            if (!hadHlc || ordinary.Timestamp.CompareTo(prevHlc) > 0)
            {
                prevHlc = ordinary.Timestamp;
            }
        }

        return stagedBytes + minOffsetByPartition.Count + ordinaryHlc.Count;
    }

    // ========================================================================
    // (3) CRDT pointwise-max delta combine
    // ========================================================================

    /// <summary>
    /// The prior shape of the pointwise-max fold: a <c>TryGetValue</c> followed
    /// by an indexer set, so two hash probes for every entry that raises the
    /// running max.
    /// </summary>
    [Benchmark]
    public int PointwiseMaxCombine_Baseline_DoubleProbe()
    {
        var increments = PointwiseMaxLongBaseline(_deltaA.Increments, _deltaB.Increments);
        var decrements = PointwiseMaxLongBaseline(_deltaA.Decrements, _deltaB.Decrements);
        return increments.Count + decrements.Count;
    }

    /// <summary>
    /// The shipped shape, calling the real production combine through the
    /// public <c>CrdtShape.CombineDeltas</c> delegate.
    /// </summary>
    [Benchmark]
    public int PointwiseMaxCombine_Optimized_SingleProbe()
    {
        var combined = (PnCounterDelta)_pnShape.CombineDeltas!(_deltaA, _deltaB);
        return combined.Increments.Count + combined.Decrements.Count;
    }

    private static Dictionary<string, long> PointwiseMaxLongBaseline(
        Dictionary<string, long>? a,
        Dictionary<string, long>? b)
    {
        var result = a is null
            ? new Dictionary<string, long>(b?.Count ?? 0, StringComparer.Ordinal)
            : new Dictionary<string, long>(a, StringComparer.Ordinal);
        if (b is not null)
        {
            foreach (var (key, value) in b)
            {
                if (!result.TryGetValue(key, out var existing) || value > existing)
                {
                    result[key] = value;
                }
            }
        }
        return result;
    }
}
