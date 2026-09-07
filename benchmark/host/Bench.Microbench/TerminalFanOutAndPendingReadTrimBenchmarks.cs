using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.InteropServices;

using BenchmarkDotNet.Attributes;

using Orleans.Lattice.Primitives;
using Orleans.Runtime;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the two core-library accumulator trims shipped in this change so
/// their per-operation byte deltas are measurable in the clear.
/// <para>
/// Lane 1 reproduces <c>ShardRootGrain.BroadcastTerminalToLeavesAsync</c>'s
/// per-leaf grouping of a saga's committed-value backstop. The prior form grew
/// every per-leaf subset dictionary from empty even though the method already
/// held two sound bounds on a subset's final width - the saga's key count and
/// the prepare-phase leaf count - so the shipped form gives each subset the
/// shard-fair capacity the shared <c>ShardFanout.BucketCapacity</c> floor/cap
/// computes from them. Two refinements were measured and rejected, and both
/// ship as arms: folding the map's read-probe-plus-store into one
/// <see cref="CollectionsMarshal.GetValueRefOrAddDefault{TKey, TValue}"/> (no
/// byte saving, and measurably more CPU, because the map is seeded with every
/// prepare-phase leaf up front so the key is already present), and guessing a
/// subset width on the no-prepare path, where no leaf count exists to divide
/// by.
/// </para>
/// <para>
/// Lane 2 reproduces <c>BPlusLeafGrain.SnapshotPendingForReadAsync</c>'s
/// per-read pending-key union. The prior form grew that map from empty on every
/// scan-path read taken while a saga is in flight; the shipped form sums the
/// pending-tx bucket widths first - an exact upper bound on the union, and an
/// exact count in the dominant single-saga case - and hints the map with it.
/// </para>
/// <para>
/// Both lanes run against the real <see cref="GrainId"/> and
/// <see cref="LwwValue{T}"/> value types so the reported delta is the collection
/// overhead the edit removes, not a payload artefact of a stand-in. Each
/// baseline arm reproduces its optimized arm's surrounding shell verbatim - same
/// inputs, same iteration order, same emitted contents - so the only difference
/// between the arms is the shape under test. Each lane also ships a contrast arm
/// for the cheaper alternative that was considered and rejected, so the
/// incremental value of the full change is visible rather than asserted.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=terminalpendingtrim</c> (or
/// <c>--suite terminalpendingtrim</c>); see <c>Program.cs</c>. The suite has no
/// Orleans silo dependency, so it is fast to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class TerminalFanOutAndPendingReadTrimBenchmarks
{
    /// <summary>Keys a mid-sized cross-shard saga commits on one shard.</summary>
    private const int SagaKeys = 256;

    /// <summary>Leaves those keys route to on this shard.</summary>
    private const int SagaLeaves = 8;

    /// <summary>Concurrent sagas holding prepared keys on one leaf.</summary>
    private const int PendingSagas = 4;

    /// <summary>Prepared keys each of those sagas holds on that leaf.</summary>
    private const int PendingKeysPerSaga = 96;

    private KeyValuePair<string, byte[]>[] _committedValues = [];
    private GrainId[] _keyToLeaf = [];
    private HashSet<GrainId> _trackedAffected = [];

    private Dictionary<Guid, Dictionary<string, LwwValue<byte[]>>> _pendingTx = [];

    /// <summary>Builds the inputs shared by both lanes' arms.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var leaves = new GrainId[SagaLeaves];
        for (var i = 0; i < SagaLeaves; i++)
        {
            leaves[i] = GrainId.Create("bplusleaf", "tree/0/leaf-" + i.ToString("D3", CultureInfo.InvariantCulture));
        }

        // The saga's committed-value backstop, and the leaf each key routes to.
        // Keys are interleaved across leaves exactly as a range-partitioned
        // traversal hands them back, so every arm sees the same probe sequence.
        _committedValues = new KeyValuePair<string, byte[]>[SagaKeys];
        _keyToLeaf = new GrainId[SagaKeys];
        for (var i = 0; i < SagaKeys; i++)
        {
            _committedValues[i] = new KeyValuePair<string, byte[]>(
                "saga-key-" + i.ToString("D5", CultureInfo.InvariantCulture),
                new byte[16]);
            _keyToLeaf[i] = leaves[i % SagaLeaves];
        }

        // The prepare-phase leaves. Every leaf that owns a committed key also
        // took a prepare, which is the shape a completed saga presents.
        _trackedAffected = new HashSet<GrainId>(leaves);

        // The leaf's pending-tx buckets: several in-flight sagas, each holding a
        // disjoint run of prepared keys, which is the shape a scan-path read
        // walks while sagas are outstanding.
        var clock = HybridLogicalClock.Tick(HybridLogicalClock.Zero);
        _pendingTx = new Dictionary<Guid, Dictionary<string, LwwValue<byte[]>>>(PendingSagas);
        for (var s = 0; s < PendingSagas; s++)
        {
            var bucket = new Dictionary<string, LwwValue<byte[]>>(PendingKeysPerSaga, StringComparer.Ordinal);
            for (var k = 0; k < PendingKeysPerSaga; k++)
            {
                var key = "tx" + s.ToString(CultureInfo.InvariantCulture)
                    + "-key-" + k.ToString("D4", CultureInfo.InvariantCulture);
                bucket[key] = LwwValue<byte[]>.Create(new byte[16], clock);
            }

            _pendingTx[Guid.NewGuid()] = bucket;
        }
    }

    // ------------------------------------------------------------------
    // Lane 1: BroadcastTerminalToLeavesAsync per-leaf target grouping
    // ------------------------------------------------------------------

    /// <summary>
    /// The shape before the change: the per-leaf target map is seeded from the
    /// prepare-phase leaves, then each committed key costs a
    /// <c>TryGetValue</c> plus an indexer store whenever the leaf's subset has
    /// not been created yet, and every subset dictionary grows from empty.
    /// </summary>
    [Benchmark(Baseline = true, Description = "TerminalFanOut: unhinted subsets (baseline)")]
    public int TerminalFanOut_Baseline()
    {
        var leafTargets = new Dictionary<GrainId, Dictionary<string, byte[]>?>(_trackedAffected.Count);
        foreach (var id in _trackedAffected)
        {
            leafTargets[id] = null;
        }

        for (var i = 0; i < _committedValues.Length; i++)
        {
            var leafId = _keyToLeaf[i];
            if (!leafTargets.TryGetValue(leafId, out var bucket) || bucket is null)
            {
                bucket = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                leafTargets[leafId] = bucket;
            }

            bucket[_committedValues[i].Key] = _committedValues[i].Value;
        }

        return leafTargets.Count;
    }

    /// <summary>
    /// The shipped shape: each subset is created with the shard-fair capacity
    /// the shared fan-out helper computes from the saga's key count and the
    /// prepare-phase leaf count. The probe sequence is byte-for-byte the
    /// baseline's, so the reported delta is exactly the resize chain the hint
    /// removes.
    /// </summary>
    [Benchmark(Description = "TerminalFanOut: hinted subsets (optimized)")]
    public int TerminalFanOut_Optimized()
    {
        var leafTargets = new Dictionary<GrainId, Dictionary<string, byte[]>?>(_trackedAffected.Count);
        foreach (var id in _trackedAffected)
        {
            leafTargets[id] = null;
        }

        var subsetCapacity = BucketCapacity(_committedValues.Length, Math.Max(1, _trackedAffected.Count));
        for (var i = 0; i < _committedValues.Length; i++)
        {
            var leafId = _keyToLeaf[i];
            if (!leafTargets.TryGetValue(leafId, out var bucket) || bucket is null)
            {
                bucket = new Dictionary<string, byte[]>(subsetCapacity, StringComparer.Ordinal);
                leafTargets[leafId] = bucket;
            }

            bucket[_committedValues[i].Key] = _committedValues[i].Value;
        }

        return leafTargets.Count;
    }

    /// <summary>
    /// The refinement that was measured and rejected: fold the read probe and
    /// the miss-path store into one ref-returning probe, which also collapses
    /// the "absent" and "seeded but null" cases into a single write-through.
    /// It is the shape this repository's earlier fan-out sweeps shipped, and it
    /// is wrong here: those sweeps folded a probe whose key is usually
    /// <em>absent</em>, whereas this map is seeded with every prepare-phase leaf
    /// up front, so in the steady state the key is already present and the
    /// add-capable probe is pure overhead. It removes no bytes at all.
    /// </summary>
    [Benchmark(Description = "TerminalFanOut: hinted subsets + ref probe fold (rejected)")]
    public int TerminalFanOut_Rejected_RefProbeFold()
    {
        var leafTargets = new Dictionary<GrainId, Dictionary<string, byte[]>?>(_trackedAffected.Count);
        foreach (var id in _trackedAffected)
        {
            leafTargets[id] = null;
        }

        var subsetCapacity = BucketCapacity(_committedValues.Length, Math.Max(1, _trackedAffected.Count));
        for (var i = 0; i < _committedValues.Length; i++)
        {
            var leafId = _keyToLeaf[i];
            ref var bucket = ref CollectionsMarshal.GetValueRefOrAddDefault(leafTargets, leafId, out _);
            bucket ??= new Dictionary<string, byte[]>(subsetCapacity, StringComparer.Ordinal);
            bucket[_committedValues[i].Key] = _committedValues[i].Value;
        }

        return leafTargets.Count;
    }

    /// <summary>
    /// The no-prepare path, which the change deliberately leaves alone: an abort
    /// or a replayed terminal whose prepare set never reached this shard root
    /// arrives with no leaf count to derive a subset width from. The arm is here
    /// to pin that the path is untouched, next to the guess that was rejected
    /// for it.
    /// </summary>
    [Benchmark(Description = "TerminalFanOut (no prepare set): unhinted, unchanged")]
    public int TerminalFanOut_NoTracked_Unchanged()
    {
        var leafTargets = new Dictionary<GrainId, Dictionary<string, byte[]>?>();
        for (var i = 0; i < _committedValues.Length; i++)
        {
            var leafId = _keyToLeaf[i];
            if (!leafTargets.TryGetValue(leafId, out var bucket) || bucket is null)
            {
                bucket = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                leafTargets[leafId] = bucket;
            }

            bucket[_committedValues[i].Key] = _committedValues[i].Value;
        }

        return leafTargets.Count;
    }

    /// <summary>
    /// The guess that was measured and rejected for that path: with no prepare
    /// set, treat the saga's key count as the subset width. Every leaf then
    /// reserves room for the whole payload, which is why the shipped code
    /// applies no hint at all when it has no leaf count to divide by.
    /// </summary>
    [Benchmark(Description = "TerminalFanOut (no prepare set): width guessed from key count (rejected)")]
    public int TerminalFanOut_NoTracked_Rejected_GuessedWidth()
    {
        var leafTargets = new Dictionary<GrainId, Dictionary<string, byte[]>?>(
            Math.Min(_committedValues.Length, 64));
        var subsetCapacity = BucketCapacity(_committedValues.Length, 1);
        for (var i = 0; i < _committedValues.Length; i++)
        {
            var leafId = _keyToLeaf[i];
            if (!leafTargets.TryGetValue(leafId, out var bucket) || bucket is null)
            {
                bucket = new Dictionary<string, byte[]>(subsetCapacity, StringComparer.Ordinal);
                leafTargets[leafId] = bucket;
            }

            bucket[_committedValues[i].Key] = _committedValues[i].Value;
        }

        return leafTargets.Count;
    }

    // ------------------------------------------------------------------
    // Lane 2: SnapshotPendingForReadAsync pending-key union
    // ------------------------------------------------------------------

    /// <summary>
    /// The shape before the change: the per-read pending-key union grows from
    /// empty across every in-flight saga's prepared bucket, reallocating its
    /// backing store up the whole chain on each scan-path read.
    /// </summary>
    [Benchmark(Description = "PendingRead: union grows from empty (baseline)")]
    public int PendingRead_Baseline()
    {
        var txids = new List<Guid>(_pendingTx.Count);
        var pendingKeys = new Dictionary<string, (Guid, LwwValue<byte[]>)>();
        foreach (var (txid, bucket) in _pendingTx)
        {
            txids.Add(txid);
            foreach (var (key, value) in bucket)
            {
                pendingKeys.TryAdd(key, (txid, value));
            }
        }

        return pendingKeys.Count + txids.Count;
    }

    /// <summary>
    /// The shipped shape: a pre-pass sums the bucket widths - an exact upper
    /// bound on the union, and an exact count when the sagas touch disjoint keys
    /// - and hints the map with it. The union body is identical.
    /// </summary>
    [Benchmark(Description = "PendingRead: union hinted by summed widths (optimized)")]
    public int PendingRead_Optimized()
    {
        var txids = new List<Guid>(_pendingTx.Count);
        var pendingBound = 0;
        foreach (var bucket in _pendingTx.Values)
        {
            pendingBound += bucket.Count;
        }

        var pendingKeys = new Dictionary<string, (Guid, LwwValue<byte[]>)>(
            Math.Min(pendingBound, PendingReadKeyCapacityLimit));
        foreach (var (txid, bucket) in _pendingTx)
        {
            txids.Add(txid);
            foreach (var (key, value) in bucket)
            {
                pendingKeys.TryAdd(key, (txid, value));
            }
        }

        return pendingKeys.Count + txids.Count;
    }

    /// <summary>
    /// The cheaper alternative that was considered and rejected: hint the union
    /// with the widest single bucket, the conventional bound for a union of maps
    /// that may overlap. It is sound but loose here - concurrent sagas hold
    /// disjoint keys, so the widest bucket under-counts the union by the saga
    /// count and leaves most of the resize chain in place.
    /// </summary>
    [Benchmark(Description = "PendingRead: union hinted by widest bucket (contrast)")]
    public int PendingRead_Contrast_WidestBucket()
    {
        var txids = new List<Guid>(_pendingTx.Count);
        var widest = 0;
        foreach (var bucket in _pendingTx.Values)
        {
            if (bucket.Count > widest)
            {
                widest = bucket.Count;
            }
        }

        var pendingKeys = new Dictionary<string, (Guid, LwwValue<byte[]>)>(widest);
        foreach (var (txid, bucket) in _pendingTx)
        {
            txids.Add(txid);
            foreach (var (key, value) in bucket)
            {
                pendingKeys.TryAdd(key, (txid, value));
            }
        }

        return pendingKeys.Count + txids.Count;
    }

    /// <summary>
    /// Mirrors the production clamp on the pending-key capacity hint.
    /// </summary>
    private const int PendingReadKeyCapacityLimit = 4096;

    /// <summary>
    /// Mirrors <c>ShardFanout.BucketCapacity</c>, the shared shard-fair
    /// floor/cap the production grouping now uses for each per-leaf subset.
    /// </summary>
    private static int BucketCapacity(int itemCount, int shardCount)
        => Math.Min(Math.Max(4, itemCount / Math.Max(1, shardCount)), 256);
}
