using System.Collections.Generic;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three remaining physical-shard fan-out sites that partition a
/// batch into a <em>richer-than-a-list</em> per-shard slot, which is why the
/// earlier dense-partitioning sweeps (which converted the
/// <c>List&lt;T&gt;</c>-bucketed sites) passed them over.
/// <para>
/// All three share one observation: the grouping key is a <em>physical</em>
/// shard index - a dense, non-negative domain of typically 1-16 values - while
/// the space being partitioned is sized to the batch. Hashing that tiny domain
/// once per item (twice on the first touch of each shard, because the shape was
/// a <c>TryGetValue</c> miss followed by an indexer store) is pure overhead
/// against an owner-indexed array. Each site additionally grew every per-shard
/// bucket from empty, so a bucket abandoned its whole entry array at each step
/// of the 3/7/17/37/71/163/353/761 rehash chain even though the method already
/// held a sound shard-fair bound on its final width.
/// </para>
/// <para>
/// The pairs mirror the production edits exactly:
/// (1) the cross-cluster replication apply leg
/// (<c>LatticeGrain.ApplyMergeManyCoreAsync</c>), which is the receiver-side
/// hot path for every shipped batch. Both arms reproduce its full outer shell -
/// the lazy single-shard fast path that only promotes to a per-shard map once a
/// second shard appears, and the whole-batch presize on the first bucket - so
/// only the promoted path differs;
/// (2) the tree-merge drain (<c>TreeMergeGrain.MergeShardAsync</c>), which
/// re-shards one source leaf's delta into the target map and repeats that for
/// every leaf of the source chain;
/// (3) the atomic write saga's terminal backstop
/// (<c>AtomicWriteGrain.BroadcastTerminalsAsync</c>), which computes each
/// shard's subset of the saga's committed values on every commit.
/// </para>
/// <para>
/// <c>Allocated</c> is the primary column for all three: it is deterministic and
/// reproduces bit-identically across rounds, whereas <c>Mean</c> carries this
/// shared host's timing noise and should be read as a direction tally over
/// several rounds rather than as a single delta.
/// </para>
/// <para>
/// <see cref="ApplyMerge_Contrast_PresizeOnly"/> is the deliberate contrast arm
/// for lane (1): it applies the cheaper half of the change (the shard-fair
/// bucket presize) without the dense owner indexing, so the marginal value of
/// the slot map is visible rather than asserted.
/// </para>
/// <para>
/// Only lane (1)'s prior shape carries <c>Baseline = true</c> (BenchmarkDotNet
/// permits one per class, matching the sibling suites); read each pair's own
/// <c>Mean</c> and <c>Allocated</c> rather than the cross-lane <c>Ratio</c>.
/// Run via <c>BENCH_MICROBENCH_SUITE=applymergefanout</c> (or
/// <c>--suite applymergefanout</c>); see <c>Program.cs</c>. The suite has no
/// Orleans silo dependency, so it is cheap to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class ReplicationApplyAndMergeFanoutBenchmarks
{
    private const int VirtualShardCount = 4096;
    private const int PhysicalShardCount = 8;

    /// <summary>A shipped replication batch, the receiver-side apply unit.</summary>
    private const int ApplyItemCount = 1000;

    /// <summary>One source leaf's delta, the tree-merge re-shard unit.</summary>
    private const int LeafDeltaCount = 512;

    /// <summary>A wide transactional write, the saga backstop unit.</summary>
    private const int SagaEntryCount = 1000;

    private ShardMap _shardMap = null!;
    private IReadOnlyList<int> _physicalShards = null!;

    private string[] _applyKeys = null!;
    private LwwValue<byte[]>[] _applyValues = null!;
    private int _applyBucketCapacity;

    private KeyValuePair<string, LwwValue<byte[]>>[] _leafDelta = null!;
    private int _leafBucketCapacity;

    private KeyValuePair<string, byte[]>[] _sagaEntries = null!;
    private int _sagaBucketCapacity;

    [GlobalSetup]
    public void Setup()
    {
        _shardMap = ShardMap.CreateDefault(VirtualShardCount, PhysicalShardCount);
        _physicalShards = _shardMap.GetPhysicalShardIndices();

        _applyKeys = new string[ApplyItemCount];
        _applyValues = new LwwValue<byte[]>[ApplyItemCount];
        for (var i = 0; i < ApplyItemCount; i++)
        {
            _applyKeys[i] = $"tenant-a/orders/{i:D6}";
            _applyValues[i] = new LwwValue<byte[]> { Value = new byte[16] };
        }

        _applyBucketCapacity = ShardFanout.BucketCapacity(ApplyItemCount, _physicalShards.Count);

        _leafDelta = new KeyValuePair<string, LwwValue<byte[]>>[LeafDeltaCount];
        for (var i = 0; i < LeafDeltaCount; i++)
        {
            _leafDelta[i] = new KeyValuePair<string, LwwValue<byte[]>>(
                $"tenant-a/merge/{i:D6}", new LwwValue<byte[]> { Value = new byte[16] });
        }

        _leafBucketCapacity = ShardFanout.BucketCapacity(LeafDeltaCount, _physicalShards.Count);

        _sagaEntries = new KeyValuePair<string, byte[]>[SagaEntryCount];
        for (var i = 0; i < SagaEntryCount; i++)
            _sagaEntries[i] = new KeyValuePair<string, byte[]>($"tenant-a/saga/{i:D6}", new byte[16]);

        _sagaBucketCapacity = ShardFanout.BucketCapacity(SagaEntryCount, _physicalShards.Count);
    }

    // =====================================================================
    // (1) The cross-cluster replication apply leg.
    // =====================================================================

    /// <summary>
    /// Prior shape: once a second shard appears, promote to a
    /// <c>Dictionary&lt;int, Dictionary&lt;string, LwwValue&lt;byte[]&gt;&gt;&gt;</c>
    /// - one hash of the dense shard index per item, two on each shard's first
    /// touch - and grow every promoted bucket from empty.
    /// </summary>
    [Benchmark(Baseline = true, Description = "(1) apply leg: hashed shard map, unsized buckets")]
    public int ApplyMerge_HashedUnsized()
    {
        var keys = _applyKeys;
        var values = _applyValues;

        Dictionary<int, Dictionary<string, LwwValue<byte[]>>>? byShard = null;
        var firstShard = -1;
        Dictionary<string, LwwValue<byte[]>>? firstBatch = null;

        for (var i = 0; i < keys.Length; i++)
        {
            var shardIndex = _shardMap.Resolve(keys[i]);

            if (firstBatch is null)
            {
                firstShard = shardIndex;
                firstBatch = new Dictionary<string, LwwValue<byte[]>>(capacity: keys.Length)
                {
                    [keys[i]] = values[i],
                };
                continue;
            }

            if (byShard is null && shardIndex == firstShard)
            {
                firstBatch[keys[i]] = values[i];
                continue;
            }

            byShard ??= new Dictionary<int, Dictionary<string, LwwValue<byte[]>>>
            {
                [firstShard] = firstBatch,
            };

            if (!byShard.TryGetValue(shardIndex, out var batch))
            {
                batch = new Dictionary<string, LwwValue<byte[]>>();
                byShard[shardIndex] = batch;
            }

            batch[keys[i]] = values[i];
        }

        var total = 0;
        if (byShard is null)
            return firstBatch!.Count;

        foreach (var (_, batch) in byShard)
            total += batch.Count;

        return total;
    }

    /// <summary>
    /// Contrast arm: the cheaper half of the change alone - the shard-fair
    /// bucket presize, keeping the hashed outer map. Shows how much of the win
    /// the dense owner indexing is actually responsible for.
    /// </summary>
    [Benchmark(Description = "(1) apply leg: CONTRAST - presize only, still hashed")]
    public int ApplyMerge_Contrast_PresizeOnly()
    {
        var keys = _applyKeys;
        var values = _applyValues;
        var bucketCapacity = _applyBucketCapacity;

        Dictionary<int, Dictionary<string, LwwValue<byte[]>>>? byShard = null;
        var firstShard = -1;
        Dictionary<string, LwwValue<byte[]>>? firstBatch = null;

        for (var i = 0; i < keys.Length; i++)
        {
            var shardIndex = _shardMap.Resolve(keys[i]);

            if (firstBatch is null)
            {
                firstShard = shardIndex;
                firstBatch = new Dictionary<string, LwwValue<byte[]>>(capacity: keys.Length)
                {
                    [keys[i]] = values[i],
                };
                continue;
            }

            if (byShard is null && shardIndex == firstShard)
            {
                firstBatch[keys[i]] = values[i];
                continue;
            }

            byShard ??= new Dictionary<int, Dictionary<string, LwwValue<byte[]>>>
            {
                [firstShard] = firstBatch,
            };

            if (!byShard.TryGetValue(shardIndex, out var batch))
            {
                batch = new Dictionary<string, LwwValue<byte[]>>(capacity: bucketCapacity);
                byShard[shardIndex] = batch;
            }

            batch[keys[i]] = values[i];
        }

        var total = 0;
        if (byShard is null)
            return firstBatch!.Count;

        foreach (var (_, batch) in byShard)
            total += batch.Count;

        return total;
    }

    /// <summary>
    /// Replacement shape, calling the real production primitive: the promoted
    /// map is a dense owner-indexed <see cref="ShardSlots{T}"/>, so the
    /// per-item hash of the shard index becomes a bounds-checked array read and
    /// the first-touch double probe disappears; each bucket is presized to the
    /// shard-fair fraction of the batch.
    /// </summary>
    [Benchmark(Description = "(1) apply leg: dense slots, shard-fair presize [production]")]
    public int ApplyMerge_DenseSlots()
    {
        var keys = _applyKeys;
        var values = _applyValues;
        var bucketCapacity = _applyBucketCapacity;

        ShardSlots<Dictionary<string, LwwValue<byte[]>>>? byShard = null;
        var firstShard = -1;
        Dictionary<string, LwwValue<byte[]>>? firstBatch = null;

        for (var i = 0; i < keys.Length; i++)
        {
            var shardIndex = _shardMap.Resolve(keys[i]);

            if (firstBatch is null)
            {
                firstShard = shardIndex;
                firstBatch = new Dictionary<string, LwwValue<byte[]>>(capacity: keys.Length)
                {
                    [keys[i]] = values[i],
                };
                continue;
            }

            if (byShard is null && shardIndex == firstShard)
            {
                firstBatch[keys[i]] = values[i];
                continue;
            }

            if (byShard is null)
            {
                byShard = new ShardSlots<Dictionary<string, LwwValue<byte[]>>>(_physicalShards);
                byShard.Set(firstShard, firstBatch);
            }

            var batch = byShard.Get(shardIndex);
            if (batch is null)
            {
                batch = new Dictionary<string, LwwValue<byte[]>>(capacity: bucketCapacity);
                byShard.Set(shardIndex, batch);
            }

            batch[keys[i]] = values[i];
        }

        var total = 0;
        if (byShard is null)
            return firstBatch!.Count;

        foreach (var (_, batch) in byShard)
            total += batch.Count;

        return total;
    }

    // =====================================================================
    // (2) The tree-merge drain's per-leaf re-shard.
    // =====================================================================

    /// <summary>
    /// Prior shape: an unsized <c>Dictionary&lt;int, ...&gt;</c> rebuilt per
    /// leaf, a miss-then-store double probe on each shard's first touch, and an
    /// unsized inner bucket per target shard.
    /// </summary>
    [Benchmark(Description = "(2) tree merge: hashed shard map, unsized buckets")]
    public int TreeMerge_HashedUnsized()
    {
        var delta = _leafDelta;
        var targetBuckets = new Dictionary<int, Dictionary<string, LwwValue<byte[]>>>();
        for (var i = 0; i < delta.Length; i++)
        {
            var targetIdx = _shardMap.Resolve(delta[i].Key);
            if (!targetBuckets.TryGetValue(targetIdx, out var bucket))
            {
                bucket = [];
                targetBuckets[targetIdx] = bucket;
            }

            bucket[delta[i].Key] = delta[i].Value;
        }

        var total = 0;
        foreach (var (_, bucket) in targetBuckets)
            total += bucket.Count;

        return total;
    }

    /// <summary>
    /// Replacement shape, calling the real production primitive: owner-indexed
    /// slots at a shard-fair capacity.
    /// </summary>
    [Benchmark(Description = "(2) tree merge: dense slots, shard-fair presize [production]")]
    public int TreeMerge_DenseSlots()
    {
        var delta = _leafDelta;
        var bucketCapacity = _leafBucketCapacity;
        var targetBuckets = new ShardSlots<Dictionary<string, LwwValue<byte[]>>>(_physicalShards);
        for (var i = 0; i < delta.Length; i++)
        {
            var targetIdx = _shardMap.Resolve(delta[i].Key);
            var bucket = targetBuckets.Get(targetIdx);
            if (bucket is null)
            {
                bucket = new Dictionary<string, LwwValue<byte[]>>(capacity: bucketCapacity);
                targetBuckets.Set(targetIdx, bucket);
            }

            bucket[delta[i].Key] = delta[i].Value;
        }

        var total = 0;
        foreach (var (_, bucket) in targetBuckets)
            total += bucket.Count;

        return total;
    }

    // =====================================================================
    // (3) The atomic write saga's terminal backstop subsets.
    // =====================================================================

    /// <summary>
    /// Prior shape: an unsized outer map, a miss-then-store double probe on each
    /// shard's first touch, and an unsized inner bucket per shard.
    /// </summary>
    [Benchmark(Description = "(3) saga backstop: unsized outer, double probe, unsized buckets")]
    public int SagaBackstop_UnsizedDoubleProbe()
    {
        var entries = _sagaEntries;
        var perShardCommitted = new Dictionary<int, Dictionary<string, byte[]>>();
        for (var i = 0; i < entries.Length; i++)
        {
            var owner = _shardMap.Resolve(entries[i].Key);
            if (!perShardCommitted.TryGetValue(owner, out var bucket))
            {
                bucket = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                perShardCommitted[owner] = bucket;
            }

            bucket[entries[i].Key] = entries[i].Value;
        }

        var total = 0;
        foreach (var (_, bucket) in perShardCommitted)
            total += bucket.Count;

        return total;
    }

    /// <summary>
    /// Replacement shape: the outer map is sized to the physical shard count
    /// once, the first-touch probe is folded to a single hash through
    /// <see cref="CollectionsMarshal.GetValueRefOrAddDefault{TKey,TValue}"/>,
    /// and each bucket is presized to the shard-fair fraction of the saga.
    /// <para>
    /// This lane deliberately keeps the outer <c>Dictionary</c> rather than
    /// taking the dense slot map of lanes (1) and (2): the production fan-out
    /// also writes shard indices drawn from the saga's touched-shard set, which
    /// can carry a transitively-discovered shard the current routing snapshot
    /// does not list, so a dense array indexed by that value is not sound here.
    /// </para>
    /// </summary>
    [Benchmark(Description = "(3) saga backstop: presized outer, folded probe, presized buckets [production]")]
    public int SagaBackstop_PresizedSingleProbe()
    {
        var entries = _sagaEntries;
        var bucketCapacity = _sagaBucketCapacity;
        var perShardCommitted = new Dictionary<int, Dictionary<string, byte[]>>(_physicalShards.Count);
        for (var i = 0; i < entries.Length; i++)
        {
            var owner = _shardMap.Resolve(entries[i].Key);
            ref var bucket = ref CollectionsMarshal.GetValueRefOrAddDefault(
                perShardCommitted, owner, out _);
            bucket ??= new Dictionary<string, byte[]>(bucketCapacity, StringComparer.Ordinal);
            bucket[entries[i].Key] = entries[i].Value;
        }

        var total = 0;
        foreach (var (_, bucket) in perShardCommitted)
            total += bucket.Count;

        return total;
    }
}
