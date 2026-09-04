using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates three batch-path reductions - the per-shard fan-out bucketing every
/// multi-key read and write performs, the cross-leaf snapshot baseline union,
/// and the WAL batch-append partition grouping - so the per-operation time and
/// byte deltas are measurable in the clear.
/// <para>
/// All three lanes are CPU wins first, so <c>Mean</c> is the primary column;
/// lanes (1) and (2) also drop allocations (an over-presized bucket set and a
/// red-black tree plus a parallel map respectively), so <c>Allocated</c> moves
/// there too. The end-to-end cluster benchmarks route every operation through
/// Orleans serialization, persistence and task machinery, which buries a
/// per-entry hash probe below their run-to-run noise floor; each pair below
/// runs the prior shape against its replacement with no cluster in the loop, so
/// the delta is precisely the work the production change removes.
/// </para>
/// <para>
/// The pairs mirror the production edits:
/// (1) the batch fan-out bucketing in <c>LatticeGrain</c>
/// (<c>GetManyAsync</c>, <c>SetManyAsyncCore</c>, <c>SetManyWhereAsyncCore</c>,
/// <c>ApplyCrdtDeltaManyAsync</c>). Every batch entry was hashed into a
/// <c>Dictionary&lt;int, List&lt;T&gt;&gt;</c> keyed by the owning <em>physical</em>
/// shard index - a domain of 1-16 dense, non-negative values - and the read path
/// additionally presized every bucket to the whole batch size, so an 8-shard
/// 1000-key read reserved 8000 slots to store 1000 keys. The replacement buckets
/// into an owner-indexed <c>List&lt;T&gt;?[]</c> (no hashing at all) and presizes
/// each bucket to its shard-fair share. This lane calls the <b>real production
/// code</b> (<see cref="ShardFanout.BucketKeys"/>) on the optimized side;
/// (2) <c>ShardRootGrain.CaptureSnapshotBaselineAsync</c>'s cross-leaf union,
/// rebuilt on every snapshot capture. The prior form kept a
/// <c>SortedDictionary</c> (a red-black node per key, and two ordinal-string
/// tree walks per row - one to probe, one to store) <em>plus</em> a parallel
/// <c>Dictionary</c> of per-key merge modes (a second keyed store per row) and a
/// third keyed read per key at materialise time. The replacement folds value and
/// mode into one flat map written with a single probe, and imposes the ordinal
/// order with one final key sort - output-identical, since both orders are
/// ascending under <see cref="StringComparer.Ordinal"/> over a distinct key set.
/// This lane calls the <b>real production code</b>
/// (<c>ShardRootGrain.FoldRowsIntoUnion</c>) on the optimized side;
/// (3) <c>WalCommitLogWriter.AppendManyAsync</c>'s partition grouping, run on
/// every batched commit. The prior form built the same
/// <c>$"{treeId}/{partition}"</c> key three times over three parallel
/// dictionaries, costing per entry a string interpolation, a hash probe for the
/// entry list and a <em>second</em> full hash lookup to append the reverse
/// index. The replacement keeps one batch object per partition and memoizes the
/// last one, so the dominant single-partition batch formats and hashes its grain
/// key exactly once for the whole batch. The production method is async over
/// grain calls, so this lane reproduces the two grouping shapes exactly rather
/// than driving a silo.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=batchfolds</c> (or
/// <c>--suite batchfolds</c>); see <c>Program.cs</c>. The suite has no Orleans
/// silo dependency, so it is fast to run at <c>BENCH_MICROBENCH_FIDELITY=full</c>
/// for tight confidence intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class FanoutSnapshotAndWalBatchBenchmarks
{
    // ---- (1) the production fan-out shape: 4096 virtual slots over 8 physical
    //      shards, as ShardMap.CreateDefault emits, with a 1000-key batch ----
    private const int BatchKeyCount = 1000;

    private ShardMap _shardMap = null!;
    private IReadOnlyList<int> _physicalShards = null!;
    private string[] _batchKeys = null!;
    private KeyValuePair<string, byte[]>[] _batchEntries = null!;
    private int _readBucketCapacity;
    private int _fairBucketCapacity;

    // ---- (2) the snapshot union shape: 16 leaves x 400 rows, with a small
    //      donor-orphan overlap between adjacent leaves ----
    private const int SnapshotLeafCount = 16;
    private const int SnapshotRowsPerLeaf = 400;

    private List<LeafSnapshotRow>[] _snapshotLeaves = null!;

    // ---- (3) the WAL batch shape: 512 records for one tree, the dominant
    //      single-partition case plus a 4-partition spread ----
    private const int WalBatchCount = 512;

    private WalBatchEntry[] _walSinglePartition = null!;
    private WalBatchEntry[] _walSpreadPartitions = null!;

    [GlobalSetup]
    public void Setup()
    {
        _shardMap = ShardMap.CreateDefault(4096, 8);
        _physicalShards = _shardMap.GetPhysicalShardIndices();

        _batchKeys = new string[BatchKeyCount];
        _batchEntries = new KeyValuePair<string, byte[]>[BatchKeyCount];
        for (var i = 0; i < BatchKeyCount; i++)
        {
            var key = $"tenant-a/orders/{i:D6}";
            _batchKeys[i] = key;
            _batchEntries[i] = new KeyValuePair<string, byte[]>(key, new byte[16]);
        }

        // The pre-change read path presized every bucket to the whole batch;
        // the write path already used the shard-fair shape the change now
        // applies uniformly.
        _readBucketCapacity = BatchKeyCount;
        _fairBucketCapacity = ShardFanout.BucketCapacity(BatchKeyCount, _physicalShards.Count);

        _snapshotLeaves = new List<LeafSnapshotRow>[SnapshotLeafCount];
        for (var leaf = 0; leaf < SnapshotLeafCount; leaf++)
        {
            var rows = new List<LeafSnapshotRow>(SnapshotRowsPerLeaf);
            for (var r = 0; r < SnapshotRowsPerLeaf; r++)
            {
                // Leaves own disjoint ranges, bar a deliberate two-row overlap
                // with the previous leaf standing in for a donor orphan, which
                // is the branch that exercises the LWW merge and mode carry.
                var ordinal = (leaf * SnapshotRowsPerLeaf) + r - (r < 2 ? 2 : 0);
                rows.Add(new LeafSnapshotRow(
                    $"tenant-a/catalog/{ordinal:D8}",
                    LwwValue<byte[]>.Create(
                        new byte[24],
                        new HybridLogicalClock { WallClockTicks = 1_000 + ordinal, Counter = 0 }),
                    r % 3 == 0 ? LatticeMergeMode.LwwRegister : null));
            }

            _snapshotLeaves[leaf] = rows;
        }

        _walSinglePartition = BuildWalBatch(partitionSpread: 1);
        _walSpreadPartitions = BuildWalBatch(partitionSpread: 4);
    }

    private static WalBatchEntry[] BuildWalBatch(int partitionSpread)
    {
        var batch = new WalBatchEntry[WalBatchCount];
        for (var i = 0; i < WalBatchCount; i++)
            batch[i] = new WalBatchEntry("orders-tree", i % partitionSpread, i);

        return batch;
    }

    // =====================================================================
    // (1) Batch fan-out bucketing by owning physical shard.
    // =====================================================================

    /// <summary>
    /// Prior shape: hash every batch key's owning physical shard index into a
    /// dictionary, with each bucket presized to the whole batch.
    /// </summary>
    [Benchmark(Baseline = true, Description = "(1) fanout: hashed buckets, batch-sized presize")]
    public int FanOutBucketing_Hashed()
    {
        var buckets = new Dictionary<int, List<string>>();
        for (var i = 0; i < _batchKeys.Length; i++)
        {
            var key = _batchKeys[i];
            var idx = _shardMap.Resolve(key);
            if (!buckets.TryGetValue(idx, out var bucket))
            {
                bucket = new List<string>(capacity: _readBucketCapacity);
                buckets[idx] = bucket;
            }

            bucket.Add(key);
        }

        var total = 0;
        foreach (var (_, bucket) in buckets)
            total += bucket.Count;

        return total;
    }

    /// <summary>
    /// Replacement shape, calling the real production helper: an owner-indexed
    /// dense array with a shard-fair bucket presize and no hashing.
    /// </summary>
    [Benchmark(Description = "(1) fanout: dense owner array, shard-fair presize [production]")]
    public int FanOutBucketing_Dense()
    {
        var buckets = ShardFanout.BucketKeys(
            _batchKeys, _shardMap, _physicalShards, _fairBucketCapacity);

        var total = 0;
        foreach (var (_, bucket) in buckets)
            total += bucket.Count;

        return total;
    }

    /// <summary>
    /// The write-side variant, which fans out key/value entries rather than
    /// bare keys. Prior shape.
    /// </summary>
    [Benchmark(Description = "(1b) fanout entries: hashed buckets")]
    public int FanOutEntries_Hashed()
    {
        var buckets = new Dictionary<int, List<KeyValuePair<string, byte[]>>>();
        for (var i = 0; i < _batchEntries.Length; i++)
        {
            var entry = _batchEntries[i];
            var idx = _shardMap.Resolve(entry.Key);
            if (!buckets.TryGetValue(idx, out var bucket))
            {
                bucket = new List<KeyValuePair<string, byte[]>>(capacity: _fairBucketCapacity);
                buckets[idx] = bucket;
            }

            bucket.Add(entry);
        }

        var total = 0;
        foreach (var (_, bucket) in buckets)
            total += bucket.Count;

        return total;
    }

    /// <summary>
    /// The write-side variant, calling the real production helper.
    /// </summary>
    [Benchmark(Description = "(1b) fanout entries: dense owner array [production]")]
    public int FanOutEntries_Dense()
    {
        var buckets = ShardFanout.BucketEntries(
            _batchEntries, _shardMap, _physicalShards, _fairBucketCapacity);

        var total = 0;
        foreach (var (_, bucket) in buckets)
            total += bucket.Count;

        return total;
    }

    // =====================================================================
    // (2) Cross-leaf snapshot baseline union.
    // =====================================================================

    /// <summary>
    /// Prior shape: a <see cref="SortedDictionary{TKey, TValue}"/> of values
    /// probed and stored per row, a parallel merge-mode map stored per row, and
    /// a third keyed read per key while materialising.
    /// </summary>
    [Benchmark(Description = "(2) snapshot union: sorted tree + parallel mode map")]
    public int SnapshotUnion_SortedPlusParallelMap()
    {
        var union = new SortedDictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal);
        var unionModes = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal);

        for (var leaf = 0; leaf < _snapshotLeaves.Length; leaf++)
        {
            var rows = _snapshotLeaves[leaf];
            for (var i = 0; i < rows.Count; i++)
            {
                var row = rows[i];
                if (union.TryGetValue(row.Key, out var existing))
                {
                    var merged = LwwValue<byte[]>.Merge(existing, row.Value);
                    union[row.Key] = merged;
                    if (ReferenceEquals(merged.Value, row.Value.Value)
                        || merged.Timestamp.Equals(row.Value.Timestamp))
                    {
                        if (row.MergeMode is { } incoming)
                            unionModes[row.Key] = incoming;
                        else
                            unionModes.Remove(row.Key);
                    }
                }
                else
                {
                    union[row.Key] = row.Value;
                    if (row.MergeMode is { } mode)
                        unionModes[row.Key] = mode;
                }
            }
        }

        var materialised = new List<LeafSnapshotRow>(union.Count);
        foreach (var (key, value) in union)
        {
            var mode = unionModes.TryGetValue(key, out var m) ? m : (LatticeMergeMode?)null;
            materialised.Add(new LeafSnapshotRow(key, value, mode));
        }

        return materialised.Count;
    }

    /// <summary>
    /// Replacement shape, calling the real production fold: one flat map
    /// carrying value and mode together, written with a single probe per row,
    /// then one ordinal key sort.
    /// </summary>
    [Benchmark(Description = "(2) snapshot union: single-probe flat map + one sort [production]")]
    public int SnapshotUnion_FlatMapPlusSort()
    {
        var union = new Dictionary<string, (LwwValue<byte[]> Value, LatticeMergeMode? Mode)>(
            StringComparer.Ordinal);

        for (var leaf = 0; leaf < _snapshotLeaves.Length; leaf++)
        {
            if (leaf == 0)
                union.EnsureCapacity(_snapshotLeaves[0].Count * _snapshotLeaves.Length);

            ShardRootGrain.FoldRowsIntoUnion(_snapshotLeaves[leaf], union);
        }

        var orderedKeys = new string[union.Count];
        union.Keys.CopyTo(orderedKeys, 0);
        Array.Sort(orderedKeys, StringComparer.Ordinal);

        var materialised = new List<LeafSnapshotRow>(orderedKeys.Length);
        foreach (var key in orderedKeys)
        {
            var (value, mergeMode) = union[key];
            materialised.Add(new LeafSnapshotRow(key, value, mergeMode));
        }

        return materialised.Count;
    }

    // =====================================================================
    // (3) WAL batch-append partition grouping.
    // =====================================================================

    /// <summary>
    /// Prior shape: three parallel dictionaries keyed by the same interpolated
    /// grain key, so each entry pays a string format, a probe for the entry
    /// list and a second full hash lookup for the reverse index.
    /// </summary>
    [Benchmark(Description = "(3) wal batch: 3 parallel maps, key re-formatted per entry")]
    public int WalGrouping_ParallelMaps()
        => WalGroupingParallelMaps(_walSinglePartition) + WalGroupingParallelMaps(_walSpreadPartitions);

    /// <summary>
    /// Replacement shape: one batch object per partition with a last-partition
    /// memo, so a single-partition batch formats and hashes its grain key once.
    /// </summary>
    [Benchmark(Description = "(3) wal batch: one map + last-partition memo [production shape]")]
    public int WalGrouping_MemoizedSingleMap()
        => WalGroupingMemoized(_walSinglePartition) + WalGroupingMemoized(_walSpreadPartitions);

    private static int WalGroupingParallelMaps(WalBatchEntry[] batch)
    {
        var partitionEntries = new Dictionary<string, List<int>>(StringComparer.Ordinal);
        var partitionReverse = new Dictionary<string, List<int>>(StringComparer.Ordinal);
        var partitionMeta = new Dictionary<string, (string TreeId, int Partition)>(StringComparer.Ordinal);

        for (var i = 0; i < batch.Length; i++)
        {
            var entry = batch[i];
            var grainKey = $"{entry.TreeId}/{entry.Partition}";
            if (!partitionEntries.TryGetValue(grainKey, out var list))
            {
                list = [];
                partitionEntries[grainKey] = list;
                partitionReverse[grainKey] = [];
                partitionMeta[grainKey] = (entry.TreeId, entry.Partition);
            }

            list.Add(entry.Payload);
            partitionReverse[grainKey].Add(i);
        }

        var total = 0;
        foreach (var (_, list) in partitionEntries)
            total += list.Count;

        return total;
    }

    private static int WalGroupingMemoized(WalBatchEntry[] batch)
    {
        var partitions = new Dictionary<string, WalBatchGroup>(StringComparer.Ordinal);
        string? lastTreeId = null;
        var lastPartition = -1;
        WalBatchGroup? lastGroup = null;

        for (var i = 0; i < batch.Length; i++)
        {
            var entry = batch[i];
            WalBatchGroup group;
            if (lastGroup is not null && lastPartition == entry.Partition
                && string.Equals(lastTreeId, entry.TreeId, StringComparison.Ordinal))
            {
                group = lastGroup;
            }
            else
            {
                var grainKey = $"{entry.TreeId}/{entry.Partition}";
                if (!partitions.TryGetValue(grainKey, out var existing))
                {
                    var capacity = Math.Max(4, batch.Length / 4);
                    existing = new WalBatchGroup(entry.TreeId, entry.Partition, capacity);
                    partitions[grainKey] = existing;
                }

                group = existing;
                lastTreeId = entry.TreeId;
                lastPartition = entry.Partition;
                lastGroup = existing;
            }

            group.Entries.Add(entry.Payload);
            group.Reverse.Add(i);
        }

        var total = 0;
        foreach (var group in partitions.Values)
            total += group.Entries.Count;

        return total;
    }

    private readonly record struct WalBatchEntry(string TreeId, int Partition, int Payload);

    private sealed class WalBatchGroup(string treeId, int partition, int capacity)
    {
        public string TreeId { get; } = treeId;

        public int Partition { get; } = partition;

        public List<int> Entries { get; } = new(capacity);

        public List<int> Reverse { get; } = new(capacity);
    }
}
