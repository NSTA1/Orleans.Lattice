using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three physical-shard fan-out sites that the dense-partitioning
/// sweep had not yet reached, so the per-operation time and byte deltas are
/// measurable in the clear.
/// <para>
/// Each pair runs the shape the production code used before the change against
/// the shape it uses after, and every "after" lane calls the <b>real production
/// primitive</b> rather than a copy of it. The end-to-end cluster benchmarks
/// route each of these through Orleans serialization, persistence and task
/// machinery, which buries a per-item hash probe below their run-to-run noise
/// floor; with no cluster in the loop the delta here is precisely the work the
/// change removes.
/// </para>
/// <para>
/// The common observation is the same one that drove the earlier sweeps: all
/// three partition by a <em>physical</em> shard index - a dense, non-negative
/// domain of typically 1-16 values - while the space being partitioned is sized
/// to the batch or to the whole restore stream. Hashing that tiny domain once
/// (or, on a miss, twice) per item is pure overhead against an owner-indexed
/// array.
/// </para>
/// <para>
/// The pairs mirror the production edits:
/// (1) the atomic write saga's prepare fan-out
/// (<c>AtomicWriteGrain.PrepareAsync</c>), run once per transactional write. It
/// hashed every entry into an unsized <c>Dictionary&lt;int, List&lt;(string, int)&gt;&gt;</c>
/// of unsized tuple lists, holding a second reference to each key purely so the
/// per-shard capture could read it back. The replacement calls
/// <see cref="ShardFanout.BucketIndices{T}"/>, which indexes buckets by owner,
/// gives each a shard-fair capacity, and buckets bare <c>int</c> indices - a
/// quarter the element width, with no second key reference;
/// (2) the public bulk-load fan-out (<c>LatticeExtensions.BulkLoadAsync</c>),
/// which maintained <b>four</b> parallel dictionaries keyed by the same physical
/// shard index: one hash per buffered entry and six more per chunk flush. The
/// replacement collapses all four into one <c>ShardSlots&lt;T&gt;</c> of a single
/// per-shard slot object, so the per-entry hash becomes an array read and the
/// per-flush six become none;
/// (3) the streaming restore accumulators
/// (<c>LatticeBackupRestoreService.BulkLoadRawAsync</c> and
/// <c>MergeApplyAsync</c>), which accumulated the whole restore stream through
/// an unsized <c>Dictionary&lt;int, ...&gt;</c> double-probe per record and grew
/// each per-shard merge batch from empty despite flushing it at a known,
/// caller-supplied size. The replacement uses the same <c>ShardSlots&lt;T&gt;</c>
/// and presizes each batch to its flush bound.
/// </para>
/// <para>
/// Lanes (1) and (3) are allocation wins as well as CPU wins, so
/// <c>Allocated</c> is the primary column for them - it is deterministic and
/// reproduces exactly, where <c>Mean</c> carries the host's timing noise. Lane
/// (2) is a pure probe fold whose allocation is byte-identical on both sides by
/// construction, so <c>Mean</c> is the column that moves there. Only lane (1)
/// carries <c>Baseline = true</c> (BenchmarkDotNet permits one per class, and
/// this matches the sibling suites); read each pair's own <c>Mean</c> and
/// <c>Allocated</c> rather than the cross-lane <c>Ratio</c>. Run via
/// <c>BENCH_MICROBENCH_SUITE=fanoutslots</c> (or <c>--suite fanoutslots</c>);
/// see <c>Program.cs</c>. The suite has no Orleans silo dependency, so it is
/// cheap to run at <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class SagaPrepareAndRestoreFanoutBenchmarks
{
    private const int VirtualShardCount = 4096;
    private const int PhysicalShardCount = 8;

    // ---- (1) the saga shape: a transactional write over a 1000-entry batch ----
    private const int SagaEntryCount = 1000;

    // ---- (2) the bulk-load shape: 10 default chunks buffered and flushed ----
    private const int BulkLoadEntryCount = 10_000;
    private const int BulkLoadChunkSize = 1000;

    // ---- (3) the restore shape: a 20k-record stream flushed in 4k batches ----
    private const int RestoreRecordCount = 20_000;
    private const int RestoreBatchSize = 4096;

    private ShardMap _shardMap = null!;
    private IReadOnlyList<int> _physicalShards = null!;

    private KeyValuePair<string, byte[]>[] _sagaEntries = null!;
    private int _sagaBucketCapacity;

    private KeyValuePair<string, byte[]>[] _bulkLoadEntries = null!;
    private int[] _bulkLoadShardIndices = null!;
    private string[] _restoreKeys = null!;

    [GlobalSetup]
    public void Setup()
    {
        _shardMap = ShardMap.CreateDefault(VirtualShardCount, PhysicalShardCount);
        _physicalShards = _shardMap.GetPhysicalShardIndices();

        _sagaEntries = new KeyValuePair<string, byte[]>[SagaEntryCount];
        for (var i = 0; i < SagaEntryCount; i++)
            _sagaEntries[i] = new KeyValuePair<string, byte[]>($"tenant-a/orders/{i:D6}", new byte[16]);

        _sagaBucketCapacity = ShardFanout.BucketCapacity(SagaEntryCount, _physicalShards.Count);

        _bulkLoadEntries = new KeyValuePair<string, byte[]>[BulkLoadEntryCount];
        _bulkLoadShardIndices = new int[BulkLoadEntryCount];
        for (var i = 0; i < BulkLoadEntryCount; i++)
        {
            _bulkLoadEntries[i] = new KeyValuePair<string, byte[]>($"tenant-a/import/{i:D7}", new byte[16]);
            _bulkLoadShardIndices[i] = _shardMap.Resolve(_bulkLoadEntries[i].Key);
        }

        _restoreKeys = new string[RestoreRecordCount];
        for (var i = 0; i < RestoreRecordCount; i++)
            _restoreKeys[i] = $"tenant-a/restore/{i:D7}";
    }

    // =====================================================================
    // (1) The atomic write saga's prepare fan-out.
    // =====================================================================

    /// <summary>
    /// Prior shape: hash every entry into an unsized dictionary of unsized
    /// <c>(Key, Index)</c> tuple lists, so each bucket element carries a second
    /// reference to a key the saga already holds.
    /// </summary>
    [Benchmark(Baseline = true, Description = "(1) saga prepare: hashed dict of (key, index) tuple lists")]
    public int SagaPrepare_HashedTuples()
    {
        var entries = _sagaEntries;
        var byShard = new Dictionary<int, List<(string Key, int Index)>>();
        for (var i = 0; i < entries.Length; i++)
        {
            var shardIndex = _shardMap.Resolve(entries[i].Key);
            if (!byShard.TryGetValue(shardIndex, out var bucket))
            {
                bucket = [];
                byShard[shardIndex] = bucket;
            }

            bucket.Add((entries[i].Key, i));
        }

        var total = 0;
        foreach (var (_, bucket) in byShard)
            total += bucket.Count;

        return total;
    }

    /// <summary>
    /// Replacement shape, calling the real production helper: owner-indexed
    /// buckets at a shard-fair capacity, holding bare indices.
    /// </summary>
    [Benchmark(Description = "(1) saga prepare: dense index buckets [production]")]
    public int SagaPrepare_DenseIndices()
    {
        var buckets = ShardFanout.BucketIndices(
            _sagaEntries, static e => e.Key, _shardMap, _physicalShards, _sagaBucketCapacity);

        var total = 0;
        foreach (var (_, bucket) in buckets)
            total += bucket.Count;

        return total;
    }

    // =====================================================================
    // (2) The public bulk-load fan-out's per-shard state lookup.
    // =====================================================================

    /// <summary>
    /// Prior shape: four parallel dictionaries keyed by the same physical shard
    /// index - one probe per buffered entry, and six more on every chunk flush.
    /// <para>
    /// Both lanes are fed pre-resolved shard indices, so the measured region is
    /// exactly the per-entry state lookup the change touches. The production
    /// path resolves each key first in both shapes, and that resolve (a string
    /// hash roughly five times the cost of the probe) is identical on both
    /// sides, so leaving it in the loop would only bury the delta under work
    /// the change does not affect.
    /// </para>
    /// </summary>
    [Benchmark(Description = "(2) bulk load: four parallel dictionaries keyed by shard")]
    public int BulkLoad_ParallelDictionaries()
    {
        var buffers = new Dictionary<int, List<KeyValuePair<string, byte[]>>>();
        var inFlight = new Dictionary<int, Task>();
        var chunkCounters = new Dictionary<int, int>();
        var shards = new Dictionary<int, object>();

        foreach (var shardIndex in _physicalShards)
        {
            buffers[shardIndex] = new List<KeyValuePair<string, byte[]>>(BulkLoadChunkSize);
            inFlight[shardIndex] = Task.CompletedTask;
            chunkCounters[shardIndex] = 0;
            shards[shardIndex] = ShardSentinel;
        }

        var entries = _bulkLoadEntries;
        var shardIndices = _bulkLoadShardIndices;
        var flushed = 0;
        for (var i = 0; i < entries.Length; i++)
        {
            var shardIndex = shardIndices[i];
            var buffer = buffers[shardIndex];
            buffer.Add(entries[i]);
            if (buffer.Count < BulkLoadChunkSize)
                continue;

            // The flush path re-probed every parallel map: the shard grain, the
            // in-flight task to await, the chunk counter to read and to write
            // back, and the buffer to swap.
            _ = shards[shardIndex];
            _ = inFlight[shardIndex];
            var counter = chunkCounters[shardIndex];
            chunkCounters[shardIndex] = counter + 1;
            inFlight[shardIndex] = Task.CompletedTask;
            buffers[shardIndex] = new List<KeyValuePair<string, byte[]>>(BulkLoadChunkSize);
            flushed++;
        }

        return flushed;
    }

    /// <summary>
    /// Replacement shape, calling the real production primitive: one
    /// owner-indexed slot per shard holding all four pieces of state, so the
    /// per-entry probe is an array read and the per-flush six disappear.
    /// </summary>
    [Benchmark(Description = "(2) bulk load: one dense slot per shard [production]")]
    public int BulkLoad_DenseSlots()
    {
        var slots = new ShardSlots<BulkLoadSlot>(_physicalShards);
        foreach (var shardIndex in _physicalShards)
        {
            slots.Set(shardIndex, new BulkLoadSlot(
                new List<KeyValuePair<string, byte[]>>(BulkLoadChunkSize), ShardSentinel));
        }

        var entries = _bulkLoadEntries;
        var shardIndices = _bulkLoadShardIndices;
        var flushed = 0;
        for (var i = 0; i < entries.Length; i++)
        {
            var slot = slots.Get(shardIndices[i])!;
            slot.Buffer.Add(entries[i]);
            if (slot.Buffer.Count < BulkLoadChunkSize)
                continue;

            _ = slot.Shard;
            _ = slot.InFlight;
            slot.ChunkCounter++;
            slot.InFlight = Task.CompletedTask;
            slot.Buffer = new List<KeyValuePair<string, byte[]>>(BulkLoadChunkSize);
            flushed++;
        }

        return flushed;
    }

    // =====================================================================
    // (3) The streaming restore accumulators.
    // =====================================================================

    /// <summary>
    /// Prior shape: a double-probe into an unsized <c>Dictionary&lt;int, ...&gt;</c>
    /// per record, and a per-shard batch grown from empty even though it is
    /// flushed at a known size - so every batch abandons its whole bucket and
    /// entry array at each doubling on the way there.
    /// </summary>
    [Benchmark(Description = "(3) restore stream: hashed shard map, unsized batches")]
    public int Restore_HashedUnsizedBatches()
    {
        var perShard = new Dictionary<int, Dictionary<string, int>>();
        var flushed = 0;

        foreach (var key in _restoreKeys)
        {
            var shardIndex = _shardMap.Resolve(key);
            if (!perShard.TryGetValue(shardIndex, out var batch))
            {
                batch = new Dictionary<string, int>();
                perShard[shardIndex] = batch;
            }

            batch[key] = key.Length;
            if (batch.Count < RestoreBatchSize)
                continue;

            perShard[shardIndex] = new Dictionary<string, int>();
            flushed++;
        }

        return flushed + perShard.Count;
    }

    /// <summary>
    /// Replacement shape, calling the real production primitive: owner-indexed
    /// slots and a batch presized to its flush bound.
    /// </summary>
    [Benchmark(Description = "(3) restore stream: dense slots, presized batches [production]")]
    public int Restore_DenseSlotsPresizedBatches()
    {
        var perShard = new ShardSlots<Dictionary<string, int>>(_physicalShards);
        var flushed = 0;

        foreach (var key in _restoreKeys)
        {
            var shardIndex = _shardMap.Resolve(key);
            var batch = perShard.Get(shardIndex);
            if (batch is null)
            {
                batch = new Dictionary<string, int>(RestoreBatchSize);
                perShard.Set(shardIndex, batch);
            }

            batch[key] = key.Length;
            if (batch.Count < RestoreBatchSize)
                continue;

            perShard.Set(shardIndex, new Dictionary<string, int>(RestoreBatchSize));
            flushed++;
        }

        return flushed + perShard.Count;
    }

    /// <summary>
    /// Stands in for the resolved shard grain reference the real slot holds; the
    /// benchmark only needs the reference read, not a live grain.
    /// </summary>
    private static readonly object ShardSentinel = new();

    private sealed class BulkLoadSlot(List<KeyValuePair<string, byte[]>> buffer, object shard)
    {
        public List<KeyValuePair<string, byte[]>> Buffer { get; set; } = buffer;

        public Task InFlight { get; set; } = Task.CompletedTask;

        public int ChunkCounter { get; set; }

        public object Shard { get; } = shard;
    }
}
