using System;
using System.Collections.Generic;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates three remaining physical-shard partitioning paths so the
/// per-operation time and byte deltas are measurable in the clear.
/// <para>
/// Each pair runs the shape the production code used before the change against
/// the shape it uses after, and every "after" lane calls the <b>real production
/// method</b> rather than a copy of it. The end-to-end cluster benchmarks route
/// each of these through Orleans serialization, persistence and task machinery,
/// which buries a per-slot hash probe below their run-to-run noise floor; with
/// no cluster in the loop the delta here is precisely the work the change
/// removes.
/// </para>
/// <para>
/// The common observation is that all three partition by a <em>physical</em>
/// shard index - a dense, non-negative domain of typically 1-16 values - while
/// the space being partitioned is sized to the <em>virtual</em> shard count
/// (4096 by default) or to the batch. Hashing that tiny domain once (or twice)
/// per item is pure overhead against an owner-indexed array.
/// </para>
/// <para>
/// The pairs mirror the production edits:
/// (1) <c>LatticeGrain.GroupSlotsByOwner</c>, which the strongly-consistent
/// scan reconciliation runs whenever the shard map moves mid-scan. The prior
/// form hashed each requested slot's owner into a
/// <c>Dictionary&lt;int, List&lt;int&gt;&gt;</c> (two probes per slot on a miss),
/// grew each bucket from empty, and then had every caller copy the bucket into
/// a fresh array and sort it. The replacement counts into an owner-indexed
/// <c>int[]</c>, allocates each bucket at its exact size, and sorts in place -
/// so the growth chain and the copy both disappear;
/// (2) the snapshot cursor's per-shard ownership filter
/// (<c>LatticeCursorGrain.BuildOwnedSlotsByShard</c>), rebuilt once per cursor
/// activation over the whole pinned slot array. It was an open-coded duplicate
/// of <c>LatticeGrain.BuildOwnedSlotMap</c>, which already answers exactly this
/// question with a counting pass; the replacement simply calls it;
/// (3) the bulk-append chunk fan-out
/// (<c>LatticeGrain.BulkAppendChunkAsync</c>), run once per chunk for the whole
/// duration of a bulk load. It hashed every entry into an unsized dictionary of
/// unsized lists; the replacement reuses <see cref="ShardFanout.BucketEntries"/>,
/// which indexes buckets by owner directly and gives each a shard-fair capacity.
/// </para>
/// <para>
/// Lanes (1) and (2) are allocation wins as well as CPU wins, so
/// <c>Allocated</c> is the primary column for them - it is deterministic and
/// reproduces exactly, where <c>Mean</c> carries the host's timing noise. Run
/// via <c>BENCH_MICROBENCH_SUITE=slotgroupfolds</c> (or
/// <c>--suite slotgroupfolds</c>); see <c>Program.cs</c>. The suite has no
/// Orleans silo dependency, so it is cheap to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class SlotGroupingAndBulkFanoutBenchmarks
{
    // ---- (1) the reconciliation shape: a scan that must re-drain a quarter of
    //      a 4096-slot map spread over 8 physical shards ----
    private const int VirtualShardCount = 4096;
    private const int PhysicalShardCount = 8;
    private const int NeedSlotCount = 1024;

    private ShardMap _shardMap = null!;
    private HashSet<int> _needSlots = null!;

    // ---- (2) the snapshot cursor shape: the whole pinned slot array ----
    private int[] _pinnedSlots = null!;

    // ---- (3) the bulk-append chunk shape: the default 1000-entry chunk ----
    private const int ChunkEntryCount = 1000;

    private KeyValuePair<string, byte[]>[] _chunkEntries = null!;
    private IReadOnlyList<int> _physicalShards = null!;
    private int _fairBucketCapacity;

    [GlobalSetup]
    public void Setup()
    {
        _shardMap = ShardMap.CreateDefault(VirtualShardCount, PhysicalShardCount);
        _pinnedSlots = _shardMap.Slots;
        _physicalShards = _shardMap.GetPhysicalShardIndices();

        // A deterministic, deliberately unordered subset: HashSet enumeration
        // order is what forces the per-bucket sort in both shapes.
        var rng = new Random(20260905);
        _needSlots = new HashSet<int>(NeedSlotCount);
        while (_needSlots.Count < NeedSlotCount)
            _needSlots.Add(rng.Next(VirtualShardCount));

        _chunkEntries = new KeyValuePair<string, byte[]>[ChunkEntryCount];
        for (var i = 0; i < ChunkEntryCount; i++)
            _chunkEntries[i] = new KeyValuePair<string, byte[]>($"tenant-a/orders/{i:D6}", new byte[16]);

        _fairBucketCapacity = ShardFanout.BucketCapacity(ChunkEntryCount, _physicalShards.Count);
    }

    // =====================================================================
    // (1) Grouping the slots a scan must re-drain, by owning physical shard.
    // =====================================================================

    /// <summary>
    /// Prior shape: hash each slot's owner into a dictionary of lists grown
    /// from empty, then copy every bucket into a fresh array and sort it -
    /// which is what the two call sites did via <c>ToSortedArray</c>.
    /// </summary>
    [Benchmark(Baseline = true, Description = "(1) slot grouping: hashed lists, then copy-and-sort")]
    public int GroupSlots_HashedThenSort()
    {
        var mapSlots = _shardMap.Slots;
        var byOwner = new Dictionary<int, List<int>>();
        foreach (var s in _needSlots)
        {
            if ((uint)s >= (uint)mapSlots.Length) continue;
            var owner = mapSlots[s];
            if (!byOwner.TryGetValue(owner, out var list))
            {
                list = [];
                byOwner[owner] = list;
            }

            list.Add(s);
        }

        var total = 0;
        foreach (var (_, list) in byOwner)
        {
            var sorted = list.ToArray();
            Array.Sort(sorted);
            total += sorted.Length;
        }

        return total;
    }

    /// <summary>
    /// Replacement shape, calling the real production helper: an owner-indexed
    /// counting pass, exact-size buckets, and the sort fused in.
    /// </summary>
    [Benchmark(Description = "(1) slot grouping: dense counts, exact buckets, fused sort [production]")]
    public int GroupSlots_DenseFused()
    {
        var byOwner = LatticeGrain.GroupSlotsByOwner(_needSlots, _shardMap);

        var total = 0;
        foreach (var (_, sorted) in byOwner)
            total += sorted.Length;

        return total;
    }

    // =====================================================================
    // (2) The snapshot cursor's per-shard owned-slot partition.
    // =====================================================================

    /// <summary>
    /// Prior shape: the cursor's own open-coded partition - a forward scan
    /// hashing every virtual slot into a dictionary of lists grown from empty,
    /// then a second dictionary holding a copy of each list.
    /// </summary>
    [Benchmark(Description = "(2) cursor owned slots: hashed lists, then copy")]
    public int OwnedSlots_HashedLists()
    {
        var slots = _pinnedSlots;
        var lists = new Dictionary<int, List<int>>();
        for (var slot = 0; slot < slots.Length; slot++)
        {
            var shard = slots[slot];
            if (!lists.TryGetValue(shard, out var list))
            {
                list = new List<int>();
                lists[shard] = list;
            }

            list.Add(slot);
        }

        var result = new Dictionary<int, int[]>(lists.Count);
        foreach (var (shard, list) in lists)
            result[shard] = list.ToArray();

        var total = 0;
        foreach (var (_, owned) in result)
            total += owned.Length;

        return total;
    }

    /// <summary>
    /// Replacement shape, calling the real production helper the cursor now
    /// shares with the count-routing paths: a counting pass into owner-indexed
    /// exact-size arrays, emitted already ascending.
    /// </summary>
    [Benchmark(Description = "(2) cursor owned slots: shared dense partition [production]")]
    public int OwnedSlots_SharedDense()
    {
        var result = LatticeGrain.BuildOwnedSlotMap(_pinnedSlots);

        var total = 0;
        foreach (var (_, owned) in result)
            total += owned.Length;

        return total;
    }

    // =====================================================================
    // (3) The bulk-append chunk fan-out.
    // =====================================================================

    /// <summary>
    /// Prior shape: hash every chunk entry's owning shard into an unsized
    /// dictionary of unsized lists.
    /// </summary>
    [Benchmark(Description = "(3) bulk-append fan-out: hashed buckets, no presize")]
    public int BulkFanout_Hashed()
    {
        var shardBuckets = new Dictionary<int, List<KeyValuePair<string, byte[]>>>();
        foreach (var entry in _chunkEntries)
        {
            var idx = _shardMap.Resolve(entry.Key);
            if (!shardBuckets.TryGetValue(idx, out var bucket))
            {
                bucket = [];
                shardBuckets[idx] = bucket;
            }

            bucket.Add(entry);
        }

        var total = 0;
        foreach (var (_, bucket) in shardBuckets)
            total += bucket.Count;

        return total;
    }

    /// <summary>
    /// Replacement shape, calling the real production helper: an owner-indexed
    /// dense array with a shard-fair bucket presize and no hashing.
    /// </summary>
    [Benchmark(Description = "(3) bulk-append fan-out: shared dense buckets [production]")]
    public int BulkFanout_Dense()
    {
        var shardBuckets = ShardFanout.BucketEntries(
            _chunkEntries, _shardMap, _physicalShards, _fairBucketCapacity);

        var total = 0;
        foreach (var (_, bucket) in shardBuckets)
            total += bucket.Count;

        return total;
    }
}
