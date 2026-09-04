using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three hash-probe reductions made to the reshard coordinator's
/// slot histogram, the tenancy record's CRDT merge, and the WAL GC's durable-pin
/// union, so the per-operation time and byte deltas are measurable in the clear.
/// <para>
/// These are CPU wins first, so the column to read is <c>Mean</c>. The
/// tenancy merge is byte-identical in allocation - collapsing two hash
/// probes into one removes work, not bytes - while lanes (1) and (3) also
/// drop an intermediate dictionary and a grow-and-rehash chain
/// respectively, so <c>Allocated</c> moves there too. The end-to-end cluster
/// benchmarks route every operation through Orleans serialization,
/// persistence and task machinery, which buries a per-slot or per-entry
/// probe fold below their run-to-run noise floor; each pair below runs the
/// prior shape against its replacement with no cluster in the loop, so the
/// delta is precisely the work the production change removes.
/// </para>
/// <para>
/// The pairs mirror the production edits:
/// (1) <see cref="TreeReshardGrain.CountSlotsPerPhysicalShard"/>, the
/// virtual-slot ownership histogram every <c>Migrating</c> tick of an online
/// reshard rebuilds. The prior form hashed a <c>Dictionary&lt;int, int&gt;</c>
/// twice per virtual slot (a read plus a write for the increment) across the
/// whole virtual space - 4096 slots by default - then hashed again once per
/// physical shard on the eligibility scan and a third time when projecting the
/// count into the eligible list. Physical shard indices are small, dense and
/// non-negative, so the replacement counts into an owner-indexed array and
/// hashes nothing. This lane calls the <b>real production code</b> on the
/// optimized side;
/// (2) <see cref="TenantRecord.MergeFrom"/>, the composite tenancy CRDT join
/// run on every replica-to-replica reconcile. Its four slot maps - admin
/// subjects, cross-tenant grants, allowed regions and region statuses - each
/// looped a <c>TryGetValue</c> followed by an indexer set on the same key, so
/// two hash probes per merged entry, collapsed to one via
/// <see cref="CollectionsMarshal.GetValueRefOrAddDefault{TKey, TValue}"/>. This
/// lane calls the <b>real production code</b> through the public API on the
/// optimized side;
/// (3) the WAL GC's durable-pin union min-fold
/// (<c>LatticeWalGc.ReadDurablePinsAsync</c> /
/// <c>ReadDurablePinOffsetsAsync</c>), which folds the per-shard pin dictionaries
/// read from every <c>IWalMaterialiserPinGrain</c> into one lowest-wins map on
/// each GC sweep. Same double-probe shape - and it probed twice on <em>both</em>
/// the hit and the miss branch - same single-probe replacement, plus a
/// capacity hint so the union no longer grows from empty. The methods are
/// private and async over grain calls, so this lane reproduces the two fold
/// shapes exactly rather than driving a silo.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=reshardfolds</c> (or
/// <c>--suite reshardfolds</c>); see <c>Program.cs</c>. The suite has no Orleans
/// silo dependency, so it is fast to run at <c>BENCH_MICROBENCH_FIDELITY=full</c>
/// for tight confidence intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class ReshardMergeAndPinFoldBenchmarks
{
    // ---- (1) the production reshard shape: 4096 virtual slots over 16
    //      physical shards, as ShardMap.CreateDefault emits ----
    private ShardMap _shardMap = null!;
    private IReadOnlyList<int> _physicalShards = null!;

    // ---- (2) two divergent tenancy records over a realistic estate, built so
    //      every one of the four slot maps overlaps and therefore takes the
    //      merge branch the fold exists to serve. Each lane owns its own target
    //      so neither perturbs the other; the join is idempotent, so repeating
    //      it against a converged target still performs the full per-entry
    //      probe-and-merge work the fold targets, without a per-operation
    //      Clone dominating the measurement. ----
    private TenantRecord _baselineMergeTarget = null!;
    private TenantRecord _optimizedMergeTarget = null!;
    private TenantRecord _mergeSource = null!;

    // ---- (3) the per-shard durable-pin dictionaries a GC sweep unions ----
    private IReadOnlyDictionary<string, HybridLogicalClock>[] _pinShards = null!;
    private IReadOnlyDictionary<string, long>[] _offsetShards = null!;

    /// <summary>Builds the inputs shared by the benchmark pairs.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _shardMap = ShardMap.CreateDefault(4096, 16);
        _physicalShards = _shardMap.GetPhysicalShardIndices();

        _baselineMergeTarget = BuildTenantRecord("w1", baseTicks: 10, offset: 0);
        _optimizedMergeTarget = BuildTenantRecord("w1", baseTicks: 10, offset: 0);
        _mergeSource = BuildTenantRecord("w2", baseTicks: 500, offset: 1);

        // The pin routing shards a tree's materialiser pins across a small
        // number of grains; each shard reports the consumers it saw, and the
        // union takes the lowest pin per consumer. Consumers overlap across
        // shards, so the fold's compare branch is the common case.
        const int pinShards = 8;
        const int consumers = 64;
        var pins = new IReadOnlyDictionary<string, HybridLogicalClock>[pinShards];
        var offsets = new IReadOnlyDictionary<string, long>[pinShards];
        for (var s = 0; s < pinShards; s++)
        {
            var pinMap = new Dictionary<string, HybridLogicalClock>(consumers, StringComparer.Ordinal);
            var offsetMap = new Dictionary<string, long>(consumers, StringComparer.Ordinal);
            for (var c = 0; c < consumers; c++)
            {
                var consumerId = "consumer-" + c.ToString("D3");
                // Descending across shards so each later shard lowers the
                // running minimum - the branch that writes, not the no-op.
                pinMap[consumerId] = new HybridLogicalClock
                {
                    WallClockTicks = ((pinShards - s) * 1000L) + c,
                    Counter = 0,
                };
                offsetMap[consumerId] = ((pinShards - s) * 1000L) + c;
            }
            pins[s] = pinMap;
            offsets[s] = offsetMap;
        }

        _pinShards = pins;
        _offsetShards = offsets;
    }

    private static TenantRecord BuildTenantRecord(string writer, long baseTicks, int offset)
    {
        const int subjects = 48;
        const int grants = 48;
        const int regions = 16;

        var record = TenantRecord.Create(
            TenantId.Parse("acme"),
            TenantStatus.Active,
            new TenantQuotas { MaxKeys = 1000 },
            TenantPlacement.Shared,
            Clock(baseTicks),
            writer);

        for (var i = 0; i < subjects; i++)
        {
            record.AddAdminSubject("admin-" + i.ToString("D3"), Clock(baseTicks + i), writer);
        }

        for (var i = 0; i < grants; i++)
        {
            record.AddGrant(
                CrossTenantGrant.Create(
                    "beta",
                    TenantGranteeKind.Tenant,
                    "tree-" + i.ToString("D3"),
                    TenantGrantOperations.Read),
                Clock(baseTicks + i),
                writer);
        }

        for (var i = 0; i < regions; i++)
        {
            var regionId = "region-" + i.ToString("D2");
            record.AuthorizeRegion(regionId, Clock(baseTicks + i), writer);
            // Alternate the status by side so the region-status map diverges
            // rather than merging two identical values.
            record.SetRegionStatus(
                regionId,
                ((i + offset) % 2) == 0 ? TenantRegionStatus.Online : TenantRegionStatus.Draining,
                Clock(baseTicks + i),
                writer);
        }

        return record;
    }

    private static HybridLogicalClock Clock(long ticks) =>
        new() { WallClockTicks = ticks, Counter = 0 };

    // ========================================================================
    // (1) reshard virtual-slot ownership histogram
    // ========================================================================

    /// <summary>
    /// The prior shape: a hashed <c>Dictionary&lt;int, int&gt;</c> histogram
    /// seeded per physical shard, incremented once per virtual slot (two probes
    /// each), then re-read by physical index on the eligibility scan and a third
    /// time when projecting the count into the eligible list.
    /// </summary>
    [Benchmark]
    public int ReshardSlotHistogram_Baseline_HashedDictionary()
    {
        var slotCounts = new Dictionary<int, int>(_physicalShards.Count);
        foreach (var idx in _physicalShards) slotCounts[idx] = 0;
        foreach (var slot in _shardMap.Slots) slotCounts[slot]++;

        var splittingIndices = new List<int>(_physicalShards.Count);
        foreach (var idx in _physicalShards)
        {
            if (slotCounts[idx] < 2) continue;
            splittingIndices.Add(idx);
        }

        var eligible = new List<(int Shard, int Slots)>(splittingIndices.Count);
        for (var i = 0; i < splittingIndices.Count; i++)
        {
            eligible.Add((splittingIndices[i], slotCounts[splittingIndices[i]]));
        }

        return eligible.Count;
    }

    /// <summary>
    /// The shipped shape, calling the real production histogram: an
    /// owner-indexed dense counter array, read back by ordinal so the
    /// eligibility scan hashes nothing either.
    /// </summary>
    [Benchmark]
    public int ReshardSlotHistogram_Optimized_DenseCounters()
    {
        var slotCounts = TreeReshardGrain.CountSlotsPerPhysicalShard(_physicalShards, _shardMap.Slots);

        var splittingIndices = new List<int>(_physicalShards.Count);
        var splittingSlotCounts = new List<int>(_physicalShards.Count);
        for (var i = 0; i < _physicalShards.Count; i++)
        {
            var owned = slotCounts[i];
            if (owned < 2) continue;
            splittingIndices.Add(_physicalShards[i]);
            splittingSlotCounts.Add(owned);
        }

        var eligible = new List<(int Shard, int Slots)>(splittingIndices.Count);
        for (var i = 0; i < splittingIndices.Count; i++)
        {
            eligible.Add((splittingIndices[i], splittingSlotCounts[i]));
        }

        return eligible.Count;
    }

    // ========================================================================
    // (2) tenancy record composite CRDT merge
    // ========================================================================

    /// <summary>
    /// The prior shape of the four slot-map joins: a <c>TryGetValue</c> followed
    /// by an indexer set on the same key, so two hash probes per merged entry.
    /// </summary>
    [Benchmark]
    public int TenantMergeFrom_Baseline_DoubleProbe()
    {
        var target = _baselineMergeTarget;
        var other = _mergeSource;

        foreach (var (subjectId, slot) in other.Subjects)
        {
            target.Subjects[subjectId] = target.Subjects.TryGetValue(subjectId, out var mine)
                ? TenantSubjectSlot.Merge(mine, slot)
                : slot;
        }

        foreach (var (grantId, slot) in other.GrantSlots)
        {
            target.GrantSlots[grantId] = target.GrantSlots.TryGetValue(grantId, out var mine)
                ? TenantGrantSlot.Merge(mine, slot)
                : slot;
        }

        foreach (var (regionId, slot) in other.AllowedRegions)
        {
            target.AllowedRegions[regionId] = target.AllowedRegions.TryGetValue(regionId, out var mine)
                ? TenantRegionAllowSlot.Merge(mine, slot)
                : slot;
        }

        foreach (var (regionId, slot) in other.RegionStatuses)
        {
            target.RegionStatuses[regionId] = target.RegionStatuses.TryGetValue(regionId, out var mine)
                ? TenantRegionStatusSlot.Merge(mine, slot)
                : slot;
        }

        return target.Subjects.Count + target.GrantSlots.Count
            + target.AllowedRegions.Count + target.RegionStatuses.Count;
    }

    /// <summary>
    /// The shipped shape, calling the real production join: each of the four
    /// slot-map folds collapsed to a single hash probe.
    /// </summary>
    [Benchmark]
    public int TenantMergeFrom_Optimized_SingleProbe()
    {
        var target = _optimizedMergeTarget;
        target.MergeFrom(_mergeSource);
        return target.Subjects.Count + target.GrantSlots.Count
            + target.AllowedRegions.Count + target.RegionStatuses.Count;
    }

    // ========================================================================
    // (3) WAL GC durable-pin union min-fold
    // ========================================================================

    /// <summary>
    /// The prior shape of the pin union: a <c>TryGetValue</c> and then an
    /// indexer set on the same key, on <em>both</em> the hit and the miss
    /// branch, so two hash probes for every consumer entry of every shard.
    /// </summary>
    [Benchmark]
    public int PinUnion_Baseline_DoubleProbe()
    {
        var pins = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);
        for (var i = 0; i < _pinShards.Length; i++)
        {
            foreach (var (consumerId, pin) in _pinShards[i])
            {
                if (pins.TryGetValue(consumerId, out var existing))
                {
                    if (pin < existing) pins[consumerId] = pin;
                }
                else
                {
                    pins[consumerId] = pin;
                }
            }
        }

        var offsets = new Dictionary<string, long>(StringComparer.Ordinal);
        for (var i = 0; i < _offsetShards.Length; i++)
        {
            foreach (var (consumerId, offset) in _offsetShards[i])
            {
                if (offsets.TryGetValue(consumerId, out var existing))
                {
                    if (offset < existing) offsets[consumerId] = offset;
                }
                else
                {
                    offsets[consumerId] = offset;
                }
            }
        }

        return pins.Count + offsets.Count;
    }

    /// <summary>
    /// The shipped shape: the min-fold collapsed to a single hash probe, with
    /// the add-if-missing case folded into the same compare.
    /// </summary>
    [Benchmark]
    public int PinUnion_Optimized_SingleProbe()
    {
        var pins = new Dictionary<string, HybridLogicalClock>(
            WidestCount(_pinShards), StringComparer.Ordinal);
        for (var i = 0; i < _pinShards.Length; i++)
        {
            foreach (var (consumerId, pin) in _pinShards[i])
            {
                ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(pins, consumerId, out var existed);
                if (!existed || pin < slot) slot = pin;
            }
        }

        var offsets = new Dictionary<string, long>(
            WidestCount(_offsetShards), StringComparer.Ordinal);
        for (var i = 0; i < _offsetShards.Length; i++)
        {
            foreach (var (consumerId, offset) in _offsetShards[i])
            {
                ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(offsets, consumerId, out var existed);
                if (!existed || offset < slot) slot = offset;
            }
        }

        return pins.Count + offsets.Count;
    }

    private static int WidestCount<TValue>(IReadOnlyDictionary<string, TValue>?[] results)
    {
        var widest = 0;
        for (var i = 0; i < results.Length; i++)
        {
            var count = results[i]?.Count ?? 0;
            if (count > widest) widest = count;
        }
        return widest;
    }
}
