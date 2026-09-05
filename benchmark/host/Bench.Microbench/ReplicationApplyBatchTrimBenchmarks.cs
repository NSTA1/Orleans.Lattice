using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three reductions made to the replication <i>batch apply</i>
/// path, so the per-operation time and byte deltas are measurable in the clear.
/// <para>
/// Each pair runs the prior shape against its replacement with no silo, no
/// transport and no storage in the loop, so the delta is precisely the work the
/// production change removes. The end-to-end replication benchmarks route every
/// batch through Orleans serialization, gRPC framing and persistence, which
/// buries a per-entry struct copy or a per-run rehash below their run-to-run
/// noise floor.
/// </para>
/// <para>
/// The three edits under test are all in
/// <c>src/lattice.replication</c> and sit on private members
/// (<c>ReplicationApplier.BuildParallelApplyPlanOrNull</c>,
/// <c>ReplicationApplier.ApplyOriginRunAsync</c> and
/// <c>LeafReReplayer.ApplyCaps</c>), so neither lane can call the production
/// method directly. Both lanes therefore reproduce the <b>same</b> surrounding
/// shell - identical inputs, identical interface-typed access, identical item
/// construction and identical emit logic - and differ only in the body under
/// test. That symmetry is the point: a baseline that skips part of the
/// optimized arm's shell fabricates a regression (see the
/// <c>ReceiverAppliedContentIndex</c> note in
/// <see cref="ReplicationShipApplyTrimBenchmarks"/>).
/// </para>
/// <para>
/// The pairs mirror the production edits:
/// (1) the multi-tree run-segmentation scan in
/// <c>BuildParallelApplyPlanOrNull</c>. <see cref="WalRecord"/> is a wide
/// <c>readonly record struct</c>, so every read through the
/// <see cref="IReadOnlyList{T}"/> indexer copies the whole record onto the
/// stack; the prior shape indexed the candidate <b>three</b> times per entry
/// (tree, origin, mode) and then paid a second full string hash to store the
/// run into its per-tree bucket. The replacement binds each entry once and
/// folds the probe-then-store pair onto a single
/// <see cref="CollectionsMarshal.GetValueRefOrAddDefault{TKey, TValue}"/> slot;
/// (2) the four pending accumulators in <c>ApplyOriginRunAsync</c>, which
/// defer a run's batchable entries into one grain dispatch. They grew from
/// empty, so a full run walked the 4/8/16/.../1024 doubling chain and abandoned
/// every intermediate backing array. The replacement sizes them from the run
/// length, which is an exact upper bound, clamped so a mostly-deduped run
/// cannot over-allocate;
/// (3) the atomic-batch grouping in <c>LeafReReplayer.ApplyCaps</c>, which runs
/// once per re-replay repair selection. It probed twice per batch member, grew
/// each member list from empty despite the batch declaring its own width in
/// <see cref="WalRecord.AtomicBatchSize"/>, and grew the emitted-transaction
/// set from empty. The replacement folds the probe and sizes both exactly.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=replicationapplytrims</c> (or
/// <c>--suite replicationapplytrims</c>); see <c>Program.cs</c>. The suite has
/// no Orleans silo dependency, so it is fast to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c> for tight confidence intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class ReplicationApplyBatchTrimBenchmarks
{
    // ---- (1) a multi-tree inbound batch: the only shape that reaches the
    //      parallel-plan builder at all, since a single-tree batch bails to the
    //      allocation-free sequential walk. Held behind the interface so both
    //      lanes pay the same indexer dispatch the production code pays ----
    private IReadOnlyList<WalRecord> _multiTreeBatch = null!;

    // ---- (2) one contiguous (tree, origin) run of batchable LWW Sets, the
    //      steady-state shape the pending buckets accumulate ----
    private WalRecord[] _run = null!;

    // ---- (3) a re-replay selection carrying atomic batches ----
    private List<WalRecord> _chosen = null!;

    private const int TreeCount = 8;
    private const int RunLength = 16;
    private const int AtomicBatchWidth = 8;

    /// <summary>Builds the inputs shared by the benchmark pairs.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var payload = new byte[128];
        for (var i = 0; i < payload.Length; i++)
        {
            payload[i] = (byte)i;
        }

        // (1) 8 trees x 4 rounds x 16-entry runs = 512 entries in 32 runs. A
        // cross-tree batch arrives interleaved (the drain is per-WAL-partition,
        // not per-tree), so each tree owns several non-adjacent runs - exactly
        // the case the per-tree grouping exists to gather.
        var batch = new List<WalRecord>(TreeCount * 4 * RunLength);
        for (var round = 0; round < 4; round++)
        {
            for (var tree = 0; tree < TreeCount; tree++)
            {
                for (var k = 0; k < RunLength; k++)
                {
                    batch.Add(new WalRecord
                    {
                        TreeId = "tree-" + tree.ToString("D2"),
                        Op = MutationKind.Set,
                        Key = "customer/" + k.ToString("D6"),
                        Value = payload,
                        Timestamp = new HybridLogicalClock
                        {
                            WallClockTicks = 638_000_000_000_000_000L + (round * 1000) + k,
                            Counter = 0,
                        },
                        OriginClusterId = "region-00",
                        Mode = LatticeMergeMode.LwwRegister,
                    });
                }
            }
        }
        _multiTreeBatch = batch;

        // (2) a 512-entry run: the transport ships per-(tree, peer), so a
        // steady-state inbound batch is one long same-origin run.
        const int runSize = 512;
        _run = new WalRecord[runSize];
        for (var i = 0; i < runSize; i++)
        {
            _run[i] = new WalRecord
            {
                TreeId = "orders",
                Op = MutationKind.Set,
                Key = "customer/" + i.ToString("D6"),
                Value = payload,
                Timestamp = new HybridLogicalClock
                {
                    WallClockTicks = 638_000_000_000_000_000L + i,
                    Counter = 0,
                },
                OriginClusterId = "region-00",
                Mode = LatticeMergeMode.LwwRegister,
            };
        }

        // (3) 256 entries: 32 atomic batches of 8 members, each member carrying
        // its batch width, interleaved with ordinary point writes.
        _chosen = new List<WalRecord>(256);
        var tx = 0;
        for (var i = 0; i < 256; i++)
        {
            var inAtomic = (i / AtomicBatchWidth) % 2 == 0;
            if (inAtomic && i % AtomicBatchWidth == 0)
            {
                tx++;
            }

            _chosen.Add(new WalRecord
            {
                TreeId = "orders",
                Op = MutationKind.Set,
                Key = "customer/" + i.ToString("D6"),
                Value = payload,
                Timestamp = new HybridLogicalClock
                {
                    WallClockTicks = 638_000_000_000_000_000L + i,
                    Counter = 0,
                },
                OriginClusterId = "region-00",
                AtomicBatchSize = inAtomic ? AtomicBatchWidth : 0,
                AtomicBatchIndex = inAtomic ? i % AtomicBatchWidth : 0,
                TransactionId = inAtomic ? GuidForTx(tx) : Guid.Empty,
            });
        }
    }

    private static Guid GuidForTx(int tx)
    {
        Span<byte> bytes = stackalloc byte[16];
        bytes.Clear();
        BitConverter.TryWriteBytes(bytes, tx);
        return new Guid(bytes);
    }

    // ========================================================================
    // (1) multi-tree run segmentation in BuildParallelApplyPlanOrNull
    // ========================================================================

    /// <summary>
    /// The prior shape: the inner run scan read the candidate record through
    /// the interface indexer three times (copying the whole wide struct each
    /// time), and the per-tree bucket was resolved with a
    /// <c>TryGetValue</c> miss followed by an indexer store - two full string
    /// hashes per run.
    /// </summary>
    [Benchmark]
    public int ParallelPlanRunScan_Baseline_TripleIndexAndDoubleProbe()
    {
        var entries = _multiTreeBatch;
        var groups = new Dictionary<string, List<(int Start, int End)>>(StringComparer.Ordinal);
        var order = new List<string>();
        var i = 0;
        while (i < entries.Count)
        {
            var startTreeId = entries[i].TreeId ?? string.Empty;
            var startOrigin = entries[i].OriginClusterId;
            var startMode = entries[i].Mode;
            var j = i + 1;
            while (j < entries.Count
                && string.Equals(entries[j].TreeId ?? string.Empty, startTreeId, StringComparison.Ordinal)
                && string.Equals(entries[j].OriginClusterId, startOrigin, StringComparison.Ordinal)
                && entries[j].Mode == startMode)
            {
                j++;
            }

            if (!groups.TryGetValue(startTreeId, out var list))
            {
                list = new List<(int Start, int End)>();
                groups[startTreeId] = list;
                order.Add(startTreeId);
            }
            list.Add((i, j));
            i = j;
        }

        return order.Count + groups.Count;
    }

    /// <summary>
    /// The shipped shape: each entry is bound once and its run-key fields are
    /// projected off that single copy, and the probe-then-store pair folds onto
    /// one <see cref="CollectionsMarshal.GetValueRefOrAddDefault{TKey, TValue}"/>
    /// slot reference.
    /// </summary>
    [Benchmark]
    public int ParallelPlanRunScan_Optimized_HoistedReadAndProbeFold()
    {
        var entries = _multiTreeBatch;
        var groups = new Dictionary<string, List<(int Start, int End)>>(StringComparer.Ordinal);
        var order = new List<string>();
        var i = 0;
        while (i < entries.Count)
        {
            var start = entries[i];
            var startTreeId = start.TreeId ?? string.Empty;
            var startOrigin = start.OriginClusterId;
            var startMode = start.Mode;
            var j = i + 1;
            while (j < entries.Count)
            {
                var candidate = entries[j];
                if (!string.Equals(candidate.TreeId ?? string.Empty, startTreeId, StringComparison.Ordinal)
                    || !string.Equals(candidate.OriginClusterId, startOrigin, StringComparison.Ordinal)
                    || candidate.Mode != startMode)
                {
                    break;
                }
                j++;
            }

            ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(groups, startTreeId, out var existed);
            if (!existed)
            {
                slot = new List<(int Start, int End)>();
                order.Add(startTreeId);
            }
            slot!.Add((i, j));
            i = j;
        }

        return order.Count + groups.Count;
    }

    // ========================================================================
    // (2) pending accumulators in ApplyOriginRunAsync
    // ========================================================================

    /// <summary>
    /// The prior shape: both pending buckets were lazily constructed with no
    /// capacity, so a full run walked the whole doubling chain and abandoned
    /// each intermediate backing array.
    /// </summary>
    [Benchmark]
    public int PendingAccumulators_Baseline_GrowFromEmpty()
    {
        List<ApplyMergeItem>? pendingItems = null;
        List<(int EntryIndex, long StartTs)>? pendingApplies = null;

        for (var k = 0; k < _run.Length; k++)
        {
            var entry = _run[k];
            pendingItems ??= new List<ApplyMergeItem>();
            pendingApplies ??= new List<(int, long)>();
            pendingItems.Add(BuildMergeItem(in entry));
            pendingApplies.Add((k, k));
        }

        return (pendingItems?.Count ?? 0) + (pendingApplies?.Count ?? 0);
    }

    /// <summary>
    /// The shipped shape: the buckets are sized from the run length (an exact
    /// upper bound), clamped so a mostly-deduped run cannot over-allocate. They
    /// remain lazily constructed, so a run that defers nothing still allocates
    /// nothing.
    /// </summary>
    [Benchmark]
    public int PendingAccumulators_Optimized_Presized()
    {
        List<ApplyMergeItem>? pendingItems = null;
        List<(int EntryIndex, long StartTs)>? pendingApplies = null;
        var hint = Math.Clamp(_run.Length, 4, 256);

        for (var k = 0; k < _run.Length; k++)
        {
            var entry = _run[k];
            pendingItems ??= new List<ApplyMergeItem>(hint);
            pendingApplies ??= new List<(int, long)>(hint);
            pendingItems.Add(BuildMergeItem(in entry));
            pendingApplies.Add((k, k));
        }

        return (pendingItems?.Count ?? 0) + (pendingApplies?.Count ?? 0);
    }

    // Shared by both lanes so the item-construction cost is identical and only
    // the accumulator growth differs.
    private static ApplyMergeItem BuildMergeItem(in WalRecord entry) => new()
    {
        Key = entry.Key,
        Value = entry.Op == MutationKind.Set ? entry.Value : null,
        SourceHlc = entry.Timestamp,
        OriginClusterId = entry.OriginClusterId!,
        SourceVectorClock = null,
        ExpiresAtTicks = entry.Op == MutationKind.Set ? entry.ExpiresAtTicks : 0,
        IsTombstone = entry.Op == MutationKind.Delete,
    };

    // ========================================================================
    // (3) atomic-batch grouping in LeafReReplayer.ApplyCaps
    // ========================================================================

    /// <summary>
    /// The prior shape: two hashes per batch member, each member list grown
    /// from empty despite the batch declaring its own width, and the
    /// emitted-transaction set grown from empty.
    /// </summary>
    [Benchmark]
    public int ApplyCapsGrouping_Baseline_DoubleProbeGrowFromEmpty()
    {
        var chosen = _chosen;
        Dictionary<Guid, List<WalRecord>>? batches = null;
        foreach (var e in chosen)
        {
            if (e.AtomicBatchSize > 0 && e.TransactionId != Guid.Empty)
            {
                batches ??= [];
                if (!batches.TryGetValue(e.TransactionId, out var list))
                {
                    list = [];
                    batches[e.TransactionId] = list;
                }
                list.Add(e);
            }
        }

        HashSet<Guid>? emittedTx = batches is null ? null : [];
        return EmitCapped(chosen, batches, emittedTx);
    }

    /// <summary>
    /// The shipped shape: one hash per member, each member list sized from
    /// <see cref="WalRecord.AtomicBatchSize"/> (its exact final size), and the
    /// emitted-transaction set sized from the distinct-transaction count.
    /// </summary>
    [Benchmark]
    public int ApplyCapsGrouping_Optimized_FoldAndPresize()
    {
        var chosen = _chosen;
        Dictionary<Guid, List<WalRecord>>? batches = null;
        foreach (var e in chosen)
        {
            if (e.AtomicBatchSize > 0 && e.TransactionId != Guid.Empty)
            {
                batches ??= [];
                ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(
                    batches, e.TransactionId, out var existed);
                if (!existed)
                {
                    slot = new List<WalRecord>(e.AtomicBatchSize);
                }
                slot!.Add(e);
            }
        }

        HashSet<Guid>? emittedTx = batches is null ? null : new HashSet<Guid>(batches.Count);
        return EmitCapped(chosen, batches, emittedTx);
    }

    // The cap-and-emit tail is identical in production between the two shapes,
    // so both lanes run this shared copy: the measured delta is the grouping
    // body alone, not the emit walk.
    private static int EmitCapped(
        List<WalRecord> chosen,
        Dictionary<Guid, List<WalRecord>>? batches,
        HashSet<Guid>? emittedTx)
    {
        const int maxEntries = int.MaxValue;
        const long maxBytes = long.MaxValue;

        var result = new List<WalRecord>(chosen.Count);
        long bytes = 0;
        foreach (var e in chosen)
        {
            var isAtomic = e.AtomicBatchSize > 0 && e.TransactionId != Guid.Empty;
            int unitCount;
            long unitBytes;
            List<WalRecord>? batch = null;
            if (isAtomic)
            {
                if (!emittedTx!.Add(e.TransactionId))
                {
                    continue;
                }
                batch = batches![e.TransactionId];
                unitCount = batch.Count;
                unitBytes = 0;
                foreach (var u in batch)
                {
                    unitBytes += EstimateBytes(u);
                }
            }
            else
            {
                unitCount = 1;
                unitBytes = EstimateBytes(e);
            }

            if (result.Count > 0 && (result.Count + unitCount > maxEntries || bytes + unitBytes > maxBytes))
            {
                break;
            }

            if (batch is not null)
            {
                result.AddRange(batch);
            }
            else
            {
                result.Add(e);
            }
            bytes += unitBytes;
        }

        return result.Count;
    }

    private static long EstimateBytes(in WalRecord r)
        => (r.Value?.Length ?? 0) + (r.Delta?.Length ?? 0) + (r.Key?.Length ?? 0) + 64;
}
