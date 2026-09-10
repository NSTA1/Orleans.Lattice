using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Companion to <see cref="RoundTripWaveBenchmarks"/> for the three remaining
/// serial per-target waves that suite's sweep sighted but did not take. Each one
/// awaited a call per target before issuing the next, so a walk over N mutually
/// independent targets cost N round-trip latencies to do work carrying no
/// cross-target ordering constraint.
/// <para>
/// (1) <b>Per-partition source-head HLC scan.</b>
/// <c>ViewMaintainerGrain.CaptureSourceHeadHlcAsync</c> walks every WAL partition
/// of the source tree and, for each, reads that partition's head offset and then
/// cursors the single entry below it to recover its timestamp. That is a
/// <b>two-call chain per partition</b>, which makes it the most latency-dense of
/// the three. Both calls are pure reads against a distinct partition, so no
/// partition's probe can observe another's effect, and the reduction is max over
/// the per-partition head clocks - commutative and associative - so the answer
/// cannot depend on arrival order.
/// </para>
/// <para>
/// (2) <b>Producer-designation probe.</b>
/// <c>ViewMaintainerGrain.IsSourceLocallyReadableAsync</c> answers "has this
/// cluster ever written the view's source tree" by probing partition head offsets
/// until one reads greater than zero. The reduction is a logical OR, so it is
/// order-independent, but unlike the other two lanes the serial form could
/// <b>short-circuit</b>. The shipped shape therefore keeps the first probe on its
/// own direct await and collapses only the remainder, so a producer whose
/// partition 0 has been written still answers in exactly one call while a miss
/// costs two round-trip latencies instead of N. This lane ships two fixtures and a
/// contrast arm in each: the <i>miss</i>, where a false answer - the
/// consumer-cluster designation the probe exists to make - always walked every
/// partition, and the <i>first-partition hit</i>, where the shipped arm must read
/// as identical to the serial baseline. The contrast is the rejected unconditional
/// fan-out, which wins the miss by one latency and loses the hit by every probe it
/// issues to answer a question one call had already settled.
/// </para>
/// <para>
/// (3) <b>Orphan-shadow purge.</b>
/// <c>LatticeBackupRestoreService.GarbageCollectOrphanShadowAsync</c> purges every
/// physical shard of an abandoned restore shadow. Each call destroys one shard's
/// own data through that shard's own root grain, and the shadow is unreachable by
/// the time this runs, so there is no cross-shard ordering constraint and no
/// observer of a partially-purged state. A shard count is a deployment-scale
/// number, so this is the corpus-sized shape and uses
/// <c>BoundedFanOut.ForEachAsync</c>, whose live call set stays at the width
/// however wide the tree is.
/// </para>
/// <para>
/// <b>The fanned-out lanes call the shipped code.</b> <c>BoundedFanOut.RunAsync</c>
/// and <c>BoundedFanOut.ForEachAsync</c> are the production helpers, reached
/// through <c>InternalsVisibleTo</c>, so what is measured is the change as it
/// ships rather than a mimic of it. The serial baselines have to be hand-written,
/// because the shipped methods are the optimised ones; each reproduces its
/// partner's shell exactly - same store, same fold, same return type - so the only
/// difference between an arm and its baseline is the dispatch shape.
/// </para>
/// <para>
/// <b>Read the yielding-store lanes, not the completed-task ones.</b> A store that
/// returns an already-completed task prices a round trip at approximately zero, so
/// against it an overlapping change can only look flat or worse: it pays for the
/// task array without being credited the serialisation it removed. The
/// completed-task pairs are kept precisely to bound the cost of the change in that
/// degenerate case. A single yield per call is still orders of magnitude cheaper
/// than a real grain hop, which makes the yielding lanes a <b>lower bound</b> on
/// the saving rather than an estimate of it.
/// </para>
/// <para>
/// The <c>[ThreadingDiagnoser]</c> "Completed Work Items" column is the honest
/// measure here: it counts the calls each shape issues and reproduces
/// bit-identically across rounds, where the timing column on a shared host does
/// not. A census that stays flat while the time falls is the proof that what was
/// removed is serialisation and not work - and on lane (2) it is also what makes
/// the extra probes the fanned-out form issues visible rather than hidden.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=partitionwaves</c> (or
/// <c>--suite partitionwaves</c>); see <c>Program.cs</c>. There is no Orleans silo
/// dependency, so it is cheap to run at <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class PartitionWaveBenchmarks
{
    /// <summary>The production window, adopted from the router grain's <c>maxLocalWorkers</c>.</summary>
    private const int FanOutWidth = 32;

    private int[] _shardIndices = null!;
    private CompletedTaskPartitionStore _store = null!;
    private YieldingPartitionStore _asyncStore = null!;

    /// <summary>
    /// Partition / shard count, deliberately straddling the width bound because
    /// <c>BoundedFanOut.RunAsync</c> is two different algorithms either side of it.
    /// <para>
    /// At <b>16</b> the count is under the width of 32, so <c>RunAsync</c> skips the
    /// semaphore entirely and degenerates to a plain <c>Task.WhenAll</c> over the
    /// slots. This is the regime lanes (1) and (2) actually occupy: a tree's WAL
    /// partition count is single to low double digits, so it is the honest figure
    /// for them.
    /// </para>
    /// <para>
    /// At <b>500</b> the count is well past the bound. Lane (3) is the one that
    /// genuinely runs at this size, because a physical shard count is a
    /// deployment-scale number, and it uses <c>ForEachAsync</c>, which walks a wave
    /// at a time and holds its live set at O(width). Lanes (1) and (2) are reported
    /// at 500 as well, not because production reaches it, but because it is where
    /// the two helpers diverge and it would be dishonest to omit the size at which
    /// the arm chosen here stops paying.
    /// </para>
    /// <para>
    /// Neither value is a multiple of the width, so the ragged trailing wave is
    /// always paid for rather than flattered away.
    /// </para>
    /// </summary>
    [Params(16, 500)]
    public int ItemCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _store = new CompletedTaskPartitionStore();

        var shards = new int[ItemCount];
        for (var i = 0; i < ItemCount; i++)
        {
            shards[i] = i;

            // A written partition: a non-zero head offset and a tail entry whose
            // physical-clock component rises with the partition index, so the max
            // fold has a distinct winner and cannot be satisfied by the first
            // arrival.
            _store.SeedWrittenPartition(i, head: (i * 1000L) + 1, tailTimestamp: 1_700_000_000_000L + i);
        }

        _shardIndices = shards;
        _asyncStore = new YieldingPartitionStore(_store);
    }

    // -- (1) source-head HLC scan: two-call chain per distinct partition, max fold --

    [Benchmark(Baseline = true, Description = "head HLC: N serial (offset, tail) chains then max (yielding store)")]
    public async Task<long> HeadHlc_Serial_Async()
    {
        var head = 0L;
        for (var partition = 0; partition < ItemCount; partition++)
        {
            var headOffset = await _asyncStore.GetHeadOffsetAsync(partition, CancellationToken.None);
            if (headOffset <= 0)
            {
                continue;
            }

            await foreach (var timestamp in _asyncStore.ReadTailAsync(partition, headOffset - 2, CancellationToken.None))
            {
                if (timestamp > head)
                {
                    head = timestamp;
                }
            }
        }

        return head;
    }

    [Benchmark(Description = "head HLC: BoundedFanOut.RunAsync, width 32, then max (yielding store)")]
    public async Task<long> HeadHlc_BoundedFanOut_Async()
    {
        var partitionHeads = await BoundedFanOut.RunAsync(
            ItemCount,
            FanOutWidth,
            PartitionHeadAsync,
            CancellationToken.None);

        var head = 0L;
        for (var partition = 0; partition < partitionHeads.Length; partition++)
        {
            if (partitionHeads[partition] > head)
            {
                head = partitionHeads[partition];
            }
        }

        return head;

        async Task<long> PartitionHeadAsync(int partition)
        {
            var headOffset = await _asyncStore.GetHeadOffsetAsync(partition, CancellationToken.None);
            if (headOffset <= 0)
            {
                return 0L;
            }

            var partitionHead = 0L;
            await foreach (var timestamp in _asyncStore.ReadTailAsync(partition, headOffset - 2, CancellationToken.None))
            {
                if (timestamp > partitionHead)
                {
                    partitionHead = timestamp;
                }
            }

            return partitionHead;
        }
    }

    [Benchmark(Description = "head HLC: N serial (offset, tail) chains then max (completed-task store)")]
    public async Task<long> HeadHlc_Serial()
    {
        var head = 0L;
        for (var partition = 0; partition < ItemCount; partition++)
        {
            var headOffset = await _store.GetHeadOffsetAsync(partition, CancellationToken.None);
            if (headOffset <= 0)
            {
                continue;
            }

            await foreach (var timestamp in _store.ReadTailAsync(partition, headOffset - 2, CancellationToken.None))
            {
                if (timestamp > head)
                {
                    head = timestamp;
                }
            }
        }

        return head;
    }

    [Benchmark(Description = "head HLC: BoundedFanOut.RunAsync, width 32, then max (completed-task store)")]
    public async Task<long> HeadHlc_BoundedFanOut()
    {
        var partitionHeads = await BoundedFanOut.RunAsync(
            ItemCount,
            FanOutWidth,
            PartitionHeadAsync,
            CancellationToken.None);

        var head = 0L;
        for (var partition = 0; partition < partitionHeads.Length; partition++)
        {
            if (partitionHeads[partition] > head)
            {
                head = partitionHeads[partition];
            }
        }

        return head;

        async Task<long> PartitionHeadAsync(int partition)
        {
            var headOffset = await _store.GetHeadOffsetAsync(partition, CancellationToken.None);
            if (headOffset <= 0)
            {
                return 0L;
            }

            var partitionHead = 0L;
            await foreach (var timestamp in _store.ReadTailAsync(partition, headOffset - 2, CancellationToken.None))
            {
                if (timestamp > partitionHead)
                {
                    partitionHead = timestamp;
                }
            }

            return partitionHead;
        }
    }

    // -- (2) producer-designation probe: OR fold, and the short-circuit it gives up --

    /// <summary>
    /// The miss: no partition has ever been written, which is the consumer-cluster
    /// designation this probe exists to make. The serial form cannot short-circuit
    /// here, so it pays a full round trip per partition every single time.
    /// </summary>
    [Benchmark(Description = "readable probe (miss): N serial head probes, no short-circuit (yielding store)")]
    public async Task<bool> ReadableProbeMiss_Serial_Async()
    {
        for (var partition = 0; partition < ItemCount; partition++)
        {
            if (await _asyncStore.GetEmptyHeadOffsetAsync(partition, CancellationToken.None) > 0)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// The shipped shape: probe 0 stays on its own direct await so the
    /// short-circuit survives, and only the remainder is collapsed. On a miss that
    /// is two round-trip latencies instead of N.
    /// </summary>
    [Benchmark(Description = "readable probe (miss): probe 0 then BoundedFanOut.RunAsync on the rest (yielding store)")]
    public async Task<bool> ReadableProbeMiss_ShortCircuitThenFanOut_Async()
    {
        if (await _asyncStore.GetEmptyHeadOffsetAsync(0, CancellationToken.None) > 0)
        {
            return true;
        }

        var heads = await BoundedFanOut.RunAsync(
            ItemCount - 1,
            FanOutWidth,
            slot => _asyncStore.GetEmptyHeadOffsetAsync(slot + 1, CancellationToken.None),
            CancellationToken.None);

        for (var slot = 0; slot < heads.Length; slot++)
        {
            if (heads[slot] > 0)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Contrast arm for the rejected alternative: drop the leading probe and fan
    /// every partition out unconditionally. On the miss it is the fastest arm - it
    /// pays one latency rather than two - and this suite reports that rather than
    /// hiding it. What it buys that latency with is visible in the
    /// <see cref="ReadableProbeHit_UnconditionalFanOut_Async"/> pair below, where
    /// the same shape issues every probe in a case the shipped arm answers in one
    /// call. Shipped so the cost of keeping the short-circuit is measured on both
    /// sides rather than asserted on one.
    /// </summary>
    [Benchmark(Description = "readable probe (miss): contrast, unconditional fan-out over all partitions (yielding store)")]
    public async Task<bool> ReadableProbeMiss_UnconditionalFanOut_Async()
    {
        var heads = await BoundedFanOut.RunAsync(
            ItemCount,
            FanOutWidth,
            partition => _asyncStore.GetEmptyHeadOffsetAsync(partition, CancellationToken.None),
            CancellationToken.None);

        for (var partition = 0; partition < heads.Length; partition++)
        {
            if (heads[partition] > 0)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// The cost side of the same change, shipped rather than argued: partition 0 is
    /// written, so a walk that keeps its leading probe answers in exactly one call.
    /// The shipped arm and the serial baseline are therefore expected to read as
    /// <b>identical</b> here, which is what establishes that the collapse is free in
    /// the common producer case.
    /// </summary>
    [Benchmark(Description = "readable probe (first-partition hit): N serial head probes, short-circuits after 1 (yielding store)")]
    public async Task<bool> ReadableProbeHit_Serial_Async()
    {
        for (var partition = 0; partition < ItemCount; partition++)
        {
            if (await _asyncStore.GetHeadOffsetAsync(partition, CancellationToken.None) > 0)
            {
                return true;
            }
        }

        return false;
    }

    [Benchmark(Description = "readable probe (first-partition hit): probe 0 then BoundedFanOut.RunAsync on the rest (yielding store)")]
    public async Task<bool> ReadableProbeHit_ShortCircuitThenFanOut_Async()
    {
        if (await _asyncStore.GetHeadOffsetAsync(0, CancellationToken.None) > 0)
        {
            return true;
        }

        var heads = await BoundedFanOut.RunAsync(
            ItemCount - 1,
            FanOutWidth,
            slot => _asyncStore.GetHeadOffsetAsync(slot + 1, CancellationToken.None),
            CancellationToken.None);

        for (var slot = 0; slot < heads.Length; slot++)
        {
            if (heads[slot] > 0)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// The rejected alternative measured where it loses: it issues a probe per
    /// partition to answer a question one call already settled. This is the arm the
    /// shipped design refuses, and the <c>[ThreadingDiagnoser]</c> census makes the
    /// extra calls explicit rather than leaving them as an argument about load.
    /// </summary>
    [Benchmark(Description = "readable probe (first-partition hit): contrast, unconditional fan-out over all partitions (yielding store)")]
    public async Task<bool> ReadableProbeHit_UnconditionalFanOut_Async()
    {
        var heads = await BoundedFanOut.RunAsync(
            ItemCount,
            FanOutWidth,
            partition => _asyncStore.GetHeadOffsetAsync(partition, CancellationToken.None),
            CancellationToken.None);

        for (var partition = 0; partition < heads.Length; partition++)
        {
            if (heads[partition] > 0)
            {
                return true;
            }
        }

        return false;
    }

    // -- (3) orphan-shadow purge: one destructive call per distinct shard root --

    [Benchmark(Description = "shadow purge: N serial per-shard purges (yielding store)")]
    public async Task<int> ShadowPurge_Serial_Async()
    {
        var purged = 0;
        for (var i = 0; i < _shardIndices.Length; i++)
        {
            await _asyncStore.PurgeShardAsync(_shardIndices[i]);
            purged++;
        }

        return purged;
    }

    [Benchmark(Description = "shadow purge: BoundedFanOut.ForEachAsync, width 32 (yielding store)")]
    public async Task<int> ShadowPurge_BoundedFanOut_Async()
    {
        await BoundedFanOut.ForEachAsync(
            _shardIndices,
            FanOutWidth,
            shardIndex => _asyncStore.PurgeShardAsync(shardIndex));

        return _shardIndices.Length;
    }

    [Benchmark(Description = "shadow purge: N serial per-shard purges (completed-task store)")]
    public async Task<int> ShadowPurge_Serial()
    {
        var purged = 0;
        for (var i = 0; i < _shardIndices.Length; i++)
        {
            await _store.PurgeShardAsync(_shardIndices[i]);
            purged++;
        }

        return purged;
    }

    [Benchmark(Description = "shadow purge: BoundedFanOut.ForEachAsync, width 32 (completed-task store)")]
    public async Task<int> ShadowPurge_BoundedFanOut()
    {
        await BoundedFanOut.ForEachAsync(
            _shardIndices,
            FanOutWidth,
            shardIndex => _store.PurgeShardAsync(shardIndex));

        return _shardIndices.Length;
    }

    /// <summary>
    /// The narrowest stand-in for the three production seams. Every method returns
    /// a completed task, so a lane running against it charges the async machinery
    /// and the task array and nothing else - which is exactly why an overlapping
    /// change reads as flat or worse here.
    /// </summary>
    private sealed class CompletedTaskPartitionStore
    {
        private readonly Dictionary<int, long> _heads = [];
        private readonly Dictionary<int, long> _tails = [];

        public void SeedWrittenPartition(int partition, long head, long tailTimestamp)
        {
            _heads[partition] = head;
            _tails[partition] = tailTimestamp;
        }

        public Task<long> GetHeadOffsetAsync(int partition, CancellationToken cancellationToken) =>
            Task.FromResult(_heads.TryGetValue(partition, out var head) ? head : 0L);

        /// <summary>
        /// The never-written frontier of a consumer cluster. The lookup is kept so
        /// the body does the same dictionary work the seeded probe does, leaving the
        /// dispatch shape as the only difference between the two fixtures.
        /// </summary>
        public Task<long> GetEmptyHeadOffsetAsync(int partition, CancellationToken cancellationToken)
        {
            _ = _heads.ContainsKey(partition);
            return Task.FromResult(0L);
        }

        /// <summary>
        /// The cursored tail read: one entry, matching the single record the
        /// production scan recovers by starting two below the head offset.
        /// </summary>
        public async IAsyncEnumerable<long> ReadTailAsync(
            int partition,
            long fromOffset,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            if (_tails.TryGetValue(partition, out var timestamp))
            {
                yield return timestamp;
            }
        }

        public Task PurgeShardAsync(int shardIndex)
        {
            // Deliberately non-destructive: the lanes are re-run thousands of times
            // against one fixture. The lookup is kept so the body does the same
            // dictionary work a real purge's routing would.
            _ = _heads.ContainsKey(shardIndex);
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// The same seams with genuinely asynchronous completions. An Orleans grain call
    /// never completes synchronously, so this is the shape the production seam
    /// actually has. One yield per call is still far cheaper than a real hop, so
    /// these lanes floor the saving rather than estimate it - and they make the
    /// <c>[ThreadingDiagnoser]</c> work-item count a direct census of the calls each
    /// shape issues.
    /// </summary>
    private sealed class YieldingPartitionStore(CompletedTaskPartitionStore inner)
    {
        public async Task<long> GetHeadOffsetAsync(int partition, CancellationToken cancellationToken)
        {
            await Task.Yield();
            return await inner.GetHeadOffsetAsync(partition, cancellationToken);
        }

        public async Task<long> GetEmptyHeadOffsetAsync(int partition, CancellationToken cancellationToken)
        {
            await Task.Yield();
            return await inner.GetEmptyHeadOffsetAsync(partition, cancellationToken);
        }

        public async IAsyncEnumerable<long> ReadTailAsync(
            int partition,
            long fromOffset,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            // The second hop of the per-partition chain. Yielding here is what makes
            // this lane cost two serialised round trips per partition in the
            // baseline, which is the shape the production scan has.
            await Task.Yield();
            await foreach (var timestamp in inner.ReadTailAsync(partition, fromOffset, cancellationToken))
            {
                yield return timestamp;
            }
        }

        public async Task PurgeShardAsync(int shardIndex)
        {
            await Task.Yield();
            await inner.PurgeShardAsync(shardIndex);
        }
    }
}
