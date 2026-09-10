using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three serial grain-call chains this suite was added for. Each one
/// awaited a call per item before issuing the next, so a batch of N items cost N
/// round-trip latencies to do work carrying no cross-item ordering constraint.
/// <para>
/// (1) <b>Per-partition WAL head probes.</b>
/// <c>ViewMaintainerGrain.ComputeLagAsync</c> walks every WAL partition of the
/// source tree, reads that partition's head offset, and <b>sums</b> the per-partition
/// lag. Each probe targets a distinct partition and mutates nothing, so no probe
/// can observe another's effect, and the reduction is addition - commutative and
/// associative - so the answer cannot depend on the order the heads arrive in. The
/// same walk appears in the view maintainer's two shadow-swap resume-floor captures
/// and in <c>LatticeBackupCaptureService.CaptureWalHeadsAsync</c>, all of which now
/// share this shape. It is the hottest of the three: every view maintainer in the
/// cluster pays it on each drain tick and again on every convergence check.
/// </para>
/// <para>
/// (2) <b>Backup-restore per-shard drain.</b> A restore streams the whole backup
/// into a shard-indexed accumulator and then delivers one batch per physical shard.
/// Every bucket goes to a <b>different</b> shard root grain, so the buckets are
/// mutually independent; the per-shard ascending key order the raw bulk load
/// requires lives <i>inside</i> a bucket and is untouched by the order the buckets
/// are dispatched in. The serial drain was therefore paying a round trip per shard
/// to preserve an ordering that never existed between shards.
/// </para>
/// <para>
/// (3) <b>Group-atomic set cutover.</b> <c>RestoreParticipant.CommitSetAsync</c>
/// builds a shadow per member tree and then swaps every member's alias. The swaps
/// run <b>inside</b> a write fence spanning the whole group, so the length of that
/// loop is the window during which every member tree refuses writes. The group's
/// atomicity comes from the fence spanning all members, not from the order they
/// flip in, so overlapping the swaps shortens the write stall without weakening the
/// guarantee. This lane models the build wave and the fenced swap wave together.
/// </para>
/// <para>
/// <b>The fanned-out lanes call the shipped code.</b>
/// <c>BoundedFanOut.RunAsync</c> and <c>BoundedFanOut.ForEachAsync</c> are the
/// production helpers, reached through <c>InternalsVisibleTo</c>, so what is
/// measured is the change as it ships rather than a mimic of it. The serial
/// baselines have to be hand-written, because the shipped methods are the optimised
/// ones; each reproduces its partner's shell exactly - same store, same item list,
/// same accumulation and return type - so the only difference between an arm and
/// its baseline is the dispatch shape.
/// </para>
/// <para>
/// <b>Read the yielding-store lanes, not the completed-task ones.</b> A store that
/// returns an already-completed task prices a round trip at approximately zero, so
/// against it an overlapping change can only look flat or worse: it pays for the
/// task array without being credited the serialisation it removed. The
/// completed-task pairs are kept precisely to show that, and to bound the cost of
/// the change in the degenerate case where there is nothing to overlap. A single
/// yield per call is still orders of magnitude cheaper than a real grain hop, which
/// makes the yielding lanes a <b>lower bound</b> on the saving rather than an
/// estimate of it.
/// </para>
/// <para>
/// The <c>[ThreadingDiagnoser]</c> "Completed Work Items" column is the honest
/// measure here: it counts the calls each shape issues and reproduces
/// bit-identically across rounds, where the timing column on a shared host does not.
/// A census that stays flat while the time falls is the proof that what was removed
/// is serialisation and not work.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=roundtripwaves</c> (or
/// <c>--suite roundtripwaves</c>); see <c>Program.cs</c>. There is no Orleans silo
/// dependency, so it is cheap to run at <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class RoundTripWaveBenchmarks
{
    /// <summary>The production window, adopted from the router grain's <c>maxLocalWorkers</c>.</summary>
    private const int FanOutWidth = 32;

    private ShardBucketStub[] _buckets = null!;
    private SetMemberStub[] _members = null!;
    private CompletedTaskWaveStore _store = null!;
    private YieldingWaveStore _asyncStore = null!;

    /// <summary>
    /// Partition / shard / member count. The two values are deliberately chosen to
    /// straddle the width bound, because <c>BoundedFanOut.RunAsync</c> behaves as
    /// two different algorithms either side of it and the three lanes do not occupy
    /// the same regime in production.
    /// <para>
    /// At <b>16</b> the count is under the width of 32, so <c>RunAsync</c> skips
    /// the semaphore and its gating entirely and degenerates to a plain
    /// <c>Task.WhenAll</c> over the slots. This is the regime lanes (1) and (3)
    /// actually occupy: a tree's WAL partition count and a backup set's member
    /// count are single to low double digits, so it is the honest figure for them
    /// and not a flattering one.
    /// </para>
    /// <para>
    /// At <b>500</b> the count is well past the bound, so <c>RunAsync</c> allocates
    /// the semaphore and launches all N gated tasks up front, holding O(N) tasks
    /// alive rather than O(width). Lane (2) is the one that genuinely runs at this
    /// size, because a physical shard count is a deployment-scale number - and it
    /// uses <c>ForEachAsync</c>, which walks a wave at a time and so keeps its live
    /// set at O(width) throughout. Lanes (1) and (3) are reported at 500 as well,
    /// not because production reaches it, but because it is where the two helpers
    /// diverge: it is the measurement that shows <c>RunAsync</c> is the right
    /// choice only below the bound, and it would be dishonest to omit the size at
    /// which the arm chosen here stops paying.
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
        _store = new CompletedTaskWaveStore();

        var buckets = new ShardBucketStub[ItemCount];
        var members = new SetMemberStub[ItemCount];
        for (var i = 0; i < ItemCount; i++)
        {
            var treeId = "tree-" + i.ToString("D6", CultureInfo.InvariantCulture);
            buckets[i] = new ShardBucketStub(i, i * 7);
            members[i] = new SetMemberStub(treeId);
            _store.SeedPartition(i, i * 1000L);
        }

        _buckets = buckets;
        _members = members;
        _asyncStore = new YieldingWaveStore(_store);
    }

    // -- (1) per-partition WAL head probes: distinct partitions, commutative sum --

    [Benchmark(Baseline = true, Description = "head probes: N serial reads then sum (yielding store)")]
    public async Task<long> HeadProbes_Serial_Async()
    {
        long lag = 0;
        for (var partition = 0; partition < ItemCount; partition++)
        {
            var head = await _asyncStore.GetHeadOffsetAsync(partition, CancellationToken.None);
            lag += PartitionLag(head, partition);
        }

        return lag;
    }

    [Benchmark(Description = "head probes: BoundedFanOut.RunAsync, width 32, then sum (yielding store)")]
    public async Task<long> HeadProbes_BoundedFanOut_Async()
    {
        var heads = await BoundedFanOut.RunAsync(
            ItemCount,
            FanOutWidth,
            partition => _asyncStore.GetHeadOffsetAsync(partition, CancellationToken.None),
            CancellationToken.None);

        long lag = 0;
        for (var partition = 0; partition < heads.Length; partition++)
        {
            lag += PartitionLag(heads[partition], partition);
        }

        return lag;
    }

    /// <summary>
    /// Contrast arm for the standing rejected alternative: drop the width bound and
    /// put every probe in flight at once.
    /// <para>
    /// Below the bound this arm is not an alternative at all - it is what
    /// <c>RunAsync</c> already does, because that helper skips its semaphore
    /// outright when the slot count fits inside the width. The two should therefore
    /// read as near-identical at <c>ItemCount = 16</c>, and that agreement is the
    /// point: it is what establishes that the bounded helper costs nothing in the
    /// regime lanes (1) and (3) actually occupy.
    /// </para>
    /// <para>
    /// Past the bound the arm is genuinely faster, and this suite reports that
    /// rather than hiding it. What it buys the time with is a live task set that
    /// scales with the batch instead of staying at 32, which shows up here as Gen1
    /// collections the wave-walking lane does not incur. That is the trade the
    /// bound exists to refuse: a sweep whose working set is a constant is worth
    /// more to a silo hosting thousands of grains than one sweep's wall clock, and
    /// an unbounded wave also puts N calls on a router grain that admits 32. Shipped
    /// so the cost of keeping the bound is measured rather than asserted.
    /// </para>
    /// </summary>
    [Benchmark(Description = "head probes: contrast, unbounded fan-out (yielding store)")]
    public async Task<long> HeadProbes_UnboundedFanOut_Async()
    {
        var all = new Task<long>[ItemCount];
        for (var partition = 0; partition < ItemCount; partition++)
        {
            all[partition] = _asyncStore.GetHeadOffsetAsync(partition, CancellationToken.None);
        }

        var heads = await Task.WhenAll(all);
        long lag = 0;
        for (var partition = 0; partition < heads.Length; partition++)
        {
            lag += PartitionLag(heads[partition], partition);
        }

        return lag;
    }

    [Benchmark(Description = "head probes: N serial reads then sum (completed-task store)")]
    public async Task<long> HeadProbes_Serial()
    {
        long lag = 0;
        for (var partition = 0; partition < ItemCount; partition++)
        {
            var head = await _store.GetHeadOffsetAsync(partition, CancellationToken.None);
            lag += PartitionLag(head, partition);
        }

        return lag;
    }

    [Benchmark(Description = "head probes: BoundedFanOut.RunAsync, width 32, then sum (completed-task store)")]
    public async Task<long> HeadProbes_BoundedFanOut()
    {
        var heads = await BoundedFanOut.RunAsync(
            ItemCount,
            FanOutWidth,
            partition => _store.GetHeadOffsetAsync(partition, CancellationToken.None),
            CancellationToken.None);

        long lag = 0;
        for (var partition = 0; partition < heads.Length; partition++)
        {
            lag += PartitionLag(heads[partition], partition);
        }

        return lag;
    }

    // -- (2) backup-restore per-shard drain: one batch per distinct shard root --

    [Benchmark(Description = "shard drain: N serial per-shard loads (yielding store)")]
    public async Task<int> ShardDrain_Serial_Async()
    {
        var drained = 0;
        for (var i = 0; i < _buckets.Length; i++)
        {
            await _asyncStore.BulkLoadShardAsync(_buckets[i]);
            drained++;
        }

        return drained;
    }

    [Benchmark(Description = "shard drain: BoundedFanOut.ForEachAsync, width 32 (yielding store)")]
    public async Task<int> ShardDrain_BoundedFanOut_Async()
    {
        await BoundedFanOut.ForEachAsync(
            _buckets,
            FanOutWidth,
            bucket => _asyncStore.BulkLoadShardAsync(bucket));

        return _buckets.Length;
    }

    [Benchmark(Description = "shard drain: N serial per-shard loads (completed-task store)")]
    public async Task<int> ShardDrain_Serial()
    {
        var drained = 0;
        for (var i = 0; i < _buckets.Length; i++)
        {
            await _store.BulkLoadShardAsync(_buckets[i]);
            drained++;
        }

        return drained;
    }

    [Benchmark(Description = "shard drain: BoundedFanOut.ForEachAsync, width 32 (completed-task store)")]
    public async Task<int> ShardDrain_BoundedFanOut()
    {
        await BoundedFanOut.ForEachAsync(
            _buckets,
            FanOutWidth,
            bucket => _store.BulkLoadShardAsync(bucket));

        return _buckets.Length;
    }

    // -- (3) group-atomic set cutover: build wave, then the FENCED swap wave --

    [Benchmark(Description = "set cutover: N serial builds + N serial fenced swaps (yielding store)")]
    public async Task<int> SetCutover_Serial_Async()
    {
        var built = new List<ShadowStub>(_members.Length);
        for (var i = 0; i < _members.Length; i++)
        {
            built.Add(await _asyncStore.BuildShadowAsync(_members[i]));
        }

        // Everything past this point runs inside the group write fence.
        for (var i = 0; i < built.Count; i++)
        {
            await _asyncStore.CommitShadowAsync(built[i]);
        }

        return built.Count;
    }

    [Benchmark(Description = "set cutover: RunAsync builds + ForEachAsync fenced swaps, width 32 (yielding store)")]
    public async Task<int> SetCutover_BoundedFanOut_Async()
    {
        var built = await BoundedFanOut.RunAsync(
            _members.Length,
            FanOutWidth,
            slot => _asyncStore.BuildShadowAsync(_members[slot]),
            CancellationToken.None);

        await BoundedFanOut.ForEachAsync(
            built,
            FanOutWidth,
            shadow => _asyncStore.CommitShadowAsync(shadow));

        return built.Length;
    }

    [Benchmark(Description = "set cutover: N serial builds + N serial fenced swaps (completed-task store)")]
    public async Task<int> SetCutover_Serial()
    {
        var built = new List<ShadowStub>(_members.Length);
        for (var i = 0; i < _members.Length; i++)
        {
            built.Add(await _store.BuildShadowAsync(_members[i]));
        }

        for (var i = 0; i < built.Count; i++)
        {
            await _store.CommitShadowAsync(built[i]);
        }

        return built.Count;
    }

    [Benchmark(Description = "set cutover: RunAsync builds + ForEachAsync fenced swaps, width 32 (completed-task store)")]
    public async Task<int> SetCutover_BoundedFanOut()
    {
        var built = await BoundedFanOut.RunAsync(
            _members.Length,
            FanOutWidth,
            slot => _store.BuildShadowAsync(_members[slot]),
            CancellationToken.None);

        await BoundedFanOut.ForEachAsync(
            built,
            FanOutWidth,
            shadow => _store.CommitShadowAsync(shadow));

        return built.Length;
    }

    /// <summary>
    /// Stand-in for the production lag fold: the checkpoint is a fixed offset below
    /// the head, so every partition contributes and the sum is order-independent by
    /// construction - which is the property the fanned-out lane relies on.
    /// </summary>
    private static long PartitionLag(long head, int partition)
    {
        var checkpoint = partition - 1;
        var lag = head - (checkpoint + 1);
        return lag > 0 ? lag : 0;
    }

    /// <summary>One physical shard's accumulated restore batch.</summary>
    private readonly record struct ShardBucketStub(int ShardIndex, int EntryCount);

    /// <summary>One member tree of a backup set.</summary>
    private readonly record struct SetMemberStub(string TreeId);

    /// <summary>The shadow a member's build produced, ready for its alias swap.</summary>
    private readonly record struct ShadowStub(string TreeId, string ShadowPhysicalTreeId);

    /// <summary>
    /// The narrowest stand-in for the three production seams. Every method returns a
    /// completed task, so a lane running against it charges the async machinery and
    /// the task array and nothing else - which is exactly why an overlapping change
    /// reads as flat or worse here. What the change actually removes is the
    /// serialisation of real round trips.
    /// </summary>
    private sealed class CompletedTaskWaveStore
    {
        private readonly Dictionary<int, long> _heads = [];

        public void SeedPartition(int partition, long head) => _heads[partition] = head;

        public Task<long> GetHeadOffsetAsync(int partition, CancellationToken cancellationToken) =>
            Task.FromResult(_heads.TryGetValue(partition, out var head) ? head : 0L);

        public Task BulkLoadShardAsync(ShardBucketStub bucket)
        {
            // Deliberately non-destructive: the lanes are re-run thousands of times
            // against one fixture. The lookup is kept so the body does the same
            // dictionary work a real drain's routing would.
            _ = _heads.ContainsKey(bucket.ShardIndex);
            return Task.CompletedTask;
        }

        public Task<ShadowStub> BuildShadowAsync(SetMemberStub member) =>
            Task.FromResult(new ShadowStub(member.TreeId, member.TreeId + "#shadow"));

        public Task CommitShadowAsync(ShadowStub shadow) => Task.CompletedTask;
    }

    /// <summary>
    /// The same seams with genuinely asynchronous completions. An Orleans grain call
    /// never completes synchronously, so this is the shape the production seam
    /// actually has. One yield per call is still far cheaper than a real hop, so
    /// these lanes floor the saving rather than estimate it - and they make the
    /// <c>[ThreadingDiagnoser]</c> work-item count a direct census of the calls each
    /// shape issues.
    /// </summary>
    private sealed class YieldingWaveStore(CompletedTaskWaveStore inner)
    {
        public async Task<long> GetHeadOffsetAsync(int partition, CancellationToken cancellationToken)
        {
            await Task.Yield();
            return await inner.GetHeadOffsetAsync(partition, cancellationToken);
        }

        public async Task BulkLoadShardAsync(ShardBucketStub bucket)
        {
            await Task.Yield();
            await inner.BulkLoadShardAsync(bucket);
        }

        public async Task<ShadowStub> BuildShadowAsync(SetMemberStub member)
        {
            await Task.Yield();
            return await inner.BuildShadowAsync(member);
        }

        public async Task CommitShadowAsync(ShadowStub shadow)
        {
            await Task.Yield();
            await inner.CommitShadowAsync(shadow);
        }
    }
}
