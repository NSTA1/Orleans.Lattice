using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three serial grain-call chains this suite was added for. Each
/// one awaited a call per item before issuing the next, so a batch of N items
/// cost N round-trip latencies to do work with no cross-item ordering
/// constraint.
/// <para>
/// (1) <c>ViewMaintainerGrain.ApplySurvivorsAsync</c> applies the survivors of a
/// drain batch to the view tree. The caller's fold has already coalesced the
/// batch by view key, so every survivor carries a <b>distinct</b> key, and this
/// path is only taken when the batch contains no range delete. No two writes can
/// therefore touch the same view row, which is what makes the order-insensitive
/// fan-out sound. Its sibling <c>ApplyInSourceOrderAsync</c> - the range path -
/// deliberately stays a serial HLC-ordered walk and is not modelled here.
/// </para>
/// <para>
/// (2) <c>BPlusInternalGrain.InitializeWithChildrenAsync</c> seeds the digest
/// chain by telling each child this node is its parent and pulling the child's
/// snapshot back: two calls per child, all of them targeting a distinct child
/// grain. Unlike (1) the consumer is <b>not</b> order-insensitive - each
/// snapshot is folded into this node's shared aggregates and persisted - so only
/// the child-facing reads overlap and the folds still run strictly in input
/// order. That is exactly the guarantee <c>ReadAheadAsync</c> provides, so the
/// persisted aggregate is byte-for-byte the one the serial loop produced.
/// </para>
/// <para>
/// (3) <c>CrossClusterSagaParticipantGrain</c> delivered the 2PC commit decision
/// (and, on the abort legs, the compensations) to each local participant one at
/// a time. Once prepare has succeeded the decision is fixed before the loop
/// begins, so no participant's outcome can change whether another's call is
/// issued. The compensation leg additionally routes through a
/// <c>SafeAbortAsync</c> that swallows per-participant faults, so the "one
/// failure must not strand the others" property is a property of the body, not
/// of the serialisation.
/// </para>
/// <para>
/// <b>The fanned-out lanes call the shipped code.</b>
/// <c>BoundedFanOut.ForEachAsync</c> and <c>BoundedFanOut.ReadAheadAsync</c> are
/// the production helpers, reached through <c>InternalsVisibleTo</c>, so what is
/// measured is the change as it ships rather than a mimic of it. The serial
/// baselines have to be hand-written, because the shipped methods are the
/// optimised ones; each reproduces its partner's shell exactly - same store,
/// same item list, same accumulation and return type - so the only difference
/// between an arm and its baseline is the dispatch shape.
/// </para>
/// <para>
/// <b>Read the yielding-store lanes, not the completed-task ones.</b> A store
/// that returns an already-completed task prices a round trip at approximately
/// zero, so against it an overlapping change can only look flat or worse: it
/// pays for the task list without being credited the serialisation it removed.
/// The completed-task pairs are kept precisely to show that, and to bound the
/// cost of the change in the degenerate case where there is nothing to overlap.
/// A single yield per call is still orders of magnitude cheaper than a real
/// grain hop, which makes the yielding lanes a <b>lower bound</b> on the saving
/// rather than an estimate of it.
/// </para>
/// <para>
/// The <c>[ThreadingDiagnoser]</c> "Completed Work Items" column is the honest
/// measure here: it counts the calls each shape issues and reproduces
/// bit-identically across rounds, where the timing column on a shared host does
/// not.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=fanoutcollapse</c> (or
/// <c>--suite fanoutcollapse</c>); see <c>Program.cs</c>. There is no Orleans
/// silo dependency, so it is cheap to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class FanOutCollapseBenchmarks
{
    /// <summary>The production window, adopted from the router grain's <c>maxLocalWorkers</c>.</summary>
    private const int FanOutWidth = 32;

    private ViewWriteStub[] _survivors = null!;
    private ChildStub[] _children = null!;
    private ParticipantStub[] _participants = null!;
    private CompletedTaskFanOutStore _store = null!;
    private YieldingFanOutStore _asyncStore = null!;

    /// <summary>
    /// Batch size for all three lanes: survivors in a drain batch, children of a
    /// freshly initialised internal node, and participants in a saga. Neither
    /// value is an exact multiple of the window, so the ragged trailing wave is
    /// always paid for rather than flattered away, and both match the sizes the
    /// sibling fan-out suites use so results are directly comparable.
    /// <para>
    /// Honest caveat on lane (3): a real saga's local participant set is small
    /// (single digits), so the wall-clock figure at these sizes overstates what
    /// that lane saves in production. Its evidence is the work-item census and
    /// the removed serialisation shape, not the microsecond column. Lanes (1)
    /// and (2) do run at these sizes and larger - a drain batch and an internal
    /// node's fan-out are both corpus-sized.
    /// </para>
    /// </summary>
    [Params(70, 500)]
    public int ItemCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _store = new CompletedTaskFanOutStore();

        var survivors = new ViewWriteStub[ItemCount];
        var children = new ChildStub[ItemCount];
        var participants = new ParticipantStub[ItemCount];
        for (var i = 0; i < ItemCount; i++)
        {
            var key = "row-" + i.ToString("D6", CultureInfo.InvariantCulture);
            survivors[i] = new ViewWriteStub(key, [(byte)(i & 0xFF)]);
            children[i] = new ChildStub(i, IsLeaf: (i & 1) == 0);
            participants[i] = new ParticipantStub(i);
            _store.Seed(key, [(byte)(i & 0xFF)]);
        }

        _survivors = survivors;
        _children = children;
        _participants = participants;
        _asyncStore = new YieldingFanOutStore(_store);
    }

    // -- (1) view-drain survivor apply: order-insensitive, distinct view keys --

    [Benchmark(Baseline = true, Description = "survivors: N serial point writes (yielding store)")]
    public async Task<int> Survivors_Serial_Async()
    {
        var applied = 0;
        for (var i = 0; i < _survivors.Length; i++)
        {
            await _asyncStore.ApplyAsync(_survivors[i], CancellationToken.None);
            applied++;
        }

        return applied;
    }

    [Benchmark(Description = "survivors: BoundedFanOut.ForEachAsync, width 32 (yielding store)")]
    public async Task<int> Survivors_BoundedFanOut_Async()
    {
        await BoundedFanOut.ForEachAsync(
            _survivors,
            FanOutWidth,
            write => _asyncStore.ApplyAsync(write, CancellationToken.None));

        return _survivors.Length;
    }

    /// <summary>
    /// Contrast arm for the rejected alternative: drop the cap and put every
    /// write in flight at once. It cannot overlap further than the router
    /// grain's 32 local workers allow, so past that width it buys no additional
    /// concurrency - it only holds N tasks alive instead of 32, turning a sweep
    /// whose working set is a constant into one that scales with the batch.
    /// </summary>
    [Benchmark(Description = "survivors: contrast, unbounded fan-out (yielding store)")]
    public async Task<int> Survivors_UnboundedFanOut_Async()
    {
        var all = new Task[_survivors.Length];
        for (var i = 0; i < _survivors.Length; i++)
        {
            all[i] = _asyncStore.ApplyAsync(_survivors[i], CancellationToken.None);
        }

        await Task.WhenAll(all);
        return _survivors.Length;
    }

    [Benchmark(Description = "survivors: N serial point writes (completed-task store)")]
    public async Task<int> Survivors_Serial()
    {
        var applied = 0;
        for (var i = 0; i < _survivors.Length; i++)
        {
            await _store.ApplyAsync(_survivors[i], CancellationToken.None);
            applied++;
        }

        return applied;
    }

    [Benchmark(Description = "survivors: BoundedFanOut.ForEachAsync, width 32 (completed-task store)")]
    public async Task<int> Survivors_BoundedFanOut()
    {
        await BoundedFanOut.ForEachAsync(
            _survivors,
            FanOutWidth,
            write => _store.ApplyAsync(write, CancellationToken.None));

        return _survivors.Length;
    }

    // -- (2) internal-node child seeding: overlapped reads, strictly ordered fold --

    [Benchmark(Description = "child seed: 2N serial calls then fold (yielding store)")]
    public async Task<long> ChildSeed_Serial_Async()
    {
        long folded = 0;
        for (var i = 0; i < _children.Length; i++)
        {
            var snapshot = await _asyncStore.SeedChildAsync(_children[i]);
            folded = FoldChildSnapshot(folded, snapshot);
        }

        return folded;
    }

    [Benchmark(Description = "child seed: BoundedFanOut.ReadAheadAsync, width 32, serial fold (yielding store)")]
    public async Task<long> ChildSeed_ReadAhead_Async()
    {
        long folded = 0;
        await foreach (var snapshot in BoundedFanOut.ReadAheadAsync(
            _children,
            FanOutWidth,
            child => _asyncStore.SeedChildAsync(child)))
        {
            folded = FoldChildSnapshot(folded, snapshot);
        }

        return folded;
    }

    /// <summary>
    /// Contrast arm for the alternative that was rejected on <b>correctness</b>:
    /// overlap the fold as well, so snapshots land in completion order. The
    /// production fold mutates this node's shared child-digest map and subtree
    /// aggregates and persists them, so completion-order folding races the
    /// aggregate and makes the persisted digest depend on scheduling. It is
    /// shipped so the cost of keeping the fold ordered is measured rather than
    /// assumed - and the measurement is the interesting part, because the
    /// unsafe lane is not merely unusable, it is <b>slower</b> than the ordered
    /// read-ahead it would replace and allocates well over twice as much. So
    /// ordering the fold is not a concession bought with throughput; the
    /// interleaved-continuation shape simply costs more than draining a ring
    /// window in order does.
    /// </summary>
    [Benchmark(Description = "child seed: contrast, fold in completion order - UNSAFE (yielding store)")]
    public async Task<long> ChildSeed_UnorderedFold_Async()
    {
        long folded = 0;
        await BoundedFanOut.ForEachAsync(
            _children,
            FanOutWidth,
            async child =>
            {
                var snapshot = await _asyncStore.SeedChildAsync(child);
                Interlocked.Add(ref folded, snapshot.EntryCount + snapshot.Hash);
            });

        return folded;
    }

    [Benchmark(Description = "child seed: 2N serial calls then fold (completed-task store)")]
    public async Task<long> ChildSeed_Serial()
    {
        long folded = 0;
        for (var i = 0; i < _children.Length; i++)
        {
            var snapshot = await _store.SeedChildAsync(_children[i]);
            folded = FoldChildSnapshot(folded, snapshot);
        }

        return folded;
    }

    [Benchmark(Description = "child seed: BoundedFanOut.ReadAheadAsync, width 32 (completed-task store)")]
    public async Task<long> ChildSeed_ReadAhead()
    {
        long folded = 0;
        await foreach (var snapshot in BoundedFanOut.ReadAheadAsync(
            _children,
            FanOutWidth,
            child => _store.SeedChildAsync(child)))
        {
            folded = FoldChildSnapshot(folded, snapshot);
        }

        return folded;
    }

    // -- (3) saga participant commit / compensate delivery --

    [Benchmark(Description = "saga: N serial commit + N serial compensate (yielding store)")]
    public async Task<int> Saga_Serial_Async()
    {
        var delivered = 0;
        for (var i = 0; i < _participants.Length; i++)
        {
            await _asyncStore.CommitParticipantAsync(_participants[i]);
            delivered++;
        }

        for (var i = 0; i < _participants.Length; i++)
        {
            await _asyncStore.SafeAbortParticipantAsync(_participants[i]);
            delivered++;
        }

        return delivered;
    }

    [Benchmark(Description = "saga: BoundedFanOut.ForEachAsync commit + compensate, width 32 (yielding store)")]
    public async Task<int> Saga_BoundedFanOut_Async()
    {
        await BoundedFanOut.ForEachAsync(
            _participants,
            FanOutWidth,
            participant => _asyncStore.CommitParticipantAsync(participant));

        await BoundedFanOut.ForEachAsync(
            _participants,
            FanOutWidth,
            participant => _asyncStore.SafeAbortParticipantAsync(participant));

        return _participants.Length * 2;
    }

    [Benchmark(Description = "saga: N serial commit + N serial compensate (completed-task store)")]
    public async Task<int> Saga_Serial()
    {
        var delivered = 0;
        for (var i = 0; i < _participants.Length; i++)
        {
            await _store.CommitParticipantAsync(_participants[i]);
            delivered++;
        }

        for (var i = 0; i < _participants.Length; i++)
        {
            await _store.SafeAbortParticipantAsync(_participants[i]);
            delivered++;
        }

        return delivered;
    }

    [Benchmark(Description = "saga: BoundedFanOut.ForEachAsync commit + compensate, width 32 (completed-task store)")]
    public async Task<int> Saga_BoundedFanOut()
    {
        await BoundedFanOut.ForEachAsync(
            _participants,
            FanOutWidth,
            participant => _store.CommitParticipantAsync(participant));

        await BoundedFanOut.ForEachAsync(
            _participants,
            FanOutWidth,
            participant => _store.SafeAbortParticipantAsync(participant));

        return _participants.Length * 2;
    }

    /// <summary>
    /// Stand-in for the production fold. Order-dependent by construction (the
    /// running accumulator is mixed into each step) so a lane that reorders the
    /// folds produces a different answer - which is the point the unsafe
    /// contrast arm is making.
    /// </summary>
    private static long FoldChildSnapshot(long running, ChildSnapshotStub snapshot) =>
        (running * 31) + snapshot.EntryCount + snapshot.Hash;

    /// <summary>A coalesced drain survivor: a distinct view key and its payload.</summary>
    private readonly record struct ViewWriteStub(string Key, byte[] Value);

    /// <summary>A child of the internal node being initialised.</summary>
    private readonly record struct ChildStub(int Index, bool IsLeaf);

    /// <summary>The digest a child returns once its parent slot has been seeded.</summary>
    private readonly record struct ChildSnapshotStub(long EntryCount, long Hash);

    /// <summary>A local saga participant.</summary>
    private readonly record struct ParticipantStub(int Index);

    /// <summary>
    /// The narrowest stand-in for the three production seams, over a plain
    /// dictionary. Every method returns a completed task, so a lane running
    /// against it charges the async machinery and the task list and nothing
    /// else - which is exactly why an overlapping change reads as flat or worse
    /// here. What the change actually removes is the serialisation of real round
    /// trips.
    /// </summary>
    private sealed class CompletedTaskFanOutStore
    {
        private readonly Dictionary<string, byte[]> _map = new(StringComparer.Ordinal);

        public void Seed(string key, byte[] value) => _map[key] = value;

        public Task ApplyAsync(ViewWriteStub write, CancellationToken cancellationToken)
        {
            // Deliberately non-destructive: the lanes are re-run thousands of
            // times against one fixture, and a mutating apply would make every
            // iteration after the first walk a different store. The lookup is
            // kept so the body does the same dictionary work a real apply's
            // routing would.
            _ = _map.ContainsKey(write.Key);
            return Task.CompletedTask;
        }

        public Task<ChildSnapshotStub> SeedChildAsync(ChildStub child) =>
            // Models both production calls (SetParentAsync then
            // GetChildDigestSnapshotAsync) as the one unit the fan-out issues.
            Task.FromResult(new ChildSnapshotStub(child.Index, child.IsLeaf ? child.Index * 2 : child.Index * 3));

        public Task CommitParticipantAsync(ParticipantStub participant) => Task.CompletedTask;

        public Task SafeAbortParticipantAsync(ParticipantStub participant) => Task.CompletedTask;
    }

    /// <summary>
    /// The same seams with genuinely asynchronous completions. An Orleans grain
    /// call never completes synchronously, so this is the shape the production
    /// seam actually has. One yield per call is still far cheaper than a real
    /// hop, so these lanes floor the saving rather than estimate it - and they
    /// make the <c>[ThreadingDiagnoser]</c> work-item count a direct census of
    /// the calls each shape issues.
    /// </summary>
    private sealed class YieldingFanOutStore(CompletedTaskFanOutStore inner)
    {
        public async Task ApplyAsync(ViewWriteStub write, CancellationToken cancellationToken)
        {
            await Task.Yield();
            await inner.ApplyAsync(write, cancellationToken);
        }

        public async Task<ChildSnapshotStub> SeedChildAsync(ChildStub child)
        {
            // Two yields, because the production unit is two grain calls:
            // SetParentAsync followed by GetChildDigestSnapshotAsync.
            await Task.Yield();
            await Task.Yield();
            return await inner.SeedChildAsync(child);
        }

        public async Task CommitParticipantAsync(ParticipantStub participant)
        {
            await Task.Yield();
            await inner.CommitParticipantAsync(participant);
        }

        public async Task SafeAbortParticipantAsync(ParticipantStub participant)
        {
            await Task.Yield();
            await inner.SafeAbortParticipantAsync(participant);
        }
    }
}
