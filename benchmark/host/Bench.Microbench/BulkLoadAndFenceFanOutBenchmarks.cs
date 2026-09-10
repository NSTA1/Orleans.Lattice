using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three structural round-trip reductions this suite was added
/// for. All three are the same defect in different clothes: a loop that awaited
/// one grain call before it issued the next, over a set of targets that were
/// already known and mutually independent.
/// <para>
/// (1) <c>ShardRootGrain.BulkLoadAsync</c> / <c>BulkLoadRawAsync</c> built the
/// leaf chain one leaf at a time, and each leaf cost five separately-gated,
/// separately-persisted calls: <c>SetTreeIdAsync</c>,
/// <c>SetShardIndexAsync</c>, <c>MergeEntriesAsync</c>, then a
/// <c>SetNextSiblingAsync</c> on the previous leaf and a
/// <c>SetPrevSiblingAsync</c> on this one. The first four collapse into the one
/// <c>InitializeSiblingAsync</c> batch the leaf-split donor already uses, and
/// once the chain is planned up front the leaves are independent, so the units
/// overlap. 5N-2 serial calls become 2N calls in ceil(N/32) waves.
/// </para>
/// <para>
/// (2) <c>ShardRootGrain.PurgeAsync</c>'s internal-node pre-walk asked every
/// node <c>AreChildrenLeavesAsync</c> and then <c>GetChildIdsAsync</c> - two
/// round trips for two fields of one state - in a strictly serial depth-first
/// stack. <c>GetRoutingTableAsync</c> returns both in one snapshot, and a
/// breadth-first walk can overlap a whole level's reads. 2I serial calls become
/// I calls in ceil(level/32) waves per level. This is the walk that feeds the
/// sweep PR #2472 already made concurrent, so the sweep was fast and the walk
/// that finds its input was not.
/// </para>
/// <para>
/// (3) <c>SagaWriteFenceGrain</c>'s group-atomic fan-outs walked trees x shards
/// (and trees x peers) one <c>await</c> at a time, so the wall-clock of engaging
/// a write fence grew linearly with the fenced topology while the fence itself
/// was already blocking writers. The calls are independent per target and group
/// atomicity is defined by every target having acked before the helper returns,
/// which a wave preserves exactly.
/// </para>
/// <para>
/// <b>The fanned-out lanes call the shipped code.</b>
/// <c>BoundedFanOut.ForEachAsync</c> and <c>BoundedFanOut.ReadAheadAsync</c> are
/// the production helpers, reached through <c>InternalsVisibleTo</c>, so what is
/// measured is the dispatch shape as it ships rather than a mimic of it. The
/// serial baselines have to be hand-written, because the shipped methods are the
/// optimised ones; each reproduces its partner's shell exactly - same store,
/// same target list, same accumulation and return type - so the only difference
/// is the dispatch shape.
/// </para>
/// <para>
/// <b>Every lane ships a contrast arm.</b> Changes (1) and (2) do two things at
/// once: they collapse the call count AND they overlap what is left. The
/// <c>_Contrast_CollapsedOnly</c> lanes apply just the collapse and keep the
/// serial await, which is what shows whether the overlap earns its place rather
/// than the collapse carrying the whole result. Change (3) is overlap-only, so
/// its contrast is the rejected alternative instead: an unbounded fan-out, which
/// cannot overlap further than the router grain's 32 local workers allow and
/// only trades a constant working set for one that scales with the topology.
/// </para>
/// <para>
/// <b>Read the yielding-store lanes, not the completed-task ones.</b> A store
/// that returns an already-completed task prices a round trip at approximately
/// zero, so against it an overlapping change can only look flat or worse: it
/// pays for the wave list without being credited the serialisation it removed.
/// The <c>CompletedTask</c> pair is kept precisely to show that, so the
/// yielding-lane result is not mistaken for an artefact of the harness. A single
/// yield per call is still orders of magnitude cheaper than a real grain hop,
/// which makes the yielding lanes a <b>lower bound</b> on the saving rather than
/// an estimate of it.
/// </para>
/// <para>
/// The <c>[ThreadingDiagnoser]</c> "Completed Work Items" column is the primary
/// evidence for (1) and (2), because those genuinely reduce the number of calls
/// issued and the column is a direct census of them that reproduces
/// bit-identically where the timing column on a shared host does not. For (3),
/// which overlaps the same calls without removing any, the count is flat by
/// construction and serves as the check that the change issues exactly the calls
/// it used to; lead there on the within-round ratio instead.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=bulkloadfanout</c> (or
/// <c>--suite bulkloadfanout</c>); see <c>Program.cs</c>. There is no Orleans
/// silo dependency, so it is cheap to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class BulkLoadAndFenceFanOutBenchmarks
{
    /// <summary>The production window, adopted from the router grain's <c>maxLocalWorkers</c>.</summary>
    private const int FanOutWidth = 32;

    /// <summary>Children per internal node, matching the default <c>MaxInternalChildren</c> order of magnitude.</summary>
    private const int Fanout = 8;

    private FakeTopology _topology = null!;
    private int[] _leafSlots = null!;
    private string[] _leafIds = null!;
    private string[] _fenceTargets = null!;
    private string[] _internalRoots = null!;

    /// <summary>
    /// Scale of the fanned-out set: leaves in a bulk load, internal nodes in a
    /// purge pre-walk, shard roots in a write fence. Neither value is an exact
    /// multiple of the window, so the ragged trailing wave is always paid for
    /// rather than flattered away. A real bulk load or a real multi-tree fence
    /// is far larger, which only widens the gap.
    /// </summary>
    [Params(70, 500)]
    public int TargetCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _topology = new FakeTopology();

        _leafSlots = new int[TargetCount];
        _leafIds = new string[TargetCount];
        _fenceTargets = new string[TargetCount];
        for (var i = 0; i < TargetCount; i++)
        {
            _leafSlots[i] = i;
            _leafIds[i] = "leaf-" + i.ToString("D6", CultureInfo.InvariantCulture);
            _fenceTargets[i] = "tree-a/" + i.ToString(CultureInfo.InvariantCulture);
        }

        _internalRoots = [_topology.BuildInternalTree(TargetCount, Fanout)];

        // A mis-built fake tree does not fail - it just terminates the walk
        // early and reports a flattering, meaningless number, which is exactly
        // how the first draft of this suite measured a 70-node walk as a
        // 1-node one. Assert the shape the lanes are supposed to traverse.
        var visited = _topology.CountReachableInternalNodes(_internalRoots[0]);
        if (visited != TargetCount)
        {
            throw new InvalidOperationException(
                $"fake internal tree is malformed: reached {visited} of {TargetCount} nodes.");
        }
    }

    // -- (1) the bulk-load leaf chain --

    /// <summary>
    /// The shipped shape before the change: per leaf, five gated calls awaited
    /// one at a time, with the sibling chain wired up one leaf behind the loop.
    /// </summary>
    [Benchmark(Baseline = true, Description = "bulkload: 5 serial calls per leaf (yielding store)")]
    public async Task<int> BulkLoad_Serial_FiveCalls_Async()
    {
        string? prevLeafId = null;
        for (var i = 0; i < _leafIds.Length; i++)
        {
            var leafId = _leafIds[i];
            await _topology.SetTreeIdAsync(leafId, "tree-a");
            await _topology.SetShardIndexAsync(leafId, 0);
            await _topology.MergeEntriesAsync(leafId, i);

            if (prevLeafId is not null)
            {
                await _topology.SetNextSiblingAsync(prevLeafId, leafId);
                await _topology.SetPrevSiblingAsync(leafId, prevLeafId);
            }

            prevLeafId = leafId;
        }

        return _leafIds.Length;
    }

    /// <summary>
    /// The shipped shape after the change: the four birth-time setters collapse
    /// into one <c>InitializeSiblingAsync</c> batch, and the leaves - now fully
    /// planned before any is touched - are issued through the production
    /// <c>BoundedFanOut.ForEachAsync</c>.
    /// </summary>
    [Benchmark(Description = "bulkload: 2 calls per leaf, BoundedFanOut width 32 (yielding store)")]
    public async Task<int> BulkLoad_Collapsed_BoundedFanOut_Async()
    {
        await BoundedFanOut.ForEachAsync(
            _leafSlots,
            FanOutWidth,
            async slot =>
            {
                var leafId = _leafIds[slot];
                await _topology.InitializeSiblingAsync(
                    leafId,
                    "tree-a",
                    0,
                    slot > 0 ? _leafIds[slot - 1] : null,
                    slot + 1 < _leafIds.Length ? _leafIds[slot + 1] : null);
                await _topology.MergeEntriesAsync(leafId, slot);
            });

        return _leafIds.Length;
    }

    /// <summary>
    /// Contrast arm: the collapse alone, without the overlap. Isolates how much
    /// of the result is the call-count reduction and how much is the wave, so
    /// the added fan-out is not credited with the batching's win (or the other
    /// way round).
    /// </summary>
    [Benchmark(Description = "bulkload: contrast, 2 calls per leaf but still serial (yielding store)")]
    public async Task<int> BulkLoad_Contrast_CollapsedOnly_Async()
    {
        for (var slot = 0; slot < _leafIds.Length; slot++)
        {
            var leafId = _leafIds[slot];
            await _topology.InitializeSiblingAsync(
                leafId,
                "tree-a",
                0,
                slot > 0 ? _leafIds[slot - 1] : null,
                slot + 1 < _leafIds.Length ? _leafIds[slot + 1] : null);
            await _topology.MergeEntriesAsync(leafId, slot);
        }

        return _leafIds.Length;
    }

    [Benchmark(Description = "bulkload: 5 serial calls per leaf (completed-task store)")]
    public async Task<int> BulkLoad_Serial_FiveCalls()
    {
        string? prevLeafId = null;
        for (var i = 0; i < _leafIds.Length; i++)
        {
            var leafId = _leafIds[i];
            await _topology.SetTreeIdCompletedAsync(leafId, "tree-a");
            await _topology.SetShardIndexCompletedAsync(leafId, 0);
            await _topology.MergeEntriesCompletedAsync(leafId, i);

            if (prevLeafId is not null)
            {
                await _topology.SetNextSiblingCompletedAsync(prevLeafId, leafId);
                await _topology.SetPrevSiblingCompletedAsync(leafId, prevLeafId);
            }

            prevLeafId = leafId;
        }

        return _leafIds.Length;
    }

    [Benchmark(Description = "bulkload: 2 calls per leaf, BoundedFanOut width 32 (completed-task store)")]
    public async Task<int> BulkLoad_Collapsed_BoundedFanOut()
    {
        await BoundedFanOut.ForEachAsync(
            _leafSlots,
            FanOutWidth,
            async slot =>
            {
                var leafId = _leafIds[slot];
                await _topology.InitializeSiblingCompletedAsync(
                    leafId,
                    "tree-a",
                    0,
                    slot > 0 ? _leafIds[slot - 1] : null,
                    slot + 1 < _leafIds.Length ? _leafIds[slot + 1] : null);
                await _topology.MergeEntriesCompletedAsync(leafId, slot);
            });

        return _leafIds.Length;
    }

    // -- (2) the purge internal-node pre-walk --

    /// <summary>
    /// The shipped shape before the change: a serial depth-first stack that
    /// pays two round trips per node to read two fields of one state.
    /// </summary>
    [Benchmark(Description = "purgewalk: serial DFS, 2 calls per node (yielding store)")]
    public async Task<int> PurgeWalk_Serial_TwoCalls_Async()
    {
        var collected = 0;
        var stack = new Stack<string>();
        stack.Push(_internalRoots[0]);

        while (stack.Count > 0)
        {
            var nodeId = stack.Pop();
            collected++;

            if (await _topology.AreChildrenLeavesAsync(nodeId))
            {
                continue;
            }

            var children = await _topology.GetChildIdsAsync(nodeId);
            for (var i = children.Count - 1; i >= 0; i--)
            {
                stack.Push(children[i]);
            }
        }

        return collected;
    }

    /// <summary>
    /// The shipped shape after the change: one routing snapshot per node, and a
    /// whole level's reads overlapped through the production
    /// <c>BoundedFanOut.ReadAheadAsync</c>.
    /// </summary>
    [Benchmark(Description = "purgewalk: level-parallel BFS, 1 call per node (yielding store)")]
    public async Task<int> PurgeWalk_LevelParallel_OneCall_Async()
    {
        var collected = 0;
        var level = new List<string> { _internalRoots[0] };

        while (level.Count > 0)
        {
            collected += level.Count;

            var next = new List<string>();
            await foreach (var routing in BoundedFanOut.ReadAheadAsync(
                level,
                FanOutWidth,
                id => _topology.GetRoutingTableAsync(id)))
            {
                if (routing.ChildrenAreLeaves)
                {
                    continue;
                }

                next.AddRange(routing.ChildIds);
            }

            level = next;
        }

        return collected;
    }

    /// <summary>
    /// Contrast arm: the snapshot collapse alone, without the level overlap.
    /// Halves the call count and changes nothing else, so it separates the
    /// batching's contribution from the fan-out's.
    /// </summary>
    [Benchmark(Description = "purgewalk: contrast, 1 call per node but still serial DFS (yielding store)")]
    public async Task<int> PurgeWalk_Contrast_CollapsedOnly_Async()
    {
        var collected = 0;
        var stack = new Stack<string>();
        stack.Push(_internalRoots[0]);

        while (stack.Count > 0)
        {
            var nodeId = stack.Pop();
            collected++;

            var routing = await _topology.GetRoutingTableAsync(nodeId);
            if (routing.ChildrenAreLeaves)
            {
                continue;
            }

            for (var i = routing.ChildIds.Count - 1; i >= 0; i--)
            {
                stack.Push(routing.ChildIds[i]);
            }
        }

        return collected;
    }

    // -- (3) the saga write-fence fan-out --

    /// <summary>
    /// The shipped shape before the change: one shard root fenced per await,
    /// so the group-atomic step's wall-clock is linear in the fenced topology.
    /// </summary>
    [Benchmark(Description = "fence: serial engage over trees x shards (yielding store)")]
    public async Task<int> Fence_Serial_Async()
    {
        for (var i = 0; i < _fenceTargets.Length; i++)
        {
            await _topology.EngageWriteFenceAsync(_fenceTargets[i], "saga-1");
        }

        return _fenceTargets.Length;
    }

    [Benchmark(Description = "fence: BoundedFanOut.ForEachAsync width 32 (yielding store)")]
    public async Task<int> Fence_BoundedFanOut_Async()
    {
        await BoundedFanOut.ForEachAsync(
            _fenceTargets,
            FanOutWidth,
            target => _topology.EngageWriteFenceAsync(target, "saga-1"));

        return _fenceTargets.Length;
    }

    /// <summary>
    /// Contrast arm for the rejected alternative: drop the cap and put every
    /// engage in flight at once. It cannot overlap further than the router
    /// grain's 32 local workers allow, so past that width it buys no additional
    /// concurrency - it only holds N tasks alive instead of 32, turning a fence
    /// whose working set is a constant into one that scales with the topology,
    /// with every call racing a single Orleans response deadline.
    /// </summary>
    [Benchmark(Description = "fence: contrast, unbounded fan-out (yielding store)")]
    public async Task<int> Fence_UnboundedFanOut_Async()
    {
        var all = new Task[_fenceTargets.Length];
        for (var i = 0; i < _fenceTargets.Length; i++)
        {
            all[i] = _topology.EngageWriteFenceAsync(_fenceTargets[i], "saga-1");
        }

        await Task.WhenAll(all);
        return _fenceTargets.Length;
    }

    /// <summary>
    /// The narrowest stand-in for the grain surfaces the three lanes drive: the
    /// call shapes they contrast, over plain in-memory dictionaries. Every
    /// <c>*CompletedAsync</c> member returns an already-completed task, so a lane
    /// running against it charges the async machinery and the wave list and
    /// nothing else - which is exactly why an overlapping change reads as flat or
    /// worse there. The default members yield once, which is the shape a real
    /// Orleans grain call actually has (it never completes synchronously) and is
    /// still far cheaper than a real hop, so those lanes floor the saving rather
    /// than estimate it.
    /// </summary>
    private sealed class FakeTopology
    {
        // Concurrent maps, and deliberately so on both sides of every contrast.
        // A real grain activation is single-threaded, so the production code has
        // no such synchronisation to pay for; here the fanned-out lanes really do
        // write from several threads, and a plain Dictionary corrupts under them.
        // Making only the fanned-out lanes pay for concurrency-safe writes would
        // load the dice against the change, so every lane - serial and fanned-out
        // alike - goes through the same ConcurrentDictionary and the ratio stays
        // a measurement of the dispatch shape rather than of the harness.
        private readonly ConcurrentDictionary<string, string> _treeIds = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, int> _shardIndexes = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, int> _entries = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, string?> _next = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, string?> _prev = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, string> _fences = new(StringComparer.Ordinal);

        // Written once in GlobalSetup and only read thereafter, so concurrent
        // reads of a plain Dictionary are safe and the walk lanes are not taxed.
        private readonly Dictionary<string, FakeInternalNode> _nodes = new(StringComparer.Ordinal);

        /// <summary>
        /// Builds an internal-node tree of exactly <paramref name="nodeCount"/>
        /// nodes, laid out as the standard implicit n-ary heap: node <c>i</c>'s
        /// children are <c>i * fanout + 1 .. i * fanout + fanout</c>, bounded by
        /// the node count. Every node is therefore reachable from node 0 exactly
        /// once, and the nodes whose child range falls off the end report their
        /// children as leaves, which is what terminates both walks. Returns the
        /// root id.
        /// </summary>
        public string BuildInternalTree(int nodeCount, int fanout)
        {
            var ids = new List<string>(nodeCount);
            for (var i = 0; i < nodeCount; i++)
            {
                ids.Add("node-" + i.ToString("D6", CultureInfo.InvariantCulture));
            }

            for (var parent = 0; parent < nodeCount; parent++)
            {
                var children = new List<string>(fanout);
                for (var c = 1; c <= fanout; c++)
                {
                    var child = (parent * fanout) + c;
                    if (child >= nodeCount)
                    {
                        break;
                    }

                    children.Add(ids[child]);
                }

                _nodes[ids[parent]] = children.Count > 0
                    ? new FakeInternalNode(children, false)
                    : new FakeInternalNode([], true);
            }

            return ids[0];
        }

        /// <summary>
        /// Counts the internal nodes reachable from <paramref name="rootId"/>,
        /// synchronously, so <see cref="Setup"/> can assert the tree it just
        /// built is the size the lanes expect.
        /// </summary>
        public int CountReachableInternalNodes(string rootId)
        {
            var visited = 0;
            var stack = new Stack<string>();
            stack.Push(rootId);

            while (stack.Count > 0)
            {
                var nodeId = stack.Pop();
                visited++;

                var node = _nodes[nodeId];
                if (node.ChildrenAreLeaves)
                {
                    continue;
                }

                foreach (var child in node.ChildIds)
                {
                    stack.Push(child);
                }
            }

            return visited;
        }

        // -- yielding (grain-shaped) members --

        public async Task SetTreeIdAsync(string leafId, string treeId)
        {
            await Task.Yield();
            _treeIds[leafId] = treeId;
        }

        public async Task SetShardIndexAsync(string leafId, int shardIndex)
        {
            await Task.Yield();
            _shardIndexes[leafId] = shardIndex;
        }

        public async Task MergeEntriesAsync(string leafId, int count)
        {
            await Task.Yield();
            _entries[leafId] = count;
        }

        public async Task SetNextSiblingAsync(string leafId, string? siblingId)
        {
            await Task.Yield();
            _next[leafId] = siblingId;
        }

        public async Task SetPrevSiblingAsync(string leafId, string? siblingId)
        {
            await Task.Yield();
            _prev[leafId] = siblingId;
        }

        public async Task InitializeSiblingAsync(
            string leafId,
            string treeId,
            int shardIndex,
            string? prevSibling,
            string? nextSibling)
        {
            await Task.Yield();
            ApplyInitializeSibling(leafId, treeId, shardIndex, prevSibling, nextSibling);
        }

        public async Task<bool> AreChildrenLeavesAsync(string nodeId)
        {
            await Task.Yield();
            return _nodes[nodeId].ChildrenAreLeaves;
        }

        public async Task<IReadOnlyList<string>> GetChildIdsAsync(string nodeId)
        {
            await Task.Yield();
            return _nodes[nodeId].ChildIds;
        }

        public async Task<FakeInternalNode> GetRoutingTableAsync(string nodeId)
        {
            await Task.Yield();
            return _nodes[nodeId];
        }

        public async Task EngageWriteFenceAsync(string shardKey, string sagaId)
        {
            await Task.Yield();
            _fences[shardKey] = sagaId;
        }

        // -- completed-task members (the harness control) --

        public Task SetTreeIdCompletedAsync(string leafId, string treeId)
        {
            _treeIds[leafId] = treeId;
            return Task.CompletedTask;
        }

        public Task SetShardIndexCompletedAsync(string leafId, int shardIndex)
        {
            _shardIndexes[leafId] = shardIndex;
            return Task.CompletedTask;
        }

        public Task MergeEntriesCompletedAsync(string leafId, int count)
        {
            _entries[leafId] = count;
            return Task.CompletedTask;
        }

        public Task SetNextSiblingCompletedAsync(string leafId, string? siblingId)
        {
            _next[leafId] = siblingId;
            return Task.CompletedTask;
        }

        public Task SetPrevSiblingCompletedAsync(string leafId, string? siblingId)
        {
            _prev[leafId] = siblingId;
            return Task.CompletedTask;
        }

        public Task InitializeSiblingCompletedAsync(
            string leafId,
            string treeId,
            int shardIndex,
            string? prevSibling,
            string? nextSibling)
        {
            ApplyInitializeSibling(leafId, treeId, shardIndex, prevSibling, nextSibling);
            return Task.CompletedTask;
        }

        private void ApplyInitializeSibling(
            string leafId,
            string treeId,
            int shardIndex,
            string? prevSibling,
            string? nextSibling)
        {
            _treeIds[leafId] = treeId;
            _shardIndexes[leafId] = shardIndex;
            _prev[leafId] = prevSibling;
            _next[leafId] = nextSibling;
        }
    }

    /// <summary>
    /// The two fields the purge pre-walk needs from an internal node - which is
    /// exactly what the shipped <c>RoutingTableSnapshot</c> carries, and exactly
    /// why one call can replace two.
    /// </summary>
    private sealed record FakeInternalNode(IReadOnlyList<string> ChildIds, bool ChildrenAreLeaves);
}
