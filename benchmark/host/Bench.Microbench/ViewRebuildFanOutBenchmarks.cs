using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three corpus-sized round-trip reductions this suite was added
/// for - the view-generation clear, the shard purge's internal-node sweep, and
/// the view rebuild's source read - all three of which used to issue one grain
/// call per item and await it before issuing the next.
/// <para>
/// (1) <c>ViewMaintainerGrain.ClearTreeAsync</c> drains every key of a view
/// generation and then deletes them one at a time, so clearing an N-key
/// generation cost N sequential round trips. Every key was materialised before
/// the first delete is issued and a per-key delete is idempotent, so the
/// deletions are independent and their completion order is immaterial.
/// </para>
/// <para>
/// (2) <c>ShardRootGrain.PurgeAsync</c> has the same shape over the internal
/// nodes its pre-walk collected. (The leaf chain that precedes it stays serial
/// and is not modelled here: a leaf's sibling pointer must be read before its
/// state is cleared, so that walk genuinely cannot be reordered.) Structurally
/// it is the same lane as (1) - one independent call per collected item - so it
/// is represented by the clear lanes rather than duplicated.
/// </para>
/// <para>
/// (3) <c>InPlaceRebuildAsync</c> / <c>BuildShadowAsync</c> read one source key
/// per projected entry with <c>GetWithVersionAsync</c>. Unlike (1) and (2) the
/// consumer here is <b>not</b> order-insensitive: the projection writes as it
/// goes and must see the keys in the order it scanned them. Only the reads
/// overlap, and results are still yielded in input order, so the loop body keeps
/// exactly the sequence it had when the reads were serial.
/// </para>
/// <para>
/// <b>The fanned-out lanes call the shipped code.</b>
/// <c>BoundedFanOut.ForEachAsync</c> and <c>BoundedFanOut.ReadAheadAsync</c> are
/// the production helpers, reached through <c>InternalsVisibleTo</c>, so what is
/// measured is the change as it ships rather than a mimic of it. The serial
/// baselines have to be hand-written, because the shipped methods are the
/// optimised ones; each reproduces its partner's shell exactly - same store,
/// same item list, same accumulation and return type - so the only difference is
/// the dispatch shape.
/// </para>
/// <para>
/// <b>Read the yielding-store lanes, not the completed-task ones.</b> A store
/// that returns an already-completed task prices a round trip at approximately
/// zero, so against it an overlapping change can only look flat or worse: it
/// pays for the task list without being credited the serialisation it removed.
/// The <c>CompletedTask</c> pair is kept precisely to show that, so the
/// yielding-lane result is not mistaken for an artefact of the harness. A single
/// yield per call is still orders of magnitude cheaper than a real grain hop,
/// which makes the yielding lanes a <b>lower bound</b> on the saving rather than
/// an estimate of it.
/// </para>
/// <para>
/// The <c>[ThreadingDiagnoser]</c> "Completed Work Items" column is the honest
/// measure here: it counts the calls each shape issues and reproduces
/// bit-identically, where the timing column on a shared host does not.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=viewrebuildfanout</c> (or
/// <c>--suite viewrebuildfanout</c>); see <c>Program.cs</c>. There is no Orleans
/// silo dependency, so it is cheap to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class ViewRebuildFanOutBenchmarks
{
    /// <summary>The production window, adopted from the router grain's <c>maxLocalWorkers</c>.</summary>
    private const int FanOutWidth = 32;

    private string[] _keys = null!;
    private CompletedTaskViewStore _store = null!;
    private YieldingViewStore _asyncStore = null!;

    /// <summary>
    /// Size of the generation being cleared / rebuilt. Neither value is an exact
    /// multiple of the window, so the ragged trailing wave is always paid for
    /// rather than flattered away; 70 is a small view and 500 a modest one. Real
    /// generations are far larger, which only widens the gap.
    /// </summary>
    [Params(70, 500)]
    public int KeyCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var keys = new string[KeyCount];
        _store = new CompletedTaskViewStore();
        for (var i = 0; i < KeyCount; i++)
        {
            var key = "row-" + i.ToString("D6", CultureInfo.InvariantCulture);
            keys[i] = key;
            _store.Seed(key, [(byte)(i & 0xFF)]);
        }

        _keys = keys;
        _asyncStore = new YieldingViewStore(_store);
    }

    // -- (1) and (2): the order-insensitive clear / purge sweep --

    [Benchmark(Baseline = true, Description = "clear: N serial DeleteAsync (yielding store)")]
    public async Task<int> Clear_Serial_Async()
    {
        var deleted = 0;
        for (var i = 0; i < _keys.Length; i++)
        {
            await _asyncStore.DeleteAsync(_keys[i], CancellationToken.None);
            deleted++;
        }

        return deleted;
    }

    [Benchmark(Description = "clear: BoundedFanOut.ForEachAsync, width 32 (yielding store)")]
    public async Task<int> Clear_BoundedFanOut_Async()
    {
        await BoundedFanOut.ForEachAsync(
            _keys,
            FanOutWidth,
            key => _asyncStore.DeleteAsync(key, CancellationToken.None));

        return _keys.Length;
    }

    /// <summary>
    /// Contrast arm for the rejected alternative: drop the cap and put every
    /// delete in flight at once. It cannot overlap any further than the router
    /// grain's 32 local workers allow, so past that width it buys no additional
    /// concurrency - it only holds N tasks alive instead of 32, turning a sweep
    /// whose working set is a constant into one that scales with the corpus.
    /// </summary>
    [Benchmark(Description = "clear: contrast, unbounded fan-out (yielding store)")]
    public async Task<int> Clear_UnboundedFanOut_Async()
    {
        var all = new Task<bool>[_keys.Length];
        for (var i = 0; i < _keys.Length; i++)
        {
            all[i] = _asyncStore.DeleteAsync(_keys[i], CancellationToken.None);
        }

        await Task.WhenAll(all);
        return _keys.Length;
    }

    [Benchmark(Description = "clear: N serial DeleteAsync (completed-task store)")]
    public async Task<int> Clear_Serial()
    {
        var deleted = 0;
        for (var i = 0; i < _keys.Length; i++)
        {
            await _store.DeleteAsync(_keys[i], CancellationToken.None);
            deleted++;
        }

        return deleted;
    }

    [Benchmark(Description = "clear: BoundedFanOut.ForEachAsync, width 32 (completed-task store)")]
    public async Task<int> Clear_BoundedFanOut()
    {
        await BoundedFanOut.ForEachAsync(
            _keys,
            FanOutWidth,
            key => _store.DeleteAsync(key, CancellationToken.None));

        return _keys.Length;
    }

    // -- (3): the order-preserving rebuild source read --

    [Benchmark(Description = "rebuild: N serial GetWithVersionAsync (yielding store)")]
    public async Task<int> RebuildRead_Serial_Async()
    {
        var projected = 0;
        for (var i = 0; i < _keys.Length; i++)
        {
            var versioned = await _asyncStore.GetWithVersionAsync(_keys[i], CancellationToken.None);
            if (versioned is not null)
            {
                projected += versioned.Length;
            }
        }

        return projected;
    }

    [Benchmark(Description = "rebuild: BoundedFanOut.ReadAheadAsync, width 32 (yielding store)")]
    public async Task<int> RebuildRead_ReadAhead_Async()
    {
        var projected = 0;
        await foreach (var versioned in BoundedFanOut.ReadAheadAsync(
            _keys,
            FanOutWidth,
            key => _asyncStore.GetWithVersionAsync(key, CancellationToken.None)))
        {
            if (versioned is not null)
            {
                projected += versioned.Length;
            }
        }

        return projected;
    }

    /// <summary>
    /// The narrowest stand-in for a view tree: the two call shapes the lanes
    /// contrast, over a plain dictionary. Every method returns a completed task,
    /// so a lane running against it charges the async machinery and the task list
    /// and nothing else - which is exactly why an overlapping change reads as
    /// flat or worse here. What the change actually removes is the serialisation
    /// of real round trips.
    /// </summary>
    private sealed class CompletedTaskViewStore
    {
        private readonly Dictionary<string, byte[]> _map = new(StringComparer.Ordinal);

        public void Seed(string key, byte[] value) => _map[key] = value;

        public Task<bool> DeleteAsync(string key, CancellationToken cancellationToken) =>
            // Deliberately does not mutate the map: the lanes are re-run
            // thousands of times against one fixture, and a destructive delete
            // would make every iteration after the first walk an empty store.
            Task.FromResult(_map.ContainsKey(key));

        public Task<byte[]?> GetWithVersionAsync(string key, CancellationToken cancellationToken) =>
            Task.FromResult(_map.TryGetValue(key, out var v) ? v : null);
    }

    /// <summary>
    /// The same store with genuinely asynchronous completions. An Orleans grain
    /// call never completes synchronously, so this is the shape the production
    /// seam actually has. One yield per call is still far cheaper than a real
    /// hop, so these lanes floor the saving rather than estimate it - and they
    /// make the <c>[ThreadingDiagnoser]</c> work-item count a direct census of
    /// the calls each shape issues.
    /// </summary>
    private sealed class YieldingViewStore(CompletedTaskViewStore inner)
    {
        public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken)
        {
            await Task.Yield();
            return await inner.DeleteAsync(key, cancellationToken);
        }

        public async Task<byte[]?> GetWithVersionAsync(string key, CancellationToken cancellationToken)
        {
            await Task.Yield();
            return await inner.GetWithVersionAsync(key, cancellationToken);
        }
    }
}
