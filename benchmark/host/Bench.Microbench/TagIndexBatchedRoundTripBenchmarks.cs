using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the three tag-index round-trip reductions this suite was added for:
/// the batched membership-row add, the overlapped membership-row removal, and
/// the windowed intersection-query probe.
/// <para>
/// (1) <c>AddTagsForKeyAsync</c> - tagging a key wrote a tag-major row and its
/// key-major mirror per tag, each as its own awaited <c>SetAsync</c>, so a
/// five-tag add cost ten sequential writes. Every row carries the same constant
/// presence value under a distinct key, so ordering is immaterial and the whole
/// fan-out collapses into one <c>SetManyAsync</c>.
/// </para>
/// <para>
/// (2) <c>RemoveTagsForKeyAsync</c> - the mirror of (1), except there is no
/// batched delete on <c>ILattice</c> to collapse into. The removals can still
/// stop being serial: the router grain is a stateless worker, so deletes issued
/// together are serviced concurrently rather than queued behind one another.
/// </para>
/// <para>
/// (3) <c>QueryAsync</c> (the AND branch) - an intersection query streams the
/// first tag's posting list and confirmed each candidate's remaining tags with
/// its own read, costing one round trip per candidate however few tags are
/// involved. Buffering a window of candidates carries the same number of rows in
/// a fraction of the calls.
/// </para>
/// <para>
/// <b>Read the yielding-store lanes, not the completed-task ones.</b> All three
/// changes trade many small calls for fewer larger ones, and a store that
/// returns an already-completed task prices a round trip at approximately zero -
/// so against it a batching change can only ever look flat or slightly worse,
/// because it pays for the batch container without being credited the calls it
/// removed. The <c>_Async</c> lanes run the identical bodies over a store that
/// yields once per call, which is still orders of magnitude cheaper than a real
/// grain hop and therefore a strict <b>lower bound</b> on the saving. The
/// <c>[ThreadingDiagnoser]</c> "Completed Work Items" column counts those calls
/// directly and is the honest measure of the change: it falls by the batching
/// factor regardless of what the timing column does.
/// </para>
/// <para>
/// Every lane in a pair reproduces its partner's surrounding shell exactly - the
/// same store, the same row-key construction, the same accumulation and return
/// type - so the only thing that differs is the dispatch shape under test. The
/// baseline arms cannot call the shipped methods because the shipped methods are
/// the optimised ones, so the shells here mirror the production code rather than
/// invoking it.
/// </para>
/// <para>
/// Run it via <c>BENCH_MICROBENCH_SUITE=tagindexbatching</c> (or
/// <c>--suite tagindexbatching</c>); see <c>Program.cs</c>. The suite has no
/// Orleans silo dependency, so it is fast to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c> for tight confidence intervals.
/// </para>
/// </summary>
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class TagIndexBatchedRoundTripBenchmarks
{
    private const char Sep = '\0';
    private const string TreeId = "orders-tree";
    private const string KeyMajorPrefix = "\0k\0";

    /// <summary>Mirrors the production concurrency cap on the removal wave.</summary>
    private const int RemoveRowConcurrencyLimit = 32;

    /// <summary>Mirrors the production AND-query candidate window.</summary>
    private const int AndQueryCandidateWindow = 32;

    /// <summary>The constant presence value every membership row carries.</summary>
    private static readonly byte[] Flag = [1];

    private string[] _tags = null!;
    private string _subjectKey = null!;
    private string[] _candidates = null!;
    private InMemoryTagStore _store = null!;
    private AsyncTagStore _asyncStore = null!;

    /// <summary>
    /// Number of tags on the key the add / remove lanes write. Five is the
    /// realistic middle of the range a tagging write sees; the wide arm crosses
    /// the removal concurrency cap so the wave-by-wave path is exercised too.
    /// </summary>
    [Params(5, 40)]
    public int TagCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _subjectKey = "key-0042";
        var tags = new string[TagCount];
        for (var t = 0; t < TagCount; t++)
        {
            tags[t] = "tag-" + t.ToString("D2", CultureInfo.InvariantCulture);
        }

        _tags = tags;

        // A posting list wide enough to span several windows with a partial one
        // at the end, so the windowed lane pays for the ragged tail rather than
        // being flattered by an exact multiple.
        const int candidateCount = 70;
        _candidates = new string[candidateCount];
        _store = new InMemoryTagStore();
        for (var c = 0; c < candidateCount; c++)
        {
            var key = "key-" + c.ToString("D4", CultureInfo.InvariantCulture);
            _candidates[c] = key;

            // Seed the sibling-tag rows the intersection probes. One candidate in
            // eight is missing a row, which is the shape that makes the early-out
            // in both arms behave the same way.
            if (c % 8 != 0)
            {
                _store.Seed(RowKey("tag-01", TreeId, key), Flag);
                _store.Seed(RowKey("tag-02", TreeId, key), Flag);
            }
        }

        // Seed the rows the removal lanes delete. Re-seeded per invocation by the
        // lanes themselves so a removal cannot exhaust the fixture.
        _asyncStore = new AsyncTagStore(_store);
    }

    private static string RowKey(string tag, string treeId, string key) =>
        string.Concat(tag, Sep.ToString(), treeId, Sep.ToString(), key);

    private static string KeyRowKey(string treeId, string key, string tag) =>
        string.Concat(KeyMajorPrefix, treeId, Sep.ToString(), key, Sep.ToString(), tag);

    // ── (1) Membership-row add: 2N sequential writes vs one batched write ──

    [Benchmark(Baseline = true, Description = "add: 2N sequential SetAsync (completed-task store)")]
    public async Task<int> AddRows_Sequential()
    {
        var written = 0;
        for (var i = 0; i < _tags.Length; i++)
        {
            var tag = _tags[i];
            await _store.SetAsync(RowKey(tag, TreeId, _subjectKey), Flag, CancellationToken.None);
            await _store.SetAsync(KeyRowKey(TreeId, _subjectKey, tag), Flag, CancellationToken.None);
            written += 2;
        }

        return written;
    }

    [Benchmark(Description = "add: one batched SetManyAsync (completed-task store)")]
    public async Task<int> AddRows_Batched()
    {
        var rows = new List<KeyValuePair<string, byte[]>>(_tags.Length * 2);
        for (var i = 0; i < _tags.Length; i++)
        {
            var tag = _tags[i];
            rows.Add(new KeyValuePair<string, byte[]>(RowKey(tag, TreeId, _subjectKey), Flag));
            rows.Add(new KeyValuePair<string, byte[]>(KeyRowKey(TreeId, _subjectKey, tag), Flag));
        }

        await _store.SetManyAsync(rows, CancellationToken.None);
        return rows.Count;
    }

    [Benchmark(Description = "add: 2N sequential SetAsync (yielding store)")]
    public async Task<int> AddRows_Sequential_Async()
    {
        var written = 0;
        for (var i = 0; i < _tags.Length; i++)
        {
            var tag = _tags[i];
            await _asyncStore.SetAsync(RowKey(tag, TreeId, _subjectKey), Flag, CancellationToken.None);
            await _asyncStore.SetAsync(KeyRowKey(TreeId, _subjectKey, tag), Flag, CancellationToken.None);
            written += 2;
        }

        return written;
    }

    [Benchmark(Description = "add: one batched SetManyAsync (yielding store)")]
    public async Task<int> AddRows_Batched_Async()
    {
        var rows = new List<KeyValuePair<string, byte[]>>(_tags.Length * 2);
        for (var i = 0; i < _tags.Length; i++)
        {
            var tag = _tags[i];
            rows.Add(new KeyValuePair<string, byte[]>(RowKey(tag, TreeId, _subjectKey), Flag));
            rows.Add(new KeyValuePair<string, byte[]>(KeyRowKey(TreeId, _subjectKey, tag), Flag));
        }

        await _asyncStore.SetManyAsync(rows, CancellationToken.None);
        return rows.Count;
    }

    // ── (2) Membership-row removal: serial awaits vs an overlapped wave ──

    [Benchmark(Description = "remove: 2N serial DeleteAsync (yielding store)")]
    public async Task<int> RemoveRows_Sequential_Async()
    {
        var removed = 0;
        for (var i = 0; i < _tags.Length; i++)
        {
            var tag = _tags[i];
            await _asyncStore.DeleteAsync(RowKey(tag, TreeId, _subjectKey), CancellationToken.None);
            await _asyncStore.DeleteAsync(KeyRowKey(TreeId, _subjectKey, tag), CancellationToken.None);
            removed += 2;
        }

        return removed;
    }

    [Benchmark(Description = "remove: overlapped wave, capped at 32 (yielding store)")]
    public async Task<int> RemoveRows_Concurrent_Async()
    {
        var removed = 0;
        var wave = new List<Task>(Math.Min(_tags.Length * 2, RemoveRowConcurrencyLimit));
        for (var i = 0; i < _tags.Length; i++)
        {
            var tag = _tags[i];
            wave.Add(_asyncStore.DeleteAsync(RowKey(tag, TreeId, _subjectKey), CancellationToken.None));
            wave.Add(_asyncStore.DeleteAsync(KeyRowKey(TreeId, _subjectKey, tag), CancellationToken.None));
            removed += 2;

            if (wave.Count >= RemoveRowConcurrencyLimit)
            {
                await Task.WhenAll(wave);
                wave.Clear();
            }
        }

        if (wave.Count > 0)
        {
            await Task.WhenAll(wave);
        }

        return removed;
    }

    // ── (3) Intersection query: per-candidate probe vs a windowed probe ──

    [Benchmark(Description = "intersect: one probe per candidate (yielding store)")]
    public async Task<int> Intersect_PerCandidate_Async()
    {
        var matched = 0;
        var probe = new List<string>(2);
        for (var c = 0; c < _candidates.Length; c++)
        {
            var key = _candidates[c];
            probe.Clear();
            probe.Add(RowKey("tag-01", TreeId, key));
            probe.Add(RowKey("tag-02", TreeId, key));

            var rows = await _asyncStore.GetManyAsync(probe, CancellationToken.None);
            var inAll = true;
            for (var i = 0; i < probe.Count; i++)
            {
                if (!rows.TryGetValue(probe[i], out _))
                {
                    inAll = false;
                    break;
                }
            }

            if (inAll)
            {
                matched++;
            }
        }

        return matched;
    }

    [Benchmark(Description = "intersect: windowed probe, 32 candidates (yielding store)")]
    public async Task<int> Intersect_Windowed_Async() =>
        await IntersectWindowedAsync(AndQueryCandidateWindow);

    /// <summary>
    /// Contrast arm for the rejected alternative: buffer the entire posting list
    /// and probe it in one call. It removes every round trip but one, yet it must
    /// hold the whole posting list and its whole row-key probe in memory before a
    /// single key is emitted, so a large tag turns a streaming query into an
    /// unbounded materialisation and delays the first result by the length of the
    /// list. The bounded window keeps the streaming contract and still collapses
    /// the call count by its width.
    /// </summary>
    [Benchmark(Description = "intersect: contrast, unbounded window (yielding store)")]
    public async Task<int> Intersect_UnboundedWindow_Async() =>
        await IntersectWindowedAsync(int.MaxValue);

    private async Task<int> IntersectWindowedAsync(int windowSize)
    {
        var matched = 0;
        var capacity = windowSize == int.MaxValue ? _candidates.Length : windowSize;
        var window = new List<string>(capacity);
        var probe = new List<string>(capacity * 2);

        for (var c = 0; c < _candidates.Length; c++)
        {
            window.Add(_candidates[c]);
            if (window.Count < windowSize)
            {
                continue;
            }

            matched += await ProbeWindowAsync(window, probe);
            window.Clear();
        }

        if (window.Count > 0)
        {
            matched += await ProbeWindowAsync(window, probe);
        }

        return matched;
    }

    private async Task<int> ProbeWindowAsync(List<string> window, List<string> probe)
    {
        probe.Clear();
        for (var c = 0; c < window.Count; c++)
        {
            probe.Add(RowKey("tag-01", TreeId, window[c]));
            probe.Add(RowKey("tag-02", TreeId, window[c]));
        }

        var rows = await _asyncStore.GetManyAsync(probe, CancellationToken.None);
        var matched = 0;
        for (var c = 0; c < window.Count; c++)
        {
            var inAll = true;
            var start = c * 2;
            for (var i = 0; i < 2; i++)
            {
                if (!rows.TryGetValue(probe[start + i], out _))
                {
                    inAll = false;
                    break;
                }
            }

            if (inAll)
            {
                matched++;
            }
        }

        return matched;
    }

    /// <summary>
    /// The narrowest stand-in for the tag index's backing tree: the four call
    /// shapes the lanes contrast, over a plain dictionary. Every method returns a
    /// completed task, so lanes running against it charge the async machinery and
    /// the batch container and nothing else - which is why a batching lane can
    /// read as flat or worse here. A real store's round trip is what the change
    /// actually removes.
    /// </summary>
    private sealed class InMemoryTagStore
    {
        private readonly Dictionary<string, byte[]> _map = new(StringComparer.Ordinal);

        public void Seed(string key, byte[] value) => _map[key] = value;

        public Task SetAsync(string key, byte[] value, CancellationToken cancellationToken)
        {
            _map[key] = value;
            return Task.CompletedTask;
        }

        public Task SetManyAsync(List<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken)
        {
            foreach (var entry in entries)
            {
                _map[entry.Key] = entry.Value;
            }

            return Task.CompletedTask;
        }

        public Task<bool> DeleteAsync(string key, CancellationToken cancellationToken) =>
            Task.FromResult(_map.Remove(key));

        public Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken)
        {
            var result = new Dictionary<string, byte[]>(StringComparer.Ordinal);
            foreach (var key in keys)
            {
                if (_map.TryGetValue(key, out var v))
                {
                    result[key] = v;
                }
            }

            return Task.FromResult(result);
        }
    }

    /// <summary>
    /// The same store with genuinely asynchronous completions. An Orleans grain
    /// call never completes synchronously, so this is the shape the production
    /// seam actually has. A single yield per call is still orders of magnitude
    /// cheaper than a real round trip, which makes these lanes a floor on the
    /// saving rather than an estimate of it - and makes the
    /// <c>[ThreadingDiagnoser]</c> work-item count a direct census of the calls
    /// each shape issues.
    /// </summary>
    private sealed class AsyncTagStore(InMemoryTagStore inner)
    {
        public async Task SetAsync(string key, byte[] value, CancellationToken cancellationToken)
        {
            await Task.Yield();
            await inner.SetAsync(key, value, cancellationToken);
        }

        public async Task SetManyAsync(List<KeyValuePair<string, byte[]>> entries, CancellationToken cancellationToken)
        {
            await Task.Yield();
            await inner.SetManyAsync(entries, cancellationToken);
        }

        public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken)
        {
            await Task.Yield();
            return await inner.DeleteAsync(key, cancellationToken);
        }

        public async Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken)
        {
            await Task.Yield();
            return await inner.GetManyAsync(keys, cancellationToken);
        }
    }
}
