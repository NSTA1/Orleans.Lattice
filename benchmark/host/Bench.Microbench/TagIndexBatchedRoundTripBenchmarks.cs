using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Isolates the tag-index round-trip reductions this suite was added for: the
/// batched membership-row add, the overlapped membership-row removal, the
/// windowed intersection-query probe, and - added later - the three reductions
/// on the orphan-reconcile and flag-mode add paths, lanes (4) to (6).
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
/// (4) <c>ReconcileSubjectAsync</c> - the orphan pass re-verified every orphan
/// candidate against the subject tree with its own <c>ExistsAsync</c>. A key
/// carrying T tags produces T candidate rows, so it paid T identical probes for
/// one key. The windowed lane folds the candidates to their distinct keys and
/// confirms them a window at a time.
/// </para>
/// <para>
/// (5) the orphan delete that follows (4) - each confirmed orphan deleted its
/// tag-major row and its key-major mirror on two sequential awaits, the same
/// serial shape (2) removed from the ordinary removal path.
/// </para>
/// <para>
/// (6) <c>AddTagsForKeyAsync</c> under a flag membership mode - a flag row is
/// authored as an enable delta minted against that row's own state, so the rows
/// cannot collapse into one value batch the way (1) does. They can still stop
/// being serial, exactly as (2) does for deletes.
/// </para>
/// <para>
/// (7) the atomic (cross-tree transactional) commit under a flag membership
/// mode - each row's enable delta is minted against that row's own state, and
/// the commit read those states with one <c>GetAsync</c> per row, 2N sequential
/// reads before the transaction could even be staged. The states are only read,
/// never written between reads, so one <c>GetManyAsync</c> fetches them all.
/// </para>
/// <para>
/// (8) the covered-marker self-heal in <c>GetCoveredTreesAsync</c> - a tree
/// found by scanning membership rather than through its marker had its marker
/// written back with its own awaited <c>SetAsync</c>, one per missing tree. The
/// markers carry the same constant value under distinct keys, so they collapse
/// into one <c>SetManyAsync</c> exactly as (1) does.
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
    private const string CoveredMarkerPrefix = "\0covered\0";

    /// <summary>Mirrors the production concurrency cap on the removal wave.</summary>
    private const int RemoveRowConcurrencyLimit = 32;

    /// <summary>Mirrors the production AND-query candidate window.</summary>
    private const int AndQueryCandidateWindow = 32;

    /// <summary>
    /// Mirrors the production window the orphan pass confirms candidate keys in.
    /// It is the same constant as the removal cap for the same reason: it is the
    /// router's original stateless-worker count.
    /// </summary>
    private const int OrphanVerifyWindow = 32;

    /// <summary>
    /// Distinct keys the orphan-reconcile lanes find stranded membership rows
    /// for. Each one contributes <see cref="TagCount"/> candidate rows, so the
    /// de-duplication factor the lanes contrast is the tag width itself.
    /// </summary>
    private const int OrphanKeyCount = 16;

    /// <summary>The constant presence value every membership row carries.</summary>
    private static readonly byte[] Flag = [1];

    private string[] _tags = null!;
    private string _subjectKey = null!;
    private string[] _candidates = null!;
    private OrphanCandidate[] _orphanCandidates = null!;
    private InMemoryTagStore _store = null!;
    private AsyncTagStore _asyncStore = null!;
    private InMemoryTagStore _subject = null!;
    private AsyncTagStore _asyncSubject = null!;
    private List<string> _mintRowKeys = null!;
    private string[] _markerKeys = null!;

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

        // The orphan-reconcile fixture. Every distinct key contributes TagCount
        // candidate rows, which is exactly the duplication the de-duplicating
        // lanes collapse. One key in four is still live on the subject tree -
        // the concurrent-write race the re-verification exists to catch - so
        // both arms do the same confirm-then-skip work rather than the
        // optimised one being flattered by an all-orphan corpus.
        _subject = new InMemoryTagStore();
        var orphanRows = new List<OrphanCandidate>(OrphanKeyCount * TagCount);
        for (var k = 0; k < OrphanKeyCount; k++)
        {
            var key = "orphan-" + k.ToString("D4", CultureInfo.InvariantCulture);
            if (k % 4 == 0)
            {
                _subject.Seed(key, Flag);
            }

            for (var t = 0; t < TagCount; t++)
            {
                var tag = _tags[t];
                orphanRows.Add(new OrphanCandidate(RowKey(tag, TreeId, key), key, tag));
                _store.Seed(RowKey(tag, TreeId, key), Flag);
                _store.Seed(KeyRowKey(TreeId, key, tag), Flag);
            }
        }

        _orphanCandidates = [.. orphanRows];
        _asyncSubject = new AsyncTagStore(_subject);

        // The atomic flag-mode mint fixture: a tag-major row and its key-major
        // mirror per tag, in the staging order the commit reads them. Every
        // other tag's tag-major row already carries state, so both arms mint
        // over a mix of present and absent rows rather than an all-absent one
        // the batched read could short-circuit.
        _mintRowKeys = new List<string>(TagCount * 2);
        for (var t = 0; t < TagCount; t++)
        {
            var rowKey = RowKey(_tags[t], TreeId, "mint-key");
            if (t % 2 == 0)
            {
                _store.Seed(rowKey, [1, 2, 3]);
            }

            _mintRowKeys.Add(rowKey);
            _mintRowKeys.Add(KeyRowKey(TreeId, "mint-key", _tags[t]));
        }

        // One covered marker per tree carrying membership. The lanes reuse
        // TagCount as the tree count so the two widths match the other lanes.
        _markerKeys = new string[TagCount];
        for (var t = 0; t < TagCount; t++)
        {
            _markerKeys[t] = CoveredMarkerPrefix + "tree-" + t.ToString("D2", CultureInfo.InvariantCulture);
        }

        AssertLanesAgree();
    }

    /// <summary>
    /// Every optimised lane added for (4) to (6) must return exactly what its
    /// baseline returns, including over the corpus that violates the thing the
    /// optimisation leans on: a quarter of the orphan keys are still live, so a
    /// lane that confused "absent from the batched result" with "no row" would
    /// disagree here rather than in production.
    /// </summary>
    private void AssertLanesAgree()
    {
        var perRow = ReconcileVerify_PerRow_Async().GetAwaiter().GetResult();
        var dedup = ReconcileVerify_DedupOnly_Async().GetAwaiter().GetResult();
        var windowed = ReconcileVerify_Windowed_Async().GetAwaiter().GetResult();
        if (perRow != dedup || perRow != windowed)
        {
            throw new InvalidOperationException(
                $"Orphan re-verification lanes disagree: per-row={perRow}, dedup={dedup}, windowed={windowed}.");
        }

        var serialDeletes = ReconcileDelete_Serial_Async().GetAwaiter().GetResult();
        var waveDeletes = ReconcileDelete_Wave_Async().GetAwaiter().GetResult();
        if (serialDeletes != waveDeletes)
        {
            throw new InvalidOperationException(
                $"Orphan delete lanes disagree: serial={serialDeletes}, wave={waveDeletes}.");
        }

        var serialAdds = AddFlagRows_Sequential_Async().GetAwaiter().GetResult();
        var waveAdds = AddFlagRows_Wave_Async().GetAwaiter().GetResult();
        if (serialAdds != waveAdds)
        {
            throw new InvalidOperationException(
                $"Flag-mode add lanes disagree: serial={serialAdds}, wave={waveAdds}.");
        }

        var serialMint = MintFlagRows_SerialReads_Async().GetAwaiter().GetResult();
        var batchedMint = MintFlagRows_BatchedRead_Async().GetAwaiter().GetResult();
        if (serialMint != batchedMint)
        {
            throw new InvalidOperationException(
                $"Atomic flag mint lanes disagree: serial={serialMint}, batched={batchedMint}.");
        }

        var serialMarkers = CoveredMarkers_Serial_Async().GetAwaiter().GetResult();
        var batchedMarkers = CoveredMarkers_Batched_Async().GetAwaiter().GetResult();
        if (serialMarkers != batchedMarkers)
        {
            throw new InvalidOperationException(
                $"Covered-marker lanes disagree: serial={serialMarkers}, batched={batchedMarkers}.");
        }
    }

    /// <summary>
    /// A stranded membership row, carrying the subject key it was authored for.
    /// Mirrors the production candidate buffered by the reconcile scan.
    /// </summary>
    private readonly record struct OrphanCandidate(string RowKey, string Key, string Tag);

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

    // ── (4) Orphan re-verification: per-row probe vs de-duplicated window ──

    [Benchmark(Description = "reconcile: one ExistsAsync per orphan row (yielding store)")]
    public async Task<int> ReconcileVerify_PerRow_Async()
    {
        var orphans = 0;
        for (var i = 0; i < _orphanCandidates.Length; i++)
        {
            if (await _asyncSubject.ExistsAsync(_orphanCandidates[i].Key, CancellationToken.None))
            {
                continue;
            }

            orphans++;
        }

        return orphans;
    }

    /// <summary>
    /// Isolating arm. The shipped change does two things at once - it stops
    /// probing the same key once per tag, and it probes a window of keys in one
    /// call - and the first alone accounts for a factor of <c>TagCount</c>. This
    /// lane applies only the de-duplication, so the windowed lane's remaining
    /// margin over it is attributable to the batching and nothing else.
    /// </summary>
    [Benchmark(Description = "reconcile: contrast, de-duplicated but one probe per key (yielding store)")]
    public async Task<int> ReconcileVerify_DedupOnly_Async()
    {
        var distinct = DistinctOrphanKeys();
        var absent = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < distinct.Count; i++)
        {
            if (!await _asyncSubject.ExistsAsync(distinct[i], CancellationToken.None))
            {
                absent.Add(distinct[i]);
            }
        }

        return CountRowsFor(absent);
    }

    [Benchmark(Description = "reconcile: de-duplicated, windowed probe of 32 keys (yielding store)")]
    public async Task<int> ReconcileVerify_Windowed_Async()
    {
        var distinct = DistinctOrphanKeys();
        var absent = new HashSet<string>(StringComparer.Ordinal);
        var window = new List<string>(Math.Min(distinct.Count, OrphanVerifyWindow));

        for (var i = 0; i < distinct.Count; i++)
        {
            window.Add(distinct[i]);
            if (window.Count < OrphanVerifyWindow)
            {
                continue;
            }

            await ConfirmAbsentAsync(window, absent);
            window.Clear();
        }

        if (window.Count > 0)
        {
            await ConfirmAbsentAsync(window, absent);
        }

        return CountRowsFor(absent);
    }

    private async Task ConfirmAbsentAsync(List<string> window, HashSet<string> absent)
    {
        var present = await _asyncSubject.GetManyAsync(window, CancellationToken.None);
        for (var i = 0; i < window.Count; i++)
        {
            if (!present.ContainsKey(window[i]))
            {
                absent.Add(window[i]);
            }
        }
    }

    private List<string> DistinctOrphanKeys()
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var distinct = new List<string>();
        for (var i = 0; i < _orphanCandidates.Length; i++)
        {
            var key = _orphanCandidates[i].Key;
            if (seen.Add(key))
            {
                distinct.Add(key);
            }
        }

        return distinct;
    }

    private int CountRowsFor(HashSet<string> keys)
    {
        var rows = 0;
        for (var i = 0; i < _orphanCandidates.Length; i++)
        {
            if (keys.Contains(_orphanCandidates[i].Key))
            {
                rows++;
            }
        }

        return rows;
    }

    // ── (5) Orphan delete: 2 serial awaits per orphan vs an overlapped wave ──

    [Benchmark(Description = "reconcile: 2 serial deletes per orphan (yielding store)")]
    public async Task<int> ReconcileDelete_Serial_Async()
    {
        var removed = 0;
        for (var i = 0; i < _orphanCandidates.Length; i++)
        {
            var candidate = _orphanCandidates[i];
            await _asyncStore.DeleteAsync(candidate.RowKey, CancellationToken.None);
            await _asyncStore.DeleteAsync(
                KeyRowKey(TreeId, candidate.Key, candidate.Tag), CancellationToken.None);
            removed += 2;
        }

        return removed;
    }

    [Benchmark(Description = "reconcile: orphan deletes in a wave, capped at 32 (yielding store)")]
    public async Task<int> ReconcileDelete_Wave_Async()
    {
        var removed = 0;
        var wave = new List<Task>(
            Math.Min(_orphanCandidates.Length * 2, RemoveRowConcurrencyLimit));

        for (var i = 0; i < _orphanCandidates.Length; i++)
        {
            var candidate = _orphanCandidates[i];
            wave.Add(_asyncStore.DeleteAsync(candidate.RowKey, CancellationToken.None));
            wave.Add(_asyncStore.DeleteAsync(
                KeyRowKey(TreeId, candidate.Key, candidate.Tag), CancellationToken.None));
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

    // ── (6) Flag-mode add: 2N serial enables vs an overlapped wave ──

    [Benchmark(Description = "flag add: 2N serial EnableAsync (yielding store)")]
    public async Task<int> AddFlagRows_Sequential_Async()
    {
        var written = 0;
        for (var i = 0; i < _tags.Length; i++)
        {
            var tag = _tags[i];
            await _asyncStore.EnableAsync(RowKey(tag, TreeId, _subjectKey), CancellationToken.None);
            await _asyncStore.EnableAsync(KeyRowKey(TreeId, _subjectKey, tag), CancellationToken.None);
            written += 2;
        }

        return written;
    }

    [Benchmark(Description = "flag add: enable wave, capped at 32 (yielding store)")]
    public async Task<int> AddFlagRows_Wave_Async()
    {
        var written = 0;
        var wave = new List<Task>(Math.Min(_tags.Length * 2, RemoveRowConcurrencyLimit));
        for (var i = 0; i < _tags.Length; i++)
        {
            var tag = _tags[i];
            wave.Add(_asyncStore.EnableAsync(RowKey(tag, TreeId, _subjectKey), CancellationToken.None));
            wave.Add(_asyncStore.EnableAsync(KeyRowKey(TreeId, _subjectKey, tag), CancellationToken.None));
            written += 2;

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

        return written;
    }

    // ── (7) Atomic flag-mode commit: 2N serial row reads vs one batched read ──

    [Benchmark(Description = "atomic flag mint: 2N serial GetAsync (yielding store)")]
    public async Task<int> MintFlagRows_SerialReads_Async()
    {
        var minted = 0;
        for (var i = 0; i < _mintRowKeys.Count; i++)
        {
            var state = await _asyncStore.GetAsync(_mintRowKeys[i], CancellationToken.None);
            minted += MintFlagEnableRow(state);
        }

        return minted;
    }

    [Benchmark(Description = "atomic flag mint: one batched GetManyAsync (yielding store)")]
    public async Task<int> MintFlagRows_BatchedRead_Async()
    {
        var minted = 0;
        var states = await _asyncStore.GetManyAsync(_mintRowKeys, CancellationToken.None);
        for (var i = 0; i < _mintRowKeys.Count; i++)
        {
            minted += MintFlagEnableRow(states.GetValueOrDefault(_mintRowKeys[i]));
        }

        return minted;
    }

    /// <summary>
    /// Stands in for the per-row enable mint: the minted counter depends on the
    /// row's own state, which is why each row needs that state read first.
    /// </summary>
    private static int MintFlagEnableRow(byte[]? state) => state is null ? 1 : state.Length + 1;

    // ── (8) Covered-marker self-heal: N serial marker writes vs one batched write ──

    [Benchmark(Description = "covered markers: N serial SetAsync (yielding store)")]
    public async Task<int> CoveredMarkers_Serial_Async()
    {
        var written = 0;
        for (var i = 0; i < _markerKeys.Length; i++)
        {
            await _asyncStore.SetAsync(_markerKeys[i], Flag, CancellationToken.None);
            written++;
        }

        return written;
    }

    [Benchmark(Description = "covered markers: one batched SetManyAsync (yielding store)")]
    public async Task<int> CoveredMarkers_Batched_Async()
    {
        var entries = new List<KeyValuePair<string, byte[]>>(_markerKeys.Length);
        for (var i = 0; i < _markerKeys.Length; i++)
        {
            entries.Add(new KeyValuePair<string, byte[]>(_markerKeys[i], Flag));
        }

        await _asyncStore.SetManyAsync(entries, CancellationToken.None);
        return entries.Count;
    }

    /// <summary>
    /// The narrowest stand-in for the tag index's backing tree: the call shapes
    /// the lanes contrast, over a plain dictionary. Every method returns a
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

        public Task<bool> ExistsAsync(string key, CancellationToken cancellationToken) =>
            Task.FromResult(_map.ContainsKey(key));

        public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken) =>
            Task.FromResult(_map.TryGetValue(key, out var value) ? value : null);

        /// <summary>
        /// The flag-membership write shape: a row's enable delta is minted
        /// against that row's own current state, so the write is a
        /// read-modify-write of one row rather than a blind value set. Modelled
        /// as a single call because the grain performs the mint behind one hop.
        /// </summary>
        public Task EnableAsync(string key, CancellationToken cancellationToken)
        {
            _map[key] = _map.TryGetValue(key, out var current) ? current : Flag;
            return Task.CompletedTask;
        }

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

        public async Task<bool> ExistsAsync(string key, CancellationToken cancellationToken)
        {
            await Task.Yield();
            return await inner.ExistsAsync(key, cancellationToken);
        }

        public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken)
        {
            await Task.Yield();
            return await inner.GetAsync(key, cancellationToken);
        }

        public async Task EnableAsync(string key, CancellationToken cancellationToken)
        {
            await Task.Yield();
            await inner.EnableAsync(key, cancellationToken);
        }

        public async Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys, CancellationToken cancellationToken)
        {
            await Task.Yield();
            return await inner.GetManyAsync(keys, cancellationToken);
        }
    }
}
