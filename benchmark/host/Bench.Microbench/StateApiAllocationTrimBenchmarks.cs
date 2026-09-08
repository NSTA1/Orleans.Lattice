using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Benchmark.Microbench;

/// <summary>
/// Allocation trims on three cold-but-repeating paths: the state API's catalog
/// ordering, its metrics delta tick, and the shard root's raw batch-read
/// bucketing.
/// <para>
/// <b>Judge this suite on Allocated.</b> Every lane here is sub-microsecond and
/// the benchmark host is shared, so Mean moves between rounds for reasons that
/// have nothing to do with the change. Allocated reproduces bit-for-bit, and the
/// three trims are allocation trims - each removes objects that had to exist
/// only because of how the loop was written, never because of what it computes.
/// </para>
/// <para>
/// <b>Shell fidelity.</b> Every <c>*_Baseline</c> arm is the prior body pasted
/// verbatim into the identical surrounding shell as its <c>*_Shipped</c>
/// counterpart, over the identical pre-built input, with all routing and setup
/// hoisted out of the measured region. The only difference inside the measured
/// region is the body under test. Where an alternative was considered and
/// rejected, it ships as a <c>*_Contrast</c> arm so the rejection is measured
/// rather than asserted. <see cref="Catalog_RealQuery_Shipped"/> anchors lane 1
/// to the genuinely shipped code by driving the real
/// <c>LatticeStateQuery.ListTreesAsync</c> end to end.
/// </para>
/// <para>
/// Nothing here starts a silo, so the suite is cheap enough to run at
/// <c>BENCH_MICROBENCH_FIDELITY=full</c>. Run it via
/// <c>BENCH_MICROBENCH_SUITE=statetrims</c> (or <c>--suite statetrims</c>); see
/// <c>Program.cs</c>.
/// </para>
/// </summary>
[MemoryDiagnoser]
public class StateApiAllocationTrimBenchmarks
{
    /// <summary>Default catalog page size the state API pages at.</summary>
    private const int PageSize = 100;

    /// <summary>Trees the metrics feed samples per tick.</summary>
    private const int MetricsTreeCount = 256;

    /// <summary>Keys in one saga capture batch.</summary>
    private const int BatchKeyCount = 64;

    // ------------------------------------------------------------------
    // Lane 1 - catalog ordering (LatticeStateQuery.List*Async)
    // ------------------------------------------------------------------

    /// <summary>Catalog width the ordering lane filters and orders.</summary>
    [Params(512, 4096)]
    public int CatalogSize { get; set; }

    private List<string> _catalogIds = [];
    private string? _catalogPageToken;
    private LatticeStateQuery _realQuery = null!;

    // ------------------------------------------------------------------
    // Lane 2 - metrics delta tick (LatticeStateMetricsObserver.ObserveAsync)
    // ------------------------------------------------------------------

    private Dictionary<string, TreeMetrics> _currentSample = [];
    private Dictionary<string, TreeMetrics> _retainedBaseline = [];
    private Dictionary<string, TreeMetrics> _priorSample = [];

    // ------------------------------------------------------------------
    // Lane 3 - raw batch-read bucketing (ShardRootGrain.GetRawEntriesAsync)
    // ------------------------------------------------------------------

    /// <summary>
    /// Distinct leaves the batch routes to. <c>1</c> is the shape the saga
    /// capture path actually produces (its keys were resolved from one leaf
    /// set); <c>4</c> is present to show the multi-leaf shape is not regressed.
    /// </summary>
    [Params(1, 4)]
    public int LeafCount { get; set; }

    private List<string> _batchKeys = [];
    private GrainId[] _routedLeaves = [];
    private List<LwwEntry?> _leafResponse = [];

    /// <summary>
    /// Builds every lane's inputs. All routing, catalog construction and metrics
    /// sampling happens here so no measured body pays for its own setup.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _catalogIds = new List<string>(CatalogSize);
        for (var i = 0; i < CatalogSize; i++)
        {
            _catalogIds.Add(string.Create(CultureInfo.InvariantCulture, $"catalog-tree-{i:D5}"));
        }

        // Page from the middle, which is the steady state of a paging walk: the
        // token filter drops a prefix and the rest survives to be ordered.
        _catalogPageToken = _catalogIds[CatalogSize / 2];

        _realQuery = CatalogHarness.BuildQuery(new CatalogGrainSurface(_catalogIds), visibility: false);

        _currentSample = new Dictionary<string, TreeMetrics>(MetricsTreeCount, StringComparer.Ordinal);
        for (var i = 0; i < MetricsTreeCount; i++)
        {
            var id = string.Create(CultureInfo.InvariantCulture, $"metrics-tree-{i:D4}");
            _currentSample[id] = new TreeMetrics
            {
                TreeId = id,
                ShardCount = 4,
                LiveKeys = 1_000 + i,
            };
        }

        // Steady state: the observer holds a warm baseline whose contents match
        // the current sample, so the tick emits an empty delta. That is the tick
        // an idle dashboard subscription actually pays for, thousands of times.
        _retainedBaseline = new Dictionary<string, TreeMetrics>(_currentSample, StringComparer.Ordinal);
        _priorSample = new Dictionary<string, TreeMetrics>(_currentSample, StringComparer.Ordinal);

        _batchKeys = new List<string>(BatchKeyCount);
        _routedLeaves = new GrainId[BatchKeyCount];
        for (var i = 0; i < BatchKeyCount; i++)
        {
            _batchKeys.Add(string.Create(CultureInfo.InvariantCulture, $"saga-key-{i:D4}"));

            // Routing is precomputed so the measured region contains only the
            // bucketing, never the traversal both arms share.
            _routedLeaves[i] = GrainId.Create("bench-leaf", (i % LeafCount).ToString(CultureInfo.InvariantCulture));
        }

        _leafResponse = new List<LwwEntry?>(BatchKeyCount);
        for (var i = 0; i < BatchKeyCount; i++)
        {
            _leafResponse.Add(new LwwEntry { Key = _batchKeys[i], Value = [1, 2, 3, 4] });
        }
    }

    // ==================================================================
    // Lane 1: catalog ordering
    // ==================================================================

    /// <summary>
    /// Baseline: the prior LINQ chain - a capturing closure, two filter
    /// delegates, a key-selector delegate, two fused Where iterators, an
    /// OrderedEnumerable, and OrderBy's buffer, key array and index map, all to
    /// serve one page.
    /// </summary>
    [Benchmark(Description = "Catalog ordering: LINQ Where+OrderBy (baseline)")]
    public int CatalogOrdering_Baseline()
    {
        var ordered = _catalogIds
            .Where(id => !id.StartsWith("__sys/", StringComparison.Ordinal))
            .Where(id => _catalogPageToken is null || string.CompareOrdinal(id, _catalogPageToken) > 0)
            .OrderBy(id => id, StringComparer.Ordinal);

        var taken = 0;
        foreach (var id in ordered)
        {
            if (++taken == PageSize)
            {
                break;
            }
        }

        return taken;
    }

    /// <summary>
    /// Shipped: one presized list (the source count is an exact upper bound, per
    /// the capacity-hint rule) filled by an index-based pass, then sorted in
    /// place. One allocation instead of nine, and the same ordering.
    /// </summary>
    [Benchmark(Description = "Catalog ordering: presized list + in-place sort (shipped)")]
    public int CatalogOrdering_Shipped()
    {
        var pageToken = _catalogPageToken;
        var ordered = new List<string>(_catalogIds.Count);
        for (var i = 0; i < _catalogIds.Count; i++)
        {
            var candidate = _catalogIds[i];
            if (candidate.StartsWith("__sys/", StringComparison.Ordinal))
            {
                continue;
            }

            if (pageToken is not null && string.CompareOrdinal(candidate, pageToken) <= 0)
            {
                continue;
            }

            ordered.Add(candidate);
        }

        ordered.Sort(StringComparer.Ordinal);

        var taken = 0;
        foreach (var id in ordered)
        {
            if (++taken == PageSize)
            {
                break;
            }
        }

        return taken;
    }

    /// <summary>
    /// Contrast: the same single-pass shape with the capacity hint omitted, i.e.
    /// the "just let the list grow" alternative. It is kept as an arm because
    /// the repository's capacity-hint rule is empirical, and this is the
    /// measurement behind it - the doubling regrowth reallocates and copies the
    /// backing array log2(n) times, so it allocates roughly twice the shipped
    /// arm on the same input.
    /// </summary>
    [Benchmark(Description = "Catalog ordering: single pass, no capacity hint (contrast)")]
    public int CatalogOrdering_Contrast_NoCapacity()
    {
        var pageToken = _catalogPageToken;
        var ordered = new List<string>();
        for (var i = 0; i < _catalogIds.Count; i++)
        {
            var candidate = _catalogIds[i];
            if (candidate.StartsWith("__sys/", StringComparison.Ordinal))
            {
                continue;
            }

            if (pageToken is not null && string.CompareOrdinal(candidate, pageToken) <= 0)
            {
                continue;
            }

            ordered.Add(candidate);
        }

        ordered.Sort(StringComparer.Ordinal);

        var taken = 0;
        foreach (var id in ordered)
        {
            if (++taken == PageSize)
            {
                break;
            }
        }

        return taken;
    }

    // ==================================================================
    // Lane 2: metrics delta tick
    // ==================================================================

    /// <summary>
    /// Baseline: the prior delta tick. A whole new baseline Dictionary is built
    /// from the current sample on every tick (an O(trees) bucket array plus
    /// entry array, discarded one tick later), and the removed set is produced
    /// by a capturing <c>Where</c> over the baseline keys followed by an
    /// <c>OrderBy(...).ToArray()</c> - which allocates a closure, two iterators
    /// and OrderBy's three arrays purely to yield an empty set on a tick where
    /// nothing was removed.
    /// </summary>
    [Benchmark(Description = "Metrics delta tick: rebuilt baseline + LINQ removed (baseline)")]
    public int MetricsDelta_Baseline()
    {
        var previous = _priorSample;

        var changed = new List<TreeMetrics>(_currentSample.Count);
        foreach (var pair in _currentSample)
        {
            if (!previous.TryGetValue(pair.Key, out var prior) || !SameMetrics(prior, pair.Value))
            {
                changed.Add(pair.Value);
            }
        }

        var removed = previous.Keys
            .Where(id => !_currentSample.ContainsKey(id))
            .OrderBy(id => id, StringComparer.Ordinal)
            .ToArray();

        var ordered = changed.OrderBy(m => m.TreeId, StringComparer.Ordinal).ToArray();

        _priorSample = new Dictionary<string, TreeMetrics>(_currentSample, StringComparer.Ordinal);
        return ordered.Length + removed.Length;
    }

    /// <summary>
    /// Shipped: the baseline map is private to the diff loop and never handed
    /// out, so it is retained and refilled with <c>Clear()</c> (which keeps the
    /// backing arrays); the removed list is built lazily and stays null on the
    /// overwhelmingly common tick where nothing was removed; and the changed
    /// list is sorted in place rather than re-projected through OrderBy.
    /// </summary>
    [Benchmark(Description = "Metrics delta tick: retained baseline + lazy removed (shipped)")]
    public int MetricsDelta_Shipped()
    {
        var previous = _retainedBaseline;

        var changed = new List<TreeMetrics>(_currentSample.Count);
        foreach (var pair in _currentSample)
        {
            if (!previous.TryGetValue(pair.Key, out var prior) || !SameMetrics(prior, pair.Value))
            {
                changed.Add(pair.Value);
            }
        }

        List<string>? removed = null;
        foreach (var id in previous.Keys)
        {
            if (!_currentSample.ContainsKey(id))
            {
                (removed ??= []).Add(id);
            }
        }

        removed?.Sort(StringComparer.Ordinal);
        changed.Sort(TreeMetricsByTreeId.Instance);

        previous.Clear();
        foreach (var pair in _currentSample)
        {
            previous[pair.Key] = pair.Value;
        }

        return changed.Count + (removed?.Count ?? 0);
    }

    /// <summary>
    /// Contrast: retain the baseline map but keep the LINQ removed set. This
    /// splits the lane's win between its two halves and shows the map rebuild is
    /// the dominant term, so the trim is not merely "LINQ is slow".
    /// </summary>
    [Benchmark(Description = "Metrics delta tick: retained baseline + LINQ removed (contrast)")]
    public int MetricsDelta_Contrast_RetainedOnly()
    {
        var previous = _retainedBaseline;

        var changed = new List<TreeMetrics>(_currentSample.Count);
        foreach (var pair in _currentSample)
        {
            if (!previous.TryGetValue(pair.Key, out var prior) || !SameMetrics(prior, pair.Value))
            {
                changed.Add(pair.Value);
            }
        }

        var removed = previous.Keys
            .Where(id => !_currentSample.ContainsKey(id))
            .OrderBy(id => id, StringComparer.Ordinal)
            .ToArray();

        var ordered = changed.OrderBy(m => m.TreeId, StringComparer.Ordinal).ToArray();

        previous.Clear();
        foreach (var pair in _currentSample)
        {
            previous[pair.Key] = pair.Value;
        }

        return ordered.Length + removed.Length;
    }

    // ==================================================================
    // Lane 3: raw batch-read bucketing
    // ==================================================================

    /// <summary>
    /// Baseline: bucket unconditionally into a
    /// <c>Dictionary&lt;GrainId, List&lt;(string, int)&gt;&gt;</c>, then copy
    /// each bucket's keys out into a second, whole-bucket-width
    /// <c>List&lt;string&gt;</c> for the grain call. On the single-leaf shape
    /// this is a dictionary, a tuple list that grows from empty, and a full copy
    /// of the caller's key list - none of which carry any information the
    /// caller's own list did not already have.
    /// </summary>
    [Benchmark(Description = "Raw batch bucketing: eager dictionary + key copy (baseline)")]
    public int RawBatch_Baseline()
    {
        var result = new List<LwwEntry?>(_batchKeys.Count);
        for (var i = 0; i < _batchKeys.Count; i++)
        {
            result.Add(null);
        }

        var leafBuckets = new Dictionary<GrainId, List<(string Key, int Index)>>();
        for (var i = 0; i < _batchKeys.Count; i++)
        {
            var leafId = _routedLeaves[i];
            if (!leafBuckets.TryGetValue(leafId, out var bucket))
            {
                bucket = [];
                leafBuckets[leafId] = bucket;
            }

            bucket.Add((_batchKeys[i], i));
        }

        foreach (var (_, bucket) in leafBuckets)
        {
            var leafKeys = new List<string>(bucket.Count);
            foreach (var (key, _) in bucket)
            {
                leafKeys.Add(key);
            }

            var leafResult = ReadLeaf(leafKeys);
            for (var i = 0; i < bucket.Count; i++)
            {
                var raw = leafResult[i];
                if (raw is null || raw.Value.Value is null)
                {
                    continue;
                }

                result[bucket[i].Index] = raw;
            }
        }

        return result.Count;
    }

    /// <summary>
    /// Shipped: track a sole leaf id and a running count, and only materialise
    /// buckets once a second distinct leaf is actually observed (back-filling
    /// the already-routed prefix, which is known to belong to the first leaf).
    /// The single-leaf shape then hands the caller's own key list straight to
    /// the leaf. Buckets, when they are needed, hold parallel key and index
    /// lists so the per-leaf key copy disappears too.
    /// </summary>
    [Benchmark(Description = "Raw batch bucketing: lazy sole-leaf + parallel buckets (shipped)")]
    public int RawBatch_Shipped()
    {
        var result = new List<LwwEntry?>(_batchKeys.Count);
        for (var i = 0; i < _batchKeys.Count; i++)
        {
            result.Add(null);
        }

        GrainId soleLeafId = default;
        var soleLeafCount = 0;
        Dictionary<GrainId, BenchBucket>? leafBuckets = null;

        for (var i = 0; i < _batchKeys.Count; i++)
        {
            var leafId = _routedLeaves[i];
            if (leafBuckets is null)
            {
                if (i == 0)
                {
                    soleLeafId = leafId;
                    soleLeafCount = 1;
                    continue;
                }

                if (leafId == soleLeafId)
                {
                    soleLeafCount++;
                    continue;
                }

                leafBuckets = new Dictionary<GrainId, BenchBucket>(2);
                var first = new BenchBucket(soleLeafCount);
                for (var j = 0; j < soleLeafCount; j++)
                {
                    first.Add(_batchKeys[j], j);
                }

                leafBuckets[soleLeafId] = first;
            }

            if (!leafBuckets.TryGetValue(leafId, out var bucket))
            {
                bucket = new BenchBucket(4);
                leafBuckets[leafId] = bucket;
            }

            bucket.Add(_batchKeys[i], i);
        }

        if (leafBuckets is null)
        {
            var soleResult = ReadLeaf(_batchKeys);
            for (var i = 0; i < _batchKeys.Count; i++)
            {
                var raw = soleResult[i];
                if (raw is null || raw.Value.Value is null)
                {
                    continue;
                }

                result[i] = raw;
            }

            return result.Count;
        }

        foreach (var (_, bucket) in leafBuckets)
        {
            var leafResult = ReadLeaf(bucket.Keys);
            for (var i = 0; i < bucket.Keys.Count; i++)
            {
                var raw = leafResult[i];
                if (raw is null || raw.Value.Value is null)
                {
                    continue;
                }

                result[bucket.Indices[i]] = raw;
            }
        }

        return result.Count;
    }

    /// <summary>
    /// Contrast: keep the eager dictionary but switch the bucket to the parallel
    /// key/index lists, i.e. drop only the per-leaf key copy. It separates the
    /// two halves of the lane's win and shows that on the single-leaf shape the
    /// laziness, not the bucket layout, is what carries it.
    /// </summary>
    [Benchmark(Description = "Raw batch bucketing: eager dictionary + parallel buckets (contrast)")]
    public int RawBatch_Contrast_EagerParallel()
    {
        var result = new List<LwwEntry?>(_batchKeys.Count);
        for (var i = 0; i < _batchKeys.Count; i++)
        {
            result.Add(null);
        }

        var leafBuckets = new Dictionary<GrainId, BenchBucket>();
        for (var i = 0; i < _batchKeys.Count; i++)
        {
            var leafId = _routedLeaves[i];
            if (!leafBuckets.TryGetValue(leafId, out var bucket))
            {
                bucket = new BenchBucket(4);
                leafBuckets[leafId] = bucket;
            }

            bucket.Add(_batchKeys[i], i);
        }

        foreach (var (_, bucket) in leafBuckets)
        {
            var leafResult = ReadLeaf(bucket.Keys);
            for (var i = 0; i < bucket.Keys.Count; i++)
            {
                var raw = leafResult[i];
                if (raw is null || raw.Value.Value is null)
                {
                    continue;
                }

                result[bucket.Indices[i]] = raw;
            }
        }

        return result.Count;
    }

    // ==================================================================
    // Production anchor
    // ==================================================================

    /// <summary>
    /// End-to-end anchor for lane 1: the real
    /// <c>LatticeStateQuery.ListTreesAsync</c>, paged to exhaustion over a
    /// catalog of <see cref="CatalogSize"/> trees, exercising the shipped
    /// ordering in situ rather than in a copied shell.
    /// </summary>
    /// <remarks>
    /// This is an anchor, not an A/B lane: it has no baseline counterpart,
    /// because the prior ordering no longer exists to run. Its job is to show
    /// the shipped shape is reached by, and correct under, the real query - the
    /// A/B evidence is the shell lanes above. The surface and query are built in
    /// <see cref="Setup"/> so the measured region is the paging walk only.
    /// </remarks>
    [Benchmark(Description = "Catalog paging: real LatticeStateQuery (production anchor)")]
    public async Task<int> Catalog_RealQuery_Shipped()
    {
        var emitted = 0;
        string? token = null;
        do
        {
            var page = await _realQuery.ListTreesAsync(new CatalogRequest { PageToken = token }).ConfigureAwait(false);
            emitted += page.Entries.Count;
            token = page.NextPageToken;
        }
        while (token is not null);

        return emitted;
    }

    /// <summary>
    /// Stands in for the per-leaf grain call. It is a plain lookup so the
    /// measured delta is exactly the bucketing, and it is identical in every
    /// lane-3 arm.
    /// </summary>
    private List<LwwEntry?> ReadLeaf(List<string> keys) =>
        keys.Count == 0 ? [] : _leafResponse;

    /// <summary>
    /// The parallel-list bucket the shipped and contrast arms use, mirroring
    /// <c>ShardRootGrain.LeafKeyBucket</c>.
    /// </summary>
    private sealed class BenchBucket(int capacity)
    {
        internal List<string> Keys { get; } = new(capacity);

        internal List<int> Indices { get; } = new(capacity);

        internal void Add(string key, int index)
        {
            Keys.Add(key);
            Indices.Add(index);
        }
    }

    /// <summary>Cached ordinal comparer, mirroring the shipped observer's.</summary>
    private sealed class TreeMetricsByTreeId : IComparer<TreeMetrics>
    {
        internal static readonly TreeMetricsByTreeId Instance = new();

        public int Compare(TreeMetrics? x, TreeMetrics? y) =>
            string.CompareOrdinal(x?.TreeId, y?.TreeId);
    }

    /// <summary>
    /// The observer's tree-equality test, copied verbatim so both delta arms
    /// perform identical comparison work.
    /// </summary>
    private static bool SameMetrics(TreeMetrics a, TreeMetrics b) =>
        a.Lifecycle == b.Lifecycle
        && a.ShardCount == b.ShardCount
        && a.LiveKeys == b.LiveKeys
        && a.Tombstones == b.Tombstones
        && a.MinDepth == b.MinDepth
        && a.MaxDepth == b.MaxDepth
        && a.ShardsSplitting == b.ShardsSplitting
        && a.ViewCount == b.ViewCount
        && a.ViewLagTotal == b.ViewLagTotal
        && a.ShardHotness.Count == b.ShardHotness.Count;
}
