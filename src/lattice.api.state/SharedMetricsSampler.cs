using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.State;

/// <summary>
/// Shared, reference-counted metric sampler. All metric subscriptions and
/// one-shot polls funnel through this singleton so that N concurrent
/// dashboard clients watching the same scope do not multiply the underlying
/// sampling load: one sampling loop per distinct request signature produces a
/// full per-tree aggregate map each tick and fans it out to every attached
/// subscriber. Server cost is therefore O(trees + shards) per tick, never
/// O(clients x mutations).
/// </summary>
/// <remarks>
/// The sampler emits full aggregate maps; per-subscriber delta encoding is a
/// cheap local diff done by <see cref="LatticeStateMetricsObserver"/>. Because
/// a late joiner simply attaches to the next shared tick, sharing introduces
/// no staleness beyond the sampling cadence the feed already documents.
/// </remarks>
internal sealed class SharedMetricsSampler(
    ILatticeStateQuery query,
    IOptions<LatticeApiStateOptions> apiOptions,
    IServiceProvider services)
{
    private const int CatalogPageSize = 200;

    // Above this many explicitly-requested tree ids the per-tick de-duplication
    // switches from a linear ordinal scan to a hash set. A dashboard scope names
    // a handful of trees, so the linear path is the common one.
    private const int SmallTreeIdRequestThreshold = 16;

    private readonly ILatticeStateQuery _query = query
        ?? throw new ArgumentNullException(nameof(query));

    private readonly LatticeApiStateOptions _apiOptions = (apiOptions
        ?? throw new ArgumentNullException(nameof(apiOptions))).Value;

    // Resolves the caller subject so the shared sampling loops are keyed by
    // visibility, never coalescing subscribers with different read access onto a
    // single loop (issue #971). Constructed exactly as LatticeStateQuery does:
    // when no real access gate is registered (or the host opted out) it reports
    // Enabled == false and resolves no subject, so signatures stay identity-free
    // and coalescing behaves byte-for-byte as before, at zero cost.
    private readonly LatticeStateVisibilityFilter _visibility = new(
        services ?? throw new ArgumentNullException(nameof(services)),
        (apiOptions ?? throw new ArgumentNullException(nameof(apiOptions))).Value);

    // Best-effort: a host that did not register the core lattice services (for
    // example a transport-only test harness) resolves null here and simply
    // never pauses detail sampling. Mirrors the opt-in semantics the snapshot
    // admission control already relies on.
    private readonly IWalSaturationSignal? _saturationSignal = (services
        ?? throw new ArgumentNullException(nameof(services))).GetService<IWalSaturationSignal>();

    private readonly object _gate = new();
    private readonly Dictionary<string, SamplerLoop> _loops = new(StringComparer.Ordinal);

    private long _totalSampleCount;

    /// <summary>
    /// Total number of shared sampling passes performed across all loops. A
    /// test probe used to prove coalescing: with N subscribers on one
    /// signature this grows once per tick, not once per subscriber per tick.
    /// </summary>
    public long TotalSampleCount => Interlocked.Read(ref _totalSampleCount);

    /// <summary>Number of distinct sampling loops currently running. A test probe.</summary>
    public int ActiveSamplerCount
    {
        get { lock (_gate) { return _loops.Count; } }
    }

    /// <summary>Performs one ad-hoc full sample, bypassing the shared loops.</summary>
    public Task<Dictionary<string, TreeMetrics>> SampleOnceAsync(
        TreeMetricsRequest request,
        CancellationToken cancellationToken)
        => SampleAllAsync(request, cancellationToken);

    /// <summary>
    /// Attaches to the shared sampling loop for the request's signature,
    /// yielding the full per-tree aggregate map on each tick until the caller
    /// cancels. The loop is started on first attach and torn down when the last
    /// subscriber detaches.
    /// </summary>
    public async IAsyncEnumerable<IReadOnlyDictionary<string, TreeMetrics>> SubscribeAsync(
        TreeMetricsRequest request,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var interval = request.SampleInterval ?? _apiOptions.MetricsSampleInterval;
        if (interval <= TimeSpan.Zero)
        {
            interval = TimeSpan.FromSeconds(1);
        }

        // Resolve the caller subject so the shared loop is keyed by visibility.
        // The loop captures the first subscriber's ambient credential and samples
        // the per-tree map filtered to that identity, then fans the SAME map to
        // every subscriber on the signature; keying the signature by the resolved
        // subject guarantees every co-attached subscriber has identical read
        // access, so a lower-privilege subscriber can never receive metrics for a
        // tree it cannot read. When visibility is disabled (no auth gate, or the
        // host opted out) the subject is null and the signature is identity-free,
        // so coalescing is unchanged and costs nothing.
        var subject = await _visibility.ResolveSubjectAsync(cancellationToken).ConfigureAwait(false);

        var signature = BuildSignature(request, interval, subject);
        var subscriber = Attach(signature, request, interval, out var loop);
        try
        {
            var reader = subscriber.Channel.Reader;
            while (await reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                while (reader.TryRead(out var map))
                {
                    yield return map;
                }
            }
        }
        finally
        {
            Detach(signature, loop, subscriber);
        }
    }

    private Subscriber Attach(
        string signature,
        TreeMetricsRequest request,
        TimeSpan interval,
        out SamplerLoop loop)
    {
        // A capacity-1, drop-oldest channel keeps a slow subscriber from
        // back-pressuring the shared loop or its peers: it always sees the
        // newest sample, never an unbounded backlog.
        var channel = Channel.CreateBounded<IReadOnlyDictionary<string, TreeMetrics>>(
            new BoundedChannelOptions(1)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true,
                SingleWriter = false,
            });

        var subscriber = new Subscriber(channel);
        lock (_gate)
        {
            if (!_loops.TryGetValue(signature, out loop!))
            {
                var createdLoop = new SamplerLoop(request, interval);
                loop = createdLoop;
                _loops[signature] = createdLoop;
                createdLoop.Task = Task.Run(() => RunLoopAsync(createdLoop));
            }

            loop.Subscribers.Add(subscriber);
            loop.RefreshSnapshot();
        }

        return subscriber;
    }

    private void Detach(string signature, SamplerLoop loop, Subscriber subscriber)
    {
        lock (_gate)
        {
            loop.Subscribers.Remove(subscriber);
            loop.RefreshSnapshot();
            subscriber.Channel.Writer.TryComplete();

            if (loop.Subscribers.Count == 0 && _loops.TryGetValue(signature, out var current) && ReferenceEquals(current, loop))
            {
                _loops.Remove(signature);
                loop.Cancellation.Cancel();
            }
        }
    }

    private async Task RunLoopAsync(SamplerLoop loop)
    {
        var token = loop.Cancellation.Token;
        try
        {
            while (!token.IsCancellationRequested)
            {
                Dictionary<string, TreeMetrics> map;
                try
                {
                    map = await SampleAllAsync(loop.Request, token).ConfigureAwait(false);
                    Interlocked.Increment(ref _totalSampleCount);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch
                {
                    // A transient sampling failure must not kill the shared loop
                    // and disconnect every subscriber; skip this tick.
                    await DelayAsync(loop.Interval, token).ConfigureAwait(false);
                    continue;
                }

                // Lock-free, allocation-free in steady state: the snapshot is
                // only rebuilt when subscriber membership changes.
                foreach (var subscriber in loop.CurrentSubscribers)
                {
                    subscriber.Channel.Writer.TryWrite(map);
                }

                await DelayAsync(loop.Interval, token).ConfigureAwait(false);
            }
        }
        finally
        {
            lock (_gate)
            {
                foreach (var subscriber in loop.Subscribers)
                {
                    subscriber.Channel.Writer.TryComplete();
                }

                loop.Subscribers.Clear();
                loop.RefreshSnapshot();
            }
        }
    }

    private async Task<Dictionary<string, TreeMetrics>> SampleAllAsync(
        TreeMetricsRequest request,
        CancellationToken cancellationToken)
    {
        var treeIds = request.TreeIds is { Count: > 0 } requested
            ? DistinctOrdinal(requested)
            : await EnumerateTreeIdsAsync(request.IncludeSystemTrees, cancellationToken).ConfigureAwait(false);

        var viewLag = request.IncludeViewLag
            ? await SampleViewLagAsync(request.IncludeSystemTrees, cancellationToken).ConfigureAwait(false)
            : null;

        var result = new Dictionary<string, TreeMetrics>(StringComparer.Ordinal);
        foreach (var treeId in treeIds)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var (viewCount, viewLagTotal) = ResolveViewLag(viewLag, treeId);

            // A saturated tree's shard roots are already contended by the write
            // backlog; a fresh per-shard diagnostics walk would only pile on. Skip
            // it and serve a degraded view: registry-sourced lifecycle + shard
            // count, live counts paused, hotness empty. The detail returns
            // automatically once the tree settles.
            if (_saturationSignal?.GetCurrentState(treeId) == WalSaturationState.Saturated)
            {
                var shardCount = await _query.GetPhysicalShardCountAsync(treeId, cancellationToken).ConfigureAwait(false);
                if (shardCount is null)
                {
                    continue;
                }

                result[treeId] = new TreeMetrics
                {
                    TreeId = treeId,
                    Lifecycle = TreeLifecycleState.Active,
                    ShardCount = shardCount.Value,
                    DetailPaused = true,
                    ViewCount = viewCount,
                    ViewLagTotal = viewLagTotal,
                    ShardHotness = Array.Empty<ShardHotness>(),
                };
                continue;
            }

            // A single deep per-shard diagnostics fan-out backs both the tile
            // aggregates and the per-shard hotness rows, so the metrics sample
            // walks each shard once, not twice.
            var shards = await _query.GetShardSummariesAsync(treeId, deep: true, cancellationToken).ConfigureAwait(false);
            if (shards.Status != StateQueryStatus.Found)
            {
                continue;
            }

            result[treeId] = BuildTreeMetrics(
                treeId,
                shards.Shards,
                request.IncludeShardHotness,
                viewCount,
                viewLagTotal);
        }

        return result;
    }

    /// <summary>
    /// De-duplicates <paramref name="source"/> ordinally, preserving first-seen
    /// order, exactly as <c>Distinct(StringComparer.Ordinal)</c> does - but as a
    /// direct fold into a presized list rather than a deferred LINQ iterator, so
    /// the per-tick sample no longer allocates the iterator, its internal set and
    /// a list grown from empty. An explicit tree-id list is short in practice
    /// (a dashboard watches a handful of trees), so below the threshold a linear
    /// ordinal scan of the survivors is cheaper than hashing; the set is kept for
    /// larger requests so a pathological list cannot go quadratic.
    /// </summary>
    private static List<string> DistinctOrdinal(IReadOnlyList<string> source)
    {
        // At most one survivor per input, so the capacity is a tight upper bound
        // and a short request presizes short.
        var distinct = new List<string>(source.Count);

        if (source.Count <= SmallTreeIdRequestThreshold)
        {
            for (var i = 0; i < source.Count; i++)
            {
                var id = source[i];
                var seen = false;
                for (var j = 0; j < distinct.Count; j++)
                {
                    if (string.Equals(distinct[j], id, StringComparison.Ordinal))
                    {
                        seen = true;
                        break;
                    }
                }

                if (!seen)
                {
                    distinct.Add(id);
                }
            }

            return distinct;
        }

        var seenIds = new HashSet<string>(source.Count, StringComparer.Ordinal);
        for (var i = 0; i < source.Count; i++)
        {
            if (seenIds.Add(source[i]))
            {
                distinct.Add(source[i]);
            }
        }

        return distinct;
    }

    private static (int? ViewCount, long? ViewLagTotal) ResolveViewLag(
        Dictionary<string, ViewRollup>? viewLag,
        string treeId)
    {
        if (viewLag is null)
        {
            return (null, null);
        }

        // One probe, not two: both halves of the answer come from the same
        // rollup, and a missing tree yields the same (0, null) the two separate
        // lookups produced from a defaulted ViewRollup.
        return viewLag.TryGetValue(treeId, out var rollup)
            ? (rollup.Count, rollup.LagTotal)
            : (0, null);
    }

    private static TreeMetrics BuildTreeMetrics(
        string treeId,
        IReadOnlyList<ShardStateSummary> shards,
        bool includeHotness,
        int? viewCount,
        long? viewLagTotal)
    {
        long liveKeys = 0;
        long tombstones = 0;
        var minDepth = int.MaxValue;
        var maxDepth = 0;
        var splitting = 0;

        var hotness = includeHotness && shards.Count > 0
            ? new ShardHotness[shards.Count]
            : Array.Empty<ShardHotness>();

        for (var i = 0; i < shards.Count; i++)
        {
            var shard = shards[i];
            liveKeys += shard.LiveKeys;
            tombstones += shard.Tombstones;
            if (shard.Depth < minDepth) minDepth = shard.Depth;
            if (shard.Depth > maxDepth) maxDepth = shard.Depth;
            if (shard.SplitInProgress) splitting++;

            if (includeHotness && hotness.Length > 0)
            {
                hotness[i] = new ShardHotness
                {
                    ShardIndex = shard.ShardIndex,
                    OpsPerSecond = shard.OpsPerSecond,
                    LiveKeys = shard.LiveKeys,
                    SplitInProgress = shard.SplitInProgress,
                };
            }
        }

        return new TreeMetrics
        {
            TreeId = treeId,
            Lifecycle = TreeLifecycleState.Active,
            ShardCount = shards.Count,
            LiveKeys = liveKeys,
            Tombstones = tombstones,
            MinDepth = minDepth == int.MaxValue ? 0 : minDepth,
            MaxDepth = maxDepth,
            ShardsSplitting = splitting,
            ViewCount = viewCount,
            ViewLagTotal = viewLagTotal,
            ShardHotness = hotness,
        };
    }

    private async Task<List<string>> EnumerateTreeIdsAsync(bool includeSystemTrees, CancellationToken cancellationToken)
    {
        var ids = new List<string>();
        string? pageToken = null;
        do
        {
            var page = await _query.ListTreesAsync(
                new CatalogRequest
                {
                    PageSize = CatalogPageSize,
                    PageToken = pageToken,
                    IncludeSystemTrees = includeSystemTrees,
                },
                cancellationToken).ConfigureAwait(false);

            foreach (var entry in page.Entries)
            {
                ids.Add(entry.TreeId);
            }

            pageToken = page.NextPageToken;
        }
        while (!string.IsNullOrEmpty(pageToken));

        return ids;
    }

    private async Task<Dictionary<string, ViewRollup>> SampleViewLagAsync(
        bool includeSystemTrees,
        CancellationToken cancellationToken)
    {
        var rollups = new Dictionary<string, ViewRollup>(StringComparer.Ordinal);
        string? pageToken = null;
        do
        {
            var page = await _query.ListViewsAsync(
                new CatalogRequest
                {
                    PageSize = CatalogPageSize,
                    PageToken = pageToken,
                    IncludeSystemTrees = includeSystemTrees,
                    IncludeViewStats = true,
                },
                cancellationToken).ConfigureAwait(false);

            foreach (var view in page.Entries)
            {
                // Single probe per view rather than a TryGetValue followed by an
                // indexer set. A key absent from the map lands on a defaulted
                // ViewRollup - (0, null) - which is exactly what the failed
                // TryGetValue used to hand back, so the fold is unchanged.
                ref var rollup = ref CollectionsMarshal.GetValueRefOrAddDefault(
                    rollups, view.SourceTreeId, out _);
                rollup = new ViewRollup(
                    rollup.Count + 1,
                    view.Lag is { } lag ? (rollup.LagTotal ?? 0) + lag : rollup.LagTotal);
            }

            pageToken = page.NextPageToken;
        }
        while (!string.IsNullOrEmpty(pageToken));

        return rollups;
    }

    private static string BuildSignature(TreeMetricsRequest request, TimeSpan interval, LatticeSubject? subject)
    {
        string ids;
        if (request.TreeIds is { Count: > 0 } treeIds)
        {
            // Sort the de-duplicated ids in place rather than via OrderBy's
            // deferred iterator, so the signature is canonical (order-insensitive)
            // without the extra LINQ sort-buffer allocation.
            var distinct = treeIds.Distinct(StringComparer.Ordinal).ToArray();
            Array.Sort(distinct, StringComparer.Ordinal);
            ids = string.Join(',', distinct);
        }
        else
        {
            ids = "*";
        }

        var shape = string.Create(
            System.Globalization.CultureInfo.InvariantCulture,
            $"{ids}|h={(request.IncludeShardHotness ? 1 : 0)}|v={(request.IncludeViewLag ? 1 : 0)}|s={(request.IncludeSystemTrees ? 1 : 0)}|i={interval.Ticks}");

        var identity = BuildIdentityComponent(subject);
        return identity.Length == 0 ? shape : string.Concat(shape, "|id=", identity);
    }

    /// <summary>
    /// Canonical, order-insensitive rendering of the visibility-determining
    /// identity of the caller <paramref name="subject"/> - its stable id, the full
    /// transitively-expanded group closure, and the claim bag the access gate
    /// authorizes over. Two subscribers share a sampling loop only when this
    /// matches, so the first subscriber's identity-filtered map is a correct view
    /// for every subscriber attached to that loop. Empty when
    /// <paramref name="subject"/> is <see langword="null"/> (visibility disabled),
    /// preserving the identity-free legacy signature and its coalescing.
    /// </summary>
    private static string BuildIdentityComponent(LatticeSubject? subject)
    {
        if (subject is not { } resolved)
        {
            return string.Empty;
        }

        // Length-prefix every caller- and identity-provider-supplied field so the
        // signature is injective: no subject id, group id, or claim key/value can
        // contain a delimiter that shifts a field boundary or lets two distinct
        // identities render the same string. Joining groups with a bare ',' would
        // alias groups ["a,b"] with ["a","b"] (both "a,b"), and joining a claim as
        // "key=value" would alias {"a=b":"c"} with {"a":"b=c"} (both "a=b=c"),
        // wrongly coalescing distinct subjects onto one sampling loop and breaking
        // the identical-signature-implies-identical-read-access guarantee (#971).
        // This mirrors the length-prefix framing GrainIndexFingerprint.FeedString
        // and AggregationLatticeViewProjection apply to the same aliasing class.
        var builder = new StringBuilder();
        AppendLengthPrefixed(builder, resolved.SubjectId);

        builder.Append("|g=");
        if (resolved.GroupIds.Count > 0)
        {
            var groups = resolved.GroupIds.ToArray();
            Array.Sort(groups, StringComparer.Ordinal);
            foreach (var group in groups)
            {
                AppendLengthPrefixed(builder, group);
            }
        }

        builder.Append("|c=");
        if (resolved.Claims is { Count: > 0 } claims)
        {
            var pairs = new KeyValuePair<string, string>[claims.Count];
            var i = 0;
            foreach (var claim in claims)
            {
                pairs[i++] = claim;
            }

            Array.Sort(pairs, static (left, right) =>
            {
                var keyOrder = string.CompareOrdinal(left.Key, right.Key);
                return keyOrder != 0 ? keyOrder : string.CompareOrdinal(left.Value, right.Value);
            });

            foreach (var pair in pairs)
            {
                AppendLengthPrefixed(builder, pair.Key);
                AppendLengthPrefixed(builder, pair.Value);
            }
        }

        return builder.ToString();
    }

    /// <summary>
    /// Appends <paramref name="value"/> to <paramref name="builder"/> as its
    /// length in UTF-16 code units, a <c>:</c> separator, then the value itself,
    /// so a concatenation of such segments is injective: the length frames each
    /// field exactly, and no delimiter a value happens to contain can shift a
    /// field boundary or alias two distinct field sequences to the same string.
    /// </summary>
    private static void AppendLengthPrefixed(StringBuilder builder, string value)
        => builder.Append(value.Length).Append(':').Append(value);

    private static async Task DelayAsync(TimeSpan interval, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(interval, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
    }

    private readonly record struct ViewRollup(int Count, long? LagTotal);

    private sealed class Subscriber(Channel<IReadOnlyDictionary<string, TreeMetrics>> channel)
    {
        public Channel<IReadOnlyDictionary<string, TreeMetrics>> Channel { get; } = channel;
    }

    private sealed class SamplerLoop(TreeMetricsRequest request, TimeSpan interval)
    {
        private Subscriber[] _snapshot = Array.Empty<Subscriber>();

        public TreeMetricsRequest Request { get; } = request;

        public TimeSpan Interval { get; } = interval;

        public List<Subscriber> Subscribers { get; } = new();

        public CancellationTokenSource Cancellation { get; } = new();

        public Task? Task { get; set; }

        /// <summary>
        /// A copy-on-write snapshot of <see cref="Subscribers"/>, rebuilt under
        /// the sampler gate whenever membership changes. The per-tick fan-out
        /// reads it lock-free (see <see cref="CurrentSubscribers"/>), so the
        /// steady-state sampling loop neither locks nor allocates per tick.
        /// </summary>
        public Subscriber[] CurrentSubscribers => Volatile.Read(ref _snapshot);

        /// <summary>Rebuilds the lock-free snapshot. Must be called under the gate.</summary>
        public void RefreshSnapshot() => Volatile.Write(ref _snapshot, Subscribers.ToArray());
    }
}
