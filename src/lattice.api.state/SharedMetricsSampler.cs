using System.Threading.Channels;
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
    IOptions<LatticeApiStateOptions> apiOptions)
{
    private const int CatalogPageSize = 200;

    private readonly ILatticeStateQuery _query = query
        ?? throw new ArgumentNullException(nameof(query));

    private readonly LatticeApiStateOptions _apiOptions = (apiOptions
        ?? throw new ArgumentNullException(nameof(apiOptions))).Value;

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

        var signature = BuildSignature(request, interval);
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
        }

        return subscriber;
    }

    private void Detach(string signature, SamplerLoop loop, Subscriber subscriber)
    {
        lock (_gate)
        {
            loop.Subscribers.Remove(subscriber);
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

                Subscriber[] subscribers;
                lock (_gate)
                {
                    subscribers = loop.Subscribers.ToArray();
                }

                foreach (var subscriber in subscribers)
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
            }
        }
    }

    private async Task<Dictionary<string, TreeMetrics>> SampleAllAsync(
        TreeMetricsRequest request,
        CancellationToken cancellationToken)
    {
        var treeIds = request.TreeIds is { Count: > 0 }
            ? request.TreeIds.Distinct(StringComparer.Ordinal).ToList()
            : await EnumerateTreeIdsAsync(request.IncludeSystemTrees, cancellationToken).ConfigureAwait(false);

        var viewLag = request.IncludeViewLag
            ? await SampleViewLagAsync(request.IncludeSystemTrees, cancellationToken).ConfigureAwait(false)
            : null;

        var result = new Dictionary<string, TreeMetrics>(StringComparer.Ordinal);
        foreach (var treeId in treeIds)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var summary = await _query.GetTreeSummaryAsync(treeId, deep: true, cancellationToken).ConfigureAwait(false);
            if (summary.Status != StateQueryStatus.Found || summary.Summary is null)
            {
                continue;
            }

            var hotness = request.IncludeShardHotness
                ? await SampleHotnessAsync(treeId, cancellationToken).ConfigureAwait(false)
                : Array.Empty<ShardHotness>();

            int? viewCount = null;
            long? viewLagTotal = null;
            if (viewLag is not null)
            {
                viewCount = viewLag.TryGetValue(treeId, out var rollup) ? rollup.Count : 0;
                viewLagTotal = viewLag.TryGetValue(treeId, out var lag) ? lag.LagTotal : null;
            }

            var state = summary.Summary;
            result[treeId] = new TreeMetrics
            {
                TreeId = treeId,
                Lifecycle = state.Lifecycle,
                ShardCount = state.ShardCount,
                LiveKeys = state.TotalLiveKeys,
                Tombstones = state.TombstoneCount,
                MinDepth = state.MinDepth,
                MaxDepth = state.MaxDepth,
                ShardsSplitting = state.ShardsSplitting,
                ViewCount = viewCount,
                ViewLagTotal = viewLagTotal,
                ShardHotness = hotness,
            };
        }

        return result;
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
                rollups.TryGetValue(view.SourceTreeId, out var current);
                rollups[view.SourceTreeId] = new ViewRollup(
                    current.Count + 1,
                    view.Lag is { } lag ? (current.LagTotal ?? 0) + lag : current.LagTotal);
            }

            pageToken = page.NextPageToken;
        }
        while (!string.IsNullOrEmpty(pageToken));

        return rollups;
    }

    private async Task<IReadOnlyList<ShardHotness>> SampleHotnessAsync(string treeId, CancellationToken cancellationToken)
    {
        var shards = await _query.GetShardSummariesAsync(treeId, deep: false, cancellationToken).ConfigureAwait(false);
        if (shards.Status != StateQueryStatus.Found || shards.Shards.Count == 0)
        {
            return Array.Empty<ShardHotness>();
        }

        var hotness = new ShardHotness[shards.Shards.Count];
        for (var i = 0; i < shards.Shards.Count; i++)
        {
            var shard = shards.Shards[i];
            hotness[i] = new ShardHotness
            {
                ShardIndex = shard.ShardIndex,
                OpsPerSecond = shard.OpsPerSecond,
                LiveKeys = shard.LiveKeys,
                SplitInProgress = shard.SplitInProgress,
            };
        }

        return hotness;
    }

    private static string BuildSignature(TreeMetricsRequest request, TimeSpan interval)
    {
        var ids = request.TreeIds is { Count: > 0 }
            ? string.Join(',', request.TreeIds.Distinct(StringComparer.Ordinal).OrderBy(static x => x, StringComparer.Ordinal))
            : "*";

        return string.Create(
            System.Globalization.CultureInfo.InvariantCulture,
            $"{ids}|h={(request.IncludeShardHotness ? 1 : 0)}|v={(request.IncludeViewLag ? 1 : 0)}|s={(request.IncludeSystemTrees ? 1 : 0)}|i={interval.Ticks}");
    }

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
        public TreeMetricsRequest Request { get; } = request;

        public TimeSpan Interval { get; } = interval;

        public List<Subscriber> Subscribers { get; } = new();

        public CancellationTokenSource Cancellation { get; } = new();

        public Task? Task { get; set; }
    }
}
