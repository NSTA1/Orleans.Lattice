using System.Runtime.CompilerServices;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.State;

/// <summary>
/// Default <see cref="ILatticeStateMetricsObserver"/> implementation. On each
/// tick it samples the read facade's already-maintained aggregates - the
/// per-tree structural / stats summary, optional per-shard hotness, and
/// optional materialised-view lag - assembling one <see cref="TreeMetrics"/>
/// per visible tree, then delta-encodes successive ticks at tree granularity.
/// Registered as a silo singleton by <c>AddLatticeStateApi</c>.
/// </summary>
/// <remarks>
/// The feed sources strictly from the structural digest and the existing
/// metrics surface (never from any per-mutation tracking), so its cost is
/// O(trees + shards) on a timer and a foreground writer pays nothing for an
/// active subscription. Cancellation ends the sampling loop with no residual
/// timer.
/// </remarks>
internal sealed class LatticeStateMetricsObserver(
    ILatticeStateQuery query,
    IOptions<LatticeApiStateOptions> apiOptions) : ILatticeStateMetricsObserver
{
    private const int CatalogPageSize = 200;

    private readonly ILatticeStateQuery _query = query
        ?? throw new ArgumentNullException(nameof(query));

    private readonly LatticeApiStateOptions _apiOptions = (apiOptions
        ?? throw new ArgumentNullException(nameof(apiOptions))).Value;

    /// <inheritdoc />
    public async Task<TreeMetricsSnapshot> SampleAsync(
        TreeMetricsRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var current = await SampleAllAsync(request, cancellationToken).ConfigureAwait(false);
        return new TreeMetricsSnapshot
        {
            SampledAt = DateTimeOffset.UtcNow,
            IsInitial = true,
            Trees = Ordered(current.Values),
        };
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<TreeMetricsSnapshot> ObserveAsync(
        TreeMetricsRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var interval = request.SampleInterval ?? _apiOptions.MetricsSampleInterval;
        if (interval <= TimeSpan.Zero)
        {
            interval = TimeSpan.FromSeconds(1);
        }

        Dictionary<string, TreeMetrics>? previous = null;

        while (!cancellationToken.IsCancellationRequested)
        {
            var current = await SampleAllAsync(request, cancellationToken).ConfigureAwait(false);

            if (previous is null)
            {
                yield return new TreeMetricsSnapshot
                {
                    SampledAt = DateTimeOffset.UtcNow,
                    IsInitial = true,
                    Trees = Ordered(current.Values),
                };
            }
            else
            {
                var changed = new List<TreeMetrics>();
                foreach (var pair in current)
                {
                    if (!previous.TryGetValue(pair.Key, out var prior) || !SameMetrics(prior, pair.Value))
                    {
                        changed.Add(pair.Value);
                    }
                }

                var removed = previous.Keys.Where(id => !current.ContainsKey(id)).ToList();

                yield return new TreeMetricsSnapshot
                {
                    SampledAt = DateTimeOffset.UtcNow,
                    IsInitial = false,
                    Trees = Ordered(changed),
                    RemovedTreeIds = removed.OrderBy(static id => id, StringComparer.Ordinal).ToArray(),
                };
            }

            previous = current;
            await DelayAsync(interval, cancellationToken).ConfigureAwait(false);
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
                // The tree vanished between enumeration and sampling; treat it
                // as absent so it surfaces as a removal on the next delta tick.
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

    private static IReadOnlyList<TreeMetrics> Ordered(IEnumerable<TreeMetrics> metrics)
        => metrics.OrderBy(static m => m.TreeId, StringComparer.Ordinal).ToArray();

    private static bool SameMetrics(TreeMetrics a, TreeMetrics b)
    {
        if (a.Lifecycle != b.Lifecycle
            || a.ShardCount != b.ShardCount
            || a.LiveKeys != b.LiveKeys
            || a.Tombstones != b.Tombstones
            || a.MinDepth != b.MinDepth
            || a.MaxDepth != b.MaxDepth
            || a.ShardsSplitting != b.ShardsSplitting
            || a.ViewCount != b.ViewCount
            || a.ViewLagTotal != b.ViewLagTotal
            || a.ShardHotness.Count != b.ShardHotness.Count)
        {
            return false;
        }

        for (var i = 0; i < a.ShardHotness.Count; i++)
        {
            if (a.ShardHotness[i] != b.ShardHotness[i])
            {
                return false;
            }
        }

        return true;
    }

    private static async Task DelayAsync(TimeSpan interval, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(interval, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Cancellation tears the sampling loop down cleanly.
        }
    }

    private readonly record struct ViewRollup(int Count, long? LagTotal);
}
