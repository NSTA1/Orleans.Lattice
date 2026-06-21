using System.Runtime.CompilerServices;

namespace Orleans.Lattice.Api.State;

/// <summary>
/// Default <see cref="ILatticeStateMetricsObserver"/> implementation. Delegates
/// the expensive per-tree aggregate sampling to the shared
/// <see cref="SharedMetricsSampler"/> (so concurrent subscribers coalesce onto
/// one sampling loop) and performs only the cheap, per-subscriber delta diff
/// locally. Registered as a silo singleton by <c>AddLatticeStateApi</c>.
/// </summary>
/// <remarks>
/// Sourcing strictly from already-maintained aggregates (never per-mutation
/// tracking) and coalescing the sampling means the feed's cost is
/// O(trees + shards) per tick regardless of subscriber count, and a foreground
/// writer pays nothing for an active subscription. Cancellation ends the local
/// diff loop and detaches from the shared sampler with no residual timer.
/// </remarks>
internal sealed class LatticeStateMetricsObserver(SharedMetricsSampler sampler)
    : ILatticeStateMetricsObserver
{
    private readonly SharedMetricsSampler _sampler = sampler
        ?? throw new ArgumentNullException(nameof(sampler));

    /// <inheritdoc />
    public async Task<TreeMetricsSnapshot> SampleAsync(
        TreeMetricsRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var current = await _sampler.SampleOnceAsync(request, cancellationToken).ConfigureAwait(false);
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

        Dictionary<string, TreeMetrics>? previous = null;

        await foreach (var current in _sampler.SubscribeAsync(request, cancellationToken).ConfigureAwait(false))
        {
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

            previous = new Dictionary<string, TreeMetrics>(current, StringComparer.Ordinal);
        }
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
}
