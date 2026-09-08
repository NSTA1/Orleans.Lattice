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
            Trees = Ordered(current),
        };
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<TreeMetricsSnapshot> ObserveAsync(
        TreeMetricsRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        // The previous tick's map is a private diff baseline that is never handed
        // out, so it is retained and refilled in place rather than reallocated.
        // The prior shape built a whole new Dictionary from `current` on EVERY
        // tick, which is an O(trees) bucket + entry array allocation per
        // subscriber per tick, discarded a tick later - the single largest cost
        // of an otherwise idle delta feed. Clear() keeps the backing arrays, so
        // after the first tick the baseline refill allocates nothing at all.
        Dictionary<string, TreeMetrics>? previous = null;

        await foreach (var current in _sampler.SubscribeAsync(request, cancellationToken).ConfigureAwait(false))
        {
            if (previous is null)
            {
                yield return new TreeMetricsSnapshot
                {
                    SampledAt = DateTimeOffset.UtcNow,
                    IsInitial = true,
                    Trees = Ordered(current),
                };

                previous = new Dictionary<string, TreeMetrics>(current.Count, StringComparer.Ordinal);
            }
            else
            {
                // The changed set is bounded by the current sample's tree count;
                // presizing to it removes the list's grow-from-empty regrowth on
                // every delta tick.
                var changed = new List<TreeMetrics>(current.Count);
                foreach (var pair in current)
                {
                    if (!previous.TryGetValue(pair.Key, out var prior) || !SameMetrics(prior, pair.Value))
                    {
                        changed.Add(pair.Value);
                    }
                }

                // A tree disappearing is rare, so the removed list is created
                // lazily and the steady-state tick allocates nothing for it. The
                // prior shape ran a capturing Where over previous.Keys and then an
                // OrderBy, which allocated a closure, two iterators and OrderBy's
                // buffer/key/map arrays on every tick just to yield an empty set.
                List<string>? removed = null;
                foreach (var id in previous.Keys)
                {
                    if (!current.ContainsKey(id))
                    {
                        (removed ??= []).Add(id);
                    }
                }

                removed?.Sort(StringComparer.Ordinal);

                yield return new TreeMetricsSnapshot
                {
                    SampledAt = DateTimeOffset.UtcNow,
                    IsInitial = false,
                    Trees = Ordered(changed),
                    RemovedTreeIds = removed ?? (IReadOnlyList<string>)Array.Empty<string>(),
                };
            }

            previous.Clear();
            foreach (var pair in current)
            {
                previous[pair.Key] = pair.Value;
            }
        }
    }

    /// <summary>
    /// Orders a whole sample map by tree id into one exact-width array.
    /// </summary>
    /// <remarks>
    /// <c>OrderBy(...).ToArray()</c> buffers its source, materialises a parallel
    /// key array and an index map, sorts the map, and then projects a third array
    /// out of it. Copying the values straight into one exact-width array and
    /// sorting that array in place produces the identical ordering for one
    /// allocation instead of four.
    /// </remarks>
    private static IReadOnlyList<TreeMetrics> Ordered(IReadOnlyDictionary<string, TreeMetrics> metrics)
    {
        if (metrics.Count == 0)
        {
            return Array.Empty<TreeMetrics>();
        }

        var ordered = new TreeMetrics[metrics.Count];
        var next = 0;
        foreach (var value in metrics.Values)
        {
            ordered[next++] = value;
        }

        Array.Sort(ordered, TreeMetricsByTreeId.Instance);
        return ordered;
    }

    /// <summary>
    /// Orders an already-materialised delta list by tree id, in place. The list is
    /// built here and handed straight out, so sorting it costs nothing beyond the
    /// list that already exists.
    /// </summary>
    private static IReadOnlyList<TreeMetrics> Ordered(List<TreeMetrics> metrics)
    {
        metrics.Sort(TreeMetricsByTreeId.Instance);
        return metrics;
    }

    /// <summary>
    /// Ordinal comparison of two <see cref="TreeMetrics"/> by
    /// <see cref="TreeMetrics.TreeId"/>. A single cached instance replaces the
    /// per-call key selector delegate the LINQ ordering allocated.
    /// </summary>
    private sealed class TreeMetricsByTreeId : IComparer<TreeMetrics>
    {
        internal static readonly TreeMetricsByTreeId Instance = new();

        public int Compare(TreeMetrics? x, TreeMetrics? y) =>
            string.CompareOrdinal(x?.TreeId, y?.TreeId);
    }

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
