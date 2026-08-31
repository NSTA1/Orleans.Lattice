using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace Orleans.Lattice.GrainIndex.Observability;

/// <summary>
/// The per-silo store of backfill progress the grain-index observable gauges
/// read: one frozen sample per index whose crawl this silo hosts.
/// </summary>
/// <remarks>
/// <para>
/// The gauges cannot read the durable checkpoint on the scrape path - that
/// would put a tree read behind every metrics scrape, on a schedule the
/// application does not control. Instead the backfill grain publishes its
/// status here whenever it changes (once per pass at most), and each gauge
/// callback returns the pre-built measurement array for that publication. A
/// scrape therefore recomputes nothing, allocates no measurement, and performs
/// no input or output.
/// </para>
/// <para>
/// The snapshot is rebuilt on publication rather than on observation because
/// publication is the rare event: a crawl advances at most once per
/// <see cref="GrainIndexOptions.BackfillInterval"/>, while a scrape may be far
/// more frequent and must stay allocation-free.
/// </para>
/// <para>
/// State is static because an <see cref="ObservableGauge{T}"/> callback is a
/// plain delegate with no container behind it, and because a silo hosts one
/// crawl per index regardless of how the index was registered. Entries are
/// keyed by index name, so two indexes never collide, and a test can return the
/// registry to its initial state with <see cref="Clear"/>.
/// </para>
/// </remarks>
internal static class GrainIndexBackfillProgressRegistry
{
    private static readonly ConcurrentDictionary<string, Sample> Samples = new(StringComparer.Ordinal);

    private static Snapshot _snapshot = Snapshot.Empty;

    /// <summary>
    /// Publishes an index's current crawl progress, replacing whatever this
    /// silo last reported for it.
    /// </summary>
    /// <param name="status">The crawl's status. Must not be <c>null</c>.</param>
    /// <param name="total">
    /// The best-effort size of the population the crawl has to cover, or
    /// <c>null</c> when the key source cannot bound it.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="status"/> is <c>null</c>.</exception>
    internal static void Publish(GrainIndexBackfillStatus status, long? total)
    {
        ArgumentNullException.ThrowIfNull(status);

        Samples[status.IndexName] = new Sample(
            GrainIndexMetrics.IndexTag(status.IndexName),
            status.Visited,
            total,
            PercentComplete(status.State, status.Visited, total),
            (int)status.State);

        Rebuild();
    }

    /// <summary>
    /// Stops reporting for an index, for a silo that no longer hosts its crawl.
    /// Removing an index that was never published is a no-op.
    /// </summary>
    /// <param name="indexName">The index to stop reporting. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="indexName"/> is <c>null</c>.</exception>
    internal static void Remove(string indexName)
    {
        ArgumentNullException.ThrowIfNull(indexName);

        if (Samples.TryRemove(indexName, out _))
            Rebuild();
    }

    /// <summary>Drops every published sample.</summary>
    internal static void Clear()
    {
        Samples.Clear();
        Rebuild();
    }

    /// <summary>The keys each hosted crawl has taken from its key source.</summary>
    /// <returns>One measurement per index this silo reports for.</returns>
    internal static IEnumerable<Measurement<long>> ObserveProcessed() => Volatile.Read(ref _snapshot).Processed;

    /// <summary>The bounded population size of each hosted crawl that has one.</summary>
    /// <returns>One measurement per index whose key source bounds its population.</returns>
    internal static IEnumerable<Measurement<long>> ObserveTotal() => Volatile.Read(ref _snapshot).Total;

    /// <summary>How far through its population each hosted crawl has reached.</summary>
    /// <returns>One measurement per index whose progress is expressible as a percentage.</returns>
    internal static IEnumerable<Measurement<double>> ObservePercentComplete() =>
        Volatile.Read(ref _snapshot).PercentComplete;

    /// <summary>The lifecycle state of each hosted crawl.</summary>
    /// <returns>One measurement per index this silo reports for.</returns>
    internal static IEnumerable<Measurement<int>> ObserveState() => Volatile.Read(ref _snapshot).State;

    /// <summary>
    /// The percentage of the population a crawl has covered, or <c>null</c> when
    /// that is not knowable.
    /// </summary>
    /// <remarks>
    /// A completed crawl is complete whether or not the key source could bound
    /// the population, so it reports 100 rather than nothing: "finished" is the
    /// one progress fact that never needs an estimate. Everything else needs a
    /// positive bound, and the ratio is clamped because a key source's count is
    /// explicitly approximate and may be exceeded.
    /// </remarks>
    /// <param name="state">The crawl's lifecycle state.</param>
    /// <param name="processed">Keys taken from the key source so far.</param>
    /// <param name="total">The bounded population size, or <c>null</c>.</param>
    /// <returns>The percentage, or <c>null</c>.</returns>
    internal static double? PercentComplete(GrainIndexBackfillState state, long processed, long? total)
    {
        if (state == GrainIndexBackfillState.Completed)
            return 100d;

        if (total is not > 0)
            return null;

        return Math.Clamp(processed * 100d / total.Value, 0d, 100d);
    }

    private static void Rebuild()
    {
        // ConcurrentDictionary.Values is already a point-in-time snapshot, so the
        // counting pass and the filling pass below walk exactly the same set - and
        // copying it into a list first would allocate a second one for nothing.
        var samples = Samples.Values;
        var count = samples.Count;
        if (count == 0)
        {
            Volatile.Write(ref _snapshot, Snapshot.Empty);
            return;
        }

        var bounded = 0;
        var withPercent = 0;
        foreach (var sample in samples)
        {
            if (sample.Total is not null)
                bounded++;

            if (sample.PercentComplete is not null)
                withPercent++;
        }

        var processed = new Measurement<long>[count];
        var state = new Measurement<int>[count];
        Measurement<long>[] totals = bounded == 0 ? [] : new Measurement<long>[bounded];
        Measurement<double>[] percents = withPercent == 0 ? [] : new Measurement<double>[withPercent];

        var i = 0;
        var t = 0;
        var p = 0;
        foreach (var sample in samples)
        {
            processed[i] = new Measurement<long>(sample.Processed, sample.Tags);
            state[i] = new Measurement<int>(sample.State, sample.Tags);
            i++;

            if (sample.Total is { } total)
                totals[t++] = new Measurement<long>(total, sample.Tags);

            if (sample.PercentComplete is { } percent)
                percents[p++] = new Measurement<double>(percent, sample.Tags);
        }

        Volatile.Write(ref _snapshot, new Snapshot(processed, totals, percents, state));
    }

    /// <summary>One index's frozen progress, with its tag array built once.</summary>
    private sealed class Sample(
        KeyValuePair<string, object?> indexTag,
        long processed,
        long? total,
        double? percentComplete,
        int state)
    {
        /// <summary>
        /// The measurement's tags, held as the array a
        /// <see cref="Measurement{T}"/> stores, so building a measurement copies
        /// no tags. It carries the index tag and the constant platform-sentinel
        /// <see cref="LatticeTenantLabel.TagTenant"/> dimension
        /// (<see cref="LatticeTenantLabel.Platform"/>): a grain index is a
        /// cluster-local, multi-grain aggregate that no single tenant owns. The
        /// array is built once per publication, so the far more frequent scrape
        /// path stays allocation-free.
        /// </summary>
        internal KeyValuePair<string, object?>[] Tags { get; } = [indexTag, LatticeTenantLabel.Platform];

        internal long Processed { get; } = processed;

        internal long? Total { get; } = total;

        internal double? PercentComplete { get; } = percentComplete;

        internal int State { get; } = state;
    }

    /// <summary>
    /// The four pre-built measurement arrays a scrape returns without touching
    /// the sample store.
    /// </summary>
    private sealed class Snapshot(
        Measurement<long>[] processed,
        Measurement<long>[] total,
        Measurement<double>[] percentComplete,
        Measurement<int>[] state)
    {
        internal static Snapshot Empty { get; } = new([], [], [], []);

        internal Measurement<long>[] Processed { get; } = processed;

        internal Measurement<long>[] Total { get; } = total;

        internal Measurement<double>[] PercentComplete { get; } = percentComplete;

        internal Measurement<int>[] State { get; } = state;
    }
}
