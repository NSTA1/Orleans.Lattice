namespace Orleans.Lattice.Api.State;

/// <summary>
/// Transport-agnostic live metadata / metrics observation facade. Samples
/// low-cardinality per-tree aggregates (live keys, tombstones, depth, shard
/// count, split activity, optional per-shard hotness and view lag) on a
/// cadence and exposes them as either a one-shot poll or a delta-encoded
/// stream. Sourced strictly from already-maintained aggregates (the structural
/// digest and the existing metrics surface), so the feed adds no per-mutation
/// cost: its work is bounded by tree / shard count on a timer.
/// </summary>
public interface ILatticeStateMetricsObserver
{
    /// <summary>
    /// Returns a single, full metrics snapshot for the requested trees
    /// (<see cref="TreeMetricsSnapshot.IsInitial"/> is always
    /// <see langword="true"/>). Intended for clients that prefer pull over a
    /// stream.
    /// </summary>
    /// <param name="request">Scope and sampling options (the cadence is ignored).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<TreeMetricsSnapshot> SampleAsync(
        TreeMetricsRequest request,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams metrics snapshots on the configured (or requested) cadence. The
    /// first tick is a full snapshot
    /// (<see cref="TreeMetricsSnapshot.IsInitial"/>); subsequent ticks are
    /// deltas that carry only the trees whose aggregates changed. The stream
    /// runs until the caller cancels, which tears the sampler down cleanly.
    /// </summary>
    /// <param name="request">Scope and sampling options.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    IAsyncEnumerable<TreeMetricsSnapshot> ObserveAsync(
        TreeMetricsRequest request,
        CancellationToken cancellationToken = default);
}
