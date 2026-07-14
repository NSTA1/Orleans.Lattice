namespace Orleans.Lattice.Scaling;

/// <summary>
/// Internal seam over the cluster-wide runtime statistics used by the compute
/// axis: the active-silo count and a per-silo CPU / memory / activation sample.
/// The production implementation
/// (<see cref="ManagementClusterRuntimeStatisticsSource"/>) sources these from
/// Orleans' management grain and caches the result briefly so the compute
/// collector and the replica-count provider share a single round-trip per sample
/// tick. Tests substitute a deterministic fake.
/// </summary>
internal interface IClusterRuntimeStatisticsSource
{
    /// <summary>
    /// Returns a recent cluster-wide runtime-statistics snapshot. Cheap to call
    /// repeatedly: the implementation caches the underlying round-trip.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>A task yielding the current <see cref="ClusterRuntimeSnapshot"/>.</returns>
    ValueTask<ClusterRuntimeSnapshot> SampleAsync(CancellationToken cancellationToken);
}
