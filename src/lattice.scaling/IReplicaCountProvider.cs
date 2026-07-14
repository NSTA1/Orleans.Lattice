namespace Orleans.Lattice.Scaling;

/// <summary>
/// Internal seam yielding the current cluster replica count (the number of active
/// silos). Backed by the same cached cluster round-trip as
/// <see cref="IClusterRuntimeStatisticsSource"/> so reading the replica count and
/// the per-silo resource samples for one tick costs a single management call.
/// The scalar math multiplies the dominant normalised pressure by this count to
/// express demand in replica units.
/// </summary>
internal interface IReplicaCountProvider
{
    /// <summary>
    /// Returns the current number of active silo replicas in the cluster. Returns
    /// at least <c>1</c> so the replica-units scalar is never multiplied by zero
    /// when the cluster view is momentarily unavailable.
    /// </summary>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>A task yielding the active replica count (at least <c>1</c>).</returns>
    ValueTask<int> GetActiveReplicaCountAsync(CancellationToken cancellationToken);
}
