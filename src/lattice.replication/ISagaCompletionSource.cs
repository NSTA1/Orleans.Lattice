namespace Orleans.Lattice.Replication;

/// <summary>
/// Internal seam that reports whether a cross-cluster saga has <b>globally
/// completed</b> - every participant has flipped and the coordinator has
/// reached its terminal <see cref="CrossClusterSagaPhase.Completed"/> phase.
/// <para>
/// The durable write-fence / shipping-pause primitive
/// (<see cref="Grains.ISagaWriteFenceGrain"/>) gates its cross-cluster shipping
/// resume on this signal: a participant that merely received its own local
/// commit does <b>not</b> thereby know every other participant has flipped, so
/// resuming shipping on a local flip is unsafe (an early-flipping cluster could
/// receive a laggard's still-advanced post-cut entries and re-advance itself).
/// Only global completion guarantees no post-cut entries remain anywhere to
/// re-propagate.
/// </para>
/// <para>
/// The default implementation
/// (<see cref="CoordinatorSagaCompletionSource"/>) dials the coordinator grain's
/// <see cref="Grains.ICrossClusterSagaCoordinatorGrain.IsCompleteAsync"/>. The
/// seam is injectable so tests can simulate a laggard (completion withheld)
/// deterministically without a full multi-cluster coordinator.
/// </para>
/// </summary>
internal interface ISagaCompletionSource
{
    /// <summary>
    /// Returns <see langword="true"/> when the saga identified by
    /// <paramref name="sagaId"/> (coordinated by
    /// <paramref name="coordinatorClusterId"/>) has globally completed. A
    /// transient failure to reach the coordinator surfaces as
    /// <see langword="false"/> (not yet observed complete) so the caller keeps
    /// its shipping pause engaged - the fail-safe direction.
    /// </summary>
    /// <param name="sagaId">Identifier of the saga to probe.</param>
    /// <param name="coordinatorClusterId">Cluster id hosting the coordinator.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<bool> IsSagaCompleteAsync(string sagaId, string coordinatorClusterId, CancellationToken cancellationToken = default);
}
