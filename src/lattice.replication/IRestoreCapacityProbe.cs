using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Replication;

/// <summary>
/// Admission seam consulted by the restore saga participant before it fences or
/// builds a shadow tree, and by the dispatcher on the coordinator cluster before
/// it starts a coordinated restore. Given the target backup's self-describing
/// size and topology (<see cref="RestoreAdmissionReport"/>), it reports whether
/// the local cluster has the headroom to host the rebuilt tree, so an infeasible
/// target is hard-refused up front - the same all-or-nothing posture as an
/// offline peer - rather than after fencing the fleet and building most of a
/// large shadow.
/// <para>
/// The default <see cref="UnboundedRestoreCapacityProbe"/> always admits; a host
/// that enforces a storage or memory budget registers its own singleton before
/// <see cref="LatticeReplicationServiceCollectionExtensions.AddLatticeReplication"/>
/// runs.
/// </para>
/// </summary>
internal interface IRestoreCapacityProbe
{
    /// <summary>
    /// Reports whether the local cluster can host the restore described by
    /// <paramref name="report"/>.
    /// </summary>
    /// <param name="report">The target backup's self-describing size and topology. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><c>true</c> when the target is feasible on this cluster; otherwise <c>false</c>.</returns>
    Task<bool> CanHostAsync(RestoreAdmissionReport report, CancellationToken cancellationToken = default);
}
