namespace Orleans.Lattice.Backup;

/// <summary>
/// Backup-local seam consulted by the restore path so a restore into a
/// replicated tree can be promoted to an all-or-nothing coordinated restore
/// across every cluster that replicates the target, while the backup package
/// stays saga-unaware. The backup package cannot reference the replication
/// package (that would invert the intended layering: backup depends only on core
/// lattice), so this interface lets the restore path hand a request to the
/// coordinated path without a direct dependency on the coordinator, the write
/// fence, or the participant model.
/// <para>
/// A default no-op implementation
/// (<see cref="NoRestoreSagaDispatcher"/>) is registered by
/// <see cref="LatticeBackupServiceCollectionExtensions.AddLatticeBackup(Orleans.Hosting.ISiloBuilder, System.Action{LatticeBackupOptions})"/>
/// that never dispatches, which is correct for a single-cluster deployment where
/// the replication package is not wired: every restore takes the plain local
/// path. The replication package supplies the real implementation that inspects
/// the target tree's current replication topology and runs the coordinated saga
/// only when the target is replicated.
/// </para>
/// </summary>
public interface IRestoreSagaDispatcher
{
    /// <summary>
    /// Offers a restore request to the coordinated path. The decision is a
    /// function of the <b>target tree now</b>, never of the backup's origin: if the
    /// target tree is not replicated (no current peers) the dispatcher declines and
    /// the caller runs the plain local restore; if it is replicated the dispatcher
    /// runs the coordinated saga over the target's current peer set and returns the
    /// local cluster's restore result.
    /// </summary>
    /// <param name="request">The restore request being dispatched. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// The restore result when the coordinated path handled the request; otherwise
    /// <see langword="null"/> to signal the caller should run the local restore.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    Task<LatticeRestoreResult?> TryDispatchAsync(
        LatticeRestoreRequest request,
        CancellationToken cancellationToken = default);
}
