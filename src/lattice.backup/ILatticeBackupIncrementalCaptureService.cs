namespace Orleans.Lattice.Backup;

/// <summary>
/// Captures a named, timestamped incremental backup layered on a base backup.
/// The captured manifest records the base id as its
/// <see cref="BackupManifest.BaseBackupId"/> and is registered in the catalog
/// exactly like a full capture, so the backup chain is enumerable and
/// restorable.
/// <para>
/// This seam is the entry point the scheduling / retention coordinator invokes
/// for a scheduled incremental and the backup control facade invokes for an
/// on-demand one. <c>AddLatticeBackup</c> serves it (TryAdd, so a host may
/// pre-register its own) from the same capture engine as
/// <see cref="ILatticeBackupCaptureService"/>: the increment is a forward
/// write-ahead-log delta from the base backup's consistency cut. The engine
/// instead captures a fresh full backup of the base's scope - a new chain with
/// no base, not an increment - when the base chain is owned by another
/// capturing cluster, when retention has trimmed the log past the base resume
/// point, or when a range delete surfaces in the delta window.
/// </para>
/// </summary>
public interface ILatticeBackupIncrementalCaptureService
{
    /// <summary>
    /// Captures the incremental backup described by <paramref name="request"/> and
    /// returns the content-addressed id and manifest of the stored backup.
    /// </summary>
    /// <param name="request">The incremental capture request. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the capture.</param>
    /// <returns>The captured backup's id and manifest.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="request"/> is <c>null</c>.</exception>
    /// <exception cref="KeyNotFoundException">The base backup id does not exist in the sink.</exception>
    /// <exception cref="LatticeAuthorizationDeniedException">The caller is not authorized to back up the base backup's scope.</exception>
    /// <exception cref="Orleans.Lattice.LatticeTenantAccessDeniedException">The active tenant is not admitted to start the capture.</exception>
    /// <exception cref="Orleans.Lattice.LatticeSnapshotReplayBudgetExceededException">The fallback full capture would exceed the configured snapshot replay budget.</exception>
    Task<LatticeBackupCaptureResult> CaptureIncrementalAsync(
        LatticeBackupIncrementalCaptureRequest request,
        CancellationToken cancellationToken = default);
}
