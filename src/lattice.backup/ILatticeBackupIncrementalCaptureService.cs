namespace Orleans.Lattice.Backup;

/// <summary>
/// Captures a named, timestamped incremental backup layered on a base backup.
/// The captured manifest records the base id as its
/// <see cref="BackupManifest.BaseBackupId"/> and is registered in the catalog
/// exactly like a full capture, so the backup chain is enumerable and
/// restorable.
/// <para>
/// This seam is the entry point the scheduling / retention coordinator invokes
/// for a scheduled or on-demand incremental. A cluster that has wired a
/// dedicated incremental-capture engine overrides the default registration; when
/// none is registered, a baseline stand-in captures a full snapshot and stamps
/// it as an increment of the supplied base, which keeps the chain shape correct
/// while a true differential engine is being introduced.
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
    Task<LatticeBackupCaptureResult> CaptureIncrementalAsync(
        LatticeBackupIncrementalCaptureRequest request,
        CancellationToken cancellationToken = default);
}
