namespace Orleans.Lattice.Backup;

/// <summary>
/// Verifies that a backup's durable sink payload is present and intact. A backup
/// is only a usable restore point if its manifest and every referenced artifact
/// are present in the sink <b>and</b> each artifact's content still hashes to the
/// digest the manifest recorded when it was captured. This service performs that
/// two-part check - presence and hash consistency - and produces a
/// <see cref="BackupHealthReport"/> precise enough to drive a diagnostics dialog.
/// </summary>
public interface ILatticeBackupHealthService
{
    /// <summary>
    /// Verifies the backup identified by <paramref name="backupId"/> against the
    /// durable sink and returns a fresh health report. Reuses the sink's cheap
    /// presence probe for manifest / artifact presence, then downloads every
    /// present artifact and re-hashes it against its recorded content hash to catch
    /// silent corruption. Does not persist the report; the caller decides whether
    /// to store it.
    /// </summary>
    /// <param name="backupId">The backup id to verify. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the verification.</param>
    /// <returns>The point-in-time health report for the backup.</returns>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> is <c>null</c> or empty.</exception>
    Task<BackupHealthReport> VerifyAsync(string backupId, CancellationToken cancellationToken = default);
}
