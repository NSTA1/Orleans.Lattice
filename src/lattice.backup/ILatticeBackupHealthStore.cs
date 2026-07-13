namespace Orleans.Lattice.Backup;

/// <summary>
/// Persistence for per-backup health state: the latest
/// <see cref="BackupHealthReport"/> a verification produced, and the per-backup
/// <see cref="BackupHealthConfig"/> that governs whether and how often the periodic
/// monitor re-verifies a backup. Reports and configuration are stored in the
/// reserved <c>sys-backup-health</c> <c>ILattice</c> tree, keyed by backup id, so
/// the periodic monitor that writes reports and the management UI that reads them
/// share a single durable projection - there is no second external store.
/// </summary>
public interface ILatticeBackupHealthStore
{
    /// <summary>
    /// Persists (or replaces) the latest health report for its
    /// <see cref="BackupHealthReport.BackupId"/>.
    /// </summary>
    /// <param name="report">The report to persist. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <exception cref="ArgumentNullException"><paramref name="report"/> is <c>null</c>.</exception>
    Task SetReportAsync(BackupHealthReport report, CancellationToken cancellationToken = default);

    /// <summary>Reads the latest health report for a backup, or <c>null</c> when none has been stored.</summary>
    /// <param name="backupId">The backup id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> is <c>null</c> or empty.</exception>
    Task<BackupHealthReport?> GetReportAsync(string backupId, CancellationToken cancellationToken = default);

    /// <summary>Enumerates every stored health report, in backup-id order.</summary>
    /// <param name="cancellationToken">Cancels the scan.</param>
    IAsyncEnumerable<BackupHealthReport> ListReportsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes the stored health report (and configuration) for a backup. Returns
    /// <c>true</c> when anything was removed. Called when a backup is deleted so its
    /// health state does not linger.
    /// </summary>
    /// <param name="backupId">The backup id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> is <c>null</c> or empty.</exception>
    Task<bool> RemoveAsync(string backupId, CancellationToken cancellationToken = default);

    /// <summary>Persists (or replaces) the health-monitor configuration for a backup.</summary>
    /// <param name="backupId">The backup id. Must not be <c>null</c> or empty.</param>
    /// <param name="config">The configuration to persist. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="config"/> is <c>null</c>.</exception>
    Task SetConfigAsync(string backupId, BackupHealthConfig config, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads the per-backup health-monitor configuration, or <c>null</c> when the
    /// backup uses the configured defaults (no explicit override stored).
    /// </summary>
    /// <param name="backupId">The backup id. Must not be <c>null</c> or empty.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <exception cref="ArgumentException"><paramref name="backupId"/> is <c>null</c> or empty.</exception>
    Task<BackupHealthConfig?> GetConfigAsync(string backupId, CancellationToken cancellationToken = default);
}
