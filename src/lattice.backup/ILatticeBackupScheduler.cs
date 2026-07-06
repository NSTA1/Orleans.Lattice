namespace Orleans.Lattice.Backup;

/// <summary>
/// The public entry point for triggering backups on demand, registering
/// recurring backup schedules, and pruning a backup chain per its retention
/// policy. Each operation targets a <see cref="BackupScopeSelector"/> and is
/// coordinated by a single per-scope grain, so on-demand triggers, scheduled
/// captures, and retention for the same scope never overlap.
/// <para>
/// Scheduling and retention are configured per scope through
/// <c>ConfigureLatticeBackupSchedule</c> and are disabled by default; the
/// on-demand trigger methods work regardless of whether a schedule is
/// configured.
/// </para>
/// </summary>
public interface ILatticeBackupScheduler
{
    /// <summary>
    /// Triggers a full backup of <paramref name="scope"/> and returns its backup
    /// id, or <c>null</c> when a capture for the scope is already in flight.
    /// </summary>
    /// <param name="scope">The scope to capture. Must not be <c>null</c>.</param>
    /// <returns>The captured backup id, or <c>null</c> when skipped by the overlap guard.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scope"/> is <c>null</c>.</exception>
    Task<string?> TriggerFullBackupAsync(BackupScopeSelector scope);

    /// <summary>
    /// Triggers an incremental backup of <paramref name="scope"/>, layered on the
    /// most recent existing backup for the scope (or a full baseline when none
    /// exists), and returns its backup id, or <c>null</c> when a capture for the
    /// scope is already in flight.
    /// </summary>
    /// <param name="scope">The scope to capture. Must not be <c>null</c>.</param>
    /// <returns>The captured backup id, or <c>null</c> when skipped by the overlap guard.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scope"/> is <c>null</c>.</exception>
    Task<string?> TriggerIncrementalBackupAsync(BackupScopeSelector scope);

    /// <summary>
    /// Registers (or updates) the recurring full- and incremental-backup schedule
    /// reminders for <paramref name="scope"/>, honouring its configured
    /// <see cref="LatticeBackupScheduleOptions"/>. Idempotent.
    /// </summary>
    /// <param name="scope">The scope to schedule. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="scope"/> is <c>null</c>.</exception>
    Task EnsureScheduleAsync(BackupScopeSelector scope);

    /// <summary>
    /// Prunes the backup chain for <paramref name="scope"/> per its retention
    /// policy and returns the outcome. Preserves the base chain of every retained
    /// increment; a no-op that retains everything when retention is disabled.
    /// </summary>
    /// <param name="scope">The scope to prune. Must not be <c>null</c>.</param>
    /// <returns>The retention outcome.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="scope"/> is <c>null</c>.</exception>
    Task<BackupRetentionReport> PruneAsync(BackupScopeSelector scope);
}
