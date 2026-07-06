using Orleans.Concurrency;

namespace Orleans.Lattice.Backup;

/// <summary>
/// Per-scope background scheduling and retention coordinator. One activation
/// exists per backup scope, keyed by the scope key returned by
/// <see cref="BackupScopeKey.For(BackupScopeSelector)"/>.
/// <para>
/// The coordinator registers recurring full- and incremental-backup schedule
/// reminders (both operator opt-in, disabled by default) honouring the per-scope
/// <see cref="LatticeBackupScheduleOptions"/>, drives on-demand and scheduled
/// captures through the landed full-capture engine and the incremental-capture
/// seam, and prunes the backup chain through the sink while never orphaning an
/// increment from its base. An activation-local overlap guard ensures a new
/// capture never starts while one is already in flight for the same scope.
/// </para>
/// </summary>
[Alias(BackupTypeAliases.ILatticeBackupSchedulerGrain)]
internal interface ILatticeBackupSchedulerGrain : IGrainWithStringKey
{
    /// <summary>
    /// Captures a full backup of <paramref name="scope"/> on demand and returns
    /// its backup id, or <c>null</c> when a capture for the scope is already in
    /// flight (the overlap guard skipped this request). Persists the scope so
    /// later scheduled firings can reconstruct it.
    /// </summary>
    /// <param name="scope">The scope to capture. Must not be <c>null</c>.</param>
    [AlwaysInterleave]
    Task<string?> TriggerFullAsync(BackupScopeSelector scope);

    /// <summary>
    /// Captures an incremental backup of <paramref name="scope"/> on demand,
    /// layered on the most recent existing backup for the scope, and returns its
    /// backup id. Falls back to a full capture when no base exists yet. Returns
    /// <c>null</c> when a capture for the scope is already in flight.
    /// </summary>
    /// <param name="scope">The scope to capture. Must not be <c>null</c>.</param>
    [AlwaysInterleave]
    Task<string?> TriggerIncrementalAsync(BackupScopeSelector scope);

    /// <summary>
    /// Registers (or updates) the recurring full- and incremental-backup schedule
    /// reminders for <paramref name="scope"/>, honouring the per-scope
    /// <see cref="LatticeBackupScheduleOptions"/>. Idempotent: repeated calls
    /// converge on a single schedule. A schedule whose knob is disabled has its
    /// reminder unregistered instead.
    /// </summary>
    /// <param name="scope">The scope to schedule. Must not be <c>null</c>.</param>
    Task EnsureScheduleAsync(BackupScopeSelector scope);

    /// <summary>
    /// Prunes the backup chain for <paramref name="scope"/> per the per-scope
    /// retention policy, deleting superseded manifests and artifacts through the
    /// sink and removing their catalog entries while preserving the base chain of
    /// every retained increment. A no-op that retains everything when retention
    /// is disabled.
    /// </summary>
    /// <param name="scope">The scope to prune. Must not be <c>null</c>.</param>
    Task<BackupRetentionReport> PruneAsync(BackupScopeSelector scope);

    /// <summary>
    /// Runs one scheduled cycle synchronously - a full or incremental capture
    /// followed by a retention pass when enabled - and returns the captured
    /// backup id (or <c>null</c> when the overlap guard skipped it). This is the
    /// same work a schedule reminder performs; exposed so it can be driven
    /// directly without waiting for the reminder cadence.
    /// </summary>
    /// <param name="incremental"><c>true</c> for an incremental cycle, <c>false</c> for a full cycle.</param>
    [AlwaysInterleave]
    Task<string?> RunScheduledCycleAsync(bool incremental);

    /// <summary>Returns <c>true</c> when no capture is currently in flight for the scope.</summary>
    [AlwaysInterleave]
    Task<bool> IsIdleAsync();

    /// <summary>
    /// Returns <c>true</c> when a schedule reminder of the requested kind is
    /// currently registered for the scope.
    /// </summary>
    /// <param name="incremental"><c>true</c> to check the incremental schedule, <c>false</c> for the full schedule.</param>
    [AlwaysInterleave]
    Task<bool> HasScheduleAsync(bool incremental);
}
