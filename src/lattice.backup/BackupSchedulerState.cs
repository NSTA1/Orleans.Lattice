namespace Orleans.Lattice.Backup;

/// <summary>
/// Persistent state for the per-scope <c>BackupSchedulerGrain</c>. Records the
/// scope the grain coordinates so a reminder firing after a silo restart can
/// reconstruct the exact region to capture without the caller re-supplying it.
/// The in-flight overlap guard is intentionally not persisted: it is an
/// activation-local flag, so a crash that deactivates the grain clears any
/// in-progress capture rather than leaving a stale lock.
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupSchedulerState)]
internal sealed class BackupSchedulerState
{
    /// <summary>
    /// The scope this scheduler coordinates, captured on the first schedule or
    /// trigger call, or <c>null</c> before the grain has been configured.
    /// </summary>
    [Id(0)]
    public BackupScopeSelector? Scope { get; set; }
}
