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

    /// <summary>The wall-clock time the most recent full-capture cycle started, or <c>null</c> when none has run.</summary>
    [Id(1)]
    public DateTimeOffset? LastFullRunUtc { get; set; }

    /// <summary>The wall-clock time the most recent full-capture cycle succeeded, or <c>null</c> when none has.</summary>
    [Id(2)]
    public DateTimeOffset? LastFullSuccessUtc { get; set; }

    /// <summary>The wall-clock time the most recent incremental-capture cycle started, or <c>null</c> when none has run.</summary>
    [Id(3)]
    public DateTimeOffset? LastIncrementalRunUtc { get; set; }

    /// <summary>The wall-clock time the most recent incremental-capture cycle succeeded, or <c>null</c> when none has.</summary>
    [Id(4)]
    public DateTimeOffset? LastIncrementalSuccessUtc { get; set; }

    /// <summary>The terminal outcome of the most recent capture cycle of either kind.</summary>
    [Id(5)]
    public BackupScopeRunOutcome LastRunOutcome { get; set; }

    /// <summary>
    /// The cadence of a runtime-registered recurring full-backup schedule (for
    /// example one requested from a management UI), or <c>null</c> when no runtime
    /// full schedule has been registered. Recorded for observability and to make
    /// a re-registration idempotent; the Orleans reminder itself is the durable
    /// source of truth for whether the schedule fires. A runtime interval
    /// overrides the configured <see cref="LatticeBackupScheduleOptions.FullBackupInterval"/>
    /// cadence for this scope.
    /// </summary>
    [Id(6)]
    public TimeSpan? RuntimeFullBackupInterval { get; set; }

    /// <summary>
    /// The cadence of a runtime-registered recurring incremental-backup schedule,
    /// or <c>null</c> when no runtime incremental schedule has been registered.
    /// The runtime counterpart of
    /// <see cref="LatticeBackupScheduleOptions.IncrementalBackupInterval"/>.
    /// </summary>
    [Id(7)]
    public TimeSpan? RuntimeIncrementalBackupInterval { get; set; }
}
