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
}
