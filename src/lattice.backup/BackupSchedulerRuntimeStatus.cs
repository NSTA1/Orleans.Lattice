namespace Orleans.Lattice.Backup;

/// <summary>
/// A read-only snapshot of a per-scope backup scheduler's runtime status: whether
/// each schedule kind is registered, the start and success timestamps of the most
/// recent full and incremental capture cycles, the terminal outcome of the most
/// recent cycle, and the runtime schedule intervals. Returned by the scheduler grain so the admin status surface
/// can report a scope's health without scraping metrics or reading the grain's
/// persistent state directly.
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupSchedulerRuntimeStatus)]
[Immutable]
public sealed record BackupSchedulerRuntimeStatus
{
    /// <summary>Initializes a new <see cref="BackupSchedulerRuntimeStatus"/>.</summary>
    /// <param name="fullScheduleRegistered">Whether a full-backup schedule reminder is registered.</param>
    /// <param name="incrementalScheduleRegistered">Whether an incremental-backup schedule reminder is registered.</param>
    /// <param name="lastFullRunUtc">The start time of the most recent full cycle, or <c>null</c> when none.</param>
    /// <param name="lastFullSuccessUtc">The success time of the most recent full cycle, or <c>null</c> when none.</param>
    /// <param name="lastIncrementalRunUtc">The start time of the most recent incremental cycle, or <c>null</c> when none.</param>
    /// <param name="lastIncrementalSuccessUtc">The success time of the most recent incremental cycle, or <c>null</c> when none.</param>
    /// <param name="lastRunOutcome">The terminal outcome of the most recent cycle of either kind.</param>
    /// <param name="runtimeFullBackupInterval">The runtime full-backup cadence, or <c>null</c> when none is registered.</param>
    /// <param name="runtimeIncrementalBackupInterval">The runtime incremental-backup cadence, or <c>null</c> when none is registered.</param>
    public BackupSchedulerRuntimeStatus(
        bool fullScheduleRegistered,
        bool incrementalScheduleRegistered,
        DateTimeOffset? lastFullRunUtc,
        DateTimeOffset? lastFullSuccessUtc,
        DateTimeOffset? lastIncrementalRunUtc,
        DateTimeOffset? lastIncrementalSuccessUtc,
        BackupScopeRunOutcome lastRunOutcome,
        TimeSpan? runtimeFullBackupInterval = null,
        TimeSpan? runtimeIncrementalBackupInterval = null)
    {
        FullScheduleRegistered = fullScheduleRegistered;
        IncrementalScheduleRegistered = incrementalScheduleRegistered;
        LastFullRunUtc = lastFullRunUtc;
        LastFullSuccessUtc = lastFullSuccessUtc;
        LastIncrementalRunUtc = lastIncrementalRunUtc;
        LastIncrementalSuccessUtc = lastIncrementalSuccessUtc;
        LastRunOutcome = lastRunOutcome;
        RuntimeFullBackupInterval = runtimeFullBackupInterval;
        RuntimeIncrementalBackupInterval = runtimeIncrementalBackupInterval;
    }

    /// <summary>Whether a full-backup schedule reminder is registered for the scope.</summary>
    [Id(0)]
    public bool FullScheduleRegistered { get; init; }

    /// <summary>Whether an incremental-backup schedule reminder is registered for the scope.</summary>
    [Id(1)]
    public bool IncrementalScheduleRegistered { get; init; }

    /// <summary>The start time of the most recent full-capture cycle, or <c>null</c> when none.</summary>
    [Id(2)]
    public DateTimeOffset? LastFullRunUtc { get; init; }

    /// <summary>The success time of the most recent full-capture cycle, or <c>null</c> when none.</summary>
    [Id(3)]
    public DateTimeOffset? LastFullSuccessUtc { get; init; }

    /// <summary>The start time of the most recent incremental-capture cycle, or <c>null</c> when none.</summary>
    [Id(4)]
    public DateTimeOffset? LastIncrementalRunUtc { get; init; }

    /// <summary>The success time of the most recent incremental-capture cycle, or <c>null</c> when none.</summary>
    [Id(5)]
    public DateTimeOffset? LastIncrementalSuccessUtc { get; init; }

    /// <summary>The terminal outcome of the most recent capture cycle of either kind.</summary>
    [Id(6)]
    public BackupScopeRunOutcome LastRunOutcome { get; init; }

    /// <summary>The runtime-registered full-backup cadence, or <c>null</c> when none is registered.</summary>
    [Id(7)]
    public TimeSpan? RuntimeFullBackupInterval { get; init; }

    /// <summary>The runtime-registered incremental-backup cadence, or <c>null</c> when none is registered.</summary>
    [Id(8)]
    public TimeSpan? RuntimeIncrementalBackupInterval { get; init; }
}
