using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup;

/// <summary>
/// An operator-facing status report for a single backup scope: whether each
/// schedule kind is registered, the start and success timestamps and terminal
/// outcome of the scope's most recent full and incremental capture cycles, the
/// current chain depth of the scope's latest backup, and the runtime schedule intervals. Returned by
/// <see cref="ILatticeBackupControl.GetScopeStatusAsync"/>.
/// </summary>
[GenerateSerializer]
[Alias(ApiBackupTypeAliases.BackupScopeStatus)]
[Immutable]
public sealed record BackupScopeStatus
{
    /// <summary>Initializes a new <see cref="BackupScopeStatus"/>.</summary>
    /// <param name="scope">The scope this status describes. Must not be <c>null</c>.</param>
    /// <param name="fullScheduleRegistered">Whether a full-backup schedule is registered.</param>
    /// <param name="incrementalScheduleRegistered">Whether an incremental-backup schedule is registered.</param>
    /// <param name="lastFullRunUtc">The start time of the most recent full cycle, or <c>null</c> when none.</param>
    /// <param name="lastFullSuccessUtc">The success time of the most recent full cycle, or <c>null</c> when none.</param>
    /// <param name="lastIncrementalRunUtc">The start time of the most recent incremental cycle, or <c>null</c> when none.</param>
    /// <param name="lastIncrementalSuccessUtc">The success time of the most recent incremental cycle, or <c>null</c> when none.</param>
    /// <param name="lastRunOutcome">The terminal outcome of the most recent cycle of either kind.</param>
    /// <param name="chainDepth">The base-chain length of the scope's latest backup (0 when none exists).</param>
    /// <param name="runtimeFullBackupInterval">The runtime full-backup cadence, or <c>null</c> when none is registered.</param>
    /// <param name="runtimeIncrementalBackupInterval">The runtime incremental-backup cadence, or <c>null</c> when none is registered.</param>
    /// <exception cref="ArgumentNullException"><paramref name="scope"/> is <c>null</c>.</exception>
    public BackupScopeStatus(
        BackupScopeSelector scope,
        bool fullScheduleRegistered,
        bool incrementalScheduleRegistered,
        DateTimeOffset? lastFullRunUtc,
        DateTimeOffset? lastFullSuccessUtc,
        DateTimeOffset? lastIncrementalRunUtc,
        DateTimeOffset? lastIncrementalSuccessUtc,
        BackupScopeRunOutcome lastRunOutcome,
        int chainDepth,
        TimeSpan? runtimeFullBackupInterval = null,
        TimeSpan? runtimeIncrementalBackupInterval = null)
    {
        ArgumentNullException.ThrowIfNull(scope);
        Scope = scope;
        FullScheduleRegistered = fullScheduleRegistered;
        IncrementalScheduleRegistered = incrementalScheduleRegistered;
        LastFullRunUtc = lastFullRunUtc;
        LastFullSuccessUtc = lastFullSuccessUtc;
        LastIncrementalRunUtc = lastIncrementalRunUtc;
        LastIncrementalSuccessUtc = lastIncrementalSuccessUtc;
        LastRunOutcome = lastRunOutcome;
        ChainDepth = chainDepth;
        RuntimeFullBackupInterval = runtimeFullBackupInterval;
        RuntimeIncrementalBackupInterval = runtimeIncrementalBackupInterval;
    }

    /// <summary>The scope this status describes.</summary>
    [Id(0)] public BackupScopeSelector Scope { get; init; }

    /// <summary>Whether a full-backup schedule is registered for the scope.</summary>
    [Id(1)] public bool FullScheduleRegistered { get; init; }

    /// <summary>Whether an incremental-backup schedule is registered for the scope.</summary>
    [Id(2)] public bool IncrementalScheduleRegistered { get; init; }

    /// <summary>The start time of the most recent full-capture cycle, or <c>null</c> when none.</summary>
    [Id(3)] public DateTimeOffset? LastFullRunUtc { get; init; }

    /// <summary>The success time of the most recent full-capture cycle, or <c>null</c> when none.</summary>
    [Id(4)] public DateTimeOffset? LastFullSuccessUtc { get; init; }

    /// <summary>The start time of the most recent incremental-capture cycle, or <c>null</c> when none.</summary>
    [Id(5)] public DateTimeOffset? LastIncrementalRunUtc { get; init; }

    /// <summary>The success time of the most recent incremental-capture cycle, or <c>null</c> when none.</summary>
    [Id(6)] public DateTimeOffset? LastIncrementalSuccessUtc { get; init; }

    /// <summary>The terminal outcome of the most recent capture cycle of either kind.</summary>
    [Id(7)] public BackupScopeRunOutcome LastRunOutcome { get; init; }

    /// <summary>The base-chain length of the scope's latest backup (0 when none exists).</summary>
    [Id(8)] public int ChainDepth { get; init; }

    /// <summary>The runtime-registered full-backup cadence, or <c>null</c> when none is registered.</summary>
    [Id(9)] public TimeSpan? RuntimeFullBackupInterval { get; init; }

    /// <summary>The runtime-registered incremental-backup cadence, or <c>null</c> when none is registered.</summary>
    [Id(10)] public TimeSpan? RuntimeIncrementalBackupInterval { get; init; }
}
