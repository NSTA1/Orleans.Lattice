namespace Orleans.Lattice.Backup;

/// <summary>
/// A backup-set schedule's configuration and state: its membership, cadence and
/// retention, whether its reminder is registered and when it next fires, whether
/// a run is in flight, the outcome of its recent runs, and the sets it retains.
/// Tree ids are effective (tenant-composed) ids, as backup manifests carry them.
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupSetScheduleStatus)]
[Immutable]
public sealed record BackupSetScheduleStatus
{
    /// <summary>Initializes a new <see cref="BackupSetScheduleStatus"/>.</summary>
    /// <param name="name">The schedule's name, as registered.</param>
    /// <param name="request">The schedule's configuration (effective tree ids).</param>
    /// <param name="memberScopes">The scopes the next run captures (or, for a schedule that re-resolves its trees, the last run captured).</param>
    /// <param name="isScheduled">Whether the schedule's reminder is registered.</param>
    /// <param name="nextRunUtc">When the reminder next fires, or <c>null</c> when it is not registered.</param>
    /// <param name="isRunning">Whether a run is in flight.</param>
    /// <param name="registeredAtUtc">When the schedule was registered or its cadence last changed.</param>
    /// <param name="lastRunUtc">When the most recent run started, or <c>null</c> when none has.</param>
    /// <param name="lastSuccessUtc">When the most recent successful run finished, or <c>null</c> when none has.</param>
    /// <param name="lastOutcome">How the most recent run ended.</param>
    /// <param name="lastFailureReason">A one-line reason for the most recent failed run, or <c>null</c>.</param>
    /// <param name="consecutiveFailures">How many runs in a row have failed.</param>
    /// <param name="recentRuns">The most recent runs, newest first.</param>
    /// <param name="retainedSets">The sets the schedule retains, newest first.</param>
    /// <param name="currentRun">The live progress of the run in flight, or <c>null</c> when none is.</param>
    public BackupSetScheduleStatus(
        string name,
        LatticeBackupSetScheduleRequest request,
        IReadOnlyList<BackupScopeSelector> memberScopes,
        bool isScheduled,
        DateTimeOffset? nextRunUtc,
        bool isRunning,
        DateTimeOffset registeredAtUtc,
        DateTimeOffset? lastRunUtc,
        DateTimeOffset? lastSuccessUtc,
        BackupScopeRunOutcome lastOutcome,
        string? lastFailureReason,
        int consecutiveFailures,
        IReadOnlyList<BackupSetScheduleRun> recentRuns,
        IReadOnlyList<ScheduledBackupSet> retainedSets,
        BackupSetScheduleRunProgress? currentRun = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(memberScopes);
        ArgumentNullException.ThrowIfNull(recentRuns);
        ArgumentNullException.ThrowIfNull(retainedSets);
        Name = name;
        Request = request;
        MemberScopes = memberScopes;
        IsScheduled = isScheduled;
        NextRunUtc = nextRunUtc;
        IsRunning = isRunning;
        RegisteredAtUtc = registeredAtUtc;
        LastRunUtc = lastRunUtc;
        LastSuccessUtc = lastSuccessUtc;
        LastOutcome = lastOutcome;
        LastFailureReason = lastFailureReason;
        ConsecutiveFailures = consecutiveFailures;
        RecentRuns = recentRuns;
        RetainedSets = retainedSets;
        CurrentRun = currentRun;
    }

    /// <summary>The schedule's name, as registered.</summary>
    [Id(0)]
    public string Name { get; init; }

    /// <summary>The schedule's configuration, with effective tree ids.</summary>
    [Id(1)]
    public LatticeBackupSetScheduleRequest Request { get; init; }

    /// <summary>
    /// The scopes the next run captures; for a schedule that re-resolves its
    /// trees at every run, the scopes the last run captured.
    /// </summary>
    [Id(2)]
    public IReadOnlyList<BackupScopeSelector> MemberScopes { get; init; }

    /// <summary>Whether the schedule's reminder is registered.</summary>
    [Id(3)]
    public bool IsScheduled { get; init; }

    /// <summary>When the reminder next fires, or <c>null</c> when it is not registered.</summary>
    [Id(4)]
    public DateTimeOffset? NextRunUtc { get; init; }

    /// <summary>Whether a run is in flight.</summary>
    [Id(5)]
    public bool IsRunning { get; init; }

    /// <summary>When the schedule was registered or its cadence last changed (the anchor its runs are timed from).</summary>
    [Id(6)]
    public DateTimeOffset RegisteredAtUtc { get; init; }

    /// <summary>When the most recent run started, or <c>null</c> when none has.</summary>
    [Id(7)]
    public DateTimeOffset? LastRunUtc { get; init; }

    /// <summary>When the most recent successful run finished, or <c>null</c> when none has.</summary>
    [Id(8)]
    public DateTimeOffset? LastSuccessUtc { get; init; }

    /// <summary>How the most recent run ended.</summary>
    [Id(9)]
    public BackupScopeRunOutcome LastOutcome { get; init; }

    /// <summary>A one-line reason for the most recent failed run, or <c>null</c>.</summary>
    [Id(10)]
    public string? LastFailureReason { get; init; }

    /// <summary>How many runs in a row have failed.</summary>
    [Id(11)]
    public int ConsecutiveFailures { get; init; }

    /// <summary>The most recent runs, newest first.</summary>
    [Id(12)]
    public IReadOnlyList<BackupSetScheduleRun> RecentRuns { get; init; }

    /// <summary>The sets the schedule retains, newest first.</summary>
    [Id(13)]
    public IReadOnlyList<ScheduledBackupSet> RetainedSets { get; init; }

    /// <summary>
    /// The live progress of the run in flight - its phase, how far through it,
    /// the tree being written and the capture attempt - or <c>null</c> when no
    /// run is in flight.
    /// </summary>
    [Id(14)]
    public BackupSetScheduleRunProgress? CurrentRun { get; init; }
}
