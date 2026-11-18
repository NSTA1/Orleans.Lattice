namespace Orleans.Lattice.Backup;

/// <summary>
/// Persistent state of a backup-set schedule's grain
/// (<see cref="ILatticeBackupSetSchedulerGrain"/>): its configuration, the anchor
/// its runs are timed from, its run bookkeeping, and the exact membership of every
/// set it retains. The in-flight flag is deliberately not persisted, so a crash
/// can never wedge the schedule.
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupSetSchedulerState)]
internal sealed class BackupSetSchedulerState
{
    /// <summary>The schedule's configuration (effective scopes), or <c>null</c> before registration.</summary>
    [Id(0)]
    public BackupSetScheduleConfig? Config { get; set; }

    /// <summary>What the facade resolved at registration, or <c>null</c> before registration.</summary>
    [Id(1)]
    public BackupSetScheduleTarget? Target { get; set; }

    /// <summary>When the reminder was last (re)registered: the anchor the schedule's runs are timed from.</summary>
    [Id(2)]
    public DateTimeOffset RegisteredAtUtc { get; set; }

    /// <summary>Whether the schedule's reminder is meant to be registered (a paused schedule keeps its sets).</summary>
    [Id(3)]
    public bool Scheduled { get; set; }

    /// <summary>When the most recent run started.</summary>
    [Id(4)]
    public DateTimeOffset? LastRunUtc { get; set; }

    /// <summary>When the most recent successful run finished.</summary>
    [Id(5)]
    public DateTimeOffset? LastSuccessUtc { get; set; }

    /// <summary>How the most recent run ended.</summary>
    [Id(6)]
    public BackupScopeRunOutcome LastOutcome { get; set; }

    /// <summary>A one-line reason for the most recent failed run.</summary>
    [Id(7)]
    public string? LastFailureReason { get; set; }

    /// <summary>How many runs in a row have failed.</summary>
    [Id(8)]
    public int ConsecutiveFailures { get; set; }

    /// <summary>The most recent runs, newest first, bounded.</summary>
    [Id(9)]
    public List<BackupSetScheduleRun> RecentRuns { get; set; } = [];

    /// <summary>The sets the schedule retains, newest first.</summary>
    [Id(10)]
    public List<ScheduledBackupSet> Sets { get; set; } = [];

    /// <summary>The scopes the last run resolved (an all-trees schedule that re-resolves every run).</summary>
    [Id(11)]
    public List<BackupScopeSelector> LastResolvedScopes { get; set; } = [];

    /// <summary>
    /// The per-scope scheduler keys this schedule has placed a retention hold on,
    /// so a later pass - or the schedule's removal - can lift holds on scopes its
    /// retained sets no longer touch, even after a restart.
    /// </summary>
    [Id(12)]
    public List<string> HeldScopeKeys { get; set; } = [];
}
