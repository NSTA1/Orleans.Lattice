namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content projection of a backup-set schedule's status,
/// returned by <c>lattice_backup_set_schedule_set</c>,
/// <c>lattice_backup_set_schedule_get</c> and (per entry)
/// <c>lattice_backup_set_schedule_list</c>: the schedule's configuration, its
/// resolved member trees, its next and recent runs, and its retained sets.
/// <see cref="Found"/> is <see langword="false"/> (and the remaining members carry
/// their empty defaults) when no schedule has the requested name.
/// </summary>
internal sealed record McpBackupSetSchedule
{
    /// <summary>Whether a schedule with the requested name exists.</summary>
    public required bool Found { get; init; }

    /// <summary>The schedule's tenant-local name.</summary>
    public string? Name { get; init; }

    /// <summary>How the schedule chooses its trees: <c>Explicit</c> or <c>AllTrees</c>.</summary>
    public string? Membership { get; init; }

    /// <summary>The tree ids an <c>Explicit</c> schedule names; empty for <c>AllTrees</c>.</summary>
    public IReadOnlyList<string> TreeIds { get; init; } = Array.Empty<string>();

    /// <summary>The tree ids the next run captures (for a schedule that re-resolves its trees, those the last run captured).</summary>
    public IReadOnlyList<string> MemberTreeIds { get; init; } = Array.Empty<string>();

    /// <summary>The cadence between runs, in whole seconds.</summary>
    public long IntervalSeconds { get; init; }

    /// <summary>The keep-last retention bound, or <see langword="null"/> when none.</summary>
    public int? RetentionKeepLast { get; init; }

    /// <summary>The max-age retention bound in whole seconds, or <see langword="null"/> when none.</summary>
    public long? RetentionMaxAgeSeconds { get; init; }

    /// <summary>Whether each set is captured at one cross-tree causal fence.</summary>
    public bool CrossTreeConsistent { get; init; }

    /// <summary>Whether an <c>AllTrees</c> schedule also captures trees created later.</summary>
    public bool IncludeNewTrees { get; init; }

    /// <summary>The tree-id prefixes an <c>AllTrees</c> schedule excludes.</summary>
    public IReadOnlyList<string> ExcludedTreePrefixes { get; init; } = Array.Empty<string>();

    /// <summary>Whether the schedule's reminder is registered (<see langword="false"/> once paused).</summary>
    public bool IsScheduled { get; init; }

    /// <summary>When the next run is due, or <see langword="null"/> when none is scheduled.</summary>
    public DateTimeOffset? NextRunUtc { get; init; }

    /// <summary>Whether a run is in flight.</summary>
    public bool IsRunning { get; init; }

    /// <summary>When the schedule was registered or its cadence last changed.</summary>
    public DateTimeOffset? RegisteredAtUtc { get; init; }

    /// <summary>When the most recent run started, or <see langword="null"/> when none has.</summary>
    public DateTimeOffset? LastRunUtc { get; init; }

    /// <summary>When the most recent successful run finished, or <see langword="null"/> when none has.</summary>
    public DateTimeOffset? LastSuccessUtc { get; init; }

    /// <summary>The outcome of the most recent run: <c>None</c>, <c>Success</c>, or <c>Failure</c>.</summary>
    public string? LastOutcome { get; init; }

    /// <summary>Why the most recent run failed, or <see langword="null"/>.</summary>
    public string? LastFailureReason { get; init; }

    /// <summary>The number of consecutive failed runs.</summary>
    public int ConsecutiveFailures { get; init; }

    /// <summary>The schedule's recent runs, newest first.</summary>
    public IReadOnlyList<McpBackupSetScheduleRun> RecentRuns { get; init; } = Array.Empty<McpBackupSetScheduleRun>();

    /// <summary>The sets the schedule retains, newest first; pass a set id to a set restore.</summary>
    public IReadOnlyList<McpScheduledBackupSet> RetainedSets { get; init; } = Array.Empty<McpScheduledBackupSet>();
}
