namespace Orleans.Lattice.Backup;

/// <summary>
/// One run of a backup-set schedule: when it ran, what started it, how it ended,
/// and - when it succeeded - the set it captured.
/// </summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupSetScheduleRun)]
[Immutable]
public sealed record BackupSetScheduleRun
{
    /// <summary>Initializes a new <see cref="BackupSetScheduleRun"/>.</summary>
    /// <param name="startedAtUtc">When the run started.</param>
    /// <param name="finishedAtUtc">When the run ended.</param>
    /// <param name="trigger">What started the run.</param>
    /// <param name="outcome">How the run ended.</param>
    /// <param name="setId">The id of the set the run captured, or <c>null</c> when it captured none.</param>
    /// <param name="memberCount">How many trees the run captured (or tried to).</param>
    /// <param name="attempts">How many capture attempts the run made, counting retries after a cross-tree fence could not be established.</param>
    /// <param name="failureReason">A one-line reason when the run failed, otherwise <c>null</c>.</param>
    public BackupSetScheduleRun(
        DateTimeOffset startedAtUtc,
        DateTimeOffset finishedAtUtc,
        BackupSetScheduleTrigger trigger,
        BackupScopeRunOutcome outcome,
        string? setId,
        int memberCount,
        int attempts,
        string? failureReason)
    {
        StartedAtUtc = startedAtUtc;
        FinishedAtUtc = finishedAtUtc;
        Trigger = trigger;
        Outcome = outcome;
        SetId = setId;
        MemberCount = memberCount;
        Attempts = attempts;
        FailureReason = failureReason;
    }

    /// <summary>When the run started.</summary>
    [Id(0)]
    public DateTimeOffset StartedAtUtc { get; init; }

    /// <summary>When the run ended.</summary>
    [Id(1)]
    public DateTimeOffset FinishedAtUtc { get; init; }

    /// <summary>What started the run.</summary>
    [Id(2)]
    public BackupSetScheduleTrigger Trigger { get; init; }

    /// <summary>How the run ended.</summary>
    [Id(3)]
    public BackupScopeRunOutcome Outcome { get; init; }

    /// <summary>The id of the set the run captured, or <c>null</c> when it captured none.</summary>
    [Id(4)]
    public string? SetId { get; init; }

    /// <summary>How many trees the run captured (or tried to).</summary>
    [Id(5)]
    public int MemberCount { get; init; }

    /// <summary>How many capture attempts the run made.</summary>
    [Id(6)]
    public int Attempts { get; init; }

    /// <summary>A one-line reason when the run failed, otherwise <c>null</c>.</summary>
    [Id(7)]
    public string? FailureReason { get; init; }
}
