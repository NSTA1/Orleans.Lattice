namespace Orleans.Lattice.Backup;

/// <summary>
/// The live progress of a backup-set schedule's run that is in flight: what
/// started it, which capture attempt it is on, the phase it is in, and how far
/// through that phase it is. Reported on <see cref="BackupSetScheduleStatus.CurrentRun"/>
/// while the run is in flight and cleared when it ends; the finished run is then
/// recorded in <see cref="BackupSetScheduleStatus.RecentRuns"/>.
/// </summary>
/// <remarks>
/// A run moves through <see cref="BackupOperationPhases.ResolvingMembers"/>, then
/// - for a cross-tree-consistent set - <see cref="BackupOperationPhases.CapturingMembers"/>
/// while it pins every member's snapshot under the set's decision gate and
/// <see cref="BackupOperationPhases.WritingMembers"/> while it writes the members,
/// and finally <see cref="BackupOperationPhases.ApplyingRetention"/>. A set that is
/// not cross-tree consistent captures and writes each member in
/// <see cref="BackupOperationPhases.CapturingMembers"/>. Member phases count in
/// <see cref="BackupOperationUnits.Members"/>, and <see cref="CurrentTreeId"/> names
/// the tree being written.
/// </remarks>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupSetScheduleRunProgress)]
[Immutable]
public sealed record BackupSetScheduleRunProgress
{
    /// <summary>Initializes a new <see cref="BackupSetScheduleRunProgress"/>.</summary>
    /// <param name="startedAtUtc">When the run started.</param>
    /// <param name="trigger">What started the run.</param>
    /// <param name="attempt">The capture attempt in progress, counting retries after a cross-tree fence could not be established (1 for the first).</param>
    /// <param name="phase">The phase in progress (a <see cref="BackupOperationPhases"/> value).</param>
    /// <param name="completedUnits">How many units of the phase are done.</param>
    /// <param name="totalUnits">How many units the phase has, or <c>null</c> when it has no known total.</param>
    /// <param name="unitName">The unit the phase counts in (a <see cref="BackupOperationUnits"/> value), or <c>null</c>.</param>
    /// <param name="currentTreeId">The (effective) tree being captured or written, or <c>null</c>.</param>
    /// <param name="updatedAtUtc">When the progress was last reported.</param>
    /// <exception cref="ArgumentException"><paramref name="phase"/> is <c>null</c> or empty.</exception>
    public BackupSetScheduleRunProgress(
        DateTimeOffset startedAtUtc,
        BackupSetScheduleTrigger trigger,
        int attempt,
        string phase,
        long completedUnits,
        long? totalUnits,
        string? unitName,
        string? currentTreeId,
        DateTimeOffset updatedAtUtc)
    {
        ArgumentException.ThrowIfNullOrEmpty(phase);
        StartedAtUtc = startedAtUtc;
        Trigger = trigger;
        Attempt = attempt;
        Phase = phase;
        CompletedUnits = completedUnits;
        TotalUnits = totalUnits;
        UnitName = unitName;
        CurrentTreeId = currentTreeId;
        UpdatedAtUtc = updatedAtUtc;
    }

    /// <summary>When the run started.</summary>
    [Id(0)]
    public DateTimeOffset StartedAtUtc { get; init; }

    /// <summary>What started the run.</summary>
    [Id(1)]
    public BackupSetScheduleTrigger Trigger { get; init; }

    /// <summary>The capture attempt in progress (1 for the first).</summary>
    [Id(2)]
    public int Attempt { get; init; }

    /// <summary>The phase in progress (a <see cref="BackupOperationPhases"/> value).</summary>
    [Id(3)]
    public string Phase { get; init; }

    /// <summary>How many units of the phase are done.</summary>
    [Id(4)]
    public long CompletedUnits { get; init; }

    /// <summary>How many units the phase has, or <c>null</c> when it has no known total.</summary>
    [Id(5)]
    public long? TotalUnits { get; init; }

    /// <summary>The unit the phase counts in, or <c>null</c>.</summary>
    [Id(6)]
    public string? UnitName { get; init; }

    /// <summary>The (effective) tree being captured or written, or <c>null</c>.</summary>
    [Id(7)]
    public string? CurrentTreeId { get; init; }

    /// <summary>When the progress was last reported.</summary>
    [Id(8)]
    public DateTimeOffset UpdatedAtUtc { get; init; }
}
