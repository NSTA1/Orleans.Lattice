namespace Orleans.Lattice.Backup;

/// <summary>Persistent state of one tree's <c>RestoreCopyRetirementGrain</c> (issue #6103).</summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.RestoreCopyRetirementState)]
internal sealed class RestoreCopyRetirementState
{
    /// <summary>Whether a pass is owed: set by every schedule, cleared once nothing is left to retire.</summary>
    [Id(0)] public bool Pending { get; set; }

    /// <summary>Consecutive failed passes, driving the exponential back-off. Reset by a pass that succeeds.</summary>
    [Id(1)] public int ConsecutiveFailures { get; set; }

    /// <summary>The earliest time the next pass may act, while backing off.</summary>
    [Id(2)] public DateTimeOffset? NextAttemptUtc { get; set; }

    /// <summary>
    /// A revertible restore shadow committed before its revert target was recorded,
    /// first observed by a pass; its revert window runs from <see cref="UnknownRevertObservedAtUtc"/>.
    /// </summary>
    [Id(3)] public string? UnknownRevertShadow { get; set; }

    /// <summary>When <see cref="UnknownRevertShadow"/> was first observed.</summary>
    [Id(4)] public DateTimeOffset? UnknownRevertObservedAtUtc { get; set; }
}
