namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content projection of one backup-set schedule run, carried
/// in <see cref="McpBackupSetSchedule.RecentRuns"/>.
/// </summary>
internal sealed record McpBackupSetScheduleRun
{
    /// <summary>When the run started.</summary>
    public DateTimeOffset StartedAtUtc { get; init; }

    /// <summary>When the run finished.</summary>
    public DateTimeOffset FinishedAtUtc { get; init; }

    /// <summary>What started the run: <c>Schedule</c> or <c>Manual</c>.</summary>
    public required string Trigger { get; init; }

    /// <summary>The run's outcome: <c>None</c>, <c>Success</c>, or <c>Failure</c>.</summary>
    public required string Outcome { get; init; }

    /// <summary>The id of the set the run captured, or <see langword="null"/> when it captured none.</summary>
    public string? SetId { get; init; }

    /// <summary>The number of member trees the run captured.</summary>
    public int MemberCount { get; init; }

    /// <summary>The number of capture attempts the run made.</summary>
    public int Attempts { get; init; }

    /// <summary>Why the run failed, or <see langword="null"/>.</summary>
    public string? FailureReason { get; init; }
}
