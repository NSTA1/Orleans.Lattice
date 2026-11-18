namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content projection of a backup-set schedule's run in
/// flight, carried in <see cref="McpBackupSetSchedule.CurrentRun"/>.
/// </summary>
internal sealed record McpBackupSetScheduleRunProgress
{
    /// <summary>When the run started.</summary>
    public DateTimeOffset StartedAtUtc { get; init; }

    /// <summary>What started the run: <c>Schedule</c> or <c>Manual</c>.</summary>
    public required string Trigger { get; init; }

    /// <summary>The capture attempt in progress (1 for the first).</summary>
    public int Attempt { get; init; }

    /// <summary>The phase in progress, for example <c>CapturingMembers</c> or <c>WritingMembers</c>.</summary>
    public required string Phase { get; init; }

    /// <summary>How many units of the phase are done.</summary>
    public long CompletedUnits { get; init; }

    /// <summary>How many units the phase has, or <see langword="null"/> when it has no known total.</summary>
    public long? TotalUnits { get; init; }

    /// <summary>The unit the phase counts in, for example <c>members</c>, or <see langword="null"/>.</summary>
    public string? UnitName { get; init; }

    /// <summary>The tree being captured or written, or <see langword="null"/>.</summary>
    public string? CurrentTreeId { get; init; }

    /// <summary>When the progress was last reported.</summary>
    public DateTimeOffset UpdatedAtUtc { get; init; }
}
