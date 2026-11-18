namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of <c>lattice_backup_set_schedule_cancel</c>,
/// <c>lattice_backup_set_schedule_remove</c> and
/// <c>lattice_backup_set_schedule_run_now</c>. <see cref="Found"/> is
/// <see langword="false"/> when no schedule has the name; <see cref="Applied"/>
/// reports whether the schedule was paused (cancel), removed (remove), or a run
/// was started (run-now - <see langword="false"/> when a run was already in flight).
/// </summary>
internal sealed record McpBackupSetScheduleActionResult
{
    /// <summary>The schedule's name.</summary>
    public required string Name { get; init; }

    /// <summary>Whether a schedule with the name exists.</summary>
    public required bool Found { get; init; }

    /// <summary>Whether the action took effect.</summary>
    public required bool Applied { get; init; }
}
