namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of <c>lattice_backup_set_schedule_list</c>:
/// the caller's readable backup-set schedules, in name order.
/// </summary>
internal sealed record McpBackupSetScheduleList
{
    /// <summary>The schedules, each with <see cref="McpBackupSetSchedule.Found"/> set.</summary>
    public IReadOnlyList<McpBackupSetSchedule> Schedules { get; init; } = Array.Empty<McpBackupSetSchedule>();
}
