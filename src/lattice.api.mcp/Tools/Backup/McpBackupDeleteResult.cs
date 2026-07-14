namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_backup_delete</c> tool: the
/// targeted backup id and whether a backup was actually removed
/// (<see langword="false"/> when no backup with the id existed).
/// </summary>
internal sealed record McpBackupDeleteResult
{
    /// <summary>The backup id the delete targeted.</summary>
    public required string BackupId { get; init; }

    /// <summary>Whether a backup was removed (<see langword="false"/> when none existed).</summary>
    public required bool Deleted { get; init; }
}
