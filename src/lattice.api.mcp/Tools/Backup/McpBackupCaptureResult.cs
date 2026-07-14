namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of a capture tool (<c>lattice_backup_create</c> /
/// <c>lattice_backup_create_incremental</c>): the content-addressed
/// <see cref="BackupId"/> of the captured backup and its compact
/// <see cref="Manifest"/> projection.
/// </summary>
internal sealed record McpBackupCaptureResult
{
    /// <summary>The content-addressed id of the captured backup.</summary>
    public required string BackupId { get; init; }

    /// <summary>The compact manifest projection of the captured backup.</summary>
    public required McpBackupManifest Manifest { get; init; }
}
