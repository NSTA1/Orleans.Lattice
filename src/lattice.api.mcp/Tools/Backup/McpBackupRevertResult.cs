namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_backup_revert_restore</c> tool:
/// an acknowledgement that the shadow-cutover restore for the named target tree
/// was reverted. The operation is idempotent on the facade, so a repeated revert
/// still reports success.
/// </summary>
internal sealed record McpBackupRevertResult
{
    /// <summary>The backup id whose restore was reverted.</summary>
    public required string BackupId { get; init; }

    /// <summary>The target tree whose shadow-cutover was reverted.</summary>
    public required string TargetTreeId { get; init; }

    /// <summary>Always <see langword="true"/> once the revert completes without throwing.</summary>
    public required bool Reverted { get; init; }
}
