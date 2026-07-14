namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of the <c>lattice_backup_describe</c> tool: the
/// described backup's compact <see cref="Manifest"/> plus the base-first ordered
/// <see cref="ChainBackupIds"/> that must be replayed to restore it.
/// <see cref="Found"/> is <see langword="false"/> (and the other members carry
/// their empty defaults) when no backup with the requested id exists.
/// </summary>
internal sealed record McpBackupChain
{
    /// <summary>Whether a backup with the requested id was found.</summary>
    public required bool Found { get; init; }

    /// <summary>
    /// The described backup's compact manifest projection, or
    /// <see langword="null"/> when <see cref="Found"/> is <see langword="false"/>.
    /// </summary>
    public McpBackupManifest? Manifest { get; init; }

    /// <summary>
    /// The base-first ordered chain of backup ids replayed to restore the
    /// backup, ending with the described backup's own id. Empty when the backup
    /// was not found.
    /// </summary>
    public IReadOnlyList<string> ChainBackupIds { get; init; } = Array.Empty<string>();
}
