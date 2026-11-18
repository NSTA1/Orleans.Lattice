namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content projection of one member of a retained backup set,
/// carried in <see cref="McpScheduledBackupSet.Members"/>.
/// </summary>
internal sealed record McpScheduledBackupSetMember
{
    /// <summary>The captured tree id.</summary>
    public required string TreeId { get; init; }

    /// <summary>The scope extent: <c>WholeTree</c>, <c>Prefix</c>, or <c>Key</c>.</summary>
    public required string ScopeKind { get; init; }

    /// <summary>The exact key or key prefix the scope covers, or <see langword="null"/> for a whole-tree scope.</summary>
    public string? KeyOrPrefix { get; init; }

    /// <summary>The member backup's id.</summary>
    public required string BackupId { get; init; }
}
