namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of <c>lattice_backup_operation_list</c>: one
/// page of the caller's tracked backup operations, newest-first.
/// </summary>
internal sealed record McpBackupOperationPage
{
    /// <summary>The operations on this page.</summary>
    public IReadOnlyList<McpBackupOperation> Operations { get; init; } = [];

    /// <summary>The cursor for the next page, or <see langword="null"/> on the last page.</summary>
    public string? NextPageToken { get; init; }
}
