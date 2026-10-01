namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of <c>lattice_treeadmin_operation_list</c>: one
/// page of the caller's tracked tree-administration operations, newest-first.
/// </summary>
internal sealed record McpTreeAdminOperationPage
{
    /// <summary>The operations on this page.</summary>
    public IReadOnlyList<McpTreeAdminOperation> Operations { get; init; } = [];

    /// <summary>The cursor for the next page, or <see langword="null"/> on the last page.</summary>
    public string? NextPageToken { get; init; }
}
