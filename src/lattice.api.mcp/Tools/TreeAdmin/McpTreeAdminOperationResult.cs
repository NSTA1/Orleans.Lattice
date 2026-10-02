namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of <c>lattice_treeadmin_operation_status</c>
/// and <c>lattice_treeadmin_operation_cancel</c>: whether the operation was found,
/// and its view when it was. Not found covers an unknown id and an operation the
/// caller may not see alike.
/// </summary>
internal sealed record McpTreeAdminOperationResult
{
    /// <summary>The operation id that was asked for.</summary>
    public required string OperationId { get; init; }

    /// <summary><see langword="true"/> when the operation is visible to the caller.</summary>
    public bool Found { get; init; }

    /// <summary>The operation, or <see langword="null"/> when not found.</summary>
    public McpTreeAdminOperation? Operation { get; init; }
}
