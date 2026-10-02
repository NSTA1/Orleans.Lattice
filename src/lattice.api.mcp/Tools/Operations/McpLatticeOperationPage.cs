namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The MCP structured-content result of an operation list tool: one page of the
/// caller's tracked operations of the tool's kind, newest-first.
/// </summary>
internal sealed record McpLatticeOperationPage
{
    /// <summary>The operations on this page.</summary>
    public IReadOnlyList<McpLatticeOperation> Operations { get; init; } = [];

    /// <summary>The cursor for the next page, or <see langword="null"/> on the last page.</summary>
    public string? NextPageToken { get; init; }
}