namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The result of <c>repocontext_reset_index</c>: the repository whose code index
/// was dropped and how many entries were tombstoned across the code-index trees
/// (structural, symbol, content, cross-reference, session, and every vector
/// tree) plus the repository root marker. The agent-memory tree is preserved
/// and its entries are never counted. A count of zero means neither an index
/// nor a root marker was present.
/// </summary>
/// <remarks>
/// An MCP protocol payload projected to JSON by the SDK, not an Orleans grain
/// message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextIndexResetResult
{
    /// <summary>The repository identity whose code index was reset.</summary>
    public required string RepoId { get; init; }

    /// <summary>
    /// The total number of entries tombstoned across the code-index trees,
    /// including the repository root marker. Records in the memory tree are
    /// preserved and are never counted here.
    /// </summary>
    public required int EntriesDeleted { get; init; }
}
