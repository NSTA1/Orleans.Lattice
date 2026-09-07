namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The result of <c>repocontext_reset_index</c>: the repository whose code index
/// was dropped and how many entries were tombstoned across the code-index trees
/// (structural, symbol, content, cross-reference, session, and every vector
/// tree). The repository root marker is preserved - rewritten with its
/// index-derived fields cleared so the repository stays listed by
/// <c>repocontext_list_repos</c> - and so is never counted here, and neither is
/// any record in the agent-memory tree. A count of zero means no code index was
/// present.
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
    /// The total number of entries tombstoned across the code-index trees. The
    /// repository root marker is preserved rather than deleted, so it is not
    /// counted; records in the memory tree are preserved and are never counted
    /// here either.
    /// </summary>
    public required int EntriesDeleted { get; init; }
}
