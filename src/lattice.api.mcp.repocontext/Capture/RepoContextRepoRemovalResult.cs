namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The result of <c>repocontext_remove_repo</c>: the repository whose records
/// were dropped and how many entries were tombstoned across every context tree
/// (structural, memory, and the vector trees) including the repository root
/// marker. A count of zero means the repository was already absent.
/// </summary>
/// <remarks>
/// An MCP protocol payload projected to JSON by the SDK, not an Orleans grain
/// message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextRepoRemovalResult
{
    /// <summary>The repository identity whose records were removed.</summary>
    public required string RepoId { get; init; }

    /// <summary>
    /// The total number of entries tombstoned across every context tree, including
    /// the repository root marker.
    /// </summary>
    public required int EntriesDeleted { get; init; }
}
