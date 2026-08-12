namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The result of <c>repocontext_list_repos</c>: every repository currently
/// registered in the context store, in ascending repository-id order, each with
/// its last-ingested marker and recorded file count.
/// </summary>
/// <remarks>
/// An MCP protocol payload projected to JSON by the SDK, not an Orleans grain
/// message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextRepoListResult
{
    /// <summary>The registered repositories, ordered by repository id.</summary>
    public required IReadOnlyList<RepoContextRepoSummary> Repos { get; init; }

    /// <summary>The number of registered repositories.</summary>
    public required int Count { get; init; }
}
