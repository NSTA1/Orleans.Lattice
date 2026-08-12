namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A single registered repository as reported by <c>repocontext_list_repos</c>:
/// its identity, the last time it was ingested, and the number of files recorded
/// for it. An agent uses the list to discover which repositories under the
/// mounted workspace are queryable before it searches, recalls, or removes one.
/// </summary>
/// <remarks>
/// An MCP protocol payload projected to JSON by the SDK, not an Orleans grain
/// message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextRepoSummary
{
    /// <summary>The repository identity records are filed under.</summary>
    public required string RepoId { get; init; }

    /// <summary>
    /// The last-ingested marker recorded on the repository root, or
    /// <see langword="null"/> when none was written.
    /// </summary>
    public string? LastIngested { get; init; }

    /// <summary>
    /// The number of files recorded for the repository as of its last ingestion,
    /// or <see langword="null"/> when the count was not recorded (a repository
    /// ingested before the count was tracked).
    /// </summary>
    public long? FileCount { get; init; }
}
