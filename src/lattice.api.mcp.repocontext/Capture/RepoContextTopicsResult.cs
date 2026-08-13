namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// One memory topic and how many live entries it holds, returned as an element of
/// <see cref="RepoContextTopicsResult"/>.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextTopicSummary
{
    /// <summary>The memory topic bucket (the <c>{topic}</c> segment of a memory key).</summary>
    public required string Topic { get; init; }

    /// <summary>The number of live memory entries filed under the topic.</summary>
    public required int EntryCount { get; init; }
}

/// <summary>
/// The result of the <c>repocontext_list_topics</c> tool: the distinct agent
/// memory topics available for a repository, each with its live entry count, in
/// ascending topic order.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextTopicsResult
{
    /// <summary>The repository the topics belong to.</summary>
    public required string RepoId { get; init; }

    /// <summary>The distinct memory topics, in ascending ordinal order.</summary>
    public required IReadOnlyList<RepoContextTopicSummary> Topics { get; init; }
}
