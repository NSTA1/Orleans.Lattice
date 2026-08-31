namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The result of a <c>repocontext_search</c> query: the ranked hits plus the
/// <see cref="Mode"/> that produced them, so a caller can tell a semantic search
/// from a degraded keyword scan. The mode is <c>"semantic"</c> when the configured
/// index answered the query, <c>"keyword"</c> when search fell back to structural
/// token matching (no index or embedder configured, the embedder was unreachable,
/// or no vectors matched the query's embedding space), and <c>"empty"</c> when the
/// query matched nothing.
/// </summary>
/// <remarks>
/// This is an MCP protocol payload projected to JSON by the SDK, not an Orleans
/// grain message, so it carries no Orleans serialization attributes.
/// </remarks>
public sealed record RepoContextSearchResult
{
    /// <summary>The repository the query ran against.</summary>
    public required string RepoId { get; init; }

    /// <summary>The original query text.</summary>
    public required string Query { get; init; }

    /// <summary>
    /// How the hits were produced: <c>"semantic"</c> (the vector index answered),
    /// <c>"keyword"</c> (a degraded structural token scan), or <c>"empty"</c> (no
    /// match).
    /// </summary>
    public required string Mode { get; init; }

    /// <summary>
    /// The precise retrieval path that answered, from the closed
    /// <see cref="RepoContextRetrievalPath"/> vocabulary. It rides <b>alongside</b>
    /// <see cref="Mode"/> without changing it, and disambiguates what <see cref="Mode"/>
    /// cannot: whether a semantic answer had complete or bounded recall, and - when the
    /// answer was a keyword scan - whether that is an intended keyword-only deployment
    /// (<see cref="RepoContextRetrievalPath.KeywordNoEmbedder"/>) or a real capability
    /// loss (<see cref="RepoContextRetrievalPath.KeywordVectorPlaneUnavailable"/> or
    /// <see cref="RepoContextRetrievalPath.KeywordIndexDegraded"/>). Always server-derived
    /// and never <see langword="null"/>.
    /// </summary>
    public required string RetrievalPath { get; init; }

    /// <summary>The ranked hits in descending score order; empty when nothing matched.</summary>
    public required IReadOnlyList<RepoContextSearchHit> Hits { get; init; }
}
