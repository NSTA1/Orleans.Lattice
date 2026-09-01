namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The result of asking a source strategy to prepare the content for the next
/// index generation.
/// </summary>
internal enum RepoContextSourceOutcome
{
    /// <summary>
    /// Content is staged and the (possibly rewritten) job request should be indexed.
    /// </summary>
    Proceed = 0,

    /// <summary>
    /// The source resolved to the exact revision already indexed, so there is
    /// nothing to do. The last-good index keeps serving unchanged.
    /// </summary>
    UpToDate = 1,

    /// <summary>
    /// Preparation failed - missing credentials, an unreachable remote, a partial
    /// fetch, or a role that may not fetch. Nothing is indexed and nothing is
    /// pruned, so the last-good index keeps serving. A failure never falls back to
    /// a different source.
    /// </summary>
    Failed = 2,
}
