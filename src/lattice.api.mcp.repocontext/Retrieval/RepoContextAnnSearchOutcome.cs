namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The outcome of one query against the approximate retrieval plane: which path
/// inside the plane answered, and the matches it produced.
/// <para>
/// A <see cref="RepoContextAnnServingState.Bootstrapping"/> outcome carries no
/// matches: the plane could not answer, so the caller serves from the exact
/// scan instead. The other two states carry the plane's own answer.
/// </para>
/// </summary>
/// <param name="State">Which path inside the plane answered.</param>
/// <param name="Matches">The matches the plane produced; empty when it did not answer.</param>
internal readonly record struct RepoContextAnnSearchOutcome(
    RepoContextAnnServingState State,
    IReadOnlyList<RepoContextVectorMatch> Matches)
{
    /// <summary>
    /// The outcome reported when no usable index exists yet for a repository and
    /// embedding space, so the caller must serve from the exact scan. Cached, so
    /// reporting it costs no allocation on the per-query path.
    /// </summary>
    internal static RepoContextAnnSearchOutcome Bootstrapping { get; } =
        new(RepoContextAnnServingState.Bootstrapping, Array.Empty<RepoContextVectorMatch>());
}
