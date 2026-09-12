namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The pluggable seam that ranks the stored vectors of a repository against a
/// query vector and returns the closest matches. The shipped default
/// (<see cref="ExactKnnSemanticIndex"/>) is a brute-force exact search - perfect
/// recall at the local scale the repository-context surface targets - but a host
/// can bind an external approximate-nearest-neighbour engine instead without any
/// other part of the retrieval surface changing.
/// </summary>
/// <remarks>
/// <para>
/// An implementation is a <b>derived projection</b>: it ranks vectors that the
/// WAL-backed vector trees hold authoritatively, and every result carries the
/// canonical <see cref="RepoContextVectorMatch.SourceKey"/> so the search service
/// hydrates the record from the store of record rather than trusting the index as
/// a second copy.
/// </para>
/// <para>
/// The seam is fail-closed on embedding space: an implementation must never
/// compare a query vector against a stored vector from a different embedding
/// space (a different model, dimension, or normalization). The shipped default
/// skips any candidate whose space does not match, so a mixed-space store never
/// yields a meaningless score.
/// </para>
/// </remarks>
internal interface IRepoContextSemanticIndex
{
    /// <summary>
    /// The <see cref="RepoContextRetrievalPath"/> value this implementation serves when
    /// it answers a query: <see cref="RepoContextRetrievalPath.SemanticExact"/> for a
    /// complete-recall search, or
    /// <see cref="RepoContextRetrievalPath.SemanticApproximate"/> for a bounded-recall
    /// one. The declaration is re-validated locally through
    /// <see cref="RepoContextRetrievalPath.NormalizeSemantic(string?)"/> before it
    /// reaches a response, so an unrecognised value reports the weaker (approximate)
    /// claim rather than over-promising recall.
    /// </summary>
    string RetrievalPath { get; }

    /// <summary>
    /// Ranks the stored vectors of <paramref name="repoId"/> against
    /// <paramref name="query"/> and returns up to <paramref name="k"/> closest
    /// matches in descending score order. Candidates whose embedding space does
    /// not match <paramref name="querySpace"/> are skipped.
    /// </summary>
    /// <param name="repoId">The repository whose vectors to search.</param>
    /// <param name="query">The query vector, produced in <paramref name="querySpace"/>.</param>
    /// <param name="querySpace">The embedding space the query vector was produced in.</param>
    /// <param name="k">The maximum number of matches to return. Must be positive.</param>
    /// <param name="cancellationToken">Cancels the search.</param>
    /// <returns>The closest matches, at most <paramref name="k"/>, in descending
    /// score order.</returns>
    Task<IReadOnlyList<RepoContextVectorMatch>> SearchAsync(
        string repoId,
        ReadOnlyMemory<float> query,
        EmbeddingSpaceTag querySpace,
        int k,
        CancellationToken cancellationToken);

    /// <summary>
    /// Whether this implementation is currently withholding an exact fallback it
    /// would otherwise run for <paramref name="repoId"/>, because a gather over that
    /// repository has already proved it cannot finish.
    /// <para>
    /// <b>Per repository, deliberately, unlike <see cref="RetrievalPath"/>.</b> That
    /// property is per index and so must declare a constant - one index serves every
    /// repository, and a declaration tracking current state would be wrong the moment
    /// two repositories were in different states (see issue #2441, which declined
    /// exactly that change). This method takes the repository as an argument, so it
    /// carries a genuinely per-observation fact and none of that reasoning applies.
    /// </para>
    /// <para>
    /// <b>Read only to explain an empty result, never to skip a search.</b> The
    /// search service calls it after a search returned no matches, to tell a
    /// suppressed fallback
    /// (<see cref="RepoContextRetrievalPath.KeywordExactFallbackSuppressed"/>) from a
    /// plane that holds nothing
    /// (<see cref="RepoContextRetrievalPath.KeywordVectorPlaneUnavailable"/>). It is
    /// a racy snapshot and is safe only in that direction: a suppression lifted
    /// between the search and this read reports the older, weaker classification,
    /// which was the answer before this method existed and never over-claims.
    /// </para>
    /// <para>
    /// The default is <see langword="false"/>, which is correct for every
    /// implementation that has no such guard - including
    /// <see cref="ExactKnnSemanticIndex"/>, which is the fallback rather than a
    /// caller of one.
    /// </para>
    /// </summary>
    /// <param name="repoId">The repository being searched.</param>
    /// <returns><see langword="true"/> when an exact fallback is being withheld.</returns>
    bool IsExactFallbackSuppressed(string repoId) => false;
}
