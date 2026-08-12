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
}
