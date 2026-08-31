using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The approximate retrieval plane behind <see cref="AnnRepoContextSemanticIndex"/>:
/// a persisted approximate nearest-neighbour index per <c>(repository, embedding
/// space)</c>, maintained in place as vectors are written and retired, and
/// queried instead of re-scanning the whole vector-metadata prefix on every cache
/// miss.
/// <para>
/// <b>Derived, never authoritative.</b> Every match carries the canonical
/// <see cref="RepoContextVectorMatch.SourceKey"/>, so the search service still
/// hydrates the record from the store of record. Discarding the whole plane is
/// always safe: it recomputes from the vector trees.
/// </para>
/// <para>
/// <b>Fails soft rather than answering from a partial corpus.</b> A repository
/// whose index is still building reports
/// <see cref="RepoContextAnnServingState.Bootstrapping"/> and returns nothing, so
/// the caller serves the exact scan instead of quietly losing recall to a corpus
/// the plane has not finished ingesting.
/// </para>
/// </summary>
internal interface IRepoContextAnnIndex
{
    /// <summary>
    /// Searches the plane for a repository and embedding space, returning up to
    /// <paramref name="k"/> matches in descending score order along with the path
    /// that answered.
    /// </summary>
    /// <param name="repoId">The repository to search. Must not be <see langword="null"/>.</param>
    /// <param name="query">The query vector.</param>
    /// <param name="space">The embedding space the query was produced in.</param>
    /// <param name="k">The maximum number of matches. Must be positive.</param>
    /// <param name="cancellationToken">Cancels the search.</param>
    /// <returns>The plane's answer, or
    /// <see cref="RepoContextAnnSearchOutcome.Bootstrapping"/> when it has none.</returns>
    ValueTask<RepoContextAnnSearchOutcome> SearchAsync(
        string repoId,
        ReadOnlyMemory<float> query,
        EmbeddingSpaceTag space,
        int k,
        CancellationToken cancellationToken);

    /// <summary>
    /// Reports the build progress of the index for a repository and embedding
    /// space, so a host can tell "warming up" from "steady state" without issuing
    /// a query. Returns <see langword="false"/> when no index has been opened for
    /// the pair yet.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <param name="space">The embedding space.</param>
    /// <param name="progress">The build progress when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when an index exists for the pair.</returns>
    bool TryGetProgress(string repoId, EmbeddingSpaceTag space, out VectorIndexBuildProgress progress);

    /// <summary>
    /// Applies a completed local vector write to the plane so the index never
    /// lags the store of record on a change it could observe: the vectors a
    /// source now holds are upserted, and the identifiers it no longer holds are
    /// retired before they can be returned again.
    /// <para>
    /// Called at the same seam that invalidates the warm candidate cache. It is a
    /// no-op for a repository and space the plane holds no index for - a later
    /// build streams the store of record and picks the write up there.
    /// </para>
    /// </summary>
    /// <param name="repoId">The repository the write landed in. Must not be <see langword="null"/>.</param>
    /// <param name="space">The embedding space the vectors were written under.</param>
    /// <param name="upserts">The vectors the source now holds. Must not be <see langword="null"/>.</param>
    /// <param name="retired">The vector identifiers the source no longer holds. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the maintenance.</param>
    Task ApplyWriteAsync(
        string repoId,
        EmbeddingSpaceTag space,
        IReadOnlyList<RepoContextAnnVectorUpdate> upserts,
        IReadOnlyList<string> retired,
        CancellationToken cancellationToken);

    /// <summary>
    /// Retires vector identifiers across every embedding space the plane holds an
    /// index for in a repository. Used when a whole source is removed, where the
    /// space the vectors were written under is not read back: an identifier is
    /// unique within a repository, so applying the retirement everywhere is exact,
    /// and it is a no-op in an index that never held it.
    /// </summary>
    /// <param name="repoId">The repository the source was removed from. Must not be <see langword="null"/>.</param>
    /// <param name="retired">The vector identifiers the repository no longer holds. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the maintenance.</param>
    Task ApplyRetirementAsync(
        string repoId, IReadOnlyList<string> retired, CancellationToken cancellationToken);
}
