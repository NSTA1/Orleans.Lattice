using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Binds the approximate retrieval plane to its two backing stores for one
/// <c>(repository, embedding space)</c>: the store of record it derives itself
/// from, and the durable store it persists itself on.
/// <para>
/// It exists so the plane's own logic - opening, building, maintaining, and
/// searching an index - is free of Orleans and of the grain factory, and can be
/// exercised in full against in-memory doubles without a silo. The shipped
/// implementation is <see cref="LatticeRepoContextAnnBackingFactory"/>.
/// </para>
/// </summary>
internal interface IRepoContextAnnBackingFactory
{
    /// <summary>
    /// Creates the store-of-record view of a repository's vectors in one
    /// embedding space.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <param name="space">The embedding space the view is filtered to.</param>
    /// <returns>The store-of-record view.</returns>
    IRepoContextVectorSource CreateSource(string repoId, EmbeddingSpaceTag space);

    /// <summary>
    /// Creates the durable store one index persists itself on. The index owns its
    /// key prefix exclusively - its recovery path deletes whole key ranges under
    /// it - so an implementation must not point two indexes at the same prefix.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <param name="space">The embedding space the index covers.</param>
    /// <returns>The durable store.</returns>
    IVectorIndexStore CreateStore(string repoId, EmbeddingSpaceTag space);

    /// <summary>
    /// Retires every sibling index prefix of one repository whose embedding-space
    /// fingerprint is not <paramref name="liveSpace"/>'s, and reports how many were
    /// retired.
    /// <para>
    /// A model, dimension, or normalization change is a new embedding space and so
    /// a wholly separate index under a separate prefix. That separation is
    /// deliberate - retirement works by prefix delete, so two spaces sharing a
    /// prefix would delete each other's generations - but it leaves the abandoned
    /// space resident forever: invisible to queries, harmless to correctness, and
    /// unbounded in size. This is the only thing that reclaims it.
    /// </para>
    /// <para>
    /// An implementation must never place the live prefix, or anything outside the
    /// repository's own index root, in reach of the delete, and must treat an
    /// interrupted enumeration as a failure rather than as a short key list - a
    /// short list here is a partial delete that silently leaves a superseded space
    /// behind.
    /// </para>
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <param name="liveSpace">The embedding space whose index must be preserved.</param>
    /// <param name="cancellationToken">Cancels the walk.</param>
    /// <returns>How many superseded space prefixes were retired.</returns>
    Task<int> ReclaimSupersededSpacesAsync(
        string repoId, EmbeddingSpaceTag liveSpace, CancellationToken cancellationToken);
}
