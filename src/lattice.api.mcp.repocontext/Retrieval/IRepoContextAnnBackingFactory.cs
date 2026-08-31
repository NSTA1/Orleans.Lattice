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
}
