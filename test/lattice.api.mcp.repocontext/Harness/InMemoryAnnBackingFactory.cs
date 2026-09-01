using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// An in-memory <see cref="IRepoContextAnnBackingFactory"/> that hands the
/// approximate plane a per-<c>(repository, embedding space)</c> pair of in-memory
/// doubles, and keeps them addressable so a test can seed the store of record,
/// mutate it, and inspect what the index persisted.
/// <para>
/// The durable store instance is deliberately stable per pair: constructing a
/// second registry over the same factory is exactly a process restart, because
/// the new index opens onto the records the previous one committed.
/// </para>
/// </summary>
internal sealed class InMemoryAnnBackingFactory : IRepoContextAnnBackingFactory
{
    private readonly Dictionary<(string RepoId, EmbeddingSpaceTag Space), Pair> _pairs = [];

    /// <summary>
    /// Returns the pair of doubles backing one repository and embedding space,
    /// creating them on first use so a test can seed the source before the plane
    /// ever opens an index.
    /// </summary>
    /// <param name="repoId">The repository.</param>
    /// <param name="space">The embedding space.</param>
    /// <returns>The store of record and the durable store for the pair.</returns>
    public Pair For(string repoId, EmbeddingSpaceTag space)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        var key = (repoId, space);
        if (!_pairs.TryGetValue(key, out var pair))
        {
            pair = new Pair(new InMemoryRepoContextVectorSource(space), new InMemoryVectorIndexStore());
            _pairs[key] = pair;
        }

        return pair;
    }

    /// <inheritdoc />
    public IRepoContextVectorSource CreateSource(string repoId, EmbeddingSpaceTag space)
        => For(repoId, space).Source;

    /// <inheritdoc />
    public IVectorIndexStore CreateStore(string repoId, EmbeddingSpaceTag space)
        => For(repoId, space).Store;

    /// <summary>The doubles backing one repository and embedding space.</summary>
    /// <param name="Source">The store-of-record view.</param>
    /// <param name="Store">The durable store the index persists itself on.</param>
    internal sealed record Pair(InMemoryRepoContextVectorSource Source, InMemoryVectorIndexStore Store);
}
