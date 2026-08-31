using Orleans.Lattice.Vector.Persistence;
using Orleans.Serialization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The shipped <see cref="IRepoContextAnnBackingFactory"/>: binds the approximate
/// retrieval plane to real Lattice trees. The store of record is the reserved
/// vector-metadata and vector-payload pair; the durable index sits on its own
/// dedicated <see cref="RepoContextTrees.VectorIndex"/> tree, under a key prefix
/// unique to the <c>(repository, embedding space)</c> pair.
/// <para>
/// The per-pair prefix is not cosmetic. A durable index owns its prefix
/// exclusively because its recovery path deletes whole key ranges under it, so
/// two indexes sharing a prefix would delete each other's generations. Keying the
/// prefix by the embedding space as well as the repository is what lets a host
/// re-embed under a new model without the old index and the new one colliding.
/// </para>
/// </summary>
internal sealed class LatticeRepoContextAnnBackingFactory : IRepoContextAnnBackingFactory
{
    private readonly IGrainFactory _grainFactory;
    private readonly Serializer _serializer;

    /// <summary>Creates the backing factory.</summary>
    /// <param name="grainFactory">The grain factory used to reach the vector trees. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer used to decode vector records. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public LatticeRepoContextAnnBackingFactory(IGrainFactory grainFactory, Serializer serializer)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(serializer);
        _grainFactory = grainFactory;
        _serializer = serializer;
    }

    /// <summary>
    /// The key prefix the index for one repository and embedding space owns
    /// exclusively inside the index tree. The space contributes a stable
    /// fingerprint of its model, dimension, and normalization convention, so two
    /// spaces never share a prefix and the prefix never carries a model id
    /// verbatim into a key.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <param name="space">The embedding space.</param>
    /// <returns>The exclusive key prefix.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    internal static string KeyPrefix(string repoId, EmbeddingSpaceTag space)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        var fingerprint = VectorCodec.SourceId(
            $"{space.ModelId}|{space.Dimension}|{space.Normalization}");
        return $"repo/{repoId}/{RepoContextAnnOptions.KeyPrefixRoot}{fingerprint}/";
    }

    /// <inheritdoc />
    public IRepoContextVectorSource CreateSource(string repoId, EmbeddingSpaceTag space)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        return new RepoContextVectorSource(_grainFactory, _serializer, repoId, space);
    }

    /// <inheritdoc />
    public IVectorIndexStore CreateStore(string repoId, EmbeddingSpaceTag space)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        return new LatticeVectorIndexStore(_grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorIndex));
    }
}
