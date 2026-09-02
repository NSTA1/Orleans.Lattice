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
    /// verbatim into a key. The layout itself lives on
    /// <see cref="RepoContextAnnIndexKeys"/>, which also owns the sibling-space
    /// enumeration the reclamation walk depends on.
    /// </summary>
    /// <param name="repoId">The repository. Must not be <see langword="null"/>.</param>
    /// <param name="space">The embedding space.</param>
    /// <returns>The exclusive key prefix.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="repoId"/> is null.</exception>
    internal static string KeyPrefix(string repoId, EmbeddingSpaceTag space)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        return RepoContextAnnIndexKeys.IndexPrefix(repoId, space);
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

    /// <inheritdoc />
    /// <remarks>
    /// <para>
    /// The walk is a <b>skip scan</b>, not an enumeration. Every space a repository
    /// has been indexed under is a sibling in one contiguous ordinal range beneath
    /// <see cref="RepoContextAnnIndexKeys.RepositoryRoot"/>, so reading the single
    /// first key at or after the cursor names a whole space, and the cursor then
    /// jumps to the exclusive upper bound of that space's prefix. The cost is
    /// therefore one bounded read per space the repository has ever used - two or
    /// three - rather than one per persisted record, of which there are hundreds of
    /// thousands.
    /// </para>
    /// <para>
    /// It is a <b>keys-only</b> walk for the same reason: the values under this
    /// root are the index's own vector chunks, and streaming them to learn a
    /// fingerprint would read the very hundreds of megabytes the reclamation exists
    /// to remove.
    /// </para>
    /// <para>
    /// It is walked through the abort-resilient
    /// <see cref="LatticeExtensions.ScanKeysAsync"/> rather than the raw stream,
    /// because a reclaimed remote enumerator yields a SHORT result, and a short
    /// result here reads as "no more spaces" - so an abort would silently leave a
    /// superseded index behind rather than failing. Cancellation and enumeration
    /// aborts propagate; the caller treats a fault as "not reclaimed" and retries
    /// on a later pass.
    /// </para>
    /// </remarks>
    public async Task<int> ReclaimSupersededSpacesAsync(
        string repoId, EmbeddingSpaceTag liveSpace, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);

        var tree = _grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorIndex);
        var root = RepoContextAnnIndexKeys.RepositoryRoot(repoId);
        var rootEnd = LatticeKeyRange.PrefixUpperBound(root);
        var livePrefix = RepoContextAnnIndexKeys.IndexPrefix(repoId, liveSpace);

        var cursor = root;
        var retired = 0;
        while (cursor is not null && (rootEnd is null || string.CompareOrdinal(cursor, rootEnd) < 0))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var observed = await FirstKeyAsync(tree, cursor, rootEnd, cancellationToken).ConfigureAwait(false);
            if (observed is null)
            {
                break;
            }

            if (!RepoContextAnnIndexKeys.TrySpacePrefix(root, observed, out var spacePrefix))
            {
                // A key under the root that names no space. It is not this plane's
                // to delete, so step past exactly it and carry on rather than
                // guessing at a prefix that could span every space.
                cursor = LatticeKeyRange.PrefixUpperBound(observed);
                continue;
            }

            if (!string.Equals(spacePrefix, livePrefix, StringComparison.Ordinal))
            {
                var spaceEnd = LatticeKeyRange.PrefixUpperBound(spacePrefix);
                if (spaceEnd is not null)
                {
                    await tree.DeleteRangeAsync(spacePrefix, spaceEnd, cancellationToken).ConfigureAwait(false);
                    retired++;
                }
            }

            cursor = LatticeKeyRange.PrefixUpperBound(spacePrefix);
        }

        return retired;
    }

    /// <summary>
    /// Reads the single first key in a half-open range, or <see langword="null"/>
    /// when the range is empty. Enumerating one key and stopping is what keeps the
    /// reclamation walk proportional to the number of spaces rather than to the
    /// number of records.
    /// </summary>
    private static async Task<string?> FirstKeyAsync(
        ILattice tree, string startInclusive, string? endExclusive, CancellationToken cancellationToken)
    {
        var keys = tree.ScanKeysAsync(startInclusive, endExclusive, cancellationToken: cancellationToken);
        await foreach (var key in keys.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            return key;
        }

        return null;
    }
}
