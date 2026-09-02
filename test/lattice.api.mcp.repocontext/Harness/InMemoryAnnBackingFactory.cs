using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// An in-memory <see cref="IRepoContextAnnBackingFactory"/> that hands the
/// approximate plane a per-<c>(repository, embedding space)</c> store-of-record
/// double over <b>one shared durable store</b>, and keeps both addressable so a
/// test can seed the store of record, mutate it, and inspect what the index
/// persisted.
/// <para>
/// The single shared durable store is faithful rather than convenient: in a real
/// host every index in every space lives on one
/// <see cref="RepoContextTrees.VectorIndex"/> tree and is isolated only by its key
/// prefix. Sharing it here is what makes the superseded-prefix reclamation
/// observable at all - two spaces have to be siblings in one keyspace before one
/// of them can be a sibling to retire.
/// </para>
/// <para>
/// The store instance is stable across registries, so constructing a second
/// registry over the same factory is exactly a process restart: the new index
/// opens onto the records the previous one committed.
/// </para>
/// </summary>
internal sealed class InMemoryAnnBackingFactory : IRepoContextAnnBackingFactory
{
    private readonly Dictionary<(string RepoId, EmbeddingSpaceTag Space), Pair> _pairs = [];

    /// <summary>
    /// The one durable store every index in this rig persists itself on, exactly
    /// as every index in a host shares one index tree.
    /// </summary>
    public InMemoryVectorIndexStore Shared { get; } = new();

    /// <summary>How many reclamation sweeps have been requested.</summary>
    public int ReclaimCalls { get; private set; }

    /// <summary>
    /// Returns the pair of doubles backing one repository and embedding space,
    /// creating the store-of-record double on first use so a test can seed it
    /// before the plane ever opens an index.
    /// </summary>
    /// <param name="repoId">The repository.</param>
    /// <param name="space">The embedding space.</param>
    /// <returns>The store of record and the shared durable store.</returns>
    public Pair For(string repoId, EmbeddingSpaceTag space)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        var key = (repoId, space);
        if (!_pairs.TryGetValue(key, out var pair))
        {
            pair = new Pair(new InMemoryRepoContextVectorSource(space), Shared);
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

    /// <inheritdoc />
    /// <remarks>
    /// The same shape the Lattice-backed factory uses, and deliberately through the
    /// same <see cref="RepoContextAnnIndexKeys.TrySpacePrefix"/>, so the prefix
    /// arithmetic that decides what is in reach of a range delete is exercised here
    /// rather than reimplemented.
    /// </remarks>
    public async Task<int> ReclaimSupersededSpacesAsync(
        string repoId, EmbeddingSpaceTag liveSpace, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ReclaimCalls++;

        var root = RepoContextAnnIndexKeys.RepositoryRoot(repoId);
        var live = RepoContextAnnIndexKeys.IndexPrefix(repoId, liveSpace);
        var doomed = new List<string>();

        foreach (var key in Shared.Keys)
        {
            if (!RepoContextAnnIndexKeys.TrySpacePrefix(root, key, out var spacePrefix))
            {
                continue;
            }

            if (!string.Equals(spacePrefix, live, StringComparison.Ordinal)
                && !doomed.Contains(spacePrefix, StringComparer.Ordinal))
            {
                doomed.Add(spacePrefix);
            }
        }

        foreach (var prefix in doomed)
        {
            await Shared.DeletePrefixAsync(prefix, cancellationToken).ConfigureAwait(false);
        }

        return doomed.Count;
    }

    /// <summary>The doubles backing one repository and embedding space.</summary>
    /// <param name="Source">The store-of-record view.</param>
    /// <param name="Store">The durable store the index persists itself on.</param>
    internal sealed record Pair(InMemoryRepoContextVectorSource Source, InMemoryVectorIndexStore Store);
}
