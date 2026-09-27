using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
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
/// <para>
/// The index tree is registered with a leaf key bound sized to its records
/// before anything touches it. Its records are durable-index chunks of up to
/// 64 KiB, far larger than the
/// small values the core default of 128 keys per leaf was chosen for, so at that
/// default a leaf splits on its key count at about an eighth of the byte bound
/// every other tree splits on (issue #2829).
/// </para>
/// </summary>
internal sealed class LatticeRepoContextAnnBackingFactory : IRepoContextAnnBackingFactory
{
    private readonly IGrainFactory _grainFactory;
    private readonly Serializer _serializer;
    private readonly IOptionsMonitor<LatticeOptions> _options;
    private readonly Func<int, Task> _registerIndexTree;
    private readonly Func<Task> _ensureIndexTreePinned;
    private Task? _indexTreePinned;

    /// <summary>Creates the backing factory.</summary>
    /// <param name="grainFactory">The grain factory used to reach the vector trees. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer used to decode vector records. Must not be <see langword="null"/>.</param>
    /// <param name="options">The per-tree Lattice options the index tree's byte bound is read from. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public LatticeRepoContextAnnBackingFactory(
        IGrainFactory grainFactory, Serializer serializer, IOptionsMonitor<LatticeOptions> options)
        : this(grainFactory, serializer, options, registerIndexTree: null)
    {
    }

    /// <summary>
    /// Creates the backing factory with a substitute for the registry call that
    /// pins the index tree's structure, so the pin's value and its ordering can
    /// be observed without a cluster.
    /// </summary>
    /// <param name="grainFactory">The grain factory used to reach the vector trees. Must not be <see langword="null"/>.</param>
    /// <param name="serializer">The Orleans serializer used to decode vector records. Must not be <see langword="null"/>.</param>
    /// <param name="options">The per-tree Lattice options the index tree's byte bound is read from. Must not be <see langword="null"/>.</param>
    /// <param name="registerIndexTree">
    /// Registers <see cref="RepoContextTrees.VectorIndex"/> with the given leaf key
    /// bound, or <see langword="null"/> for the tree registry itself.
    /// </param>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    internal LatticeRepoContextAnnBackingFactory(
        IGrainFactory grainFactory,
        Serializer serializer,
        IOptionsMonitor<LatticeOptions> options,
        Func<int, Task>? registerIndexTree)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(options);
        _grainFactory = grainFactory;
        _serializer = serializer;
        _options = options;
        _registerIndexTree = registerIndexTree ?? RegisterIndexTreeAsync;
        _ensureIndexTreePinned = EnsureIndexTreePinnedAsync;
    }

    /// <summary>
    /// The leaf key bound the <see cref="RepoContextTrees.VectorIndex"/> tree is
    /// registered with: derived from that tree's own byte bound and the size of
    /// the records a durable index writes, so the two bounds cross at about the
    /// same leaf size (issue #2829). See
    /// <see cref="DurableVectorIndexOptions.ResolveMaxLeafKeys(long)"/>.
    /// </summary>
    internal int IndexTreeMaxLeafKeys =>
        DurableVectorIndexOptions.ResolveMaxLeafKeys(_options.Get(RepoContextTrees.VectorIndex).MaxLeafBytes);

    /// <summary>
    /// Registers the <see cref="RepoContextTrees.VectorIndex"/> tree with
    /// <see cref="IndexTreeMaxLeafKeys"/>, once per factory, and returns the task
    /// that completes when it has.
    /// <para>
    /// Registration is idempotent at the registry: a tree that already exists
    /// keeps the structure it was created with, so an existing deployment keeps
    /// its pin and adopts the derived one only through an operator-run resize.
    /// A registration that faulted or was cancelled is retried on the next call
    /// rather than cached, so one transient failure does not leave every later
    /// operation failing with it.
    /// </para>
    /// </summary>
    internal Task EnsureIndexTreePinnedAsync()
    {
        var pin = Volatile.Read(ref _indexTreePinned);
        if (pin is not null && !pin.IsFaulted && !pin.IsCanceled)
        {
            return pin;
        }

        var fresh = _registerIndexTree(IndexTreeMaxLeafKeys);
        var raced = Interlocked.CompareExchange(ref _indexTreePinned, fresh, pin);
        return ReferenceEquals(raced, pin) ? fresh : raced!;
    }

    private Task RegisterIndexTreeAsync(int maxLeafKeys) =>
        _grainFactory.GetLatticeRegistry().RegisterAsync(
            RepoContextTrees.VectorIndex,
            new TreeRegistryEntry { MaxLeafKeys = maxLeafKeys });

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
        return new RepoContextPinnedVectorIndexStore(
            new LatticeVectorIndexStore(_grainFactory.GetGrain<ILattice>(RepoContextTrees.VectorIndex)),
            _ensureIndexTreePinned);
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

        // The walk's key scan is itself a first touch of the index tree, and a
        // first touch of an unregistered tree registers it with the core default
        // leaf bound, so the pin has to land before it.
        await EnsureIndexTreePinnedAsync().WaitAsync(cancellationToken).ConfigureAwait(false);

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
