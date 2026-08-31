namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// A <see cref="VectorIndex"/> that survives a restart: persisted in bounded
/// chunks on a durable store, maintained in place as vectors are written and
/// retired, built in the background from the store of record when it does not
/// yet exist, and rebuilt rather than trusted when what was persisted cannot be
/// verified.
/// <para>
/// <b>The coherence contract.</b> The index is a derived projection of an
/// <see cref="IVectorSource"/>, which is authoritative. Five rules define what it
/// may and may not do when the two disagree.
/// </para>
/// <list type="number">
/// <item><description>
/// <b>No ghosts.</b> A vector retired from the store of record never appears in a
/// result, before or after a restart. A retirement writes a durable tombstone
/// <i>before</i> the in-memory removal and drops it only once that removal is
/// durable, so a crash mid-deletion completes the deletion on the next load
/// instead of resurrecting the vector.
/// </description></item>
/// <item><description>
/// <b>Lag only in the missing direction.</b> The index may be behind the source
/// on vectors it has not ingested yet, which costs recall and nothing else. It is
/// never allowed to be ahead of it. Outstanding work is reported by
/// <see cref="Progress"/> rather than hidden.
/// </description></item>
/// <item><description>
/// <b>Derived, never authoritative.</b> Every inconsistency is resolved by
/// discarding index state and recomputing from the source. Nothing here ever
/// writes to a store of record - that asymmetry is what makes throwing the index
/// away always safe.
/// </description></item>
/// <item><description>
/// <b>Verified load.</b> Persisted state is admitted only if its manifest, every
/// record checksum, every partition's chunk set, and its declared vector count
/// all agree. A truncated, corrupt, incomplete, or version-incompatible index
/// fails that check and is rebuilt; it is never partially served.
/// </description></item>
/// <item><description>
/// <b>Honest mode.</b> Every search reports its <see cref="VectorSearchMode"/>.
/// Before the partitioning exists the index answers by exhaustive scan, which is
/// <i>exact</i> - slower, not worse - and must be surfaced as warming up, never
/// as an error or as a fallback to a different kind of retrieval.
/// </description></item>
/// </list>
/// <para>
/// <b>Threading.</b> An instance is safe for concurrent readers <i>or</i> a single
/// writer, not both, exactly as the underlying index is. It does no locking and
/// starts no background work of its own: the build is a caller-driven pump
/// (<see cref="BuildStepAsync"/>), so the host decides when it runs and it can
/// never race a mutation. A single-threaded grain turn is the natural home.
/// </para>
/// </summary>
public sealed partial class DurableVectorIndex
{
    private readonly IVectorIndexStore _store;
    private readonly IVectorSource _source;
    private readonly DurableVectorIndexOptions _options;
    private readonly VectorKeyDictionary _keys;
    private readonly string _prefix;
    private readonly VectorIndexLoadMode _loadMode;
    private readonly HashSet<long> _pendingRetirements = [];

    private VectorIndex _index;
    private long[] _persistedPartitionVersion = [];
    private long[] _persistedEpoch = [];
    private int[] _persistedChunkCount = [];
    private bool[] _resident = [];
    private long _generation;
    private long _centroidEpoch;
    private bool _centroidsPersisted;
    private int _persistedPartitions;
    private VectorIndexBuildPhase _phase;
    private string? _cursor;
    private int _expected;
    private int _updatesSinceTraining;
    private bool _restored;

    private DurableVectorIndex(
        IVectorIndexStore store,
        IVectorSource source,
        DurableVectorIndexOptions options,
        VectorIndexLoadMode loadMode)
    {
        _store = store;
        _source = source;
        _options = options;
        _loadMode = loadMode;
        _prefix = options.KeyPrefix;
        _keys = new VectorKeyDictionary(store, _prefix, options.KeyReservationBlock);
        _index = new VectorIndex(options.Index);
    }

    /// <summary>
    /// Opens an index over a store, adopting whatever durable state can be
    /// verified and discarding whatever cannot.
    /// <para>
    /// Opening never blocks on a build. A store with nothing usable on it yields
    /// an empty index in <see cref="VectorIndexBuildPhase.NotStarted"/>, which
    /// answers correctly (over nothing) from the first moment and is filled by
    /// driving <see cref="BuildStepAsync"/>. That is the upgrade path for an
    /// existing deployment: it serves through the exhaustive path while the index
    /// materialises behind it.
    /// </para>
    /// </summary>
    /// <param name="store">The durable store the index is persisted on.</param>
    /// <param name="source">The store of record the index is derived from.</param>
    /// <param name="options">The index and layout configuration.</param>
    /// <param name="loadMode">How much of a persisted index to bring into memory.</param>
    /// <param name="cancellationToken">Cancels the open.</param>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    /// <exception cref="ArgumentException">The options are unusable, or the source's dimensionality contradicts them.</exception>
    public static async Task<DurableVectorIndex> OpenAsync(
        IVectorIndexStore store,
        IVectorSource source,
        DurableVectorIndexOptions options,
        VectorIndexLoadMode loadMode = VectorIndexLoadMode.Full,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        if (source.Dimensions != options.Index.Dimensions)
        {
            throw new ArgumentException(
                $"The source supplies {source.Dimensions}-dimensional vectors but the index is configured for {options.Index.Dimensions}.",
                nameof(source));
        }

        var index = new DurableVectorIndex(store, source, options.Clone(), loadMode);
        await index.LoadAsync(cancellationToken).ConfigureAwait(false);
        return index;
    }

    /// <summary>The key prefix every durable record of this index sits under.</summary>
    public string KeyPrefix => _prefix;

    /// <summary>The generation currently loaded. Bumped only by a retrain or a rebuild.</summary>
    public long Generation => _generation;

    /// <summary>How much of the index is resident, and therefore whether it can be mutated.</summary>
    public VectorIndexLoadMode LoadMode => _loadMode;

    /// <summary>
    /// The underlying index's shape and readiness. Note that
    /// <see cref="VectorIndexStatus.State"/> answers a different question from
    /// <see cref="Progress"/>: it says whether a usable <i>partitioning</i>
    /// exists, while the progress phase says whether the <i>build</i> has
    /// finished. A corpus below the training threshold legitimately finishes its
    /// build without a partitioning, and answers exactly by exhaustive scan.
    /// </summary>
    public VectorIndexStatus Status => _index.Status;

    /// <summary>The number of vectors the index currently holds.</summary>
    public int Count => _index.Count;

    /// <summary>
    /// How many vectors have been added, replaced, or retired since the
    /// partitioning was last computed.
    /// <para>
    /// This is the drift signal, and it is worth understanding what it does and
    /// does not mean. Incremental maintenance keeps the index <i>correct</i>
    /// forever: every vector sits in the cell nearest to it among the trained
    /// centroids, and nothing stale is ever returned. What it cannot do is keep
    /// the cells <i>descriptive</i> once the corpus has moved away from the
    /// distribution they were trained on, and a partitioning that no longer
    /// describes its data loses approximate recall quietly, with every individual
    /// record still perfectly valid. A host that lets this counter grow large
    /// relative to <see cref="Count"/> - a quarter of the corpus is a reasonable
    /// threshold - should call <see cref="RetrainAsync"/> off the request path.
    /// </para>
    /// </summary>
    public int UpdatesSinceTraining => _updatesSinceTraining;

    /// <summary>
    /// What the index can honestly say about itself right now. This is the signal
    /// a readiness probe and a retrieval-path attribution are built from.
    /// </summary>
    public VectorIndexBuildProgress Progress => new(
        _phase,
        _generation,
        _index.Count,
        _expected,
        _persistedPartitions,
        _index.PartitionCount,
        _restored);

    /// <summary>
    /// Searches the resident index, writing hits into the caller's span in
    /// descending score order and reporting which path answered.
    /// <para>
    /// This path allocates nothing. In
    /// <see cref="VectorIndexLoadMode.Lazy"/> it answers from whatever cells are
    /// already resident, so use <see cref="SearchAsync"/> there unless a
    /// best-effort answer is what you want.
    /// </para>
    /// </summary>
    /// <param name="query">The query vector, of exactly the index's dimensionality.</param>
    /// <param name="results">The caller-owned span the ranked hits are written into.</param>
    /// <param name="mode">Which retrieval path answered.</param>
    /// <returns>The number of results written.</returns>
    /// <exception cref="ArgumentException"><paramref name="query"/> has the wrong length.</exception>
    public int Search(ReadOnlySpan<float> query, Span<VectorSearchResult> results, out VectorSearchMode mode) =>
        _index.Search(query, results, out mode);

    /// <summary>
    /// Searches, fetching any cell the query would probe that is not yet
    /// resident. In <see cref="VectorIndexLoadMode.Full"/> nothing is ever
    /// missing, so this completes synchronously and matches
    /// <see cref="Search"/> exactly.
    /// </summary>
    /// <param name="query">The query vector, of exactly the index's dimensionality.</param>
    /// <param name="results">The caller-owned buffer the ranked hits are written into.</param>
    /// <param name="cancellationToken">Cancels any fetch the search needs.</param>
    /// <returns>How many results were written and which path answered.</returns>
    /// <exception cref="ArgumentException"><paramref name="query"/> has the wrong length.</exception>
    public ValueTask<VectorSearchOutcome> SearchAsync(
        ReadOnlyMemory<float> query,
        Memory<VectorSearchResult> results,
        CancellationToken cancellationToken = default)
    {
        if (_loadMode == VectorIndexLoadMode.Full || _index.PartitionCount == 0 || !_index.CentroidsComplete)
        {
            var found = _index.Search(query.Span, results.Span, out var mode);
            return new ValueTask<VectorSearchOutcome>(new VectorSearchOutcome(found, mode));
        }

        return SearchLazyAsync(query, results, cancellationToken);
    }

    /// <summary>
    /// Resolves an index key back to the identifier the store of record uses.
    /// This never touches the store.
    /// </summary>
    /// <param name="key">A key from a <see cref="VectorSearchResult"/>.</param>
    /// <param name="id">The source identifier when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the key is mapped.</returns>
    public bool TryGetId(long key, out string id) => _keys.TryGetId(key, out id);

    /// <summary>Looks up the index key an identifier is mapped to.</summary>
    /// <param name="id">The source identifier.</param>
    /// <param name="key">The index key when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the identifier is mapped.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="id"/> is null.</exception>
    public bool TryGetKey(string id, out long key) => _keys.TryGetKey(id, out key);

    /// <summary>
    /// Adds or replaces one vector. A re-embedded identifier keeps the key it
    /// already had, so this is an in-place update of one cell rather than a
    /// delete and an insert, and it never forces a rebuild.
    /// <para>
    /// The already-mapped path is fully synchronous and allocates nothing, which
    /// is the common case for a maintenance loop following a source that is being
    /// re-embedded.
    /// </para>
    /// </summary>
    /// <param name="id">The source identifier.</param>
    /// <param name="vector">The vector, of exactly the index's dimensionality.</param>
    /// <param name="cancellationToken">Cancels the identifier assignment, if one is needed.</param>
    /// <returns><see langword="true"/> when an existing vector was replaced.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="id"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="id"/> is empty, or the vector has the wrong length.</exception>
    /// <exception cref="InvalidOperationException">The index was opened lazily and is read-only.</exception>
    public ValueTask<bool> UpsertAsync(
        string id, ReadOnlyMemory<float> vector, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);
        RequireMutable();

        // Any mutation the build did not itself apply breaks the assumption that
        // the ingest cell is an append-only extension of what is committed, so
        // the next checkpoint rewrites it wholesale instead of appending to a
        // prefix that has shifted underneath it.
        _ingestAppendOnly = false;
        _updatesSinceTraining++;

        return _keys.TryGetKey(id, out var key)
            ? new ValueTask<bool>(_index.Upsert(key, vector.Span))
            : UpsertNewAsync(id, vector, cancellationToken);
    }

    /// <summary>
    /// Retires one vector, so it can never appear in a later result.
    /// <para>
    /// A durable tombstone is written before the in-memory removal and dropped
    /// only once the removal is durable, so a crash between the two completes the
    /// deletion on the next load rather than resurrecting the vector. This is the
    /// mechanism behind the first rule of the coherence contract.
    /// </para>
    /// </summary>
    /// <param name="id">The source identifier.</param>
    /// <param name="cancellationToken">Cancels the retirement.</param>
    /// <returns><see langword="true"/> when a vector was retired.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="id"/> is null.</exception>
    /// <exception cref="InvalidOperationException">The index was opened lazily and is read-only.</exception>
    public async Task<bool> RemoveAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);
        RequireMutable();
        _ingestAppendOnly = false;

        if (!_keys.TryGetKey(id, out var key))
        {
            return false;
        }

        _updatesSinceTraining++;
        await WriteRetirementAsync(key, cancellationToken).ConfigureAwait(false);
        var removed = _index.Remove(key);
        await _keys.RemoveAsync(id, cancellationToken).ConfigureAwait(false);
        return removed;
    }

    /// <summary>
    /// Removes every indexed vector the store of record no longer holds, and
    /// reports how many there were.
    /// <para>
    /// This is the repair direction of the coherence contract, and the only one
    /// there is: disagreement is always settled in the source's favour. It exists
    /// for the case the tombstone journal cannot cover - a vector removed from
    /// the source by something that never told the index - and is a bounded walk
    /// over the mapped identifiers rather than a rebuild.
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">Cancels the sweep.</param>
    /// <returns>The number of vectors removed.</returns>
    /// <exception cref="InvalidOperationException">The index was opened lazily and is read-only.</exception>
    public async Task<int> ReconcileAsync(CancellationToken cancellationToken = default)
    {
        RequireMutable();

        var stale = new List<string>();
        foreach (var id in _keys.Ids)
        {
            if (!await _source.ContainsAsync(id, cancellationToken).ConfigureAwait(false))
            {
                stale.Add(id);
            }
        }

        var removed = 0;
        foreach (var id in stale)
        {
            if (await RemoveAsync(id, cancellationToken).ConfigureAwait(false))
            {
                removed++;
            }
        }

        return removed;
    }

    private async ValueTask<bool> UpsertNewAsync(
        string id, ReadOnlyMemory<float> vector, CancellationToken cancellationToken)
    {
        var key = await _keys.GetOrAddAsync(id, cancellationToken).ConfigureAwait(false);
        return _index.Upsert(key, vector.Span);
    }

    private Task WriteRetirementAsync(long key, CancellationToken cancellationToken)
    {
        _pendingRetirements.Add(key);
        return _store.WriteAsync(
            [new KeyValuePair<string, byte[]>(
                VectorIndexStorageKeys.Retirement(_prefix, key), VectorIndexRecord.Wrap([]))],
            cancellationToken);
    }

    private void RequireMutable()
    {
        if (_loadMode == VectorIndexLoadMode.Lazy)
        {
            throw new InvalidOperationException(
                "A lazily loaded index is read-only: it does not hold the cells a mutation would have to update, "
                + "so applying one would silently lose it. Open with VectorIndexLoadMode.Full to maintain the index.");
        }
    }
}
