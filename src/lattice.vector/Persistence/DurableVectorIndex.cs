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

    // What the store holds for each partition of the current generation, one
    // entry per stored chunk: the epoch it lives under, and a hash of its content
    // so a flush can tell which chunks it has to rewrite. A null hash entry means
    // the content is unknown and the chunk is rewritten on the next flush.
    private long[][] _persistedChunkEpochs = [];
    private UInt128[]?[] _persistedChunkHashes = [];
    private bool[] _resident = [];
    private long _generation;
    private long _centroidEpoch;
    private bool _centroidsPersisted;
    private bool _loaded;
    private bool _keysLoaded;
    private int _persistedPartitions;
    private VectorIndexBuildPhase _phase;
    private string? _cursor;
    private int _expected;
    private int _updatesSinceTraining;
    private bool _restored;
    private int _slicesDeadlined;
    private int _slicesDeadlinedWithoutProgress;
    private int _emptyDeadlinesSinceAdvance;

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
        await index.LoadOrResumeAsync(cancellationToken).ConfigureAwait(false);
        return index;
    }

    /// <summary>
    /// Creates the index object without reading anything, so a caller that drives
    /// the load itself can <b>keep the instance across a load that faults</b> and
    /// call <see cref="LoadOrResumeAsync"/> again to continue it.
    /// <para>
    /// <b>Why this is separate from <see cref="OpenAsync"/>.</b> The factory builds
    /// into a local and returns only on success, so a faulted load discards the
    /// partially-built identifier mapping along with the instance holding it. The
    /// caller then opens again from nothing and reissues the whole O(corpus) walk.
    /// On a tree whose leaves are slow to activate that regenerates the identical
    /// demand on every attempt, which is the amplification half of #2953. Splitting
    /// construction from loading is what lets the progress survive the fault.
    /// </para>
    /// <para>
    /// The index is <b>not usable</b> until a <see cref="LoadOrResumeAsync"/> call
    /// returns successfully; <see cref="IsLoaded"/> reports when that has happened.
    /// Prefer <see cref="OpenAsync"/> unless you are implementing the retry.
    /// </para>
    /// </summary>
    /// <param name="store">The durable store the index is persisted on.</param>
    /// <param name="source">The store of record the index is derived from.</param>
    /// <param name="options">The index and layout configuration.</param>
    /// <param name="loadMode">How much of a persisted index to bring into memory.</param>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    /// <exception cref="ArgumentException">The options are unusable, or the source's dimensionality contradicts them.</exception>
    public static DurableVectorIndex CreateUnloaded(
        IVectorIndexStore store,
        IVectorSource source,
        DurableVectorIndexOptions options,
        VectorIndexLoadMode loadMode = VectorIndexLoadMode.Full)
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

        return new DurableVectorIndex(store, source, options.Clone(), loadMode);
    }

    /// <summary>
    /// Runs the durable load, continuing a previous attempt that faulted partway
    /// rather than starting it again. Returns without doing anything once the load
    /// has completed, so a caller may call it on every retry tick unconditionally.
    /// </summary>
    /// <param name="cancellationToken">Cancels the load.</param>
    public Task LoadOrResumeAsync(CancellationToken cancellationToken = default)
        => LoadOrResumeAsync(cancellationToken, cancellationToken);

    /// <summary>
    /// Runs the durable load under two separate tokens, so that a caller slicing
    /// the load by wall clock bounds only the part of it that can resume.
    /// </summary>
    /// <param name="keyWalkToken">
    /// Cancels the O(corpus) key-map walk, which banks its position per entry and
    /// therefore resumes from where it stopped. A caller passes its slice deadline
    /// here.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancels the load as a whole. A caller passes its own token here, and only
    /// its own token.
    /// </param>
    /// <remarks>
    /// <para>
    /// <b>The split is a correctness requirement, not a refinement.</b> The restore
    /// that follows the walk - manifest, centroids, partition states, chunks -
    /// builds into a local and is assigned only on success, so it banks NOTHING
    /// when interrupted. Cancelling it on a slice deadline would make every
    /// attempt restart it, so an index whose restore takes longer than one slice
    /// could never open at all: not a slow open, but an open that provably cannot
    /// terminate. That is the trap #2953 names, and bounding only the resumable
    /// phase is what avoids it.
    /// </para>
    /// <para>
    /// The restore is therefore unbounded in time, which is acceptable for a
    /// different reason than the walk: its cost scales with the size of the stored
    /// index, not with the number of cold grain activations the walk pays for.
    /// </para>
    /// </remarks>
    public async Task LoadOrResumeAsync(
        CancellationToken keyWalkToken, CancellationToken cancellationToken)
    {
        if (_loaded)
        {
            return;
        }

        await LoadAsync(keyWalkToken, cancellationToken).ConfigureAwait(false);
        _loaded = true;
    }

    /// <summary>
    /// How many identifier mappings the key walk has loaded so far. Monotonic
    /// within a load attempt sequence, so a caller that slices the load can tell
    /// a slice that banked progress from one that banked none.
    /// </summary>
    /// <remarks>
    /// This is the open's counterpart to
    /// <see cref="VectorIndexBuildProgress.EmptyDeadlinesSinceLastAdvance"/>, and
    /// it exists for the same reason: only a figure a caller can compare across
    /// attempts can distinguish a load that is advancing slowly from one that is
    /// not advancing at all.
    /// </remarks>
    public int LoadedKeyCount => _keys.Count;

    /// <summary>
    /// Whether a load has completed successfully on this instance. False on one
    /// built by <see cref="CreateUnloaded"/> whose load has not yet finished,
    /// including one whose load faulted and is waiting to be resumed.
    /// </summary>
    public bool IsLoaded => _loaded;

    /// <summary>
    /// Whether an interrupted load banked progress that a further
    /// <see cref="LoadOrResumeAsync"/> will continue from rather than re-read.
    /// <para>
    /// A resumed load and a restarted one reach the same final state, so this is
    /// the only witness that the resume happened at all. Reported here so the
    /// caller that owns the retry can record it, since only the caller knows a
    /// retry occurred.
    /// </para>
    /// <para>
    /// <b>A COMPLETED KEY WALK COUNTS AS BANKED PROGRESS, AND READING ONLY THE
    /// CURSOR WOULD MISS IT.</b> The dictionary clears its cursor when its walk
    /// finishes, so an interruption that lands AFTER the walk and before the load
    /// completes leaves no cursor - yet the walk is exactly the O(corpus) work a
    /// resume exists to keep, and the next call really does skip it. Reporting
    /// that case as "not resumed" would tell an operator the resume had failed at
    /// the very moment it did the most good. Gated on <see cref="IsLoaded"/> so a
    /// finished load, which resumes nothing because there is nothing left to do,
    /// does not claim banked progress.
    /// </para>
    /// </summary>
    public bool HasBankedLoadProgress => !_loaded && (_keys.HasBankedLoadProgress || _keysLoaded);

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
        _restored,
        _slicesDeadlined,
        _slicesDeadlinedWithoutProgress)
    {
        EmptyDeadlinesSinceLastAdvance = _emptyDeadlinesSinceAdvance,
    };

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
    /// <para>
    /// In <see cref="VectorIndexLoadMode.Lazy"/> it also completes synchronously
    /// once every cell the query probes is resident, which is the steady state a
    /// warm index sits in. That case is answered without entering an
    /// asynchronous frame at all, so it allocates nothing in <i>any</i> build:
    /// Roslyn emits an async method's state machine as a struct under
    /// <c>&lt;Optimize&gt;</c> and as a class without it, so a frame that is
    /// merely never suspended is still heap-allocated per call in an unoptimized
    /// build. Answering before the frame is entered removes that cost rather
    /// than relying on the compiler to elide it - see issue #2450.
    /// </para>
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

        if (TrySearchResident(query.Span, results.Span, out var outcome))
        {
            return new ValueTask<VectorSearchOutcome>(outcome);
        }

        return SearchLazyAsync(query, results, cancellationToken);
    }

    /// <summary>
    /// The probe count this will select into stack space. A query wanting more
    /// declines the fast path rather than renting, which costs nothing in
    /// correctness: declining simply takes the asynchronous path, which is what
    /// every query did before this fast path existed.
    /// </summary>
    private const int ResidentProbeStackLimit = 64;

    /// <summary>
    /// Answers a lazy search synchronously when the query probes no cell that
    /// still needs fetching. This is deliberately a conservative test: a false
    /// negative costs only the asynchronous path, whereas a false positive would
    /// answer from an incomplete index.
    /// </summary>
    /// <param name="query">The query vector.</param>
    /// <param name="results">The caller-owned buffer ranked hits are written into.</param>
    /// <param name="outcome">The completed outcome when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the search was answered without any fetch.</returns>
    private bool TrySearchResident(
        ReadOnlySpan<float> query, Span<VectorSearchResult> results, out VectorSearchOutcome outcome)
    {
        var wanted = Math.Min(_index.Probes, _index.PartitionCount);
        if (wanted > ResidentProbeStackLimit)
        {
            outcome = default;
            return false;
        }

        Span<int> probes = stackalloc int[ResidentProbeStackLimit];
        var selected = _index.SelectPartitions(query, probes[..wanted]);
        for (var i = 0; i < selected; i++)
        {
            if (!_resident[probes[i]])
            {
                outcome = default;
                return false;
            }
        }

        // Matches what the asynchronous path does once every probed cell is
        // resident: its fetch loop finds nothing to do, so neither the
        // retirement replay nor the residency write runs, and the search is
        // exactly this.
        var found = _index.Search(query, results, out var mode);
        outcome = new VectorSearchOutcome(found, mode);
        return true;
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
        _updatesSinceTraining++;

        if (!_keys.TryGetKey(id, out var key))
        {
            return UpsertNewAsync(id, vector, cancellationToken);
        }

        var replaced = _index.Upsert(key, vector.Span);
        NoteOutOfBandUpsert(replaced);
        return new ValueTask<bool>(replaced);
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

        if (!_keys.TryGetKey(id, out var key))
        {
            return false;
        }

        _updatesSinceTraining++;
        await WriteRetirementAsync(key, cancellationToken).ConfigureAwait(false);
        var removed = _index.Remove(key);
        if (removed)
        {
            // A removal vacates a position and backfills it from the tail, so a
            // committed chunk that held either of them no longer matches the
            // cell. Unlike an append, there is no position at which this is
            // harmless, so the next checkpoint has to rewrite the cell whole.
            // A removal that found nothing shifted nothing and must not pay it.
            _ingestAppendOnly = false;
        }

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
        var replaced = _index.Upsert(key, vector.Span);
        NoteOutOfBandUpsert(replaced);
        return replaced;
    }

    /// <summary>
    /// Records whether an upsert the build did not itself apply has cost the
    /// ingest cell its append-only property.
    /// <para>
    /// Only a <b>replacement</b> can. It vacates a position and backfills it from
    /// the tail, so a committed chunk holding either no longer matches the cell
    /// and the next checkpoint has to rewrite the cell whole. This is the rule the
    /// build's own ingest loop already applies to the vectors it streams - a
    /// replacement is not an append - and it holds just the same for a write that
    /// arrived from outside the build.
    /// </para>
    /// <para>
    /// A <b>plain append</b> is indistinguishable from one the build would have
    /// made itself: it lands at the tail and leaves every committed chunk exactly
    /// as it was. Charging it a rewrite makes the next checkpoint rewrite the
    /// whole cell, which over a build whose writer hands a batch over once per slice is
    /// quadratic in corpus size rather than linear. That is the amplification
    /// behind issue #2691, where one tree reached 25 GB of write-ahead log while
    /// its largest sibling reached 185 MB.
    /// </para>
    /// </summary>
    private void NoteOutOfBandUpsert(bool replaced)
    {
        if (replaced)
        {
            _ingestAppendOnly = false;
        }
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
