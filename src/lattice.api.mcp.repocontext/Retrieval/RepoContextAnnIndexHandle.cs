using System.Buffers;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Vector;
using Orleans.Lattice.Vector.Persistence;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// One repository's persisted approximate index for one embedding space, and the
/// single-threaded turn that owns it.
/// <para>
/// <b>Why a turn.</b> The underlying index tolerates concurrent readers <i>or</i> a
/// single writer, never both, and it does no locking of its own. A build slice, a
/// maintenance update, and a query are therefore all serialized through one
/// asynchronous gate, which is the same discipline a grain turn would impose. The
/// gate is uncontended in the steady state: a query holds it only for the span of a
/// partition probe.
/// </para>
/// <para>
/// <b>Why it refuses to answer while it is building.</b> A partially ingested index
/// would return real, correctly scored, and quietly incomplete results - the exact
/// silent-degradation failure this work exists to remove. Until the build reaches
/// <see cref="VectorIndexBuildPhase.Ready"/> the handle reports
/// <see cref="RepoContextAnnServingState.Bootstrapping"/> and answers nothing, so
/// the caller serves the exact scan and recall stays complete throughout.
/// </para>
/// <para>
/// <b>Why it catches up on open.</b> Maintenance updates are flushed in batches, so
/// an unclean stop can leave the persisted index a few vectors behind the store of
/// record. A shortfall is detected with a key-only count walk when the index opens
/// and repaired by streaming the source for the identifiers the index does not
/// hold. The repair direction is always the source's: nothing here ever writes to
/// a store of record.
/// </para>
/// </summary>
internal sealed class RepoContextAnnIndexHandle : IDisposable
{
    private readonly IRepoContextVectorSource _source;
    private readonly IVectorIndexStore _store;
    private readonly RepoContextAnnOptions _options;
    private readonly DurableVectorIndexOptions _durableOptions;
    private readonly EmbeddingSpaceTag _space;
    private readonly string _repoId;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _turn = new(1, 1);

    private DurableVectorIndex? _index;
    private VectorIndexBuildProgress _progress;
    private int _pendingFlush;
    private bool _serving;
    private bool _disposed;

    /// <summary>Creates the handle. Nothing is opened until the first advance.</summary>
    /// <param name="repoId">The repository this index covers. Must not be <see langword="null"/>.</param>
    /// <param name="space">The embedding space this index covers.</param>
    /// <param name="source">The store-of-record view the index derives itself from. Must not be <see langword="null"/>.</param>
    /// <param name="store">The durable store the index persists itself on. Must not be <see langword="null"/>.</param>
    /// <param name="options">The plane's shaping and maintenance options. Must not be <see langword="null"/>.</param>
    /// <param name="keyPrefix">The key prefix this index owns exclusively. Must not be <see langword="null"/>.</param>
    /// <param name="logger">The logger the build-state report is written to. Must not be <see langword="null"/>.</param>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public RepoContextAnnIndexHandle(
        string repoId,
        EmbeddingSpaceTag space,
        IRepoContextVectorSource source,
        IVectorIndexStore store,
        RepoContextAnnOptions options,
        string keyPrefix,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(repoId);
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(keyPrefix);
        ArgumentNullException.ThrowIfNull(logger);

        _repoId = repoId;
        _space = space;
        _source = source;
        _store = store;
        _options = options;
        _durableOptions = options.ToDurableOptions(space, keyPrefix);
        _logger = logger;
    }

    /// <summary>
    /// Whether the index can answer a query right now. Read without taking the
    /// turn, so a query on a still-building index costs nothing and never waits
    /// behind a build slice.
    /// </summary>
    public bool IsServing => Volatile.Read(ref _serving);

    /// <summary>
    /// The build progress the last completed step reported, which is what the
    /// plane surfaces as its honest "warming up or steady state" answer.
    /// </summary>
    public VectorIndexBuildProgress Progress => _progress;

    /// <summary>
    /// Whether the loaded index came from durable records rather than from a
    /// rebuild. This is the cold-start attribution signal: "loaded in" against
    /// "recomputed".
    /// </summary>
    public bool RestoredFromDurableState => _progress.RestoredFromDurableState;

    /// <summary>
    /// Opens the index if it is not open yet, then advances the build by exactly
    /// one bounded step, and reports where it got to. Driving this to
    /// <see cref="VectorIndexBuildPhase.Ready"/> is what
    /// <see cref="EnsureBuiltAsync(CancellationToken)"/> does; a test drives it a
    /// step at a time so no assertion depends on a background task or a clock.
    /// </summary>
    /// <param name="cancellationToken">Cancels the step.</param>
    /// <returns>Progress after the step.</returns>
    /// <exception cref="ObjectDisposedException">The handle has been disposed.</exception>
    public async Task<VectorIndexBuildProgress> AdvanceAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _turn.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var index = await OpenAsync(cancellationToken).ConfigureAwait(false);
            if (index.Progress.Phase != VectorIndexBuildPhase.Ready)
            {
                _progress = await index.BuildStepAsync(cancellationToken).ConfigureAwait(false);
                if (_progress.Phase == VectorIndexBuildPhase.Ready)
                {
                    await CatchUpAsync(index, cancellationToken).ConfigureAwait(false);
                    MarkServing(index);
                }

                return _progress;
            }

            // Already built: the remaining work is the shortfall repair, which is
            // idempotent and completes in one step.
            await CatchUpAsync(index, cancellationToken).ConfigureAwait(false);
            MarkServing(index);
            return _progress;
        }
        finally
        {
            _turn.Release();
        }
    }

    /// <summary>
    /// Drives <see cref="AdvanceAsync(CancellationToken)"/> until the index is
    /// serving. Each step is bounded and the turn is released between steps, so a
    /// query issued while this runs is answered by the fall-back path immediately
    /// rather than queueing behind the build.
    /// </summary>
    /// <param name="cancellationToken">Cancels the build between steps.</param>
    /// <exception cref="ObjectDisposedException">The handle has been disposed.</exception>
    public async Task EnsureBuiltAsync(CancellationToken cancellationToken)
    {
        while (!IsServing)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await AdvanceAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Searches the index, resolving each hit's identifier back to the canonical
    /// source key from the store of record so the caller hydrates the record
    /// rather than trusting the index as a second copy.
    /// </summary>
    /// <param name="query">The query vector.</param>
    /// <param name="k">The maximum number of matches. Must be positive.</param>
    /// <param name="cancellationToken">Cancels the search.</param>
    /// <returns>The plane's answer, or
    /// <see cref="RepoContextAnnSearchOutcome.Bootstrapping"/> when the index is
    /// not serving yet.</returns>
    public async ValueTask<RepoContextAnnSearchOutcome> SearchAsync(
        ReadOnlyMemory<float> query, int k, CancellationToken cancellationToken)
    {
        if (_disposed || !IsServing)
        {
            return RepoContextAnnSearchOutcome.Bootstrapping;
        }

        VectorSearchMode mode;
        List<string> ids;
        float[] scores;

        await _turn.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var index = _index;
            if (index is null)
            {
                return RepoContextAnnSearchOutcome.Bootstrapping;
            }

            // The result buffer is rented rather than allocated: a query's width is
            // the caller's over-fetch pool, which is far larger than the k it
            // finally returns.
            var buffer = ArrayPool<VectorSearchResult>.Shared.Rent(k);
            try
            {
                var outcome = await index
                    .SearchAsync(query, buffer.AsMemory(0, k), cancellationToken)
                    .ConfigureAwait(false);
                mode = outcome.Mode;

                ids = new List<string>(outcome.Count);
                scores = outcome.Count == 0 ? [] : new float[outcome.Count];
                for (var i = 0; i < outcome.Count; i++)
                {
                    if (index.TryGetId(buffer[i].Key, out var id))
                    {
                        scores[ids.Count] = buffer[i].Score;
                        ids.Add(id);
                    }
                }
            }
            finally
            {
                ArrayPool<VectorSearchResult>.Shared.Return(buffer);
            }
        }
        finally
        {
            _turn.Release();
        }

        var state = mode == VectorSearchMode.Approximate
            ? RepoContextAnnServingState.Approximate
            : RepoContextAnnServingState.Exhaustive;

        if (ids.Count == 0)
        {
            return new RepoContextAnnSearchOutcome(state, Array.Empty<RepoContextVectorMatch>());
        }

        // Resolved outside the turn: it is a read of the store of record, not of
        // the index, so it must not hold the gate a build slice needs.
        var sourceKeys = await _source.ResolveSourceKeysAsync(ids, cancellationToken).ConfigureAwait(false);
        var matches = new List<RepoContextVectorMatch>(ids.Count);
        for (var i = 0; i < ids.Count; i++)
        {
            // An identifier the store of record no longer resolves is dropped rather
            // than returned: the index may lag in the missing direction, never hold
            // something the store will not stand behind.
            if (sourceKeys.TryGetValue(ids[i], out var sourceKey))
            {
                matches.Add(new RepoContextVectorMatch(ids[i], sourceKey, scores[i]));
            }
        }

        return new RepoContextAnnSearchOutcome(state, matches);
    }

    /// <summary>
    /// Applies a completed local write: the identifiers the source no longer holds
    /// are retired first, so a replaced vector can never be returned alongside its
    /// replacement, and the current ones are then upserted.
    /// </summary>
    /// <param name="upserts">The vectors the source now holds. Must not be <see langword="null"/>.</param>
    /// <param name="retired">The identifiers the source no longer holds. Must not be <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancels the maintenance.</param>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public async Task ApplyWriteAsync(
        IReadOnlyList<RepoContextAnnVectorUpdate> upserts,
        IReadOnlyList<string> retired,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(upserts);
        ArgumentNullException.ThrowIfNull(retired);

        if (_disposed || (upserts.Count == 0 && retired.Count == 0))
        {
            return;
        }

        await _turn.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var index = _index;
            if (index is null)
            {
                // No index is open for this pair, so there is nothing to keep in
                // step: a later build streams the store of record, which already
                // carries this write.
                return;
            }

            var applied = 0;
            for (var i = 0; i < retired.Count; i++)
            {
                if (await index.RemoveAsync(retired[i], cancellationToken).ConfigureAwait(false))
                {
                    applied++;
                }
            }

            for (var i = 0; i < upserts.Count; i++)
            {
                var update = upserts[i];
                if (update.Vector.Length != _space.Dimension)
                {
                    continue;
                }

                await index.UpsertAsync(update.VectorId, update.Vector, cancellationToken).ConfigureAwait(false);
                applied++;
            }

            if (applied == 0)
            {
                return;
            }

            _pendingFlush += applied;
            _progress = index.Progress;
            await MaintainAsync(index, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _turn.Release();
        }
    }

    /// <summary>
    /// Persists whatever maintenance the index is holding, so a caller that knows
    /// a batch has finished does not have to wait for the update threshold.
    /// </summary>
    /// <param name="cancellationToken">Cancels the flush.</param>
    public async Task FlushAsync(CancellationToken cancellationToken)
    {
        if (_disposed)
        {
            return;
        }

        await _turn.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var index = _index;
            if (index is null || _pendingFlush == 0)
            {
                return;
            }

            await index.FlushAsync(cancellationToken).ConfigureAwait(false);
            _pendingFlush = 0;
            _progress = index.Progress;
        }
        finally
        {
            _turn.Release();
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        // The turn is deliberately left undisposed. A query reads IsServing without
        // holding it, so disposing the gate would let a shutdown race throw
        // ObjectDisposedException out of a search - which the fail-closed guard above
        // would report as keyword.index_degraded, a spurious capability loss on a box
        // that is merely stopping. A SemaphoreSlim that was never asked for its wait
        // handle holds no unmanaged resource, so there is nothing to release and the
        // flag above is what actually stops the handle serving.
        Volatile.Write(ref _serving, false);
    }

    private async Task<DurableVectorIndex> OpenAsync(CancellationToken cancellationToken)
    {
        if (_index is not null)
        {
            return _index;
        }

        // Full rather than lazy: a lazily loaded index is read-only by contract, and
        // this one has to be maintained in place as vectors are written.
        _index = await DurableVectorIndex
            .OpenAsync(_store, _source, _durableOptions, VectorIndexLoadMode.Full, cancellationToken)
            .ConfigureAwait(false);
        _progress = _index.Progress;

        _logger.LogInformation(
            "Repository-context approximate index for {RepoId} in space {ModelId}/{Dimension} opened in phase "
            + "{Phase} holding {VectorsIndexed} vectors (restored from durable state: {Restored}).",
            _repoId,
            _space.ModelId,
            _space.Dimension,
            _progress.Phase,
            _progress.VectorsIndexed,
            _progress.RestoredFromDurableState);

        return _index;
    }

    private async Task CatchUpAsync(DurableVectorIndex index, CancellationToken cancellationToken)
    {
        // A key-only walk, so this costs a fraction of the streaming enumeration and
        // is the cheapest honest way to learn whether the persisted index is behind.
        // In a repository holding more than one embedding space the count is an upper
        // bound, which makes the repair run when it need not - never the reverse.
        //
        // AND IF THE COUNT CANNOT BE OBTAINED AT ALL, THE SAME REASONING APPLIES.
        // The count is a hint that decides whether to SKIP the repair, so failing to
        // get it must mean "repair", not "give up". Letting the abort propagate is
        // what made a whole index build fail on a real deployment (#1844): the walk
        // covers the repository's entire vector prefix, activating every leaf of a
        // cold metadata tree, and on a large enough tree it can outrun even a
        // generous reconnect budget. Treating exhaustion as "unknown, therefore
        // possibly behind" keeps the build going down the path that repairs, which
        // is the safe direction and the one the upper-bound case already takes.
        var behind = true;
        try
        {
            var expected = await _source.CountAsync(cancellationToken).ConfigureAwait(false);
            behind = expected > index.Count;
        }
        catch (EnumerationAbortedException ex)
        {
            _logger.LogInformation(
                ex,
                "Repository-context approximate index for {RepoId} in space {ModelId}/{Dimension} could not count the "
                + "source within its reconnect budget; treating the persisted index as possibly behind and repairing.",
                _repoId,
                _space.ModelId,
                _space.Dimension);
        }

        if (!behind)
        {
            await MaintainAsync(index, cancellationToken).ConfigureAwait(false);
            return;
        }

        var recovered = 0;
        await foreach (var entry in _source
            .EnumerateAsync(null, cancellationToken)
            .ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (index.TryGetKey(entry.Id, out _))
            {
                continue;
            }

            await index.UpsertAsync(entry.Id, entry.Vector, cancellationToken).ConfigureAwait(false);
            recovered++;
        }

        if (recovered > 0)
        {
            _logger.LogInformation(
                "Repository-context approximate index for {RepoId} in space {ModelId}/{Dimension} recovered "
                + "{Recovered} vectors the persisted index was behind on.",
                _repoId,
                _space.ModelId,
                _space.Dimension,
                recovered);

            // Flushed unconditionally rather than left to the update threshold: a
            // repair that is not made durable is repeated on every start, so the
            // shortfall would be paid for forever instead of once.
            await index.FlushAsync(cancellationToken).ConfigureAwait(false);
            _pendingFlush = 0;
            _progress = index.Progress;
        }

        await MaintainAsync(index, cancellationToken).ConfigureAwait(false);
    }

    private async Task MaintainAsync(DurableVectorIndex index, CancellationToken cancellationToken)
    {
        // Retraining first: it rewrites every partition and commits a fresh
        // generation, which subsumes the flush the pending updates would have done.
        // It is synchronous and expensive, and it runs here - on the maintenance turn
        // a write took - deliberately: that turn belongs to the background indexing
        // pass, never to a query, so the cost lands on the writer rather than on a
        // caller. A query issued while it runs waits for the turn, which is why the
        // trigger is a quarter of the corpus rather than a handful of updates.
        if (ShouldRetrain(index))
        {
            _logger.LogInformation(
                "Repository-context approximate index for {RepoId} in space {ModelId}/{Dimension} retraining after "
                + "{Updates} updates against {Count} vectors: the partitioning no longer describes the corpus.",
                _repoId,
                _space.ModelId,
                _space.Dimension,
                index.UpdatesSinceTraining,
                index.Count);

            await index.RetrainAsync(cancellationToken).ConfigureAwait(false);
            _pendingFlush = 0;
            _progress = index.Progress;
            return;
        }

        if (_pendingFlush >= _options.FlushAfterUpdates)
        {
            await index.FlushAsync(cancellationToken).ConfigureAwait(false);
            _pendingFlush = 0;
            _progress = index.Progress;
        }
    }

    private bool ShouldRetrain(DurableVectorIndex index)
    {
        // Only a trained index can drift: an untrained one has no partitioning for
        // the corpus to move away from, and retraining it would be a no-op that
        // rewrote every record for nothing.
        if (index.Progress.Phase != VectorIndexBuildPhase.Ready
            || index.Status.State != VectorIndexState.Ready
            || index.Count <= 0
            || _options.RetrainAfterUpdateFraction <= 0d)
        {
            return false;
        }

        return index.UpdatesSinceTraining >= index.Count * _options.RetrainAfterUpdateFraction;
    }

    private void MarkServing(DurableVectorIndex index)
    {
        _progress = index.Progress;
        if (IsServing)
        {
            return;
        }

        Volatile.Write(ref _serving, true);
        _logger.LogInformation(
            "Repository-context approximate index for {RepoId} in space {ModelId}/{Dimension} is serving "
            + "{VectorsIndexed} vectors across {Partitions} partitions; semantic retrieval is now approximate.",
            _repoId,
            _space.ModelId,
            _space.Dimension,
            _progress.VectorsIndexed,
            _progress.PartitionsTotal);
    }
}
