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
/// the caller serves the exact scan. Recall then stays complete for as long as
/// that scan can complete - which is not unconditional: where a gather has already
/// stalled, the caller's breaker withholds it and keyword recall serves instead,
/// reported as <see cref="RepoContextRetrievalPath.KeywordExactFallbackSuppressed"/>
/// (issue #2720).
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
    private readonly RepoContextAnnPartitioningReporter? _partitioning;
    private readonly RepoContextAnnIndexLoadReporter? _load;

    // Held across a load that FAULTED, which is the entire mechanism: the
    // partially-built identifier mapping lives on this instance, so discarding it
    // is what made every retry reissue the whole O(corpus) walk (#2953).
    private DurableVectorIndex? _loading;
    private readonly SemaphoreSlim _turn = new(1, 1);

    private DurableVectorIndex? _index;
    private VectorIndexBuildProgress _progress;
    private int _pendingFlush;

    /// <summary>
    /// Identifiers the writer handed over while the build was still streaming, and
    /// which were therefore recorded instead of applied. Holds identifiers only,
    /// never vectors, so it stays a few bytes per write of a build that is already
    /// reading the whole corpus. Drained by the catch-up once the build is Ready.
    /// </summary>
    private HashSet<string>? _deferredWrites;
    private bool _serving;
    private bool _disposed;

    /// <summary>
    /// The largest identifier-mapping count any deferred open has observed, and
    /// how many deferrals have since banked nothing. Together they let a bounded
    /// open tell "advancing slowly" from "not advancing at all". See the escalation
    /// in <c>OpenAsync</c>.
    /// </summary>
    private int _lastOpenKeyCount;
    private int _emptyOpenDeferrals;

    /// <summary>
    /// How many consecutive budget expiries that banked no mapping are tolerated
    /// before the open is declared unable to progress. Small on purpose: every one
    /// of them is a wasted coordinator turn, and the only configuration that
    /// produces them cannot be fixed by waiting.
    /// </summary>
    private const int MaxEmptyOpenDeferrals = 3;

    /// <summary>
    /// The smallest corpus at which another threshold-crossing training may be
    /// attempted, or <c>0</c> when none has been declined yet.
    /// <para>
    /// This exists only to bound the pathological case, and it is deliberately not
    /// the trigger. A corpus at or above
    /// <see cref="RepoContextAnnOptions.MinimumTrainingCount"/> can still resolve
    /// to fewer than two partitions - an explicit
    /// <see cref="RepoContextAnnOptions.PartitionCount"/> of one, or a minimum set
    /// low enough that the automatic count rounds to one - and such a training
    /// declines again, leaving the trigger condition exactly as it found it. Without
    /// this, every maintenance turn would retrain and the stuck state would have
    /// been traded for a hot one. Doubling caps the attempts at one per doubling of
    /// the corpus, so an activation makes at most a logarithmic number of them, and
    /// it costs the healthy case nothing because that case succeeds on the first.
    /// </para>
    /// <para>
    /// It is activation-local and is not persisted, and that is correct rather than
    /// a shortcut: the state it guards against is re-derived from the index on every
    /// open, so a restart that forgets it costs exactly one training attempt. A
    /// persisted counter would instead have to be right forever, and this defect
    /// exists because a trigger was keyed on a counter that could not be.
    /// </para>
    /// </summary>
    private int _nextPartitionAttemptCount;

    /// <summary>Creates the handle. Nothing is opened until the first advance.</summary>
    /// <param name="repoId">The repository this index covers. Must not be <see langword="null"/>.</param>
    /// <param name="space">The embedding space this index covers.</param>
    /// <param name="source">The store-of-record view the index derives itself from. Must not be <see langword="null"/>.</param>
    /// <param name="store">The durable store the index persists itself on. Must not be <see langword="null"/>.</param>
    /// <param name="options">The plane's shaping and maintenance options. Must not be <see langword="null"/>.</param>
    /// <param name="keyPrefix">The key prefix this index owns exclusively. Must not be <see langword="null"/>.</param>
    /// <param name="logger">The logger the build-state report is written to. Must not be <see langword="null"/>.</param>
    /// <param name="partitioning">
    /// The reporter the plane's partitioning state is metered on, or
    /// <see langword="null"/> to publish nothing. Null is for a test driving the
    /// handle directly; the registry always supplies one, so no deployment runs
    /// without the instrument.
    /// </param>
    /// <param name="load">
    /// The reporter durable-load attempts are metered on, or <see langword="null"/>
    /// to publish nothing. Null is for a test driving the handle directly; the
    /// registry always supplies one, so no deployment runs without the instrument.
    /// </param>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public RepoContextAnnIndexHandle(
        string repoId,
        EmbeddingSpaceTag space,
        IRepoContextVectorSource source,
        IVectorIndexStore store,
        RepoContextAnnOptions options,
        string keyPrefix,
        ILogger logger,
        RepoContextAnnPartitioningReporter? partitioning = null,
        RepoContextAnnIndexLoadReporter? load = null)
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
        _partitioning = partitioning;
        _load = load;
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
    public Task<VectorIndexBuildProgress> AdvanceAsync(CancellationToken cancellationToken)
        => AdvanceAsync(phase: null, cancellationToken);

    /// <summary>
    /// Advances the build by one step, reporting the phase each part of the step
    /// ran in through <paramref name="phase"/>.
    /// </summary>
    /// <param name="phase">
    /// The caller's phase probe, or <see langword="null"/> when the caller does not
    /// meter the step. Written only on this call, so it never carries a phase some
    /// other caller's concurrent step was in.
    /// </param>
    /// <param name="cancellationToken">Cancels the step.</param>
    /// <returns>Progress after the step.</returns>
    /// <exception cref="ObjectDisposedException">The handle has been disposed.</exception>
    /// <remarks>
    /// <para>
    /// <b>The probe is written twice, and the second write is the load-bearing
    /// one.</b> Before the step it records the phase the index is ENTERING, which
    /// is the honest label for a step that completes: it says what the step did. If
    /// the step throws it is rewritten from the index's own phase AT THE FAULT
    /// SITE, which is strictly better than the entry reading because a step entered
    /// at <see cref="VectorIndexBuildPhase.Training"/> trains and then persists in
    /// the same call - the index moves itself to
    /// <see cref="VectorIndexBuildPhase.Persisting"/> between the two - so a
    /// persist fault on a training-entry step is attributed to the persist rather
    /// than to the training. That distinction is the whole point of the dimension
    /// (issue #2855): the persist writes into the same tree a corpus-read defect
    /// would already have named, so mislabelling it as training or ingest is what
    /// would let one defect be scored as two.
    /// </para>
    /// <para>
    /// The catch-up branch marks <see cref="RepoContextAnnBuildStepPhase.Reconciling"/>
    /// only from inside its own fault handler, so a step that built AND caught up
    /// successfully is still reported under the phase it built in rather than
    /// under the maintenance that followed it.
    /// </para>
    /// </remarks>
    public async Task<VectorIndexBuildProgress> AdvanceAsync(
        RepoContextAnnBuildPhaseProbe? phase, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        await _turn.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            phase?.Enter(RepoContextAnnBuildStepPhase.Opening);
            var index = await OpenAsync(cancellationToken).ConfigureAwait(false);
            if (index is null)
            {
                // The open spent its budget and banked its progress. The step ends
                // here, still in Opening, which is the honest phase: the index is
                // opening and has not finished. Returning the unchanged progress
                // rather than throwing is what lets the coordinator release its
                // turn, answer its keep-alive, and resume on the next tick - the
                // entire point of bounding the open. See
                // RepoContextAnnOptions.OpenSliceBudget.
                return _progress;
            }

            if (index.Progress.Phase != VectorIndexBuildPhase.Ready)
            {
                var restoredAtOpen = index.Progress.RestoredFromDurableState;
                phase?.Enter(MapStepPhase(index.Progress.Phase));
                try
                {
                    _progress = await index.BuildStepAsync(cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    // The index has moved its own phase as far as it got, so this
                    // reading places the fault inside the step rather than at the
                    // step's entry. See the remarks above.
                    phase?.Enter(MapStepPhase(index.Progress.Phase));
                    throw;
                }

                if (_progress.Phase == VectorIndexBuildPhase.Ready)
                {
                    // This process streamed the corpus itself, so it knows what the
                    // index covers - unless the index it resumed was restored
                    // part-built, in which case an earlier process streamed some of
                    // it and only the probe can confirm the join.
                    try
                    {
                        await CatchUpAsync(index, restoredAtOpen, cancellationToken).ConfigureAwait(false);
                        MarkServing(index);
                        RecordPartitioningState();
                    }
                    catch
                    {
                        phase?.Enter(RepoContextAnnBuildStepPhase.Reconciling);
                        throw;
                    }
                }

                return _progress;
            }

            // Already built: the remaining work is the shortfall repair, which is
            // idempotent and completes in one step. Reaching Ready at OPEN means the
            // index came off durable state, so the probe is the only way to learn
            // whether the store of record has moved on since.
            phase?.Enter(RepoContextAnnBuildStepPhase.Reconciling);
            await CatchUpAsync(index, probeSource: true, cancellationToken).ConfigureAwait(false);
            MarkServing(index);
            RecordPartitioningState();
            return _progress;
        }
        finally
        {
            _turn.Release();
        }
    }

    /// <summary>
    /// Projects the index's own build phase onto the phase this plane reports.
    /// </summary>
    /// <param name="phase">The phase the durable index is in.</param>
    /// <returns>The reported phase.</returns>
    /// <remarks>
    /// <see cref="VectorIndexBuildPhase.NotStarted"/> maps onto
    /// <see cref="RepoContextAnnBuildStepPhase.Ingesting"/> because the step taken
    /// from it counts the source, which is a read of the same corpus by the same
    /// path - so a fault there is an ingest-read fault however the index labels the
    /// phase it was in. <see cref="VectorIndexBuildPhase.Ready"/> maps onto
    /// <see cref="RepoContextAnnBuildStepPhase.Persisting"/> rather than onto
    /// <see cref="RepoContextAnnBuildStepPhase.Reconciling"/>, because this mapping
    /// is only ever applied INSIDE a build step: an index reporting Ready there has
    /// reached it during its own persist and has not finished that persist yet.
    /// Reconciling is written explicitly by the two catch-up paths, which is the
    /// only place it is true.
    /// </remarks>
    private static RepoContextAnnBuildStepPhase MapStepPhase(VectorIndexBuildPhase phase) => phase switch
    {
        VectorIndexBuildPhase.Training => RepoContextAnnBuildStepPhase.Training,
        VectorIndexBuildPhase.Persisting => RepoContextAnnBuildStepPhase.Persisting,
        VectorIndexBuildPhase.Ready => RepoContextAnnBuildStepPhase.Persisting,
        _ => RepoContextAnnBuildStepPhase.Ingesting,
    };

    /// <summary>
    /// Drives <see cref="AdvanceAsync(CancellationToken)"/> until the index is
    /// serving. The turn is released between steps.
    /// <para>
    /// A concurrent <see cref="SearchAsync"/> does not wait on the build: it reads
    /// <see cref="IsServing"/> without taking the turn and falls back to the
    /// exact scan while a build runs. What a long step does block is every other
    /// caller of this handle - the coordinator's own pump, and the arming path.
    /// See <see cref="RepoContextAnnOptions.IngestSliceBudget"/> and issue #2483.
    /// </para>
    /// <para>
    /// <b>The open is bounded in time as well, and separately.</b> The vector-count
    /// and wall-clock budgets named above govern the INGEST portion of a step only:
    /// they are consulted once the index is open. <c>OpenAsync</c> - which restores
    /// or rebuilds the durable index BEFORE any of them is read - carries its own
    /// ceiling, <see cref="RepoContextAnnOptions.OpenSliceBudget"/>. An open that
    /// reaches it banks what it walked, returns the turn, and continues on the next
    /// step, so a cold open over a large plane is sliced rather than run to
    /// completion inside one non-reentrant coordinator turn.
    /// </para>
    /// <para>
    /// <b>The history matters, because this paragraph has been wrong before.</b> It
    /// once asserted a time bound that did not exist, and issue #3130 spent its
    /// investigation looking PAST the open because the documentation said the open
    /// could not be where the time was going - a cold open was in fact holding the
    /// turn for over thirty minutes. Bounding it had to wait on the load being
    /// resumable (#2953), because a bound that discards its progress converts a
    /// slow open into one that never finishes. Both halves are now in place; if
    /// either is removed, this paragraph is false again.
    /// </para>
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

                if (index.Progress.Phase != VectorIndexBuildPhase.Ready)
                {
                    // THE BUILD IS STREAMING THE STORE OF RECORD RIGHT NOW, AND
                    // THIS WRITE IS ALREADY IN IT. Applying it here anyway costs
                    // far more than it looks: while the index is ingesting it is
                    // one untrained cell, and any write the build did not make
                    // itself ends the cell's append-only property, so the next
                    // ingest checkpoint rewrites EVERY chunk of it instead of
                    // appending. The writer hands a batch over once per build
                    // slice, so the build pays a whole-index rewrite per slice
                    // and its write-ahead volume becomes quadratic in corpus
                    // size rather than linear. That is issue #2691, where this
                    // tree reached 25 GB of log while its largest sibling
                    // reached 185 MB.
                    //
                    // Recording the identifier rather than dropping it is what
                    // keeps this exact: the build re-reads anything it has not
                    // reached, and CatchUpAsync replays these once the build is
                    // Ready, so an identifier the build had already passed is
                    // refreshed instead of being left stale.
                    (_deferredWrites ??= new HashSet<string>(StringComparer.Ordinal))
                        .Add(update.VectorId);
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

    private async Task<DurableVectorIndex?> OpenAsync(CancellationToken cancellationToken)
    {
        if (_index is not null)
        {
            return _index;
        }

        // RETAINED ACROSS A FAULTED LOAD. The factory builds into a local and
        // returns only on success, so a load that threw used to discard the
        // partially-built identifier mapping along with the instance holding it -
        // and the next phase tick reissued the entire O(corpus) key-map walk. On a
        // tree whose leaves are slow to activate that regenerates the identical
        // demand on every attempt, which is the amplification half of #2953.
        // Keeping the instance is what lets the walk bank its progress.
        //
        // Full rather than lazy: a lazily loaded index is read-only by contract, and
        // this one has to be maintained in place as vectors are written.
        var resuming = _loading is not null && _loading.HasBankedLoadProgress;
        _loading ??= DurableVectorIndex.CreateUnloaded(
            _store, _source, _durableOptions, VectorIndexLoadMode.Full);

        // THE OPEN IS BOUNDED IN TIME, AND THIS IS ISSUE #3130's ITEM 1.
        //
        // Everything below this line used to run for as long as it ran, inside a
        // single non-reentrant coordinator turn, with the keep-alive reminder and
        // every arming call queued behind it - measured at over thirty minutes on
        // the acceptance rig. The ingest budget does not reach here: it is read
        // once the index is already open.
        //
        // Two token sources rather than one because the two cancellations mean
        // opposite things and the handler below has to tell them apart. The budget
        // source is the one this method owns; the linked source is what the load
        // actually sees, so a caller cancelling still cancels. Both are skipped
        // entirely when the bound is disabled, so the unbounded configuration pays
        // nothing for a feature it declined.
        var budget = _options.OpenSliceBudget;
        using var deadline = budget > TimeSpan.Zero
            ? new CancellationTokenSource(budget, _options.TimeProvider)
            : null;
        using var linked = deadline is null
            ? null
            : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, deadline.Token);

        try
        {
            // The deadline reaches the RESUMABLE key walk only; the caller's token
            // governs the load as a whole. Passing the deadline to both would bound
            // the restore too, and the restore banks nothing when interrupted - so
            // an index whose restore exceeds one slice would restart it every
            // attempt and could never open. See DurableVectorIndex.LoadOrResumeAsync.
            await _loading
                .LoadOrResumeAsync(linked?.Token ?? cancellationToken, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
            when (deadline is { IsCancellationRequested: true } && !cancellationToken.IsCancellationRequested)
        {
            // THE BUDGET EXPIRED, WHICH IS NOT A FAULT AND MUST NOT BE COUNTED AS
            // ONE. The distinction is read from the two sources rather than from
            // the exception, which carries no way to tell the difference: the
            // deadline fired and the caller's token did not, so this is the bound
            // working. The instance is deliberately kept - its banked progress is
            // the whole reason the bound is safe - and the next tick resumes past
            // it. See RepoContextAnnIndexLoadOutcome.Deferred for why folding this
            // into Faulted would reproduce the very wedge signal that diagnosed
            // this defect.
            // ONE ARM IS RECORDED PER ATTEMPT, SO THE DECISION COMES FIRST.
            // Recording Deferred here and then escalating would count a single
            // attempt under two arms and break the partition the snapshot claims.
            //
            // A DEFERRAL THAT BANKED NOTHING IS THE ONE WAY THIS BOUND CAN WEDGE,
            // so it is counted and escalated rather than retried for ever.
            //
            // EnsureBuiltAsync loops until the handle serves. Every slice that
            // banks at least one mapping makes that loop terminate, which is the
            // normal case and why a cold open over a large plane is merely sliced.
            // A budget too small to read a single record banks nothing on every
            // slice, and the loop then spins for ever having reproduced exactly
            // the wedge this issue exists to remove - a bounded open being, in
            // that configuration, strictly worse than an unbounded one.
            //
            // The counter is PRESENT-TENSE and is cleared by any slice that
            // advances, deliberately mirroring
            // VectorIndexBuildProgress.EmptyDeadlinesSinceLastAdvance. A lifetime
            // tally would be the wrong shape for the same reason documented there:
            // a plane that took a few empty slices early and then advanced
            // perfectly would go on reporting a wedge for ever.
            var loaded = _loading.LoadedKeyCount;
            if (loaded > _lastOpenKeyCount)
            {
                _lastOpenKeyCount = loaded;
                _emptyOpenDeferrals = 0;
            }
            else if (++_emptyOpenDeferrals >= MaxEmptyOpenDeferrals)
            {
                // Recorded HERE and not by the fault arm below, which cannot see
                // this: an exception thrown from inside a catch clause is not
                // caught by a sibling clause of the same try. Same reasoning as
                // that arm's own "record before the rethrow" note.
                _load?.Record(RepoContextAnnIndexLoadOutcome.Faulted);
                throw new InvalidOperationException(
                    $"The repository-context approximate index for '{_repoId}' in space "
                    + $"{_space.ModelId}/{_space.Dimension} reached its {budget} open budget "
                    + $"{MaxEmptyOpenDeferrals} times in succession without loading a single identifier "
                    + "mapping, so the open cannot make progress and would retry for ever. Raise "
                    + $"{nameof(RepoContextAnnOptions)}.{nameof(RepoContextAnnOptions.OpenSliceBudget)} "
                    + "above the time one store read takes, or set it to zero to open unbounded.");
            }

            _load?.Record(RepoContextAnnIndexLoadOutcome.Deferred);

            _logger.LogDebug(
                "Repository-context approximate index for {RepoId} in space {ModelId}/{Dimension} reached its "
                + "{Budget} open budget and yielded; the progress it banked is resumed on the next tick.",
                _repoId,
                _space.ModelId,
                _space.Dimension,
                budget);

            return null;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Recorded before the rethrow so the fault arm cannot be lost to the
            // propagation, and so faults and resumptions are counted on the same
            // path. The instance is deliberately NOT cleared: its banked progress
            // is what the next attempt resumes from.
            _load?.Record(RepoContextAnnIndexLoadOutcome.Faulted);
            throw;
        }

        _load?.Record(resuming
            ? RepoContextAnnIndexLoadOutcome.Resumed
            : RepoContextAnnIndexLoadOutcome.Fresh);

        _index = _loading;
        _loading = null;
        _progress = _index.Progress;

        _logger.LogInformation(
            "Repository-context approximate index for {RepoId} in space {ModelId}/{Dimension} opened in phase "
            + "{Phase} holding {VectorsIndexed} vectors (restored from durable state: {Restored}, "
            + "resumed a previously faulted load: {Resumed}).",
            _repoId,
            _space.ModelId,
            _space.Dimension,
            _progress.Phase,
            _progress.VectorsIndexed,
            _progress.RestoredFromDurableState,
            resuming);

        return _index;
    }

    private async Task CatchUpAsync(
        DurableVectorIndex index, bool probeSource, CancellationToken cancellationToken)
    {
        // WRITES DEFERRED DURING THE BUILD ARE REPLAYED HERE, and this is taken
        // before the probe's early exit below rather than after it. That exit
        // reasons that "anything written since arrives through the writer's
        // write-through seam" - which is precisely the seam ApplyWriteAsync now
        // defers, so leaving the drain behind it would strand every deferred
        // write on the path the build's own process takes.
        var deferred = _deferredWrites;
        _deferredWrites = null;

        // ONLY AN INDEX THIS PROCESS DID NOT STREAM NEEDS THE SHORTFALL PROBE.
        // The probe is an O(corpus) key walk whose only job is to decide whether a
        // persisted index is BEHIND the store of record. When this activation
        // streamed the corpus itself, the index covers exactly what it streamed and
        // anything written since arrives through the writer's write-through seam,
        // so the walk is pure cost - and it is the single most timeout-prone call
        // in the build, which took the whole build down with it (#1844).
        if (!probeSource && deferred is null)
        {
            await MaintainAsync(index, cancellationToken).ConfigureAwait(false);
            return;
        }

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
        //
        // There are now two ways to be unknown and they are handled identically. An
        // EnumerationAbortedException is the store losing the enumerator; a
        // RepoContextCountBudgetExceededException is the source declining to spend
        // more wall clock on the walk (#2447). The distinction matters in a log line
        // and nowhere else: neither yields a figure, and a missing figure has exactly
        // one safe reading here.
        var behind = true;
        if (deferred is null)
        {
            try
            {
                var expected = await _source.CountAsync(cancellationToken).ConfigureAwait(false);
                behind = expected > index.Count;
            }
            catch (Exception ex) when (ex is EnumerationAbortedException or RepoContextCountBudgetExceededException)
            {
                _logger.LogInformation(
                    ex,
                    "Repository-context approximate index for {RepoId} in space {ModelId}/{Dimension} could not count the "
                    + "source within its budget; treating the persisted index as possibly behind and repairing.",
                    _repoId,
                    _space.ModelId,
                    _space.Dimension);
            }
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
            if (index.TryGetKey(entry.Id, out _)
                && (deferred is null || !deferred.Contains(entry.Id)))
            {
                // Present and not deferred: the build read it, so it is current.
                // A deferred identifier is refreshed even when present, because
                // that is exactly the case the build cannot have picked up - it
                // had already streamed past that identifier when the write
                // arrived.
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
        // THRESHOLD CROSSING FIRST, and it is a different question from drift.
        // Drift asks "does the partitioning still describe the corpus"; this asks
        // "is there now enough corpus to partition at all". An index that declined
        // to partition can only ever be answered by the second, and until issue
        // #2706 only the first was asked - so a plane that declined on an empty
        // corpus stayed unpartitioned however large the corpus later grew.
        if (ShouldPartition(index))
        {
            _logger.LogInformation(
                "Repository-context approximate index for {RepoId} in space {ModelId}/{Dimension} is training a "
                + "partitioning for {Count} vectors: an earlier training declined because the corpus was below "
                + "the minimum training count of {Minimum}, and the corpus has since crossed it.",
                _repoId,
                _space.ModelId,
                _space.Dimension,
                index.Count,
                _options.MinimumTrainingCount);

            var attemptedAt = index.Count;
            await index.RetrainAsync(cancellationToken).ConfigureAwait(false);
            _pendingFlush = 0;
            _progress = index.Progress;

            var partitioned = index.Status.PartitionCount > 0;
            _partitioning?.RecordRepartition(partitioned);
            if (partitioned)
            {
                _logger.LogInformation(
                    "Repository-context approximate index for {RepoId} in space {ModelId}/{Dimension} is serving "
                    + "{VectorsIndexed} vectors across {Partitions} partitions; semantic retrieval is now "
                    + "approximate.",
                    _repoId,
                    _space.ModelId,
                    _space.Dimension,
                    _progress.VectorsIndexed,
                    _progress.PartitionsTotal);
                return;
            }

            // The corpus met the minimum and still resolved to fewer than two
            // partitions, so repeating the attempt at this size would burn a full
            // training pass per maintenance turn to reach the same answer. See
            // _nextPartitionAttemptCount.
            _nextPartitionAttemptCount = attemptedAt >= int.MaxValue / 2 ? int.MaxValue : Math.Max(1, attemptedAt) * 2;
            _logger.LogInformation(
                "Repository-context approximate index for {RepoId} in space {ModelId}/{Dimension} met the minimum "
                + "training count of {Minimum} with {Count} vectors but still resolves to fewer than two "
                + "partitions, so it stays exhaustive and exact; the next attempt waits for {Next} vectors.",
                _repoId,
                _space.ModelId,
                _space.Dimension,
                _options.MinimumTrainingCount,
                attemptedAt,
                _nextPartitionAttemptCount);
            return;
        }

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

    /// <summary>
    /// Whether the corpus has crossed the training minimum since a training
    /// declined to partition it, so a partitioning should be trained now.
    /// <para>
    /// <b>Every clause is read from the index itself, and that is the point.</b>
    /// This condition holds no memory of the decline that produced the state, and
    /// is not allowed to: the deployment issue #2706 measured had been latched for
    /// hours before the fix existed, so any trigger keyed on something recorded at
    /// decline time would have been keyed on a value that deployment does not have
    /// and never will. <c>Ready</c> with no partitioning and a corpus at or above
    /// the minimum is the whole signature, it is observable from a cold start over
    /// untouched durable state, and it is what makes the repair self-healing rather
    /// than something an operator has to trigger.
    /// </para>
    /// </summary>
    /// <param name="index">The index to judge.</param>
    /// <returns><see langword="true"/> when a partitioning should be trained.</returns>
    private bool ShouldPartition(DurableVectorIndex index)
    {
        // Phase Ready means the build pipeline ran to the end. It does NOT mean the
        // pipeline produced a partitioning, and the gap between those two is exactly
        // the state being repaired.
        if (index.Progress.Phase != VectorIndexBuildPhase.Ready
            || index.Status.PartitionCount > 0
            || index.Count < _options.MinimumTrainingCount)
        {
            return false;
        }

        return index.Count >= _nextPartitionAttemptCount;
    }

    private bool ShouldRetrain(DurableVectorIndex index)
    {
        // Only a PARTITIONED index can drift: drift is the corpus moving away from
        // a partitioning, so an index that holds none has nothing to move away from
        // and no fraction of it is meaningful. That is why this guard reads
        // VectorIndexState.Ready, which is reached only when PartitionCount is
        // positive.
        //
        // What this must not be read as saying - and did say, until issue #2706 -
        // is that retraining an unpartitioned index would be a no-op. For an index
        // that declined to partition because the corpus was below the minimum
        // training count, retraining once the corpus has grown past it is not a
        // no-op, it is the entire remedy, and asserting otherwise is what kept a
        // deployment answering every query by brute-force scan for 8.6 hours with a
        // corpus 7.5x the threshold. That case is a threshold crossing rather than
        // drift, it is unreachable from this predicate by construction, and it is
        // ShouldPartition's to answer.
        if (index.Progress.Phase != VectorIndexBuildPhase.Ready
            || index.Status.State != VectorIndexState.Ready
            || index.Count <= 0
            || _options.RetrainAfterUpdateFraction <= 0d)
        {
            return false;
        }

        return index.UpdatesSinceTraining >= index.Count * _options.RetrainAfterUpdateFraction;
    }

    /// <summary>
    /// Meters what the plane's partitioning looks like now that a build has
    /// finished, so the large-and-unpartitioned state is readable from a series
    /// instead of from three correlated log lines.
    /// </summary>
    private void RecordPartitioningState() => _partitioning?.RecordPartitioning(
        RepoContextAnnPartitioningReporter.Classify(
            _progress.PartitionsTotal, _progress.VectorsIndexed, _options.MinimumTrainingCount));

    private void MarkServing(DurableVectorIndex index)
    {
        _progress = index.Progress;
        if (IsServing)
        {
            return;
        }

        Volatile.Write(ref _serving, true);

        // The latch is deliberately NOT conditioned on the partition count. A
        // build that finished without partitioning still serves, exhaustively and
        // exactly, and declining to latch would spin EnsureBuiltAsync forever
        // against a corpus that is simply too small to partition. What must not
        // survive the partition count being zero is the CLAIM: announcing
        // approximate retrieval for an index holding no partitioning is the
        // dishonest half, and it is the half that is fixed here.
        if (_progress.PartitionsTotal > 0)
        {
            _logger.LogInformation(
                "Repository-context approximate index for {RepoId} in space {ModelId}/{Dimension} is serving "
                + "{VectorsIndexed} vectors across {Partitions} partitions; semantic retrieval is now approximate.",
                _repoId,
                _space.ModelId,
                _space.Dimension,
                _progress.VectorsIndexed,
                _progress.PartitionsTotal);
            return;
        }

        _logger.LogInformation(
            "Repository-context index for {RepoId} in space {ModelId}/{Dimension} is serving "
            + "{VectorsIndexed} vectors with no partitioning, so semantic retrieval stays exhaustive and exact. "
            + "Training declined to partition this corpus; it is below the minimum training count or resolves "
            + "to fewer than two partitions.",
            _repoId,
            _space.ModelId,
            _space.Dimension,
            _progress.VectorsIndexed);
    }
}
