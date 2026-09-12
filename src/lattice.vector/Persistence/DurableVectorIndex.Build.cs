using System.Buffers;

namespace Orleans.Lattice.Vector.Persistence;

public sealed partial class DurableVectorIndex
{
    /// <summary>
    /// Advances the background build by one bounded step and reports where it
    /// got to.
    /// <para>
    /// The build is a pump the host drives rather than a thread the index owns.
    /// That is deliberate on three counts: the underlying index tolerates
    /// concurrent readers or a single writer but not both, so a build that ran
    /// itself would have to be fenced against every mutation; the host - a grain
    /// timer, a hosted service - already knows when it can afford the work; and a
    /// caller-driven step is testable without waiting on a clock.
    /// </para>
    /// <para>
    /// Every step ends at a durable checkpoint, and the checkpoint is written
    /// after the vectors it accounts for, so an interruption at any point resumes
    /// without duplicating or losing a vector. Meanwhile the index answers every
    /// query correctly by exhaustive scan, so the box serves throughout.
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">Cancels the step.</param>
    /// <returns>Progress after the step.</returns>
    /// <exception cref="InvalidOperationException">The index was opened lazily and is read-only.</exception>
    public async Task<VectorIndexBuildProgress> BuildStepAsync(CancellationToken cancellationToken = default)
    {
        RequireMutable();

        switch (_phase)
        {
            case VectorIndexBuildPhase.NotStarted:
                await StartBuildAsync(cancellationToken).ConfigureAwait(false);
                break;
            case VectorIndexBuildPhase.Ingesting:
                await IngestAsync(cancellationToken).ConfigureAwait(false);
                break;
            case VectorIndexBuildPhase.Training:
                Train();

                // Training continues straight into persisting rather than
                // yielding between them. The boundary cannot be an activation
                // boundary: AdoptBuildState deliberately normalises a durable
                // Persisting back to Training, because the manifest on disk is
                // the pre-training one and the partitioning that phase was about
                // to write does not exist. That normalisation is correct, but it
                // makes a build-state write at this boundary dead - the value
                // written is always read back as the value before it - so a host
                // that grants one build step per activation would re-train and
                // re-yield forever, never reaching Ready. Persisting here keeps
                // the recovery semantics (a crash mid-persist still resumes at
                // Training) while guaranteeing the step makes durable progress.
                await PersistTrainedAsync(cancellationToken).ConfigureAwait(false);
                break;
            case VectorIndexBuildPhase.Persisting:
                await PersistTrainedAsync(cancellationToken).ConfigureAwait(false);
                break;
            default:
                break;
        }

        return Progress;
    }

    /// <summary>
    /// Drives <see cref="BuildStepAsync"/> until the build completes. Convenient
    /// for a test or a start-up path that is willing to wait; a host that must
    /// stay responsive should drive the steps itself and serve in between.
    /// </summary>
    /// <param name="cancellationToken">Cancels the build between steps.</param>
    /// <returns>Progress once the build is complete.</returns>
    /// <exception cref="InvalidOperationException">The index was opened lazily and is read-only.</exception>
    public async Task<VectorIndexBuildProgress> RunBuildAsync(CancellationToken cancellationToken = default)
    {
        while (_phase != VectorIndexBuildPhase.Ready)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await BuildStepAsync(cancellationToken).ConfigureAwait(false);
        }

        return Progress;
    }

    /// <summary>
    /// Throws away every durable trace of the index and starts the build again
    /// from the store of record.
    /// <para>
    /// This is always safe, and that asymmetry is the point: the index is derived,
    /// so it may be discarded and recomputed at any time, while the store of
    /// record never may. It is the recovery path a failed verification takes
    /// automatically, and is exposed so an operator can take it deliberately.
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">Cancels the rebuild.</param>
    /// <exception cref="InvalidOperationException">The index was opened lazily and is read-only.</exception>
    public async Task RebuildAsync(CancellationToken cancellationToken = default)
    {
        RequireMutable();
        await DiscardAsync(cancellationToken).ConfigureAwait(false);
        _ingestAppendOnly = true;
        _chunkBoundaryCursor = null;
        _durableCursor = null;
    }

    /// <summary>
    /// Recomputes the partitioning over the vectors already in memory and
    /// commits it as a fresh generation.
    /// <para>
    /// Incremental maintenance keeps the index <i>correct</i> indefinitely - a
    /// vector is always in the cell nearest to it among the trained centroids -
    /// but it cannot keep it <i>well partitioned</i> when the corpus drifts away
    /// from the distribution it was trained on. Cells trained on the old
    /// distribution do not describe the new one, so more of the true neighbours
    /// fall outside the probed cells and approximate recall falls, silently and
    /// without any record being wrong. <see cref="UpdatesSinceTraining"/> is the
    /// signal for that, and this is the repair.
    /// </para>
    /// <para>
    /// It re-reads nothing: the corpus is already resident, so this costs a
    /// training pass and one rewrite rather than a pass over the store of record.
    /// It is synchronous and expensive - the same cost as the build's training
    /// step - so it belongs off the request path.
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">Cancels the rewrite that follows training.</param>
    /// <exception cref="InvalidOperationException">The index was opened lazily and is read-only.</exception>
    public async Task RetrainAsync(CancellationToken cancellationToken = default)
    {
        RequireMutable();

        TrainCore();

        var superseded = _generation;
        await WritePartitionsAsync(_generation + 1, full: true, cancellationToken).ConfigureAwait(false);
        await _store.DeletePrefixAsync(
            VectorIndexStorageKeys.GenerationPrefix(_prefix, superseded), cancellationToken).ConfigureAwait(false);

        _phase = VectorIndexBuildPhase.Ready;
    }

    private async Task StartBuildAsync(CancellationToken cancellationToken)
    {
        _expected = await CountSourceOrUnknownAsync(cancellationToken).ConfigureAwait(false);
        _phase = VectorIndexBuildPhase.Ingesting;

        // Reserving up front is what makes the ingest run allocation-free: the
        // cell block is sized once instead of doubling as the corpus arrives.
        if (_expected > 0)
        {
            _index.EnsureCapacity(_expected);
        }

        await WriteBuildStateAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The source's vector count, or <c>0</c> when it cannot be obtained.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>No failure of the count may fail the build.</b>
    /// <see cref="IVectorSource.CountAsync"/> states that the figure exists only to
    /// size the initial reservation and report progress, and that nothing depends on
    /// it for correctness. An unguarded <c>await</c> contradicted that contract: it
    /// made the one call in the build that is explicitly allowed to be wrong into the
    /// one call that could abort it.
    /// </para>
    /// <para>
    /// That is not hypothetical. It is what #1844 diagnosed on a live deployment - a
    /// reclaimed enumerator on the count walk took down the whole index build, which
    /// then retried and failed identically on every later query, so no index was ever
    /// persisted while retrieval silently fell back to the exact scan. The fix
    /// hardened the shortfall probe in the repository-context handle, which is the
    /// OTHER caller of this same method. This call site was never hardened, so the
    /// same fault arriving a few milliseconds earlier in the build still had the same
    /// effect. Bounding the walk by wall clock (#2447) adds a second, deliberate way
    /// for the count to be unavailable, which is what makes closing this residue
    /// necessary rather than merely tidy.
    /// </para>
    /// <para>
    /// The catch is broad on purpose, and the contract is what makes that correct
    /// rather than careless: the count is a hint from an implementation this index
    /// does not own, so the set of ways it can fail is not this type's to enumerate,
    /// and every one of them means the same thing here. Narrowing it to the fault
    /// types known today would re-open the residue for the next one. Cancellation is
    /// re-thrown, because a cancelled build must stop rather than quietly build
    /// itself without a reservation.
    /// </para>
    /// <para>
    /// Degrading to <c>0</c> costs only the up-front reservation: the ingest grows
    /// the cell block as the corpus arrives instead of sizing it once, and progress
    /// reports an unknown denominator. Both are the documented latitude of a hint.
    /// </para>
    /// </remarks>
    private async Task<int> CountSourceOrUnknownAsync(CancellationToken cancellationToken)
    {
        try
        {
            return await _source.CountAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception)
        {
            return 0;
        }
    }

    private async Task IngestAsync(CancellationToken cancellationToken)
    {
        var budget = _options.IngestBatchSize;
        var consumed = 0;
        var chunkSize = _options.EffectiveItemsPerChunk;

        // The wall-clock half of the bound. A step is capped by work count AND by
        // elapsed time, because those two are only interchangeable while the
        // per-item cost is small and predictable - which a source that streams
        // over a remote store of record does not guarantee. See
        // DurableVectorIndexOptions.IngestSliceBudget.
        var sliceBudget = _options.IngestSliceBudget;
        var timeProvider = _options.TimeProvider;
        var timeBounded = sliceBudget > TimeSpan.Zero;
        var startedAt = timeBounded ? timeProvider.GetTimestamp() : 0L;

        // Exhaustion is recorded where it is OBSERVED rather than inferred once
        // the loop is over, and the distinction is load-bearing. Running the loop
        // to its end is the source signalling that the corpus is finished, and it
        // is the only positive evidence of that there is. The inference it
        // replaces - "I consumed fewer than I was allowed, so the source must have
        // run out" - is sound only while exhaustion and the work budget are the
        // ONLY ways out of this loop, which is a property nobody wrote down and
        // which any new early exit silently breaks.
        //
        // Getting that wrong is not a build that stalls, which is loud and costs
        // nothing but time. It is a build that trains on a fraction of the corpus,
        // persists it, reports Ready, and serves approximate answers from a corpus
        // missing an arbitrary tail, with no error anywhere. Any early exit added
        // below must therefore clear this flag.
        var exhausted = true;

        // The deadline half of the bound. Sampling the budget in the loop body
        // bounds the time between items and nothing else, so it is not a bound at
        // all on a slice whose source yields NO item: the sample is never reached
        // and the step runs for as long as the source takes. A deadline is checked
        // while waiting, which is the state the step is actually stuck in. See
        // AwaitSourceMoveAsync and issues #2536 and #2483.
        //
        // Created UNARMED, and armed by AwaitSourceMoveAsync when the slice first
        // has to WAIT. Arming it here instead - at the start of the slice, before
        // the enumerator even exists - is what regressed #2651: the timer runs on
        // the clock rather than on consumption, so on a budget too small for one
        // item it fires before the first read is issued, the source is handed an
        // already-cancelled token, and the slice consumes nothing and advances no
        // cursor. That is the exact stall the post-consumption sample below was
        // written to prevent, reintroduced above it where it cannot be seen.
        // Arming on the first wait is what keeps the two compatible: the deadline
        // governs waiting, and a source that answers without waiting never meets
        // it at all. See SliceDeadline.
        using var sliceDeadline = timeBounded
            ? new SliceDeadline(timeProvider, sliceBudget, startedAt)
            : null;
        using var sliceCancellation = sliceDeadline is null
            ? null
            : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, sliceDeadline.Token);

        // Handed to the SOURCE so a spent budget also unwinds whatever retry,
        // backoff, or reconnect loop the source is running internally. Every
        // durability write below is deliberately issued on the CALLER's token
        // instead: the slice token is already cancelled by the time the banking
        // path runs, so writing under it would abandon the checkpoint that is the
        // entire point of stopping.
        var sourceToken = sliceCancellation?.Token ?? cancellationToken;

        var enumerator = _source.EnumerateAsync(_cursor, sourceToken).GetAsyncEnumerator(sourceToken);

        // A source read that outlived the deadline is ABANDONED rather than
        // awaited, so the step's bound does not degrade to the source's own
        // timeout. The pending read is left to complete in the background and its
        // outcome is observed there.
        Task<bool>? abandoned = null;
        try
        {
            try
            {
                while (true)
                {
                    var (hasNext, pending) = await AwaitSourceMoveAsync(
                        enumerator, sliceDeadline, consumed).ConfigureAwait(false);

                    if (pending is not null)
                    {
                        // The deadline won the race. Everything consumed so far is
                        // banked by the checkpoint below, exactly as a spent work
                        // budget would bank it.
                        abandoned = pending;
                        exhausted = false;
                        RecordDeadlinedSlice(consumed);
                        break;
                    }

                    if (!hasNext)
                    {
                        break;
                    }

                    var entry = enumerator.Current;
                    var position = _index.Count;

                    // Deliberately the CALLER's token and not the slice token. An
                    // item the source has already handed over is work already
                    // done, and cutting the key reservation short would discard it
                    // and leave the cursor behind it - so a budget that expired
                    // while this item was in flight would cost the very progress
                    // the deadline exists to preserve. The deadline governs
                    // WAITING for the source; it does not interrupt banking an
                    // item that has already arrived.
                    var key = await _keys.GetOrAddAsync(entry.Id, cancellationToken).ConfigureAwait(false);
                    if (_index.Upsert(key, entry.Vector.Span))
                    {
                        // A replacement is not an append, so the committed chunk prefix
                        // is no longer a prefix of the cell and the checkpoint has to
                        // rewrite it wholesale.
                        _ingestAppendOnly = false;
                    }
                    else if ((position + 1) % chunkSize == 0)
                    {
                        _chunkBoundaryCursor = entry.Id;
                    }

                    _cursor = entry.Id;
                    if (++consumed >= budget)
                    {
                        exhausted = false;
                        break;
                    }

                    // Checked AFTER an item has been consumed, so a budget too small for
                    // even one item degrades to one item per step rather than to a step
                    // that consumes nothing and spins forever making no progress.
                    //
                    // This sample bounds the gap BETWEEN items and nothing more. It is
                    // still the right check for a source that responds, and it is not
                    // the bound for a source that does not: that case never reaches
                    // this line, and it is the deadline above - not this - that governs
                    // it. Do not "simplify" the two into one.
                    //
                    // The bound is carried by its own flag rather than by a sentinel
                    // timestamp: zero is a perfectly ordinary reading of a clock, so a
                    // "0 means disabled" sentinel silently disables the bound on any
                    // provider whose epoch the step happens to start at.
                    if (timeBounded && timeProvider.GetElapsedTime(startedAt) >= sliceBudget)
                    {
                        exhausted = false;
                        break;
                    }
                }
            }
            catch (OperationCanceledException) when (SliceDeadlineSpent(sliceDeadline, cancellationToken))
            {
                // The deadline reached the source before the race did - the source
                // observed the token and unwound itself. That is the budget doing
                // its job, not a fault, so it takes the same exit a spent work
                // budget takes: bank what was consumed and report an incomplete
                // slice. Rethrowing instead would turn every bounded slice into a
                // failed phase tick.
                exhausted = false;
                RecordDeadlinedSlice(consumed);
            }
            catch (Exception) when (!cancellationToken.IsCancellationRequested)
            {
                // The slice faulted part way through. Everything consumed before the
                // fault is already in the in-memory cell and the cursor already names
                // the last of it, so the ONLY thing standing between that work and
                // durability is the checkpoint below - which the fault would otherwise
                // skip on its way out. See BankFaultedSliceAsync.
                exhausted = false;
                if (consumed > 0)
                {
                    await BankFaultedSliceAsync(cancellationToken).ConfigureAwait(false);
                }

                throw;
            }
        }
        finally
        {
            await DisposeSourceEnumeratorAsync(enumerator, abandoned).ConfigureAwait(false);
        }

        await WriteIngestCheckpointAsync(exhausted, cancellationToken).ConfigureAwait(false);

        if (exhausted)
        {
            _phase = VectorIndexBuildPhase.Training;
        }

        await WriteBuildStateAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Records that a slice was stopped by its wall-clock deadline, and whether
    /// it banked anything before it was.
    /// </summary>
    /// <param name="consumed">How many source items the slice consumed.</param>
    /// <remarks>
    /// The second figure is the one worth having. "A deadline fired" says only
    /// that the bound works; it cannot distinguish a build that is advancing in
    /// short slices from one that is not advancing at all, and those need
    /// opposite remedies - tune the budget, versus fix a source read that cannot
    /// complete. Recording them apart is what lets a later run report a verdict
    /// rather than an argument. See
    /// <see cref="VectorIndexBuildProgress.IsStarvedBySource"/>.
    /// </remarks>
    private void RecordDeadlinedSlice(int consumed)
    {
        _slicesDeadlined++;
        if (consumed == 0)
        {
            _slicesDeadlinedWithoutProgress++;
        }
    }

    /// <summary>
    /// Whether a slice stopped because its own deadline was spent rather than
    /// because the caller cancelled the build.
    /// </summary>
    /// <remarks>
    /// The distinction is the whole reason the deadline is a separate source
    /// linked into the caller's token rather than the caller's token itself. A
    /// spent deadline is a bounded slice reporting for duty - it banks and
    /// returns - while a cancelled build must stop and say so. Collapsing the two
    /// onto one token makes them indistinguishable at the catch, and whichever
    /// way that ambiguity is resolved is wrong half the time: treat it as
    /// cancellation and every bounded slice becomes a failed phase tick, treat it
    /// as a budget yield and a cancelled build quietly checkpoints and carries on.
    /// </remarks>
    private static bool SliceDeadlineSpent(
        SliceDeadline? sliceDeadline, CancellationToken cancellationToken) =>
        sliceDeadline is { IsCancellationRequested: true } && !cancellationToken.IsCancellationRequested;

    /// <summary>
    /// Advances the source by one item, racing the read against the slice
    /// deadline, and reports either the item or the read the deadline abandoned.
    /// </summary>
    /// <param name="enumerator">The source enumerator being walked.</param>
    /// <param name="sliceDeadline">The slice deadline, or <see langword="null"/> when the step is not time-bounded.</param>
    /// <param name="consumed">How many items the slice has already banked.</param>
    /// <returns>
    /// <c>HasNext</c> is the read's result when the read won; <c>Pending</c> is
    /// non-<see langword="null"/> exactly when the deadline won, and carries the
    /// still-running read so the caller can abandon rather than await it.
    /// </returns>
    /// <remarks>
    /// <para>
    /// <b>Why the read is raced rather than merely cancelled.</b> Handing the
    /// source a cancelled token unwinds the loops the source controls - its
    /// retries, its backoff delays, its reconnects - which is most of the damage.
    /// It does not unwind a single remote call already in flight, because a call
    /// that has been dispatched is bounded by the callee's own ceiling and not by
    /// this token. Awaiting it would silently widen the step's bound from the
    /// slice budget to the slice budget PLUS that ceiling, which on the measured
    /// deployment is the difference between staying inside the coordinator's
    /// reminder timeout and missing it.
    /// </para>
    /// <para>
    /// <b>Why the fast path matters, and what it does NOT carry.</b> A source that
    /// has already buffered a page completes synchronously, which is the
    /// overwhelming majority of items in a slice. Racing those would allocate a
    /// task and a registration per item for no benefit, so a completed read is
    /// taken directly and the race is built only for a read that actually has to
    /// wait. It is tempting to promote this to the guarantee - "a synchronous
    /// source never meets the deadline, so it cannot be preempted" - and that
    /// reading is true but is not what holds the guarantee up. Perturbing this
    /// branch away, so that an already-completed read is raced too, leaves the
    /// fixture green 20 times in 20: an already-completed <c>pending</c> beats a
    /// <see cref="TaskCompletionSource"/> that still needs a timer callback, and
    /// <see cref="Task.WhenAny(Task[])"/> resolves in its favour deterministically.
    /// The guarantee is carried by WHEN the deadline is armed - see
    /// <see cref="SliceDeadline"/>, whose perturbation is red 20 times in 20 - and
    /// this branch is the optimisation it has always been. Recorded because the
    /// overclaim is the more plausible comment to write, and it would have sent the
    /// next reader to defend the wrong line.
    /// </para>
    /// </remarks>
    private static async ValueTask<(bool HasNext, Task<bool>? Pending)> AwaitSourceMoveAsync(
        IAsyncEnumerator<VectorSourceEntry> enumerator,
        SliceDeadline? sliceDeadline,
        int consumed)
    {
        var move = enumerator.MoveNextAsync();
        if (move.IsCompleted || sliceDeadline is null)
        {
            return (await move.ConfigureAwait(false), null);
        }

        // The step has to wait, which is the only state a deadline can help with,
        // so this is where the deadline starts running. It is deliberately NOT
        // started when the slice starts: see the call site.
        sliceDeadline.Arm(consumed);

        var pending = move.AsTask();
        var deadlineReached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var registration = sliceDeadline.Token
            .Register(static state => ((TaskCompletionSource)state!).TrySetResult(), deadlineReached)
            .ConfigureAwait(false);

        var winner = await Task.WhenAny(pending, deadlineReached.Task).ConfigureAwait(false);
        return ReferenceEquals(winner, pending)
            ? (await pending.ConfigureAwait(false), null)
            : (false, pending);
    }

    /// <summary>
    /// A slice's wall-clock deadline, armed when the slice first has to WAIT for
    /// its source rather than when the slice begins.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>The arming moment is the whole of this type.</b> A deadline created
    /// already running measures the slice, and a slice is not what needs bounding:
    /// the step's own post-consumption sample already bounds a source that
    /// answers, and it does so in a way that cannot stall, because it is evaluated
    /// only after an item has been banked. What that sample cannot bound is a
    /// source that never answers, because it is never reached. The deadline exists
    /// for exactly that gap and for nothing else, so it is started where the gap
    /// is - at a wait.
    /// </para>
    /// <para>
    /// Starting it a moment earlier reopens the stall it was added to close, from
    /// the other end. #2584 armed it at the top of the slice, so on a budget too
    /// small for one item the timer fired before the first read was even issued:
    /// the source was handed a token that was already cancelled, unwound without
    /// yielding, and the slice banked nothing and moved no cursor - once per step,
    /// forever. #2651 measured that as a coin flip in four distinct shapes, at
    /// rates between 30% and 95% depending on who ran it and under what load, on
    /// a tree that was byte-identical every time. The absence of a stable rate is
    /// itself the finding: a real timer racing a source read resolves differently
    /// on every machine and every run, so no single figure here is "the" rate and
    /// only a paired before/after contrast means anything. Arming on the wait
    /// removes the race rather than shortening its odds: a source that answers
    /// synchronously never reaches <see cref="Arm"/> at all.
    /// </para>
    /// <para>
    /// <b>A slice that has banked nothing gets the full budget.</b> The window is
    /// measured from the wait, and a slice with nothing banked is given the whole
    /// budget rather than whatever remains of it. Truncating that window protects
    /// no progress - there is none to protect - while guaranteeing the stall, so
    /// the two cases are not symmetric and are not treated as though they were.
    /// Once the slice has banked something, cutting a later wait short does
    /// preserve real work, so the remaining budget is the right window and is what
    /// is used. Either way the slice still returns within about one budget of the
    /// moment it started waiting, which is the bound #2536 and #2483 need.
    /// </para>
    /// <para>
    /// The timer is created from the <see cref="TimeProvider"/> so a host that
    /// controls the clock controls the deadline too, and it is armed at most once:
    /// the first wait is what the bound is measured from, and re-arming on each
    /// later wait would let a source that answers just often enough extend the
    /// slice indefinitely, one grudging item at a time. That hazard needs an
    /// adversarial source to exhibit and no fixture currently drives one, so this
    /// particular guard rests on the argument rather than on a measurement -
    /// removing it leaves the suite green. Worth knowing before it is tidied away
    /// on the strength of that green.
    /// </para>
    /// </remarks>
    private sealed class SliceDeadline(TimeProvider timeProvider, TimeSpan budget, long startedAt) : IDisposable
    {
        private readonly CancellationTokenSource _cancellation = new();
        private ITimer? _timer;
        private bool _armed;

        /// <summary>The token that is cancelled once the deadline is spent.</summary>
        internal CancellationToken Token => _cancellation.Token;

        /// <summary>Whether the deadline has been spent.</summary>
        internal bool IsCancellationRequested => _cancellation.IsCancellationRequested;

        /// <summary>
        /// Starts the deadline, if it is not already running.
        /// </summary>
        /// <param name="consumed">How many items the slice has banked so far.</param>
        internal void Arm(int consumed)
        {
            if (_armed)
            {
                return;
            }

            _armed = true;

            // A slice with nothing banked is bounded from here by the whole
            // budget; one that has banked is bounded by what is left of it. See
            // the remarks.
            var window = consumed == 0 ? budget : budget - timeProvider.GetElapsedTime(startedAt);
            if (window <= TimeSpan.Zero)
            {
                _cancellation.Cancel();
                return;
            }

            _timer = timeProvider.CreateTimer(
                static state => ((CancellationTokenSource)state!).Cancel(),
                _cancellation,
                window,
                Timeout.InfiniteTimeSpan);
        }

        public void Dispose()
        {
            _timer?.Dispose();
            _cancellation.Dispose();
        }
    }

    /// <summary>
    /// Releases the source enumerator a slice was walking, deferring the release
    /// when a read the deadline abandoned is still in flight.
    /// </summary>
    /// <param name="enumerator">The enumerator to release.</param>
    /// <param name="abandoned">The abandoned read, or <see langword="null"/> when no read is outstanding.</param>
    /// <remarks>
    /// <para>
    /// Disposing an async enumerator while one of its <c>MoveNextAsync</c> calls
    /// is still running is undefined by the contract and is a use-after-free in
    /// practice for any implementation that pools per-call state, so the disposal
    /// is chained onto the outstanding read instead of racing it.
    /// </para>
    /// <para>
    /// The continuation also OBSERVES the abandoned read. That read is very likely
    /// to fault - the deadline fired precisely because it was not returning - and
    /// an unobserved faulted task is a process-level escalation on a
    /// <c>TaskScheduler.UnobservedTaskException</c> policy that rethrows. The
    /// slice already reported what it needed to by banking and returning, so the
    /// fault is discarded rather than surfaced: raising it here would attribute a
    /// read the step deliberately walked away from to whichever unrelated turn
    /// happened to be running when it finally completed.
    /// </para>
    /// </remarks>
    private static ValueTask DisposeSourceEnumeratorAsync(
        IAsyncEnumerator<VectorSourceEntry> enumerator, Task<bool>? abandoned)
    {
        if (abandoned is null)
        {
            return enumerator.DisposeAsync();
        }

        _ = DrainAbandonedSourceReadAsync(enumerator, abandoned);
        return ValueTask.CompletedTask;
    }

    private static async Task DrainAbandonedSourceReadAsync(
        IAsyncEnumerator<VectorSourceEntry> enumerator, Task<bool> abandoned)
    {
        try
        {
            await abandoned.ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Deliberately discarded; see DisposeSourceEnumeratorAsync's remarks.
        }

        try
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Deliberately discarded; see DisposeSourceEnumeratorAsync's remarks.
        }
    }

    /// <summary>
    /// Persists the items a faulted ingest slice had already consumed, so a slice
    /// that dies part way through banks that work instead of discarding it.
    /// </summary>
    /// <param name="cancellationToken">Cancels the checkpoint writes.</param>
    /// <remarks>
    /// <para>
    /// <b>The step's progress guarantee did not extend to the slice.</b>
    /// <see cref="DurableVectorIndexOptions.IngestSliceBudget"/> documents that the
    /// budget is checked only after an item has been consumed, so a step always
    /// makes progress. That reasoning holds for the budget and only for the budget.
    /// It says nothing about a slice that FAULTS, and a fault took a different exit:
    /// it propagated out of the enumeration before the checkpoint, so the items
    /// already consumed - up to a full batch of them - were durably discarded and
    /// re-read on the next step, which then faulted at the same place.
    /// </para>
    /// <para>
    /// <b>And it stopped one step short of the case that bites.</b> The sentence
    /// above correctly names the boundary of its own argument, then does not follow
    /// it to the end: consuming zero items defeats the budget AND this banking, from
    /// a single cause, because both are conditioned on the same event. The
    /// <c>consumed &gt; 0</c> guard at this method's call site is the second half of
    /// that sentence. So banking is necessary and is not sufficient, and a fix that
    /// stops here measures like no fix at all against the fault that was actually
    /// observed - a source page that stalls BEFORE yielding its first item, which
    /// banks nothing, advances no cursor, and is re-read identically on every
    /// subsequent step. The sufficient half is that the budget is also armed as a
    /// deadline, so a slice is bounded whether or not the source ever answers.
    /// </para>
    /// <para>
    /// That is what #2536 measured on a live deployment. A source that streams over
    /// a remote store of record reaches it through a grain call, and a grain call
    /// against a contended non-reentrant shard root can exceed the cluster's
    /// response timeout while merely QUEUED, before its first statement runs. The
    /// caller sees a bare <c>TimeoutException</c>, the slice unwinds, and the build
    /// re-reads the same range on every tick without ever banking a byte. On the
    /// measured container the approximate plane answered none of 13 searches and
    /// took over four hours to reach Ready.
    /// </para>
    /// <para>
    /// Banking converts that from all-or-nothing into monotone progress: each slice
    /// keeps what it read, so a build advances through a contended range at the rate
    /// the range can actually be read rather than not at all. It does NOT hide the
    /// fault - the caller rethrows, so the coordinator still logs and still counts
    /// the failure. Making a repeated failure cheap is the point; making it silent
    /// would not be.
    /// </para>
    /// <para>
    /// <b>Exhaustion is not claimed.</b> The caller clears its exhaustion flag before
    /// calling this, and the checkpoint is written as incomplete. A fault is the one
    /// exit that carries no evidence the corpus ended, and claiming otherwise would
    /// train on a truncated corpus, persist it, and report Ready with no error
    /// anywhere - the failure the flag's own comment exists to prevent.
    /// </para>
    /// <para>
    /// <b>A failure to bank is swallowed.</b> The store this writes to is the same
    /// one whose contention produced the original fault, so it is entirely plausible
    /// that it refuses this write too. That must not replace the slice's own
    /// exception with a second one: the first names why the slice stopped, which is
    /// what an operator needs, whereas a failed checkpoint only means this slice
    /// banked nothing - exactly the pre-existing behaviour, and no worse than it.
    /// The catch is broad for the same reason it is broad on
    /// <c>CountSourceOrUnknownAsync</c>: the failure modes of a store this type does
    /// not own are not this type's to enumerate, and every one of them means the
    /// same thing here.
    /// </para>
    /// </remarks>
    private async Task BankFaultedSliceAsync(CancellationToken cancellationToken)
    {
        try
        {
            await WriteIngestCheckpointAsync(false, cancellationToken).ConfigureAwait(false);
            await WriteBuildStateAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Deliberately swallowed; see the remarks.
        }
    }

    private void Train()
    {
        // Synchronous and by far the most expensive step, which is exactly why it
        // is a step of its own: a host that cannot afford it right now simply
        // does not call this one, and keeps serving exact exhaustive answers.
        TrainCore();
        _phase = VectorIndexBuildPhase.Persisting;
    }

    /// <summary>
    /// Runs the training pass and consumes its result, which reports whether a
    /// usable partitioning was produced.
    /// <para>
    /// That return value used to be discarded at both call sites, and every
    /// dishonest readiness signal downstream originated in the discard: nothing
    /// between here and a consumer could tell a build that partitioned from one
    /// that did not, because the only value that said so had been thrown away and
    /// the phase advanced regardless. The phase advancing is correct - the build
    /// really did finish, and the index really is serving exact exhaustive
    /// answers - so the repair is not to withhold
    /// <see cref="VectorIndexBuildPhase.Ready"/> but to keep the partitioning
    /// observable beside it.
    /// </para>
    /// <para>
    /// <see cref="VectorIndexBuildProgress.PartitionsTotal"/> is what carries it,
    /// and <see cref="VectorIndexBuildProgress.IsReady"/> now depends on the two
    /// agreeing: a <see langword="false"/> return drops the partitioning, and a
    /// <see langword="true"/> return commits at least two cells. Checking that
    /// agreement here is what consumes the value rather than discarding it again,
    /// and it converts any future divergence into a loud failure at the point of
    /// training instead of a quiet lie in a readiness signal several layers away.
    /// It cannot fire against the current implementation.
    /// </para>
    /// </summary>
    private void TrainCore()
    {
        var trained = _index.Train();
        _updatesSinceTraining = 0;

        if (trained != _index.PartitionCount > 0)
        {
            throw new InvalidOperationException(
                $"Training reported trained={trained} while holding {_index.PartitionCount} partitions. "
                + "These must agree: readiness is reported from the partition count, so a disagreement "
                + "would publish a partitioning the index does not have, or hide one it does.");
        }
    }

    private async Task PersistTrainedAsync(CancellationToken cancellationToken)
    {
        var superseded = _generation;
        var generation = _generation + 1;

        // The trained layout is written beside the untrained one and the manifest
        // is flipped at the end, so a crash anywhere in here leaves the untrained
        // generation loadable and the build resumes at training rather than at
        // the source.
        await WritePartitionsAsync(generation, full: true, cancellationToken).ConfigureAwait(false);
        await _store.DeletePrefixAsync(
            VectorIndexStorageKeys.GenerationPrefix(_prefix, superseded), cancellationToken).ConfigureAwait(false);

        _phase = VectorIndexBuildPhase.Ready;
        _durableCursor = _cursor;
        await _store.DeleteAsync(
            [VectorIndexStorageKeys.BuildState(_prefix)], cancellationToken).ConfigureAwait(false);
    }

    private Task WriteBuildStateAsync(CancellationToken cancellationToken)
    {
        var state = new VectorIndexBuildState(_generation, _phase, _index.Count, _expected, _durableCursor);
        return _store.WriteAsync(
            [new KeyValuePair<string, byte[]>(VectorIndexStorageKeys.BuildState(_prefix), state.ToRecord())],
            cancellationToken);
    }

    private async ValueTask<VectorSearchOutcome> SearchLazyAsync(
        ReadOnlyMemory<float> query,
        Memory<VectorSearchResult> results,
        CancellationToken cancellationToken)
    {
        var wanted = Math.Min(_index.Probes, _index.PartitionCount);
        var probes = ArrayPool<int>.Shared.Rent(wanted);
        try
        {
            var selected = _index.SelectPartitions(query.Span, probes.AsSpan(0, wanted));
            for (var i = 0; i < selected; i++)
            {
                var partition = probes[i];
                if (_resident[partition])
                {
                    continue;
                }

                await ApplyPartitionAsync(
                    _index,
                    _generation,
                    partition,
                    _persistedEpoch[partition],
                    _persistedChunkCount[partition],
                    cancellationToken).ConfigureAwait(false);

                // A cell fetched after a retirement was journalled would
                // otherwise reintroduce the retired vector, so the journal is
                // replayed against every cell as it arrives, not only at load.
                foreach (var retired in _pendingRetirements)
                {
                    _index.Remove(retired);
                }

                _resident[partition] = true;
            }
        }
        finally
        {
            ArrayPool<int>.Shared.Return(probes);
        }

        var found = _index.Search(query.Span, results.Span, out var mode);
        return new VectorSearchOutcome(found, mode);
    }
}
