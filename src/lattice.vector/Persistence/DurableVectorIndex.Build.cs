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
                await WriteBuildStateAsync(cancellationToken).ConfigureAwait(false);
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
        var chunkSize = _options.MaxItemsPerChunk;

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

        var entries = _source.EnumerateAsync(_cursor, cancellationToken);
        try
        {
            await foreach (var entry in entries.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                var position = _index.Count;
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

        await WriteIngestCheckpointAsync(exhausted, cancellationToken).ConfigureAwait(false);

        if (exhausted)
        {
            _phase = VectorIndexBuildPhase.Training;
        }

        await WriteBuildStateAsync(cancellationToken).ConfigureAwait(false);
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
