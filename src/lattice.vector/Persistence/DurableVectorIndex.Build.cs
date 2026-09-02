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

        _index.Train();
        _updatesSinceTraining = 0;

        var superseded = _generation;
        await WritePartitionsAsync(_generation + 1, full: true, cancellationToken).ConfigureAwait(false);
        await _store.DeletePrefixAsync(
            VectorIndexStorageKeys.GenerationPrefix(_prefix, superseded), cancellationToken).ConfigureAwait(false);

        _phase = VectorIndexBuildPhase.Ready;
    }

    private async Task StartBuildAsync(CancellationToken cancellationToken)
    {
        _expected = await _source.CountAsync(cancellationToken).ConfigureAwait(false);
        _phase = VectorIndexBuildPhase.Ingesting;

        // Reserving up front is what makes the ingest run allocation-free: the
        // cell block is sized once instead of doubling as the corpus arrives.
        if (_expected > 0)
        {
            _index.EnsureCapacity(_expected);
        }

        await WriteBuildStateAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task IngestAsync(CancellationToken cancellationToken)
    {
        var budget = _options.IngestBatchSize;
        var consumed = 0;
        var chunkSize = _options.MaxItemsPerChunk;

        var entries = _source.EnumerateAsync(_cursor, cancellationToken);
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
                break;
            }
        }

        var complete = consumed < budget;
        await WriteIngestCheckpointAsync(complete, cancellationToken).ConfigureAwait(false);

        if (complete)
        {
            _phase = VectorIndexBuildPhase.Training;
        }

        await WriteBuildStateAsync(cancellationToken).ConfigureAwait(false);
    }

    private void Train()
    {
        // Synchronous and by far the most expensive step, which is exactly why it
        // is a step of its own: a host that cannot afford it right now simply
        // does not call this one, and keeps serving exact exhaustive answers.
        _index.Train();
        _updatesSinceTraining = 0;
        _phase = VectorIndexBuildPhase.Persisting;
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
