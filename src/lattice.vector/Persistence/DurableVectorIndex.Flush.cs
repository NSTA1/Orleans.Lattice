namespace Orleans.Lattice.Vector.Persistence;

public sealed partial class DurableVectorIndex
{
    // A flush accumulates chunk records up to this many bytes before issuing a
    // write, so one round trip stays bounded no matter how large a partition or
    // an ingest batch is.
    private const int WriteBatchBytes = 4 * 1024 * 1024;

    private bool _ingestAppendOnly = true;
    private string? _chunkBoundaryCursor;
    private string? _durableCursor;

    /// <summary>
    /// Makes the index's current contents durable.
    /// <para>
    /// Only partitions whose version stamp has moved are rewritten, so a flush
    /// after a handful of updates costs a handful of cells rather than the
    /// corpus. Records are written first and the manifest last, so an interrupted
    /// flush leaves the previously committed index intact and loadable rather
    /// than a mixture of two.
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">Cancels the flush.</param>
    /// <exception cref="InvalidOperationException">The index was opened lazily and is read-only.</exception>
    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        RequireMutable();
        return WritePartitionsAsync(_generation, full: false, cancellationToken);
    }

    /// <summary>
    /// Persists every partition of the index under a generation, writing the
    /// manifest last so the whole set becomes visible at once.
    /// </summary>
    private async Task WritePartitionsAsync(long generation, bool full, CancellationToken cancellationToken)
    {
        var snapshot = _index.CreateSnapshot(_options.MaxItemsPerChunk);
        var header = snapshot.Header;
        var slots = Math.Max(1, header.PartitionCount);
        var epoch = header.IndexVersion;
        EnsureSlotArrays(slots);

        var centroidEpoch = _centroidEpoch;
        if (header.PartitionCount > 0 && (full || !_centroidsPersisted))
        {
            centroidEpoch = epoch;
            await WriteChunkRangeAsync(
                snapshot,
                VectorIndexChunkKind.Centroids,
                partition: 0,
                first: 0,
                count: header.CentroidChunkCount,
                sequenceBase: 0,
                generation,
                epoch,
                cancellationToken).ConfigureAwait(false);
        }

        var chunkIndex = header.CentroidChunkCount;
        for (var partition = 0; partition < slots; partition++)
        {
            var chunks = CountChunks(snapshot, chunkIndex, partition);
            var size = header.PartitionCount == 0 ? _index.Count : _index.PartitionSize(partition);
            var version = PartitionVersionOf(partition);

            if (!full && version == _persistedPartitionVersion[partition])
            {
                chunkIndex += chunks;
                continue;
            }

            var previousEpoch = _persistedEpoch[partition];
            await WriteChunkRangeAsync(
                snapshot,
                VectorIndexChunkKind.Vectors,
                partition,
                first: chunkIndex,
                count: chunks,
                sequenceBase: chunkIndex,
                generation,
                epoch,
                cancellationToken).ConfigureAwait(false);

            await CommitPartitionAsync(
                generation, partition, epoch, chunks, size, version, previousEpoch, cancellationToken)
                .ConfigureAwait(false);

            chunkIndex += chunks;
        }

        await CommitManifestAsync(generation, centroidEpoch, header, header.Count, cancellationToken)
            .ConfigureAwait(false);

        _centroidsPersisted = header.PartitionCount > 0;
        _centroidEpoch = centroidEpoch;
        _generation = generation;
        _persistedPartitions = slots;
        await SweepRetirementsAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Persists the complete chunks of the single, append-only cell an untrained
    /// index holds during a build.
    /// <para>
    /// While the index is ingesting, the cell is only ever appended to, so every
    /// chunk but the last is immutable once written. Persisting only the complete
    /// ones means a checkpoint writes just the vectors arrived since the previous
    /// one - the whole build costs one pass over the corpus rather than one pass
    /// per checkpoint - and no committed record is ever rewritten, so an
    /// interrupted checkpoint cannot damage what is already durable. The price is
    /// that the cursor lags by less than one chunk, which the next build step
    /// simply re-consumes.
    /// </para>
    /// </summary>
    private async Task WriteIngestCheckpointAsync(bool complete, CancellationToken cancellationToken)
    {
        if (!_ingestAppendOnly || _index.PartitionCount > 0)
        {
            // Something removed or replaced a vector mid-build, so the cell is no
            // longer append-only and its committed chunks can no longer be
            // trusted to be a prefix of the current one. Fall back to rewriting
            // the whole cell under a fresh epoch.
            await WritePartitionsAsync(_generation, full: true, cancellationToken).ConfigureAwait(false);
            _ingestAppendOnly = true;
            _chunkBoundaryCursor = _cursor;
            _durableCursor = _cursor;
            return;
        }

        var snapshot = _index.CreateSnapshot(_options.MaxItemsPerChunk);
        var header = snapshot.Header;
        EnsureSlotArrays(1);

        var epoch = _persistedChunkCount[0] == 0 ? header.IndexVersion : _persistedEpoch[0];
        var chunks = complete
            ? header.ChunkCount
            : _index.Count / _options.MaxItemsPerChunk;
        var committedCount = complete ? _index.Count : chunks * _options.MaxItemsPerChunk;

        if (chunks > _persistedChunkCount[0])
        {
            await WriteChunkRangeAsync(
                snapshot,
                VectorIndexChunkKind.Vectors,
                partition: 0,
                first: _persistedChunkCount[0],
                count: chunks - _persistedChunkCount[0],
                sequenceBase: 0,
                _generation,
                epoch,
                cancellationToken).ConfigureAwait(false);
        }

        await CommitPartitionAsync(
            _generation,
            partition: 0,
            epoch,
            chunks,
            committedCount,
            PartitionVersionOf(0),
            previousEpoch: epoch,
            cancellationToken).ConfigureAwait(false);

        // The manifest describes the committed prefix, not the in-memory index:
        // its count and chunk count are the ones a loader will actually be able
        // to read back.
        var committedHeader = header with { Count = committedCount, ChunkCount = chunks };
        await CommitManifestAsync(_generation, _centroidEpoch, committedHeader, committedCount, cancellationToken)
            .ConfigureAwait(false);

        _persistedPartitions = 1;

        if (!complete)
        {
            // Only a prefix of the cell is committed, so the partition must stay
            // dirty even though nothing has mutated since. Marking it clean here
            // would let a later ordinary flush skip it and then write a manifest
            // claiming the whole in-memory count, which a loader would correctly
            // refuse - turning a routine flush into a spurious rebuild.
            _persistedPartitionVersion[0] = -1;
        }

        // The durable cursor names the committed prefix, never the in-memory
        // one: it may only ever be behind what is persisted, so a resume
        // re-consumes a partial chunk rather than skipping it.
        _durableCursor = complete ? _cursor : _chunkBoundaryCursor;
    }

    private async Task CommitPartitionAsync(
        long generation,
        int partition,
        long epoch,
        int chunkCount,
        int vectorCount,
        long version,
        long previousEpoch,
        CancellationToken cancellationToken)
    {
        var state = new VectorIndexPartitionState(epoch, chunkCount, vectorCount, version);
        await _store.WriteAsync(
            [new KeyValuePair<string, byte[]>(
                VectorIndexStorageKeys.PartitionState(_prefix, generation, partition), state.ToRecord())],
            cancellationToken).ConfigureAwait(false);

        _persistedEpoch[partition] = epoch;
        _persistedChunkCount[partition] = chunkCount;
        _persistedPartitionVersion[partition] = version;

        if (previousEpoch != epoch)
        {
            // The superseded epoch is unreachable the moment the state record
            // names the new one, so sweeping it is reclamation rather than part
            // of the commit.
            await _store.DeletePrefixAsync(
                VectorIndexStorageKeys.PartitionEpochPrefix(_prefix, generation, partition, previousEpoch),
                cancellationToken).ConfigureAwait(false);
        }
    }

    private Task CommitManifestAsync(
        long generation,
        long centroidEpoch,
        VectorIndexHeader header,
        int indexedCount,
        CancellationToken cancellationToken)
    {
        var manifest = new VectorIndexManifest(generation, centroidEpoch, indexedCount, header);
        return _store.WriteAsync(
            [new KeyValuePair<string, byte[]>(VectorIndexStorageKeys.Manifest(_prefix), manifest.ToRecord())],
            cancellationToken);
    }

    /// <summary>
    /// Renders a run of chunks and writes them in batches bounded by bytes rather
    /// than by count.
    /// <para>
    /// The chunk's identity is passed as plain values rather than as a pair of
    /// key-building delegates, so a flush over several hundred partitions does
    /// not allocate a closure per partition for work that is already fully
    /// described by four integers.
    /// </para>
    /// </summary>
    private async Task WriteChunkRangeAsync(
        VectorIndexSnapshot snapshot,
        VectorIndexChunkKind kind,
        int partition,
        int first,
        int count,
        int sequenceBase,
        long generation,
        long epoch,
        CancellationToken cancellationToken)
    {
        var batch = new List<KeyValuePair<string, byte[]>>();
        var batchBytes = 0;

        for (var index = first; index < first + count; index++)
        {
            var payloadLength = snapshot.MeasureChunk(index);
            var record = new byte[VectorIndexRecord.Measure(payloadLength)];

            // Rendered straight into the record's payload region and sealed in
            // place, so a chunk is never built into a temporary and copied.
            snapshot.WriteChunk(index, record.AsSpan(VectorIndexPersistenceFormat.RecordHeaderSize));
            VectorIndexRecord.Seal(record, payloadLength);

            // The sequence is the chunk's position within its own partition, which
            // is not the position within the snapshot: an append resumes at a
            // non-zero snapshot index but must keep numbering from where the
            // committed chunks left off.
            var sequence = index - sequenceBase;
            var key = kind == VectorIndexChunkKind.Centroids
                ? VectorIndexStorageKeys.CentroidChunk(_prefix, generation, epoch, sequence)
                : VectorIndexStorageKeys.VectorChunk(_prefix, generation, partition, epoch, sequence);

            batch.Add(new KeyValuePair<string, byte[]>(key, record));
            batchBytes += record.Length;

            if (batchBytes >= WriteBatchBytes)
            {
                await _store.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
                batch.Clear();
                batchBytes = 0;
            }
        }

        if (batch.Count > 0)
        {
            await _store.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Drops the retirement journal once the removals it covers are durable. The
    /// tombstones are deleted strictly after the manifest that no longer accounts
    /// for those vectors, so the window in which a crash could lose a deletion is
    /// closed rather than merely narrow.
    /// </summary>
    private async Task SweepRetirementsAsync(CancellationToken cancellationToken)
    {
        if (_pendingRetirements.Count == 0)
        {
            return;
        }

        var keys = new List<string>(_pendingRetirements.Count);
        foreach (var key in _pendingRetirements)
        {
            keys.Add(VectorIndexStorageKeys.Retirement(_prefix, key));
        }

        await _store.DeleteAsync(keys, cancellationToken).ConfigureAwait(false);
        _pendingRetirements.Clear();
    }

    private static int CountChunks(VectorIndexSnapshot snapshot, int first, int partition)
    {
        var count = 0;
        for (var index = first; index < snapshot.ChunkCount; index++)
        {
            var descriptor = snapshot.Describe(index);
            if (descriptor.Kind != VectorIndexChunkKind.Vectors)
            {
                break;
            }

            // Identity, not sequence: a partition holding no vectors contributes
            // no chunks at all, and the run must end on the next partition's
            // first chunk rather than absorbing it.
            var slot = descriptor.PartitionId < 0 ? 0 : descriptor.PartitionId;
            if (slot != partition)
            {
                break;
            }

            count++;
        }

        return count;
    }

    private long PartitionVersionOf(int partition) =>
        _index.PartitionCount == 0 ? _index.Version : _index.PartitionVersion(partition);

    private void EnsureSlotArrays(int slots)
    {
        if (_persistedPartitionVersion.Length == slots)
        {
            return;
        }

        _persistedPartitionVersion = new long[slots];
        _persistedEpoch = new long[slots];
        _persistedChunkCount = new int[slots];
        _resident = new bool[slots];

        // Fresh slots hold no committed chunks, so every partition reads as
        // dirty and the next flush is a full one. That is exactly right after a
        // retrain, where every cell's membership changed.
        for (var partition = 0; partition < slots; partition++)
        {
            _persistedPartitionVersion[partition] = -1;
        }
    }
}
