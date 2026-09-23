using System.Buffers;
using System.IO.Hashing;

namespace Orleans.Lattice.Vector.Persistence;

public sealed partial class DurableVectorIndex
{
    // A flush accumulates chunk records up to this many bytes before issuing a
    // write, so one round trip stays bounded no matter how large a partition or
    // an ingest batch is.
    private const int WriteBatchBytes = DurableVectorIndexOptions.WriteBatchBytes;

    private bool _ingestAppendOnly = true;
    private string? _chunkBoundaryCursor;
    private string? _durableCursor;

    /// <summary>
    /// Makes the index's current contents durable.
    /// <para>
    /// Only partitions whose version stamp has moved are revisited, and within
    /// them only the chunks whose content changed are rewritten, so a flush after
    /// a handful of updates costs a handful of chunks rather than the corpus or
    /// even whole cells. Records are written first and the manifest last, so an
    /// interrupted flush leaves the previously committed index intact and
    /// loadable rather than a mixture of two.
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
        var snapshot = _index.CreateSnapshot(_options.EffectiveItemsPerChunk);
        var header = snapshot.Header;
        var slots = Math.Max(1, header.PartitionCount);
        var epoch = header.IndexVersion;
        if (generation != _generation)
        {
            // A new generation holds nothing yet, so nothing it is about to write
            // can match or supersede a stored chunk: whatever the slots describe
            // belongs to the generation being replaced, which is reclaimed whole.
            ResetSlotArrays(slots);
        }
        else
        {
            EnsureSlotArrays(slots);
        }

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

            await FlushPartitionAsync(
                snapshot,
                generation,
                partition,
                first: chunkIndex,
                chunks,
                fromSequence: 0,
                NextEpoch(partition, epoch),
                onlyChanged: !full,
                size,
                version,
                cancellationToken).ConfigureAwait(false);

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

        var snapshot = _index.CreateSnapshot(_options.EffectiveItemsPerChunk);
        var header = snapshot.Header;
        EnsureSlotArrays(1);

        var itemsPerChunk = _options.EffectiveItemsPerChunk;
        var epoch = _persistedChunkCount[0] == 0 ? header.IndexVersion : _persistedEpoch[0];
        var chunks = complete
            ? header.ChunkCount
            : _index.Count / itemsPerChunk;
        var committedCount = complete ? _index.Count : chunks * itemsPerChunk;

        // Chunks below the committed count are immutable while ingesting, so only
        // the ones at or past it are rendered and written.
        await FlushPartitionAsync(
            snapshot,
            _generation,
            partition: 0,
            first: 0,
            chunks,
            fromSequence: Math.Min(_persistedChunkCount[0], chunks),
            epoch,
            onlyChanged: false,
            committedCount,
            PartitionVersionOf(0),
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

    /// <summary>
    /// Writes one partition's chunks from <paramref name="fromSequence"/> onwards
    /// and commits the partition.
    /// <para>
    /// With <paramref name="onlyChanged"/> set, a chunk whose content hashes to
    /// what the store already holds at that position is not written again: it
    /// keeps the epoch it was stored under, and the commit record names that
    /// epoch for it. A cell is a dense array that an insert extends at the tail
    /// and a removal backfills from the tail, so one mutation disturbs at most two
    /// of its chunks. Rewriting the whole cell for it is what made the vector
    /// index the dominant write-ahead log consumer of a deployment (#3427).
    /// </para>
    /// <para>
    /// Changed chunks are written under a fresh epoch, never over a live key, and
    /// the commit record is replaced after them, so an interrupted flush still
    /// leaves the previous partition whole and loadable. Only once the record
    /// names the new chunks are the superseded ones deleted.
    /// </para>
    /// </summary>
    private async Task FlushPartitionAsync(
        VectorIndexSnapshot snapshot,
        long generation,
        int partition,
        int first,
        int chunks,
        int fromSequence,
        long epoch,
        bool onlyChanged,
        int vectorCount,
        long version,
        CancellationToken cancellationToken)
    {
        var storedEpochs = _persistedChunkEpochs[partition];
        var storedHashes = _persistedChunkHashes[partition];
        var epochs = new long[chunks];
        UInt128[]? hashes = new UInt128[chunks];

        for (var sequence = 0; sequence < fromSequence; sequence++)
        {
            epochs[sequence] = storedEpochs[sequence];
            if (storedHashes is not null && hashes is not null)
            {
                hashes[sequence] = storedHashes[sequence];
            }
            else
            {
                hashes = null;
            }
        }

        var batch = new List<KeyValuePair<string, byte[]>>();
        var batchBytes = 0;
        byte[]? buffer = null;
        try
        {
            for (var sequence = fromSequence; sequence < chunks; sequence++)
            {
                var index = first + sequence;
                var payloadLength = snapshot.MeasureChunk(index);
                var recordLength = VectorIndexRecord.Measure(payloadLength);
                if (buffer is null || buffer.Length < recordLength)
                {
                    if (buffer is not null)
                    {
                        ArrayPool<byte>.Shared.Return(buffer);
                    }

                    buffer = ArrayPool<byte>.Shared.Rent(recordLength);
                }

                var payload = buffer.AsSpan(VectorIndexPersistenceFormat.RecordHeaderSize, payloadLength);
                snapshot.WriteChunk(index, payload);
                var hash = XxHash128.HashToUInt128(payload);
                if (hashes is not null)
                {
                    hashes[sequence] = hash;
                }

                if (onlyChanged &&
                    storedHashes is not null &&
                    sequence < storedEpochs.Length &&
                    sequence < storedHashes.Length &&
                    storedHashes[sequence] == hash)
                {
                    epochs[sequence] = storedEpochs[sequence];
                    continue;
                }

                var record = buffer.AsSpan(0, recordLength).ToArray();
                VectorIndexRecord.Seal(record, payloadLength);
                epochs[sequence] = epoch;

                if (batch.Count > 0 && batchBytes + record.Length > WriteBatchBytes)
                {
                    await _store.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
                    batch = [];
                    batchBytes = 0;
                }

                batch.Add(new KeyValuePair<string, byte[]>(
                    VectorIndexStorageKeys.VectorChunk(_prefix, generation, partition, epoch, sequence), record));
                batchBytes += record.Length;
            }
        }
        finally
        {
            if (buffer is not null)
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        if (batch.Count > 0)
        {
            await _store.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
        }

        await CommitPartitionAsync(generation, partition, epoch, epochs, hashes, vectorCount, version, cancellationToken)
            .ConfigureAwait(false);
    }

    private async Task CommitPartitionAsync(
        long generation,
        int partition,
        long epoch,
        long[] epochs,
        UInt128[]? hashes,
        int vectorCount,
        long version,
        CancellationToken cancellationToken)
    {
        // The record's epoch is the newest any of its chunks lives under, which
        // is what the next flush has to move past to be sure it never writes
        // over a live key.
        var commitEpoch = epochs.Length == 0 ? epoch : epochs.Max();
        var state = new VectorIndexPartitionState(commitEpoch, epochs.Length, vectorCount, version);
        await _store.WriteAsync(
            [new KeyValuePair<string, byte[]>(
                VectorIndexStorageKeys.PartitionState(_prefix, generation, partition), state.ToRecord(epochs))],
            cancellationToken).ConfigureAwait(false);

        var stored = _persistedChunkEpochs[partition];
        _persistedEpoch[partition] = commitEpoch;
        _persistedChunkCount[partition] = epochs.Length;
        _persistedChunkEpochs[partition] = epochs;
        _persistedChunkHashes[partition] = hashes;
        _persistedPartitionVersion[partition] = version;

        await ReclaimSupersededChunksAsync(generation, partition, stored, epochs, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Deletes the stored chunks the commit record no longer names.
    /// <para>
    /// An epoch that no chunk of the partition lives under any more is swept by
    /// prefix, which also reclaims anything an interrupted flush left under it.
    /// An epoch that still holds live chunks has only its superseded keys
    /// deleted. Either way this is reclamation rather than part of the commit:
    /// the superseded keys are unreachable from the moment the record is replaced.
    /// </para>
    /// </summary>
    private async Task ReclaimSupersededChunksAsync(
        long generation, int partition, long[] stored, long[] live, CancellationToken cancellationToken)
    {
        if (stored.Length == 0)
        {
            return;
        }

        HashSet<long>? liveEpochs = null;
        Dictionary<long, List<string>>? superseded = null;
        for (var sequence = 0; sequence < stored.Length; sequence++)
        {
            var storedEpoch = stored[sequence];
            if (sequence < live.Length && live[sequence] == storedEpoch)
            {
                continue;
            }

            liveEpochs ??= [.. live];
            superseded ??= [];
            if (!superseded.TryGetValue(storedEpoch, out var keys))
            {
                keys = [];
                superseded[storedEpoch] = keys;
            }

            keys.Add(VectorIndexStorageKeys.VectorChunk(_prefix, generation, partition, storedEpoch, sequence));
        }

        if (superseded is null)
        {
            return;
        }

        List<string>? pointDeletes = null;
        foreach (var (supersededEpoch, keys) in superseded)
        {
            if (liveEpochs!.Contains(supersededEpoch))
            {
                (pointDeletes ??= []).AddRange(keys);
                continue;
            }

            await _store.DeletePrefixAsync(
                VectorIndexStorageKeys.PartitionEpochPrefix(_prefix, generation, partition, supersededEpoch),
                cancellationToken).ConfigureAwait(false);
        }

        if (pointDeletes is not null)
        {
            await _store.DeleteAsync(pointDeletes, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The epoch a partition's changed chunks are written under: the index
    /// version, as before, unless the partition already holds a chunk at or past
    /// it, in which case the next epoch after its newest. A changed chunk must
    /// never land on a key the commit record still names.
    /// </summary>
    private long NextEpoch(int partition, long indexVersion) =>
        _persistedChunkEpochs[partition].Length == 0
            ? indexVersion
            : Math.Max(indexVersion, _persistedEpoch[partition] + 1);

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

            // Flushed before the record is added rather than after, so the batch
            // that is handed to the store is within the bound rather than one
            // record past it. Adding first makes the bound a floor: the write
            // that trips it always carries the record that tripped it.
            if (batch.Count > 0 && batchBytes + record.Length > WriteBatchBytes)
            {
                await _store.WriteAsync(batch, cancellationToken).ConfigureAwait(false);
                batch.Clear();
                batchBytes = 0;
            }

            batch.Add(new KeyValuePair<string, byte[]>(key, record));
            batchBytes += record.Length;
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

        ResetSlotArrays(slots);
    }

    private void ResetSlotArrays(int slots)
    {
        _persistedPartitionVersion = new long[slots];
        _persistedEpoch = new long[slots];
        _persistedChunkCount = new int[slots];
        _persistedChunkEpochs = new long[slots][];
        _persistedChunkHashes = new UInt128[]?[slots];
        _resident = new bool[slots];

        // Fresh slots hold no committed chunks, so every partition reads as
        // dirty and the next flush is a full one. That is exactly right after a
        // retrain, where every cell's membership changed.
        for (var partition = 0; partition < slots; partition++)
        {
            _persistedPartitionVersion[partition] = -1;
            _persistedChunkEpochs[partition] = [];
        }
    }
}
