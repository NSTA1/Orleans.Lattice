using System.Globalization;

namespace Orleans.Lattice.Vector.Persistence;

public sealed partial class DurableVectorIndex
{
    /// <summary>
    /// Adopts durable state if every part of it can be verified, and discards it
    /// otherwise. There is no middle path on purpose: a partially trusted index
    /// is exactly the failure mode - a stale cell resurrecting a deleted vector -
    /// that the coherence contract exists to rule out, and because the index is a
    /// derived projection, throwing it away costs only time.
    /// </summary>
    private async Task LoadAsync(CancellationToken cancellationToken)
    {
        await _keys.LoadAsync(cancellationToken).ConfigureAwait(false);

        var manifestRecord = await _store
            .ReadAsync(VectorIndexStorageKeys.Manifest(_prefix), cancellationToken).ConfigureAwait(false);
        var buildRecord = await _store
            .ReadAsync(VectorIndexStorageKeys.BuildState(_prefix), cancellationToken).ConfigureAwait(false);

        if (manifestRecord is not null &&
            VectorIndexManifest.TryReadRecord(manifestRecord, out var manifest) &&
            await TryRestoreAsync(manifest, cancellationToken).ConfigureAwait(false))
        {
            AdoptBuildState(manifest, buildRecord);

            // The cells and the identifier mapping are one index, so the mapping
            // may never fall behind the committed cells: a key with no
            // identifier is unresolvable, which is precisely the silent
            // wrong-document failure the mapping exists to prevent.
            //
            // It may legitimately run *ahead* of them - an identifier is
            // assigned before the vector it names is durable - and it may
            // legitimately be short by the outstanding retirements, because a
            // retirement drops the mapping before the cells that still carry the
            // vector are rewritten. That window is exactly what the journal
            // covers, so the tombstones are counted into the bound rather than
            // treated as damage.
            var outstanding = await ReadRetirementsAsync(cancellationToken).ConfigureAwait(false);
            if (_keys.Count >= manifest.IndexedCount - outstanding)
            {
                ApplyRetirements();
                if (_loadMode == VectorIndexLoadMode.Full)
                {
                    await SweepRetiredMappingsAsync(cancellationToken).ConfigureAwait(false);
                }

                return;
            }
        }

        if (manifestRecord is null && buildRecord is null && _keys.Count == 0)
        {
            // A store with nothing on it. Not a fault, and nothing to sweep: the
            // index simply has not been built yet.
            ResetInMemory();
            return;
        }

        if (_loadMode == VectorIndexLoadMode.Lazy)
        {
            // A lazily loaded index is a reader, and a reader must not repair
            // what it cannot maintain: discarding here would delete an index a
            // writer elsewhere may be part-way through building, and would do it
            // from a handle whose whole contract is that it does not write. It
            // refuses to serve the unverifiable state instead, and leaves the
            // repair to the writer that owns the index.
            ResetInMemory();
            return;
        }

        await DiscardAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<bool> TryRestoreAsync(VectorIndexManifest manifest, CancellationToken cancellationToken)
    {
        VectorIndex restored;
        try
        {
            restored = VectorIndex.Restore(manifest.Header, _options.Index);
        }
        catch (VectorIndexFormatException)
        {
            // A snapshot this build cannot read - a future format version, or a
            // shape that contradicts the configured dimensionality or metric.
            return false;
        }

        var partitionSlots = Math.Max(1, manifest.Header.PartitionCount);
        var epochs = new long[partitionSlots];
        var chunkCounts = new int[partitionSlots];
        var resident = new bool[partitionSlots];

        try
        {
            if (manifest.Header.PartitionCount > 0 &&
                !await ApplyCentroidsAsync(restored, manifest, cancellationToken).ConfigureAwait(false))
            {
                return false;
            }

            if (!await ReadPartitionStatesAsync(
                    manifest, partitionSlots, epochs, chunkCounts, cancellationToken).ConfigureAwait(false))
            {
                return false;
            }

            if (_loadMode == VectorIndexLoadMode.Full)
            {
                for (var partition = 0; partition < partitionSlots; partition++)
                {
                    await ApplyPartitionAsync(
                        restored,
                        manifest.Generation,
                        partition,
                        epochs[partition],
                        chunkCounts[partition],
                        cancellationToken).ConfigureAwait(false);

                    resident[partition] = true;
                }

                if (restored.Count != manifest.IndexedCount)
                {
                    return false;
                }
            }
        }
        catch (VectorIndexFormatException)
        {
            // A chunk that is truncated, mis-framed, or names a partition this
            // shape does not have. Same answer as an unreadable manifest.
            return false;
        }

        _index = restored;
        _generation = manifest.Generation;
        _centroidEpoch = manifest.CentroidEpoch;
        _centroidsPersisted = manifest.Header.PartitionCount > 0;
        _persistedEpoch = epochs;
        _persistedChunkCount = chunkCounts;
        _resident = resident;
        _persistedPartitions = partitionSlots;
        _restored = true;

        // Every partition's durable form matches what is now in memory, so the
        // next flush writes only what a subsequent mutation dirties.
        _persistedPartitionVersion = new long[partitionSlots];
        CaptureCleanPartitionVersions();
        return true;
    }

    private async Task<bool> ApplyCentroidsAsync(
        VectorIndex restored, VectorIndexManifest manifest, CancellationToken cancellationToken)
    {
        var applied = 0;
        var prefix = VectorIndexStorageKeys.CentroidPrefix(_prefix, manifest.Generation, manifest.CentroidEpoch);
        var scan = _store.ScanAsync(prefix, cancellationToken);
        await foreach (var entry in scan.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (!VectorIndexRecord.TryUnwrap(entry.Value, out var payload))
            {
                return false;
            }

            restored.ApplyChunk(payload);
            applied++;
        }

        // A partitioning that is not completely restored is worse than none: the
        // index would rank against a partly zeroed centroid block. S6 refuses to
        // report Ready in that state, and this refuses to serve it at all.
        return applied == manifest.Header.CentroidChunkCount && restored.CentroidsComplete;
    }

    private async Task<bool> ReadPartitionStatesAsync(
        VectorIndexManifest manifest,
        int partitionSlots,
        long[] epochs,
        int[] chunkCounts,
        CancellationToken cancellationToken)
    {
        var seen = new bool[partitionSlots];
        var statePrefix = VectorIndexStorageKeys.PartitionStatePrefix(_prefix, manifest.Generation);
        var scan = _store.ScanAsync(statePrefix, cancellationToken);
        await foreach (var entry in scan.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (!int.TryParse(
                    entry.Key.AsSpan(statePrefix.Length),
                    NumberStyles.None,
                    CultureInfo.InvariantCulture,
                    out var partition) ||
                partition < 0 || partition >= partitionSlots)
            {
                return false;
            }

            if (!VectorIndexPartitionState.TryReadRecord(entry.Value, out var state))
            {
                return false;
            }

            epochs[partition] = state.Epoch;
            chunkCounts[partition] = state.ChunkCount;
            seen[partition] = true;
        }

        for (var partition = 0; partition < partitionSlots; partition++)
        {
            if (!seen[partition])
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Applies exactly the chunks a partition's commit record claims, addressed
    /// by key rather than discovered by scanning.
    /// <para>
    /// Reading exactly the committed count is what makes an interrupted append
    /// safe: a chunk written but not yet committed is simply not read, so a crash
    /// between writing content and committing it leaves the partition at its
    /// previous, self-consistent extent instead of pulling in vectors the
    /// manifest does not account for.
    /// </para>
    /// </summary>
    private async Task ApplyPartitionAsync(
        VectorIndex target,
        long generation,
        int partition,
        long epoch,
        int chunkCount,
        CancellationToken cancellationToken)
    {
        const int ReadBatch = 16;
        var keys = new List<string>(Math.Min(ReadBatch, Math.Max(chunkCount, 1)));
        for (var sequence = 0; sequence < chunkCount; sequence += ReadBatch)
        {
            keys.Clear();
            var upper = Math.Min(sequence + ReadBatch, chunkCount);
            for (var i = sequence; i < upper; i++)
            {
                keys.Add(VectorIndexStorageKeys.VectorChunk(_prefix, generation, partition, epoch, i));
            }

            var records = await _store.ReadManyAsync(keys, cancellationToken).ConfigureAwait(false);
            foreach (var key in keys)
            {
                if (!records.TryGetValue(key, out var record) ||
                    !VectorIndexRecord.TryUnwrap(record, out var payload))
                {
                    throw new VectorIndexFormatException(
                        $"The vector chunk at '{key}' is missing, truncated, corrupt, or written by an unsupported build.");
                }

                target.ApplyChunk(payload);
            }
        }
    }

    /// <summary>
    /// Reads the retirement journal into memory and reports how many entries it
    /// holds, without applying any of them yet. The count is needed before the
    /// coherence check, because an outstanding retirement is exactly the amount
    /// by which the mapping is allowed to be short of the committed cells.
    /// </summary>
    private async Task<int> ReadRetirementsAsync(CancellationToken cancellationToken)
    {
        var scan = _store.ScanAsync(VectorIndexStorageKeys.RetirementPrefix(_prefix), cancellationToken);
        await foreach (var entry in scan.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (VectorIndexStorageKeys.TryReadRetirementKey(_prefix, entry.Key, out var key))
            {
                _pendingRetirements.Add(key);
            }
        }

        return _pendingRetirements.Count;
    }

    /// <summary>
    /// Replays the retirement journal. A tombstone survives exactly the window
    /// between a removal being requested and that removal being durable, so
    /// replaying it completes a deletion that a crash interrupted rather than
    /// letting the vector reappear.
    /// </summary>
    private void ApplyRetirements()
    {
        foreach (var key in _pendingRetirements)
        {
            _index.Remove(key);
        }
    }

    /// <summary>
    /// Drops the identifier mapping of anything the journal retired. A crash
    /// between the removal and the mapping delete would otherwise leave an
    /// identifier pointing at a key the index no longer holds. Only a fully
    /// resident index does this, because it is a write and a lazily loaded index
    /// does not make any.
    /// </summary>
    private async Task SweepRetiredMappingsAsync(CancellationToken cancellationToken)
    {
        foreach (var key in _pendingRetirements)
        {
            if (_keys.TryGetId(key, out var id))
            {
                await _keys.RemoveAsync(id, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private void AdoptBuildState(VectorIndexManifest manifest, byte[]? buildRecord)
    {
        if (buildRecord is not null &&
            VectorIndexBuildState.TryReadRecord(buildRecord, out var build) &&
            build.Generation == manifest.Generation)
        {
            // Persisting is normalised back to Training: the manifest that was
            // loaded is the pre-training one, so the partitioning it was about to
            // write does not exist. Training is deterministic and far cheaper
            // than re-reading the source, so redoing it is the cheap, correct
            // resume.
            _phase = build.Phase == VectorIndexBuildPhase.Persisting
                ? VectorIndexBuildPhase.Training
                : build.Phase;
            _cursor = build.Cursor;
            _durableCursor = build.Cursor;
            _chunkBoundaryCursor = build.Cursor;
            _expected = build.Expected;
            return;
        }

        _phase = VectorIndexBuildPhase.Ready;
        _cursor = null;
        _durableCursor = null;
        _chunkBoundaryCursor = null;
        _expected = _index.Count;
    }

    /// <summary>
    /// Throws away every durable trace of the index and starts again. Safe by
    /// construction: everything under the index's prefix is derived, so the worst
    /// case is the cost of recomputing it.
    /// </summary>
    private async Task DiscardAsync(CancellationToken cancellationToken)
    {
        await _store.DeleteAsync(
            [VectorIndexStorageKeys.Manifest(_prefix), VectorIndexStorageKeys.BuildState(_prefix)],
            cancellationToken).ConfigureAwait(false);
        await _store.DeletePrefixAsync(
            VectorIndexStorageKeys.AllGenerationsPrefix(_prefix), cancellationToken).ConfigureAwait(false);
        await _store.DeletePrefixAsync(
            VectorIndexStorageKeys.RetirementPrefix(_prefix), cancellationToken).ConfigureAwait(false);

        // The key mapping goes too. It is meaningful only against the chunks that
        // were just deleted, and clearing it removes the whole class of stale
        // mappings that would otherwise outlive a rebuild. The identifier counter
        // deliberately does not rewind, so no key is ever handed out twice.
        await _keys.ClearAsync(cancellationToken).ConfigureAwait(false);
        ResetInMemory();
    }

    private void ResetInMemory()
    {
        _index = new VectorIndex(_options.Index);
        _persistedPartitionVersion = [];
        _persistedEpoch = [];
        _persistedChunkCount = [];
        _resident = [];
        _persistedPartitions = 0;
        _generation = 0;
        _centroidEpoch = 0;
        _centroidsPersisted = false;
        _phase = VectorIndexBuildPhase.NotStarted;
        _cursor = null;
        _expected = 0;
        _restored = false;
        _pendingRetirements.Clear();
        _updatesSinceTraining = 0;
        _ingestAppendOnly = true;
        _chunkBoundaryCursor = null;
        _durableCursor = null;
    }

    private void CaptureCleanPartitionVersions()
    {
        if (_index.PartitionCount == 0)
        {
            _persistedPartitionVersion[0] = _index.Version;
            return;
        }

        for (var partition = 0; partition < _index.PartitionCount; partition++)
        {
            _persistedPartitionVersion[partition] = _index.PartitionVersion(partition);
        }
    }
}
