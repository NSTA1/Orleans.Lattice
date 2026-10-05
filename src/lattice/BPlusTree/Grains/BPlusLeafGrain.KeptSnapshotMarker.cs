namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The kept-snapshot coverage marker (issue #4634): a durable record, in its own
/// sidecar row (<see cref="ILeafSnapshotCoverageMarkerGrain"/>), that this leaf's
/// snapshot store once kept a snapshot covering a WAL prefix, so a later cold
/// start can tell a snapshot that vanished from one that never existed.
/// <para>
/// Under coverage-gated trim the WAL GC removes a checkpointed prefix precisely
/// because a snapshot covers it. If that snapshot later disappears - lost
/// storage, an operator delete, a partial clear - the leaf's next cold start
/// finds no snapshot. A failed load already fails closed (issue #4450), but an
/// absent one used to be read as "never had one": the cold rebuild replayed the
/// surviving WAL suffix into an empty cache, the fall-off guard (which compares
/// the tail against the persisted checkpoint, a bound the trim legitimately
/// stays within) passed, and every key whose only durable copy was the snapshot
/// read as absent, with nothing logged.
/// </para>
/// <para>
/// A raise is collected in memory wherever durable coverage is recorded and
/// written to the sidecar by the durable pin flush before it resolves a single
/// pin, so the record is durable no later than any pin that licenses a trim
/// behind the snapshot. It never rides the leaf's own state writes.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    private long[]? _pendingKeptCoverageRaise;
    private long[]? _keptCoverageRaisedTo;

    /// <summary>The marker sidecar, or <see langword="null"/> for a leaf without a Guid key.</summary>
    private ILeafSnapshotCoverageMarkerGrain? KeptSnapshotCoverageMarker =>
        context.GrainId.TryGetGuidKey(out var leafKey, out _)
            ? grainFactory.GetGrain<ILeafSnapshotCoverageMarkerGrain>(leafKey)
            : null;

    /// <summary>
    /// Collects a raise of the marker to <paramref name="covered"/>, per
    /// partition, for the next durable pin flush to make durable.
    /// </summary>
    private void RaiseKeptSnapshotCoverageMarker(long[] covered)
    {
        var raised = LeafSnapshotCoverageMarkerGrain.Raise(_keptCoverageRaisedTo, covered);
        if (raised is null)
            return;

        _pendingKeptCoverageRaise = LeafSnapshotCoverageMarkerGrain.Raise(_pendingKeptCoverageRaise, raised)
            ?? _pendingKeptCoverageRaise;
    }

    /// <summary>
    /// Makes a collected raise durable before the durable pin flush resolves any
    /// pin, so a pin that licenses a trim behind a snapshot is never published
    /// ahead of the durable record that the snapshot existed. A failure fails
    /// the flush and keeps the raise for the next one.
    /// </summary>
    private async Task PersistKeptSnapshotCoverageMarkerAsync()
    {
        if (_pendingKeptCoverageRaise is not { } pending)
            return;

        if (KeptSnapshotCoverageMarker is not { } marker)
        {
            _pendingKeptCoverageRaise = null;
            return;
        }

        await marker.RaiseAsync(pending);
        _keptCoverageRaisedTo = LeafSnapshotCoverageMarkerGrain.Raise(_keptCoverageRaisedTo, pending) ?? _keptCoverageRaisedTo;
        if (ReferenceEquals(_pendingKeptCoverageRaise, pending))
            _pendingKeptCoverageRaise = null;
    }

    /// <summary>
    /// For a leaf whose snapshot store holds no snapshot: the fault to fail the
    /// cold start with when the marker shows a kept snapshot covered a partition
    /// whose WAL has since been trimmed (its tail is past offset 0), or
    /// <see langword="null"/> when no snapshot was ever kept or the WAL still holds
    /// everything a cold rebuild needs. Only a leaf that has persisted a
    /// projection checkpoint can have had its prefix covered, so a leaf without
    /// one asks nothing. A marker or tail that cannot be read fails closed.
    /// </summary>
    private async Task<Exception?> DetectLostKeptSnapshotAsync(CancellationToken cancellationToken)
    {
        var treeId = state.State.TreeId;
        if (string.IsNullOrEmpty(treeId) || !HasPersistedProjectionCheckpoint() || KeptSnapshotCoverageMarker is not { } sidecar)
            return null;

        long[]? marker;
        try
        {
            marker = await sidecar.GetAsync();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return new InvalidOperationException(
                $"Leaf {context.GrainId} of tree '{treeId}' has no snapshot, and its record of whether one was ever "
                + "kept could not be read, so it cannot tell a lost snapshot from one that never existed (issue #4634).",
                ex);
        }

        if (marker is null)
            return null;

        for (var partition = 0; partition < marker.Length; partition++)
        {
            if (marker[partition] < 0)
                continue;

            long tail;
            try
            {
                tail = await grainFactory.GetGrain<ILeafReplayCoordinatorGrain>($"{treeId}/{partition}")
                    .GetTailOffsetAsync(cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return new InvalidOperationException(
                    $"Leaf {context.GrainId} of tree '{treeId}' has no snapshot, but its store once kept one covering "
                    + $"WAL partition {partition} through offset {marker[partition]}, and the partition's tail could "
                    + "not be read to tell whether the prefix it covered survives (issue #4634).",
                    ex);
            }

            if (tail > 0)
            {
                return new InvalidOperationException(
                    $"Leaf {context.GrainId} of tree '{treeId}' has no snapshot, but its store once kept one covering "
                    + $"WAL partition {partition} through offset {marker[partition]}, and the WAL has been trimmed to "
                    + $"offset {tail} under it. The snapshot was the only durable copy of that prefix, so a cold "
                    + "rebuild from the surviving WAL would lose it (issue #4634). Restore the snapshot, or accept the "
                    + "loss with a projection rebuild.");
            }
        }

        return null;
    }

    /// <summary>Whether any partition of this leaf has a persisted projection checkpoint.</summary>
    private bool HasPersistedProjectionCheckpoint()
    {
        if (state.State.ProjectionCheckpointOffsetsByPartition is { } byPartition)
        {
            foreach (var offset in byPartition)
            {
                if (offset >= 0)
                    return true;
            }
        }

        return state.State.ProjectionCheckpointOffset > 0
            || (state.State.ProjectionCheckpointOffset == 0 && state.State.ProjectionCheckpointOffsetAssigned == true);
    }

    /// <summary>
    /// Deletes the marker: the leaf is being removed, or an operator rebuild has
    /// accepted the loss of what its snapshot held.
    /// </summary>
    private async Task ClearKeptSnapshotCoverageMarkerAsync()
    {
        _pendingKeptCoverageRaise = null;
        _keptCoverageRaisedTo = null;
        if (KeptSnapshotCoverageMarker is { } marker)
            await marker.ClearAsync();
    }
}
