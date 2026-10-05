namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The kept-snapshot coverage record (issue #4634): a one-way flag, per WAL
/// partition, in the leaf's own state row
/// (<see cref="State.LeafNodeState.SnapshotCoveredPartitions"/>), that its
/// snapshot store once kept a snapshot covering that partition. It lets a cold
/// start tell a snapshot that vanished from one that never existed.
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
/// A partition's flag is set in memory wherever durable coverage is recorded,
/// and the durable pin flush persists it before it resolves a single pin, so the
/// flag is durable no later than any pin that licenses a trim behind the
/// snapshot. The flag lives in the same row as the projection checkpoint the
/// cold-start check keys on, so no storage loss can keep one and drop the
/// other. A flag only ever turns on, so the extra write happens at most once per
/// partition per leaf lifetime.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    private long _keptCoverageRaises;
    private long _keptCoverageDurableRaises;

    /// <summary>
    /// Sets the kept-snapshot flag of every partition <paramref name="covered"/>
    /// reaches, for the next durable pin flush to make durable.
    /// </summary>
    private void RaiseKeptSnapshotCoverageMarker(long[] covered)
    {
        var flags = state.State.SnapshotCoveredPartitions;
        for (var partition = 0; partition < covered.Length; partition++)
        {
            if (covered[partition] < 0 || (flags is not null && partition < flags.Length && flags[partition]))
                continue;

            if (flags is null || flags.Length <= partition)
            {
                var grown = new bool[Math.Max(covered.Length, partition + 1)];
                flags?.CopyTo(grown, 0);
                flags = grown;
            }

            flags[partition] = true;
            state.State.SnapshotCoveredPartitions = flags;
            _keptCoverageRaises++;
        }
    }

    /// <summary>
    /// Persists a newly set kept-snapshot flag before the durable pin flush
    /// resolves any pin, so a pin that licenses a trim behind a snapshot is never
    /// published ahead of the durable record that the snapshot existed. A failed
    /// write fails the flush; the flag stays set and the next flush retries it.
    /// </summary>
    private async Task PersistKeptSnapshotCoverageMarkerAsync()
    {
        var raised = _keptCoverageRaises;
        if (raised <= _keptCoverageDurableRaises)
            return;

        await PersistAsync();
        if (raised > _keptCoverageDurableRaises)
            _keptCoverageDurableRaises = raised;
    }

    /// <summary>
    /// For a leaf whose snapshot store holds no snapshot: the fault to fail the
    /// cold start with when a snapshot covering any partition was ever kept, or
    /// <see langword="null"/> when none was.
    /// <para>
    /// The WAL tail is deliberately not consulted. A kept snapshot's durable pin
    /// cannot be lowered once the snapshot is gone, so the WAL GC stays entitled
    /// to trim the prefix the snapshot covered for as long as a cold rebuild runs:
    /// an untrimmed tail at activation proves nothing about the slices the rebuild
    /// has yet to read. This is the rule issue #4450 applies to a snapshot that
    /// fails to load, found here by the WAL specification's <c>SnapshotLoss</c>
    /// variant (a trim between the tail check and the rebuild's first read).
    /// </para>
    /// </summary>
    private Exception? DetectLostKeptSnapshot()
    {
        if (state.State.SnapshotCoveredPartitions is not { } flags)
            return null;

        for (var partition = 0; partition < flags.Length; partition++)
        {
            if (flags[partition])
            {
                return new InvalidOperationException(
                    $"Leaf {context.GrainId} of tree '{state.State.TreeId}' has no snapshot, but its store once kept "
                    + $"one covering WAL partition {partition}. The snapshot may have been the only durable copy of the "
                    + "prefix it covered, and the WAL GC stays entitled to trim that prefix under it, so a cold rebuild "
                    + "from the WAL could lose it (issue #4634). Restore the snapshot, or accept the loss with a "
                    + "projection rebuild.");
            }
        }

        return null;
    }
    /// <summary>
    /// Drops the kept-snapshot record, for an operator rebuild that has accepted
    /// the loss of what the snapshot held. The rebuild's own state write makes
    /// it durable.
    /// </summary>
    private void ForgetKeptSnapshotCoverage()
    {
        state.State.SnapshotCoveredPartitions = null;
        _keptCoverageDurableRaises = _keptCoverageRaises;
    }
}
