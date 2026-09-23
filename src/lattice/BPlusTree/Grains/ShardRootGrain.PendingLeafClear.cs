using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Durable bookkeeping for the grain-state clear owed to a leaf that has been
/// taken out of the tree (issue #2207). See
/// <see cref="State.ShardRootState.PendingLeafClears"/> for why the record has to
/// be durable.
/// </summary>
internal sealed partial class ShardRootGrain
{
    /// <summary>
    /// Upper bound on the owed clears one pass re-attempts. Each is a grain call
    /// that activates a leaf and clears a storage row while the pass holds the
    /// shard root's turn, and the list only grows while clears keep failing - so
    /// a storage outage that fails every clear must not turn every later pass
    /// into an unbounded sweep. Entries are retried oldest first, so the cap
    /// delays a clear and never starves one.
    /// </summary>
    internal const int MaxPendingLeafClearRetriesPerPass = 64;

    /// <summary>
    /// Set when <see cref="State.ShardRootState.PendingLeafClears"/> has changed
    /// in memory and the matching storage write is still owed. Retiring an entry
    /// is not persisted on its own: the next record carries it, and
    /// <see cref="FlushPendingLeafClearsAsync"/> writes whatever is left at the
    /// end of the pass. A retirement lost to a crash costs one idempotent
    /// re-clear of a leaf whose state is already gone, not a lost clear.
    /// </summary>
    private bool _pendingLeafClearsNeedPersist;

    /// <summary>
    /// Clears the grain state of a leaf that is already out of the tree, having
    /// first recorded durably that the clear is owed.
    /// <para>
    /// Call only once the leaf is unreachable: the predecessor's
    /// compare-and-swap has committed and routing to the leaf has been retired
    /// (or never existed). From that point no walk can rediscover the leaf, so
    /// the record is the only thing that can bring a failed clear back round.
    /// </para>
    /// <para>
    /// Neither failure is surfaced. The removal has already committed, so
    /// reporting it as failed would have the caller treat an unlinked leaf as
    /// still present. A failed clear leaves the entry in place for
    /// <see cref="RetryPendingLeafClearsAsync"/>; a failed record write leaves
    /// the entry in memory, where the end-of-pass flush attempts it again.
    /// </para>
    /// </summary>
    private async Task ClearRemovedLeafAsync(GrainId leafId, string removal)
    {
        if (!state.State.PendingLeafClears.Contains(leafId))
        {
            state.State.PendingLeafClears.Add(leafId);
        }

        try
        {
            await WriteShardStateAsync();
            _pendingLeafClearsNeedPersist = false;
        }
        catch (Exception ex)
        {
            _pendingLeafClearsNeedPersist = true;
            logger.LogWarning(
                ex,
                "Shard {ShardIndex} of tree '{TreeId}' could not record that {Removal} leaf {LeafId} still owes a state clear; the clear is attempted anyway, and the record is re-attempted at the end of the pass.",
                MyShardIndex,
                TreeId,
                removal,
                leafId);
        }

        await TryClearPendingLeafAsync(leafId, removal);
    }

    /// <summary>
    /// Re-attempts the clears owed to leaves an earlier pass took out of the
    /// tree but could not clear, oldest first and at most
    /// <see cref="MaxPendingLeafClearRetriesPerPass"/> of them.
    /// </summary>
    private async Task RetryPendingLeafClearsAsync()
    {
        var owed = state.State.PendingLeafClears;
        if (owed.Count == 0)
        {
            return;
        }

        // Snapshotted because a concurrent interleaved pass on this activation
        // can append to the list while this one awaits a clear.
        var batch = new GrainId[Math.Min(owed.Count, MaxPendingLeafClearRetriesPerPass)];
        owed.CopyTo(0, batch, 0, batch.Length);

        foreach (var leafId in batch)
        {
            await TryClearPendingLeafAsync(leafId, "a previously removed");
        }

        await FlushPendingLeafClearsAsync();
    }

    /// <summary>
    /// Clears every leaf still owed a clear, as part of purging the shard.
    /// <para>
    /// These leaves are on neither the sibling chain nor any routing table, so
    /// the purge's own walks cannot reach them and would otherwise leave their
    /// state behind once the shard's own record, the only thing that names them,
    /// is cleared. A failure here propagates, as every other purge failure does,
    /// so the tree-deletion retry re-runs the purge with the record intact.
    /// </para>
    /// </summary>
    private async Task ClearPendingLeavesForPurgeAsync()
    {
        var owed = state.State.PendingLeafClears;
        if (owed.Count == 0)
        {
            return;
        }

        foreach (var leafId in owed.ToArray())
        {
            await grainFactory.GetGrain<IBPlusLeafGrain>(leafId).ClearGrainStateAsync();
            _leafGrains.TryRemove(leafId, out _);
        }
    }

    /// <summary>
    /// Persists a change to the owed-clear record that has not been written yet.
    /// A no-op when nothing changed, so a pass with nothing owed pays nothing.
    /// Best-effort: a failure leaves the in-memory record correct for this
    /// activation and is retried by the next pass.
    /// </summary>
    private async Task FlushPendingLeafClearsAsync()
    {
        if (!_pendingLeafClearsNeedPersist)
        {
            return;
        }

        try
        {
            await WriteShardStateAsync();
            _pendingLeafClearsNeedPersist = false;
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Shard {ShardIndex} of tree '{TreeId}' could not persist its record of {PendingCount} leaves still owed a state clear; the record is correct for this activation and the next pass re-attempts the write.",
                MyShardIndex,
                TreeId,
                state.State.PendingLeafClears.Count);
        }
    }

    private async Task TryClearPendingLeafAsync(GrainId leafId, string removal)
    {
        try
        {
            await ResolveLeafGrain(leafId).ClearGrainStateAsync();
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Shard {ShardIndex} of tree '{TreeId}' could not clear the state of {Removal} leaf {LeafId}; it is out of the tree, so its storage row, replay barrier and WAL materialiser pin survive until the clear lands. It stays recorded as owed, and the next empty-leaf reclaim pass or orphan repair pass retries it.",
                MyShardIndex,
                TreeId,
                removal,
                leafId);

            return;
        }

        _leafGrains.TryRemove(leafId, out _);

        if (state.State.PendingLeafClears.Remove(leafId))
        {
            _pendingLeafClearsNeedPersist = true;
        }
    }
}
