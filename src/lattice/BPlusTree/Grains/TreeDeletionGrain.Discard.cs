using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Discarding a derived physical copy - the destination of an undone resize
/// (issue #3930).
/// </summary>
/// <remarks>
/// <para>
/// An undone resize used to retire its destination exactly as a completed resize
/// retires the tree it replaced: a soft delete, then a purge once
/// <see cref="LatticeOptions.SoftDeleteDuration"/> has elapsed. The shards were
/// marked deleted, so the copy's data was unreachable at once, but everything
/// holding its write-ahead log was left in place for the whole window. Its leaves
/// were written by the drain and never checkpointed, so their durable
/// materialiser pins carried no usable offset; the WAL GC could not advance the
/// copy's cursor floor past them, retained its log in full, and kept reactivating
/// its leaves to try to heal a floor that no activation could ever lift.
/// </para>
/// <para>
/// Unlike a retired copy, a discarded one is never recovered - an undo is not
/// itself undoable, and <c>docs/lattice/consistency.md</c> already documents that
/// an undo after the swap discards every write the copy accepted. Nothing its
/// pins protect will be read again, so the discard releases them and trims the
/// log immediately. The shard marks and the deferred purge are unchanged: a
/// router that cached an alias to the copy must keep being refused, and an
/// immediate purge would clear the marks and let it read an empty tree instead.
/// </para>
/// </remarks>
internal sealed partial class TreeDeletionGrain
{
    /// <inheritdoc />
    public async Task DiscardDerivedPhysicalTreeAsync()
    {
        EnsureLifecycleOrigin();

        // Recorded before the shard marks so a retry after any later failure is
        // still a discard, and so the copy can never be recovered once its WAL
        // has been released. The record is inert until the marks land:
        // GetPhysicalRetentionAsync reports a copy that is not yet deleted as
        // live whatever this flag says.
        if (!state.State.Discarded || !state.State.SuppressLifecycleEvents)
        {
            var discarded = state.State.Discarded;
            var suppressed = state.State.SuppressLifecycleEvents;
            state.State.Discarded = true;
            state.State.SuppressLifecycleEvents = true;
            try
            {
                await state.WriteStateAsync();
            }
            catch
            {
                state.State.Discarded = discarded;
                state.State.SuppressLifecycleEvents = suppressed;
                throw;
            }
        }

        await SoftDeleteAsync(retainsRegistryEntry: false);

        // Released only after the shard marks, which is what stops any further
        // append from landing in the log this trims.
        await DeregisterLeafCursorsAsync();
        await TrimDiscardedWalAsync();
    }

    /// <inheritdoc />
    public async Task<bool> DiscardIfAbandonedDerivedCopyAsync()
    {
        EnsureLifecycleOrigin();

        var s = state.State;
        if (s.Discarded && s.IsDeleted)
            return true;

        // Only a copy a resize retired through DeleteDerivedPhysicalTreeAsync:
        // deleted silently, not a first resize's retired copy (which shares the
        // live logical id), not a delegated deletion (which a logical recover
        // reverses), and not yet purged. A caller's own DeleteTreeAsync never
        // suppresses lifecycle events, so a tree a user may still recover is
        // never taken for one.
        if (!s.IsDeleted || s.PurgeComplete || !s.SuppressLifecycleEvents || s.RetainsRegistryEntry || s.Delegated)
            return false;

        if (ResizeLogicalTreeId(TreeId) is not { } logicalTreeId)
            return false;

        // An undo recovers only the copy its coordinator still names, so a copy
        // it does not name can never be read again.
        if (await grainFactory.GetGrain<ITreeResizeGrain>(logicalTreeId).ReferencesPhysicalTreeAsync(TreeId))
            return false;

        logger.LogInformation(
            "Discarding physical tree {TreeId}: a resize retired it and no resize can recover it any longer, so its write-ahead-log retention is released.",
            TreeId);
        await DiscardDerivedPhysicalTreeAsync();
        return true;
    }

    /// <summary>
    /// The logical tree a resize's derived copy belongs to, parsed from the
    /// <c>{logicalTreeId}/resized/{operationId}</c> id the resize coordinator
    /// composes, or <see langword="null"/> for any other id.
    /// </summary>
    internal static string? ResizeLogicalTreeId(string physicalTreeId)
    {
        const string marker = "/resized/";
        var index = physicalTreeId.LastIndexOf(marker, StringComparison.Ordinal);
        if (index <= 0)
            return null;

        var operationId = physicalTreeId.AsSpan(index + marker.Length);
        if (operationId.IsEmpty || operationId.Contains('/'))
            return null;

        return physicalTreeId[..index];
    }

    /// <inheritdoc />
    public Task<PhysicalTreeRetention> GetPhysicalRetentionAsync()
    {
        var s = state.State;

        // A completed purge leaves the deletion record behind, and a later
        // write under the same id registers a new, live tree (see
        // docs/lattice/tree-deletion.md). The record describes the copy that
        // was purged, not that tree, so it must not be reported against it.
        if (s.PurgeComplete || !(s.IsDeleted || s.Delegated))
            return Task.FromResult(PhysicalTreeRetention.Live);

        return Task.FromResult(s.Discarded ? PhysicalTreeRetention.Discarded : PhysicalTreeRetention.Deleted);
    }

    /// <summary>
    /// Trims every partition of a discarded copy's write-ahead log through its
    /// head. Its leaf materialiser pins are gone and its shards reject every
    /// write, so nothing will ever replay or append to the log again, and with no
    /// consumer left the WAL GC reports no cursor and trims nothing on its own.
    /// Best-effort per partition: a failure is logged and leaves that partition's
    /// log retained, exactly as it was before the discard, and the purge that
    /// follows trims again.
    /// </summary>
    private async Task TrimDiscardedWalAsync()
    {
        int partitions;
        try
        {
            partitions = await optionsResolver.GetWalPartitionsAsync(TreeId);
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Could not resolve the WAL partition count of discarded tree {TreeId}; its write-ahead log is left retained until its purge trims it.",
                TreeId);
            return;
        }

        for (var partition = 0; partition < partitions; partition++)
        {
            try
            {
                var (provider, _, _) = await optionsResolver.GetWalProviderAsync(TreeId, partition);
                var head = await provider.GetHighestOffsetAsync(TreeId, partition, CancellationToken.None);
                if (head < 0)
                    continue;

                await provider.TrimAsync(TreeId, partition, head, CancellationToken.None);
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "Could not trim WAL partition {Partition} of discarded tree {TreeId}; it stays retained until its purge trims it.",
                    partition,
                    TreeId);
            }
        }
    }
}
