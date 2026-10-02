using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Reusing a purged tree id (issue #3940).
/// </summary>
/// <remarks>
/// <para>
/// A completed purge unregisters the tree but leaves this grain's deletion record
/// behind, and the next read or write under the same id registers a new tree with
/// fresh shards. The record used to go on describing that new tree: it read as
/// deleted and purged, so <c>DeleteTreeAsync</c> was a silent no-op, recovery and
/// purge were refused as for a purged tree, and every alias change - a resize
/// among them - was refused because the tree was "deleted". Nothing could clear
/// it, so the id was wedged for good while the tree itself served traffic.
/// </para>
/// <para>
/// A purge is terminal, so once the id is registered again the record has no job
/// left. Reads (<see cref="IsDeletedAsync"/>, <see cref="GetDeletionStatusAsync"/>,
/// <see cref="EnsureAliasWritableAsync"/>) report such an id live without writing,
/// and every lifecycle verb clears the record durably before it acts, so an estate
/// already holding one heals on its next lifecycle call. Only a record whose purge
/// completed is ever treated this way: a soft-deleted tree keeps its registry
/// entry throughout its window, so the registry alone never distinguishes it, and
/// a tree whose purge is still running or still finalising is never reported live.
/// </para>
/// </remarks>
internal sealed partial class TreeDeletionGrain
{
    /// <summary>
    /// Whether this grain holds the terminal record of a completed purge and
    /// nothing else: no deletion, purge, alias reservation, delegated work, or
    /// owed registry removal that is still in flight. A purge whose registry
    /// removal is still owed (persisted with its completion, so it survives a
    /// reactivation - issue #4265) holds the purged tree's own entry, which with
    /// the record would otherwise look like a reused id. An in-memory predicate,
    /// so reads can test it before paying for a registry round trip.
    /// </summary>
    private bool HoldsTerminalPurgeRecord
    {
        get
        {
            var s = state.State;
            if (s.Delegated || s.Discarded || s.DeletePending || s.AliasOperationId is not null
                || s.RegistryUnregisterPending)
                return false;

            // The physical side must be either untouched or a finished purge -
            // never a soft delete or a purge still running.
            var physicalSettled = !s.IsDeleted || (s.PurgeComplete && !s.PurgeInProgress);
            if (s.LogicalPhysicalTreeId is not null)
                return s.LogicalPurgeComplete && !s.LogicalPurgeInProgress && physicalSettled;

            return s.IsDeleted && s.PurgeComplete && !s.PurgeInProgress && !s.RetainsRegistryEntry;
        }
    }

    /// <inheritdoc />
    public Task<bool> HoldsCompletedPurgeAsync()
    {
        // From the persisted snapshot, so an interleaved call never reads a
        // completion a failed write could still roll back. A retired copy whose
        // id is also a live logical tree keeps its row and is not a purged id.
        var d = Durable;
        return Task.FromResult(
            d.LogicalPurgeComplete || (d.IsDeleted && d.PurgeComplete && !d.RetainsRegistryEntry));
    }

    /// <summary>
    /// Whether the id was registered again after its purge completed, so the
    /// deletion record describes a tree that no longer exists. Pure read.
    /// </summary>
    private async ValueTask<bool> IsReusedAfterPurgeAsync()
    {
        if (!HoldsTerminalPurgeRecord)
            return false;
        if (TreeId.StartsWith(LatticeConstants.SystemTreePrefix, StringComparison.Ordinal))
            return false;

        return await grainFactory.GetLatticeRegistry().ExistsAsync(TreeId);
    }

    /// <summary>
    /// Clears the deletion record of a purged tree whose id was registered again,
    /// returning whether it did. Called at the start of every lifecycle verb, so
    /// the verb then acts on the live tree the id now names. Any alias
    /// reservation is kept; <see cref="HoldsTerminalPurgeRecord"/> excludes a
    /// record carrying one anyway.
    /// </summary>
    private async Task<bool> ClearRecordIfReusedAfterPurgeAsync()
    {
        if (!await IsReusedAfterPurgeAsync())
            return false;

        var previous = state.State;
        state.State = new TreeDeletionState();
        try
        {
            await PersistAsync();
        }
        catch
        {
            state.State = previous;
            throw;
        }

        logger.LogInformation(
            "Tree {TreeId}: cleared the deletion record of a purged tree whose id has been registered again; "
            + "the id now names a live tree.",
            TreeId);

        // A completed purge unregisters its reminders; these calls only cover
        // one that was interrupted before it could. Best-effort: a stale
        // reminder that survives finds no deletion to act on.
        await UnregisterAllRemindersAsync();
        try
        {
            await RemoveLogicalReminderAsync();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to unregister the logical deletion reminder for tree {TreeId}", TreeId);
        }

        return true;
    }
}
