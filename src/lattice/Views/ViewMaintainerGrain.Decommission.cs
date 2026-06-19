using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Views;

/// <summary>
/// View teardown. <see cref="DecommissionAsync"/> reverses everything an active
/// maintainer establishes - the durable keepalive reminder, the source WAL cursor
/// pin, the backing view-tree generations, and the durable checkpoint - so a
/// deleted view leaves no orphaned reminder, pin, tree, or state behind. The
/// factory drives this and then removes the catalog entry and the durable runtime
/// registration.
/// </summary>
internal sealed partial class ViewMaintainerGrain
{
    /// <inheritdoc />
    public async Task DecommissionAsync(CancellationToken cancellationToken = default)
    {
        // Deleting a view tree is a maintainer-authorised view write (the public
        // ILattice surface rejects direct writes to view-* trees), so the whole
        // teardown runs under a view-write scope that flows on RequestContext.
        using var viewWriteScope = ViewWriteContext.BeginScope();

        // Stop background drains immediately so nothing re-pins the WAL or rebuilds
        // a tree while teardown is in flight.
        _timer?.Dispose();
        _timer = null;
        _activated = false;

        // The source tree id is needed only to release the WAL cursor pin. Prefer
        // the in-memory catalog, but fall back to the durable registry: this
        // maintainer can be activated fresh (no OnActivateAsync re-hydration) on a
        // silo whose catalog never saw the runtime Create, in which case the
        // catalog is empty yet the pin - reported during an earlier drain on
        // another activation - still holds the source WAL GC and must be released.
        var sourceTreeId = catalog.TryGet(ViewName)?.SourceTreeId
            ?? await TryGetDurableSourceTreeIdAsync();

        await UnregisterKeepaliveReminderAsync();

        // Release the source WAL pin so the GC is no longer held by this consumer.
        if (!string.IsNullOrEmpty(sourceTreeId))
        {
            try
            {
                await cursorRegistry.UnregisterAsync(sourceTreeId, ConsumerId, cancellationToken);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "View '{ViewName}' failed to release the source WAL cursor pin during decommission; continuing teardown.", ViewName);
            }
        }

        var highestDeletedGeneration = await DeleteBackingGenerationsAsync(cancellationToken);

        // Reset the durable checkpoint so a future view re-created under the same
        // name starts from a clean slate. The active generation is advanced past
        // every generation just soft-deleted: a deleted tree id is permanently
        // inaccessible, so a re-created view must address a fresh, never-used
        // generation rather than inheriting the dead generation 0.
        state.State = new ViewCheckpointState { ActiveGeneration = highestDeletedGeneration + 1 };
        await state.WriteStateAsync();

        _shipViewSuppressed = false;

        logger.LogInformation("View '{ViewName}' decommissioned: reminder unregistered, WAL pin released, backing trees deleted, checkpoint cleared.", ViewName);

        this.DeactivateOnIdle();
    }

    /// <summary>
    /// Reads the source tree id for this view from the durable runtime registry,
    /// used when the in-memory catalog has no entry (a maintainer activated fresh
    /// on a silo that never saw the runtime <see cref="ILatticeViewFactory.Create"/>).
    /// Returns <see langword="null"/> when no durable record exists or the registry
    /// cannot be read, in which case the WAL cursor pin cannot be released here.
    /// </summary>
    private async Task<string?> TryGetDurableSourceTreeIdAsync()
    {
        try
        {
            var registry = grainFactory.GetGrain<IViewRegistryGrain>(IViewRegistryGrain.SingletonKey);
            var records = await registry.ListAsync();
            foreach (var record in records)
            {
                if (string.Equals(record.ViewName, ViewName, StringComparison.Ordinal))
                {
                    return record.SourceTreeId;
                }
            }

            return null;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "View '{ViewName}' failed to read the durable runtime registry while resolving its source tree id during decommission; the source WAL cursor pin may not be released.", ViewName);
            return null;
        }
    }

    private async Task UnregisterKeepaliveReminderAsync()
    {
        try
        {
            var reminder = await reminderRegistry.GetReminder(context.GrainId, KeepaliveReminderName);
            if (reminder is not null)
            {
                await reminderRegistry.UnregisterReminder(context.GrainId, reminder);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "View '{ViewName}' failed to unregister its keepalive reminder during decommission; continuing teardown.", ViewName);
        }
    }

    /// <summary>
    /// Soft-deletes every backing view-tree generation through the standard
    /// tree-deletion machinery: the active generation, any generation awaiting
    /// reclamation, and every prior generation (each of which still holds shard
    /// state - reclamation only clears a generation's keys, it does not purge its
    /// shards). The generation-0 legacy <c>view-{name}</c> tree is always included.
    /// Returns the highest generation number that was deleted, so the caller can
    /// advance the checkpoint past it.
    /// </summary>
    private async Task<long> DeleteBackingGenerationsAsync(CancellationToken cancellationToken)
    {
        var generations = new HashSet<long>();
        for (var generation = 0L; generation <= state.State.ActiveGeneration; generation++)
        {
            generations.Add(generation);
        }

        if (state.State.HasPendingReclaim)
        {
            generations.Add(state.State.PendingReclaimGeneration);
        }

        foreach (var generation in generations)
        {
            var tree = grainFactory.GetGrain<ILattice>(GenerationTreeId(generation));
            try
            {
                await tree.DeleteTreeAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "View '{ViewName}' failed to delete backing generation {Generation} during decommission; continuing teardown.", ViewName, generation);
            }
        }

        return generations.Max();
    }
}
