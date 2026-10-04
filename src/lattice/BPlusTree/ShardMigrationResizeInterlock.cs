namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The interlock between an online resize and the shard migrations that move
/// virtual slots between a tree's shards - an adaptive split and an online
/// consolidation (issue #4452). A migration may not run while a resize is in
/// flight, nor - once it completed - while the copy it replaced still mirrors
/// into the resized copy; a resize may not start while a migration is in flight.
/// <para>
/// A resize computes the shard set it copies, shadow-forwards and fences, and
/// the map it carries onto the logical tree at the flip, once, from the routing
/// map at its start. A migration in flight across that start commits a map the
/// resize never sees: its target shard is neither copied nor fenced, so writes
/// it takes after the commit are lost at the flip, a router that cached the
/// post-migration map keeps serving it, and an undo restores the pre-migration
/// map while the source keeps the migration's moved-away seal, so the moved
/// slots are refused forever. A migration that starts during the resize does
/// the same from the other side.
/// </para>
/// <para>
/// The hold outlasts the flip. Through the soft-delete window the replaced copy
/// still mirrors into the resized copy, shard index for shard index, and a saga
/// bound to the replaced copy still prepares and delivers its terminals there.
/// A split on the resized copy in that window moves a key to a shard the mirror
/// never addresses, so the saga's bucket on the new shard is never drained and
/// an acknowledged write is lost (the shard-ownership spec's NoKeyLost trace).
/// The hold therefore lasts until no shard of the replaced copy mirrors into the
/// resized one - the purge or an undo clears that state - as
/// <see cref="ITreeResizeGrain.HoldsShardMigrationsAsync"/> reports.
/// </para>
/// <para>
/// Each side publishes its own intent durably and only then reads the other's,
/// so whichever of two racing starts reads second sees the first and backs out:
/// the resize persists its intent and then reads every routed shard's migration
/// record (<see cref="FindMigratingShardAsync"/>); a migration opens its source
/// shard's record and then reads the resize coordinator
/// (<see cref="ResizeHoldsShardMigrationsAsync"/>). Both reads are of state
/// written before the publishing call returned, so no interleaving lets both
/// proceed.
/// </para>
/// <para>
/// Both reads fail closed. A resize coordinator that cannot answer - including
/// one hosted on a silo too old to implement
/// <see cref="ITreeResizeGrain.HoldsShardMigrationsAsync"/> during a rolling
/// upgrade - holds migrations, so a split or fold is refused until it can, and
/// the autonomic drivers propose it again on a later sweep. The reshard
/// coordinator reads through <see cref="ReadResizeHoldAsync"/>, which lets the
/// fault propagate instead: to <c>ReshardAsync</c> that is a refusal too, and
/// to a reshard tick it is a faulted step whose own recovery must still run.
/// </para>
/// </summary>
internal static class ShardMigrationResizeInterlock
{
    /// <summary>
    /// Whether a resize of <paramref name="treeId"/>, or of the tree it was
    /// derived from when it is a resized physical copy (a migration coordinator
    /// can be addressed by either id), holds shard migrations (see
    /// <see cref="ITreeResizeGrain.HoldsShardMigrationsAsync"/>). Fails closed:
    /// a coordinator or registry call that throws reads as a hold.
    /// </summary>
    internal static async Task<bool> ResizeHoldsShardMigrationsAsync(IGrainFactory grainFactory, string treeId)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(treeId);

        try
        {
            return await ReadResizeHoldAsync(grainFactory, treeId);
        }
        catch (Exception)
        {
            return true;
        }
    }

    /// <summary>
    /// <see cref="ResizeHoldsShardMigrationsAsync"/> without the fail-closed
    /// catch: a coordinator or registry fault propagates. For a caller to which
    /// the exception is already a refusal or a faulted step - the reshard
    /// coordinator, whose faulted tick runs its own recovery (abandonment on a
    /// purged tree) - and which must not mistake a fault for a hold.
    /// </summary>
    internal static async Task<bool> ReadResizeHoldAsync(IGrainFactory grainFactory, string treeId)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(treeId);

        if (await grainFactory.GetGrain<ITreeResizeGrain>(treeId).HoldsShardMigrationsAsync())
        {
            return true;
        }

        var entry = await grainFactory.GetLatticeRegistry().GetEntryAsync(treeId);
        return entry?.DerivedFrom is { } owner
            && !string.Equals(owner, treeId, StringComparison.Ordinal)
            && await grainFactory.GetGrain<ITreeResizeGrain>(owner).HoldsShardMigrationsAsync();
    }

    /// <summary>
    /// The first of <paramref name="shardIndices"/> on
    /// <paramref name="physicalTreeId"/> that carries a migration record - the
    /// source of an adaptive split or the donor of an online consolidation, which
    /// keeps the record from opening its shadow-write window until its final
    /// drain - or <see langword="null"/> when none does.
    /// </summary>
    internal static async Task<int?> FindMigratingShardAsync(
        IGrainFactory grainFactory, string physicalTreeId, IReadOnlyList<int> shardIndices)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(physicalTreeId);
        ArgumentNullException.ThrowIfNull(shardIndices);

        var probes = new Task<bool>[shardIndices.Count];
        for (var i = 0; i < shardIndices.Count; i++)
        {
            probes[i] = grainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{shardIndices[i]}").IsSplittingAsync();
        }

        await Task.WhenAll(probes);
        for (var i = 0; i < probes.Length; i++)
        {
            if (probes[i].Result) return shardIndices[i];
        }

        return null;
    }
}
