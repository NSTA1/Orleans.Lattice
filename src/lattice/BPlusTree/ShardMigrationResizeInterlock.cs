namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The interlock between an online resize and the shard migrations that move
/// virtual slots between a tree's shards - an adaptive split and an online
/// consolidation (issue #4452). Neither side may run while the other is in
/// flight on the same tree.
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
/// Each side publishes its own intent durably and only then reads the other's,
/// so whichever of two racing starts reads second sees the first and backs out:
/// the resize persists its intent and then reads every routed shard's migration
/// record (<see cref="FindMigratingShardAsync"/>); a migration opens its source
/// shard's record and then reads the resize coordinator
/// (<see cref="IsResizeInFlightAsync"/>). Both reads are of state written before
/// the publishing call returned, so no interleaving lets both proceed.
/// </para>
/// </summary>
internal static class ShardMigrationResizeInterlock
{
    /// <summary>
    /// Whether a resize is in flight for <paramref name="treeId"/>, or for the
    /// tree it was derived from when it is a resized physical copy (a migration
    /// coordinator can be addressed by either id). A completed resize in its
    /// soft-delete window is not in flight: the logical tree resolves to the
    /// resized copy, which a migration then binds to, and a migration still in
    /// flight on that copy when an undo moves the alias back is abandoned by its
    /// bound-tree check (issue #4264).
    /// </summary>
    internal static async Task<bool> IsResizeInFlightAsync(IGrainFactory grainFactory, string treeId)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(treeId);

        if (!await grainFactory.GetGrain<ITreeResizeGrain>(treeId).IsIdleAsync())
        {
            return true;
        }

        var entry = await grainFactory.GetLatticeRegistry().GetEntryAsync(treeId);
        return entry?.DerivedFrom is { } owner
            && !string.Equals(owner, treeId, StringComparison.Ordinal)
            && !await grainFactory.GetGrain<ITreeResizeGrain>(owner).IsIdleAsync();
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
