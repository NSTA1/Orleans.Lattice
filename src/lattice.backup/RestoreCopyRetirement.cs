using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Backup;

/// <summary>
/// The two mutations restore-copy retirement makes (issue #6103), each under the
/// logical tree's alias reservation so it can never interleave with a restore,
/// a revert, a resize or any other alias change of the tree, and each re-reading
/// the registry under that reservation rather than trusting an earlier plan.
/// </summary>
internal static class RestoreCopyRetirement
{
    /// <summary>
    /// The prefix of every alias reservation retirement takes. A restore or revert
    /// that meets one waits briefly for it rather than failing, because a
    /// retirement holds it only while it retires one copy.
    /// </summary>
    internal const string ReservationPrefix = "restore-retire:";

    /// <summary>
    /// Makes the restore whose shadow <paramref name="treeId"/> resolves to final,
    /// when the tree still resolves to <paramref name="shadowPhysicalTreeId"/>: its
    /// revert is refused from then on and the copy it would have returned to is
    /// retired. Returns whether the tree still resolved to the shadow.
    /// </summary>
    public static async Task<bool> FinalizeAsync(
        IGrainFactory grainFactory, string treeId, string shadowPhysicalTreeId, string reservationId)
    {
        using var origin = LatticeAccessGateContext.EnterSystemOrigin();
        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(treeId);
        await deletion.BeginAliasChangeAsync(reservationId).ConfigureAwait(false);
        try
        {
            var registry = grainFactory.GetLatticeRegistry();
            if (!string.Equals(
                    await registry.ResolveAsync(treeId).ConfigureAwait(false),
                    shadowPhysicalTreeId,
                    StringComparison.Ordinal))
            {
                return false;
            }

            await FinalizeShadowAsync(registry, treeId, shadowPhysicalTreeId).ConfigureAwait(false);
            return true;
        }
        finally
        {
            await deletion.EndAliasChangeAsync(reservationId).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Marks <paramref name="shadowPhysicalTreeId"/>'s restore final when it is a
    /// restore shadow of <paramref name="treeId"/> that is not final yet. The caller
    /// holds the tree's alias reservation.
    /// </summary>
    public static async Task FinalizeShadowAsync(ILatticeRegistry registry, string treeId, string shadowPhysicalTreeId)
    {
        if (string.Equals(shadowPhysicalTreeId, treeId, StringComparison.Ordinal)
            || await registry.GetEntryAsync(shadowPhysicalTreeId).ConfigureAwait(false) is not { } entry
            || !string.Equals(entry.RestoreShadowOfTreeId, treeId, StringComparison.Ordinal)
            || entry.RestoreFinal)
        {
            return;
        }

        await registry.UpdateAsync(shadowPhysicalTreeId, entry with
        {
            RestoreFinal = true,
            RestoreRevertPhysicalTreeId = null,
        }).ConfigureAwait(false);
    }

    /// <summary>
    /// Retires <paramref name="physicalTreeId"/> when, re-planned under the tree's
    /// alias reservation, it is still a restore copy of <paramref name="treeId"/>
    /// that no revert can reach. The tree's own shards are retired as the copy a
    /// first resize retires is - their registry entry and compaction schedule
    /// belong to the live tree - and a shadow as a resize's derived copy is: its
    /// shards are marked deleted, its compaction, shard-healing and hot-shard loops
    /// stop, and its shards, write-ahead log and registry entry are purged once
    /// <see cref="LatticeOptions.SoftDeleteDuration"/> has passed. Returns whether
    /// it was retired.
    /// </summary>
    public static async Task<bool> RetireAsync(IGrainFactory grainFactory, string treeId, string physicalTreeId)
    {
        using var origin = LatticeAccessGateContext.EnterSystemOrigin();
        var reservationId = ReservationPrefix + physicalTreeId;
        var deletion = grainFactory.GetGrain<ITreeDeletionGrain>(treeId);
        await deletion.BeginAliasChangeAsync(reservationId).ConfigureAwait(false);
        try
        {
            var plan = await RestoreCopyPlanner.PlanAsync(grainFactory, treeId).ConfigureAwait(false);
            if (!plan.RetirablePhysicalTreeIds.Contains(physicalTreeId, StringComparer.Ordinal))
            {
                return false;
            }

            if (string.Equals(physicalTreeId, treeId, StringComparison.Ordinal))
            {
                await deletion.DeleteRetiredPhysicalTreeAsync().ConfigureAwait(false);
            }
            else
            {
                await grainFactory.GetGrain<ITreeDeletionGrain>(physicalTreeId)
                    .DeleteDerivedPhysicalTreeAsync().ConfigureAwait(false);
            }

            return true;
        }
        finally
        {
            await deletion.EndAliasChangeAsync(reservationId).ConfigureAwait(false);
        }
    }
}
