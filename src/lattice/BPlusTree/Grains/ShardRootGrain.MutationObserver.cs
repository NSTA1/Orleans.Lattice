using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Invokes the silo-wide <see cref="MutationObserverDispatcher"/> for the
/// shard-scoped mutation kinds that span multiple leaves (currently only
/// <see cref="MutationKind.DeleteRange"/>). Per-key <see cref="MutationKind.Set"/>
/// and <see cref="MutationKind.Delete"/> notifications are published by the
/// leaf grain itself (see <c>BPlusLeafGrain.MutationObserver</c>).
/// </summary>
internal sealed partial class ShardRootGrain
{
    /// <summary>
    /// Publishes a single <see cref="MutationKind.DeleteRange"/> notification
    /// for the completed range-delete walk. Emitted <b>unconditionally</b>
    /// — observers see every user-initiated range delete, including those
    /// that matched zero live keys. This is intentional: replication
    /// consumers must propagate the range to peer clusters regardless of
    /// whether it deleted anything locally, because peers may hold keys in
    /// the same range. Callers that only care about deletes with effect
    /// should check their own local state instead of filtering here.
    /// Contrast with the tree-event stream (<see cref="LatticeTreeEvent"/>),
    /// which collapses zero-delete ranges into a no-op event for UI / audit
    /// consumers.
    /// </summary>
    private Task PublishDeleteRangeAsync(string startInclusive, string endExclusive)
    {
        if (!mutationObservers.HasObservers) return Task.CompletedTask;
        var mutation = new LatticeMutation
        {
            TreeId = TreeId,
            Kind = MutationKind.DeleteRange,
            Key = startInclusive,
            EndExclusiveKey = endExclusive,
            IsTombstone = true,
            // Range deletes span many per-key HLCs; we deliberately surface
            // HybridLogicalClock.Zero rather than leaking a per-leaf HLC pick.
            // Observers that need per-key resolution must synthesize deletes
            // from the tree's current key set.
            Timestamp = HybridLogicalClock.Zero,
            // Range deletes read the ambient origin at publish time — there
            // is no per-key LwwValue to pull from — so replication consumers
            // can skip re-forwarding ranges that originated on another cluster.
            OriginClusterId = LatticeOriginContext.Current,
        };
        return mutationObservers.PublishAsync(mutation);
    }
}
