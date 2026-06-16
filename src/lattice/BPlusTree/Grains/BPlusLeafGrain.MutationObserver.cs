using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Invokes the silo-wide <see cref="MutationObserverDispatcher"/> for each
/// durably-committed mutation produced by this leaf. Helpers are fast-no-ops
/// when no <see cref="IMutationObserver"/> is registered, so the write path
/// pays at most one branch check when the hook is unused.
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Publishes a <see cref="MutationKind.Set"/> notification for the given
    /// key / committed LWW entry. The <see cref="LatticeMutation.IsTombstone"/>
    /// flag mirrors the entry - a <c>Set</c> may carry a tombstone when an
    /// externally-supplied value loses LWW to an existing tombstone.
    /// </summary>
    private Task PublishSetAsync(string key, LwwValue<byte[]> committed)
    {
        if (!mutationObservers.HasObservers) return Task.CompletedTask;
        var delta = ResolvePublishDelta(key);
        var batch = LatticeAtomicBatchContext.Current;
        var mutation = new LatticeMutation
        {
            TreeId = state.State.TreeId ?? string.Empty,
            Kind = MutationKind.Set,
            Key = key,
            Value = committed.IsTombstone ? null : committed.Value,
            Timestamp = committed.Timestamp,
            IsTombstone = committed.IsTombstone,
            ExpiresAtTicks = committed.ExpiresAtTicks,
            OriginClusterId = committed.OriginClusterId,
            VectorClock = committed.VectorClock,
            TransactionId = LatticeTransactionContext.Current,
            Category = LatticeMaintenanceContext.Current,
            Delta = delta,
            AtomicBatchSize = batch?.Size ?? 0,
            AtomicBatchIndex = batch?.Index ?? 0,
            IsPrepared = LatticePreparedContext.Current,
            ShardIndex = state.State.ShardIndex ?? 0,
        };
        return mutationObservers.PublishAsync(mutation);
    }

    /// <summary>
    /// Publishes a <see cref="MutationKind.Delete"/> notification for the given
    /// key / tombstone entry stamped with the leaf's latest HLC.
    /// </summary>
    private Task PublishDeleteAsync(string key, LwwValue<byte[]> tombstone)
    {
        if (!mutationObservers.HasObservers) return Task.CompletedTask;
        var delta = ResolvePublishDelta(key);
        var batch = LatticeAtomicBatchContext.Current;
        var mutation = new LatticeMutation
        {
            TreeId = state.State.TreeId ?? string.Empty,
            Kind = MutationKind.Delete,
            Key = key,
            Timestamp = tombstone.Timestamp,
            IsTombstone = true,
            OriginClusterId = tombstone.OriginClusterId,
            VectorClock = tombstone.VectorClock,
            TransactionId = LatticeTransactionContext.Current,
            Category = LatticeMaintenanceContext.Current,
            Delta = delta,
            AtomicBatchSize = batch?.Size ?? 0,
            AtomicBatchIndex = batch?.Index ?? 0,
            IsPrepared = LatticePreparedContext.Current,
            ShardIndex = state.State.ShardIndex ?? 0,
        };
        return mutationObservers.PublishAsync(mutation);
    }

    /// <summary>
    /// Resolves the author-delta to stamp onto the per-key mutation about to
    /// be published. Prefers a per-entry delta supplied through
    /// <see cref="LatticeAtomicBatchContext.CurrentDeltaMap"/> (the carry an
    /// atomic-write saga uses to stamp a distinct typed CRDT delta on each
    /// entry - for example a flag-CRDT membership row's enable-dot delta),
    /// falling back to the saga-wide / single-write
    /// <see cref="LatticeDeltaContext.Current"/> carry when no per-key delta
    /// is present for <paramref name="key"/>. Keeps every non-saga and
    /// saga-wide-only write byte-identical to the pre-existing behaviour.
    /// </summary>
    private static byte[]? ResolvePublishDelta(string key)
    {
        var deltaMap = LatticeAtomicBatchContext.CurrentDeltaMap;
        if (deltaMap is not null && deltaMap.TryGetValue(key, out var perEntryDelta))
        {
            return perEntryDelta;
        }
        return LatticeDeltaContext.Current;
    }
}
