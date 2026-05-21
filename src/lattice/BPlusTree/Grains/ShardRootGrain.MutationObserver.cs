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
    /// - observers see every user-initiated range delete, including those
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
        var delta = LatticeDeltaContext.Current;
        var batch = LatticeAtomicBatchContext.Current;
        // The producer's DeleteRangeAsyncCore pins a single issue HLC
        // for the entire fan-out via LatticeHlcOverrideContext; every
        // per-leaf tombstone is stamped at that HLC. Surface the same
        // value on the observer payload (and hence on the persisted
        // WalRecord) so receivers can pin their per-leaf tombstones to
        // the producer's authoring frontier and preserve the
        // cross-origin LWW invariant. When the override is absent
        // (legacy callers that never set it - today only the in-tree
        // foreground path sets it, but third-party hosts of the grain
        // API may not), fall back to Zero so the wire shape matches
        // the historical "range deletes carry HLC.Zero" contract.
        var mutation = new LatticeMutation
        {
            TreeId = TreeId,
            Kind = MutationKind.DeleteRange,
            Key = startInclusive,
            EndExclusiveKey = endExclusive,
            IsTombstone = true,
            // Producer's range-delete issue HLC, pinned by
            // LatticeGrain.DeleteRangeAsyncCore. Receivers honour this
            // verbatim via LatticeHlcOverrideContext on the apply seam
            // (see IReplicationApplyGrain.ApplyDeleteRangeAsync).
            Timestamp = LatticeHlcOverrideContext.Current ?? HybridLogicalClock.Zero,
            // Range deletes read the ambient origin at publish time - there
            // is no per-key LwwValue to pull from - so replication consumers
            // can skip re-forwarding ranges that originated on another cluster.
            OriginClusterId = LatticeOriginContext.Current,
            // Range deletes likewise read the ambient vector-clock context
            // at publish time so replication-aware observers see the
            // frontier captured at the time of the delete call.
            VectorClock = LatticeVectorClockContext.Current,
            // Range deletes share a single transaction id across every
            // per-shard fan-out emit so consumers that dedup or
            // batch-correlate observe one transaction per user call.
            TransactionId = LatticeTransactionContext.Current,
            // Category mirrors the ambient maintenance flag - a structural
            // rewrite that fan-outs a range delete inside a maintenance
            // scope produces Maintenance emits, otherwise User.
            Category = LatticeMaintenanceContext.Current,
            // Author's pre-merge delta - opaque bytes the producer
            // attached via LatticeDeltaContext, propagated verbatim to
            // observers. Range deletes have a natural typed-delta shape
            // (start + end + HLC + origin) that consumers may choose to
            // encode here; the lattice library itself never opens the
            // payload.
            Delta = delta,
            // Range deletes that fan out from inside a saga inherit the
            // saga's batch metadata verbatim; user-driven range deletes
            // outside a saga stamp 0 / 0. Today no saga emits a range
            // delete, but the slot is shape-stable for that case.
            AtomicBatchSize = batch?.Size ?? 0,
            AtomicBatchIndex = batch?.Index ?? 0,
        };
        return mutationObservers.PublishAsync(mutation);
    }
}
