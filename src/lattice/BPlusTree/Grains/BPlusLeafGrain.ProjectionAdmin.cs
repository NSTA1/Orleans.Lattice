namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Operator-tooling partial for <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/>. Exposes the
/// read-only projection-checkpoint accessor consumed by the public
/// materialiser-lag surface and the destructive projection-rebuild seam
/// that resets the leaf's materialised projection so the next activation
/// re-replays the per-shard WAL from offset <c>0</c>.
/// <para>
/// The rebuild seam is intentionally narrow: it clears only the
/// projection slots (<c>Entries</c>, the
/// incremental <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.ProjectionHash"/>, the
/// persisted <see cref="Orleans.Lattice.BPlusTree.State.LeafNodeState.ProjectionCheckpointOffset"/>,
/// and the per-leaf saga pending-tx map) and preserves every
/// topology-bearing slot (tree id, shard index, sibling pointers, key
/// range, parent pointer, split state). The activation-time materialiser
/// in <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/>.<c>OnActivateAsync</c> already keys
/// every WAL-filter decision on those topology slots, so the rebuild
/// observes the same routing context the pre-rebuild leaf used. The
/// operator surface deliberately does not expose
/// "edit the projection in place" or "skip a WAL entry" - those would
/// defeat the determinism contract.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <inheritdoc />
    public Task<long> GetProjectionCheckpointOffsetAsync() =>
        Task.FromResult(state.State.ProjectionCheckpointOffset);

    /// <inheritdoc />
    public async Task RebuildProjectionFromWalAsync()
    {
#if LATTICE_DIAG
        DiagSink.Write($"[DIAG rebuild-enter] gid={context.GrainId} treeId={state.State.TreeId} shardIndex={state.State.ShardIndex} " +
            $"low='{state.State.LowKeyInclusive ?? "<null>"}' high='{state.State.HighKeyExclusive ?? "<null>"}' " +
            $"entryCount={Cache.Count} entries=[{string.Join(',', Cache.Keys)}] " +
            $"checkpoint={state.State.ProjectionCheckpointOffset} clock={state.State.Clock} " +
            $"movedSlots=[{(state.State.MovedAwaySlots is null ? "" : string.Join(',', state.State.MovedAwaySlots))}] " +
            $"movedVsc={state.State.MovedAwayVirtualShardCount?.ToString() ?? "(none)"}");
#endif
        // Step 1 - clear the projection slots only. Topology-bearing
        // slots (TreeId, ShardIndex, LowKeyInclusive, HighKeyExclusive,
        // NextSibling, PrevSibling, SplitState/SplitKey/SplitSiblingId,
        // ParentId, MovedAwaySlots) are preserved verbatim so the
        // activation-time materialiser's per-entry filter
        // (ShouldApplyDuringReplay) observes the same ownership context
        // the pre-rebuild leaf used. Persisted clock and version vector
        // are likewise preserved - the materialiser advances them
        // monotonically from replayed entries, and a fresh-from-zero
        // clock would silently re-accept stale entries that the
        // pre-rebuild leaf had already merged past.
        Cache.Clear();
        state.State.ProjectionHash = null;

        // ProjectionCheckpointOffset uses "applied through offset N"
        // semantics (the materialiser advances it to entry.Offset
        // after each apply, and the replay gate reads strictly past
        // it via ReadSliceAsync's fromExclusive parameter). Resetting
        // to 0 would tell the next activation "I have already applied
        // through offset 0", silently skipping the very first WAL
        // entry that belongs to this leaf. The "nothing applied"
        // sentinel is -1, matching IWalStorageProvider.GetHighestOffsetAsync's
        // -1-for-empty-WAL contract, so the materialiser reads from
        // offset 0 inclusive on the next activation.
        state.State.ProjectionCheckpointOffset = -1;
        // Drop the per-partition slot too: a rebuild seeds a fresh
        // single-partition shape and a future write fans out lazily.
        state.State.ProjectionCheckpointOffsetsByPartition = null;
        _pendingCheckpointOffsetsByPartition = null;

        // The per-leaf saga pending-tx map and dedup sets live entirely
        // in activation memory; clearing them here makes the rebuild
        // call indistinguishable from a fresh activation with respect
        // to the saga lifecycle. The materialiser reconstructs them
        // deterministically by replaying every prepared mutation whose
        // terminal has not yet replayed.
        _pendingTx = null;
        _pendingTxOffsets = null;
        _recentlyTerminal = null;
        _backstoppedTerminals = null;

        // Drop the cached XxHash128 hasher so the rebuild's first
        // contribution allocates a fresh instance. The cached hasher
        // is activation-scoped state; leaving it in place across a
        // rebuild is harmless (XxHash128 carries no inter-call state
        // once GetHashAndReset has fired) but the explicit drop keeps
        // the rebuild call indistinguishable from a fresh activation.
        DisposeProjectionHasher();

        // Step 2 - persist the cleared projection slots. PersistAsync
        // routes through state.WriteStateAsync and surfaces transient
        // storage failures so the operator can retry; the leaf state
        // is left in the pre-persist (cleared) shape on a partial
        // failure, which is benign because a retry repeats the clear
        // before the next persist attempt.
        await PersistAsync();

#if LATTICE_DIAG
        DiagSink.Write($"[DIAG rebuild-persisted] gid={context.GrainId} entryCount={Cache.Count} checkpoint={state.State.ProjectionCheckpointOffset}");
#endif

        // Step 3 - deactivate the grain. The next activation's
        // OnActivateAsync hook drives ReplayWalSinceCheckpointAsync,
        // which sees ProjectionCheckpointOffset = -1 and walks the
        // WAL from offset 0 (inclusive) through the existing slice-budgeted
        // materialiser. The materialiser's per-entry filter
        // (ShouldApplyDuringReplay) keys on the persisted topology
        // slots that survived the rebuild, so the replay populates
        // only this leaf's owned subset of the shared shard WAL.
        context.Deactivate(new DeactivationReason(
            DeactivationReasonCode.ApplicationRequested,
            "Leaf projection rebuild from WAL requested via operator tooling."));
    }
}
