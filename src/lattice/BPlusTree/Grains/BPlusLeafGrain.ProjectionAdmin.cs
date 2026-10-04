using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Operator-tooling partial for <see cref="Orleans.Lattice.BPlusTree.Grains.BPlusLeafGrain"/>. Exposes the
/// read-only projection-checkpoint accessor consumed by the public
/// materialiser-lag surface and the destructive projection-rebuild seam
/// that resets the leaf's materialised projection so the next activation
/// re-materialises it: the activation-time path reloads the leaf's snapshot
/// where a usable one exists (the rebuild does not clear it) and replays the
/// per-shard WAL after it, from offset <c>0</c> only for a partition no
/// snapshot covers.
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
        // Step 0 - an unreadable snapshot is discarded, accepting the loss
        // (issue #4450). Every activation fails its replay closed on a snapshot
        // that will not load, because under coverage-gated trim it may be the
        // only durable copy of the prefix it covers. This explicit, operator-
        // invoked rebuild is the one path allowed past that: a snapshot that
        // is present but proven unreadable is cleared, so the next activation
        // sees no snapshot and rebuilds from the WAL that survives. Run before
        // anything is reset, so a store that cannot answer fails the rebuild
        // and leaves this activation exactly as it was for the retry.
        await DiscardUnreadableSnapshotForRebuildAsync();

        // Retire the replay before anything else (issue #2871). This method IS a
        // replay reset: it clears the projection and sets the checkpoint back so
        // the NEXT activation re-materialises it (from the leaf's snapshot where
        // one is usable, then the WAL after it). A replay still in flight from
        // THIS activation would race that - writing entries into the cache this
        // method is clearing, and advancing the checkpoint this method is about to
        // rewind - so the rebuild could complete and leave behind a projection
        // neither wholly old nor wholly new. Retiring rather than merely
        // cancelling also stops a later data operation on this activation
        // re-arming a replay against the half-cleared state; the deactivation at
        // the end of this method is what returns the leaf to a clean start.
        RetireReplayBarrier();

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

        // ProjectionCheckpointOffset uses "SCANNED through offset N"
        // semantics, not "applied through" (issue #2270). Replay advances
        // it over every entry it reads, INCLUDING entries it deliberately
        // skips as belonging to another leaf's key range or shard: the
        // advance in ReplayPartitionAsync sits outside the
        // ShouldApplyDuringReplay filter. The replay gate then reads
        // strictly past it via ReadSliceAsync's fromExclusive parameter.
        //
        // That is deliberate and load-bearing, not an oversight. Advancing
        // only over APPLIED entries would leave a leaf that owns no key in
        // a partition sitting at its old checkpoint forever: it would
        // re-scan that partition on every activation, and because
        // LatticeWalGc.ComputeMaterialiserOffsetFloorAsync takes the
        // MINIMUM of these offsets as the WAL retention floor, that one
        // stalled leaf would pin WAL truncation for the whole tree.
        //
        // Scanning ahead of applying is safe there for the same reason:
        // the floor is a minimum, and skipping only ever inflates the
        // checkpoint of a leaf that does NOT own the entry. The single
        // leaf that does own it cannot skip it, so it holds the minimum
        // below that offset until it genuinely applies, and the entry is
        // retained. The one operation that can retire that owner - empty
        // leaf reclaim - is gated on LiveRowCount == 0 measured after a
        // forced replay to head, and empty is precisely the value on which
        // "skipped" and "applied" agree, so the absorbing predecessor
        // inherits the range with nothing outstanding in it.
        //
        // Resetting to 0 here would tell the next activation "I have
        // already scanned through offset 0", silently skipping the very
        // first WAL entry that belongs to this leaf. The "nothing scanned"
        // sentinel is -1, matching IWalStorageProvider.GetHighestOffsetAsync's
        // -1-for-empty-WAL contract, so a partition no snapshot covers is read
        // from offset 0 inclusive on the next activation (a snapshot rehydrate
        // instead lifts a covered partition's checkpoint to the snapshot's).
        state.State.ProjectionCheckpointOffset = -1;
        // Clear the assignment marker alongside the sentinel so the rebuilt row
        // reads as "nothing applied" through every path (issue #2703).
        state.State.ProjectionCheckpointOffsetAssigned = null;
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
        _unmarkedPrepares = null;
        _pendingTxOffsets = null;
        _pendingTxBatches = null;
        _recentlyTerminal = null;
        _backstoppedTerminals = null;
        _terminalLandedClock = null;
        // The destination-side shadow markers are activation-scoped in exactly
        // the same way, and dropping them is load-bearing rather than tidy.
        // _shadowedSagas is gated against _recentlyTerminal: a marker is safe
        // to serve past only once its saga's terminal has been seen on this
        // leaf. Clearing _recentlyTerminal while leaving the markers in place
        // therefore does not preserve the markers, it strands them - every
        // marked key becomes permanently unsafe for the life of the
        // activation, because the terminal that would have cleared it has
        // already been forgotten and cannot arrive twice. The read gate would
        // raise StaleShardRoutingException for those keys until the grain
        // deactivates. A fresh activation holds no markers, so dropping them
        // is what actually makes the rebuild indistinguishable from one, which
        // is the property the comment above claims.
        _shadowedSagas = null;
        _shadowMarkerStamps = null;

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
        // failure, which a retry repairs because it repeats the clear
        // before the next persist attempt. Until then this activation
        // keeps running with the cleared projection: the replay barrier
        // is already retired and the deactivation below is not reached.
        await PersistAsync();

#if LATTICE_DIAG
        DiagSink.Write($"[DIAG rebuild-persisted] gid={context.GrainId} entryCount={Cache.Count} checkpoint={state.State.ProjectionCheckpointOffset}");
#endif

        // Step 3 - deactivate the grain. The next activation's
        // OnActivateAsync hook starts the activation replay (in the
        // background since issue #2871). Its step 0 snapshot rehydrate runs
        // first, as on every activation: this method does not clear the
        // leaf's snapshot, and against the -1 checkpoint and the empty cache
        // of a fresh activation a usable snapshot is always accepted, so it
        // reloads the cache and lifts each covered partition's checkpoint to
        // the snapshot's offset.
        // ReplayWalSinceCheckpointAsync then walks the WAL after those
        // checkpoints - from offset 0 (inclusive) only for a partition no
        // snapshot covers - through the existing slice-budgeted
        // materialiser. The materialiser's per-entry filter
        // (ShouldApplyDuringReplay) keys on the persisted topology
        // slots that survived the rebuild, so the replay populates
        // only this leaf's owned subset of the shared shard WAL.
        context.Deactivate(new DeactivationReason(
            DeactivationReasonCode.ApplicationRequested,
            "Leaf projection rebuild from WAL requested via operator tooling."));
    }

    /// <summary>
    /// Clears this leaf's snapshot when it is present but proven unreadable - an
    /// unreadable row payload, or a segment that is missing or unreadable - so the
    /// rebuild's next activation sees no snapshot and replays the WAL that survives,
    /// accepting the loss of whatever only that snapshot held (issue #4450). A
    /// readable or absent snapshot is left alone, exactly as before.
    /// </summary>
    /// <remarks>
    /// A load that THROWS fails the rebuild instead of clearing. A storage fault
    /// cannot be told apart from a transient one, and clearing a snapshot that was
    /// merely unreachable would destroy the only durable copy of a prefix the
    /// operator did not need to lose. The retry succeeds once the store answers.
    /// </remarks>
    private async Task DiscardUnreadableSnapshotForRebuildAsync()
    {
        var treeId = state.State.TreeId;
        if (treeId is null || !context.GrainId.TryGetGuidKey(out var leafKey, out _))
            return;

        var snapshotGrain = grainFactory.GetGrain<ILeafSnapshotStorageGrain>(leafKey);
        string? unreadable;
        try
        {
            var blob = await snapshotGrain.LoadAsync(CancellationToken.None);
            if (blob is null)
                return;

            unreadable = await DescribeUnreadableSnapshotAsync(snapshotGrain, blob);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"Cannot rebuild the projection of a leaf of tree '{treeId}': its snapshot could not be read, so "
                + "the rebuild cannot tell an unreachable snapshot from an unreadable one and will not discard it. "
                + "Retry once the snapshot store is reachable.",
                ex);
        }

        if (unreadable is null)
            return;

        ResolveLogger()?.LogWarning(
            "Leaf {GrainId} of tree {TreeId}: projection rebuild is DISCARDING its unreadable snapshot ({Reason}), "
            + "accepting the loss of any acknowledged write that only that snapshot held - under coverage-gated WAL "
            + "trimming the WAL may no longer hold the prefix it covered. The leaf will rebuild from the WAL that "
            + "survives. Restore the tree from a backup if that loss is not acceptable.",
            context.GrainId,
            treeId,
            unreadable);

        await snapshotGrain.ClearAsync(CancellationToken.None);
    }

    /// <summary>
    /// Returns why <paramref name="blob"/> cannot be rehydrated, or
    /// <see langword="null"/> when every row it claims can be read. Mirrors the
    /// rehydrate's own fail-closed checks; a segment read that throws propagates.
    /// </summary>
    private static async Task<string?> DescribeUnreadableSnapshotAsync(
        ILeafSnapshotStorageGrain snapshotGrain,
        LeafSnapshotBlob blob)
    {
        if (!blob.ValidateRowPayload())
            return "unreadable row payload";

        for (var segmentIndex = 0; segmentIndex < blob.SegmentCount; segmentIndex++)
        {
            var frame = await snapshotGrain.LoadSegmentFrameAsync(segmentIndex, CancellationToken.None);
            if (frame is not { Length: > 0 })
                return $"segment {segmentIndex} missing";

            if (!new LeafSnapshotBlob { EncodedRows = frame }.ValidateRowPayload())
                return $"segment {segmentIndex} unreadable";
        }

        return null;
    }
}
