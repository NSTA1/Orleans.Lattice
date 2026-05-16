using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Leaf-node split logic: two-phase crash-safe split with deterministic sibling identity.
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    private async Task<SplitResult> SplitAsync()
    {
        var keys = state.State.Entries.Keys.ToList();
        int mid = keys.Count / 2;
        var splitKey = keys[mid];

        // Snapshot the WAL head for this shard's partition before the
        // split's intent is persisted. The donor's foreground commits
        // up to this offset have already been applied to
        // state.State.Entries, so on a future activation the
        // materialiser can skip replaying them. This is correctness-
        // safe even without the snapshot (the per-key-range filter
        // drops foreign-range entries and LWW makes own-range
        // re-application idempotent), but the snapshot bounds replay
        // to entries committed strictly after the split.
        var treeId = state.State.TreeId;
        long walHeadAtSplit = 0;
        if (!string.IsNullOrEmpty(treeId))
        {
            var coordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
                $"{treeId}/{ReplayWalPartition}");
            walHeadAtSplit = await coordinator.GetHeadOffsetAsync(CancellationToken.None);
        }

        state.State.SplitState = state.State.SplitState.Merge(Primitives.SplitState.SplitInProgress);
        state.State.SplitKey = splitKey;
        state.State.SplitSiblingId = grainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid()).GetGrainId();
        state.State.OldNextSibling = state.State.NextSibling;
        state.State.NextSibling = state.State.SplitSiblingId;
        await PersistAsync();

        LatticeMetrics.LeafSplits.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, state.State.TreeId ?? string.Empty));
        return await CompleteSplitAsync(walHeadAtSplit);
    }

    /// <summary>
    /// Completes (or resumes) a split whose intent has already been persisted.
    /// Safe to call multiple times - MergeEntriesAsync is idempotent (LWW merge).
    /// On the recovery path (caller does not hold a captured WAL head) the
    /// optional <paramref name="walHeadAtSplit"/> is omitted; the recovery
    /// branch reads the current WAL head fresh, which is still safe - a
    /// later head only causes the sibling to skip more replay, never less.
    /// </summary>
    private async Task<SplitResult> CompleteSplitAsync(long? walHeadAtSplit = null)
    {
        var splitKey = state.State.SplitKey!;
        var siblingId = state.State.SplitSiblingId!.Value;
        // Capture the donor's pre-split high BEFORE the donor narrows
        // its own range at the end of this method. The sibling's high
        // is the donor's pre-split high (siblings inherit the donor's
        // upstream upper bound); the donor's new high becomes the
        // split key.
        var donorPreSplitHigh = state.State.HighKeyExclusive;
        var newLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(siblingId);

        // Fresh WAL-head read on the recovery path (no captured head
        // from the original SplitAsync invocation). A larger head is
        // still safe - it only causes the sibling to skip more
        // replay, never less.
        var resolvedWalHead = walHeadAtSplit;
        if (resolvedWalHead is null)
        {
            var treeId = state.State.TreeId;
            if (!string.IsNullOrEmpty(treeId))
            {
                var coordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
                    $"{treeId}/{ReplayWalPartition}");
                resolvedWalHead = await coordinator.GetHeadOffsetAsync(CancellationToken.None);
            }
        }

        await newLeaf.SetTreeIdAsync(state.State.TreeId!);
        // The split sibling inherits this leaf's owning chain-shard
        // index. A leaf-level split never crosses chain-shard
        // boundaries (cross-shard rebalance goes through the
        // shard-rewrite path, not a leaf split), so the parent's
        // ShardIndex is the correct value for the new sibling.
        // Pre-Option A leaves whose ShardIndex slot is null (legacy
        // state shape) skip this seed so the sibling remains in the
        // same legacy "apply unconditionally" mode.
        if (state.State.ShardIndex is { } parentShardIndex)
        {
            await newLeaf.SetShardIndexAsync(parentShardIndex);
        }
        // Stamp the sibling's [low, high) ownership range. The
        // sibling's low is the split key (its lowest owned key); its
        // high is the donor's pre-split high (the sibling inherits the
        // upper bound from the upstream chain). SetKeyRangeAsync is
        // idempotent on a non-null low, so a re-call from the
        // recovery path is safe.
        await newLeaf.SetKeyRangeAsync(splitKey, donorPreSplitHigh);

        var rightEntries = new Dictionary<string, LwwValue<byte[]>>();
        foreach (var (key, lww) in state.State.Entries)
        {
            if (string.Compare(key, splitKey, StringComparison.Ordinal) >= 0)
            {
                rightEntries[key] = lww;
            }
        }

        if (rightEntries.Count > 0)
        {
            await newLeaf.MergeEntriesAsync(rightEntries);
        }

        // Stamp the sibling's initial projection-checkpoint hint AFTER
        // the merge so the sibling considers its just-populated
        // entries already-materialised on its first activation. No-op
        // when no WAL writer is configured (resolvedWalHead = 0).
        if (resolvedWalHead is { } siblingHead && siblingHead > 0)
        {
            await newLeaf.SetCheckpointOffsetHintAsync(siblingHead);
        }

        var oldNextId = state.State.OldNextSibling;

        await newLeaf.SetNextSiblingAsync(oldNextId);
        await newLeaf.SetPrevSiblingAsync(context.GrainId);

        if (oldNextId is not null)
        {
            var oldNext = grainFactory.GetGrain<IBPlusLeafGrain>(oldNextId.Value);
            await oldNext.SetPrevSiblingAsync(siblingId);
        }

        foreach (var key in rightEntries.Keys)
        {
            RemoveEntry(key);
        }

        // Donor narrows its own ownership range to [low, splitKey).
        // The low is unchanged; only the high collapses to the split
        // key. Performed AFTER the sibling has been stamped with the
        // donor's pre-split high so a crash mid-CompleteSplitAsync
        // leaves the donor's slot at the original high (idempotent
        // re-run on recovery still passes the correct high to the
        // sibling).
        state.State.HighKeyExclusive = splitKey;
        state.State.OldNextSibling = null;
        state.State.SplitState = state.State.SplitState.Merge(Primitives.SplitState.SplitComplete);

        // Advance the donor's projection checkpoint to the WAL head
        // captured at split time so the donor's first post-split
        // activation skips replaying entries already reflected in
        // state.State.Entries. Routes through the projection seam so
        // the unresolved-prepare clamp is honoured. No-op when no WAL
        // writer is configured.
        if (resolvedWalHead is { } donorHead && donorHead > 0)
        {
            await ((ILeafProjection)this).SetCheckpointOffsetAsync(donorHead, CancellationToken.None);
        }

        // Forward the donor's projection-hash delta (the XOR-fold
        // over every removed entry's contribution) plus the new
        // entry-count to the parent internal node. SetCheckpointOffsetAsync
        // above already triggers an upward publish via
        // FlushPendingCheckpointAsync when a WAL writer is present,
        // but the no-WAL-writer path skips that flush - so an
        // explicit publish here keeps the chain consistent across
        // both shapes. PublishDigestUpwardAsync is a no-op when
        // _digestDirty is false (no entries crossed the split) or
        // when this leaf has no parent yet (the split-into-flat-tree
        // case where the shard-root promotion is still pending).
        await PublishDigestUpwardAsync();

        return new SplitResult
        {
            PromotedKey = splitKey,
            NewSiblingId = siblingId
        };
    }
}
