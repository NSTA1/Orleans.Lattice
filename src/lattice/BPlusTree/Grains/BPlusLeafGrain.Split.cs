using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Leaf-node split logic: two-phase crash-safe split with deterministic sibling identity.
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Per-activation gated entry point for <see cref="SplitAsync"/>.
    /// Acquires <c>_splitGate</c>, re-checks the overflow predicate
    /// inside the gate (a concurrent interleaved turn may have already
    /// split this leaf, in which case <c>Cache.Count</c> is now back
    /// under the threshold), and runs the split only if still required.
    /// Returns <see langword="null"/> when the in-flight split absorbed
    /// the caller's overflow, mirroring the no-split branch of the
    /// foreground commit paths.
    /// <para>
    /// Required by U9p step 8c-c-iv-c2-iii: the mutation surface
    /// (<see cref="IBPlusLeafGrain.SetAsync(string, byte[])"/>,
    /// <see cref="IBPlusLeafGrain.SetManyAsync"/>,
    /// <see cref="IBPlusLeafGrain.DeleteAsync"/>,
    /// <see cref="IBPlusLeafGrain.MergeManyAsync"/>) is marked
    /// <c>[AlwaysInterleave]</c>, so two interleaved turns can both
    /// observe overflow before either flips
    /// <see cref="Primitives.SplitState.SplitInProgress"/>; this gate
    /// serialises every entry to <c>SplitAsync</c> and the re-check
    /// drops the cascade caller without doing duplicate work.
    /// </para>
    /// </summary>
    private async Task<SplitResult?> SplitIfNeededUnderGateAsync(int maxLeafKeys)
    {
        await _splitGate.WaitAsync().ConfigureAwait(true);
        try
        {
            // Re-check inside the gate. A concurrent turn may have
            // already split this leaf and removed our overflow's
            // entries to the sibling, leaving Cache.Count back under
            // the threshold; in that case we have nothing to do.
            if (Cache.Count <= maxLeafKeys)
                return null;
            return await SplitAsync();
        }
        finally
        {
            _splitGate.Release();
        }
    }

    /// <summary>
    /// Per-activation gated entry point for the recovery-path
    /// <see cref="CompleteSplitAsync"/> calls in
    /// <see cref="SetCoreAsync"/> and
    /// <see cref="MergeManyAsync"/>.
    /// Acquires <c>_splitGate</c>, re-checks
    /// <see cref="Primitives.SplitState.SplitInProgress"/> inside the
    /// gate (a concurrent turn may have already completed the split),
    /// and runs <see cref="CompleteSplitAsync"/> + <see cref="PersistAsync"/>
    /// only if the in-progress state is still observed. Returns
    /// <see langword="null"/> when a concurrent turn already finished
    /// the recovery; the caller still has stable
    /// <see cref="State.LeafNodeState.SplitKey"/> /
    /// <see cref="State.LeafNodeState.SplitSiblingId"/> fields to
    /// route its own write across the donor / sibling boundary, so
    /// the post-gate routing in the caller is correct either way.
    /// </summary>
    private async Task<SplitResult?> CompleteRecoverySplitUnderGateAsync()
    {
        await _splitGate.WaitAsync().ConfigureAwait(true);
        try
        {
            if (state.State.SplitState != Primitives.SplitState.SplitInProgress)
                return null;
            var recovered = await CompleteSplitAsync();
            await PersistAsync();
            return recovered;
        }
        finally
        {
            _splitGate.Release();
        }
    }

    private async Task<SplitResult> SplitAsync()
    {
        var keys = Cache.Keys.ToList();
        int mid = keys.Count / 2;
        var splitKey = keys[mid];

        // Snapshot the WAL head for this shard's partition before the
        // split's intent is persisted. The donor's foreground commits
        // up to this offset have already been applied to the runtime
        // entry cache, so on a future activation the
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
        foreach (var (key, lww) in Cache.EnumerateRows())
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
        // activation skips replaying entries already reflected in the
        // runtime entry cache. Routes through the projection seam so
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
        // both shapes. PublishDigestUpwardInlineAsync is a no-op when
        // _digestDirty is false (no entries crossed the split) or
        // when this leaf has no parent yet (the split-into-flat-tree
        // case where the shard-root promotion is still pending).
        // The inline variant bypasses the c2-xxviii coalescing window
        // so the parent's chained fold observes the post-split
        // aggregate before the caller returns - structural events
        // are explicitly excluded from coalescing (see c2-xxviii memo).
        await PublishDigestUpwardInlineAsync();

        return new SplitResult
        {
            PromotedKey = splitKey,
            NewSiblingId = siblingId,
            ChildIsLeaf = true,
        };
    }
}
