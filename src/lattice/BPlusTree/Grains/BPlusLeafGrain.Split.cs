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

        // Snapshot the WAL head per partition before the split's
        // intent is persisted. Under multi-partition replay every
        // partition has its own offset space, so the sibling's per-
        // partition replay-from-zero must be bounded by the matching
        // partition's head; a single scalar would conflate them.
        var treeId = state.State.TreeId;
        long[]? walHeadsAtSplit = null;
        if (!string.IsNullOrEmpty(treeId))
        {
            walHeadsAtSplit = await CaptureWalHeadsByPartitionAsync(treeId);
        }

        state.State.SplitState = state.State.SplitState.Merge(Primitives.SplitState.SplitInProgress);
        state.State.SplitKey = splitKey;
        state.State.SplitSiblingId = grainFactory.GetGrain<IBPlusLeafGrain>(Guid.NewGuid()).GetGrainId();
        state.State.OldNextSibling = state.State.NextSibling;
        state.State.NextSibling = state.State.SplitSiblingId;
        await PersistAsync();

        LatticeMetrics.LeafSplits.Add(1,
            new KeyValuePair<string, object?>(LatticeMetrics.TagTree, state.State.TreeId ?? string.Empty));
        return await CompleteSplitAsync(walHeadsAtSplit);
    }

    /// <summary>
    /// Captures the current WAL head offset across every partition for
    /// the leaf's tree. Returns a non-null array whose length is the
    /// configured <see cref="LatticeOptions.WalPartitions"/>; entry
    /// <c>i</c> is the head of partition <c>i</c> at capture time.
    /// </summary>
    private async Task<long[]> CaptureWalHeadsByPartitionAsync(string treeId)
    {
        var options = await GetOptionsAsync();
        var partitionCount = Math.Max(1, options.WalPartitions);
        var heads = new long[partitionCount];
        for (var p = 0; p < partitionCount; p++)
        {
            var coordinator = grainFactory.GetGrain<ILeafReplayCoordinatorGrain>(
                $"{treeId}/{p}");
            heads[p] = await coordinator.GetHeadOffsetAsync(CancellationToken.None);
        }
        return heads;
    }

    /// <summary>
    /// Completes (or resumes) a split whose intent has already been persisted.
    /// Safe to call multiple times - MergeEntriesAsync is idempotent (LWW merge).
    /// On the recovery path (caller does not hold captured WAL heads) the
    /// optional <paramref name="walHeadsAtSplit"/> is omitted; the recovery
    /// branch reads the current WAL heads fresh, which is still safe - a
    /// later head only causes the sibling to skip more replay, never less.
    /// </summary>
    private async Task<SplitResult> CompleteSplitAsync(long[]? walHeadsAtSplit = null)
    {
        var splitKey = state.State.SplitKey!;
        var siblingId = state.State.SplitSiblingId!.Value;
        var donorPreSplitHigh = state.State.HighKeyExclusive;
        var newLeaf = grainFactory.GetGrain<IBPlusLeafGrain>(siblingId);

        // Fresh per-partition WAL-head reads on the recovery path.
        long[]? resolvedHeads = walHeadsAtSplit;
        if (resolvedHeads is null)
        {
            var treeId = state.State.TreeId;
            if (!string.IsNullOrEmpty(treeId))
            {
                resolvedHeads = await CaptureWalHeadsByPartitionAsync(treeId);
            }
        }

        await newLeaf.SetTreeIdAsync(state.State.TreeId!);
        if (state.State.ShardIndex is { } parentShardIndex)
        {
            await newLeaf.SetShardIndexAsync(parentShardIndex);
        }
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

        // Per-partition projection-checkpoint hint on the sibling.
        // Each partition's hint must be scoped to that partition so the
        // sibling's clamp targets the right partition's offset space.
        if (resolvedHeads is not null)
        {
            for (var p = 0; p < resolvedHeads.Length; p++)
            {
                var siblingHead = resolvedHeads[p];
                if (siblingHead > 0)
                {
                    using (LatticeApplyOffsetContext.BeginScope(p, siblingHead))
                    {
                        await newLeaf.SetCheckpointOffsetHintAsync(siblingHead);
                    }
                }
            }
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

        state.State.HighKeyExclusive = splitKey;
        state.State.OldNextSibling = null;
        state.State.SplitState = state.State.SplitState.Merge(Primitives.SplitState.SplitComplete);

        // Advance the donor's per-partition projection checkpoints to
        // the WAL heads captured at split time. Each partition's
        // SetCheckpointOffsetAsync call is scoped to that partition so
        // the per-partition clamp is applied correctly.
        if (resolvedHeads is not null)
        {
            var projection = (ILeafProjection)this;
            for (var p = 0; p < resolvedHeads.Length; p++)
            {
                var donorHead = resolvedHeads[p];
                if (donorHead > 0)
                {
                    using (LatticeApplyOffsetContext.BeginScope(p, donorHead))
                    {
                        await projection.SetCheckpointOffsetAsync(donorHead, CancellationToken.None);
                    }
                }
            }
        }

        await PublishDigestUpwardInlineAsync();

        return new SplitResult
        {
            PromotedKey = splitKey,
            NewSiblingId = siblingId,
            ChildIsLeaf = true,
        };
    }
}
