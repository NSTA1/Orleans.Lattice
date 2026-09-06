using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Empty-leaf chain reclaim: the direction the tree never had.
/// <para>
/// A leaf is allocated when a key range grows past one leaf's capacity, and
/// the split that allocates it is careful and well covered. Nothing ever gave
/// the leaf count a way back down. A range that grew to a thousand leaves and
/// was then emptied kept all thousand: every one of them an activation to
/// schedule, a state row to store, and a hop in every range scan that walks
/// the chain. The cost is paid in proportion to the high-water mark of the
/// range rather than to the rows that are actually live, and it never
/// subsides.
/// </para>
/// <para>
/// This pass folds an emptied leaf out of the chain. It is deliberately
/// conservative: it moves no data, it touches only leaves that hold no live
/// rows and carry no state that could resurrect any, and every step is
/// idempotent so a pass re-driven after a crash converges rather than
/// compounding. What it must never do is trade a slow scan for a corrupt
/// tree, so the ordering below is chosen so that no key is ever claimed by
/// two leaves at once - the WAL materialiser filters by each leaf's owned
/// span, and two overlapping spans would materialise the same record twice.
/// </para>
/// </summary>
internal sealed partial class ShardRootGrain
{
    /// <summary>
    /// Guards against two overlapping reclaim passes on one activation. A pass
    /// mutates topology, so a second concurrent pass would race the first over
    /// the same parent node. Zero means idle, one means a pass is running.
    /// </summary>
    private int _leafReclaimInProgress;

    /// <summary>
    /// Upper bound on how many leaves one reclaim pass will walk, whatever it
    /// finds. A chain that is longer than this is exactly the pathology the
    /// pass exists to shrink, so it makes progress over several passes rather
    /// than holding one activation turn for the length of a degenerate chain.
    /// It is also the cycle guard: a corrupt chain that loops back on itself
    /// terminates the walk instead of spinning.
    /// </summary>
    private const int MaxLeafReclaimWalk = 10_000;

    /// <inheritdoc />
    public async Task<int> ReclaimEmptyLeavesAsync(int maxLeaves)
    {
        if (maxLeaves <= 0) return 0;

        // A single-leaf tree has nothing to fold, and a shard with no root has
        // no chain at all. Decided by node TYPE rather than the persisted
        // RootIsLeaf flag, matching every other chain walk on this grain.
        if (state.State.RootNodeId is null) return 0;
        if (RootIsLeafTyped) return 0;

        // Structural churn at the shard level moves whole slot ranges between
        // shards and rewrites the moved-away seals this pass reads. Reclaim is
        // background tidy-up, so it yields rather than interleaving.
        if (state.State.SplitInProgress is not null) return 0;

        if (Interlocked.CompareExchange(ref _leafReclaimInProgress, 1, 0) != 0) return 0;
        try
        {
            await PrepareForOperationAsync();
            return await ReclaimEmptyLeavesCoreAsync(maxLeaves);
        }
        finally
        {
            Volatile.Write(ref _leafReclaimInProgress, 0);
        }
    }

    private async Task<int> ReclaimEmptyLeavesCoreAsync(int maxLeaves)
    {
        var headId = (await GetLeftmostLeafIdAsync())!.Value;

        var prevId = headId;
        var prevProbe = await ResolveLeafGrain(prevId).GetReclaimProbeAsync();

        var reclaimed = 0;
        var visited = 0;

        // Hoisted out of the walk: the descent path is scratch space reused
        // for every candidate rather than a fresh allocation per leaf, which
        // on a degenerate chain is the difference between one allocation and
        // thousands.
        var path = new Stack<GrainId>();

        // The head leaf is never a reclaim candidate: it owns the range below
        // the first separator in the tree and has no predecessor to inherit
        // it. The walk therefore always considers the leaf AFTER prevId.
        while (prevProbe.NextSibling is { } currentId && visited < MaxLeafReclaimWalk)
        {
            visited++;

            var currentProbe = await ResolveLeafGrain(currentId).GetReclaimProbeAsync();

            // Repair first, always, whether or not this leaf is a candidate.
            // A gap between the predecessor's high bound and this leaf's low
            // bound is the fingerprint of a reclaim that was interrupted after
            // it unrouted a leaf but before it widened the predecessor onto the
            // vacated range. Left alone, a write into that gap routes to the
            // predecessor but falls outside the span its WAL materialiser
            // accepts, so the row would survive in the cache and vanish on the
            // next projection rebuild. Closing the gap is monotonic and
            // harmless when there is nothing to close.
            await RepairRangeGapAsync(prevId, prevProbe, currentProbe);

            if (reclaimed < maxLeaves
                && IsReclaimCandidate(currentProbe)
                && await TryReclaimLeafAsync(prevId, currentId, currentProbe, path))
            {
                reclaimed++;

                // The predecessor has absorbed this leaf's range and now points
                // past it, so re-probe it and carry on from there rather than
                // stepping onto a leaf that has just been retired.
                prevProbe = await ResolveLeafGrain(prevId).GetReclaimProbeAsync();
                continue;
            }

            prevId = currentId;
            prevProbe = currentProbe;
        }

        if (reclaimed > 0)
        {
            logger.LogInformation(
                "Shard {ShardIndex} of tree '{TreeId}' reclaimed {Reclaimed} empty leaf/leaves from the leaf chain.",
                MyShardIndex,
                TreeId,
                reclaimed);
        }

        return reclaimed;
    }

    /// <summary>
    /// Whether a leaf may be folded out of the chain on the evidence of its
    /// probe alone. Every condition here is necessary but none is sufficient:
    /// the parent's shape is checked separately, because it needs a lookup
    /// this walk should not pay for a leaf that is plainly not a candidate.
    /// </summary>
    private static bool IsReclaimCandidate(in LeafReclaimProbe probe)
    {
        // Holds rows. This is the ordinary case and the cheapest rejection.
        if (probe.LiveRowCount != 0) return false;

        // Mid-split, sealed, or carrying a prepared transaction. See
        // BPlusLeafGrain.HasReclaimBlockingState for why each of these
        // outlives an empty row count.
        if (probe.HasBlockingState) return false;

        // The chain head has no predecessor to inherit its range.
        if (probe.PrevSibling is null) return false;

        // An unbounded low bound marks a leaf that owns everything below the
        // first separator, which is the head's role; a leaf with no low bound
        // that is not the head is a topology this pass does not understand,
        // and declining is free.
        if (probe.LowKeyInclusive is null) return false;

        return true;
    }

    /// <summary>
    /// Widens <paramref name="prevId"/>'s high bound up to
    /// <paramref name="currentProbe"/>'s low bound when a previous pass was
    /// interrupted between unrouting a leaf and widening its predecessor.
    /// </summary>
    private async Task RepairRangeGapAsync(
        GrainId prevId,
        LeafReclaimProbe prevProbe,
        LeafReclaimProbe currentProbe)
    {
        // A null predecessor bound already means unbounded to the right, and a
        // null successor low bound is not a bound this can widen onto.
        if (prevProbe.HighKeyExclusive is not { } prevHigh) return;
        if (currentProbe.LowKeyInclusive is not { } currentLow) return;

        // The healthy invariant is equality. Only a strictly narrower
        // predecessor is a gap; a wider one is the transient overlap a reclaim
        // creates on purpose and repairs itself by retiring the successor.
        if (string.CompareOrdinal(prevHigh, currentLow) >= 0) return;

        logger.LogInformation(
            "Shard {ShardIndex} of tree '{TreeId}' closing leaf-range gap: leaf {PrevLeaf} ended at '{PrevHigh}' while its successor starts at '{CurrentLow}'.",
            MyShardIndex,
            TreeId,
            prevId,
            prevHigh,
            currentLow);

        await ResolveLeafGrain(prevId).AbsorbSuccessorRangeAsync(currentLow);
    }

    /// <summary>
    /// Folds <paramref name="currentId"/> out of the chain, handing its range
    /// to <paramref name="prevId"/>, and returns whether it did.
    /// <para>
    /// The ordering is the whole of the safety argument, so it is worth
    /// stating plainly. Routing is retired first, so no new write can reach
    /// the leaf. The predecessor then takes over the chain link and the
    /// vacated range in a single compare-and-swap: the compare is what stops a
    /// split that landed underneath us from having its new leaf pointed past
    /// and orphaned, and the single write is what stops the predecessor ever
    /// routing a range its WAL replay filter would reject. The retired leaf's
    /// state is cleared last, once nothing can reach it by routing or by the
    /// chain. At no point is a key claimed by two leaves at once, and every
    /// step is idempotent, so an interrupted fold is finished by the next pass
    /// rather than left half-done.
    /// </para>
    /// </summary>
    private async Task<bool> TryReclaimLeafAsync(
        GrainId prevId,
        GrainId currentId,
        LeafReclaimProbe currentProbe,
        Stack<GrainId> path)
    {
        // Find the internal node that routes to this leaf by descending on the
        // leaf's own low bound.
        path.Clear();
        var routedLeafId = await ResolveWriteLeafAsync(currentProbe.LowKeyInclusive!, path);

        if (routedLeafId == currentId)
        {
            if (path.Count == 0) return false;

            var parentId = path.Peek();
            var parent = ResolveInternalGrain(parentId);

            var childIds = await parent.GetChildIdsAsync();
            var childIndex = childIds.IndexOf(currentId);

            // The leftmost child carries the null separator and is the
            // catch-all for everything below the first real separator in this
            // node, so it has no predecessor here to widen onto its range and
            // it stays. Reclaiming it would need a parent-level coalesce,
            // which is a larger change than this one.
            if (childIndex <= 0) return false;

            if (!await parent.RemoveChildAsync(currentId)) return false;

            // Every routing decision this activation has cached for the parent
            // still names the removed child. Not invalidating here would keep
            // routing writes onto a leaf that is about to be cleared.
            InvalidateRoutingTable(parentId);
        }

        // Otherwise the leaf is already unrouted: routing is a total function,
        // so a descent on a leaf's own low bound that lands anywhere else means
        // no key reaches this leaf any more. That is the fingerprint of a fold
        // interrupted after it retired routing, and the right response is to
        // finish it rather than to strand an empty leaf in the chain forever.

        // Unlink and widen in one compare-and-swap. A false return means a
        // split moved the predecessor underneath us and inserted a leaf
        // between it and this one; the fold is abandoned with nothing changed
        // on the predecessor, leaving this leaf unrouted, still chained, still
        // empty, and claimed by nobody, for the next pass to finish.
        var unlinked = await ResolveLeafGrain(prevId).TryUnlinkSuccessorAsync(
            currentId,
            currentProbe.NextSibling,
            currentProbe.HighKeyExclusive);

        if (!unlinked)
        {
            logger.LogDebug(
                "Shard {ShardIndex} of tree '{TreeId}' declined to fold leaf {LeafId}: its predecessor {PrevLeaf} no longer points at it, so a split landed underneath the reclaim.",
                MyShardIndex,
                TreeId,
                currentId,
                prevId);
            return false;
        }

        if (currentProbe.NextSibling is { } nextId)
        {
            await ResolveLeafGrain(nextId).SetPrevSiblingAsync(prevId);
        }

        // The leaf is now unreachable by routing and by the chain, so clearing
        // it is the last step and the only destructive one.
        await ResolveLeafGrain(currentId).ClearGrainStateAsync();

        _leafGrains.TryRemove(currentId, out _);

        logger.LogDebug(
            "Shard {ShardIndex} of tree '{TreeId}' reclaimed empty leaf {LeafId}; predecessor {PrevLeaf} now owns up to '{HighKey}'.",
            MyShardIndex,
            TreeId,
            currentId,
            prevId,
            currentProbe.HighKeyExclusive ?? "(unbounded)");

        return true;
    }
}
