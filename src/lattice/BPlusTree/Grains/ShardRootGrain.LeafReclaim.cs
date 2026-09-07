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

    /// <summary>
    /// How many leaves a pass may probe for each leaf it is allowed to fold.
    /// The walk has to be allowed to run past its candidates - a chain of a
    /// thousand healthy leaves with two empties at the end is the ordinary
    /// case - but it must not be allowed to run to the end of the chain every
    /// time, because probing a leaf activates it and counts its rows, and this
    /// method is not <c>[AlwaysInterleave]</c>. Left unbounded, a pass over a
    /// long chain head-of-line blocks every single-key read and write on the
    /// shard behind thousands of sequential cross-grain calls, and forces an
    /// activation of every leaf in the shard - the exact cost the feature
    /// exists to remove, worst on the degenerate chains it targets.
    /// </summary>
    private const int LeafReclaimProbesPerFold = 16;

    /// <summary>
    /// Where the next reclaim pass resumes its walk, as the low bound of the
    /// last leaf the previous pass visited, or <see langword="null"/> to start
    /// at the head of the chain.
    /// <para>
    /// Bounding the walk without a resume position would make successive
    /// passes re-walk the same prefix forever, so a chain longer than one
    /// pass's budget would never have its tail reclaimed at all. The cursor is
    /// per-activation on purpose: it steers where a pass starts and never what
    /// it does, so losing it on a reactivation costs one re-walked prefix and
    /// can never affect correctness. Persisting it would buy little and add a
    /// state field that has to be migrated.
    /// </para>
    /// <para>
    /// The one case where that trade bites: on a shard whose first
    /// <see cref="MaxLeafReclaimWalk"/> leaves all hold live rows, a pass that
    /// restarts from the leftmost leaf never reaches the candidates beyond
    /// them, so a very large sparse shard that recycles its activation faster
    /// than it drains stops making progress. It is self-limiting in the
    /// ordinary case, because a folded leaf leaves the chain and is not
    /// re-walked. Persisting the cursor is the fix if that shape is ever
    /// observed.
    /// </para>
    /// </summary>
    private string? _leafReclaimResumeLowKey;

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
        // Hoisted out of the walk: the descent path is scratch space reused
        // for every candidate rather than a fresh allocation per leaf, which
        // on a degenerate chain is the difference between one allocation and
        // thousands.
        var path = new Stack<GrainId>();

        var (prevId, prevProbe) = await StartLeafReclaimWalkAsync(path);

        var reclaimed = 0;
        var visited = 0;

        // The walk is allowed to run past its candidates, but not to the end
        // of a long chain: every probe activates a leaf and counts its rows,
        // and this pass holds the shard root's activation turn while it does.
        // See LeafReclaimProbesPerFold. The arithmetic is widened because
        // int.MaxValue is an ordinary argument here - it is how a caller asks
        // for an unbounded pass - and multiplying it would wrap negative and
        // silently clamp the walk to almost nothing.
        var visitBudget = (int)Math.Clamp(
            (long)maxLeaves * LeafReclaimProbesPerFold,
            LeafReclaimProbesPerFold,
            MaxLeafReclaimWalk);

        // The head leaf is never a reclaim candidate: it owns the range below
        // the first separator in the tree and has no predecessor to inherit
        // it. The walk therefore always considers the leaf AFTER prevId.
        while (prevProbe.NextSibling is { } currentId && visited < visitBudget)
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

            // A back pointer that does not name the predecessor we walked in
            // from is the fingerprint of a fold that unlinked a leaf and then
            // failed before it could re-point the successor. Nothing reads the
            // back pointer to route, so it is not a correctness bug on its
            // own, but leaving it dangling at a retired leaf hides the
            // interruption from every later walk.
            if (currentProbe.PrevSibling != prevId)
            {
                await ResolveLeafGrain(currentId).SetPrevSiblingAsync(prevId);
            }

            if (reclaimed < maxLeaves
                && IsReclaimCandidate(currentProbe)
                && await TryReclaimLeafAsync(prevId, currentId, currentProbe, path))
            {
                reclaimed++;

                // The predecessor has absorbed this leaf's range and now points
                // past it, so re-probe it and carry on from there rather than
                // stepping onto a leaf that has just been retired.
                prevProbe = await ResolveLeafGrain(prevId).GetReclaimProbeAsync();

                // Budget spent. Stop walking, not just folding: probing the
                // rest of the chain would activate every remaining leaf and
                // count its rows for a decision this pass can no longer act
                // on.
                if (reclaimed == maxLeaves) break;

                continue;
            }

            prevId = currentId;
            prevProbe = currentProbe;
        }

        // Record where to resume. A walk that ran out of chain reached the
        // tail, so the next pass starts at the head again and re-examines
        // whatever has emptied since. A walk that stopped on a budget has
        // chain left to its right, and resuming there is what stops successive
        // passes re-walking the same prefix forever and never reaching the
        // tail of a chain longer than one pass's budget.
        _leafReclaimResumeLowKey = prevProbe.NextSibling is null
            ? null
            : prevProbe.HighKeyExclusive;

        if (reclaimed > 0)
        {
            logger.LogInformation(
                "Shard {ShardIndex} of tree '{TreeId}' reclaimed {Reclaimed} empty leaf/leaves from the leaf chain after probing {Visited}.",
                MyShardIndex,
                TreeId,
                reclaimed,
                visited);
        }

        return reclaimed;
    }

    /// <summary>
    /// Chooses the leaf a pass starts from: the recorded resume position when
    /// the previous pass stopped short of the chain tail, and the head of the
    /// chain otherwise.
    /// <para>
    /// The resume position is a key rather than a leaf identity on purpose. A
    /// leaf id recorded by one pass may have been folded away, split, or
    /// migrated by the time the next pass runs, whereas routing on a key is a
    /// total function and always lands on whichever leaf owns that span now.
    /// </para>
    /// </summary>
    private async Task<(GrainId PrevId, LeafReclaimProbe PrevProbe)> StartLeafReclaimWalkAsync(
        Stack<GrainId> path)
    {
        if (_leafReclaimResumeLowKey is { } resumeKey)
        {
            try
            {
                path.Clear();
                var resumeId = await ResolveWriteLeafAsync(resumeKey, path);
                var resumeProbe = await ResolveLeafGrain(resumeId).GetReclaimProbeAsync();
                return (resumeId, resumeProbe);
            }
            catch (Exception ex)
            {
                // A resume position is an optimisation, never a requirement.
                // Falling back to the head costs one re-walked prefix.
                logger.LogDebug(
                    ex,
                    "Shard {ShardIndex} of tree '{TreeId}' could not resume its leaf-reclaim walk at '{ResumeKey}'; restarting from the head of the chain.",
                    MyShardIndex,
                    TreeId,
                    resumeKey);
                _leafReclaimResumeLowKey = null;
            }
        }

        var headId = (await GetLeftmostLeafIdAsync())!.Value;
        return (headId, await ResolveLeafGrain(headId).GetReclaimProbeAsync());
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
    /// the leaf. The leaf is then asked to latch itself retired, which both
    /// re-checks the emptiness this pass decided on several calls ago and
    /// stops its contents changing from that point: after it, what was
    /// measured is what will be destroyed. Only then does the predecessor take
    /// over the chain link and the vacated range in a single compare-and-swap:
    /// the compare is what stops a split that landed underneath us from having
    /// its new leaf pointed past and orphaned, and the single write is what
    /// stops the predecessor ever routing a range its WAL replay filter would
    /// reject. The retired leaf's state is cleared last, once nothing can
    /// reach it by routing or by the chain.
    /// </para>
    /// <para>
    /// Every step between retiring routing and completing the fold is
    /// compensated, because each leaves the tree in a state where the vacated
    /// range is owned by nobody: a write into it would route to a leaf whose
    /// replay filter rejects it, so the row would live in the cache and vanish
    /// on the next projection rebuild. The compensation restores the leaf's
    /// routing rather than widening a neighbour onto the gap, because
    /// restoring returns the tree to exactly its pre-fold state, whereas
    /// widening would leave two leaves declaring one span and both
    /// materialising the same WAL records.
    /// </para>
    /// <para>
    /// At no point is a key claimed by two leaves at once, and every step is
    /// idempotent, so an interrupted fold is finished by the next pass rather
    /// than left half-done.
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

        // Set once routing has been retired by THIS pass, which is what makes
        // the pass responsible for putting it back if it cannot finish. A leaf
        // that was already unrouted on entry was retired by an earlier,
        // interrupted pass, and abandoning it again simply leaves it as it was
        // found.
        GrainId? routingRetiredFrom = null;

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

            routingRetiredFrom = parentId;

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

        var leaf = ResolveLeafGrain(currentId);

        try
        {
            // The decision point. This re-runs the emptiness judgement against
            // the leaf as it is now rather than as the probe found it several
            // calls ago, and latches the leaf closed to further writes. Taking
            // it HERE, before anything destructive, is what makes the rest of
            // the fold safe: from this point the leaf's contents cannot change,
            // so the state measured is the state destroyed.
            if (!await leaf.TryBeginRetirementAsync())
            {
                logger.LogDebug(
                    "Shard {ShardIndex} of tree '{TreeId}' abandoned the fold of leaf {LeafId}: it is no longer empty, so a write reached it after the reclaim probe.",
                    MyShardIndex,
                    TreeId,
                    currentId);

                await RestoreRetiredRoutingAsync(routingRetiredFrom, currentId, currentProbe);
                return false;
            }
        }
        catch
        {
            await RestoreRetiredRoutingAsync(routingRetiredFrom, currentId, currentProbe);
            throw;
        }

        try
        {
            // Unlink and widen in one compare-and-swap. A false return means a
            // split moved the predecessor underneath us and inserted a leaf
            // between it and this one; the fold is abandoned with nothing
            // changed on the predecessor.
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

                await leaf.AbandonRetirementAsync();
                await RestoreRetiredRoutingAsync(routingRetiredFrom, currentId, currentProbe);
                return false;
            }
        }
        catch
        {
            await leaf.AbandonRetirementAsync();
            await RestoreRetiredRoutingAsync(routingRetiredFrom, currentId, currentProbe);
            throw;
        }

        // Past the compare-and-swap the fold has committed: the predecessor
        // owns the range and points past this leaf, so the leaf is unreachable
        // by routing and by the chain alike and there is nothing left to
        // compensate. Everything that follows is tidy-up, and a failure in it
        // must not be reported as a failed fold - doing so would have the pass
        // treat a leaf it has already unlinked as still present. Each step is
        // idempotent and is re-attempted by the walk's own repair, so log and
        // carry on.
        try
        {
            if (currentProbe.NextSibling is { } nextId)
            {
                await ResolveLeafGrain(nextId).SetPrevSiblingAsync(prevId);
            }

            await leaf.ClearGrainStateAsync();
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Shard {ShardIndex} of tree '{TreeId}' folded leaf {LeafId} out of the chain but could not finish tidying up after it; the leaf is unrouted and unlinked, and the next pass will finish the job.",
                MyShardIndex,
                TreeId,
                currentId);
        }

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

    /// <summary>
    /// Puts back the routing entry this pass retired, when the fold that
    /// retired it could not be completed.
    /// <para>
    /// Between retiring a leaf's routing and widening its predecessor onto the
    /// vacated range, that range is owned by nobody: a descent on a key in it
    /// lands on whichever leaf now covers the span, and that leaf's WAL replay
    /// filter rejects the key because it falls outside the range the leaf
    /// declares. A write into the gap would therefore be acknowledged, served
    /// from the cache, and then lost at the next projection rebuild. The
    /// walk's own <see cref="RepairRangeGapAsync"/> cannot close this one,
    /// because it compares CHAIN neighbours and on an abandoned fold the chain
    /// still tiles perfectly - the hole exists only in routing.
    /// </para>
    /// <para>
    /// Restoring the separator is the exact inverse of removing it, and is
    /// preferred over widening a neighbour onto the gap: widening would leave
    /// two leaves declaring one span, and since replay admits a record by
    /// span, both would materialise the same records. Restoring instead
    /// returns the tree to its pre-fold state, and the next pass retries the
    /// fold from scratch.
    /// </para>
    /// <para>
    /// A failure to compensate is logged and swallowed. It leaves the gap the
    /// caller was already living with, and throwing here would replace the
    /// caller's own outcome - including its exception - with this one.
    /// </para>
    /// </summary>
    private async Task RestoreRetiredRoutingAsync(
        GrainId? parentId,
        GrainId currentId,
        LeafReclaimProbe currentProbe)
    {
        if (parentId is not { } parent) return;
        if (currentProbe.LowKeyInclusive is not { } separator) return;

        try
        {
            await ResolveInternalGrain(parent).AcceptSplitAsync(separator, currentId);
            InvalidateRoutingTable(parent);

            logger.LogDebug(
                "Shard {ShardIndex} of tree '{TreeId}' restored routing for leaf {LeafId} at '{Separator}' after abandoning its fold.",
                MyShardIndex,
                TreeId,
                currentId,
                separator);
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "Shard {ShardIndex} of tree '{TreeId}' abandoned the fold of leaf {LeafId} but could not restore its routing entry at '{Separator}'; the range is unrouted until a later pass or repair closes it.",
                MyShardIndex,
                TreeId,
                currentId,
                separator);
        }
    }
}
