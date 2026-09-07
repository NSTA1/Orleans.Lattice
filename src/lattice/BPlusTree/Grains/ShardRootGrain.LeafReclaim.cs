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
    /// <para>
    /// <b>The worst-case cost of one pass, stated rather than left implicit.</b>
    /// At default settings a pass probes at most 1024 leaves (see
    /// <see cref="_leafReclaimResumeLowKey"/> for the arithmetic), and on the
    /// healthy path each probe is exactly one grain call -
    /// <c>RepairRangeGapAsync</c> and the back-pointer repair both decide
    /// locally and issue nothing. So the bound is 1024 <em>sequential</em>
    /// leaf activations inside one non-reentrant turn. Warm and co-located
    /// that is sub-second; cold against remote storage, where each activation
    /// pays a state read, it is seconds. That is affordable for background
    /// tidy-up, and the writes most likely to be racing it -
    /// <c>SetManyAsync</c> and its predicated variant - are
    /// <c>[AlwaysInterleave]</c> and are not blocked by it. Plain
    /// <c>SetAsync</c> and single-key reads are, for the length of the pass.
    /// </para>
    /// <para>
    /// <b>Coupling warning.</b> That bound is a multiple of an operator-tunable
    /// knob. Raising <see cref="LatticeOptions.CompactionLeafBatchSize"/> to
    /// 625 or beyond saturates <see cref="MaxLeafReclaimWalk"/>, making a
    /// single pass walk 10,000 leaves sequentially while holding the turn -
    /// which on cold storage will exceed the Orleans response timeout of the
    /// caller and fail the pass outright. The knob reads as a compaction
    /// batch size and does not look like it controls a non-reentrant walk, so
    /// the coupling is recorded at both ends.
    /// </para>
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
    /// The one case where that trade bites, stated as a threshold an operator
    /// can actually evaluate rather than as an adjective. A pass probes at
    /// most <c>maxLeaves * LeafReclaimProbesPerFold</c> leaves from the
    /// cursor; the sole production caller is <c>TombstoneCompactionGrain</c>,
    /// which passes <see cref="LatticeOptions.CompactionLeafBatchSize"/>, so
    /// the reach is <b>1024 leaves at default settings</b> (64 x 16).
    /// <see cref="MaxLeafReclaimWalk"/> is a cycle guard and an upper clamp,
    /// not the operative number - it only binds once a caller passes 625 or
    /// more, which no default path does. Do not read 10,000 as the reach.
    /// </para>
    /// <para>
    /// The stall condition is therefore <b>distributional, not a matter of
    /// size</b>, and the distinction is the whole point: it takes more than
    /// 1024 <em>consecutive non-reclaimable</em> leaves at the head of the
    /// chain, sustained across activations, because a shard that recycles its
    /// activation between passes always restarts at the head. A large tree
    /// does not qualify; a large tree with a long contiguous run of live
    /// leaves at its head and its empties only beyond leaf 1024 does.
    /// </para>
    /// <para>
    /// Which means this bound does not undercut the feature on the trees it
    /// exists to treat. Candidates are empty leaves, and a bloated chain is by
    /// definition dense in them, so the walk meets one almost at once, folds
    /// up to <c>maxLeaves</c>, and each folded leaf leaves the chain
    /// permanently. <b>The worse the bloat, the more certainly a pass makes
    /// progress</b> - bloat is self-clearing under this design, and the stall
    /// shape is close to its opposite. Do not reason about it from leaf count:
    /// on a tree suffering this bug, leaf count is decoupled from key count by
    /// construction, so estimating one from the other assumes the pathology is
    /// absent on precisely the tree that has it. Persisting the cursor is the
    /// fix if a chain with that head-run shape is ever observed.
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
    /// <paramref name="currentProbe"/>'s low bound when two chain-adjacent
    /// leaves have stopped tiling the keyspace.
    /// <para>
    /// Read what this compares before relying on it: both operands are the
    /// DECLARED SPANS of two leaves that are still chain-adjacent, so it can
    /// only ever see a SPAN gap. It is blind to a ROUTING gap, and the
    /// distinction matters because a routing gap is the one that loses writes.
    /// A leaf that has been unrouted still sits in the chain declaring its
    /// full span, so its predecessor's high bound still equals its low bound,
    /// this method finds the invariant intact and returns having done nothing.
    /// Routing gaps are prevented by the fold ordering - the predecessor is
    /// widened onto the range before anything stops routing to the leaf - and
    /// are NOT repaired here. Nothing in the safety argument may lean on this
    /// method to sweep one up.
    /// </para>
    /// <para>
    /// What finishes an interrupted fold is <see cref="TryReclaimLeafAsync"/>'s
    /// own already-unrouted branch: a descent on the leaf's own low bound that
    /// lands elsewhere is the fingerprint of a fold interrupted after it
    /// retired routing, and the fold is then completed rather than repaired.
    /// </para>
    /// <para>
    /// That leaves this method covering span gaps only, and no fold can
    /// produce one, because the unlink and the widen are a single
    /// compare-and-swap. It is retained as a cheap invariant check on a
    /// topology this pass does not otherwise verify - it costs three local
    /// comparisons on the healthy path and no grain call - and it logs before
    /// it acts, so a span gap arriving from anywhere else is repaired loudly
    /// rather than walked past.
    /// </para>
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
    /// stating plainly. The leaf is first asked to latch itself retired, which
    /// both re-checks the emptiness this pass decided on several calls ago and
    /// stops its contents changing from that point: after it, what was
    /// measured is what will be destroyed. The predecessor then takes over the
    /// chain link and the vacated range in a single compare-and-swap: the
    /// compare is what stops a split that landed underneath us from having its
    /// new leaf pointed past and orphaned, and the single write is what stops
    /// the predecessor ever routing a range its WAL replay filter would
    /// reject. Routing is retired only after that, and the leaf's state is
    /// cleared last, once nothing can reach it by routing or by the chain.
    /// </para>
    /// <para>
    /// Routing is retired LAST, not first, and that is the whole of the
    /// safety argument. The predecessor takes over the range before anything
    /// stops routing to this leaf, so there is never a moment when a routed
    /// leaf does not declare the span being sent to it. This is the same order
    /// the SPLIT path has always used - the sibling is initialised owning
    /// [splitKey, donorHigh) and the rows are moved before the donor narrows
    /// its own high bound - so the fold's transient state is now an overlap,
    /// as split's is, rather than a gap.
    /// </para>
    /// <para>
    /// The earlier ordering retired routing first and opened exactly such a
    /// gap: a write landing in it was accepted and acknowledged by the
    /// predecessor, whose replay filter then rejected the key because its
    /// declared span had not been widened yet, so the row lived in cache and
    /// vanished on the next projection rebuild.
    /// </para>
    /// <para>
    /// Retiring routing first used to look necessary because widening the
    /// predecessor while the leaf was still routed would let a write reach the
    /// leaf while both claimed the range - a permanent duplicate, because the
    /// leaf was then no longer empty and so was never reclaimed again. The
    /// retirement latch is what voids that: the leaf is frozen and refusing
    /// writes before the widen, so it cannot accept the write that rule
    /// existed to protect against.
    /// </para>
    /// <para>
    /// Because nothing destructive precedes the compare-and-swap, a declined
    /// fold has nothing to compensate: the leaf is unlatched and the tree is
    /// exactly as it was found. Between the swap and the routing retirement
    /// the leaf is routed but latched, so a write there is refused and
    /// retried rather than lost - a bounded, visible failure in place of a
    /// silent one.
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
        // leaf's own low bound. Routing is not retired here - it is retired
        // once the predecessor owns the range - but whether it CAN be retired
        // has to be settled before the fold commits to anything, because a
        // leftmost child cannot be unrouted at all and folding one would strand
        // its range.
        path.Clear();
        var routedLeafId = await ResolveWriteLeafAsync(currentProbe.LowKeyInclusive!, path);

        GrainId? retireRoutingFrom = null;

        if (routedLeafId == currentId)
        {
            if (path.Count == 0) return false;

            var parentId = path.Peek();
            var childIds = await ResolveInternalGrain(parentId).GetChildIdsAsync();

            // The leftmost child carries the null separator and is the
            // catch-all for everything below the first real separator in this
            // node, so it has no predecessor here to widen onto its range and
            // it stays. Reclaiming it would need a parent-level coalesce,
            // which is a larger change than this one.
            if (childIds.IndexOf(currentId) <= 0) return false;

            retireRoutingFrom = parentId;
        }

        // Otherwise the leaf is already unrouted: routing is a total function,
        // so a descent on a leaf's own low bound that lands anywhere else means
        // no key reaches this leaf any more. That is the fingerprint of a fold
        // interrupted after it retired routing, and the right response is to
        // finish it rather than to strand an empty leaf in the chain forever.

        var leaf = ResolveLeafGrain(currentId);

        // The decision point. This re-runs the emptiness judgement against the
        // leaf as it is now rather than as the probe found it several calls
        // ago, and latches the leaf closed to further writes. Taking it HERE,
        // before anything destructive, is what makes the rest of the fold
        // safe: from this point the leaf's contents cannot change, so the
        // state measured is the state destroyed. Nothing has been mutated yet,
        // so a refusal simply leaves the tree as it was found.
        if (!await leaf.TryBeginRetirementAsync())
        {
            logger.LogDebug(
                "Shard {ShardIndex} of tree '{TreeId}' abandoned the fold of leaf {LeafId}: it is no longer empty, so a write reached it after the reclaim probe.",
                MyShardIndex,
                TreeId,
                currentId);

            return false;
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
                return false;
            }
        }
        catch
        {
            await leaf.AbandonRetirementAsync();
            throw;
        }

        // Past the compare-and-swap the fold has committed: the predecessor
        // owns the range and points past this leaf, so nothing that follows
        // can be undone and a failure in it must not be reported as a failed
        // fold - doing so would have the pass treat a leaf it has already
        // unlinked as still present. Each step is idempotent and is
        // re-attempted by the walk's own repair, so log and carry on.
        //
        // Retiring routing is the one step here that is not merely tidy-up:
        // until it lands the leaf is routed but latched, so its range refuses
        // writes. That is bounded and visible rather than silent, but it is
        // worth a retry rather than a single attempt.
        if (retireRoutingFrom is { } parentToRetireFrom)
        {
            await RetireRoutingAsync(parentToRetireFrom, currentId);
        }

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
    /// Retires the routing entry for a leaf the predecessor has already
    /// absorbed.
    /// <para>
    /// This runs AFTER the compare-and-swap that widens the predecessor onto
    /// the range, which is what makes it safe. Retiring routing first would
    /// leave the range routed to a predecessor that does not yet declare it,
    /// and the two admission rules disagree: the write path admits by routing
    /// and performs no span check, while WAL replay admits by declared span.
    /// A write landing in that window is appended, merged, and acknowledged,
    /// and then filtered out the moment the projection is rebuilt - durable,
    /// readable, and gone on restart.
    /// </para>
    /// <para>
    /// Doing it in this order trades that gap for a transient overlap: until
    /// this lands, the predecessor and the folded leaf both declare the range.
    /// The two are not equally bad. An overlap costs at most a double
    /// materialisation of a record, which is visible and recoverable; a gap
    /// silently destroys an acknowledged write. And here the overlap cannot
    /// even cost that much, because the leaf is latched closed by
    /// <c>TryBeginRetirementAsync</c> before the swap and is empty, so it can
    /// neither accept a new record nor replay an old one.
    /// </para>
    /// <para>
    /// A failure is retried and then logged and swallowed. The fold has
    /// already committed on the chain, so reporting it as failed would have
    /// the pass treat an unlinked leaf as still present; and the leaf is empty
    /// and latched, so a stale routing entry costs refused writes on that
    /// range until a later pass or the activation recycles, not lost ones.
    /// </para>
    /// </summary>
    private async Task RetireRoutingAsync(GrainId parentId, GrainId currentId)
    {
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                await ResolveInternalGrain(parentId).RemoveChildAsync(currentId);

                // Every routing decision this activation has cached for the
                // parent still names the removed child. Not invalidating here
                // would keep routing writes onto a leaf that has been cleared.
                InvalidateRoutingTable(parentId);
                return;
            }
            catch (Exception ex) when (attempt < MaxRetries)
            {
                logger.LogDebug(
                    ex,
                    "Shard {ShardIndex} of tree '{TreeId}' could not retire routing for folded leaf {LeafId} on attempt {Attempt}; retrying.",
                    MyShardIndex,
                    TreeId,
                    currentId,
                    attempt + 1);
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "Shard {ShardIndex} of tree '{TreeId}' folded leaf {LeafId} onto its predecessor but could not retire its routing entry; the range is served by the predecessor and the stale entry refuses writes to the empty leaf until a later pass closes it.",
                    MyShardIndex,
                    TreeId,
                    currentId);
                return;
            }
        }
    }
}
