using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Operator-invoked repair for a leaf that is in the sibling chain but not in
/// the tree.
/// <para>
/// An interrupted split can leave behind a leaf that was spliced into the
/// chain before its parent learned about it: doubly linked to live neighbours,
/// durable, holding a WAL materialiser pin, and reachable by no descent from
/// the shard root. Issue 3265 closed the hole that minted one. It could not,
/// by construction, do anything for a tree that already had one, and that is
/// what this file is (issue 3269).
/// </para>
/// <para>
/// <b>Why an orphan is not a cosmetic defect.</b> Every leaf publishes a WAL
/// materialiser pin, and the trim floor is the minimum over all of them.
/// Nothing routes to an orphan, so it never checkpoints, so its pin never
/// advances - and the floor is pinned with it. The WAL then never trims, and
/// because compaction is strictly downstream of trim it never compacts, so one
/// orphan removes every bound on a tree's WAL growth, permanently. The only
/// remedy before this pass existed was to rebuild the tree.
/// </para>
/// <para>
/// <b>Why the empty-leaf reclaim pass cannot do this.</b> Two independent
/// reasons, and both have to be answered:
/// <list type="number">
/// <item><description><c>IsReclaimCandidate</c> rejects on its FIRST line,
/// <c>probe.LiveRowCount != 0</c>, and the descent-reachability test sits
/// downstream of that inside <c>TryReclaimLeafAsync</c>. A non-empty
/// descent-unreachable leaf is therefore invisible to reclaim forever - the
/// emptiness gate fires before anything ever looks at reachability.</description></item>
/// <item><description>An orphan is characteristically NOT empty. Rows
/// materialise at activation by replaying the WAL through
/// <c>ShouldApplyDuringReplay</c>, whose predicate keys on
/// <c>(ShardIndex, LowKeyInclusive, HighKeyExclusive)</c> and never on leaf
/// identity, so an orphan sharing bounds with a live leaf materialises a full
/// shadow copy of that leaf's range. The gate does not merely happen to fire
/// first; it fires on every orphan there is.</description></item>
/// </list>
/// Reordering reclaim's own gates was rejected: reclaim derives its entire
/// safety from emptiness - what it measures is what it destroys - and an
/// unreachable leaf needs a different argument, not a relaxed one.
/// </para>
/// <para>
/// <b>The replacement argument, and it fails closed.</b> Before this pass
/// unsplices anything it proves two things. First, that no descent from the
/// shard root reaches the leaf, which makes its row set frozen: routing is a
/// total function, so if a descent on a key lands elsewhere then no write can
/// ever be routed here. Second, key by key, that every row the leaf holds is
/// readable from the descent-reachable leaf that owns it, which makes clearing
/// the leaf lossless. A leaf that fails any check is left exactly as found and
/// reported as a refusal; it is never repaired on a partial proof, and it is
/// never repaired on the assumption that an orphan's keys are duplicated
/// somewhere. That assumption looks safe - the one orphan pair ever measured
/// did hold keys duplicated on live leaves - but it is a sample of one and
/// nothing in the mechanism that mints an orphan guarantees it.
/// </para>
/// <para>
/// <b>What this pass deliberately does not do.</b> It does not widen the
/// predecessor onto the removed range, which is the one place it diverges from
/// reclaim and the divergence most likely to be "corrected" back into a bug.
/// In reclaim the folded leaf IS routed, so handing its range to the
/// predecessor is what keeps the keyspace tiled. An orphan is NOT routed, so
/// the routed leaves already tile the keyspace completely between them;
/// widening the predecessor onto an orphan's range would make the predecessor
/// overlap a LIVE leaf, and two leaves declaring one range materialise every
/// record in it twice. For the same reason it retires no routing entry: there
/// is no routing entry to retire, which is the definition of the leaf it is
/// removing.
/// </para>
/// </summary>
internal sealed partial class ShardRootGrain
{
    /// <summary>
    /// Guards against two overlapping repair passes on one activation, exactly
    /// as <c>_leafReclaimInProgress</c> does for reclaim. Zero means idle.
    /// </summary>
    private int _orphanRepairInProgress;

    /// <summary>
    /// Upper bound on how many leaves one repair pass will walk. Bounded for
    /// the same reason every other chain walk on this grain is - probing a
    /// leaf activates it, and this method holds the shard root's non-reentrant
    /// turn while it runs - and it doubles as the cycle guard against a chain
    /// that loops back on itself.
    /// </summary>
    private const int MaxOrphanRepairWalk = 4_096;

    /// <summary>
    /// The most keys this pass will verify on one orphan before refusing it.
    /// <para>
    /// This is a safety bound, not a performance knob, and refusing is the
    /// point of it. A leaf holds at most one leaf's worth of rows, so an
    /// orphan presenting far more than that is not the topology this pass
    /// models, and a pass that cannot enumerate a leaf completely cannot prove
    /// anything about the keys it did not reach. Refusing loudly leaves an
    /// operator with a pinned WAL and a report; proceeding on a partial
    /// enumeration would silently delete whatever fell outside it.
    /// </para>
    /// </summary>
    private const int MaxOrphanKeysVerified = 100_000;

    /// <inheritdoc />
    public Task<OrphanedLeafRepairPage> RepairOrphanedLeavesAsync(
        string? resumeFromInclusive,
        bool dryRun,
        CancellationToken cancellationToken = default) =>
        RunOrphanedLeafPassAsync(resumeFromInclusive, dryRun, survey: false, cancellationToken);

    /// <inheritdoc />
    public Task<OrphanedLeafRepairPage> SurveyOrphanedLeavesAsync(
        string? resumeFromInclusive,
        CancellationToken cancellationToken = default) =>
        RunOrphanedLeafPassAsync(resumeFromInclusive, dryRun: true, survey: true, cancellationToken);

    private async Task<OrphanedLeafRepairPage> RunOrphanedLeafPassAsync(
        string? resumeFromInclusive,
        bool dryRun,
        bool survey,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Decided by node TYPE rather than the persisted RootIsLeaf flag,
        // matching every other chain walk on this grain. A single-leaf tree has
        // one leaf and it is the root, so it cannot be unreachable from itself.
        if (state.State.RootNodeId is null) return OrphanedLeafRepairPage.Empty;
        if (RootIsLeafTyped) return OrphanedLeafRepairPage.Empty;

        // A shard-level split moves whole slot ranges between shards while it
        // runs, so a descent can legitimately land off this shard and would
        // read as unreachability. Yield rather than interleave: a repair pass
        // is operator-initiated and can be re-run, and a false positive here
        // deletes a live leaf.
        if (state.State.SplitInProgress is not null)
        {
            return OrphanedLeafRepairPage.Declined(
                MyShardIndex, OrphanedLeafAuditGapReason.ShardSplitInProgress);
        }

        if (Interlocked.CompareExchange(ref _orphanRepairInProgress, 1, 0) != 0)
        {
            return OrphanedLeafRepairPage.Declined(
                MyShardIndex, OrphanedLeafAuditGapReason.ShardPassAlreadyRunning);
        }

        try
        {
            // Start the deadline where the pass starts HOLDING the turn rather
            // than where it reaches the loop; preparing the grain and resolving
            // the resume position are both runs of grain calls on a cold
            // activation. Same reasoning as reclaim (issue 1992).
            var startTimestamp = LeafWalkBudget.StartClock();

            await PrepareForOperationAsync();

            if (dryRun)
            {
                return await RepairOrphanedLeavesCoreAsync(
                    resumeFromInclusive,
                    dryRun,
                    survey,
                    startTimestamp,
                    cancellationToken);
            }

            // A repairing pass also settles clears owed by earlier removals -
            // its own unsplices and reclaim's folds alike. Those leaves are off
            // the chain, so no walk reaches them again. Skipped on a dry run,
            // which mutates nothing.
            await RetryPendingLeafClearsAsync();

            try
            {
                return await RepairOrphanedLeavesCoreAsync(
                    resumeFromInclusive,
                    dryRun,
                    survey,
                    startTimestamp,
                    cancellationToken);
            }
            finally
            {
                await FlushPendingLeafClearsAsync();
            }
        }
        finally
        {
            Volatile.Write(ref _orphanRepairInProgress, 0);
        }
    }

    private async Task<OrphanedLeafRepairPage> RepairOrphanedLeavesCoreAsync(
        string? resumeFromInclusive,
        bool dryRun,
        bool survey,
        long startTimestamp,
        CancellationToken cancellationToken)
    {
        // Scratch space reused for every descent rather than a fresh
        // allocation per leaf, and the per-key verification loop below reuses
        // it too - on a full leaf that is the difference between one
        // allocation and a leaf's worth of them.
        var path = new Stack<GrainId>();

        var options = await GetOptionsAsync();
        var budget = LeafWalkBudget.ForBackgroundDrain(MaxOrphanRepairWalk, options, startTimestamp);

        var (prevId, prevProbe) = await StartOrphanRepairWalkAsync(resumeFromInclusive, path);

        var findings = new List<OrphanedLeafFinding>();
        var gaps = new List<OrphanedLeafAuditGap>();

        // Where the NEXT page resumes. It is deliberately the low bound of the
        // last leaf this pass proved DESCENT-REACHABLE, not the predecessor's
        // high bound that reclaim records, and the difference is load-bearing.
        // Reclaim resumes by descending on the high bound of the leaf it
        // stopped on, which lands on whichever leaf owns the range beyond it -
        // and an orphan spliced in between is not that leaf, so resuming that
        // way would step straight OVER exactly the leaves this pass exists to
        // find. Descending on a reachable leaf's own low bound lands back on
        // that leaf by definition, so the walk restarts where it stopped and
        // the successor it examines next is the one it had not reached.
        //
        // Null until the pass proves some leaf reachable, which also makes the
        // head's unbounded low bound a non-case rather than a special case.
        var resumeFrom = await AdmitWalkEntryAsync(prevId, prevProbe, gaps, path);

        // The furthest key this page has re-entered the chain on. A severed
        // pointer is re-entered by descending on the severed leaf's high
        // bound, and a descent is only trustworthy here if it advances: the
        // keyspace is consumed monotonically, so a re-entry that lands at or
        // behind the last one is a topology that would drive the walk in a
        // circle rather than past the break.
        string? lastReEntryKey = null;

        var reachedEnd = false;

        while (!budget.ShouldYield())
        {
            if (prevProbe.NextSibling is not { } currentId)
            {
                // The chain says it ends here. That is believed only after it
                // is checked against a descent to the shard's rightmost leaf,
                // because an unvalidated null successor is exactly how this
                // walk used to report a whole shard clean while range scans -
                // which enter the chain by descent rather than from its head -
                // kept finding orphans past the break (issue 3301).
                var reEntry = await TryReEnterChainPastBreakAsync(
                    prevId, prevProbe, lastReEntryKey, gaps, path);

                if (reEntry is not { } resumed)
                {
                    reachedEnd = true;
                    break;
                }

                lastReEntryKey = resumed.ReEntryKey;
                prevId = resumed.Id;
                prevProbe = resumed.Probe;

                // A re-entry leaf that is itself reachable becomes the resume
                // position, so the next page starts past the break instead of
                // re-walking the whole prefix to reach it again. One that is
                // not leaves the position where it was, which costs a repeated
                // prefix and never a skipped leaf.
                resumeFrom = await AdmitWalkEntryAsync(prevId, prevProbe, gaps, path) ?? resumeFrom;
                continue;
            }
            cancellationToken.ThrowIfCancellationRequested();
            budget.RecordLeafVisited();

            var currentProbe = await ResolveLeafGrain(currentId).GetReclaimProbeAsync();

            if (currentProbe.LowKeyInclusive is null)
            {
                // No low bound means no key to descend on, so reachability
                // cannot be decided. That is the chain head's role and this
                // walk can reach it only on a topology it does not model;
                // either way, declining to judge is free and the alternative
                // is judging without evidence.
                //
                // Declining is NOT free to leave unsaid, though. The
                // range-scan chain guard judges this same population the other
                // way - a leaf declaring a trailing edge with no leading edge
                // claims the keyspace from the unbounded end, and it is
                // treated as a regression - so a silent skip here is the audit
                // disagreeing with the running system in the one direction
                // that causes no action (issue 3301).
                gaps.Add(new OrphanedLeafAuditGap
                {
                    ShardIndex = MyShardIndex,
                    Reason = OrphanedLeafAuditGapReason.LeafBoundsUndecidable,
                    LeafId = currentId.ToString(),
                    KeyHint = currentProbe.HighKeyExclusive,
                });

                prevId = currentId;
                prevProbe = currentProbe;
                continue;
            }

            if (await IsDescentReachableAsync(currentId, currentProbe, path))
            {
                resumeFrom = currentProbe.LowKeyInclusive;
                prevId = currentId;
                prevProbe = currentProbe;
                continue;
            }

            var finding = await ExamineOrphanAsync(
                prevId,
                prevProbe,
                currentId,
                currentProbe,
                dryRun,
                survey,
                path,
                cancellationToken);

            findings.Add(finding);

            if (finding.Disposition == OrphanedLeafDisposition.Repaired)
            {
                // The predecessor now points past the removed leaf, so re-probe
                // it and carry on from there rather than stepping onto a leaf
                // that has just been cleared.
                prevProbe = await ResolveLeafGrain(prevId).GetReclaimProbeAsync();
                continue;
            }

            // Every other disposition changed nothing, so the leaf is still in
            // the chain and the walk steps over it. Note it does NOT become the
            // resume point: resuming on an unreachable leaf's low bound would
            // descend to somewhere else entirely.
            prevId = currentId;
            prevProbe = currentProbe;
        }

        var truncated = gaps.Exists(
            g => g.Reason == OrphanedLeafAuditGapReason.ChainTruncatedUnrecoverable);

        var stopReason =
            truncated ? "chain-truncated"
            : reachedEnd ? "end-of-chain"
            : budget.LeavesVisited >= MaxOrphanRepairWalk ? "walk-budget"
            : "deadline";

        if (!reachedEnd && resumeFrom is null)
        {
            // The pass stopped with chain to its right but never proved a
            // single leaf reachable, so it has no position to name and the
            // caller must not be told to start again from the head - that
            // would drive an unterminating loop over the same prefix. Report
            // completion and say loudly why it is not one: a whole budget of
            // consecutive unreachable or unbounded leaves is not a topology
            // this pass models, and an operator needs to see that rather than
            // watch a drain spin.
            //
            // Saying it loudly in the LOG was not enough. The drive reads the
            // null resume position as completion, so the report this feeds was
            // a clean one - the log and the returned verdict disagreed, and
            // only the log was right (issue 3301). The gap puts the same fact
            // where the caller actually reads it.
            gaps.Add(new OrphanedLeafAuditGap
            {
                ShardIndex = MyShardIndex,
                Reason = OrphanedLeafAuditGapReason.WalkBudgetExhaustedWithoutResumePosition,
                LeafId = prevId.ToString(),
                KeyHint = prevProbe.HighKeyExclusive,
            });

            logger.LogWarning(
                "Shard {ShardIndex} of tree '{TreeId}' stopped its orphaned-leaf repair walk after {Visited} leaves without "
                + "finding a descent-reachable leaf to resume from, so the remainder of the chain was not examined.",
                MyShardIndex,
                TreeId,
                budget.LeavesVisited);
        }

        // Every pass reports, including the ones that find nothing - the same
        // reasoning that removed reclaim's `reclaimed > 0` gate. A fruitless
        // pass is the EXPENSIVE one here, because it walks its entire budget
        // and returns; gating the log on findings would make the instrument
        // anti-correlated with cost.
        logger.LogInformation(
            "Shard {ShardIndex} of tree '{TreeId}' finished an orphaned-leaf {Mode} pass in {ElapsedMs}ms: "
            + "walked {Visited} leaves, repaired {Repaired}, refused {Refused}, unexamined regions {Gaps}, "
            + "stopped on {StopReason}.",
            MyShardIndex,
            TreeId,
            dryRun ? "inspection" : "repair",
            (long)Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds,
            budget.LeavesVisited,
            findings.Count(f => f.Disposition == OrphanedLeafDisposition.Repaired),
            findings.Count(f => f.IsRefusal),
            gaps.Count,
            stopReason);

        return new OrphanedLeafRepairPage
        {
            LeavesWalked = budget.LeavesVisited,
            Findings = findings,
            Gaps = gaps,
            ResumeFromInclusive = reachedEnd ? null : resumeFrom,
        };
    }

    /// <summary>
    /// Admits the leaf a walk segment starts on - the chain head, a resume
    /// position, or a re-entry past a break - and returns the key the next
    /// page may resume from, or <see langword="null"/> when this leaf gives
    /// the walk no position to name.
    /// <para>
    /// The walk only ever examines a leaf's SUCCESSOR, because an unsplice
    /// swings a live predecessor's pointer and an entry leaf has no
    /// predecessor within reach. An entry leaf that is itself unreachable by
    /// descent is therefore an orphan the pass can see and cannot act on, and
    /// before issue 3301 it was neither reported nor repaired. It is reported
    /// as a gap rather than a finding because a finding asserts a disposition
    /// the pass never reached.
    /// </para>
    /// <para>
    /// A null low bound is the chain head's ordinary shape - it owns the
    /// keyspace from the unbounded end - so it is not judged and not reported.
    /// </para>
    /// </summary>
    private async Task<string?> AdmitWalkEntryAsync(
        GrainId entryId,
        LeafReclaimProbe entryProbe,
        List<OrphanedLeafAuditGap> gaps,
        Stack<GrainId> path)
    {
        if (entryProbe.LowKeyInclusive is not { } low) return null;

        if (await IsDescentReachableAsync(entryId, entryProbe, path)) return low;

        gaps.Add(new OrphanedLeafAuditGap
        {
            ShardIndex = MyShardIndex,
            Reason = OrphanedLeafAuditGapReason.EntryLeafUnreachable,
            LeafId = entryId.ToString(),
            KeyHint = low,
        });

        return null;
    }

    /// <summary>
    /// Decides whether a null successor really is the end of the shard's leaf
    /// chain, and when it is not, finds a way back into the chain past the
    /// break (issue 3301).
    /// <para>
    /// <b>Why an unvalidated null successor was the defect.</b> This walk
    /// enumerates candidates from the head of the sibling chain, which is the
    /// same structure an orphan damages. A pointer severed part-way across the
    /// keyspace therefore ends the walk, and because the drive reads a null
    /// resume position as completion, the shard was reported examined and
    /// clean. Range scans are not anchored that way - they enter the chain by
    /// descending on their own lower bound - so they reach the segment past
    /// the break and keep reporting the orphans in it. That is how an audit
    /// returning no findings and a tree emitting thousands of chain-repair
    /// warnings a minute were both telling the truth about the same shard.
    /// </para>
    /// <para>
    /// <b>The check is invariant-free.</b> Rather than assume the terminal
    /// leaf must declare an unbounded high bound, it descends to the shard's
    /// rightmost leaf and compares identities. That asks the tree where the
    /// keyspace ends instead of asking the chain, which is the half of the
    /// topology that is not in question.
    /// </para>
    /// <para>
    /// <b>Re-entry advances or it does not happen.</b> The way back in is a
    /// descent on the severed leaf's high bound. A descent that lands on the
    /// severed leaf itself, or on a key at or behind a previous re-entry, is
    /// a topology that would drive the walk in a circle rather than past the
    /// break, so it is refused and reported. A refused re-entry leaves the
    /// remainder of the shard unexamined, which is exactly what the gap says.
    /// </para>
    /// </summary>
    private async Task<(GrainId Id, LeafReclaimProbe Probe, string ReEntryKey)?> TryReEnterChainPastBreakAsync(
        GrainId terminalId,
        LeafReclaimProbe terminalProbe,
        string? lastReEntryKey,
        List<OrphanedLeafAuditGap> gaps,
        Stack<GrainId> path)
    {
        var rightmostId = await TraverseToRightmostLeafAsync();

        // The chain ended on the leaf the tree itself ends on. Nothing is
        // missing, and this is the healthy path that must stay free of gaps.
        if (rightmostId == terminalId) return null;

        void ReportUnrecoverable(string? keyHint) => gaps.Add(new OrphanedLeafAuditGap
        {
            ShardIndex = MyShardIndex,
            Reason = OrphanedLeafAuditGapReason.ChainTruncatedUnrecoverable,
            LeafId = terminalId.ToString(),
            KeyHint = keyHint,
        });

        if (!await IsDescentReachableAsync(terminalId, terminalProbe, path))
        {
            // The chain ended on a leaf nothing routes to, so its declared
            // high bound is not a trustworthy key to descend on - it is the
            // orphan's own idea of where it ends, and the orphan is precisely
            // the thing whose bounds disagree with the tree.
            //
            // This arm also stops the re-entry from looping. On an inspection
            // the orphan is still spliced when the walk steps over it, so a
            // re-entry taken from it would land back on its live predecessor
            // and re-examine it, reporting the same leaf twice. A repair
            // removes the orphan first, so its predecessor becomes the
            // terminal leaf and the ordinary reachable path applies.
            ReportUnrecoverable(terminalProbe.HighKeyExclusive);
            return null;
        }

        if (terminalProbe.HighKeyExclusive is not { } reEntryKey)
        {
            // A severed leaf claiming the keyspace to the unbounded end gives
            // no key to descend on, so there is no way back in.
            ReportUnrecoverable(null);
            return null;
        }

        if (lastReEntryKey is not null
            && string.CompareOrdinal(reEntryKey, lastReEntryKey) <= 0)
        {
            ReportUnrecoverable(reEntryKey);
            return null;
        }

        path.Clear();
        var nextId = await ResolveWriteLeafAsync(reEntryKey, path);

        if (nextId == terminalId)
        {
            ReportUnrecoverable(reEntryKey);
            return null;
        }

        var nextProbe = await ResolveLeafGrain(nextId).GetReclaimProbeAsync();

        // Re-entry succeeded, and the severed pointer is still a defect the
        // operator has to know about: this pass does not repair it, it walks
        // around it.
        gaps.Add(new OrphanedLeafAuditGap
        {
            ShardIndex = MyShardIndex,
            Reason = OrphanedLeafAuditGapReason.ChainTruncated,
            LeafId = terminalId.ToString(),
            KeyHint = reEntryKey,
        });

        return (nextId, nextProbe, reEntryKey);
    }

    /// <summary>
    /// Whether a descent from the shard root on this leaf's own low bound
    /// lands back on this leaf.
    /// <para>
    /// This is reclaim's own argument, reused rather than reinvented: routing
    /// is a total function, so a descent on a leaf's low bound that lands
    /// anywhere else means no key reaches this leaf. The descent is read-only,
    /// so asking the question costs nothing but grain calls.
    /// </para>
    /// </summary>
    private async Task<bool> IsDescentReachableAsync(
        GrainId leafId,
        LeafReclaimProbe probe,
        Stack<GrainId> path)
    {
        if (probe.LowKeyInclusive is null) return false;

        path.Clear();
        return await ResolveWriteLeafAsync(probe.LowKeyInclusive, path) == leafId;
    }

    /// <summary>
    /// Chooses the leaf a page starts from: the caller's resume position when
    /// it has one, and the head of the chain otherwise.
    /// <para>
    /// The position is a key rather than a leaf identity for reclaim's reason -
    /// a leaf id recorded by one page may be gone by the time the next runs,
    /// whereas routing on a key always lands on whichever leaf owns that span
    /// now - and specifically a key the previous page proved REACHABLE, so the
    /// descent lands back on the leaf the walk stopped on.
    /// </para>
    /// </summary>
    private async Task<(GrainId PrevId, LeafReclaimProbe PrevProbe)> StartOrphanRepairWalkAsync(
        string? resumeFromInclusive,
        Stack<GrainId> path)
    {
        if (resumeFromInclusive is { } resumeKey)
        {
            try
            {
                path.Clear();
                var resumeId = await ResolveWriteLeafAsync(resumeKey, path);
                return (resumeId, await ResolveLeafGrain(resumeId).GetReclaimProbeAsync());
            }
            catch (Exception ex)
            {
                // A resume position is an optimisation, never a requirement.
                // Falling back to the head costs one re-walked prefix, and the
                // pass is idempotent over it: a leaf already repaired is gone
                // from the chain, and a leaf already refused is refused again.
                logger.LogDebug(
                    ex,
                    "Shard {ShardIndex} of tree '{TreeId}' could not resume its orphaned-leaf repair walk at '{ResumeKey}'; restarting from the head of the chain.",
                    MyShardIndex,
                    TreeId,
                    resumeKey);
            }
        }

        var headId = (await GetLeftmostLeafIdAsync())!.Value;
        return (headId, await ResolveLeafGrain(headId).GetReclaimProbeAsync());
    }

    /// <summary>
    /// Decides what may be done about one descent-unreachable leaf, and does
    /// it unless <paramref name="dryRun"/> is set.
    /// <para>
    /// <b>Inspection and repair share this method rather than having one
    /// each.</b> The only thing <paramref name="dryRun"/> changes is whether
    /// the final mutating steps run; every check that decides whether a leaf
    /// MAY be repaired is the same code on both paths. A dry run that reached
    /// its verdict by different code from the repair it predicts would be
    /// worse than no dry run at all, because an operator would trust it.
    /// </para>
    /// </summary>
    private async Task<OrphanedLeafFinding> ExamineOrphanAsync(
        GrainId prevId,
        LeafReclaimProbe prevProbe,
        GrainId currentId,
        LeafReclaimProbe currentProbe,
        bool dryRun,
        bool survey,
        Stack<GrainId> path,
        CancellationToken cancellationToken)
    {
        var leaf = ResolveLeafGrain(currentId);

        OrphanedLeafFinding Finding(
            OrphanedLeafDisposition disposition,
            int keyCount,
            int verifiedKeyCount,
            string? unverifiedKey = null) => new()
            {
                ShardIndex = MyShardIndex,
                LeafId = currentId.ToString(),
                LowKeyInclusive = currentProbe.LowKeyInclusive,
                HighKeyExclusive = currentProbe.HighKeyExclusive,
                KeyCount = keyCount,
                VerifiedKeyCount = verifiedKeyCount,
                Disposition = disposition,
                UnverifiedKey = unverifiedKey,
            };

        // A split, seal or prepared transaction can resurrect rows this leaf
        // does not currently hold, which would invalidate the key-duplication
        // proof AFTER it was taken. Checked before the keys are enumerated
        // because it is the cheap rejection and it makes the enumeration
        // pointless.
        if (currentProbe.HasBlockingState)
        {
            return Finding(OrphanedLeafDisposition.RefusedBlockingState, 0, 0);
        }

        var keys = await leaf.GetKeysAsync();

        if (keys.Count > MaxOrphanKeysVerified)
        {
            return Finding(OrphanedLeafDisposition.RefusedKeyCountExceeded, keys.Count, 0);
        }

        var verified = 0;
        var surveyVerified = 0;
        var missing = 0;
        var contradictions = 0;
        string? firstUnverifiedKey = null;
        var disposition = OrphanedLeafDisposition.Repairable;

        foreach (var key in keys)
        {
            cancellationToken.ThrowIfCancellationRequested();

            path.Clear();
            var ownerId = await ResolveWriteLeafAsync(key, path);

            if (ownerId == currentId)
            {
                // The low-bound descent said this leaf is unreachable and a
                // descent on one of its own keys says it is not. Routing is a
                // total function, so both cannot be true, and a contradiction
                // is not something to resolve in favour of deleting a leaf.
                //
                // This is strictly stronger than the low-bound probe alone,
                // and it comes for free: the verification loop has to route
                // every key anyway, so it necessarily re-tests reachability
                // against EVERY key the leaf holds rather than against one.
                if (!survey)
                    return Finding(OrphanedLeafDisposition.RefusedRoutingContradiction, keys.Count, verified, key);

                if (firstUnverifiedKey is null)
                {
                    firstUnverifiedKey = key;
                    disposition = OrphanedLeafDisposition.RefusedRoutingContradiction;
                }
                contradictions++;
                continue;
            }

            // Non-null is exactly what a reader of this key would see, which
            // is the property that has to hold for clearing this leaf to be
            // lossless. Values are deliberately NOT compared: the two copies
            // are materialised from the same WAL records at independent replay
            // horizons, so a benign difference in how far each has replayed is
            // expected, and refusing on it would refuse the repair for a
            // reason that is not a data-loss risk.
            if (await ResolveLeafGrain(ownerId).GetAsync(key) is null)
            {
                if (!survey)
                    return Finding(OrphanedLeafDisposition.RefusedUnverifiedKeys, keys.Count, verified, key);

                if (firstUnverifiedKey is null)
                {
                    firstUnverifiedKey = key;
                    disposition = OrphanedLeafDisposition.RefusedUnverifiedKeys;
                }
                missing++;
                continue;
            }

            surveyVerified++;
            if (firstUnverifiedKey is null) verified++;
        }

        if (survey)
        {
            return Finding(disposition, keys.Count, verified, firstUnverifiedKey) with
            {
                SurveyVerifiedKeyCount = surveyVerified,
                SurveyMissingKeyCount = missing,
                SurveyRoutingContradictionKeyCount = contradictions,
            };
        }

        if (dryRun)
        {
            return Finding(OrphanedLeafDisposition.Repairable, keys.Count, verified);
        }

        // Latch the leaf closed. Everything above was read-only, so a refusal
        // here still leaves the tree exactly as it was found.
        if (!await leaf.TryBeginOrphanRetirementAsync())
        {
            logger.LogDebug(
                "Shard {ShardIndex} of tree '{TreeId}' declined to unsplice orphaned leaf {LeafId}: it would not latch for retirement.",
                MyShardIndex,
                TreeId,
                currentId);

            return Finding(OrphanedLeafDisposition.RefusedBlockingState, keys.Count, verified);
        }

        try
        {
            // The unsplice, as one compare-and-swap on the predecessor.
            //
            // THE THIRD ARGUMENT MUST BE THE PREDECESSOR'S OWN HIGH BOUND, and
            // this is the single most dangerous line in the file. Reclaim
            // passes the REMOVED leaf's high bound here, which widens the
            // predecessor onto the vacated range - correct there, because a
            // reclaimed leaf is routed and its range would otherwise be
            // orphaned. Doing that here would be data corruption: an orphan is
            // not routed, so a live leaf already owns this range, and widening
            // the predecessor onto it would have two leaves declare one range
            // and materialise every record in it twice.
            //
            // Passing the predecessor's own high bound makes the widen a
            // provable no-op in both of TryUnlinkSuccessorAsync's branches. The
            // guard there is:
            //
            //     prevHigh is not null
            //     && (absorb is null || CompareOrdinal(absorb, prevHigh) > 0)
            //
            // and here absorb IS prevHigh. A null prevHigh fails the first
            // conjunct, so no widen. A non-null prevHigh makes absorb non-null
            // too, and CompareOrdinal(prevHigh, prevHigh) > 0 is false, so
            // again no widen.
            //
            // Note which clause forbids null. Passing null would leave the
            // first conjunct satisfied whenever prevHigh is non-null and would
            // then satisfy `absorb is null`, widening the predecessor to
            // unbounded - the worst available outcome.
            //
            // Reading a possibly-stale prevProbe is safe: the same call's
            // compare-and-swap on NextSibling declines the whole unsplice if
            // the predecessor changed underneath the walk.
            var unlinked = await ResolveLeafGrain(prevId).TryUnlinkSuccessorAsync(
                currentId,
                currentProbe.NextSibling,
                prevProbe.HighKeyExclusive);

            if (!unlinked)
            {
                logger.LogDebug(
                    "Shard {ShardIndex} of tree '{TreeId}' declined to unsplice orphaned leaf {LeafId}: its predecessor {PrevLeaf} no longer points at it.",
                    MyShardIndex,
                    TreeId,
                    currentId,
                    prevId);

                await leaf.AbandonRetirementAsync();
                return Finding(OrphanedLeafDisposition.RefusedChainRace, keys.Count, verified);
            }
        }
        catch
        {
            await leaf.AbandonRetirementAsync();
            throw;
        }

        // Past the swap the unsplice has committed: the leaf is out of the
        // chain and nothing that follows can be undone, so a failure in it must
        // not be reported as a refusal.
        if (currentProbe.NextSibling is { } nextId)
        {
            try
            {
                await ResolveLeafGrain(nextId).SetPrevSiblingAsync(prevId);
            }
            catch (Exception ex)
            {
                // Nothing reads the back pointer to route, and the successor is
                // still on the chain, so the reclaim walk's back-pointer repair
                // re-points it on a later pass.
                logger.LogWarning(
                    ex,
                    "Shard {ShardIndex} of tree '{TreeId}' unspliced orphaned leaf {LeafId} but could not re-point its successor {NextLeaf} at predecessor {PrevLeaf}; a later pass repairs the back pointer.",
                    MyShardIndex,
                    TreeId,
                    currentId,
                    nextId,
                    prevId);
            }
        }

        // This is the step that actually recovers the WAL. Clearing the state
        // retires the replay barrier and unregisters the leaf's materialiser
        // pins, and it is the only caller that does - which is why the pass is
        // not finished when the leaf leaves the chain. A leaf unspliced but not
        // cleared still pins the trim floor, so the whole point of the exercise
        // is in this one call.
        //
        // And the unspliced leaf is on no chain now, so a re-run of this pass
        // cannot rediscover it: issue #2207. The clear is recorded durably as
        // owed before it is attempted, and a failure is retried from that
        // record rather than promised to a re-run that could never find it.
        await ClearRemovedLeafAsync(currentId, "unspliced orphaned");

        _leafGrains.TryRemove(currentId, out _);

        logger.LogInformation(
            "Shard {ShardIndex} of tree '{TreeId}' unspliced orphaned leaf {LeafId} covering ['{LowKey}','{HighKey}') after verifying "
            + "all {VerifiedKeys} of its keys are readable from descent-reachable leaves; predecessor {PrevLeaf} now points past it.",
            MyShardIndex,
            TreeId,
            currentId,
            currentProbe.LowKeyInclusive ?? "(unbounded)",
            currentProbe.HighKeyExclusive ?? "(unbounded)",
            verified,
            prevId);

        return Finding(OrphanedLeafDisposition.Repaired, keys.Count, verified);
    }
}
