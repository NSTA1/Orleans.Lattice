namespace Orleans.Lattice;

/// <summary>
/// Why an orphaned-leaf pass could not establish a verdict over some part of a
/// tree (issue 3301).
/// <para>
/// A gap is not a finding and it is not an error. It is the pass saying "I did
/// not look here", which is the one answer the report could not previously
/// give: before this existed every one of the conditions below produced an
/// empty <see cref="OrphanedLeafRepairReport.Findings"/> list that was
/// indistinguishable from a clean tree, and a clean audit is exactly the
/// signal an operator uses to decide a tree needs no attention.
/// </para>
/// <para>
/// <b>Why silence was the wrong default.</b> The pass enumerates candidates by
/// walking a shard's sibling chain from its head. That chain is the same
/// structure an orphan damages, so every way the walk can be cut short is also
/// a way the population it exists to find can hide behind the cut. Reporting
/// the cut is what keeps "found nothing" and "could not look" apart.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.OrphanedLeafAuditGapReason)]
public enum OrphanedLeafAuditGapReason

{
    /// <summary>
    /// The shard was in the middle of a shard-level split, which moves whole
    /// slot ranges between shards while it runs. A descent can legitimately
    /// land off-shard during one, and that reads as unreachability, so the
    /// pass yields rather than risk a false positive that deletes a live leaf.
    /// Re-run once the split settles.
    /// </summary>
    ShardSplitInProgress = 0,

    /// <summary>
    /// Another orphaned-leaf pass already held this shard's activation. The
    /// pass declines rather than interleaving two walks over one chain.
    /// Re-run once the other pass finishes.
    /// </summary>
    ShardPassAlreadyRunning = 1,

    /// <summary>
    /// The sibling chain ended on a leaf that is not the shard's rightmost
    /// leaf, so the chain is severed part-way across the keyspace. The walk
    /// re-entered the chain past the break by descending on the severed leaf's
    /// high bound and carried on, so the remainder WAS examined - but the
    /// severed pointer is itself a defect and is reported rather than silently
    /// worked around.
    /// </summary>
    ChainTruncated = 2,

    /// <summary>
    /// The sibling chain ended short of the shard's rightmost leaf and the
    /// walk could not re-enter past the break, so an unknown number of leaves
    /// were never examined. <b>This is the shape that produced issue 3301:</b>
    /// a head-anchored chain walk cannot see past a severed pointer, while a
    /// range scan - which enters the chain by descending on its own lower
    /// bound - reaches the segment beyond it and reports the orphans there.
    /// The audit's verdict over this shard is not merely empty, it is absent.
    /// </summary>
    ChainTruncatedUnrecoverable = 3,

    /// <summary>
    /// The pass exhausted its work budget without ever proving a leaf
    /// reachable, so it had no position to name and the drive could not be
    /// told where to continue. The remainder of the chain was not examined.
    /// </summary>
    WalkBudgetExhaustedWithoutResumePosition = 4,

    /// <summary>
    /// A leaf in the chain declared no low bound, so there was no key to
    /// descend on and its reachability could not be decided either way. The
    /// pass declines to judge it rather than judging it without evidence.
    /// <para>
    /// Note the range-scan chain guard reaches the opposite conclusion on this
    /// same population: a leaf that declares a real trailing edge while
    /// leaving its leading edge unset is claiming the keyspace from the
    /// unbounded end, and it is judged a regression. The two mechanisms
    /// genuinely disagree here, so the disagreement is surfaced rather than
    /// resolved by silence.
    /// </para>
    /// </summary>
    LeafBoundsUndecidable = 5,

    /// <summary>
    /// The leaf the walk entered on - the chain head, a resume position, or a
    /// re-entry past a break - is itself unreachable by descent. The walk only
    /// ever examines a leaf's successor, because unsplicing needs a live
    /// predecessor to swing, so an unreachable entry leaf is an orphan this
    /// pass can see but cannot judge or repair.
    /// </summary>
    EntryLeafUnreachable = 6,
}
