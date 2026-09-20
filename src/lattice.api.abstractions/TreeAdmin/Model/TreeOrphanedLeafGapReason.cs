namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// Why an orphaned-leaf audit or repair could not establish a verdict over some
/// part of a tree. The control-API mirror of the core orphaned-leaf audit gap
/// reason enum (issue 3301).
/// </summary>
/// <remarks>
/// A gap is neither a finding nor an error. It says "I did not look here", which
/// is the one answer the report could not previously give: every condition below
/// used to produce an empty findings list indistinguishable from a clean tree, and
/// a clean audit is exactly the signal an operator uses to decide a tree needs no
/// attention.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeOrphanedLeafGapReason)]
public enum TreeOrphanedLeafGapReason
{
    /// <summary>
    /// The shard was mid shard-level split, during which a descent can
    /// legitimately land off-shard and read as unreachability. Re-run once the
    /// split settles.
    /// </summary>
    ShardSplitInProgress = 0,

    /// <summary>
    /// Another orphaned-leaf pass already held the shard. Re-run once it finishes.
    /// </summary>
    ShardPassAlreadyRunning = 1,

    /// <summary>
    /// The shard's sibling chain is severed part-way across the keyspace. The walk
    /// re-entered past the break and examined the remainder, but the severed
    /// pointer is itself a defect.
    /// </summary>
    ChainTruncated = 2,

    /// <summary>
    /// The sibling chain is severed and the walk could not re-enter past the break,
    /// so an unknown number of leaves were never examined. The audit's verdict over
    /// this shard is absent rather than empty.
    /// </summary>
    ChainTruncatedUnrecoverable = 3,

    /// <summary>
    /// The pass exhausted its work budget without proving any leaf reachable, so it
    /// had no position to continue from and the remainder was not examined.
    /// </summary>
    WalkBudgetExhaustedWithoutResumePosition = 4,

    /// <summary>
    /// A leaf declared no low bound, so there was no key to descend on and its
    /// reachability could not be decided either way.
    /// </summary>
    LeafBoundsUndecidable = 5,

    /// <summary>
    /// The leaf a walk segment started on is itself unreachable by descent. The
    /// pass can see it but cannot judge or repair it, because unsplicing needs a
    /// live predecessor to swing and an entry leaf has none in reach.
    /// </summary>
    EntryLeafUnreachable = 6,
}
