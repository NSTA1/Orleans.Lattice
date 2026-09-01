namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The verdict <see cref="ShardSplitAdmissionCore"/> returns for one sampled
/// physical shard: either the shard is admitted for an autonomic split, or the
/// single load-bearing clause that refused it.
/// </summary>
/// <remarks>
/// The refusal reasons are ordered by evaluation order, cheapest and most
/// selective first, so a cold shard reports
/// <see cref="BelowRateThreshold"/> rather than a downstream structural reason.
/// Each value maps to an observability reason tag on
/// <c>orleans.lattice.split.admission.deferred</c>, so an operator can tell a
/// tree that is uniformly loaded from one that has hit its shard ceiling from
/// one whose shards are simply too small to be worth splitting.
/// </remarks>
internal enum ShardSplitAdmissionOutcome
{
    /// <summary>The shard cleared every clause and may be split.</summary>
    Admitted = 0,

    /// <summary>The shard is already the source of an unfinished split.</summary>
    AlreadySplitting = 1,

    /// <summary>
    /// The shard's observed operations per second is below
    /// <see cref="LatticeOptions.HotShardOpsPerSecondThreshold"/>.
    /// </summary>
    BelowRateThreshold = 2,

    /// <summary>
    /// The tree has already reached
    /// <see cref="LatticeOptions.MaxPhysicalShardsPerTree"/> physical shards, so
    /// autonomic growth is capped whatever the observed load.
    /// </summary>
    ShardCeilingReached = 3,

    /// <summary>
    /// The tree's load is uniform rather than skewed: the hottest shard does not
    /// carry <see cref="LatticeOptions.HotShardMinSkewRatio"/> times the tree's
    /// median shard load, so splitting would relieve nothing. This is the bulk
    /// ingest shape, where every shard is hot for the same reason.
    /// </summary>
    UniformLoad = 4,

    /// <summary>
    /// The shard split recently and is still inside
    /// <see cref="LatticeOptions.HotShardSplitCooldown"/>.
    /// </summary>
    Cooldown = 5,

    /// <summary>
    /// The shard owns fewer than two virtual slots, so there is nothing to
    /// subdivide.
    /// </summary>
    InsufficientSlots = 6,

    /// <summary>
    /// The shard holds fewer than
    /// <see cref="LatticeOptions.HotShardMinShardEntries"/> live entries, so a
    /// split cannot redistribute enough data to relieve it.
    /// </summary>
    LowOccupancy = 7,
}
