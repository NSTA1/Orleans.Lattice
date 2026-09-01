using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for the <c>TreeShardConsolidationGrain</c> coordinator.
/// Tracks the lifecycle of a single online consolidation that folds one
/// physical donor shard back into an adjacent physical survivor shard.
/// <para>
/// Consolidation is the inverse of an adaptive shard split and reuses the
/// same per-shard shadow-write primitive: the donor plays the role the split
/// gives its <i>source</i> shard, and the survivor the role of the split's
/// <i>target</i>. The difference lives entirely in the coordinator - which
/// virtual slots move, which physical shard receives them, and the extra
/// survivor-side reclaim step that consolidation needs and a split does not.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TreeShardConsolidationState)]
internal sealed class TreeShardConsolidationState
{
    /// <summary>Whether a consolidation is currently in progress for this donor shard.</summary>
    [Id(0)] public bool InProgress { get; set; }

    /// <summary>Whether the most recent consolidation completed successfully.</summary>
    [Id(1)] public bool Complete { get; set; }

    /// <summary>
    /// Unique operation id for the current or most-recent consolidation. Used
    /// to make a driver's retry idempotent and to correlate progress reports.
    /// </summary>
    [Id(2)] public string? OperationId { get; set; }

    /// <summary>Current phase of the consolidation state machine.</summary>
    [Id(3)] public ShardConsolidationPhase Phase { get; set; }

    /// <summary>Physical shard index being retired (the donor).</summary>
    [Id(4)] public int DonorShardIndex { get; set; }

    /// <summary>Physical shard index absorbing the donor's virtual slots (the survivor).</summary>
    [Id(5)] public int SurvivorShardIndex { get; set; }

    /// <summary>
    /// Sorted virtual slots being folded from <see cref="DonorShardIndex"/>
    /// back onto <see cref="SurvivorShardIndex"/>. Persisted so a crash
    /// recovery resumes drain, swap, and cleanup against the same slot set
    /// even if the live shard map has moved on.
    /// </summary>
    [Id(6)] public List<int> DonorSlots { get; set; } = [];

    /// <summary>
    /// Snapshot of the <see cref="ShardMap"/> as it existed when this
    /// consolidation was planned. Supplies the authoritative virtual shard
    /// count for every slot computation in the operation, so a concurrent
    /// re-map cannot silently change the meaning of <see cref="DonorSlots"/>.
    /// </summary>
    [Id(7)] public ShardMap? OriginalShardMap { get; set; }

    /// <summary>
    /// Resume cursor for the bounded drain: the leaf whose entries the next
    /// drain pass starts from, or <see langword="null"/> to start at the
    /// donor's leftmost leaf. Lets a thousand-leaf donor degrade into steady
    /// background work rather than one unbounded stall, and lets an
    /// interrupted drain resume instead of restarting.
    /// </summary>
    [Id(8)] public GrainId? DrainCursorLeafId { get; set; }

    /// <summary>
    /// Whether the bounded drain has walked the donor's whole leaf chain at
    /// least once since the current phase began. Reset whenever a new drain
    /// sweep starts so the authoritative post-freeze sweep is never skipped.
    /// </summary>
    [Id(9)] public bool DrainSweepComplete { get; set; }

    /// <summary>
    /// Running count of entries forwarded from the donor to the survivor.
    /// Reported through <see cref="ShardConsolidationProgress"/> so a driver
    /// can observe forward progress without inspecting either shard.
    /// </summary>
    [Id(10)] public long EntriesDrained { get; set; }

    /// <summary>
    /// Running count of donor leaves visited by the drain across all passes.
    /// Reported for progress only; it is not a correctness input.
    /// </summary>
    [Id(11)] public long LeavesScanned { get; set; }

    /// <summary>
    /// Set by <c>CancelAsync</c>. Honoured only at a phase boundary strictly
    /// before <see cref="ShardConsolidationPhase.Swap"/>, where the operation
    /// can still be abandoned without having changed the tree's routing. Once
    /// the shard map has flipped, the request is ignored and the operation
    /// runs to completion, because abandoning after the flip would strand the
    /// donor's <c>SplitInProgress</c> record.
    /// </summary>
    [Id(12)] public bool CancelRequested { get; set; }

    /// <summary>
    /// Whether the most recent consolidation ended because a cancel was
    /// honoured rather than because it completed. Mutually exclusive with
    /// <see cref="Complete"/>.
    /// </summary>
    [Id(13)] public bool Cancelled { get; set; }

    /// <summary>UTC ticks at which the current or most-recent operation started.</summary>
    [Id(14)] public long StartedAtTicks { get; set; }

    /// <summary>UTC ticks of the most recent persisted phase or drain advance.</summary>
    [Id(15)] public long UpdatedAtTicks { get; set; }
}

/// <summary>
/// Phase of an online shard consolidation. Drives the per-tick behaviour of
/// the <c>TreeShardConsolidationGrain</c> coordinator. Every transition is
/// persisted before the next phase runs, and every phase action is
/// idempotent, so an interruption at any boundary leaves a state a later
/// attempt completes or safely abandons - never a half-merged tree.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardConsolidationPhase)]
internal enum ShardConsolidationPhase
{
    /// <summary>No consolidation is active.</summary>
    None = 0,

    /// <summary>
    /// Intent has been persisted and the donor has been instructed to mirror
    /// every accepted write on the folding slots to the survivor. The
    /// background drain has not started. Both shards serve traffic normally;
    /// the routing map still points the folding slots at the donor.
    /// </summary>
    BeginShadowWrite = 1,

    /// <summary>
    /// Bounded background drain in progress: historical donor entries for the
    /// folding slots are copied to the survivor in batches, preserving their
    /// original HLC timestamps, tombstone flags, and expiry metadata. The
    /// donor keeps serving reads and writes throughout.
    /// </summary>
    Drain = 2,

    /// <summary>
    /// The single freeze-and-flip step. In one pass the donor's leaves are
    /// sealed for the folding slots, the donor enters reject, an
    /// authoritative final drain re-synchronises the survivor with the
    /// donor's now-frozen committed state, the survivor reclaims the slots,
    /// and the registry's <see cref="ShardMap"/> is re-pointed onto the
    /// survivor.
    /// </summary>
    Swap = 3,

    /// <summary>
    /// The routing map now points the folded slots at the survivor. The donor
    /// rejects operations on those slots with
    /// <see cref="StaleShardRoutingException"/> so a stale routing cache
    /// self-heals onto the survivor.
    /// </summary>
    Reject = 4,

    /// <summary>
    /// Final drain pass and donor retirement: the donor records the folded
    /// slots in its permanent moved-away set so every later stale route
    /// self-heals, and the coordinator clears its own in-progress state.
    /// </summary>
    Complete = 5,
}
