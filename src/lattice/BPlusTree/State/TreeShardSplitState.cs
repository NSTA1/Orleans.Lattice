using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for the <c>TreeShardSplitGrain</c> coordinator. Tracks
/// the lifecycle of a single adaptive split of one physical shard into two.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TreeShardSplitState)]
internal sealed class TreeShardSplitState
{
    /// <summary>Whether a split is currently in progress for this tree.</summary>
    [Id(0)] public bool InProgress { get; set; }

    /// <summary>Whether the most recent split completed successfully.</summary>
    [Id(1)] public bool Complete { get; set; }

    /// <summary>Unique operation ID for the current/most-recent split (used for idempotent retries).</summary>
    [Id(2)] public string? OperationId { get; set; }

    /// <summary>Current phase of the split state machine.</summary>
    [Id(3)] public ShardSplitPhase Phase { get; set; }

    /// <summary>Physical shard index being split.</summary>
    [Id(4)] public int SourceShardIndex { get; set; }

    /// <summary>Newly allocated physical shard index that will receive the moved virtual slots.</summary>
    [Id(5)] public int TargetShardIndex { get; set; }

    /// <summary>
    /// Virtual slots being migrated from <see cref="SourceShardIndex"/> to
    /// <see cref="TargetShardIndex"/>. Persisted so that crash-recovery can
    /// resume drain/cleanup against the same slot set.
    /// </summary>
    [Id(6)] public List<int> MovedSlots { get; set; } = [];

    /// <summary>
    /// Snapshot of the <see cref="ShardMap"/> as it existed at the start of
    /// this split. Used to compute the post-swap map and to allow rollback if
    /// the split is aborted before the swap.
    /// </summary>
    [Id(7)] public ShardMap? OriginalShardMap { get; set; }
}

/// <summary>
/// Phase of an adaptive shard split. Drives the per-phase behaviour of both
/// the <c>TreeShardSplitGrain</c> coordinator and the source <c>ShardRootGrain</c>.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardSplitPhase)]
internal enum ShardSplitPhase
{
    /// <summary>No split is active.</summary>
    None = 0,

    /// <summary>
    /// Split has been planned and the source shard has been instructed to
    /// shadow-write moved-slot writes to the target shard. Drain has not yet
    /// started.
    /// </summary>
    BeginShadowWrite = 1,

    /// <summary>
    /// Background drain in progress: historical entries on moved slots are
    /// being copied from the source shard to the target shard via
    /// <see cref="IShardRootGrain.MergeManyAsync"/>. The source shard
    /// continues to serve reads and writes, with shadow-writes mirrored to
    /// the target.
    /// </summary>
    Drain = 2,

    /// <summary>
    /// Drain has completed and the registry's <see cref="ShardMap"/> has
    /// been swapped to route moved slots to the target shard. The source
    /// shard is about to enter the reject phase.
    /// </summary>
    Swap = 3,

    /// <summary>
    /// Source shard now rejects operations on moved slots with
    /// <see cref="StaleShardRoutingException"/>, forcing stale
    /// <c>LatticeGrain</c> activations to refresh their cached
    /// <see cref="ShardMap"/> and retry against the target shard. Background
    /// cleanup of the moved entries on the source shard is in progress.
    /// </summary>
    Reject = 4,

    /// <summary>
    /// Cleanup complete; source shard's <c>SplitInProgress</c> state has
    /// been cleared. The split is fully complete.
    /// </summary>
    Complete = 5,
}

/// <summary>
/// Per-shard-root state describing an in-progress split. When non-null, the
/// shard root applies phase-specific routing checks to every operation:
/// <list type="bullet">
/// <item><description><see cref="ShardSplitPhase.BeginShadowWrite"/> /
/// <see cref="ShardSplitPhase.Drain"/> /
/// <see cref="ShardSplitPhase.Swap"/> — writes to keys whose virtual slot is
/// in <see cref="MovedSlots"/> are mirrored to <see cref="ShadowTargetShardIndex"/>
/// in addition to being written locally.</description></item>
/// <item><description><see cref="ShardSplitPhase.Reject"/> — operations on
/// keys whose virtual slot is in <see cref="MovedSlots"/> throw
/// <see cref="StaleShardRoutingException"/>.</description></item>
/// </list>
/// Stored on <see cref="ShardRootState"/>; cleared once the split coordinator
/// completes the post-cleanup phase.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardSplitInProgress)]
[Immutable]
internal sealed record ShardSplitInProgress
{
    /// <summary>Current phase of the split this shard is participating in.</summary>
    [Id(0)] public required ShardSplitPhase Phase { get; init; }

    /// <summary>Physical shard index of the new shard receiving the moved slots.</summary>
    [Id(1)] public required int ShadowTargetShardIndex { get; init; }

    /// <summary>
    /// Sorted set of virtual slot indices being migrated. Stored as a sorted
    /// array for fast binary-search membership tests on the hot path of every
    /// shard-root operation.
    /// </summary>
    [Id(2)] public required int[] MovedSlots { get; init; }

    /// <summary>
    /// Total number of virtual slots in the tree's <see cref="ShardMap"/>.
    /// Required to compute the virtual slot of a given key without a
    /// registry lookup on the hot path.
    /// </summary>
    [Id(3)] public required int VirtualShardCount { get; init; }

    /// <summary>
    /// Returns <c>true</c> if <paramref name="virtualSlot"/> is one of the
    /// migrated slots. Uses binary search over the sorted
    /// <see cref="MovedSlots"/> for O(log n) membership.
    /// </summary>
    public bool IsMovedSlot(int virtualSlot) => Array.BinarySearch(MovedSlots, virtualSlot) >= 0;
}
