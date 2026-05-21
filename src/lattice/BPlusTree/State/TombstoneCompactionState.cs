using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for <see cref="Grains.TombstoneCompactionGrain"/>.
/// Tracks the progress of an in-flight compaction pass so that it can be
/// resumed after a silo restart.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TombstoneCompactionState)]
internal sealed class TombstoneCompactionState
{
    /// <summary>Whether a compaction pass is currently in progress.</summary>
    [Id(0)] public bool InProgress { get; set; }

    /// <summary>The next shard index to compact (0-based).</summary>
    [Id(1)] public int NextShardIndex { get; set; }

    /// <summary>
    /// Number of consecutive failures for the current shard.
    /// Reset to 0 when the shard succeeds or is skipped.
    /// </summary>
    [Id(2)] public int ShardRetries { get; set; }

    /// <summary>
    /// The list of physical shard indices to compact, captured at the start of
    /// the current pass by resolving the tree's shard map. This is persisted
    /// so that the pass is resumable after a silo restart even if the shard
    /// map changes mid-pass (e.g. due to an adaptive split). When empty, the
    /// grain falls back to resolving the shard map on-demand.
    /// </summary>
    [Id(3)] public int[] PhysicalShardIndices { get; set; } = [];

    /// <summary>
    /// The resolved physical tree id (alias target) for the current pass.
    /// Persisted so that mid-pass alias rebinds don't mis-route subsequent
    /// ticks. <c>null</c> when no pass is in flight.
    /// </summary>
    [Id(4)] public string? PhysicalTreeId { get; set; }

    /// <summary>
    /// Per-physical-shard wall-clock timestamp of the last out-of-cycle
    /// compaction trigger that was honoured (i.e. landed an enqueue or
    /// mutated <see cref="InProgress"/>). Consulted by ratio- and
    /// size-based triggers to enforce the
    /// <c>LatticeOptions.CompactionTriggerCooldown</c> window. Operator
    /// requests bypass this map. Empty by default; legacy persisted state
    /// decodes the missing slot to an empty dictionary, the correct
    /// semantic default ("no triggers seen yet").
    /// </summary>
    [Id(5)] public Dictionary<int, DateTimeOffset> LastTriggerAt { get; set; } = [];

    /// <summary>
    /// In-shard leaf-walk cursor for the current shard. Points at the next
    /// leaf id to visit within the shard the coordinator is currently
    /// compacting (i.e. the shard at index
    /// <see cref="NextShardIndex"/> in <see cref="PhysicalShardIndices"/>).
    /// <c>null</c> means \"start from the leftmost leaf when this shard's
    /// turn comes\" (the legacy and end-of-shard semantic). The cursor is
    /// persisted between leaf batches so progress survives silo crashes
    /// the same way <see cref="NextShardIndex"/> does, and is cleared
    /// when the shard's leaf walk completes.
    /// <para>
    /// Legacy persisted state decodes the missing slot to <c>null</c>,
    /// which is the correct semantic default.
    /// </para>
    /// </summary>
    [Id(6)] public string? NextLeafIdInShard { get; set; }

    /// <summary>
    /// Snapshot of the dirty-leaf grain ids (as strings) returned by
    /// <c>IShardRootGrain.GetDirtyLeavesSinceLastCompactionAsync</c> when
    /// the coordinator entered the current shard via the dirty-leaves
    /// fast path. <c>null</c> means "no fast-path snapshot in flight" -
    /// either the coordinator has not yet entered a shard, or the
    /// snapshot was empty and the coordinator fell back to the legacy
    /// chain walk. Persisted so the snapshot survives silo restarts
    /// alongside <see cref="NextLeafIdInShard"/>; cleared when the shard
    /// walk completes.
    /// <para>
    /// Legacy persisted state decodes the missing slot to <c>null</c>,
    /// which is the correct semantic default.
    /// </para>
    /// </summary>
    [Id(7)] public string[]? CurrentShardDirtyLeaves { get; set; }

    /// <summary>
    /// HLC watermark observed alongside
    /// <see cref="CurrentShardDirtyLeaves"/>. Passed back to the shard
    /// root via <c>ClearDirtyLeavesUpToAsync</c> when the shard's walk
    /// completes so deletes that arrived during the in-flight pass are
    /// preserved for the next pass.
    /// </summary>
    [Id(8)] public Primitives.HybridLogicalClock CurrentShardDirtyAdvance { get; set; }
}
