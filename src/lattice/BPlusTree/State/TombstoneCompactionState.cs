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
    /// Superseded by <see cref="NextLeafKeyInShard"/> (for the chain walk) and
    /// <see cref="CurrentShardDirtyIndex"/> (for the dirty-leaves fast path),
    /// and no longer written. Retained because the alias is wire format: state
    /// persisted by an older build can still carry a leaf id here.
    /// <para>
    /// A leaf grain id is not a safe resume position for a chain walk. Orleans
    /// grains are virtual, so an id persisted across a batch boundary can
    /// activate a fresh, empty grain whose sibling pointer is
    /// <see langword="null"/>, and the resumed walk would report the shard done
    /// with most of it never visited - a silent under-compaction that looks
    /// exactly like a clean end-of-shard. On load a non-null value here is
    /// discarded and the shard restarts from its leftmost leaf, which costs a
    /// re-walk and nothing else because per-leaf compaction is idempotent
    /// (issue 1973).
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
    [Id(8)] public Orleans.Lattice.HybridLogicalClock CurrentShardDirtyAdvance { get; set; }

    /// <summary>
    /// In-shard leaf-walk resume position for the current shard's <b>chain
    /// walk</b>: the key the next leaf batch re-descends onto within the shard
    /// at index <see cref="NextShardIndex"/> of
    /// <see cref="PhysicalShardIndices"/>. <see langword="null"/> means "start
    /// from the leftmost leaf when this shard's turn comes" (also the
    /// end-of-shard semantic). Persisted between leaf batches so progress
    /// survives silo crashes the same way <see cref="NextShardIndex"/> does,
    /// and cleared when the shard's walk completes.
    /// <para>
    /// A <b>key</b>, never a leaf grain id, so a resumed walk always re-descends
    /// onto whichever leaf now owns the position instead of trusting an id that
    /// may activate empty. Replaces <see cref="NextLeafIdInShard"/>
    /// (issue 1973).
    /// </para>
    /// <para>
    /// Legacy persisted state decodes the missing slot to <see langword="null"/>,
    /// which is the correct semantic default.
    /// </para>
    /// </summary>
    [Id(9)] public string? NextLeafKeyInShard { get; set; }

    /// <summary>
    /// In-shard resume position for the current shard's <b>dirty-leaves fast
    /// path</b>: the index of the next entry to visit in
    /// <see cref="CurrentShardDirtyLeaves"/>.
    /// <para>
    /// The fast path walks a persisted, finite list the shard root nominated
    /// rather than following sibling pointers, so its natural cursor is a
    /// position in that list. An index is exact where a leaf id was only
    /// locatable by a linear search that fell back to restarting the list, and
    /// it cannot silently truncate the way an id can, because the list itself -
    /// not a chain of virtual grains - bounds the walk. Ignored, and reset to
    /// zero, whenever <see cref="CurrentShardDirtyLeaves"/> is
    /// <see langword="null"/> (issue 1973).
    /// </para>
    /// <para>
    /// Legacy persisted state decodes the missing slot to <c>0</c>, which is
    /// the correct semantic default: re-walk the snapshot from its start, which
    /// per-leaf compaction makes idempotent.
    /// </para>
    /// </summary>
    [Id(10)] public int CurrentShardDirtyIndex { get; set; }
}
