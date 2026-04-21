namespace Orleans.Lattice;

/// <summary>
/// Classifies a <see cref="LatticeTreeEvent"/>. Consumers filter by
/// <see cref="LatticeTreeEvent.Kind"/> client-side — all events for a tree
/// flow through a single Orleans stream regardless of kind.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeTreeEventKind)]
public enum LatticeTreeEventKind
{
    /// <summary>A key was written via <c>SetAsync</c>, <c>SetIfVersionAsync</c>, <c>GetOrSetAsync</c>, <c>SetManyAsync</c>, or a saga write.</summary>
    Set = 0,

    /// <summary>A key was tombstoned via <c>DeleteAsync</c> or saga compensation.</summary>
    Delete = 1,

    /// <summary>A lexicographic range-delete completed. <see cref="LatticeTreeEvent.Key"/> carries <c>"startInclusive..endExclusive"</c>.</summary>
    DeleteRange = 2,

    /// <summary>An adaptive shard split completed and the <c>ShardMap</c> was swapped.</summary>
    SplitCommitted = 3,

    /// <summary>A tombstone-compaction reminder pass completed (expired tombstones reaped).</summary>
    CompactionCompleted = 4,

    /// <summary>The tree was soft-deleted via <c>DeleteTreeAsync</c>.</summary>
    TreeDeleted = 5,

    /// <summary>A soft-deleted tree was recovered via <c>RecoverTreeAsync</c>.</summary>
    TreeRecovered = 6,

    /// <summary>A soft-deleted tree was permanently purged (reminder-driven or <c>PurgeTreeAsync</c>).</summary>
    TreePurged = 7,

    /// <summary>A tree snapshot finished copying.</summary>
    SnapshotCompleted = 8,

    /// <summary>An online resize completed and the tree alias was swapped.</summary>
    ResizeCompleted = 9,

    /// <summary>An online reshard grew the tree to the requested physical shard count.</summary>
    ReshardCompleted = 10,

    /// <summary>A <c>SetManyAtomicAsync</c> saga reached its terminal <c>Completed</c> phase.</summary>
    AtomicWriteCompleted = 11,
}
