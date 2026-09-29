using Orleans.Lattice;

namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent state for <see cref="Grains.TreeDeletionGrain"/>.
/// Tracks whether a tree has been soft-deleted and the progress of an
/// in-flight purge pass so that it can be resumed after a silo restart.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TreeDeletionState)]
internal sealed class TreeDeletionState
{
    /// <summary>Whether the tree has been soft-deleted.</summary>
    [Id(0)] public bool IsDeleted { get; set; }

    /// <summary>The UTC time at which the soft delete was initiated.</summary>
    [Id(1)] public DateTimeOffset? DeletedAtUtc { get; set; }

    /// <summary>Whether a purge pass is currently in progress.</summary>
    [Id(2)] public bool PurgeInProgress { get; set; }

    /// <summary>The next shard index to purge (0-based).</summary>
    [Id(3)] public int NextShardIndex { get; set; }

    /// <summary>
    /// Number of consecutive failures for the current shard.
    /// Reset to 0 when the shard succeeds or is skipped.
    /// </summary>
    [Id(4)] public int ShardRetries { get; set; }

    /// <summary>Whether the purge has fully completed (all grains cleared).</summary>
    [Id(5)] public bool PurgeComplete { get; set; }

    /// <summary>
    /// Whether this deletion retires a physical tree whose id is also the id of
    /// a live logical tree, set by
    /// <see cref="ITreeDeletionGrain.DeleteRetiredPhysicalTreeAsync"/>. A tree's
    /// first resize retires its original physical copy, which carries the
    /// logical id, while the logical tree lives on through an alias to the
    /// resized copy. The registry entry and the tombstone compaction schedule
    /// under that id belong to the live logical tree, so a retirement leaves
    /// both in place and its purge reclaims only the retired shards. Cleared by
    /// a successful recovery. Legacy persisted state decodes the missing slot to
    /// <see langword="false"/>, the behaviour of an ordinary deletion.
    /// </summary>
    [Id(6)] public bool RetainsRegistryEntry { get; set; }
}
