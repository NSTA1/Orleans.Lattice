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

    /// <summary>The physical target pinned by a logical alias deletion.</summary>
    [Id(7)] public string? LogicalPhysicalTreeId { get; set; }

    /// <summary>The logical deletion time, independent of physical retirement.</summary>
    [Id(8)] public DateTimeOffset? LogicalDeletedAtUtc { get; set; }

    /// <summary>Whether the delegated logical purge has started.</summary>
    [Id(9)] public bool LogicalPurgeInProgress { get; set; }

    /// <summary>Whether both physical and logical registry entries were purged.</summary>
    [Id(10)] public bool LogicalPurgeComplete { get; set; }

    /// <summary>Whether the logical soft-delete side effects completed.</summary>
    [Id(11)] public bool LogicalDeleteComplete { get; set; }

    /// <summary>Whether physical work is driven exclusively by a logical owner.</summary>
    [Id(12)] public bool Delegated { get; set; }

    /// <summary>Whether lifecycle events describe an internal physical operation.</summary>
    [Id(13)] public bool SuppressLifecycleEvents { get; set; }

    /// <summary>The durable reservation held by an alias-changing coordinator.</summary>
    [Id(14)] public string? AliasOperationId { get; set; }

    /// <summary>Blocks alias changes while the deletion target is being resolved.</summary>
    [Id(15)] public bool DeletePending { get; set; }

    /// <summary>Pins an ordinary deletion to this grain's physical id before shard effects.</summary>
    [Id(16)] public bool LocalDeleteTargetPinned { get; set; }

    /// <summary>
    /// Whether this physical copy was discarded by
    /// <see cref="ITreeDeletionGrain.DiscardDerivedPhysicalTreeAsync"/> - an
    /// undone resize's destination. A discarded copy has had its WAL retention
    /// released, so it can never be recovered; its purge also trims its log.
    /// Legacy persisted state decodes the missing slot to <see langword="false"/>.
    /// </summary>
    [Id(17)] public bool Discarded { get; set; }

}
