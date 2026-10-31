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

    /// <summary>
    /// The number of shard indices the in-flight purge walks - one past the
    /// highest physical shard the tree has ever allocated, as resolved when the
    /// walk was started and re-resolved as it runs. Reported beside
    /// <see cref="NextShardIndex"/> as the purge's progress. Legacy persisted
    /// state decodes the missing slot to 0, meaning "not yet resolved".
    /// </summary>
    [Id(18)] public int PurgeShardCount { get; set; }

    /// <summary>
    /// Whether the in-flight purge was requested explicitly (through
    /// <see cref="ITreeDeletionGrain.BeginPurgeAsync"/>) rather than started by
    /// the soft-delete reminder. A requested purge walks its shards back to back
    /// instead of at the reminder-driven purge's gentle cadence, including after
    /// a reactivation resumes it. Legacy persisted state decodes the missing
    /// slot to <see langword="false"/>.
    /// </summary>
    [Id(19)] public bool PurgeRequested { get; set; }

    /// <summary>
    /// Whether a completed purge still owes the removal of the tree's registry
    /// entry. Persisted with <see cref="PurgeComplete"/> and cleared once the
    /// entry is removed, so a removal that threw is re-driven by the keepalive
    /// reminder or the next purge call rather than leaving the id reading as a
    /// live tree that kept the purged tree's settings (issue #4265). Never set
    /// for a tree whose purge keeps its registry entry. Legacy persisted state
    /// decodes the missing slot to <see langword="false"/>.
    /// </summary>
    [Id(20)] public bool RegistryUnregisterPending { get; set; }

    /// <summary>
    /// Monotonic logical soft-delete epoch. Incremented whenever this tree id
    /// enters a soft-deleted state so snapshot-bootstrap reconciliation can
    /// detect a delete-and-recover cycle that begins and ends live.
    /// </summary>
    [Id(21)] public long DeletionEpoch { get; set; }

    /// <summary>
    /// Whether <see cref="PurgedRegistryLineage"/> was recorded with the
    /// completion of this tree's purge (issue #5417). A purge completed by a build
    /// that predates the record decodes the missing slot to <see langword="false"/>,
    /// and its owed registry removal is re-driven without the ownership check.
    /// </summary>
    [Id(22)] public bool PurgedRegistryLineageRecorded { get; set; }

    /// <summary>
    /// The <see cref="TreeRegistryEntry.Lineage"/> of the registry entry the
    /// purge removes, read in the turn that records the purge complete
    /// (issue #5417). An owed removal re-driven later acts only while the entry
    /// still carries this lineage: a missing entry means the removal landed, and
    /// a different one means the id was registered again by a write or a create,
    /// so the re-drive must not trim, deregister or unregister the new tree.
    /// <see langword="null"/> when the entry carried no lineage (a registration
    /// predating issue #4537); a registration of an absent id stamps one unless
    /// its caller supplies one, which no caller registering a tree id does. Not recorded
    /// when the entry was already absent.
    /// </summary>
    [Id(23)] public Guid? PurgedRegistryLineage { get; set; }

    /// <summary>
    /// Whether <see cref="LogicalPurgedRegistryLineage"/> was recorded, in a
    /// write of its own before a logical purge removes the logical tree's
    /// registry entry (issue #5459). Legacy persisted state decodes the missing
    /// slot to <see langword="false"/>, and the removal is then recorded on its
    /// next attempt.
    /// </summary>
    [Id(24)] public bool LogicalPurgedRegistryLineageRecorded { get; set; }

    /// <summary>
    /// The <see cref="TreeRegistryEntry.Lineage"/> of the logical tree's registry
    /// entry, read before a logical purge first removes it (issue #5459). A
    /// completion re-driven after the removal landed removes the entry again only
    /// while it still carries this lineage, so it never removes the entry of a
    /// tree that a write or a create registered under the id since.
    /// </summary>
    [Id(25)] public Guid? LogicalPurgedRegistryLineage { get; set; }
}

