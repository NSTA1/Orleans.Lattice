
namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A grain responsible for managing tree-level soft deletion and deferred purge.
/// One activation exists per tree id, keyed by <c>{treeId}</c>, and it acts only
/// on the shards stored under that id: it does not resolve a tree alias, so an
/// aliased tree's live data under another physical tree is never reached.
/// When a tree is deleted, this grain marks those shards as deleted and registers
/// a reminder that fires after <see cref="LatticeOptions.SoftDeleteDuration"/>.
/// When the reminder fires, it walks every shard (using the same timer-per-shard
/// pattern as <see cref="ITombstoneCompactionGrain"/>) and permanently purges
/// all leaf and internal node state.
/// </summary>
[Alias(TypeAliases.ITreeDeletionGrain)]
internal interface ITreeDeletionGrain : IGrainWithStringKey
{
    /// <summary>
    /// Initiates a soft delete of the tree. Marks every shard stored under this
    /// id as deleted so that subsequent reads and writes on them throw
    /// <see cref="InvalidOperationException"/>.
    /// Registers a reminder to purge the tree after the configured soft-delete
    /// duration. Idempotent - calling again on an id already recorded as deleted
    /// is a no-op, including an id whose record is a resize's retired first copy
    /// or a purged tree, even when a live tree now answers to the id.
    /// </summary>
    Task DeleteTreeAsync();

    /// <summary>
    /// Soft-deletes the physical tree a resize retired when that physical
    /// tree's id is also the id of the live logical tree - the original copy a
    /// tree's first resize replaces, which carries the logical id while the
    /// logical tree now resolves through an alias to the resized copy. Marks
    /// the retired shards deleted and schedules their purge exactly as
    /// <see cref="DeleteTreeAsync"/> does, but leaves the registry entry and the
    /// tombstone compaction schedule under this id in place, because both
    /// belong to the live logical tree: the purge that follows reclaims the
    /// retired shards and never unregisters the logical tree. Idempotent.
    /// </summary>
    Task DeleteRetiredPhysicalTreeAsync();

    /// <summary>
    /// Returns <c>true</c> if the tree has been soft-deleted (whether or not
    /// the purge has completed).
    /// </summary>
    Task<bool> IsDeletedAsync();

    /// <summary>
    /// Returns a read-only snapshot of the tree's soft-deletion lifecycle state -
    /// whether it is deleted, when, the recovery deadline derived from the
    /// configured soft-delete duration, and whether a purge is in progress or has
    /// completed. A pure read with no side effects; unlike the mutating verbs it
    /// asserts no internal-origin marker, so a diagnostics facade may call it
    /// directly.
    /// </summary>
    Task<TreeDeletionSnapshot> GetDeletionStatusAsync();

    /// <summary>
    /// Recovers a soft-deleted tree, making it accessible again. Clears the
    /// <c>IsDeleted</c> flag on every shard stored under this id and unregisters
    /// the purge reminder.
    /// Throws <see cref="InvalidOperationException"/> if the tree has not been
    /// deleted, while a purge is in progress, or if the purge has already
    /// completed (data is gone). A retry after a partial failure is safe - the
    /// per-shard unmark and re-seed are idempotent and the deletion record is
    /// cleared only after them - but a call after a successful recovery throws,
    /// because the tree is no longer deleted.
    /// </summary>
    Task RecoverAsync();

    /// <summary>
    /// Immediately triggers a full purge of a soft-deleted tree, bypassing the
    /// <see cref="LatticeOptions.SoftDeleteDuration"/> wait. Walks every shard
    /// stored under this id, clears all leaf and internal node state, and
    /// deactivates each grain.
    /// Throws <see cref="InvalidOperationException"/> if the tree has not been
    /// deleted, or if the purge has already completed.
    /// </summary>
    Task PurgeNowAsync();
}
