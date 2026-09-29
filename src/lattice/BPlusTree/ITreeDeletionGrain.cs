
namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A grain responsible for managing tree-level soft deletion and deferred purge.
/// One activation exists per tree id, keyed by <c>{treeId}</c>. A logical delete
/// of an aliased tree resolves the alias, validates that the target was derived
/// from this tree and is aliased by no other, pins it, and delegates the shard
/// marks and purge to the target's own deletion grain; physical retirement of a
/// resized copy is silent and does not make the logical tree deleted.
/// When a tree is deleted, this grain marks the shards as deleted and registers
/// a reminder that fires after <see cref="LatticeOptions.SoftDeleteDuration"/>.
/// When the reminder fires, it walks every shard (using the same timer-per-shard
/// pattern as <see cref="ITombstoneCompactionGrain"/>) and permanently purges
/// all leaf and internal node state.
/// </summary>
[Alias(TypeAliases.ITreeDeletionGrain)]
internal interface ITreeDeletionGrain : IGrainWithStringKey
{
    /// <summary>
    /// Initiates a soft delete of the logical tree, pinning its current owned
    /// alias target independently of any retired copy. Marks all target shards as deleted so that
    /// subsequent reads and writes throw <see cref="InvalidOperationException"/>.
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

    /// <summary>Retires a derived physical copy whose registry entry is disposable.</summary>
    Task DeleteDerivedPhysicalTreeAsync();

    /// <summary>
    /// Discards a derived physical copy that will never be used again - the
    /// destination of an undone resize. Marks its shards deleted exactly as
    /// <see cref="DeleteDerivedPhysicalTreeAsync"/> does, and schedules the same
    /// purge after <see cref="LatticeOptions.SoftDeleteDuration"/>, so a router
    /// that cached an alias to the copy keeps being refused rather than reading
    /// an empty tree. Unlike a deletion it records the copy as discarded, which
    /// makes it unrecoverable, and releases its write-ahead-log retention
    /// immediately: every leaf materialiser pin held against it is retired and
    /// its log is trimmed to its head. Without that, the copy's never-checkpointed
    /// pins hold its cursor floor for the whole soft-delete window, and the WAL GC
    /// reactivates its leaves to replay a log nothing will ever read (issue #3930).
    /// Idempotent; a retry re-applies the shard marks and re-releases the WAL.
    /// </summary>
    Task DiscardDerivedPhysicalTreeAsync();

    /// <summary>
    /// Reports whether the local physical copy is live, soft-deleted but still
    /// recoverable, or discarded, so the WAL GC can tell a retention floor that
    /// is merely behind from one held by a tree nobody can read. A pure read of
    /// in-memory state, interleaved so a probe never queues behind a purge.
    /// </summary>
    [Orleans.Concurrency.AlwaysInterleave]
    Task<PhysicalTreeRetention> GetPhysicalRetentionAsync();

    /// <summary>Reports deletion or partially applied delegated deletion of the local physical copy, not the logical alias.</summary>
    Task<bool> IsPhysicalDeletedAsync();

    /// <summary>Recovers the retired or delegated local physical copy, including partially applied shard marks.</summary>
    Task RecoverPhysicalAsync();

    /// <summary>Purges the retired or delegated local copy; its logical owner manages the alias.</summary>
    Task PurgePhysicalAsync();

    /// <summary>Deletes a physical copy without events or a competing purge driver.</summary>
    Task DeleteDelegatedAsync();

    /// <summary>Reserves the logical lifecycle for an idempotent alias operation.</summary>
    Task BeginAliasChangeAsync(string operationId);

    /// <summary>
    /// Releases only the matching alias-operation reservation. Internal-origin
    /// control-plane callers only; an absent or different reservation is a no-op.
    /// Owning coordinators use this after completion or to abandon a persisted
    /// preparation while idle, never on a time-based lease expiry.
    /// </summary>
    Task EndAliasChangeAsync(string operationId);

    /// <summary>Rejects alias writes while logical deletion is pending or durable.</summary>
    [Orleans.Concurrency.AlwaysInterleave]
    Task EnsureAliasWritableAsync();

    /// <summary>
    /// Returns <c>true</c> while logical deletion is pending or durable
    /// (whether or not the purge has completed). Physical retirement alone
    /// does not make the logical tree deleted.
    /// </summary>
    [Orleans.Concurrency.AlwaysInterleave]
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
    /// deleted, once its purge has started, or after the purge has completed
    /// (data is gone). A retry after a partial failure is safe - the per-shard
    /// unmark and re-seed are idempotent and the deletion record is cleared only
    /// after them - but a call after a successful recovery throws, because the
    /// tree is no longer deleted.
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
