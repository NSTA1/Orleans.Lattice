
namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A grain responsible for managing tree-level soft deletion and deferred purge.
/// One activation exists per tree, keyed by <c>{treeId}</c>.
/// When a tree is deleted, this grain marks all shards as deleted and registers
/// a reminder that fires after <see cref="LatticeOptions.SoftDeleteDuration"/>.
/// When the reminder fires, it walks every shard (using the same timer-per-shard
/// pattern as <see cref="ITombstoneCompactionGrain"/>) and permanently purges
/// all leaf and internal node state.
/// </summary>
[Alias(TypeAliases.ITreeDeletionGrain)]
internal interface ITreeDeletionGrain : IGrainWithStringKey
{
    /// <summary>
    /// Initiates a soft delete of the tree. Marks all shards as deleted so that
    /// subsequent reads and writes throw <see cref="InvalidOperationException"/>.
    /// Registers a reminder to purge the tree after the configured soft-delete
    /// duration. Idempotent - calling again on an already-deleted tree is a no-op.
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

    /// <summary>Reports deletion or partially applied delegated deletion of the local physical copy, not the logical alias.</summary>
    Task<bool> IsPhysicalDeletedAsync();

    /// <summary>Recovers the retired or delegated local physical copy, including partially applied shard marks.</summary>
    Task RecoverPhysicalAsync();

    /// <summary>Purges the retired local copy without affecting a logical alias.</summary>
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
    /// Returns <c>true</c> if the tree has been soft-deleted (whether or not
    /// the purge has completed).
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
    /// <c>IsDeleted</c> flag on all shards and unregisters the purge reminder.
    /// Throws <see cref="InvalidOperationException"/> if the tree has not been
    /// deleted, or if the purge has already completed (data is gone).
    /// Idempotent during the soft-delete window - calling multiple times is safe.
    /// </summary>
    Task RecoverAsync();

    /// <summary>
    /// Immediately triggers a full purge of a soft-deleted tree, bypassing the
    /// <see cref="LatticeOptions.SoftDeleteDuration"/> wait. Walks every shard,
    /// clears all leaf and internal node state, and deactivates each grain.
    /// Throws <see cref="InvalidOperationException"/> if the tree has not been
    /// deleted, or if the purge has already completed.
    /// </summary>
    Task PurgeNowAsync();
}
