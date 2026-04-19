using System.ComponentModel;

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
[EditorBrowsable(EditorBrowsableState.Never)]
[Alias(TypeAliases.ITreeDeletionGrain)]
public interface ITreeDeletionGrain : IGrainWithStringKey
{
    /// <summary>
    /// Initiates a soft delete of the tree. Marks all shards as deleted so that
    /// subsequent reads and writes throw <see cref="InvalidOperationException"/>.
    /// Registers a reminder to purge the tree after the configured soft-delete
    /// duration. Idempotent — calling again on an already-deleted tree is a no-op.
    /// </summary>
    Task DeleteTreeAsync();

    /// <summary>
    /// Returns <c>true</c> if the tree has been soft-deleted (whether or not
    /// the purge has completed).
    /// </summary>
    Task<bool> IsDeletedAsync();

    /// <summary>
    /// Recovers a soft-deleted tree, making it accessible again. Clears the
    /// <c>IsDeleted</c> flag on all shards and unregisters the purge reminder.
    /// Throws <see cref="InvalidOperationException"/> if the tree has not been
    /// deleted, or if the purge has already completed (data is gone).
    /// Idempotent during the soft-delete window — calling multiple times is safe.
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
