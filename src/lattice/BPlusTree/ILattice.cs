namespace Orleans.Lattice;

/// <summary>
/// Public entry point for a distributed B+ tree.
/// A stateless-worker grain that routes requests to the correct shard root
/// based on a stable hash of the key.
/// Key format: <c>{treeId}</c> — the tree this grain manages.
/// </summary>
public interface ILattice : IGrainWithStringKey
{
    /// <summary>Gets the value associated with <paramref name="key"/>, or <c>null</c> if not found.</summary>
    Task<byte[]?> GetAsync(string key);

    /// <summary>Returns <c>true</c> if <paramref name="key"/> exists and is not tombstoned.</summary>
    Task<bool> ExistsAsync(string key);

    /// <summary>
    /// Returns the values for the given <paramref name="keys"/>, fanning out to shards in parallel.
    /// Keys that do not exist or are tombstoned are omitted from the result.
    /// </summary>
    Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys);

    /// <summary>Inserts or updates the value for <paramref name="key"/>.</summary>
    Task SetAsync(string key, byte[] value);

    /// <summary>
    /// Inserts or updates multiple key-value pairs, fanning out to shards in parallel.
    /// </summary>
    Task SetManyAsync(List<KeyValuePair<string, byte[]>> entries);

    /// <summary>Deletes the value for <paramref name="key"/>. Returns <c>true</c> if it existed.</summary>
    Task<bool> DeleteAsync(string key);

    /// <summary>
    /// Returns all live keys in the tree as an ordered async stream.
    /// Keys are returned in lexicographic order (or reverse if <paramref name="reverse"/> is <c>true</c>).
    /// Optionally filters to keys in the range [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>).
    /// </summary>
    IAsyncEnumerable<string> KeysAsync(string? startInclusive = null, string? endExclusive = null, bool reverse = false);

    /// <summary>
    /// Bulk-loads key-value pairs into an empty tree, building leaves and
    /// internal nodes bottom-up without any splits. Significantly faster than
    /// individual <see cref="SetAsync"/> calls for initial data seeding.
    /// Entries do not need to be pre-sorted; the implementation sorts them internally.
    /// Throws <see cref="InvalidOperationException"/> if any shard already contains data.
    /// </summary>
    Task BulkLoadAsync(IReadOnlyList<KeyValuePair<string, byte[]>> entries);

    /// <summary>
    /// Soft-deletes the entire tree. All shards are immediately marked as deleted,
    /// causing subsequent reads and writes to throw <see cref="InvalidOperationException"/>.
    /// A grain reminder is registered to permanently purge all tree data after the
    /// configured <see cref="LatticeOptions.SoftDeleteDuration"/> has elapsed.
    /// Idempotent — calling on an already-deleted tree is a no-op.
    /// </summary>
    Task DeleteTreeAsync();

    /// <summary>
    /// Recovers a soft-deleted tree, restoring it to normal operation.
    /// All data written before the delete is accessible again.
    /// Throws <see cref="InvalidOperationException"/> if the tree has not been
    /// deleted, or if the purge has already completed (data is gone).
    /// </summary>
    Task RecoverTreeAsync();

    /// <summary>
    /// Immediately purges a soft-deleted tree without waiting for the
    /// <see cref="LatticeOptions.SoftDeleteDuration"/> window to elapse.
    /// Permanently removes all leaf and internal node state.
    /// Throws <see cref="InvalidOperationException"/> if the tree has not been
    /// deleted, or if the purge has already completed.
    /// </summary>
    Task PurgeTreeAsync();

    /// <summary>
    /// Resizes the tree by creating an offline snapshot with new
    /// <see cref="LatticeOptions.MaxLeafKeys"/> and <see cref="LatticeOptions.MaxInternalChildren"/>
    /// values into a new physical tree, then swapping the tree alias so that all
    /// subsequent reads and writes are redirected to the resized tree. The old
    /// physical tree is soft-deleted and will be purged after the configured
    /// <see cref="LatticeOptions.SoftDeleteDuration"/>.
    /// <para>
    /// The tree ID is preserved — it becomes an alias to the new physical tree.
    /// During the snapshot phase, the tree is temporarily locked (offline snapshot).
    /// After the alias swap, the tree is immediately available with the new sizing.
    /// Cache invalidation is automatic: different physical trees produce different
    /// leaf grain IDs, which create fresh cache grain instances.
    /// </para>
    /// </summary>
    /// <param name="newMaxLeafKeys">The new maximum number of keys per leaf node. Must be greater than 1.</param>
    /// <param name="newMaxInternalChildren">The new maximum number of children per internal node. Must be greater than 2.</param>
    Task ResizeAsync(int newMaxLeafKeys, int newMaxInternalChildren);

    /// <summary>
    /// Undoes the most recent resize by recovering the old physical tree,
    /// removing the alias, restoring the original registry configuration,
    /// and deleting the new snapshot tree. Only available while the old tree
    /// is still within its <see cref="LatticeOptions.SoftDeleteDuration"/>
    /// window (before purge completes).
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown if no completed resize exists to undo, or if the old tree has
    /// already been purged.
    /// </exception>
    Task UndoResizeAsync();

    /// <summary>
    /// Creates a snapshot of this tree into a new tree with the given
    /// <paramref name="destinationTreeId"/>. All live key-value pairs are copied
    /// shard-by-shard into the destination tree.
    /// <para>
    /// In <see cref="SnapshotMode.Offline"/> mode, the source tree is locked
    /// (marked deleted) during the copy, guaranteeing a consistent snapshot.
    /// In <see cref="SnapshotMode.Online"/> mode, the source tree remains
    /// available; the result is a best-effort point-in-time copy.
    /// </para>
    /// <para>
    /// The source and destination trees must have the same <see cref="LatticeOptions.ShardCount"/>.
    /// The destination tree must not already exist.
    /// </para>
    /// </summary>
    /// <param name="destinationTreeId">The ID for the new tree. Must not already exist.</param>
    /// <param name="mode">Whether to lock the source tree during the snapshot.</param>
    /// <param name="maxLeafKeys">Optional leaf sizing for the destination. If <c>null</c>, uses the source tree's configured value.</param>
    /// <param name="maxInternalChildren">Optional internal node sizing for the destination. If <c>null</c>, uses the source tree's configured value.</param>
    Task SnapshotAsync(string destinationTreeId, SnapshotMode mode, int? maxLeafKeys = null, int? maxInternalChildren = null);

    /// <summary>
    /// Returns <c>true</c> if this tree is registered in the internal tree registry.
    /// A tree is registered on its first write and unregistered when its purge completes.
    /// </summary>
    Task<bool> TreeExistsAsync();

    /// <summary>
    /// Returns the IDs of all registered trees in sorted order.
    /// System-internal trees (prefixed with <c>_lattice_</c>) are excluded.
    /// Physical trees created by <see cref="ResizeAsync"/> and
    /// <see cref="SnapshotAsync"/> are included.
    /// </summary>
    Task<IReadOnlyList<string>> GetAllTreeIdsAsync();
}
