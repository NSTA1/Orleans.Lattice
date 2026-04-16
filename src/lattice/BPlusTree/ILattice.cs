namespace Orleans.Lattice.BPlusTree;

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
}
