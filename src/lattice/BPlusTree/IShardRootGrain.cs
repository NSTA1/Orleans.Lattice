namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The root grain for a single shard of a B+ tree.
/// Key format: <c>{treeId}/{shardIndex}</c>.
/// The root grain acts as the entry point for traversal; it may be an internal
/// node or (initially) a leaf node.
/// </summary>
public interface IShardRootGrain : IGrainWithStringKey
{
    Task<byte[]?> GetAsync(string key);
    Task<bool> ExistsAsync(string key);
    Task SetAsync(string key, byte[] value);
    Task<bool> DeleteAsync(string key);

    /// <summary>
    /// Returns a page of live keys in this shard's B+ tree in sorted order,
    /// filtered to the [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>) range.
    /// Pass <paramref name="continuationToken"/> (the last key from the previous page)
    /// to resume pagination; keys &gt; the token are returned.
    /// </summary>
    Task<KeysPage> GetSortedKeysBatchAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken = null);

    /// <summary>
    /// Returns a page of live keys in <em>reverse</em> sorted order,
    /// filtered to the [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>) range.
    /// Pass <paramref name="continuationToken"/> (the last key from the previous page)
    /// to resume pagination; keys &lt; the token are returned.
    /// </summary>
    Task<KeysPage> GetSortedKeysBatchReverseAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken = null);

    /// <summary>
    /// Returns the <see cref="GrainId"/> of the leftmost leaf in this shard's B+ tree,
    /// or <c>null</c> if the tree has not been initialised yet.
    /// Used by the tombstone compaction grain to walk the leaf chain.
    /// </summary>
    Task<GrainId?> GetLeftmostLeafIdAsync();

    /// <summary>
    /// Bulk-loads pre-sorted key-value pairs into this shard, building leaves and
    /// internal nodes bottom-up. The shard must be empty (no root node).
    /// Entries must already be sorted in ascending key order.
    /// </summary>
    /// <param name="operationId">Unique ID for idempotency. Retries with the same ID are no-ops.</param>
    Task BulkLoadAsync(string operationId, List<KeyValuePair<string, byte[]>> sortedEntries);

    /// <summary>
    /// Appends a sorted batch of key-value pairs to the right edge of this shard's
    /// B+ tree. All keys in <paramref name="sortedEntries"/> must be greater than
    /// every existing key in the shard. Creates new leaves as needed and propagates
    /// separators into internal nodes. Used by the streaming bulk-load extension method.
    /// </summary>
    /// <param name="operationId">Unique ID for idempotency. Retries with the same ID are no-ops.</param>
    Task BulkAppendAsync(string operationId, List<KeyValuePair<string, byte[]>> sortedEntries);

    /// <summary>
    /// Marks this shard as deleted. Subsequent reads and writes will throw
    /// <see cref="InvalidOperationException"/>. Idempotent.
    /// </summary>
    Task MarkDeletedAsync();

    /// <summary>
    /// Clears the deleted flag on this shard, restoring it to normal operation.
    /// Idempotent.
    /// </summary>
    Task UnmarkDeletedAsync();

    /// <summary>
    /// Returns <c>true</c> if this shard has been marked as deleted.
    /// </summary>
    Task<bool> IsDeletedAsync();

    /// <summary>
    /// Permanently purges all grains in this shard (leaves, internal nodes)
    /// by clearing their persistent state and deactivating them, then clears
    /// the shard root's own state. Called by <see cref="ITreeDeletionGrain"/>
    /// after the soft-delete window has elapsed.
    /// </summary>
    Task PurgeAsync();
}
