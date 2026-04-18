using System.ComponentModel;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The root grain for a single shard of a B+ tree.
/// Key format: <c>{treeId}/{shardIndex}</c>.
/// The root grain acts as the entry point for traversal; it may be an internal
/// node or (initially) a leaf node.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public interface IShardRootGrain : IGrainWithStringKey
{
    Task<byte[]?> GetAsync(string key);
    Task<bool> ExistsAsync(string key);

    /// <summary>
    /// Returns the values for the given <paramref name="keys"/>, performing a single
    /// tree traversal per distinct leaf and batching reads at each leaf.
    /// Keys that do not exist or are tombstoned are omitted from the result.
    /// </summary>
    Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys);

    /// <summary>Inserts or updates the value for <paramref name="key"/>.</summary>
    Task SetAsync(string key, byte[] value);

    /// <summary>
    /// Sets <paramref name="key"/> to <paramref name="value"/> only if the key does not
    /// already exist (or is tombstoned). Returns the existing value when the key is live,
    /// or <c>null</c> when the write was performed.
    /// </summary>
    Task<byte[]?> GetOrSetAsync(string key, byte[] value);

    /// <summary>
    /// Inserts or updates multiple key-value pairs in a single traversal batch.
    /// </summary>
    Task SetManyAsync(List<KeyValuePair<string, byte[]>> entries);

    /// <summary>
    /// Marks <paramref name="key"/> as deleted (tombstone).
    /// Returns <c>true</c> if the key was present and live.
    /// </summary>
    Task<bool> DeleteAsync(string key);

    /// <summary>
    /// Tombstones all live keys in the range [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>)
    /// by walking the leaf chain. Returns the total number of keys tombstoned.
    /// </summary>
    Task<int> DeleteRangeAsync(string startInclusive, string endExclusive);

    /// <summary>
    /// Returns the total number of live (non-tombstoned) keys in this shard's B+ tree
    /// by walking the leaf chain and summing per-leaf counts.
    /// </summary>
    Task<int> CountAsync();

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
    /// Returns a page of live key-value entries in this shard's B+ tree in sorted order,
    /// filtered to the [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>) range.
    /// Pass <paramref name="continuationToken"/> (the last key from the previous page)
    /// to resume pagination; entries with keys &gt; the token are returned.
    /// </summary>
    Task<EntriesPage> GetSortedEntriesBatchAsync(
        string? startInclusive,
        string? endExclusive,
        int pageSize,
        string? continuationToken = null);

    /// <summary>
    /// Returns a page of live key-value entries in <em>reverse</em> sorted order,
    /// filtered to the [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>) range.
    /// Pass <paramref name="continuationToken"/> (the last key from the previous page)
    /// to resume pagination; entries with keys &lt; the token are returned.
    /// </summary>
    Task<EntriesPage> GetSortedEntriesBatchReverseAsync(
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

    /// <summary>
    /// Merges entries into this shard using LWW semantics, preserving original
    /// <see cref="Orleans.Lattice.Primitives.LwwValue{T}"/> timestamps. Routes
    /// each entry to the correct leaf via tree traversal and handles splits.
    /// Used by the tree merge operation.
    /// </summary>
    Task MergeManyAsync(Dictionary<string, Orleans.Lattice.Primitives.LwwValue<byte[]>> entries);
}
