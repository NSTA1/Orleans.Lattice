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
}
