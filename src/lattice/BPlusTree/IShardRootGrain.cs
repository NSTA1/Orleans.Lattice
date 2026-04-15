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
}
