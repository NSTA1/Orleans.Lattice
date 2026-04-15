namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A stateless-worker grain that routes requests to the correct shard root
/// based on a stable hash of the key prefix.
/// Key format: <c>{treeId}</c> — the tree this router manages.
/// </summary>
public interface IShardRouterGrain : IGrainWithStringKey
{
    /// <summary>Gets the value associated with <paramref name="key"/>, or <c>null</c> if not found.</summary>
    Task<byte[]?> GetAsync(string key);

    /// <summary>Inserts or updates the value for <paramref name="key"/>.</summary>
    Task SetAsync(string key, byte[] value);

    /// <summary>Deletes the value for <paramref name="key"/>. Returns <c>true</c> if it existed.</summary>
    Task<bool> DeleteAsync(string key);
}
