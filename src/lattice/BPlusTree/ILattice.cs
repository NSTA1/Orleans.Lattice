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

    /// <summary>Inserts or updates the value for <paramref name="key"/>.</summary>
    Task SetAsync(string key, byte[] value);

    /// <summary>Deletes the value for <paramref name="key"/>. Returns <c>true</c> if it existed.</summary>
    Task<bool> DeleteAsync(string key);

    /// <summary>
    /// Returns all live keys in the tree as an ordered async stream.
    /// Keys are returned in lexicographic order (or reverse if <paramref name="reverse"/> is <c>true</c>).
    /// Optionally filters to keys in the range [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>).
    /// </summary>
    IAsyncEnumerable<string> KeysAsync(string? startInclusive = null, string? endExclusive = null, bool reverse = false);
}
