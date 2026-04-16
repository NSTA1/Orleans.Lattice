namespace Orleans.Lattice.BPlusTree;

using Orleans.Lattice.Primitives;

/// <summary>
/// A leaf node grain in the B+ tree. Stores key-value pairs as
/// <see cref="Primitives.LwwValue{T}"/> entries for monotonic conflict resolution.
/// </summary>
public interface IBPlusLeafGrain : IGrainWithGuidKey
{
    /// <summary>Gets the value for <paramref name="key"/>, or <c>null</c> if absent/tombstoned.</summary>
    Task<byte[]?> GetAsync(string key);

    /// <summary>Returns <c>true</c> if <paramref name="key"/> exists and is not tombstoned.</summary>
    Task<bool> ExistsAsync(string key);

    /// <summary>
    /// Inserts or updates a key-value pair.
    /// Returns a <see cref="SplitResult"/> if the leaf split as a consequence, otherwise <c>null</c>.
    /// </summary>
    Task<SplitResult?> SetAsync(string key, byte[] value);

    /// <summary>
    /// Marks <paramref name="key"/> as deleted (tombstone).
    /// Returns <c>true</c> if the key was present and live.
    /// </summary>
    Task<bool> DeleteAsync(string key);

    /// <summary>Returns the grain identity of the right sibling leaf, or <c>null</c>.</summary>
    Task<GrainId?> GetNextSiblingAsync();

    /// <summary>Sets the right sibling pointer (called during splits).</summary>
    Task SetNextSiblingAsync(GrainId? siblingId);

    /// <summary>Returns the grain identity of the left sibling leaf, or <c>null</c>.</summary>
    Task<GrainId?> GetPrevSiblingAsync();

    /// <summary>Sets the left sibling pointer (called during splits).</summary>
    Task SetPrevSiblingAsync(GrainId? siblingId);

    /// <summary>
    /// Associates this leaf with a tree, enabling named options resolution.
    /// Called once by the shard root after creating the grain. Idempotent.
    /// </summary>
    Task SetTreeIdAsync(string treeId);

    /// <summary>
    /// Returns a <see cref="StateDelta"/> containing all entries whose timestamp is
    /// newer than what <paramref name="sinceVersion"/> has seen.
    /// Returns an empty delta if the caller is already up to date.
    /// </summary>
    Task<StateDelta> GetDeltaSinceAsync(VersionVector sinceVersion);

    /// <summary>
    /// Bulk-merges entries (including tombstones) into this leaf using LWW semantics,
    /// preserving original timestamps. Used during splits to transfer entries without
    /// re-stamping them. Idempotent — re-merging the same entries is a no-op.
    /// </summary>
    Task MergeEntriesAsync(Dictionary<string, LwwValue<byte[]>> entries);

    /// <summary>
    /// Returns the sorted list of live (non-tombstoned) keys in this leaf
    /// that fall within the optional [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>) range.
    /// </summary>
    Task<List<string>> GetKeysAsync(string? startInclusive = null, string? endExclusive = null);

    /// <summary>
    /// Removes tombstones whose wall-clock age exceeds <paramref name="gracePeriod"/>.
    /// Returns the number of tombstones removed. Tracks a <c>LastCompactionVersion</c>
    /// to skip redundant scans when no writes have occurred since the last compaction.
    /// </summary>
    Task<int> CompactTombstonesAsync(TimeSpan gracePeriod);

    /// <summary>
    /// Clears all persistent state for this grain and deactivates it.
    /// Used during tree purge to permanently remove leaf data.
    /// </summary>
    Task ClearGrainStateAsync();
}
