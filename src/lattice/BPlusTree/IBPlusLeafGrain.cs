namespace Orleans.Lattice.BPlusTree;

using System.ComponentModel;
using Orleans.Lattice.Primitives;

/// <summary>
/// A leaf node grain in the B+ tree. Stores key-value pairs as
/// <see cref="Primitives.LwwValue{T}"/> entries for monotonic conflict resolution.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public interface IBPlusLeafGrain : IGrainWithGuidKey
{
    /// <summary>Gets the value for <paramref name="key"/>, or <c>null</c> if absent/tombstoned.</summary>
    Task<byte[]?> GetAsync(string key);

    /// <summary>Returns <c>true</c> if <paramref name="key"/> exists and is not tombstoned.</summary>
    Task<bool> ExistsAsync(string key);

    /// <summary>
    /// Returns the values for the given <paramref name="keys"/>.
    /// Keys that do not exist or are tombstoned are omitted from the result.
    /// </summary>
    Task<Dictionary<string, byte[]>> GetManyAsync(List<string> keys);

    /// <summary>
    /// Inserts or updates a key-value pair.
    /// Returns a <see cref="SplitResult"/> if the leaf split as a consequence, otherwise <c>null</c>.
    /// </summary>
    Task<SplitResult?> SetAsync(string key, byte[] value);

    /// <summary>
    /// Sets <paramref name="key"/> to <paramref name="value"/> only if the key does not
    /// already exist (or is tombstoned). Returns a <see cref="GetOrSetResult"/> containing
    /// the existing value when the key is live, or <c>null</c> existing value when the
    /// write was performed.
    /// </summary>
    Task<GetOrSetResult> GetOrSetAsync(string key, byte[] value);

    /// <summary>
    /// Inserts or updates multiple key-value pairs.
    /// Returns the last <see cref="SplitResult"/> if any split occurred, otherwise <c>null</c>.
    /// </summary>
    Task<SplitResult?> SetManyAsync(List<KeyValuePair<string, byte[]>> entries);

    /// <summary>
    /// Marks <paramref name="key"/> as deleted (tombstone).
    /// Returns <c>true</c> if the key was present and live.
    /// </summary>
    Task<bool> DeleteAsync(string key);

    /// <summary>
    /// Tombstones all live keys in the range [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>).
    /// Returns the number of keys that were tombstoned.
    /// </summary>
    Task<int> DeleteRangeAsync(string startInclusive, string endExclusive);

    /// <summary>Returns the number of live (non-tombstoned) keys in this leaf.</summary>
    Task<int> CountAsync();

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

    /// <summary>Returns the tree ID this leaf is associated with, or <c>null</c> if not yet set.</summary>
    Task<string?> GetTreeIdAsync();

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
    /// Returns the sorted list of live (non-tombstoned) key-value pairs in this leaf
    /// that fall within the optional [<paramref name="startInclusive"/>, <paramref name="endExclusive"/>) range.
    /// If <paramref name="afterExclusive"/> is provided, only entries with keys strictly
    /// greater than that value are returned (used for continuation-token pagination to
    /// avoid transferring values that will be discarded by the caller).
    /// </summary>
    Task<List<KeyValuePair<string, byte[]>>> GetEntriesAsync(string? startInclusive = null, string? endExclusive = null, string? afterExclusive = null);

    /// <summary>
    /// Removes tombstones whose wall-clock age exceeds <paramref name="gracePeriod"/>.
    /// Returns the number of tombstones removed. Tracks a <c>LastCompactionVersion</c>
    /// to skip redundant scans when no writes have occurred since the last compaction.
    /// </summary>
    Task<int> CompactTombstonesAsync(TimeSpan gracePeriod);

    /// <summary>
    /// Returns all live (non-tombstoned) key-value pairs in this leaf.
    /// Used by the tree resize operation to drain entries before purging.
    /// </summary>
    Task<Dictionary<string, byte[]>> GetLiveEntriesAsync();

    /// <summary>
    /// Clears all persistent state for this grain and deactivates it.
    /// Used during tree purge to permanently remove leaf data.
    /// </summary>
    Task ClearGrainStateAsync();
}
