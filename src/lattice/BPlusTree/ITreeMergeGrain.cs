using System.ComponentModel;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A grain responsible for merging all entries from a source tree into
/// a target tree using LWW semantics, preserving original timestamps.
/// One activation exists per target tree, keyed by <c>{targetTreeId}</c>.
/// <para>
/// Merge works shard-by-shard: each source shard's leaf chain is drained
/// into memory (including tombstones with LWW metadata) and then merged
/// into the target tree's shards via <see cref="IShardRootGrain.MergeManyAsync"/>.
/// Entries are re-hashed to the correct target shard since source and target
/// trees may have different shard counts.
/// </para>
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
[Alias(TypeAliases.ITreeMergeGrain)]
public interface ITreeMergeGrain : IGrainWithStringKey
{
    /// <summary>
    /// Initiates a merge of all entries from <paramref name="sourceTreeId"/>
    /// into this grain's target tree.
    /// <para>
    /// Idempotent — if a merge is already in progress from the same source,
    /// this is a no-op. Calling with a different source while a merge is
    /// in progress throws <see cref="InvalidOperationException"/>.
    /// </para>
    /// </summary>
    /// <param name="sourceTreeId">The source tree to merge from. Must exist and not be the same as the target.</param>
    Task MergeAsync(string sourceTreeId);

    /// <summary>
    /// Processes all remaining source shards synchronously in a single call.
    /// Used for testing and manual operations.
    /// </summary>
    Task RunMergePassAsync();

    /// <summary>
    /// Returns <c>true</c> if no merge is in progress — either the most recent
    /// merge has completed or no merge has ever been initiated (vacuously complete).
    /// </summary>
    Task<bool> IsCompleteAsync();
}
