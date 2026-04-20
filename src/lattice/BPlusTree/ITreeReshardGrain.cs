using System.ComponentModel;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Coordinator grain that drives an online reshard end-to-end: iteratively
/// dispatches per-shard <see cref="ITreeShardSplitGrain"/> operations
/// against the largest-slot-owning physical shards until the tree's
/// <see cref="ShardMap"/> contains at least the target number of distinct
/// physical shards. All work happens online — the tree continues to serve
/// reads and writes throughout, and virtual-slot routing is swapped
/// atomically by each underlying split.
/// <para>
/// Key format: <c>{treeId}</c>.
/// </para>
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
[Alias(TypeAliases.ITreeReshardGrain)]
public interface ITreeReshardGrain : IGrainWithStringKey
{
    /// <summary>
    /// Initiates an online reshard that grows the tree to
    /// <paramref name="newShardCount"/> distinct physical shards. Returns
    /// once the intent has been persisted; the actual migration runs
    /// asynchronously, anchored by a reminder so it survives silo restarts.
    /// <para>
    /// Grow-only: <paramref name="newShardCount"/> must be strictly greater
    /// than the current physical shard count, and less than or equal to the
    /// tree's configured <see cref="LatticeOptions.VirtualShardCount"/>.
    /// Idempotent: if a reshard to the same target is already in progress,
    /// this call is a no-op. Throws <see cref="InvalidOperationException"/>
    /// if a reshard with a different target is in progress.
    /// </para>
    /// </summary>
    Task ReshardAsync(int newShardCount);

    /// <summary>
    /// Synchronously runs remaining migration work until either a dispatch
    /// cycle completes or the reshard finishes. Used by tests and internal
    /// callers to wait for reshard completion without polling.
    /// No-op if no reshard is in progress.
    /// </summary>
    Task RunReshardPassAsync();

    /// <summary>
    /// Returns <c>true</c> when no reshard is in progress (vacuously true
    /// when no reshard has ever been initiated).
    /// </summary>
    Task<bool> IsCompleteAsync();
}
