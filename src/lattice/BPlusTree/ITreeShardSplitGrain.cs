using System.ComponentModel;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Coordinator grain that drives a single online adaptive split for
/// a tree: shadow-write → drain → swap → reject → cleanup → complete.
/// One coordinator activation per (tree, source-shard) pair, so multiple
/// splits can run in parallel for a single tree up to
/// <see cref="LatticeOptions.MaxConcurrentAutoSplits"/>. Subsequent calls
/// during an in-flight split are idempotent for matching parameters and
/// rejected for differing parameters.
/// <para>
/// Key format: <c>{treeId}/{sourceShardIndex}</c>.
/// </para>
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
[Alias(TypeAliases.ITreeShardSplitGrain)]
public interface ITreeShardSplitGrain : IGrainWithStringKey
{
    /// <summary>
    /// Initiates an online adaptive split that migrates the upper half of
    /// <paramref name="sourceShardIndex"/>'s virtual slots to a newly
    /// allocated physical shard. Returns once the split intent has been
    /// persisted; the actual phase progression runs asynchronously, anchored
    /// by a reminder so it survives silo restarts.
    /// <para>
    /// Idempotent: if a split for the same source shard is already in
    /// progress, this call is a no-op. Throws
    /// <see cref="InvalidOperationException"/> if a split for a different
    /// source shard is in progress.
    /// </para>
    /// </summary>
    Task SplitAsync(int sourceShardIndex);

    /// <summary>
    /// Synchronously runs all remaining phases of the in-progress split to
    /// completion. Used by tests and by the monitor when it wants to wait
    /// for a triggered split. No-op if no split is in progress.
    /// </summary>
    Task RunSplitPassAsync();

    /// <summary>
    /// Returns <c>true</c> when the coordinator is idle — either no split
    /// has ever been initiated, or the last one has run to completion.
    /// Returns <c>false</c> while a split is in flight.
    /// </summary>
    Task<bool> IsIdleAsync();
}
