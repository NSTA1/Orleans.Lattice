using System.ComponentModel;
using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// A grain responsible for snapshotting a source tree into a new destination tree.
/// One activation exists per source tree, keyed by <c>{sourceTreeId}</c>.
/// <para>
/// Snapshot works shard-by-shard: each source shard's leaf chain is drained into
/// memory and bulk-loaded into the corresponding destination shard. In offline mode,
/// source shards are marked as deleted during the copy and unmarked upon completion.
/// In online mode, the source tree remains available throughout.
/// </para>
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
[Alias(TypeAliases.ITreeSnapshotGrain)]
public interface ITreeSnapshotGrain : IGrainWithStringKey
{
    /// <summary>
    /// Initiates a snapshot of this tree into <paramref name="destinationTreeId"/>.
    /// <para>
    /// Idempotent — if a snapshot is already in progress to the same destination
    /// with the same mode, this is a no-op. Calling with different parameters
    /// while a snapshot is in progress throws <see cref="InvalidOperationException"/>.
    /// </para>
    /// </summary>
    /// <param name="destinationTreeId">The ID for the new tree. Must not already exist.</param>
    /// <param name="mode">Whether to lock the source tree during the snapshot.</param>
    /// <param name="maxLeafKeys">Optional sizing override for the destination tree. If <c>null</c>, uses the source tree's options.</param>
    /// <param name="maxInternalChildren">Optional sizing override for the destination tree. If <c>null</c>, uses the source tree's options.</param>
    Task SnapshotAsync(string destinationTreeId, SnapshotMode mode, int? maxLeafKeys = null, int? maxInternalChildren = null);

    /// <summary>
    /// Processes all remaining shards synchronously in a single call.
    /// Used for testing and manual operations.
    /// </summary>
    Task RunSnapshotPassAsync();

    /// <summary>
    /// Returns <c>true</c> if no snapshot is in progress — either the most recent
    /// snapshot has completed or no snapshot has ever been initiated (vacuously complete).
    /// </summary>
    Task<bool> IsCompleteAsync();
}
