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
    /// Internal overload used by coordinator grains (e.g. <c>TreeResizeGrain</c>)
    /// that need the snapshot to share an <c>operationId</c> with a
    /// shadow-forward they intend to manage themselves. The supplied
    /// <paramref name="operationId"/> is stamped onto every source shard's
    /// shadow-forward state so the caller can later invoke
    /// <see cref="IShardRootGrain.EnterRejectingAsync"/> or
    /// <see cref="IShardRootGrain.ClearShadowForwardAsync"/> with the same id.
    /// </summary>
    /// <param name="destinationTreeId">The ID for the new tree. Must not already exist.</param>
    /// <param name="mode">Snapshot mode. Typically <see cref="SnapshotMode.Online"/> for this overload.</param>
    /// <param name="maxLeafKeys">Optional sizing override for the destination tree.</param>
    /// <param name="maxInternalChildren">Optional sizing override for the destination tree.</param>
    /// <param name="operationId">Externally allocated shadow-forward operation id. Must be non-empty.</param>
    /// <param name="logicalTreeId">User-visible logical tree ID associated
    /// with this snapshot. Stamped onto each source shard's shadow-forward
    /// state so a post-swap <see cref="StaleTreeRoutingException"/> carries
    /// the caller's logical name. May be empty to fall back to the source
    /// physical tree ID.</param>
    Task SnapshotWithOperationIdAsync(string destinationTreeId, SnapshotMode mode,
        int? maxLeafKeys, int? maxInternalChildren, string operationId, string logicalTreeId);

    /// <summary>
    /// Aborts an in-flight snapshot under the given <paramref name="operationId"/>.
    /// Used by coordinator grains (e.g. <c>TreeResizeGrain.UndoResizeAsync</c>)
    /// that manage the snapshot's lifetime and need to tear it down before it
    /// completes. Disposes the snapshot's internal timer, unregisters the
    /// keepalive reminder, and clears all snapshot state so the grain
    /// deactivates. Idempotent — a no-op if no snapshot is in progress or if
    /// the in-progress snapshot's <c>OperationId</c> doesn't match (so a stale
    /// coordinator cannot abort a newer operation).
    /// </summary>
    /// <param name="operationId">Operation ID the snapshot was started with.
    /// Must match the persisted <c>OperationId</c>, otherwise the call is a no-op.</param>
    Task AbortAsync(string operationId);

    /// <summary>
    /// Processes all remaining shards synchronously in a single call.
    /// Used for testing and manual operations.
    /// </summary>
    Task RunSnapshotPassAsync();

    /// <summary>
    /// Returns <c>true</c> when the coordinator is idle — either no snapshot
    /// has ever been initiated, or the last one has run to completion.
    /// Returns <c>false</c> while a snapshot is in flight.
    /// </summary>
    Task<bool> IsIdleAsync();
}
