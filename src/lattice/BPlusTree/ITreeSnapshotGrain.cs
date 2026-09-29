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
[Alias(TypeAliases.ITreeSnapshotGrain)]
internal interface ITreeSnapshotGrain : IGrainWithStringKey
{
    /// <summary>
    /// Initiates a snapshot of this tree into <paramref name="destinationTreeId"/>.
    /// <para>
    /// Idempotent - if a snapshot is already in progress to the same destination
    /// with the same mode, this is a no-op. Calling with different parameters
    /// while a snapshot is in progress throws <see cref="InvalidOperationException"/>.
    /// </para>
    /// </summary>
    /// <param name="destinationTreeId">The ID for the new tree. Must not already exist.</param>
    /// <param name="mode">Whether to lock the source tree during the snapshot.</param>
    /// <param name="maxLeafKeys">Optional sizing override for the destination tree. If <c>null</c>, uses the library default.</param>
    /// <param name="maxInternalChildren">Optional sizing override for the destination tree. If <c>null</c>, uses the library default.</param>
    Task SnapshotAsync(string destinationTreeId, SnapshotMode mode, int? maxLeafKeys = null, int? maxInternalChildren = null);

    /// <summary>
    /// Internal overload used by coordinator grains (e.g. <c>TreeResizeGrain</c>)
    /// that need the snapshot to share an <c>operationId</c> with a
    /// shadow-forward they intend to manage themselves. The supplied
    /// <paramref name="operationId"/> is stamped onto every source shard's
    /// shadow-forward state so the caller can later invoke
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.EnterRejectingAsync"/> or
    /// <see cref="Orleans.Lattice.BPlusTree.IShardRootGrain.ClearShadowForwardAsync"/> with the same id.
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
    /// deactivates. Idempotent - a no-op if no snapshot is in progress or if
    /// the in-progress snapshot's <c>OperationId</c> doesn't match (so a stale
    /// coordinator cannot abort a newer operation).
    /// </summary>
    /// <param name="operationId">Operation ID the snapshot was started with.
    /// Must match the persisted <c>OperationId</c>, otherwise the call is a no-op.</param>
    Task AbortAsync(string operationId);

    /// <summary>
    /// Processes all remaining shards synchronously in a single call.
    /// Used for testing and manual operations.
    /// <para>
    /// The call holds the snapshot's turn until the whole copy is done, so it
    /// is unbounded in wall clock: on a large or contended tree it outlives the
    /// caller's response timeout and starves the snapshot's keepalive reminder.
    /// A coordinator that drives the snapshot from a timer tick must use
    /// <see cref="RunSnapshotSliceAsync"/> instead (issue 3904).
    /// </para>
    /// </summary>
    Task RunSnapshotPassAsync();

    /// <summary>
    /// Advances the snapshot for at most one wall-clock-bounded slice, banks its
    /// progress, and returns the turn. The slice lasts for
    /// <see cref="LatticeOptions.BackgroundDrainMaxDuration"/>, capped at ten
    /// seconds (and ten seconds when that option is zero), so it returns well
    /// inside the default thirty-second response timeout and leaves the turn free
    /// for the keepalive reminder between slices. A shard whose copy is still
    /// running when the slice expires stops at the next leaf and resumes from its
    /// persisted key on the next slice. An online copy still drains up to
    /// <see cref="LatticeOptions.MaxConcurrentDrains"/> shards at once
    /// (issue 3904).
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when no snapshot is in progress any more (the copy
    /// has completed, or none was running); <see langword="false"/> when work
    /// remains and the caller should call again.
    /// </returns>
    Task<bool> RunSnapshotSliceAsync();

    /// <summary>
    /// Returns <c>true</c> when the coordinator is idle - either no snapshot
    /// has ever been initiated, or the last one has run to completion.
    /// Returns <c>false</c> while a snapshot is in flight.
    /// </summary>
    Task<bool> IsIdleAsync();

    /// <summary>
    /// Reports how far the snapshot has durably got: its phase and the shards it
    /// has finished copying out of those it copies. Answers from the snapshot state
    /// as last persisted, never from a transition a turn has applied in memory but
    /// not yet written, so it never runs ahead of work a reactivated coordinator
    /// would resume from. Marked
    /// <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/> so a status read
    /// is not held behind a copy slice. A pure read.
    /// </summary>
    [Orleans.Concurrency.AlwaysInterleave]
    Task<SnapshotProgress> GetProgressAsync();
}
