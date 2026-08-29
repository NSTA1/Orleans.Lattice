namespace Orleans.Lattice;

/// <summary>
/// Recovery strategy a leaf grain takes when the write-ahead log has been
/// trimmed past the leaf's persisted projection checkpoint and no snapshot
/// covers the gap - that is, when recovery data is genuinely unavailable.
/// <para>
/// This policy is <b>not</b> consulted for the cost triggers
/// (<see cref="LatticeOptions.MaxLeafReplayEntries"/> and
/// <see cref="LatticeOptions.LeafProjectionRetention"/>). Those indicate a
/// long or stale replay, not missing data, so the leaf tail-replays and
/// converges normally; see issue #1738.
/// </para>
/// </summary>
public enum ProjectionRebuildPolicy
{
    /// <summary>
    /// Drains the per-leaf snapshot as the recovery base, persists the
    /// snapshot offset as the new checkpoint, then tail-replays the
    /// remaining WAL entries since the snapshot.
    /// <para>
    /// <b>Status.</b> The snapshot rehydrate half of this is live and runs
    /// in core at activation Step 0
    /// (<c>BPlusLeafGrain.TryRehydrateFromSnapshotAsync</c>, backed by
    /// <c>ILeafSnapshotStorageGrain</c>). What is <b>not</b> integrated is a
    /// recovery that runs <i>after</i> the rehydrate has already declined:
    /// when the WAL has genuinely been trimmed past the checkpoint and no
    /// snapshot covers the gap, the leaf surfaces
    /// <see cref="LeafProjectionStaleException"/> rather than reconstructing
    /// the lost prefix. Failing closed there is deliberate - replaying only
    /// the surviving suffix would rebuild the leaf over the lost prefix and
    /// advance the materialiser pin past unrecoverable data.
    /// </para>
    /// <para>
    /// Note this policy is consulted <b>only</b> on genuine loss. A replay
    /// gap over <see cref="LatticeOptions.MaxLeafReplayEntries"/>, or a
    /// projection older than
    /// <see cref="LatticeOptions.LeafProjectionRetention"/>, is a cost signal
    /// and never reaches this policy - the leaf tail-replays instead (#1738).
    /// </para>
    /// </summary>
    SnapshotThenWal = 0,

    /// <summary>
    /// Diagnostic. Replays from the absolute tail of the WAL. Fails
    /// fast with <see cref="LeafProjectionStaleException"/> if the WAL
    /// has been trimmed and a complete history is unavailable.
    /// </summary>
    FullRebuildFromWal = 1,

    /// <summary>
    /// Operator-gated. Surfaces a <see cref="LeafProjectionStaleException"/>
    /// at activation time and requires an explicit operator-driven
    /// rebuild via the operator rebuild API.
    /// </summary>
    Fail = 2,
}
