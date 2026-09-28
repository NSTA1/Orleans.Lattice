namespace Orleans.Lattice;

/// <summary>
/// Recovery strategy a leaf grain takes when the write-ahead log has been
/// trimmed past the leaf's persisted projection checkpoint and no snapshot
/// covers the gap - that is, when recovery data is genuinely unavailable.
/// Every value currently fails closed there, surfacing
/// <see cref="LeafProjectionStaleException"/>; the values differ only in the
/// recovery they are intended to select.
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
    /// Intended to recover from the per-leaf snapshot as the recovery base
    /// and then tail-replay the remaining WAL entries since the snapshot.
    /// <para>
    /// <b>Status.</b> The snapshot half of that is not specific to this
    /// policy: the per-leaf snapshot rehydrate runs at the start of every
    /// activation under every policy, before the loss check, reloading the
    /// snapshot's prefix and letting the tail replay cover the remainder. What
    /// is <b>not</b> integrated is a recovery that runs <i>after</i> that
    /// rehydrate has already declined: when the WAL has genuinely been trimmed
    /// past the checkpoint and no snapshot covers the gap, the leaf surfaces
    /// <see cref="LeafProjectionStaleException"/> under this policy too, rather
    /// than reconstructing the lost prefix. Failing closed there is deliberate
    /// - replaying only the surviving suffix would rebuild the leaf over the
    /// lost prefix and advance the materialiser pin past unrecoverable data.
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
    /// Diagnostic. Intended to replay from the absolute tail of the WAL. Because
    /// this policy is consulted only on genuine loss - the WAL has been trimmed
    /// past the checkpoint and no snapshot covers the gap, so a complete history
    /// is unavailable - the leaf always surfaces
    /// <see cref="LeafProjectionStaleException"/> under it; no full-rebuild recovery path is integrated.
    /// </summary>
    FullRebuildFromWal = 1,

    /// <summary>
    /// Operator-gated. Surfaces a <see cref="LeafProjectionStaleException"/>
    /// at activation time and waits for an operator: restore the tree from a
    /// backup, or accept the loss of the trimmed range and run
    /// <see cref="ILattice.RebuildLeafProjectionAsync"/>, which rebuilds the
    /// leaf from what survives. The other two values currently behave the same
    /// way.
    /// </summary>
    Fail = 2,
}
