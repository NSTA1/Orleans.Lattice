namespace Orleans.Lattice;

/// <summary>
/// Recovery strategy a leaf grain takes when one of the fall-off-log
/// triggers fires at activation time (the WAL has been trimmed past
/// the leaf's persisted projection checkpoint, the offset gap exceeds
/// <see cref="LatticeOptions.MaxLeafReplayEntries"/>, or the leaf has
/// been cold past <see cref="LatticeOptions.LeafProjectionRetention"/>).
/// </summary>
public enum ProjectionRebuildPolicy
{
    /// <summary>
    /// Default. Drains the per-leaf snapshot via
    /// <c>ILeafSnapshotProvider</c> as the recovery base, persists the
    /// snapshot offset as the new checkpoint, then tail-replays the
    /// remaining WAL entries since the snapshot. Reliable: works even
    /// when the WAL has been trimmed below the leaf''s previous
    /// checkpoint.
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

    /// <summary>
    /// Opt-in, for derived and rebuildable trees only. When a fall-off-log
    /// trigger fires (including the cold-replay durable-frontier guard - the
    /// WAL has been trimmed past the leaf''s persisted checkpoint and no
    /// covering snapshot exists), the leaf <b>accepts the loss</b> of the
    /// trimmed prefix and rebuilds its projection from the surviving WAL
    /// suffix (from the oldest still-readable offset) instead of failing
    /// closed with <see cref="LeafProjectionStaleException"/>.
    /// <para>
    /// This is safe <b>only</b> for a tree whose contents can be
    /// re-derived from an authoritative source after the rebuild - the
    /// canonical case being the content-addressed embedding-vector
    /// projections of a repository-context host, which a downstream
    /// gap-scan re-ingests. Applying it to a store-of-record tree would
    /// silently and permanently drop committed keys, so it must never be
    /// the default: <see cref="SnapshotThenWal"/> remains the fail-closed
    /// default and this policy is enabled per tree via
    /// <c>ConfigureLattice</c>.
    /// </para>
    /// </summary>
    RebuildFromWalAcceptLoss = 3,
}
