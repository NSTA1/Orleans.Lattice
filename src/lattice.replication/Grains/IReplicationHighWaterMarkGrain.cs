using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Per-origin high-water-mark grain. Tracks the highest
/// <see cref="HybridLogicalClock"/> the receiver has applied for a
/// given <c>(treeId, originClusterId)</c> pair so re-delivery of an
/// already-applied entry is a no-op.
/// <para>
/// Grain key format: <c>{treeId}/{originClusterId}</c>. Both segments
/// are required and non-empty; the receiver-side
/// <see cref="IReplicationApplier"/> resolves the grain by composing
/// the WAL entry's <see cref="ReplogEntry.TreeId"/> and
/// <see cref="ReplogEntry.OriginClusterId"/>.
/// </para>
/// <para>
/// The HWM is monotonically non-decreasing under every concurrent
/// append. <see cref="TryAdvanceAsync"/> is the only way to grow it
/// during steady-state apply; <see cref="PinSnapshotAsync"/> sets the
/// HWM unconditionally and is intended for the bootstrap-snapshot
/// handoff (where the snapshot's <c>asOfHlc</c> is by construction the
/// highest HLC the receiver should consider applied for the origin
/// after restore).
/// </para>
/// </summary>
[Alias(ReplicationTypeAliases.IReplicationHighWaterMarkGrain)]
internal interface IReplicationHighWaterMarkGrain : IGrainWithStringKey
{
    /// <summary>
    /// Returns the current high-water-mark for this
    /// <c>(tree, origin)</c> pair, or <see cref="HybridLogicalClock.Zero"/>
    /// when no entry has been applied yet.
    /// </summary>
    Task<HybridLogicalClock> GetAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Advances the high-water-mark to <paramref name="candidate"/> if
    /// and only if <paramref name="candidate"/> is strictly greater than
    /// the current value. Returns <c>true</c> when the HWM was advanced
    /// and persisted; <c>false</c> when the candidate was less than or
    /// equal to the current HWM (re-delivery of an already-applied
    /// entry).
    /// </summary>
    Task<bool> TryAdvanceAsync(HybridLogicalClock candidate, CancellationToken cancellationToken = default);

    /// <summary>
    /// Sets the high-water-mark to <paramref name="asOfHlc"/>
    /// unconditionally. Intended for the bootstrap-snapshot handoff: a
    /// newly-bootstrapped peer pins the HWM to the snapshot's authoring
    /// HLC, then resumes incremental replication from
    /// <paramref name="asOfHlc"/> with exactly-once apply guarantees
    /// across the snapshot / incremental boundary. Idempotent.
    /// </summary>
    Task PinSnapshotAsync(HybridLogicalClock asOfHlc, CancellationToken cancellationToken = default);
}
