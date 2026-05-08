using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Grains;

/// <summary>
/// Per-tree local vector clock grain. Generalises the per-origin
/// high-water-mark table as the diagonal of a sparse vector clock so
/// receivers can serve both the existing point dedup check
/// (<see cref="GetAsync(string, CancellationToken)"/>) and the
/// causal-plus dependency check
/// (<see cref="GetVectorAsync(CancellationToken)"/>) from a single
/// piece of persistent state.
/// <para>
/// Grain key format: <c>{treeId}</c>. The receiver-side
/// <see cref="IReplicationApplier"/> resolves the grain by the WAL
/// entry's <see cref="WalRecord.TreeId"/> alone; the origin is
/// passed as a method argument so a single grain activation handles
/// every <c>(tree, origin)</c> pair for that tree.
/// </para>
/// <para>
/// Each origin's diagonal entry is monotonically non-decreasing under
/// every concurrent append. <see cref="TryAdvanceAsync"/> is the only
/// way to grow it during steady-state apply;
/// <see cref="PinSnapshotAsync"/> sets the entire vector
/// unconditionally and is intended for the bootstrap-snapshot handoff
/// (where the snapshot's <c>asOfHlc</c> is by construction the highest
/// HLC the receiver should consider applied for the originating
/// cluster after restore, and the <c>frontier</c> is the snapshot's
/// causal-stable frontier).
/// </para>
/// </summary>
[Alias(ReplicationTypeAliases.IReplicationHighWaterMarkGrain)]
internal interface IReplicationHighWaterMarkGrain : IGrainWithStringKey
{
    /// <summary>
    /// Returns the diagonal entry for the
    /// <c>(this tree, <paramref name="originClusterId"/>)</c> pair, or
    /// <see cref="HybridLogicalClock.Zero"/> when no entry has been
    /// applied yet for that origin.
    /// </summary>
    /// <param name="originClusterId">
    /// The origin cluster id whose diagonal entry to read. Must be
    /// non-null and non-empty.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<HybridLogicalClock> GetAsync(string originClusterId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns a snapshot of the full local vector clock for this tree.
    /// The returned instance is a defensive copy; callers may mutate it
    /// without affecting the grain's persistent state.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<VersionVector> GetVectorAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Advances the diagonal entry for
    /// <paramref name="originClusterId"/> to
    /// <paramref name="candidate"/> if and only if
    /// <paramref name="candidate"/> is strictly greater than the
    /// current value. Returns <c>true</c> when the entry was advanced
    /// and persisted; <c>false</c> when the candidate was less than or
    /// equal to the current value (re-delivery of an already-applied
    /// entry).
    /// </summary>
    /// <param name="originClusterId">
    /// The origin cluster id whose diagonal entry to advance. Must be
    /// non-null and non-empty.
    /// </param>
    /// <param name="candidate">The candidate HLC.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<bool> TryAdvanceAsync(string originClusterId, HybridLogicalClock candidate, CancellationToken cancellationToken = default);

    /// <summary>
    /// Replaces the local vector clock with
    /// <paramref name="frontier"/> unconditionally. Intended for the
    /// bootstrap-snapshot handoff: a newly-bootstrapped peer pins the
    /// vector to the snapshot's causal-stable frontier, then resumes
    /// incremental replication from that pinned frontier with
    /// exactly-once apply guarantees across the snapshot / incremental
    /// boundary. The <paramref name="asOfHlc"/> argument carries the
    /// snapshot's authoring HLC (the <c>as-of</c> HLC the snapshot
    /// scan was produced at) for diagnostic and protocol purposes; it
    /// is preserved in the call shape so a future bootstrap protocol
    /// extension can use it without a signature break. Idempotent at
    /// the value level: pinning the same frontier twice writes once.
    /// </summary>
    /// <param name="asOfHlc">
    /// The snapshot's authoring HLC. Carried verbatim in the call
    /// shape; future protocol revisions may use it to gate the pin or
    /// emit observability around the snapshot point. The grain itself
    /// does not consult it - the <paramref name="frontier"/> is the
    /// authoritative new vector.
    /// </param>
    /// <param name="frontier">
    /// The new local vector clock. Must be non-null. The grain stores
    /// a defensive copy; subsequent mutations to the supplied instance
    /// do not affect grain state.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task PinSnapshotAsync(HybridLogicalClock asOfHlc, VersionVector frontier, CancellationToken cancellationToken = default);
}
