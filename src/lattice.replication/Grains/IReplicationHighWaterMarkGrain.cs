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
/// unconditionally and is the intra-cluster restore re-seed, a deliberate
/// rollback; <see cref="MergeBootstrapFrontierAsync"/> is the cross-cluster
/// bootstrap handoff and only ever raises coordinates (pointwise maximum
/// with the vector already held, #4464).
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
    /// Returns the legacy <em>snapshot-pinned floor</em> entry for the
    /// <c>(this tree, <paramref name="originClusterId"/>)</c> pair, or
    /// <see cref="HybridLogicalClock.Zero"/> when none is stored.
    /// <para>
    /// The receiver no longer uses a pinned floor as a drop threshold
    /// (#4463): no single HLC per origin is downward-closed over what a
    /// snapshot holds, so dropping point writes at or below one silently
    /// discarded writes the snapshot never contained. This build never
    /// reads the floor, and <see cref="PinSnapshotAsync"/> clears any floor
    /// an earlier build persisted. The method is retained so a silo still
    /// on an earlier build can call it during a rolling upgrade; it reads
    /// <see cref="HybridLogicalClock.Zero"/> (drop nothing) once a pin by
    /// this build has landed.
    /// </para>
    /// </summary>
    /// <param name="originClusterId">
    /// The origin cluster id whose pinned-floor entry to read. Must be
    /// non-null and non-empty.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<HybridLogicalClock> GetPinnedFloorAsync(string originClusterId, CancellationToken cancellationToken = default);

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
    /// <paramref name="frontier"/> unconditionally, so it can move
    /// backwards. This is the intra-cluster restore re-seed
    /// (<see cref="LatticeReplicationLocalVcSeeder"/>): a restore is a
    /// deliberate rollback, and the vector must follow the restored
    /// values' frontier. The cross-cluster bootstrap handoff must NOT use
    /// it - a receiver that already applied an origin's writes above the
    /// source's frontier would move backwards and strand parked entries
    /// (#4464) - and uses <see cref="MergeBootstrapFrontierAsync"/>
    /// instead. Installs no drop floor and clears any floor an earlier
    /// build persisted (see <see cref="GetPinnedFloorAsync"/>). The <paramref name="asOfHlc"/> argument carries the
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

    /// <summary>
    /// Installs a bootstrap snapshot's frontier by taking the
    /// <em>pointwise maximum</em> with the vector already held, and returns
    /// whether any coordinate rose. Used by the cross-cluster bootstrap
    /// handoff (<see cref="LatticeBootstrapCoordinatorGrain"/>): the receiver
    /// may already have applied an origin's writes above the source's frontier
    /// (it receives that origin directly), and replacing the vector would move
    /// it backwards, stranding an entry parked on a dependency those writes
    /// already met (#4464). Like <see cref="PinSnapshotAsync"/> it installs no
    /// drop floor and clears any floor an earlier build persisted.
    /// <para>
    /// Contrast <see cref="PinSnapshotAsync"/>, which <em>replaces</em> the
    /// vector and is the intra-cluster restore re-seed
    /// (<see cref="LatticeReplicationLocalVcSeeder"/>): a restore is a
    /// deliberate rollback, so the vector must be able to move backwards to
    /// the restored values' frontier.
    /// </para>
    /// </summary>
    /// <param name="asOfHlc">The snapshot's authoring HLC; reserved, not consulted.</param>
    /// <param name="frontier">The snapshot frontier to merge. Must be non-null.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<bool> MergeBootstrapFrontierAsync(HybridLogicalClock asOfHlc, VersionVector frontier, CancellationToken cancellationToken = default);

    /// <summary>
    /// Records that the writes of <paramref name="originClusterId"/> at each HLC
    /// in <paramref name="applied"/> were merged into this tree, and - when
    /// <paramref name="advanceHighWaterMark"/> is set - advances the origin's
    /// high-water mark to <paramref name="highest"/> as <see cref="TryAdvanceAsync"/>
    /// does, in one call (issue #4586). A recorded identity meets every
    /// dependency that names it at once. The record is in memory only and
    /// bounded by <see cref="LatticeReplicationOptions.CausalAppliedIdentityCapacity"/>
    /// per origin; a forgotten identity is decided by the origin's frontier
    /// instead. Call it only after the writes were merged at the leaf; never for a
    /// saga prepare, which is not visible until its terminal.
    /// </summary>
    /// <param name="originClusterId">The origin of the applied writes. Must be non-null and non-empty.</param>
    /// <param name="highest">The highest HLC applied, for the high-water-mark advance.</param>
    /// <param name="applied">The applied writes' source HLCs. Must be non-null.</param>
    /// <param name="advanceHighWaterMark">Whether to advance the high-water mark as well.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Whether the high-water mark moved.</returns>
    Task<bool> AdvanceAppliedAsync(
        string originClusterId,
        HybridLogicalClock highest,
        IReadOnlyList<HybridLogicalClock> applied,
        bool advanceHighWaterMark,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Durably records that the write of <paramref name="originClusterId"/> at
    /// <paramref name="timestamp"/> was acknowledged and then lost for good - an
    /// operator discarded it from the dead-letter queue (#4603). The mark lives on
    /// the origin's <see cref="IReplicationOriginFrontierGrain"/>, because a
    /// dependency names an origin's write and not a tree (#4586); from then on
    /// <see cref="CheckDependenciesAsync"/> on any tree reports a dependent of it
    /// as <see cref="CausalDependencyVerdict.Lost"/>. Idempotent.
    /// </summary>
    /// <param name="originClusterId">The lost write's origin. Must be non-null and non-empty.</param>
    /// <param name="timestamp">The lost write's source HLC.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task RecordLostAsync(string originClusterId, HybridLogicalClock timestamp, CancellationToken cancellationToken = default);

    /// <summary>
    /// Checks each dependency vector in <paramref name="dependencies"/> (as
    /// produced by <see cref="CausalApplyBuffer.RequiredDependencies"/>) and
    /// returns one verdict per vector, in order (issue #4586). A dependency
    /// <c>(o, t)</c> names exactly one write. It is met when this tree recorded
    /// that write as applied (<see cref="AdvanceAppliedAsync"/>); otherwise the
    /// origin's <see cref="IReplicationOriginFrontierGrain"/> decides it, by
    /// <see cref="CausalFrontierCore.Decide"/>. The high-water mark is never
    /// consulted: a per-origin maximum HLC is not downward-closed (#1060).
    /// A vector is <see cref="CausalDependencyVerdict.Lost"/> when any dependency
    /// is lost, otherwise <see cref="CausalDependencyVerdict.Unmet"/> when any is
    /// unmet, otherwise <see cref="CausalDependencyVerdict.Met"/>.
    /// </summary>
    /// <param name="dependencies">The dependency vectors to check. Must be non-null.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<CausalDependencyVerdict[]> CheckDependenciesAsync(IReadOnlyList<VersionVector> dependencies, CancellationToken cancellationToken = default);
}
