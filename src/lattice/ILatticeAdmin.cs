namespace Orleans.Lattice;

using Orleans.Concurrency;

/// <summary>
/// Cluster-wide administrative surface for a Lattice deployment. A single
/// activation per cluster, resolved via
/// <c>grainFactory.GetGrain&lt;ILatticeAdmin&gt;(LatticeConstants.AdminGrainKey)</c>.
/// Exposes operations that span every registered tree rather than a single
/// tree - currently a byte-accurate storage-usage roll-up.
/// <para>
/// Unlike <see cref="ILattice"/> (one logical grain per tree), the admin
/// grain has no per-tree key: every method reduces across the full set of
/// registered trees reported by the tree registry.
/// </para>
/// </summary>
[Alias(TypeAliases.ILatticeAdmin)]
public interface ILatticeAdmin : IGrainWithStringKey
{
    /// <summary>
    /// Returns a cluster-wide byte-accurate
    /// <see cref="ClusterStorageUsageReport"/> - the summed retained
    /// footprint across every registered tree, with a per-tree breakdown.
    /// Fans out to each tree's storage-usage aggregator
    /// (<see cref="ILattice.GetStorageUsageAsync"/>), so each tree's figure is
    /// served from that tree's short-lived cache
    /// (<see cref="LatticeOptions.StorageUsageCacheTtl"/>).
    /// <para>
    /// <see cref="ClusterStorageUsageReport.Partial"/> is set when at least
    /// one tree's report was partial (for example a WAL provider without byte
    /// accounting); the cluster total is then a lower bound.
    /// </para>
    /// <para>
    /// Marked <see cref="AlwaysInterleaveAttribute"/>: the call is a
    /// read-only cluster-wide fan-out with no per-call shared state on the
    /// admin grain, so a slow tree's deep walk must not block sibling
    /// administrative calls (a concurrent <see cref="PollWalUsageAsync"/>
    /// driven by the per-silo poller, for example).
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">Cancels the cluster-wide fan-out before it begins.</param>
    [AlwaysInterleave]
    Task<ClusterStorageUsageReport> GetTotalStorageUsageAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Cluster-wide WAL-only storage-usage refresh. Fans out across every
    /// registered tree's <i>WAL-only</i> aggregator - touching no leaf,
    /// internal-node, snapshot, or shard-root grain - and drives the
    /// <c>storage.wal_bytes</c> / <c>storage.policy.over_threshold</c>
    /// observable gauges plus the byte-pressure WAL retention policy. This
    /// is the cheap path the per-silo background poller uses on its
    /// default cadence so an idle tree is never activated by polling.
    /// <para>
    /// Callers that need byte-accurate <i>leaf-state</i> or <i>snapshot</i>
    /// totals use <see cref="GetTotalStorageUsageAsync"/> (on demand) or
    /// <see cref="RefreshStorageUsageAsync"/> (operator-driven deep
    /// refresh across every tree); both pay the activation cost of the
    /// deep leaf-walk fan-out and should not be invoked on a polling
    /// cadence.
    /// </para>
    /// <para>
    /// Marked <see cref="AlwaysInterleaveAttribute"/> for the same reason
    /// as <see cref="GetTotalStorageUsageAsync"/>: a slow tree under one
    /// poll tick must not stall the next tick, and an operator-driven
    /// <see cref="RefreshStorageUsageAsync"/> must not be parked behind
    /// a poll fan-out already in flight.
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">Cancels the WAL fan-out.</param>
    [AlwaysInterleave]
    Task PollWalUsageAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Operator-driven deep refresh of the byte-accurate storage-usage
    /// report for every registered tree. Equivalent to calling
    /// <see cref="GetTotalStorageUsageAsync"/> with caches bypassed - it
    /// fans out to every shard root, every leaf, and every snapshot
    /// storage grain. Pins every leaf grain into memory for the duration
    /// of the call, so it is reserved for explicit operator action (for
    /// example after a manual storage migration, or to validate a
    /// long-running deployment's reported figures); the background
    /// storage-usage poller never invokes it.
    /// <para>
    /// Marked <see cref="AlwaysInterleaveAttribute"/>: read-only
    /// cluster-wide fan-out with no per-call shared state on the admin
    /// grain (re-anchor writes happen on the per-shard root, not here).
    /// </para>
    /// </summary>
    /// <param name="cancellationToken">Cancels the deep cluster-wide fan-out.</param>
    /// <returns>
    /// The freshly assembled byte-accurate cluster-wide report. Identical
    /// in shape to <see cref="GetTotalStorageUsageAsync"/>'s return value.
    /// </returns>
    [AlwaysInterleave]
    Task<ClusterStorageUsageReport> RefreshStorageUsageAsync(CancellationToken cancellationToken = default);
}
