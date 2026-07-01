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
    /// Marked <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/>: the call is a
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
    /// totals use <see cref="GetTotalStorageUsageAsync"/>, which reads each
    /// shard root's O(1) incrementally-maintained byte totals (it activates
    /// shard roots but never walks the leaf chain) and is therefore safe to
    /// drive on the slower, optional
    /// <see cref="LatticeOptions.StorageUsageDeepPollInterval"/> cadence. Only
    /// <see cref="RefreshStorageUsageAsync"/> pays the activation cost of the
    /// deep leaf-walk fan-out and must not be invoked on a polling cadence.
    /// </para>
    /// <para>
    /// Marked <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/> for the same reason
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
    /// Marked <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/>: read-only
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

    /// <summary>
    /// Returns the current durable WAL placement for <paramref name="treeId"/> -
    /// which <see cref="IWalStorageProviderCatalog"/> key backs each WAL
    /// partition and the placement version to use when moving a partition.
    /// </summary>
    /// <param name="treeId">The tree to inspect.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    [AlwaysInterleave]
    Task<WalPlacement> GetWalPlacementAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Audits <paramref name="treeId"/>'s WAL placement against this silo's
    /// <see cref="IWalStorageProviderCatalog"/>, flagging any partition pinned
    /// to a provider key the silo cannot resolve. Use this to detect
    /// configuration drift (a silo missing a key another silo registered) before
    /// WAL shards begin to fail closed.
    /// </summary>
    /// <param name="treeId">The tree to audit.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    [AlwaysInterleave]
    Task<WalPlacementAudit> AuditWalPlacementAsync(string treeId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Computes a read-only preview of moving partition
    /// <paramref name="partition"/> of <paramref name="treeId"/> to
    /// <paramref name="targetProviderKey"/> - the range that would be copied and
    /// whether the target resolves - without quiescing the partition or changing
    /// any placement.
    /// </summary>
    /// <param name="treeId">The tree to inspect.</param>
    /// <param name="partition">The WAL partition to preview a move for.</param>
    /// <param name="targetProviderKey">The catalog key the partition would be moved to.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    [AlwaysInterleave]
    Task<WalMovePlan> PlanWalMoveAsync(string treeId, int partition, string targetProviderKey, CancellationToken cancellationToken = default);

    /// <summary>
    /// Computes a read-only preview of moving several partitions of
    /// <paramref name="treeId"/> in one batch - one <see cref="WalMovePlan"/> per
    /// requested <c>(partition, targetProviderKey)</c> pair - without quiescing any
    /// partition or changing any placement. Use it to review a wholesale
    /// relocation (and confirm every target key resolves) before committing.
    /// <para>
    /// Throws <see cref="ArgumentException"/> when <paramref name="moves"/> is
    /// empty or names a partition more than once.
    /// </para>
    /// </summary>
    /// <param name="treeId">The tree to inspect.</param>
    /// <param name="moves">The <c>(partition, targetProviderKey)</c> pairs to preview.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    [AlwaysInterleave]
    Task<WalMoveBatchPlan> PlanWalMoveAsync(string treeId, IEnumerable<(int Partition, string TargetProviderKey)> moves, CancellationToken cancellationToken = default);

    /// <summary>
    /// Moves partition <paramref name="partition"/> of <paramref name="treeId"/>
    /// to <paramref name="targetProviderKey"/>: quiesces the source WAL shard,
    /// copies its retained tail to the target (preserving offsets and the source
    /// trim floor), optionally verifies the copy, atomically flips the placement
    /// pin with compare-and-swap, then forces the shard to deactivate so the
    /// next activation routes to the target.
    /// <para>
    /// The source partition is <b>never trimmed</b> by this call: the moved
    /// range remains on the source so the move can be reverted (move the
    /// partition back) until an explicit
    /// <see cref="ReclaimMovedWalSourceAsync"/> discards it. The operation is
    /// idempotent - re-executing a move whose pin already points at the target
    /// re-runs the post-flip repair without copying again.
    /// </para>
    /// <para>
    /// To revert a move, call this method again with the partition's original
    /// provider key as <paramref name="targetProviderKey"/>.
    /// </para>
    /// <para>
    /// <b>Not</b> marked <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/>: the move saga
    /// mutates placement and must run as a non-reentrant turn on the admin
    /// singleton so two concurrent moves of the same partition cannot interleave.
    /// </para>
    /// </summary>
    /// <param name="treeId">The tree whose partition to move.</param>
    /// <param name="partition">The WAL partition to move.</param>
    /// <param name="targetProviderKey">The catalog key to move the partition to.</param>
    /// <param name="options">Move tunables, or <see langword="null"/> for <see cref="WalMoveOptions.Default"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<WalMoveReceipt> ExecuteWalMoveAsync(string treeId, int partition, string targetProviderKey, WalMoveOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Moves several WAL partitions of <paramref name="treeId"/> in one
    /// all-or-nothing batch. Each requested <c>(partition, targetProviderKey)</c>
    /// pair runs the same quiesce-copy-verify phases as the single-partition
    /// <see cref="ExecuteWalMoveAsync(string, int, string, WalMoveOptions?, CancellationToken)"/>,
    /// bounded by <see cref="WalMoveOptions.MaxConcurrentPartitionMoves"/>, and the
    /// placement pin then flips <b>once</b> under a single compare-and-swap that
    /// applies every reassignment together. No intermediate placement is ever
    /// observable: either all moved partitions flip or the whole batch rolls back.
    /// <para>
    /// Any per-partition phase failure aborts the entire batch: the pin is never
    /// flipped, every fenced source is released back to service, and the partial
    /// target copies are retained so a re-execute resumes without recopying. As
    /// with a single move, <b>no source is trimmed</b> - reclaim each moved source
    /// explicitly with <see cref="ReclaimMovedWalSourceAsync"/>. Partitions already
    /// pinned to their requested target are an idempotent no-copy repair.
    /// </para>
    /// <para>
    /// Throws <see cref="ArgumentException"/> when <paramref name="moves"/> is
    /// empty or names a partition more than once, and
    /// <see cref="LatticeWalProviderMissingException"/> (before touching any log)
    /// when any target key is unresolvable on this silo.
    /// </para>
    /// <para>
    /// <b>Not</b> marked <see cref="Orleans.Concurrency.AlwaysInterleaveAttribute"/>: the batch saga
    /// mutates placement and must run as a non-reentrant turn on the admin
    /// singleton so two concurrent moves cannot interleave.
    /// </para>
    /// </summary>
    /// <param name="treeId">The tree whose partitions to move.</param>
    /// <param name="moves">The <c>(partition, targetProviderKey)</c> pairs to move together.</param>
    /// <param name="options">Move tunables, or <see langword="null"/> for <see cref="WalMoveOptions.Default"/>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<WalMoveBatchReceipt> ExecuteWalMoveAsync(string treeId, IEnumerable<(int Partition, string TargetProviderKey)> moves, WalMoveOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Discards the orphaned tail a completed <see cref="ExecuteWalMoveAsync"/>
    /// left on a partition's former source provider. This is the explicit,
    /// irreversible second step of a move: after calling it the move can no
    /// longer be reverted by moving the partition back, because the source no
    /// longer holds the data.
    /// <para>
    /// Fails closed if the placement pin still maps the partition to
    /// <paramref name="sourceProviderKey"/> (reclaiming the live provider would
    /// destroy the active log); the partition must already have been moved away
    /// from the source first.
    /// </para>
    /// </summary>
    /// <param name="treeId">The tree whose former source to reclaim.</param>
    /// <param name="partition">The WAL partition whose source to reclaim.</param>
    /// <param name="sourceProviderKey">The provider key the partition was moved away from.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<WalMoveReceipt> ReclaimMovedWalSourceAsync(string treeId, int partition, string sourceProviderKey, CancellationToken cancellationToken = default);
}
