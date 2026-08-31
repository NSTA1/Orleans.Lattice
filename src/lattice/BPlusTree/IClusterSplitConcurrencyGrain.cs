namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Cluster-wide singleton admission gate that caps the aggregate number of
/// autonomic shard splits in flight concurrently across <em>all</em> trees.
/// <para>
/// Only consulted for <em>admission</em> when
/// <see cref="LatticeOptions.MaxClusterConcurrentAutoSplits"/> is set to a
/// positive value; when it is <c>null</c> (the default) no monitor ever requests
/// a slot, so the admission ledger stays empty and the ceiling is entirely off
/// the hot path. A single well-known activation is used (integer key <c>0</c>).
/// </para>
/// <para>
/// The gate is driven by per-tree heartbeats rather than long-lived permits:
/// each enabled monitor reports its authoritative in-flight split count (derived
/// from shard <c>IsSplitting</c> status) every sampling pass and receives a grant
/// of new slots against the remaining cluster headroom. A footprint carries a
/// time-to-live, so a silo that crashes and stops reporting has its share
/// reclaimed on expiry rather than wedging splitting cluster-wide.
/// </para>
/// <para>
/// Independently of admission, the gate is also the cluster's <em>readable</em>
/// split-activity source: monitors publish their footprint through
/// <see cref="ReportInFlightAsync"/> even when no ceiling is configured (only
/// while they actually have splits in flight, so an idle tree calls nothing),
/// and <see cref="GetActivityAsync"/> reduces those footprints into the snapshot
/// surfaced by <see cref="ILatticeAdmin.GetSplitActivityAsync"/>. Those
/// observation-only footprints are tracked separately and never consume
/// admission headroom: a tree with no ceiling configured never agreed to share a
/// cluster budget, so it must not be able to throttle a tree that opted in.
/// </para>
/// Key format: singleton integer key <c>0</c>.
/// </summary>
[Alias(TypeAliases.IClusterSplitConcurrencyGrain)]
internal interface IClusterSplitConcurrencyGrain : IGrainWithIntegerKey
{
    /// <summary>
    /// Reports the calling tree's current in-flight autonomic split count and
    /// requests up to <paramref name="desiredNew"/> additional slots under the
    /// cluster-wide ceiling. Stale per-tree footprints are reconciled out first;
    /// the caller's footprint is then refreshed and the number of newly admitted
    /// splits is returned. The caller must trigger no more than the returned
    /// number of new splits this pass.
    /// </summary>
    /// <param name="treeId">The calling tree's id.</param>
    /// <param name="currentInFlight">The tree's authoritative in-flight split count this pass (from shard <c>IsSplitting</c>).</param>
    /// <param name="desiredNew">How many additional splits the tree wants to start this pass.</param>
    /// <param name="clusterCap">The current cluster-wide ceiling (the caller's resolved option value).</param>
    /// <param name="ttl">How long the caller's reported footprint remains valid before it may be reclaimed by expiry.</param>
    /// <returns>The number of new splits admitted (between 0 and <paramref name="desiredNew"/> inclusive).</returns>
    Task<int> AcquireSlotsAsync(string treeId, int currentInFlight, int desiredNew, int clusterCap, TimeSpan ttl);

    /// <summary>
    /// Reports the calling tree's current in-flight autonomic split count
    /// <em>without</em> requesting any new slots. This is the path taken when the
    /// cluster-wide ceiling is not configured: the tree still publishes its
    /// footprint so the gate stays an authoritative, readable cluster-wide
    /// split-activity source (<see cref="GetActivityAsync"/>), but no admission
    /// decision is made and nothing is granted.
    /// <para>
    /// Callers report edge-triggered - when they have splits in flight, and once
    /// more to clear a previously reported footprint - so an idle cluster costs
    /// no calls at all.
    /// </para>
    /// </summary>
    /// <param name="treeId">The calling tree's id.</param>
    /// <param name="inFlight">The tree's authoritative in-flight split count this pass (from shard <c>IsSplitting</c>). Zero clears the tree's footprint.</param>
    /// <param name="ttl">How long the reported footprint remains valid before it may be reclaimed by expiry.</param>
    /// <returns>A task that completes when the footprint has been recorded.</returns>
    Task ReportInFlightAsync(string treeId, int inFlight, TimeSpan ttl);

    /// <summary>
    /// Returns the current cluster-wide sum of live (non-expired) reported
    /// in-flight splits across every tree. Intended for observation and tests.
    /// </summary>
    Task<int> GetClusterInFlightAsync();

    /// <summary>
    /// Returns the cluster-wide split-activity snapshot: the summed live
    /// in-flight count plus how many trees contributed to it. This is the
    /// readable seam behind <see cref="ILatticeAdmin.GetSplitActivityAsync"/>.
    /// </summary>
    /// <returns>A point-in-time <see cref="SplitActivityReport"/>.</returns>
    Task<SplitActivityReport> GetActivityAsync();
}
