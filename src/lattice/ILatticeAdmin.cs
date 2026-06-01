namespace Orleans.Lattice;

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
    /// </summary>
    /// <param name="cancellationToken">Cancels the cluster-wide fan-out before it begins.</param>
    Task<ClusterStorageUsageReport> GetTotalStorageUsageAsync(CancellationToken cancellationToken = default);
}
