namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The durable store of per-tenant overage meters over the reserved
/// <c>sys-tenant-overage</c> tree. Reads and meter-writes address the backing tree
/// under system-origin; a meter merges the caller's per-cluster grow-only counter
/// increment into any stored record with the record's own CRDT join, so concurrent
/// metering from every cluster converges. The seam is internal to the tenancy
/// package so the overage meter and billing reader can be driven against a
/// substitute in unit tests.
/// </summary>
internal interface ITenantOverageStore
{
    /// <summary>
    /// Reads the overage meter for a tenant, or <c>null</c> when no cluster has yet
    /// metered any overage for it.
    /// </summary>
    /// <param name="tenant">The tenant to read. Must be an initialised (parsed) tenant id.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The tenant's overage meter, or <c>null</c> when absent.</returns>
    Task<TenantOverageRecord?> GetAsync(TenantId tenant, CancellationToken cancellationToken = default);

    /// <summary>Enumerates every tenant's overage meter.</summary>
    /// <param name="cancellationToken">Cancels the enumeration.</param>
    /// <returns>An async stream of overage meters.</returns>
    IAsyncEnumerable<TenantOverageRecord> ListAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Merges <paramref name="cluster"/>'s grow-only overage increment for
    /// <paramref name="tenant"/> into the stored meter and persists the converged
    /// result. Because the counters are grow-only and each cluster advances only its
    /// own component, replaying a failed write re-applies the same single increment
    /// and never double-counts.
    /// </summary>
    /// <param name="tenant">The tenant whose overage is metered. Must be an initialised tenant id.</param>
    /// <param name="cluster">The metering cluster's id (the replica key). Must not be <c>null</c> or empty.</param>
    /// <param name="increment">The overage observed this tick to add to the meter.</param>
    /// <param name="cancellationToken">Cancels the meter write.</param>
    /// <returns>The stored meter after the merge.</returns>
    Task<TenantOverageRecord> MeterAsync(
        TenantId tenant,
        string cluster,
        TenantOverageSample increment,
        CancellationToken cancellationToken = default);
}
