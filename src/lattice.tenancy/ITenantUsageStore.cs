namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The durable store of per-tenant usage records over the reserved
/// <c>sys-tenant-usage</c> tree. Reads and publishes address the backing tree
/// under system-origin; a publish merges the caller's per-cluster usage slot into
/// any stored record with the record's own last-writer-wins join, so concurrent
/// publishes from every cluster converge. The seam is internal to the tenancy
/// package so the usage publisher and index maintainer can be driven against a
/// substitute in unit tests.
/// </summary>
internal interface ITenantUsageStore
{
    /// <summary>
    /// Reads the usage record for a tenant, or <c>null</c> when no cluster has yet
    /// published a slot for it.
    /// </summary>
    /// <param name="tenant">The tenant to read. Must be an initialised (parsed) tenant id.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The tenant's usage record, or <c>null</c> when absent.</returns>
    Task<TenantUsageRecord?> GetAsync(TenantId tenant, CancellationToken cancellationToken = default);

    /// <summary>Enumerates every tenant's usage record.</summary>
    /// <param name="cancellationToken">Cancels the enumeration.</param>
    /// <returns>An async stream of usage records.</returns>
    IAsyncEnumerable<TenantUsageRecord> ListAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Merges the caller's per-cluster usage slot (carried by
    /// <paramref name="record"/>) into the stored record and persists the converged
    /// result.
    /// </summary>
    /// <param name="record">The record carrying this cluster's slot. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the publish.</param>
    /// <returns>The stored record after the merge.</returns>
    Task<TenantUsageRecord> PublishAsync(TenantUsageRecord record, CancellationToken cancellationToken = default);
}
