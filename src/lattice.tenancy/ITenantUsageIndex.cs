namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The warm, in-memory index of per-tenant admission inputs (quotas plus the
/// global and local usage aggregates), read on the allocation-free write-admission
/// path. Backed by an immutable snapshot the maintainer swaps atomically whenever
/// the tenant registry or the usage tree changes.
/// </summary>
internal interface ITenantUsageIndex
{
    /// <summary>
    /// Attempts to get the warm admission view for a tenant. A pure in-memory
    /// lookup with no allocation on the found or not-found path.
    /// </summary>
    /// <param name="tenant">The tenant to look up. Must be an initialised tenant id.</param>
    /// <param name="view">The admission view when present; otherwise <c>default</c>.</param>
    /// <returns><c>true</c> when the tenant is present in the current snapshot.</returns>
    bool TryGetView(TenantId tenant, out TenantUsageView view);
}
