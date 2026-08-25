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

    /// <summary>
    /// Ensures the warm snapshot has been built at least once, building it
    /// synchronously (awaited) when it is still cold. Idempotent. Used by the
    /// off-path observability enumeration to warm the index before a bulk read;
    /// the admission path never awaits (it reads whatever warm snapshot exists).
    /// </summary>
    /// <param name="cancellationToken">Cancels this caller's wait.</param>
    Task EnsureWarmAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// The tenants in the current warm snapshot, keyed by tenant id text, each
    /// with its usage view (quotas plus the usage aggregates). An off-path bulk
    /// read used by per-tenant observability; the warm admission path uses only
    /// <see cref="TryGetView"/>. Never <c>null</c>; empty before the first compile.
    /// </summary>
    /// <returns>The current tenant-to-usage-view map.</returns>
    IReadOnlyDictionary<string, TenantUsageView> EnumerateViews();
}
