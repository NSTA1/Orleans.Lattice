namespace Orleans.Lattice.Tenancy;

/// <summary>
/// The fail-closed read surface over per-tenant observability. A caller reads its
/// own active tenant's usage, quota, burst, and overage snapshot; the all-tenant
/// view is available only to a platform operator that explicitly asserts
/// cluster-wide scope (<see cref="TenantObservabilityScope.ClusterWide"/>), which
/// the implementation validates against the auth gate's platform-operator root of
/// trust. There is no ambient all-tenant view: an unasserted or unvalidated read
/// fails closed to the caller's active tenant.
/// </summary>
/// <remarks>
/// This is the single narrowest visibility seam for observability reads; tenant
/// ownership is never taken from a wire-supplied classification but derived from
/// the ambient active tenant (<see cref="LatticeActiveTenantContext"/>) and the
/// registry-backed usage index, and the operator subject is validated, never
/// trusted. Mirrors the isolation convention established by the tenancy
/// enumeration and backup seams.
/// </remarks>
public interface ITenantObservabilityView
{
    /// <summary>
    /// Reads the caller's own active-tenant observability snapshot, or <c>null</c>
    /// when there is no ambient active tenant or the active tenant is not present
    /// in the warm usage index. Never exposes another tenant's series.
    /// </summary>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The active tenant's snapshot, or <c>null</c> when none is resolvable.</returns>
    Task<TenantObservabilitySnapshot?> GetActiveTenantAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Enumerates observability snapshots under <paramref name="scope"/>. The
    /// default <see cref="TenantObservabilityScope.ActiveTenant"/> scope yields
    /// only the caller's active-tenant snapshot (or nothing when none is
    /// resolvable). The explicit <see cref="TenantObservabilityScope.ClusterWide"/>
    /// scope yields every tenant's snapshot only when its subject validates as a
    /// platform operator; otherwise it fails closed to the active-tenant scope.
    /// </summary>
    /// <param name="scope">The visibility scope the caller asserts.</param>
    /// <param name="cancellationToken">Cancels the enumeration.</param>
    /// <returns>An async stream of per-tenant observability snapshots.</returns>
    IAsyncEnumerable<TenantObservabilitySnapshot> ListAsync(
        TenantObservabilityScope scope,
        CancellationToken cancellationToken = default);
}
