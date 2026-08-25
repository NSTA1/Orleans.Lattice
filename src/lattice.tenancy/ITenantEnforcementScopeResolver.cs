namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Resolves the <see cref="TenantEnforcementScope"/> that governs a single
/// tenant's quota admission - whether it is admitted against the cross-cluster
/// <see cref="TenantEnforcementScope.GlobalConverged"/> fold or only its
/// <see cref="TenantEnforcementScope.PerCluster"/> local sample. A seam so the
/// scope can be a cluster-wide default today and a per-tenant override later
/// without changing the admission controller.
/// </summary>
internal interface ITenantEnforcementScopeResolver
{
    /// <summary>
    /// Returns the enforcement scope to admit <paramref name="tenant"/> under. A
    /// pure, allocation-free lookup called on the warm admission path.
    /// </summary>
    /// <param name="tenant">The tenant whose scope is being resolved.</param>
    /// <returns>The enforcement scope for the tenant.</returns>
    TenantEnforcementScope Resolve(TenantId tenant);
}
