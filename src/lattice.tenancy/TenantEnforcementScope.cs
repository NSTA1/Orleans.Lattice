namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Selects which aggregate a tenant's quota is enforced against on the
/// write-admission path.
/// </summary>
/// <remarks>
/// This is an in-process enforcement-policy selector, not persisted registry
/// state and never crossing a grain boundary, so it carries no Orleans
/// serialization attributes. It is resolved from
/// <see cref="TenantUsageAccountingOptions.DefaultEnforcementScope"/>.
/// </remarks>
public enum TenantEnforcementScope
{
    /// <summary>
    /// The default. Admits against the global sum-fold of the tenant's usage over
    /// the online resident clusters, so a tenant's total footprint across the
    /// whole cluster fabric is bounded. Consistency is converged best-effort with
    /// bounded overshoot: a concurrent write on another cluster can momentarily
    /// push the true global total slightly over the quota before the per-cluster
    /// usage slots re-converge.
    /// </summary>
    GlobalConverged = 0,

    /// <summary>
    /// Admits against only this cluster's local usage sample, so each cluster
    /// enforces the quota independently against what it alone hosts. There is no
    /// cross-cluster fold and therefore no cross-cluster overshoot, at the cost of
    /// allowing the tenant's true global footprint to reach (clusters x quota).
    /// </summary>
    PerCluster = 1,
}
