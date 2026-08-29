namespace Orleans.Lattice.Api.TenantAdmin;

/// <summary>
/// The aggregate a tenant's quota reading was taken against, reported on
/// <see cref="TenantQuotaUsageReport.EnforcementScope"/>. It mirrors the tenancy
/// engine's own enforcement-scope enum without taking a dependency on the tenancy
/// add-on, so the shared contract package stays free of the engine's internals:
/// the facade maps between this enum and the engine scope at the single
/// implementation seam.
/// </summary>
/// <remarks>
/// A usage figure is meaningless without this qualifier: the same tenant reads
/// very differently as a converged cross-cluster sum than as one cluster's local
/// share, so a surface rendering a quota bar must present the scope alongside the
/// number rather than implying a global total it may not have.
/// </remarks>
[GenerateSerializer]
[Alias(ApiTenantAdminTypeAliases.TenantQuotaEnforcementScope)]
public enum TenantQuotaEnforcementScope
{
    /// <summary>
    /// The reading is the converged cross-cluster fold: the tenant's total
    /// footprint summed over the online resident clusters. Converged best-effort
    /// with bounded overshoot, so a concurrent write elsewhere can momentarily
    /// leave the true total slightly above the reported one.
    /// </summary>
    GlobalConverged = 0,

    /// <summary>
    /// The reading is this cluster's local sample only. It is <em>not</em> the
    /// tenant's global footprint: each cluster enforces the quota independently,
    /// so the tenant's true total across the fabric may be as high as
    /// (clusters x quota).
    /// </summary>
    PerCluster = 1,
}
