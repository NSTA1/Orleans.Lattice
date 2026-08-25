namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Options controlling the aggregate per-tenant usage-accounting and quota
/// enforcement layer: the enforcement scope quotas are admitted against, and the
/// hysteresis band that gates how often a cluster republishes its local usage
/// sample. Resolved through the standard options system and configured via
/// <c>AddLatticeTenancy(...)</c> or <c>ConfigureLatticeTenancy(...)</c>.
/// </summary>
public sealed class TenantUsageAccountingOptions
{
    /// <summary>
    /// The scope a tenant's quota is enforced against on the write-admission path.
    /// Defaults to <see cref="TenantEnforcementScope.GlobalConverged"/>, which
    /// bounds the tenant's total footprint across the online resident clusters.
    /// </summary>
    public TenantEnforcementScope DefaultEnforcementScope { get; set; } = TenantEnforcementScope.GlobalConverged;

    /// <summary>
    /// The minimum absolute change, in a sample's largest dimension unit, that a
    /// re-roll-up must move before the cluster republishes its usage slot. A
    /// smaller movement is suppressed by the hysteresis gate so a stream of
    /// negligible deltas does not churn the registry. Defaults to
    /// <c>64 * 1024</c> (64 KiB / keys / trees), and must be non-negative.
    /// </summary>
    public long PublishMinAbsoluteDelta { get; set; } = 64 * 1024;

    /// <summary>
    /// The minimum <em>relative</em> change, as a fraction of the last published
    /// value on a dimension, that a re-roll-up must move before the cluster
    /// republishes. Applied per dimension alongside
    /// <see cref="PublishMinAbsoluteDelta"/>: a movement republishes when it clears
    /// that dimension's significance band, which is the larger of the absolute
    /// floor and this relative fraction of the last value. Defaults to <c>0.05</c>
    /// (5%), and must be non-negative.
    /// </summary>
    public double PublishMinRelativeDelta { get; set; } = 0.05;
}
