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

    /// <summary>
    /// The cadence on which each silo re-meters every registered tenant's trees and
    /// rolls the result up into that tenant's per-cluster usage slot. Defaults to
    /// 30 seconds. Set to <see cref="TimeSpan.Zero"/> (or a negative value) to
    /// disable metering entirely, which leaves quota admission permanently in its
    /// fail-open state - useful only for a deployment that deliberately runs
    /// tenancy without resource governance.
    /// </summary>
    /// <remarks>
    /// This cadence is what makes an authored quota bind. Metering is the input to
    /// admission: with no sample landing, <c>LatticeTenantAdmissionController</c>
    /// takes its documented "fail open until the first sample lands" branch, so a
    /// quota can never be breached. The interval trades enforcement latency against
    /// the cost of the per-cycle walk - a shorter interval detects a breach sooner,
    /// a longer one costs less. The hysteresis band
    /// (<see cref="PublishMinAbsoluteDelta"/> / <see cref="PublishMinRelativeDelta"/>)
    /// independently suppresses a republish when the roll-up barely moved, so a
    /// short cadence does not by itself churn the registry.
    /// </remarks>
    public TimeSpan MeterInterval { get; set; } = TimeSpan.FromSeconds(30);
}
