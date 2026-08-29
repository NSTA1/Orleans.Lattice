using System.Diagnostics.Metrics;

namespace Orleans.Lattice.Tenancy;

/// <summary>
/// Telemetry naming conventions and <see cref="System.Diagnostics.Metrics"/>
/// instruments for per-tenant observability in <c>Orleans.Lattice.Tenancy</c>.
/// Every tenant-scoped usage, quota, burst, and overage series is published on a
/// single <see cref="Meter"/> named <see cref="MeterName"/> and tagged by tenant
/// (<see cref="TagTenant"/>) so an OpenTelemetry pipeline can subscribe once and
/// attribute every series to the owning tenant. Mirrors the structure of
/// <c>Orleans.Lattice.Scaling.LatticeScalingMetrics</c>.
/// </summary>
/// <remarks>
/// <para>
/// Every instrument on this meter is an <see cref="ObservableGauge{T}"/>. The
/// tenancy add-on's observability publisher samples the warm per-tenant usage
/// index and the durable overage billing seam on its own timer and publishes a
/// frozen per-tenant snapshot (see <c>TenantObservabilityGaugeRegistry</c>); each
/// gauge's measurement callback simply returns the pre-built
/// <see cref="Measurement{T}"/> array for that snapshot, so the scrape path
/// allocates nothing and recomputes nothing.
/// </para>
/// <para>
/// The gauges are created only when the tenancy add-on is registered (the
/// publisher calls <c>TenantObservabilityGaugeRegistry.EnsureRegistered</c> at
/// start), so a cluster with tenancy disabled publishes no tenancy meter and adds
/// no per-tenant series at all - the tenancy-off path is byte-for-byte unchanged.
/// A snapshot <see cref="MeterListener"/> attached before the add-on starts will
/// not see the gauges; subscribers should enumerate by the published
/// <c>...Name</c> constants.
/// </para>
/// </remarks>
public static class LatticeTenantMetrics
{
    /// <summary>
    /// The root meter name for all <c>Orleans.Lattice.Tenancy</c> per-tenant
    /// observability telemetry. Internal telemetry hooks and external subscribers
    /// must reference this constant rather than hard-coding the string.
    /// </summary>
    public const string MeterName = "orleans.lattice.tenancy";

    /// <summary>
    /// The tag key carrying the owning tenant id on every tenant-scoped series.
    /// The single dimension by which a tenant's usage, quota, burst, and overage
    /// series are attributable. Aliases <see cref="LatticeTenantLabel.TagTenant"/>,
    /// the repository-wide derived tenant dimension, so this meter's per-tenant
    /// series and the derived label every other meter emits share one key.
    /// </summary>
    public const string TagTenant = LatticeTenantLabel.TagTenant;

    /// <summary>
    /// Canonical name of the registered-tenant-count observable gauge - the number
    /// of tenants in the warm usage index. A cluster-aggregate operator signal, so
    /// it is not attributable to any one tenant: it carries the reserved platform
    /// sentinel (<see cref="LatticeTenantLabel.PlatformTenant"/>) as its
    /// <see cref="TagTenant"/> value rather than a tenant id.
    /// </summary>
    public const string TenantsName = "orleans.lattice.tenancy.tenants";

    /// <summary>Canonical name of the per-tenant stored-bytes usage observable gauge.</summary>
    public const string UsageBytesName = "orleans.lattice.tenancy.usage.bytes";

    /// <summary>Canonical name of the per-tenant live-key-count usage observable gauge.</summary>
    public const string UsageKeysName = "orleans.lattice.tenancy.usage.keys";

    /// <summary>Canonical name of the per-tenant resident-memory usage observable gauge.</summary>
    public const string UsageMemoryBytesName = "orleans.lattice.tenancy.usage.memory_bytes";

    /// <summary>Canonical name of the per-tenant owned-tree-count usage observable gauge.</summary>
    public const string UsageTreesName = "orleans.lattice.tenancy.usage.trees";

    /// <summary>
    /// Canonical name of the per-tenant byte-quota observable gauge - the tenant's
    /// steady-state byte ceiling. A measurement is emitted only for a tenant whose
    /// byte quota is bounded; an unbounded dimension contributes no series.
    /// </summary>
    public const string QuotaBytesName = "orleans.lattice.tenancy.quota.bytes";

    /// <summary>
    /// Canonical name of the per-tenant key-quota observable gauge - the tenant's
    /// steady-state key ceiling. Emitted only for a bounded key quota.
    /// </summary>
    public const string QuotaKeysName = "orleans.lattice.tenancy.quota.keys";

    /// <summary>
    /// Canonical name of the per-tenant memory-quota observable gauge - the
    /// tenant's steady-state resident-memory ceiling. Emitted only for a bounded
    /// memory quota.
    /// </summary>
    public const string QuotaMemoryBytesName = "orleans.lattice.tenancy.quota.memory_bytes";

    /// <summary>
    /// Canonical name of the per-tenant tree-count-quota observable gauge - the
    /// tenant's steady-state owned-tree ceiling. Emitted only for a bounded
    /// tree-count quota.
    /// </summary>
    public const string QuotaTreesName = "orleans.lattice.tenancy.quota.trees";

    /// <summary>
    /// Canonical name of the per-tenant burst-headroom observable gauge - the
    /// transient overage percentage (<see cref="TenantQuotas.BurstPercent"/>) a
    /// tenant may momentarily exceed its bounded ceilings by before admission
    /// control engages.
    /// </summary>
    public const string QuotaBurstPercentName = "orleans.lattice.tenancy.quota.burst_percent";

    /// <summary>
    /// Canonical name of the per-tenant metered byte-overage observable gauge - the
    /// converged, durable Riemann-sum byte overage accrued by the overage meter and
    /// read from <see cref="ITenantOverageBilling"/>. The first-class billing
    /// overage signal, dimensioned by tenant.
    /// </summary>
    public const string OverageBytesName = "orleans.lattice.tenancy.overage.bytes";

    /// <summary>Canonical name of the per-tenant metered key-overage observable gauge.</summary>
    public const string OverageKeysName = "orleans.lattice.tenancy.overage.keys";

    /// <summary>Canonical name of the per-tenant metered memory-overage observable gauge.</summary>
    public const string OverageMemoryBytesName = "orleans.lattice.tenancy.overage.memory_bytes";

    /// <summary>Canonical name of the per-tenant metered tree-count-overage observable gauge.</summary>
    public const string OverageTreesName = "orleans.lattice.tenancy.overage.trees";

    /// <summary>
    /// The meter that owns every per-tenant observability instrument. Exposed
    /// publicly so integration tests and custom OpenTelemetry exporters can
    /// subscribe by reference rather than by name.
    /// </summary>
    public static readonly Meter Meter = new(MeterName);
}
