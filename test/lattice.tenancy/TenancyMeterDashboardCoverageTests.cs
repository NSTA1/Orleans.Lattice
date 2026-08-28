using System.Diagnostics.Metrics;
using Orleans.Lattice.Dashboards;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Asserts every instrument on the <c>orleans.lattice.tenancy</c> meter is charted
/// by the bundled per-tenant observability dashboard, reusing the shared
/// <see cref="MeterDashboardCoverageTestsBase"/> drift guard so a new tenancy
/// instrument cannot ship without a panel.
/// </summary>
/// <remarks>
/// Tenancy was the only add-on meter without this guard, which is why its
/// dashboard could ship while <c>docs/lattice.dashboards/metrics-to-panel-map.md</c>
/// carried no <c>orleans.lattice.tenancy</c> section at all (issue #1648).
/// </remarks>
[TestFixture]
public sealed class TenancyMeterDashboardCoverageTests : MeterDashboardCoverageTestsBase
{
    protected override string MeterName => LatticeTenantMetrics.MeterName;

    protected override Meter Meter => LatticeTenantMetrics.Meter;

    // Every tenancy instrument is an observable gauge created only when the
    // tenancy add-on's observability publisher starts (see
    // TenantObservabilityGaugeRegistry.EnsureRegistered), so a snapshot
    // MeterListener at test time does not see them; name them explicitly so the
    // coverage guard still requires a panel for each.
    protected override IEnumerable<string> AdditionalInstrumentNames => new[]
    {
        LatticeTenantMetrics.TenantsName,
        LatticeTenantMetrics.UsageBytesName,
        LatticeTenantMetrics.UsageKeysName,
        LatticeTenantMetrics.UsageMemoryBytesName,
        LatticeTenantMetrics.UsageTreesName,
        LatticeTenantMetrics.QuotaBytesName,
        LatticeTenantMetrics.QuotaKeysName,
        LatticeTenantMetrics.QuotaMemoryBytesName,
        LatticeTenantMetrics.QuotaTreesName,
        LatticeTenantMetrics.QuotaBurstPercentName,
        LatticeTenantMetrics.OverageBytesName,
        LatticeTenantMetrics.OverageKeysName,
        LatticeTenantMetrics.OverageMemoryBytesName,
        LatticeTenantMetrics.OverageTreesName,
    };

    protected override IEnumerable<string> DashboardJson => new[]
    {
        LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.Tenancy),
    };
}
