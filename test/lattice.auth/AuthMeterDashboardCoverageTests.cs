using System.Diagnostics.Metrics;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Dashboards;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Asserts every instrument on the <c>orleans.lattice.auth</c> meter is charted
/// by the bundled Identity &amp; Authorization dashboard, reusing the shared
/// <see cref="MeterDashboardCoverageTestsBase"/> drift guard so a new
/// authorization instrument cannot ship without a panel.
/// </summary>
[TestFixture]
public sealed class AuthMeterDashboardCoverageTests : MeterDashboardCoverageTestsBase
{
    protected override string MeterName => LatticeAuthMetrics.MeterName;

    protected override Meter Meter => LatticeAuthMetrics.Meter;

    // The compiled-snapshot epoch / age gauges are observable instruments created
    // by the live snapshot maintainers, so a snapshot MeterListener at test time
    // does not see them; name them explicitly so the coverage guard still
    // requires a panel.
    protected override IEnumerable<string> AdditionalInstrumentNames => new[]
    {
        LatticeAuthMetrics.SnapshotEpochName,
        LatticeAuthMetrics.SnapshotAgeName,
        LatticeAuthMetrics.SnapshotSubjectsName,
    };

    protected override IEnumerable<string> DashboardJson => new[]
    {
        LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.Authorization),
    };
}
