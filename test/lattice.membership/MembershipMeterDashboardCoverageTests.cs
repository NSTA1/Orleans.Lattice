using System.Diagnostics.Metrics;
using Orleans.Lattice.Dashboards;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Membership.Tests;

/// <summary>
/// Asserts every instrument on the <c>orleans.lattice.membership</c> meter is
/// charted by the bundled Identity &amp; Authorization dashboard, reusing the
/// shared <see cref="MeterDashboardCoverageTestsBase"/> drift guard so a new
/// membership instrument cannot ship without a panel.
/// </summary>
[TestFixture]
public sealed class MembershipMeterDashboardCoverageTests : MeterDashboardCoverageTestsBase
{
    protected override string MeterName => LatticeMembershipMetrics.MeterName;

    protected override Meter Meter => LatticeMembershipMetrics.Meter;

    protected override IEnumerable<string> DashboardJson => new[]
    {
        LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.Authorization),
    };
}
