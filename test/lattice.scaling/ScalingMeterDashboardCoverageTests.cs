using System.Diagnostics.Metrics;
using Orleans.Lattice.Dashboards;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Asserts every instrument on the <c>orleans.lattice.scaling</c> meter is
/// charted by the bundled autoscaling-signal dashboard, reusing the shared
/// <see cref="MeterDashboardCoverageTestsBase"/> drift guard so a new scaling
/// instrument cannot ship without a panel.
/// </summary>
[TestFixture]
public sealed class ScalingMeterDashboardCoverageTests : MeterDashboardCoverageTestsBase
{
    protected override string MeterName => LatticeScalingMetrics.MeterName;

    protected override Meter Meter => LatticeScalingMetrics.Meter;

    // Every scaling instrument is an observable gauge created only when the live
    // facade starts, so a snapshot MeterListener at test time does not see them;
    // name them explicitly so the coverage guard still requires a panel.
    protected override IEnumerable<string> AdditionalInstrumentNames => new[]
    {
        LatticeScalingMetrics.ScaleValueName,
        LatticeScalingMetrics.RawScaleValueName,
        LatticeScalingMetrics.ComputeActivationPressureName,
        LatticeScalingMetrics.ComputeResourcePressureName,
        LatticeScalingMetrics.ComputeWalDispatchPressureName,
        LatticeScalingMetrics.ComputeReplicasName,
        LatticeScalingMetrics.StorageAccountsOverThresholdName,
        LatticeScalingMetrics.StorageRebalanceRecommendationsName,
    };

    protected override IEnumerable<string> DashboardJson => new[]
    {
        LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.Scaling),
    };
}
