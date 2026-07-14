using System.Diagnostics.Metrics;
using Orleans.Lattice.Dashboards;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Replication.Grpc.Tests;

/// <summary>
/// Asserts every instrument on the <c>orleans.lattice.replication.grpc</c> meter
/// is charted by the bundled Replication Transport (gRPC) dashboard, reusing the
/// shared <see cref="MeterDashboardCoverageTestsBase"/> drift guard so a new
/// gRPC-transport instrument cannot ship without a panel (and a renamed / removed
/// instrument cannot leave a dashboard panel dangling).
/// </summary>
[TestFixture]
public sealed class ReplicationGrpcMeterDashboardCoverageTests : MeterDashboardCoverageTestsBase
{
    protected override string MeterName => LatticeReplicationGrpcMetrics.MeterName;

    protected override Meter Meter => LatticeReplicationGrpcMetrics.Meter;

    // The insecure-channel counter is created lazily on the meter's static
    // initialiser; name it explicitly so the coverage guard requires a panel
    // even if a snapshot MeterListener races the first construction.
    protected override IEnumerable<string> AdditionalInstrumentNames => new[]
    {
        LatticeReplicationGrpcMetrics.InsecureChannelName,
    };

    protected override IEnumerable<string> DashboardJson => new[]
    {
        LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.ReplicationGrpc),
    };
}
