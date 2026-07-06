using System.Diagnostics.Metrics;
using Orleans.Lattice.Dashboards;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Asserts every instrument on the <c>orleans.lattice.backup</c> meter is charted
/// by the bundled Backup &amp; Restore dashboard, reusing the shared
/// <see cref="MeterDashboardCoverageTestsBase"/> drift guard so a new backup
/// instrument cannot ship without a panel (and a renamed / removed instrument
/// cannot leave a dashboard panel dangling).
/// </summary>
[TestFixture]
public sealed class BackupMeterDashboardCoverageTests : MeterDashboardCoverageTestsBase
{
    protected override string MeterName => BackupMetrics.MeterName;

    protected override Meter Meter
    {
        get
        {
            // The cross-tree-fence instruments are declared on BackupMetrics, but the
            // capture / restore / retention / scheduler counters and the inventory
            // observable gauges are declared on LatticeBackupMetrics. Touch a public
            // field on that type so its static initialiser runs and publishes every
            // instrument on the shared backup meter before the base's snapshot
            // MeterListener enumerates it.
            _ = LatticeBackupMetrics.Captures;
            return BackupMetrics.Meter;
        }
    }

    protected override IEnumerable<string> DashboardJson => new[]
    {
        LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.Backup),
    };
}
