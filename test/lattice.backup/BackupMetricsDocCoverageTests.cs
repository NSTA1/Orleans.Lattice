using System.Diagnostics.Metrics;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Asserts every instrument on the <c>orleans.lattice.backup</c> meter is listed,
/// by its exact dotted name, in the instrument-to-panel reference map, reusing the
/// shared <see cref="MetricsDocCoverageTestsBase"/> drift guard so a new backup
/// instrument cannot ship without a documentation entry.
/// </summary>
[TestFixture]
public sealed class BackupMetricsDocCoverageTests : MetricsDocCoverageTestsBase
{
    protected override Meter Meter
    {
        get
        {
            // Touch a public field on LatticeBackupMetrics so its static initialiser
            // runs and publishes every backup instrument (including the inventory
            // observable gauges) on the shared meter before the base enumerates it.
            _ = LatticeBackupMetrics.Captures;
            return BackupMetrics.Meter;
        }
    }

    protected override IEnumerable<string> DocRelativePaths => new[]
    {
        "docs/lattice.dashboards/metrics-to-panel-map.md",
    };
}
