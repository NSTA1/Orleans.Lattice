using System.Diagnostics.Metrics;
using Orleans.Lattice.Dashboards;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Asserts every grain-index instrument is charted by the bundled grain-index
/// Grafana dashboard, and that the dashboard references no instrument that does
/// not exist, reusing the shared <see cref="MeterDashboardCoverageTestsBase"/>
/// drift guard.
/// </summary>
/// <remarks>
/// <para>
/// The package publishes on the shared core <c>orleans.lattice</c> meter rather
/// than one of its own, so the guard is narrowed to the instruments this package
/// owns: requiring the grain-index dashboard to also chart every core lattice
/// instrument would be nonsense, and those are covered by the core's own
/// dashboards and their guard in the dashboards test project.
/// </para>
/// <para>
/// The observable gauges are created by the <see cref="GrainIndexMetrics"/> type
/// initialiser, so a snapshot <see cref="MeterListener"/> sees them once that
/// type has been touched; naming them explicitly makes the coverage requirement
/// independent of whether some earlier test happened to touch it.
/// </para>
/// </remarks>
[TestFixture]
public sealed class GrainIndexMeterDashboardCoverageTests : MeterDashboardCoverageTestsBase
{
    private const string GrainIndexPrefix = "orleans.lattice.grainindex.";

    protected override string MeterName => GrainIndexMetrics.MeterName;

    protected override Meter Meter => GrainIndexMetrics.Meter;

    protected override bool IncludeInstrument(string instrumentName) =>
        instrumentName.StartsWith(GrainIndexPrefix, StringComparison.Ordinal);

    protected override IEnumerable<string> AdditionalInstrumentNames =>
    [
        GrainIndexMetrics.GrainsEnrolledName,
        GrainIndexMetrics.EntriesName,
        GrainIndexMetrics.WriteFailuresName,
        GrainIndexMetrics.ProjectionDurationName,
        GrainIndexMetrics.BackfillProcessedName,
        GrainIndexMetrics.BackfillTotalName,
        GrainIndexMetrics.BackfillPercentCompleteName,
        GrainIndexMetrics.BackfillStateName,
    ];

    protected override IEnumerable<string> DashboardJson =>
    [
        LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.GrainIndex),
    ];

    [Test]
    public void The_bundled_dashboard_kind_resolves_to_a_grain_index_dashboard()
    {
        var json = LatticeDashboards.GetGrafanaDashboardJson(LatticeDashboardKind.GrainIndex);

        Assert.Multiple(() =>
        {
            Assert.That(json, Is.Not.Null.And.Not.Empty);
            Assert.That(json, Does.Contain("orleans-lattice-grainindex"));
        });
    }
}
