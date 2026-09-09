using Orleans.Lattice.Testing;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Asserts every instrument declared in the grain-index package source is listed, by
/// its exact dotted name, in the instrument-to-panel reference map, reusing the
/// shared <see cref="MetricsDocCoverageTestsBase"/> drift guard so a new grain-index
/// instrument cannot ship without a documentation entry.
/// </summary>
/// <remarks>
/// The package publishes onto the core <c>orleans.lattice</c> meter rather than one
/// of its own, so it declares no meter-name literal to exclude. It complements
/// <see cref="GrainIndexMeterDashboardCoverageTests"/>, which asserts panel
/// reference rather than documentation.
/// </remarks>
[TestFixture]
public sealed class GrainIndexMetricsDocCoverageTests : MetricsDocCoverageTestsBase
{
    protected override IEnumerable<string> SourceRoots => new[] { "src/lattice.grainindex" };

    protected override IEnumerable<string> DocRelativePaths => new[]
    {
        "docs/lattice.dashboards/metrics-to-panel-map.md",
    };
}
