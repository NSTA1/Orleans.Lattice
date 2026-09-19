using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Asserts every instrument declared in the scaling package source is listed, by its
/// exact dotted name, in the instrument-to-panel reference map, reusing the shared
/// <see cref="MetricsDocCoverageTestsBase"/> drift guard so a new scaling instrument
/// cannot ship without a documentation entry.
/// </summary>
/// <remarks>
/// This complements <see cref="ScalingMeterDashboardCoverageTests"/>, which asserts
/// panel reference rather than documentation.
/// </remarks>
[TestFixture]
public sealed class ScalingMetricsDocCoverageTests : MetricsDocCoverageTestsBase
{
    protected override IEnumerable<string> SourceRoots => new[] { "src/lattice.scaling" };

    protected override IEnumerable<string> DocRelativePaths => new[]
    {
        "docs/lattice.dashboards/metrics-to-panel-map.md",
    };

    // The scaling meter name shares the instrument prefix but is not itself an instrument.
    protected override IReadOnlySet<string> NonInstrumentLiterals { get; } =
        new HashSet<string>(StringComparer.Ordinal)
        {
            "orleans.lattice.scaling",
        };
}
